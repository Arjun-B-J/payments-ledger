package com.arjunbj.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.balance.StrategyName;
import com.arjunbj.ledger.common.FailPoints.InjectedFault;
import com.arjunbj.ledger.common.FailPoints.Point;
import com.arjunbj.ledger.recon.Reconciler;
import com.arjunbj.ledger.transfer.TransferCommand;
import com.arjunbj.ledger.transfer.TransferService.CreateResult;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class FaultInjectionTest extends IntegrationTest {

    @ParameterizedTest
    @EnumSource(StrategyName.class)
    void crashBetweenDebitAndCreditRollsBackEverything(StrategyName strategy) {
        strategies.use(strategy);
        Account funding = account("funding", true, 1);
        Account alice = account("alice", false, strategy == StrategyName.SHARDED ? 4 : 1);
        Account bob = account("bob", false, 1);
        transfer(funding.id(), alice.id(), 1_000);
        long transfersBefore = count("transfers");
        long entriesBefore = count("entries");
        long outboxBefore = count("outbox");

        // alice.id < bob.id, so the debit leg runs first and the fault fires before the credit.
        failPoints.arm(Point.AFTER_DEBIT_LEG, 1);
        TransferCommand cmd = new TransferCommand(alice.id(), bob.id(), 400, "USD", false);
        assertThatThrownBy(() -> transfers.create("fault-key", cmd)).isInstanceOf(InjectedFault.class);

        assertThat(balance(alice.id())).isEqualTo(1_000);
        assertThat(balance(bob.id())).isZero();
        assertThat(count("transfers")).isEqualTo(transfersBefore);
        assertThat(count("entries")).isEqualTo(entriesBefore);
        assertThat(count("outbox")).isEqualTo(outboxBefore);

        // Nothing committed, so the key is still free and a retry applies the transfer once.
        CreateResult retry = transfers.create("fault-key", cmd);
        assertThat(retry.replayed()).isFalse();
        assertThat(balance(alice.id())).isEqualTo(600);
        assertThat(balance(bob.id())).isEqualTo(400);
        relay.drainAll();
        assertThat(reconciler.run().violations()).isEmpty();
    }

    @Test
    void crashAfterCommitBeforeResponseIsSafeToRetry() {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();
        failPoints.arm(Point.AFTER_COMMIT, 1);

        ResponseEntity<JsonNode> lost = postTransfer("commit-then-crash", body(funding, alice, 250));
        assertThat(lost.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(count("transfers")).isEqualTo(1); // the money moved; the client just never heard

        ResponseEntity<JsonNode> retried = postTransfer("commit-then-crash", body(funding, alice, 250));
        assertThat(retried.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(retried.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        String storedId = jdbc.queryForObject("SELECT id::text FROM transfers", String.class);
        assertThat(retried.getBody().get("id").asText()).isEqualTo(storedId);
        assertThat(count("transfers")).isEqualTo(1);
        assertThat(balance(alice)).isEqualTo(250);
    }

    @Test
    void relayCrashAfterDeliveryRedeliversButTheReadModelAppliesOnce() {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();
        for (int i = 0; i < 5; i++) {
            transfer(funding, alice, 100);
        }
        double duplicatesBefore = meters.counter("ledger.projector.events", "result", "duplicate").count();

        failPoints.arm(Point.RELAY_AFTER_DELIVER, 1);
        assertThatThrownBy(relay::pollOnce).isInstanceOf(InjectedFault.class);
        // The consumer committed; the relay's "published" mark did not.
        assertThat(count("processed_events")).isEqualTo(5);
        assertThat(count("statement_lines")).isEqualTo(10);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class)).isEqualTo(5);

        assertThat(relay.drainAll()).isEqualTo(5); // redelivered
        assertThat(count("statement_lines")).isEqualTo(10); // not 20
        assertThat(meters.counter("ledger.projector.events", "result", "duplicate").count() - duplicatesBefore).isEqualTo(5.0);

        Reconciler.Report report = reconciler.run();
        assertThat(report.readModelChecked()).isTrue();
        assertThat(report.violations()).isEmpty();
    }
}
