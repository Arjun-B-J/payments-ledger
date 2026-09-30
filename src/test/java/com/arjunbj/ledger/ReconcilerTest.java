package com.arjunbj.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.arjunbj.ledger.recon.Reconciler;
import com.arjunbj.ledger.recon.Reconciler.Violation;
import com.arjunbj.ledger.transfer.TransferCommand;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

/** The reconciler is only worth something if it catches real breakage, so these tests break things. */
class ReconcilerTest extends IntegrationTest {

    @Test
    void cleanLedgerHasNoViolationsAndReadModelWaitsForTheOutbox() {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();
        transfer(funding, alice, 900);

        Reconciler.Report before = reconciler.run();
        assertThat(before.violations()).isEmpty();
        assertThat(before.readModelChecked()).isFalse();

        relay.drainAll();
        Reconciler.Report after = reconciler.run();
        assertThat(after.ok()).isTrue();
        assertThat(after.readModelChecked()).isTrue();
        assertThat(after.transfers()).isEqualTo(1);
        assertThat(after.entries()).isEqualTo(2);
    }

    @Test
    void detectsABalanceThatNoLongerMatchesItsEntries() {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();
        transfer(funding, alice, 900);
        jdbc.update("UPDATE balances SET balance_minor = balance_minor + 1 WHERE account_id = ?", alice);

        assertThat(reconciler.run().violations()).extracting(Violation::check, Violation::subject)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("balance-matches-entries", "account " + alice));
    }

    @Test
    void detectsAnUnbalancedJournal() {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();
        UUID pending = transfers.create("p", new TransferCommand(funding, alice, 50, "USD", true)).transfer().id();
        // A stray credit on a pending transfer: the journal no longer sums to zero.
        jdbc.update("INSERT INTO entries (transfer_id, account_id, direction, amount_minor, currency) VALUES (?, ?, 'C', 50, 'USD')",
                pending, alice);

        assertThat(reconciler.run().violations()).extracting(Violation::check)
                .contains("transfer-entries-balanced", "journal-sums-to-zero", "balance-matches-entries");
    }

    @Test
    void detectsAReadModelThatDisagreesWithTheJournal() {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();
        transfer(funding, alice, 900);
        relay.drainAll();
        jdbc.update("DELETE FROM statement_lines WHERE account_id = ?", alice);

        assertThat(reconciler.run().violations()).extracting(Violation::check).containsExactly("read-model-matches");
    }

    @Test
    void databaseRefusesOverdraftsAndJournalRewrites() {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();
        transfer(funding, alice, 900);

        assertThatThrownBy(() -> jdbc.update("UPDATE balances SET balance_minor = -1 WHERE account_id = ?", alice))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("balances_no_overdraft");
        assertThatThrownBy(() -> jdbc.update("UPDATE entries SET amount_minor = amount_minor * 2"))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");
        assertThatThrownBy(() -> jdbc.update("DELETE FROM entries"))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");
        assertThat(reconciler.run().violations()).isEmpty();
    }
}
