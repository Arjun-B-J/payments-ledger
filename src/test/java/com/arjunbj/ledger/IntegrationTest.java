package com.arjunbj.ledger;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.account.AccountService;
import com.arjunbj.ledger.balance.StrategyName;
import com.arjunbj.ledger.balance.StrategyRegistry;
import com.arjunbj.ledger.common.FailPoints;
import com.arjunbj.ledger.outbox.OutboxRelay;
import com.arjunbj.ledger.recon.Reconciler;
import com.arjunbj.ledger.transfer.TransferCommand;
import com.arjunbj.ledger.transfer.TransferService;
import com.arjunbj.ledger.transfer.TransferView;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base for tests against the real schema on embedded PostgreSQL. All subclasses share one Spring
 * context (and one Postgres). The relay and reconciler schedules are off so tests drive them.
 * AutoConfigureObservability turns the Prometheus endpoint on, which Spring Boot disables in tests.
 */
@AutoConfigureObservability
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ledger.outbox.relay.enabled=false",
        "ledger.reconciler.scheduled=false",
        "ledger.db.pool-size=40"})
public abstract class IntegrationTest {

    @Autowired protected JdbcTemplate jdbc;
    @Autowired protected AccountService accounts;
    @Autowired protected TransferService transfers;
    @Autowired protected StrategyRegistry strategies;
    @Autowired protected FailPoints failPoints;
    @Autowired protected OutboxRelay relay;
    @Autowired protected Reconciler reconciler;
    @Autowired protected MeterRegistry meters;
    @Autowired protected TestRestTemplate http;

    @BeforeEach
    void resetLedger() {
        // TRUNCATE is not blocked by the row-level append-only trigger on entries.
        jdbc.execute("TRUNCATE statement_lines, processed_events, outbox, entries, balances, transfers, accounts RESTART IDENTITY CASCADE");
        accounts.clearCache();
        failPoints.disarmAll();
        strategies.use(StrategyName.ATOMIC);
    }

    protected Account account(String name, boolean allowOverdraft, int shards) {
        return accounts.create(name, "USD", allowOverdraft, shards);
    }

    protected long balance(long accountId) {
        return accounts.balance(accountId);
    }

    protected TransferView transfer(long from, long to, long amount) {
        return transfers.create(UUID.randomUUID().toString(), new TransferCommand(from, to, amount, "USD", false)).transfer();
    }

    protected long count(String table) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class);
    }

    protected static Map<String, Object> body(long from, long to, long amount) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("debitAccountId", from);
        body.put("creditAccountId", to);
        body.put("amountMinor", amount);
        body.put("currency", "USD");
        return body;
    }

    protected ResponseEntity<JsonNode> postTransfer(String idempotencyKey, Object body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return http.exchange("/transfers", HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
    }
}
