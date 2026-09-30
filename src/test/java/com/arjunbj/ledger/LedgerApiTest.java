package com.arjunbj.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

class LedgerApiTest extends IntegrationTest {

    private long funding;
    private long alice;
    private long bob;

    @BeforeEach
    void seed() {
        funding = createAccount("funding", "USD", true);
        alice = createAccount("alice", "USD", false);
        bob = createAccount("bob", "USD", false);
    }

    private long createAccount(String name, String currency, boolean overdraft) {
        ResponseEntity<JsonNode> r = http.postForEntity("/accounts",
                Map.of("name", name, "currency", currency, "allowOverdraft", overdraft), JsonNode.class);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return r.getBody().get("id").asLong();
    }

    private long httpBalance(long id) {
        return http.getForObject("/accounts/" + id, JsonNode.class).get("balanceMinor").asLong();
    }

    private void assertProblem(ResponseEntity<JsonNode> r, HttpStatus status, String type) {
        assertThat(r.getStatusCode()).isEqualTo(status);
        assertThat(r.getHeaders().getContentType().isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)).isTrue();
        if (type != null) {
            assertThat(r.getBody().get("type").asText()).isEqualTo("urn:ledger:problem:" + type);
        }
        assertThat(r.getBody().get("status").asInt()).isEqualTo(status.value());
    }

    @Test
    void postedTransferMovesMoneyAndWritesOneBalancedPairOfEntries() {
        ResponseEntity<JsonNode> r = postTransfer("k-1", body(funding, alice, 1_000));

        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(r.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");
        assertThat(r.getBody().get("status").asText()).isEqualTo("POSTED");
        assertThat(httpBalance(alice)).isEqualTo(1_000);
        assertThat(httpBalance(funding)).isEqualTo(-1_000);

        JsonNode entries = http.getForObject("/accounts/" + alice + "/entries", JsonNode.class).get("items");
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).get("direction").asText()).isEqualTo("C");
        assertThat(entries.get(0).get("amountMinor").asLong()).isEqualTo(1_000);
        assertThat(jdbc.queryForObject("SELECT sum(amount_minor) FROM entries", Long.class)).isZero();
        assertThat(count("outbox")).isEqualTo(1);
    }

    @Test
    void sameKeyAndBodyReplaysTheOriginalResponseWithoutMovingMoneyTwice() {
        ResponseEntity<JsonNode> first = postTransfer("k-2", body(funding, alice, 700));
        ResponseEntity<JsonNode> second = postTransfer("k-2", body(funding, alice, 700));

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(second.getBody()).isEqualTo(first.getBody());
        assertThat(count("transfers")).isEqualTo(1);
        assertThat(httpBalance(alice)).isEqualTo(700);
    }

    @Test
    void sameKeyWithDifferentBodyIsConflict() {
        postTransfer("k-3", body(funding, alice, 100));
        ResponseEntity<JsonNode> r = postTransfer("k-3", body(funding, alice, 101));

        assertProblem(r, HttpStatus.CONFLICT, "idempotency-key-reused");
        assertThat(httpBalance(alice)).isEqualTo(100);
    }

    @Test
    void overdraftIsRejectedAndLeavesNoTrace() {
        postTransfer("seed", body(funding, alice, 300));
        ResponseEntity<JsonNode> r = postTransfer("k-4", body(alice, bob, 301));

        assertProblem(r, HttpStatus.UNPROCESSABLE_ENTITY, "insufficient-funds");
        assertThat(httpBalance(alice)).isEqualTo(300);
        assertThat(httpBalance(bob)).isZero();
        assertThat(count("transfers")).isEqualTo(1);
        assertThat(count("entries")).isEqualTo(2);

        // The rejected attempt did not burn the key: once funded, the same request goes through.
        postTransfer("seed-2", body(funding, alice, 1));
        assertThat(postTransfer("k-4", body(alice, bob, 301)).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(httpBalance(bob)).isEqualTo(301);
    }

    @Test
    void currencyMismatchIsRejected() {
        long euro = createAccount("euro", "EUR", false);
        assertProblem(postTransfer("k-5", body(funding, euro, 10)), HttpStatus.UNPROCESSABLE_ENTITY, "currency-mismatch");

        Map<String, Object> gbp = body(funding, alice, 10);
        gbp.put("currency", "GBP");
        assertProblem(postTransfer("k-6", gbp), HttpStatus.UNPROCESSABLE_ENTITY, "currency-mismatch");
        assertThat(count("transfers")).isZero();
    }

    @Test
    void malformedRequestsAreBadRequestProblems() {
        assertProblem(postTransfer(null, body(funding, alice, 10)), HttpStatus.BAD_REQUEST, null);
        assertProblem(postTransfer("k-7", body(alice, alice, 10)), HttpStatus.BAD_REQUEST, "bad-request");
        assertProblem(postTransfer("k-8", body(funding, alice, 0)), HttpStatus.BAD_REQUEST, null);
        assertProblem(postTransfer("k-9", body(funding, alice, -5)), HttpStatus.BAD_REQUEST, null);
        Map<String, Object> badCurrency = body(funding, alice, 10);
        badCurrency.put("currency", "usd");
        assertProblem(postTransfer("k-10", badCurrency), HttpStatus.BAD_REQUEST, null);
        assertThat(count("transfers")).isZero();
    }

    @Test
    void unknownAccountAndTransferAreNotFound() {
        assertProblem(postTransfer("k-11", body(funding, 999, 10)), HttpStatus.NOT_FOUND, "not-found");
        assertProblem(http.getForEntity("/accounts/999", JsonNode.class), HttpStatus.NOT_FOUND, "not-found");
        assertProblem(http.getForEntity("/transfers/00000000-0000-0000-0000-000000000000", JsonNode.class),
                HttpStatus.NOT_FOUND, "not-found");
    }

    @Test
    void pendingTransferReservesFundsAndPostMovesThemOnce() {
        postTransfer("seed", body(funding, alice, 1_000));
        ResponseEntity<JsonNode> pending = postTransfer("k-12", withPending(body(alice, bob, 400)));
        String id = pending.getBody().get("id").asText();

        assertThat(pending.getBody().get("status").asText()).isEqualTo("PENDING");
        assertThat(httpBalance(alice)).isEqualTo(600); // available balance drops at reservation
        assertThat(httpBalance(bob)).isZero();
        assertThat(count("entries")).isEqualTo(2); // only the seed transfer is journaled so far

        ResponseEntity<JsonNode> posted = http.postForEntity("/transfers/" + id + "/post", null, JsonNode.class);
        assertThat(posted.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(posted.getBody().get("status").asText()).isEqualTo("POSTED");
        assertThat(httpBalance(alice)).isEqualTo(600);
        assertThat(httpBalance(bob)).isEqualTo(400);
        assertThat(count("entries")).isEqualTo(4);

        // Posting again is a no-op, voiding a posted transfer is refused.
        assertThat(http.postForEntity("/transfers/" + id + "/post", null, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertProblem(http.postForEntity("/transfers/" + id + "/void", null, JsonNode.class), HttpStatus.CONFLICT, "invalid-transfer-state");
        assertThat(httpBalance(bob)).isEqualTo(400);

        // A replay of the original create still returns the original (PENDING) response.
        ResponseEntity<JsonNode> replay = postTransfer("k-12", withPending(body(alice, bob, 400)));
        assertThat(replay.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(replay.getBody()).isEqualTo(pending.getBody());
        assertThat(http.getForObject("/transfers/" + id, JsonNode.class).get("status").asText()).isEqualTo("POSTED");
    }

    @Test
    void voidReleasesTheHoldAndWritesNoEntries() {
        postTransfer("seed", body(funding, alice, 500));
        String id = postTransfer("k-13", withPending(body(alice, bob, 500))).getBody().get("id").asText();

        // The hold counts against the available balance, so a second spend is refused.
        assertProblem(postTransfer("k-14", body(alice, bob, 1)), HttpStatus.UNPROCESSABLE_ENTITY, "insufficient-funds");

        ResponseEntity<JsonNode> voided = http.postForEntity("/transfers/" + id + "/void", null, JsonNode.class);
        assertThat(voided.getBody().get("status").asText()).isEqualTo("VOIDED");
        assertThat(httpBalance(alice)).isEqualTo(500);
        assertThat(httpBalance(bob)).isZero();
        assertThat(count("entries")).isEqualTo(2);
        assertProblem(http.postForEntity("/transfers/" + id + "/post", null, JsonNode.class), HttpStatus.CONFLICT, "invalid-transfer-state");
        assertThat(reconciler.run().violations()).isEmpty();
    }

    @Test
    void entriesPaginateNewestFirstWithAKeysetCursor() {
        for (int i = 1; i <= 5; i++) {
            postTransfer("p-" + i, body(funding, alice, i));
        }
        List<Long> amounts = new ArrayList<>();
        Set<Long> ids = new HashSet<>();
        String url = "/accounts/" + alice + "/entries?limit=2";
        int pages = 0;
        while (url != null) {
            JsonNode page = http.getForObject(url, JsonNode.class);
            page.get("items").forEach(e -> {
                amounts.add(e.get("amountMinor").asLong());
                ids.add(e.get("id").asLong());
            });
            pages++;
            JsonNode next = page.get("nextCursor");
            url = next == null || next.isNull() ? null : "/accounts/" + alice + "/entries?limit=2&before=" + next.asLong();
        }
        assertThat(pages).isEqualTo(3);
        assertThat(amounts).containsExactly(5L, 4L, 3L, 2L, 1L);
        assertThat(ids).hasSize(5);
    }

    @Test
    void reconciliationEndpointAndMetricsAreExposed() {
        postTransfer("m-1", body(funding, alice, 42));
        relay.drainAll();

        JsonNode report = http.getForObject("/reconciliation", JsonNode.class);
        assertThat(report.get("ok").asBoolean()).isTrue();
        assertThat(report.get("readModelChecked").asBoolean()).isTrue();

        String prometheus = http.getForObject("/actuator/prometheus", String.class);
        assertThat(prometheus).contains("ledger_transfer_latency_seconds_bucket", "ledger_outbox_lag_seconds",
                "ledger_reconciler_violations", "ledger_outbox_unpublished");
    }

    private static Map<String, Object> withPending(Map<String, Object> body) {
        body.put("pending", true);
        return body;
    }
}
