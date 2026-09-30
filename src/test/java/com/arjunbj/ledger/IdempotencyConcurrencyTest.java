package com.arjunbj.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class IdempotencyConcurrencyTest extends IntegrationTest {

    private static final int CALLERS = 32;

    @Test
    void concurrentRequestsWithTheSameKeyCreateExactlyOneTransfer() throws Exception {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();

        List<ResponseEntity<JsonNode>> responses = race(i -> postTransfer("same-key", body(funding, alice, 500)));

        assertThat(responses).allSatisfy(r -> assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED));
        assertThat(responses.stream().map(r -> r.getBody().get("id").asText()).distinct()).hasSize(1);
        assertThat(responses.stream().filter(r -> "false".equals(r.getHeaders().getFirst("Idempotent-Replayed")))).hasSize(1);
        assertThat(count("transfers")).isEqualTo(1);
        assertThat(count("entries")).isEqualTo(2);
        assertThat(balance(alice)).isEqualTo(500);
    }

    @Test
    void concurrentRequestsWithTheSameKeyButDifferentBodiesLetExactlyOneWin() throws Exception {
        long funding = account("funding", true, 1).id();
        long alice = account("alice", false, 1).id();

        List<ResponseEntity<JsonNode>> responses = race(i -> postTransfer("contested", body(funding, alice, 100 + i)));

        long created = responses.stream().filter(r -> r.getStatusCode() == HttpStatus.CREATED).count();
        long conflicts = responses.stream().filter(r -> r.getStatusCode() == HttpStatus.CONFLICT).count();
        assertThat(created).isEqualTo(1);
        assertThat(conflicts).isEqualTo(CALLERS - 1);
        assertThat(count("transfers")).isEqualTo(1);
        long winner = responses.stream().filter(r -> r.getStatusCode() == HttpStatus.CREATED)
                .findFirst().orElseThrow().getBody().get("amountMinor").asLong();
        assertThat(balance(alice)).isEqualTo(winner);
    }

    private interface Call {
        ResponseEntity<JsonNode> run(int caller);
    }

    private List<ResponseEntity<JsonNode>> race(Call call) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(CALLERS);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<ResponseEntity<JsonNode>>> futures = new ArrayList<>();
            for (int i = 0; i < CALLERS; i++) {
                int caller = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    return call.run(caller);
                }));
            }
            start.countDown();
            List<ResponseEntity<JsonNode>> responses = new ArrayList<>();
            for (Future<ResponseEntity<JsonNode>> f : futures) {
                responses.add(f.get());
            }
            return responses;
        } finally {
            pool.shutdownNow();
        }
    }
}
