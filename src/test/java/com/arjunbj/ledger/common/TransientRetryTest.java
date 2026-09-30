package com.arjunbj.ledger.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.dao.PessimisticLockingFailureException;

class TransientRetryTest {

    private final List<String> reasons = new ArrayList<>();
    private final TransientRetry retry = new TransientRetry(4, 1, 2, (label, reason) -> reasons.add(reason));

    private static RuntimeException sqlState(String state) {
        return new PessimisticLockingFailureException("db", new SQLException("boom", state));
    }

    @Test
    void retriesDeadlockAndSerializationFailuresThenSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        String result = retry.run("ATOMIC", () -> {
            int n = calls.incrementAndGet();
            if (n == 1) {
                throw sqlState("40P01");
            }
            if (n == 2) {
                throw sqlState("40001");
            }
            return "ok";
        });
        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(3);
        assertThat(reasons).containsExactly("deadlock", "serialization");
        assertThat(retry.retries()).isEqualTo(2);
    }

    @Test
    void givesUpAfterMaxAttempts() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> retry.run("ATOMIC", () -> {
            calls.incrementAndGet();
            throw sqlState("40P01");
        })).isInstanceOf(PessimisticLockingFailureException.class);
        assertThat(calls).hasValue(4);
    }

    @Test
    void doesNotRetryOtherErrors() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> retry.run("ATOMIC", () -> {
            calls.incrementAndGet();
            throw sqlState("23505");
        })).isInstanceOf(PessimisticLockingFailureException.class);
        assertThat(calls).hasValue(1);
        assertThat(reasons).isEmpty();
    }
}
