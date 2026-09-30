package com.arjunbj.ledger.common;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Re-runs a whole transaction when Postgres aborts it with a serialization failure (40001) or a
 * deadlock (40P01). The retry wraps the transaction and never runs inside it: after either error
 * the transaction is already aborted. Attempts are bounded and backoff is capped with jitter so
 * retries cannot pile up behind a hot row.
 */
public class TransientRetry {

    private final int maxAttempts;
    private final long baseBackoffMs;
    private final long maxBackoffMs;
    private final BiConsumer<String, String> onRetry;
    private final LongAdder retries = new LongAdder();

    public TransientRetry(int maxAttempts, long baseBackoffMs, long maxBackoffMs, BiConsumer<String, String> onRetry) {
        this.maxAttempts = maxAttempts;
        this.baseBackoffMs = baseBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.onRetry = onRetry;
    }

    public <T> T run(String label, Supplier<T> action) {
        for (int attempt = 1; ; attempt++) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                String reason = transientReason(e);
                if (reason == null || attempt >= maxAttempts) {
                    throw e;
                }
                retries.increment();
                onRetry.accept(label, reason);
                sleep(backoffMs(attempt));
            }
        }
    }

    /** Total retries since start (or the last reset). The benchmark reads this per run. */
    public long retries() {
        return retries.sum();
    }

    public void resetCount() {
        retries.reset();
    }

    long backoffMs(int attempt) {
        long cap = Math.max(1, Math.min(maxBackoffMs, baseBackoffMs << Math.min(attempt - 1, 20)));
        return ThreadLocalRandom.current().nextLong(cap / 2, cap + 1);
    }

    public static String transientReason(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql) {
                if ("40001".equals(sql.getSQLState())) {
                    return "serialization";
                }
                if ("40P01".equals(sql.getSQLState())) {
                    return "deadlock";
                }
            }
        }
        return null;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during retry backoff", e);
        }
    }
}
