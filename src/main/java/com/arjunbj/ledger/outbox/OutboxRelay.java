package com.arjunbj.ledger.outbox;

import com.arjunbj.ledger.common.FailPoints;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves committed outbox rows to the sink. The batch stays locked while it is delivered and is
 * marked published in the same transaction; if the process dies before that commit, the rows are
 * still unpublished and the next poll delivers them again (at-least-once, never lost).
 */
@Component
public class OutboxRelay {

    private final OutboxRepository outbox;
    private final OutboxSink sink;
    private final FailPoints failPoints;
    private final TransactionTemplate tx;
    private final int batchSize;
    private final Counter relayed;

    public OutboxRelay(OutboxRepository outbox, OutboxSink sink, FailPoints failPoints, PlatformTransactionManager txManager,
                       MeterRegistry meters, @Value("${ledger.outbox.relay.batch-size:500}") int batchSize) {
        this.outbox = outbox;
        this.sink = sink;
        this.failPoints = failPoints;
        this.tx = new TransactionTemplate(txManager);
        this.batchSize = batchSize;
        this.relayed = meters.counter("ledger.outbox.relayed");
        Gauge.builder("ledger.outbox.lag.seconds", outbox, OutboxRepository::oldestUnpublishedAgeSeconds)
                .description("Age of the oldest event not yet relayed").register(meters);
        Gauge.builder("ledger.outbox.unpublished", outbox, OutboxRepository::unpublishedCount)
                .description("Events waiting for the relay").register(meters);
    }

    /** Relays one batch and returns how many events it delivered. */
    public int pollOnce() {
        Integer delivered = tx.execute(status -> {
            List<OutboxRepository.Pending> batch = outbox.lockBatch(batchSize);
            if (batch.isEmpty()) {
                return 0;
            }
            for (OutboxRepository.Pending pending : batch) {
                sink.deliver(pending.event());
            }
            failPoints.hit(FailPoints.Point.RELAY_AFTER_DELIVER);
            outbox.markPublished(batch.stream().map(OutboxRepository.Pending::id).toList());
            return batch.size();
        });
        int n = delivered == null ? 0 : delivered;
        relayed.increment(n);
        return n;
    }

    /** Relays until the outbox is empty or maxBatches batches were sent. */
    public int drain(int maxBatches) {
        int total = 0;
        for (int i = 0; i < maxBatches; i++) {
            int n = pollOnce();
            total += n;
            if (n < batchSize) {
                break;
            }
        }
        return total;
    }

    public int drainAll() {
        return drain(Integer.MAX_VALUE);
    }
}
