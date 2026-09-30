package com.arjunbj.ledger.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Consumer that builds the account-statement read model. It commits in its own transaction
 * (REQUIRES_NEW), exactly as an external consumer would, so a relay crash after delivery really
 * does produce a second delivery. The processed_events insert makes that second delivery a no-op.
 */
@Component
public class StatementProjector implements OutboxSink {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate ownTransaction;
    private final Counter applied;
    private final Counter duplicates;

    public StatementProjector(JdbcTemplate jdbc, PlatformTransactionManager txManager, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.ownTransaction = new TransactionTemplate(txManager);
        this.ownTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.applied = meters.counter("ledger.projector.events", "result", "applied");
        this.duplicates = meters.counter("ledger.projector.events", "result", "duplicate");
    }

    @Override
    public void deliver(TransferEvent event) {
        apply(event);
    }

    /** Returns false when the event was already applied. */
    public boolean apply(TransferEvent event) {
        Boolean fresh = ownTransaction.execute(status -> {
            int inserted = jdbc.update("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING",
                    event.eventId());
            if (inserted == 0) {
                return false;
            }
            if (TransferEvent.POSTED.equals(event.type())) {
                OffsetDateTime at = OffsetDateTime.ofInstant(event.occurredAt(), ZoneOffset.UTC);
                jdbc.update("INSERT INTO statement_lines (account_id, transfer_id, amount_minor, currency, posted_at) "
                                + "VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)",
                        event.debitAccountId(), event.transferId(), -event.amountMinor(), event.currency(), at,
                        event.creditAccountId(), event.transferId(), event.amountMinor(), event.currency(), at);
            }
            return true;
        });
        boolean wasFresh = Boolean.TRUE.equals(fresh);
        (wasFresh ? applied : duplicates).increment();
        return wasFresh;
    }
}
