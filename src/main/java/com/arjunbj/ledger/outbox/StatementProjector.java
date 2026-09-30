package com.arjunbj.ledger.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Array;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Consumer that builds the account-statement read model. It commits in its own transaction
 * (REQUIRES_NEW), exactly as an external consumer would, so a relay crash after delivery really
 * does produce a second delivery. Claiming each event id in processed_events first makes that
 * second delivery a no-op. A whole relay batch is applied in one transaction, so the cost per
 * event is a few rows rather than a commit.
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
    public void deliver(List<TransferEvent> events) {
        apply(events);
    }

    /** Applies the events not seen before and returns how many that was. */
    public int apply(List<TransferEvent> events) {
        if (events.isEmpty()) {
            return 0;
        }
        Integer fresh = ownTransaction.execute(status -> {
            Set<UUID> claimed = new HashSet<>(jdbc.query(con -> {
                Array ids = con.createArrayOf("uuid", events.stream().map(TransferEvent::eventId).toArray());
                var ps = con.prepareStatement("INSERT INTO processed_events (event_id) SELECT unnest(?::uuid[]) "
                        + "ON CONFLICT DO NOTHING RETURNING event_id");
                ps.setArray(1, ids);
                return ps;
            }, (rs, n) -> rs.getObject(1, UUID.class)));
            List<Object[]> lines = new ArrayList<>();
            for (TransferEvent e : events) {
                if (claimed.contains(e.eventId()) && TransferEvent.POSTED.equals(e.type())) {
                    OffsetDateTime at = OffsetDateTime.ofInstant(e.occurredAt(), ZoneOffset.UTC);
                    lines.add(new Object[] {e.debitAccountId(), e.transferId(), -e.amountMinor(), e.currency(), at});
                    lines.add(new Object[] {e.creditAccountId(), e.transferId(), e.amountMinor(), e.currency(), at});
                }
            }
            if (!lines.isEmpty()) {
                jdbc.batchUpdate("INSERT INTO statement_lines (account_id, transfer_id, amount_minor, currency, posted_at) "
                        + "VALUES (?, ?, ?, ?, ?)", lines);
            }
            return claimed.size();
        });
        int n = fresh == null ? 0 : fresh;
        applied.increment(n);
        duplicates.increment(events.size() - n);
        return n;
    }
}
