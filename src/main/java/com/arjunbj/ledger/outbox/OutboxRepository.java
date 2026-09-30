package com.arjunbj.ledger.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Array;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    public record Pending(long id, TransferEvent event) {
    }

    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public OutboxRepository(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /** Must run inside the transaction that changes the transfer: that is the whole point of an outbox. */
    public void append(TransferEvent event) {
        jdbc.update("INSERT INTO outbox (event_id, event_type, transfer_id, payload) VALUES (?, ?, ?, ?::jsonb)",
                event.eventId(), event.type(), event.transferId(), write(event));
    }

    /**
     * Locks the oldest unpublished rows. SKIP LOCKED lets several relay instances take disjoint
     * batches instead of queueing behind each other.
     */
    public List<Pending> lockBatch(int limit) {
        return jdbc.query("SELECT id, payload::text AS payload FROM outbox WHERE published_at IS NULL "
                        + "ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED",
                (rs, n) -> new Pending(rs.getLong("id"), read(rs.getString("payload"))), limit);
    }

    public void markPublished(List<Long> ids) {
        jdbc.update(con -> {
            Array array = con.createArrayOf("bigint", ids.toArray());
            var ps = con.prepareStatement("UPDATE outbox SET published_at = now() WHERE id = ANY (?)");
            ps.setArray(1, array);
            return ps;
        });
    }

    public long unpublishedCount() {
        Long n = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class);
        return n == null ? 0 : n;
    }

    /** Age of the oldest event not yet relayed; 0 when the outbox is drained. */
    public double oldestUnpublishedAgeSeconds() {
        List<Double> age = jdbc.query("SELECT extract(epoch FROM now() - created_at)::float8 FROM outbox "
                + "WHERE published_at IS NULL ORDER BY id LIMIT 1", (rs, n) -> rs.getDouble(1));
        return age.isEmpty() ? 0.0 : age.get(0);
    }

    private String write(TransferEvent event) {
        try {
            return json.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize " + event, e);
        }
    }

    private TransferEvent read(String payload) {
        try {
            return json.readValue(payload, TransferEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot parse outbox payload " + payload, e);
        }
    }
}
