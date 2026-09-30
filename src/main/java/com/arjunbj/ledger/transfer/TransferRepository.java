package com.arjunbj.ledger.transfer;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class TransferRepository {

    private static final String COLUMNS = "id, idempotency_key, request_hash, debit_account, credit_account, "
            + "amount_minor, currency, status, created_at, updated_at";

    private static final RowMapper<Transfer> MAPPER = (rs, n) -> new Transfer(
            rs.getObject("id", UUID.class),
            rs.getString("idempotency_key"),
            rs.getString("request_hash"),
            rs.getLong("debit_account"),
            rs.getLong("credit_account"),
            rs.getLong("amount_minor"),
            rs.getString("currency"),
            Transfer.Status.valueOf(rs.getString("status")),
            rs.getObject("created_at", OffsetDateTime.class).toInstant(),
            rs.getObject("updated_at", OffsetDateTime.class).toInstant());

    private final JdbcTemplate jdbc;

    public TransferRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Claims the idempotency key. ON CONFLICT DO NOTHING waits for a concurrent insert of the same
     * key to commit or roll back, so exactly one request wins; the others get an empty result and
     * read the winner's row.
     */
    public Optional<Transfer> insertIfAbsent(UUID id, String key, String hash, TransferCommand c, Transfer.Status status) {
        List<Transfer> rows = jdbc.query(
                "INSERT INTO transfers (id, idempotency_key, request_hash, debit_account, credit_account, amount_minor, currency, status) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING RETURNING " + COLUMNS,
                MAPPER, id, key, hash, c.debitAccountId(), c.creditAccountId(), c.amountMinor(), c.currency(), status.name());
        return rows.stream().findFirst();
    }

    public Optional<Transfer> findByKey(String key) {
        return jdbc.query("SELECT " + COLUMNS + " FROM transfers WHERE idempotency_key = ?", MAPPER, key).stream().findFirst();
    }

    public Optional<Transfer> find(UUID id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM transfers WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    /** Locks the transfer row so a concurrent post and void of the same transfer serialize. */
    public Optional<Transfer> findForUpdate(UUID id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM transfers WHERE id = ? FOR UPDATE", MAPPER, id).stream().findFirst();
    }

    public Transfer updateStatus(UUID id, Transfer.Status status) {
        return jdbc.queryForObject(
                "UPDATE transfers SET status = ?, updated_at = now() WHERE id = ? RETURNING " + COLUMNS,
                MAPPER, status.name(), id);
    }
}
