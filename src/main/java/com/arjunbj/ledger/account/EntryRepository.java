package com.arjunbj.ledger.account;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class EntryRepository {

    public record EntryView(long id, UUID transferId, long accountId, String direction, long amountMinor,
                            String currency, Instant createdAt) {
    }

    /** One page of entries, newest first. nextCursor is null on the last page. */
    public record EntryPage(List<EntryView> items, Long nextCursor) {
    }

    private static final String COLUMNS = "id, transfer_id, account_id, direction, amount_minor, currency, created_at";

    private static final RowMapper<EntryView> MAPPER = (rs, n) -> new EntryView(
            rs.getLong("id"),
            rs.getObject("transfer_id", UUID.class),
            rs.getLong("account_id"),
            rs.getString("direction"),
            rs.getLong("amount_minor"),
            rs.getString("currency"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant());

    private final JdbcTemplate jdbc;

    public EntryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The two legs of a posted transfer in one statement: a negative debit and an equal positive credit. */
    public void insertPair(UUID transferId, long debitAccount, long creditAccount, long amountMinor, String currency) {
        jdbc.update("INSERT INTO entries (transfer_id, account_id, direction, amount_minor, currency) "
                        + "VALUES (?, ?, 'D', ?, ?), (?, ?, 'C', ?, ?)",
                transferId, debitAccount, -amountMinor, currency,
                transferId, creditAccount, amountMinor, currency);
    }

    /**
     * Keyset pagination on the entry id: "id < cursor ORDER BY id DESC" walks the (account_id, id)
     * index, so page 1000 costs the same as page 1, and rows inserted meanwhile do not shift pages.
     */
    public EntryPage page(long accountId, Long before, int limit) {
        List<EntryView> rows = before == null
                ? jdbc.query("SELECT " + COLUMNS + " FROM entries WHERE account_id = ? ORDER BY id DESC LIMIT ?",
                        MAPPER, accountId, limit + 1)
                : jdbc.query("SELECT " + COLUMNS + " FROM entries WHERE account_id = ? AND id < ? ORDER BY id DESC LIMIT ?",
                        MAPPER, accountId, before, limit + 1);
        if (rows.size() > limit) {
            List<EntryView> page = rows.subList(0, limit);
            return new EntryPage(List.copyOf(page), page.get(limit - 1).id());
        }
        return new EntryPage(rows, null);
    }
}
