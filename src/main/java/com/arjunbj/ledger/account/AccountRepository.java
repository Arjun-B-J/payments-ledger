package com.arjunbj.ledger.account;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class AccountRepository {

    private static final String COLUMNS = "id, name, currency, allow_overdraft, shards, created_at";

    static final RowMapper<Account> MAPPER = (rs, n) -> new Account(
            rs.getLong("id"),
            rs.getString("name"),
            rs.getString("currency"),
            rs.getBoolean("allow_overdraft"),
            rs.getInt("shards"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant());

    private final JdbcTemplate jdbc;

    public AccountRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts the account and one zero balance row per shard. Caller provides the transaction. */
    public Account insert(String name, String currency, boolean allowOverdraft, int shards) {
        Account account = jdbc.queryForObject(
                "INSERT INTO accounts (name, currency, allow_overdraft, shards) VALUES (?, ?, ?, ?) RETURNING " + COLUMNS,
                MAPPER, name, currency, allowOverdraft, shards);
        List<Object[]> rows = new ArrayList<>(shards);
        for (int shard = 0; shard < shards; shard++) {
            rows.add(new Object[] {account.id(), shard, allowOverdraft});
        }
        jdbc.batchUpdate("INSERT INTO balances (account_id, shard, balance_minor, allow_overdraft) VALUES (?, ?, 0, ?)", rows);
        return account;
    }

    public Optional<Account> find(long id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM accounts WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    /** Available balance: the sum of the account's shard rows. */
    public long balance(long id) {
        Long total = jdbc.queryForObject("SELECT coalesce(sum(balance_minor), 0) FROM balances WHERE account_id = ?", Long.class, id);
        return total == null ? 0 : total;
    }
}
