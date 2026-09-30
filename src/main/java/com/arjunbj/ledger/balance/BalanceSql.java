package com.arjunbj.ledger.balance;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.common.InsufficientFundsException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** The SQL the strategies share. Every statement touches exactly one (account, shard) row. */
@Component
class BalanceSql {

    private final JdbcTemplate jdbc;

    BalanceSql(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    void add(long accountId, int shard, long amountMinor) {
        int updated = jdbc.update(
                "UPDATE balances SET balance_minor = balance_minor + ?, version = version + 1 WHERE account_id = ? AND shard = ?",
                amountMinor, accountId, shard);
        if (updated != 1) {
            throw new IllegalStateException("no balance row for account " + accountId + " shard " + shard);
        }
    }

    /**
     * Debit only if covered, in one statement. Under READ COMMITTED Postgres takes the row lock and
     * re-checks the WHERE clause against the newest committed version, so two concurrent debits can
     * never both pass on a stale balance. Returns the new balance, or null when refused.
     */
    Long subtractIfCovered(long accountId, int shard, long amountMinor) {
        List<Long> rows = jdbc.query(
                "UPDATE balances SET balance_minor = balance_minor - ?, version = version + 1 "
                        + "WHERE account_id = ? AND shard = ? AND (allow_overdraft OR balance_minor >= ?) "
                        + "RETURNING balance_minor",
                (rs, n) -> rs.getLong(1), amountMinor, accountId, shard, amountMinor);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** SELECT ... FOR UPDATE: the row stays locked until the transaction ends. */
    long lock(long accountId, int shard) {
        Long balance = jdbc.queryForObject(
                "SELECT balance_minor FROM balances WHERE account_id = ? AND shard = ? FOR UPDATE",
                Long.class, accountId, shard);
        if (balance == null) {
            throw new IllegalStateException("no balance row for account " + accountId + " shard " + shard);
        }
        return balance;
    }

    /** Writes a value computed in Java. Only safe while holding the row lock from lock(). */
    void set(long accountId, int shard, long newBalance) {
        jdbc.update("UPDATE balances SET balance_minor = ?, version = version + 1 WHERE account_id = ? AND shard = ?",
                newBalance, accountId, shard);
    }

    /**
     * Debit spread over several shard rows. Shards are visited in ascending order, each locked
     * before it is read, and the walk stops as soon as the amount is covered. The fixed order means
     * two drains never wait on each other in a cycle; a credit holds one shard and never waits for
     * another shard of the same account. Overdraft is judged on the total of the shards visited.
     */
    void drain(Account account, long amountMinor) {
        if (account.allowOverdraft()) {
            // No check needed; spread the debit like a credit so the account never becomes a hot row.
            int shard = ThreadLocalRandom.current().nextInt(account.shards());
            subtractIfCovered(account.id(), shard, amountMinor);
            return;
        }
        if (account.shards() == 1) {
            if (subtractIfCovered(account.id(), 0, amountMinor) == null) {
                throw new InsufficientFundsException(account.id(), amountMinor);
            }
            return;
        }
        long remaining = amountMinor;
        for (int shard = 0; shard < account.shards() && remaining > 0; shard++) {
            long balance = lock(account.id(), shard);
            long take = Math.min(balance, remaining);
            if (take > 0) {
                set(account.id(), shard, balance - take);
                remaining -= take;
            }
        }
        if (remaining > 0) {
            // Throwing rolls back the partial drain along with the rest of the transaction.
            throw new InsufficientFundsException(account.id(), amountMinor);
        }
    }
}
