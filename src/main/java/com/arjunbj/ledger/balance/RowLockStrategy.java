package com.arjunbj.ledger.balance;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.common.InsufficientFundsException;
import org.springframework.stereotype.Component;

/**
 * Baseline: lock the row, read it, decide in Java, write the new value. Correct because the row
 * lock is held until commit, and the balances_no_overdraft CHECK backs the Java decision. Costs an
 * extra round trip while the lock is held, which is what hurts on a hot account.
 */
@Component
class RowLockStrategy implements BalanceStrategy {

    private final BalanceSql sql;

    RowLockStrategy(BalanceSql sql) {
        this.sql = sql;
    }

    @Override
    public StrategyName name() {
        return StrategyName.ROW_LOCK;
    }

    @Override
    public void debit(Account account, long amountMinor) {
        if (account.shards() > 1) {
            sql.drain(account, amountMinor); // funds may sit on any shard
            return;
        }
        long current = sql.lock(account.id(), 0);
        if (!account.allowOverdraft() && current < amountMinor) {
            throw new InsufficientFundsException(account.id(), amountMinor);
        }
        sql.set(account.id(), 0, Math.subtractExact(current, amountMinor));
    }

    @Override
    public void credit(Account account, long amountMinor) {
        long current = sql.lock(account.id(), 0);
        sql.set(account.id(), 0, Math.addExact(current, amountMinor));
    }
}
