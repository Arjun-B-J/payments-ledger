package com.arjunbj.ledger.balance;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.common.InsufficientFundsException;
import org.springframework.stereotype.Component;

/**
 * One conditional UPDATE ... RETURNING per leg. The check and the write happen in the same
 * statement on the locked row, so there is no read-then-write gap and one round trip less than
 * ROW_LOCK while the lock is held. Still one row per account, so a hot account serializes.
 */
@Component
class AtomicStrategy implements BalanceStrategy {

    private final BalanceSql sql;

    AtomicStrategy(BalanceSql sql) {
        this.sql = sql;
    }

    @Override
    public StrategyName name() {
        return StrategyName.ATOMIC;
    }

    @Override
    public void debit(Account account, long amountMinor) {
        if (account.shards() > 1) {
            sql.drain(account, amountMinor); // funds may sit on any shard
            return;
        }
        if (sql.subtractIfCovered(account.id(), 0, amountMinor) == null) {
            throw new InsufficientFundsException(account.id(), amountMinor);
        }
    }

    @Override
    public void credit(Account account, long amountMinor) {
        sql.add(account.id(), 0, amountMinor);
    }
}
