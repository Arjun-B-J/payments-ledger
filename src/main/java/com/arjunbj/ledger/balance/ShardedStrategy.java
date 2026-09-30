package com.arjunbj.ledger.balance;

import com.arjunbj.ledger.account.Account;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/**
 * Spreads a hot account over N balance rows. Credits (the common case for a merchant) land on a
 * random shard, so up to N credits proceed in parallel instead of queueing on one row lock.
 * Debits drain shards in ascending order and are refused if the total cannot cover them.
 * The account's balance is always the sum of its shards.
 */
@Component
class ShardedStrategy implements BalanceStrategy {

    private final BalanceSql sql;

    ShardedStrategy(BalanceSql sql) {
        this.sql = sql;
    }

    @Override
    public StrategyName name() {
        return StrategyName.SHARDED;
    }

    @Override
    public void debit(Account account, long amountMinor) {
        sql.drain(account, amountMinor);
    }

    @Override
    public void credit(Account account, long amountMinor) {
        int shard = account.shards() == 1 ? 0 : ThreadLocalRandom.current().nextInt(account.shards());
        sql.add(account.id(), shard, amountMinor);
    }
}
