package com.arjunbj.ledger.balance;

import com.arjunbj.ledger.account.Account;

/**
 * How a transfer changes balance rows. Implementations run inside the caller's transaction and
 * must leave the overdraft decision to the database (conditional UPDATE or the
 * balances_no_overdraft CHECK), never to Java alone.
 */
public interface BalanceStrategy {

    StrategyName name();

    /** Takes money out of the account. Throws InsufficientFundsException when the database refuses. */
    void debit(Account account, long amountMinor);

    void credit(Account account, long amountMinor);
}
