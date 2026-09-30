package com.arjunbj.ledger.balance;

public enum StrategyName {
    /** SELECT ... FOR UPDATE, check in Java, then UPDATE: the read-modify-write most ORMs produce. */
    ROW_LOCK,
    /** One conditional UPDATE ... RETURNING: the overdraft check is the WHERE clause. */
    ATOMIC,
    /** N balance rows per account: credits pick a random row, debits drain rows in a fixed order. */
    SHARDED
}
