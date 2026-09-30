package com.arjunbj.ledger.common;

import org.springframework.http.HttpStatus;

public class InsufficientFundsException extends LedgerException {

    public InsufficientFundsException(long accountId, long amountMinor) {
        super(HttpStatus.UNPROCESSABLE_ENTITY, "insufficient-funds", "Insufficient funds",
                "account " + accountId + " cannot cover " + amountMinor + " minor units");
    }
}
