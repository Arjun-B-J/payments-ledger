package com.arjunbj.ledger.transfer;

import java.time.Instant;
import java.util.UUID;

public record Transfer(UUID id, String idempotencyKey, String requestHash, long debitAccountId, long creditAccountId,
                       long amountMinor, String currency, Status status, Instant createdAt, Instant updatedAt) {

    public enum Status { PENDING, POSTED, VOIDED }
}
