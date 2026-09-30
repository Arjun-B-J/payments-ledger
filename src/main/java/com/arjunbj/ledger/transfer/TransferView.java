package com.arjunbj.ledger.transfer;

import java.time.Instant;
import java.util.UUID;

public record TransferView(UUID id, long debitAccountId, long creditAccountId, long amountMinor, String currency,
                           Transfer.Status status, Instant createdAt, Instant updatedAt) {

    public static TransferView of(Transfer t) {
        return new TransferView(t.id(), t.debitAccountId(), t.creditAccountId(), t.amountMinor(), t.currency(),
                t.status(), t.createdAt(), t.updatedAt());
    }

    /**
     * The exact body the first POST /transfers returned. It is rebuilt from immutable columns plus
     * the pending flag, which the matching request hash proves is the same as the original, so a
     * replay after a later post or void still returns the original response.
     */
    public static TransferView asCreated(Transfer t, boolean pending) {
        return new TransferView(t.id(), t.debitAccountId(), t.creditAccountId(), t.amountMinor(), t.currency(),
                pending ? Transfer.Status.PENDING : Transfer.Status.POSTED, t.createdAt(), t.createdAt());
    }
}
