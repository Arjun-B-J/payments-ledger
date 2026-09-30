package com.arjunbj.ledger.outbox;

import com.arjunbj.ledger.transfer.Transfer;
import java.time.Instant;
import java.util.UUID;

/** The outbox payload. eventId is what consumers deduplicate on. */
public record TransferEvent(UUID eventId, String type, UUID transferId, long debitAccountId, long creditAccountId,
                            long amountMinor, String currency, Instant occurredAt) {

    public static final String POSTED = "TransferPosted";
    public static final String VOIDED = "TransferVoided";

    public static TransferEvent of(String type, Transfer t) {
        return new TransferEvent(UUID.randomUUID(), type, t.id(), t.debitAccountId(), t.creditAccountId(),
                t.amountMinor(), t.currency(), t.updatedAt());
    }
}
