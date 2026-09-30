package com.arjunbj.ledger.outbox;

import java.util.List;

/**
 * Where the relay delivers events, one batch at a time. Delivery is at-least-once: a batch can
 * arrive again after a relay crash, so every sink's consumer must be idempotent on eventId. The
 * default sink is in-process (StatementProjector); a broker-backed sink would implement this.
 */
public interface OutboxSink {

    void deliver(List<TransferEvent> events);
}
