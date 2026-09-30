package com.arjunbj.ledger.outbox;

/**
 * Where the relay delivers events. Delivery is at-least-once: the same event can arrive again
 * after a relay crash, so every sink's consumer must be idempotent on eventId. The default sink
 * is in-process (StatementProjector); a broker-backed sink would implement this interface.
 */
public interface OutboxSink {

    void deliver(TransferEvent event);
}
