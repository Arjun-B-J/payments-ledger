package com.arjunbj.ledger.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "ledger.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRelay relay;

    OutboxRelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.relay.interval-ms:100}")
    void tick() {
        try {
            relay.drain(20);
        } catch (RuntimeException e) {
            // The batch rolled back and stays unpublished; the next tick delivers it again.
            log.warn("outbox relay pass failed: {}", e.toString());
        }
    }
}
