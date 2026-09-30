package com.arjunbj.ledger.config;

import com.arjunbj.ledger.common.TransientRetry;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class LedgerConfig {

    @Bean
    TransientRetry transientRetry(@Value("${ledger.retry.max-attempts:5}") int maxAttempts,
                                  @Value("${ledger.retry.base-backoff-ms:2}") long baseBackoffMs,
                                  @Value("${ledger.retry.max-backoff-ms:100}") long maxBackoffMs,
                                  MeterRegistry meters) {
        return new TransientRetry(maxAttempts, baseBackoffMs, maxBackoffMs,
                (strategy, reason) -> meters.counter("ledger.transfer.retries", "strategy", strategy, "reason", reason).increment());
    }
}
