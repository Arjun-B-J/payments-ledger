package com.arjunbj.ledger.account;

import java.time.Instant;

/** Accounts are immutable once created, which is what makes caching them safe. */
public record Account(long id, String name, String currency, boolean allowOverdraft, int shards, Instant createdAt) {
}
