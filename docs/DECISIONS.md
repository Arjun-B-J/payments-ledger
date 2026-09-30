# Design decisions

Each decision lists what was chosen, why, and what it costs.

## 1. Spring JDBC (`JdbcTemplate`) instead of JPA

**Chosen:** explicit SQL through `JdbcTemplate`, one repository per table.

**Why:** the correctness of a ledger lives in a handful of statements: the conditional `UPDATE`, `SELECT ... FOR UPDATE`, `INSERT ... ON CONFLICT DO NOTHING`, `FOR UPDATE SKIP LOCKED`. With JPA those are hidden behind dirty checking, flush ordering and an entity cache, and a lost update is one missing annotation away. With plain SQL a reviewer can read exactly which rows are locked and when.

**Cost:** more boilerplate (row mappers, column lists) and no automatic schema-to-object mapping.

## 2. Money is `BIGINT` minor units, one currency per transfer

**Chosen:** `amount_minor BIGINT` everywhere, `long` in Java, amounts must be positive, and a transfer's currency must equal both accounts' currency (422 otherwise).

**Why:** integers add exactly; floating point does not. Rejecting mixed currencies keeps every transfer summing to zero in a single unit. FX would be two transfers through a conversion account, which is out of scope.

**Cost:** the API speaks minor units (`amountMinor: 1050` for 10.50), and currencies with three decimals need the client to know the exponent.

## 3. Double entry with an append-only journal; balances are derived

**Chosen:** a posted transfer writes exactly one debit entry (negative) and one credit entry (positive) of the same size. `entries` rejects `UPDATE` and `DELETE` through a trigger, and `UNIQUE (transfer_id, direction)` allows at most one debit and one credit per transfer. `balances` is a derived table that must always equal the journal.

**Why:** every movement has a source and a destination, so money cannot appear or vanish without breaking a sum that can be checked (see decision 13). Corrections are new transfers, never edits, so history is auditable.

**Cost:** two writes per transfer instead of one, and funding needs an account that is allowed to go negative (the "funding" account in the examples plays that role).

## 4. Overdraft is refused by the database, not by Java

**Chosen:** the debit is a conditional `UPDATE ... WHERE allow_overdraft OR balance_minor >= :amount`. Zero rows updated means insufficient funds. As a backstop, `balances` has `CHECK (allow_overdraft OR balance_minor >= 0)`; `allow_overdraft` is copied onto each balance row so the check needs no join.

**Why:** a check done in Java on a value read earlier can be stale by the time the write happens. Under READ COMMITTED, Postgres takes the row lock and then re-evaluates the `WHERE` clause against the newest committed version, so the check and the write are one atomic step. The `CHECK` catches any code path that forgets this, including the ROW_LOCK strategy, which decides in Java on purpose (decision 7).

**Cost:** `allow_overdraft` is duplicated; that is safe only because accounts are immutable after creation.

## 5. Lock ordering, and balance rows last

**Chosen:** inside the transfer transaction the order is: claim the idempotency key (insert the transfer), insert both entries, insert the outbox event, and only then update balances, lower account id first.

**Why:** two transfers A to B and B to A that each lock "their" debit account first can deadlock. Ordering every lock by account id makes a wait cycle impossible. Balance rows are the contended rows, so taking their locks last keeps them held for the shortest time before commit. The retry in decision 6 is a safety net; the Retries column in [BENCHMARKS.md](BENCHMARKS.md) shows how often it fired.

**Cost:** the code has two branches for the leg order. A transfer that is refused for funds has already written its journal rows, which the rollback discards. Locking the hottest account last instead of the lowest id first would shorten its lock hold further; that is not implemented or measured.

## 6. READ COMMITTED, with a bounded retry around the whole transaction

**Chosen:** default isolation. SQLSTATE `40001` (serialization failure) and `40P01` (deadlock) re-run the entire transaction up to 5 attempts with capped, jittered exponential backoff. Each retry increments `ledger.transfer.retries{strategy,reason}`.

**Why:** the conditional update and row locks give the guarantees the ledger needs without SERIALIZABLE, which would add aborts under contention. The retry wraps the transaction because after either error the transaction is already aborted.

**Cost:** a transfer that keeps losing gives up after 5 attempts and the client sees a 500, which is safe to retry with the same key.

## 7. Strategy ROW_LOCK

`SELECT balance_minor ... FOR UPDATE`, compute the new value in Java, `UPDATE ... SET balance_minor = :new`. This is the read-modify-write most ORM code produces. It is correct because the lock is held to commit. It costs one extra round trip per leg while the lock is held, which on a hot account directly lowers throughput.

## 8. Strategy ATOMIC

One `UPDATE ... SET balance_minor = balance_minor - :amt WHERE ... AND (allow_overdraft OR balance_minor >= :amt) RETURNING balance_minor` per leg. The check and write happen in one statement, so the lock is held for fewer round trips than ROW_LOCK. It is still one row per account, so every transfer that touches a hot account waits for the previous one to commit.

## 9. Strategy SHARDED

An account created with `shards = N` has N balance rows; its balance is their sum. Credits go to a random shard, so up to N credits to the same account can proceed in parallel. Debits drain shards in ascending order, locking each shard before reading it, stop as soon as the amount is covered, and are refused (rolling back any partial drain) if the total cannot cover them. Drains never wait on each other in a cycle because the order is fixed, and a credit holds a single shard.

**Cost:** reading a balance sums N rows; a debit on a sharded account can take up to 2N statements; and the "is there enough money" answer is only as fresh as the shards it locked. It pays off for accounts that mostly receive (a merchant, a fee account). ROW_LOCK and ATOMIC credit shard 0 and fall back to the ordered drain for debits on a multi-shard account, so switching strategy never misreads a balance.

## 10. BATCHED strategy: not built

Per-account micro-batching (queue transfers for a hot account, apply many in one transaction) was left out. Doing it correctly needs per-item failure isolation inside the batch (savepoints or a pre-check), idempotency per item, and a way to complete each caller's request after the shared commit. That was more than could be built and tested in the time available.

## 11. Idempotency design

- The key lives on the transfer row (`idempotency_key UNIQUE`, `request_hash`), and is claimed with `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` in the same transaction that moves the money. If the process dies after commit but before responding, the retry finds the committed row. If it dies before commit, nothing exists and the retry starts fresh.
- Concurrent requests with the same key: Postgres makes the second insert wait for the first transaction, then reports the conflict. Exactly one request creates the transfer; the others replay it (tested with 32 concurrent callers).
- `request_hash` is SHA-256 over the semantic fields (accounts, amount, currency, pending), so a retry with reordered JSON is still "the same request". Same key with a different body is 409.
- A replay returns the original status code and body (with `Idempotent-Replayed: true`). The body is rebuilt from immutable columns plus the pending flag, which the matching hash proves is unchanged, so a replay after a later post or void still shows the original response.
- A refused request (insufficient funds, validation) stores nothing, so it does not burn the key: the client can retry the same key once the account is funded. This differs from Stripe, which stores error responses.

**Cost:** keys are global, not scoped per client, and never expire. A multi-tenant version would add a client id to the unique key and a retention job.

## 12. Two-phase transfers reserve funds without journal entries

**Chosen:** `pending: true` debits the source account's balance row (a hold) and writes no entries. `post` writes both entries and credits the destination; `void` credits the hold back. Repeating the same post or void is a no-op; the opposite transition after completion is 409. The transfer row is locked with `SELECT ... FOR UPDATE` so a concurrent post and void cannot both win.

**Why:** the balance row means "available balance", so a hold immediately stops double spending, while the journal only records money that actually moved. The reconciler's balance check becomes: balance = sum of entries minus pending holds.

## 13. Transactional outbox instead of dual writes

**Chosen:** every post or void inserts an `outbox` row in the same transaction. A relay locks unpublished rows with `FOR UPDATE SKIP LOCKED`, hands them to an `OutboxSink`, and marks them published in the same relay transaction.

**Why:** writing to the database and then publishing to a broker (or the reverse) loses or invents events when the process dies between the two. With the outbox, the event exists if and only if the transfer committed. SKIP LOCKED lets several relay instances take disjoint batches.

**Cost:** delivery is at-least-once, with lag equal to the poll interval plus batch time (exposed as `ledger.outbox.lag.seconds`). A crash after delivery and before the published mark redelivers the batch, so consumers must be idempotent.

## 14. Idempotent consumer for the read model

The default sink is in-process: `StatementProjector` builds `statement_lines` in its own transaction (`REQUIRES_NEW`), the way an external consumer would, and first inserts the event id into `processed_events ... ON CONFLICT DO NOTHING`. A redelivered event finds its id and is skipped. The fault-injection test crashes the relay after delivery and shows the redelivered batch is counted as duplicates and not applied twice.

## 15. Reconciler reads one snapshot

All five checks run in one `REPEATABLE READ`, read-only transaction. Each transfer commits its entries and balance changes together, so any snapshot contains all of a transfer or none of it, and the reconciler can run while traffic flows. The read-model check is skipped (and says so) while the outbox has undelivered events, because missing lines are expected then. It runs as `GET /reconciliation`, on a schedule (`ledger.reconciler.interval-ms`) and as a CLI mode (`--ledger.cli=reconcile`, exit code 1 on violations). The last run's violation count is the `ledger.reconciler.violations` gauge.

## 16. No Kafka, Debezium or Redis in this build

The outbox is written so that a broker can be added behind `OutboxSink`, but nothing in this repository talks to Kafka, because it could not be run and tested here. The same goes for CDC with Debezium (an alternative to the polling relay) and for Redis. Claims in this repository are limited to what the tests and the benchmark exercise.

## 17. Embedded PostgreSQL for tests and local runs

zonky `embedded-postgres` downloads real PostgreSQL binaries (17.11 here) as Maven artifacts, so `./mvnw verify` needs only a JDK. Tests run against the same schema, locks and constraints as production, not an in-memory imitation. Setting `SPRING_DATASOURCE_URL` switches to a real server. The embedded defaults include `synchronous_commit=off` and `fsync=off`, which the benchmark report states next to its numbers.
