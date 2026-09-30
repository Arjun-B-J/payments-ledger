package com.arjunbj.ledger.recon;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Re-derives every invariant from the journal and compares it with the derived state. All checks
 * run in one REPEATABLE READ snapshot: each transfer commits its entries and balance changes
 * atomically, so any snapshot sees all of a transfer or none of it, and the reconciler can run
 * against a live ledger without false alarms.
 */
@Service
public class Reconciler {

    public record Violation(String check, String subject, String detail) {
    }

    public record Report(Instant ranAt, boolean ok, long transfers, long entries, long accounts,
                         boolean readModelChecked, String readModelNote, List<Violation> violations) {
    }

    private static final int LIMIT = 100;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate snapshot;
    private final AtomicInteger lastViolations = new AtomicInteger();
    private final Counter runs;

    public Reconciler(JdbcTemplate jdbc, PlatformTransactionManager txManager, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.snapshot = new TransactionTemplate(txManager);
        this.snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        this.snapshot.setReadOnly(true);
        this.runs = meters.counter("ledger.reconciler.runs");
        Gauge.builder("ledger.reconciler.violations", lastViolations, AtomicInteger::get)
                .description("Violations found by the last reconciler run").register(meters);
    }

    public Report run() {
        Report report = snapshot.execute(status -> check());
        runs.increment();
        lastViolations.set(report.violations().size());
        return report;
    }

    private Report check() {
        List<Violation> violations = new ArrayList<>();

        // (i) A posted transfer has exactly one debit and one credit that cancel out and match the
        //     transfer's accounts and amount. Pending and voided transfers have no entries.
        jdbc.query("""
                SELECT t.id, t.status, count(e.id) AS n, coalesce(sum(e.amount_minor), 0) AS total
                FROM transfers t LEFT JOIN entries e ON e.transfer_id = t.id
                GROUP BY t.id, t.status
                HAVING (t.status = 'POSTED' AND (count(e.id) <> 2 OR coalesce(sum(e.amount_minor), 0) <> 0
                        OR coalesce(bool_or((e.direction = 'D' AND (e.account_id <> t.debit_account OR e.amount_minor <> -t.amount_minor))
                                         OR (e.direction = 'C' AND (e.account_id <> t.credit_account OR e.amount_minor <> t.amount_minor))), false)))
                    OR (t.status <> 'POSTED' AND count(e.id) <> 0)
                LIMIT ?""",
                rs -> {
                    violations.add(new Violation("transfer-entries-balanced", "transfer " + rs.getString("id"),
                            rs.getString("status") + " with " + rs.getLong("n") + " entries summing to " + rs.getLong("total")));
                }, LIMIT);

        // (ii) The whole journal sums to zero.
        long journalTotal = jdbc.queryForObject("SELECT coalesce(sum(amount_minor), 0) FROM entries", Long.class);
        if (journalTotal != 0) {
            violations.add(new Violation("journal-sums-to-zero", "entries", "all entries sum to " + journalTotal));
        }

        // (iii) Each account's balance (sum of shards) equals its posted entries minus pending holds.
        jdbc.query("""
                WITH b AS (SELECT account_id, sum(balance_minor) AS balance FROM balances GROUP BY account_id),
                     e AS (SELECT account_id, sum(amount_minor) AS posted FROM entries GROUP BY account_id),
                     h AS (SELECT debit_account AS account_id, sum(amount_minor) AS held
                           FROM transfers WHERE status = 'PENDING' GROUP BY debit_account)
                SELECT a.id, coalesce(b.balance, 0) AS balance, coalesce(e.posted, 0) AS posted, coalesce(h.held, 0) AS held
                FROM accounts a
                LEFT JOIN b ON b.account_id = a.id
                LEFT JOIN e ON e.account_id = a.id
                LEFT JOIN h ON h.account_id = a.id
                WHERE coalesce(b.balance, 0) <> coalesce(e.posted, 0) - coalesce(h.held, 0)
                LIMIT ?""",
                rs -> {
                    violations.add(new Violation("balance-matches-entries", "account " + rs.getLong("id"),
                            "balance " + rs.getLong("balance") + " but entries " + rs.getLong("posted")
                                    + " minus holds " + rs.getLong("held")));
                }, LIMIT);

        // (iv) No negative balance, in total or on any shard, where overdraft is not allowed.
        jdbc.query("""
                SELECT a.id, sum(b.balance_minor) AS balance, min(b.balance_minor) AS lowest_shard
                FROM accounts a JOIN balances b ON b.account_id = a.id
                WHERE NOT a.allow_overdraft
                GROUP BY a.id
                HAVING sum(b.balance_minor) < 0 OR min(b.balance_minor) < 0
                LIMIT ?""",
                rs -> {
                    violations.add(new Violation("no-overdraft", "account " + rs.getLong("id"),
                            "balance " + rs.getLong("balance") + ", lowest shard " + rs.getLong("lowest_shard")));
                }, LIMIT);

        // (v) The statement read model agrees with the journal, checked only once the outbox is
        //     drained: before that, lines are legitimately missing.
        long undelivered = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class);
        boolean readModelChecked = undelivered == 0;
        String readModelNote;
        if (readModelChecked) {
            readModelNote = "outbox drained; statement lines compared with entries";
            jdbc.query("""
                    WITH e AS (SELECT account_id, sum(amount_minor) AS total, count(*) AS n FROM entries GROUP BY account_id),
                         r AS (SELECT account_id, sum(amount_minor) AS total, count(*) AS n FROM statement_lines GROUP BY account_id)
                    SELECT coalesce(e.account_id, r.account_id) AS account_id, e.total AS e_total, e.n AS e_n,
                           r.total AS r_total, r.n AS r_n
                    FROM e FULL JOIN r ON r.account_id = e.account_id
                    WHERE e.total IS DISTINCT FROM r.total OR e.n IS DISTINCT FROM r.n
                    LIMIT ?""",
                    rs -> {
                        violations.add(new Violation("read-model-matches", "account " + rs.getLong("account_id"),
                                "entries " + rs.getObject("e_total") + " (" + rs.getObject("e_n") + " rows) vs statement "
                                        + rs.getObject("r_total") + " (" + rs.getObject("r_n") + " rows)"));
                    }, LIMIT);
        } else {
            readModelNote = "skipped: " + undelivered + " outbox events not yet relayed";
        }

        long transfers = jdbc.queryForObject("SELECT count(*) FROM transfers", Long.class);
        long entries = jdbc.queryForObject("SELECT count(*) FROM entries", Long.class);
        long accounts = jdbc.queryForObject("SELECT count(*) FROM accounts", Long.class);
        return new Report(Instant.now(), violations.isEmpty(), transfers, entries, accounts, readModelChecked,
                readModelNote, List.copyOf(violations));
    }
}
