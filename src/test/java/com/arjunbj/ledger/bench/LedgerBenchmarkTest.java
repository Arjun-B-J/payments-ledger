package com.arjunbj.ledger.bench;

import static org.assertj.core.api.Assertions.assertThat;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.account.AccountService;
import com.arjunbj.ledger.balance.StrategyName;
import com.arjunbj.ledger.balance.StrategyRegistry;
import com.arjunbj.ledger.common.InsufficientFundsException;
import com.arjunbj.ledger.common.TransientRetry;
import com.arjunbj.ledger.outbox.OutboxRelay;
import com.arjunbj.ledger.outbox.OutboxRepository;
import com.arjunbj.ledger.recon.Reconciler;
import com.arjunbj.ledger.transfer.TransferCommand;
import com.arjunbj.ledger.transfer.TransferService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Throughput and latency of each balance strategy under a skewed and a uniform workload.
 * Run with: mvnw -B -Pbench test   (excluded from the normal build by the "bench" tag)
 *
 * Clients are threads calling TransferService in the same JVM; HTTP is not in the loop, so the
 * numbers isolate the database transaction. Each run starts from empty tables, warms up, then
 * measures for a fixed duration; the reconciler checks the ledger after every run.
 */
@Tag("bench")
@SpringBootTest(properties = {
        "ledger.outbox.relay.enabled=true",
        "ledger.outbox.relay.interval-ms=50",
        "ledger.reconciler.scheduled=false",
        "ledger.db.pool-size=80",
        "logging.level.com.arjunbj.ledger=WARN"})
class LedgerBenchmarkTest {

    enum Workload { SKEWED, UNIFORM }

    record Result(String workload, String strategy, int clients, double seconds, long transfers, double tps,
                  double p50Ms, double p95Ms, double p99Ms, double maxMs, long rejected, long errors, long retries,
                  int violations, boolean readModelChecked, List<String> sampleErrors) {
    }

    private static final long USER_SEED = 1_000_000_000L;
    private static final long MAX_AMOUNT = 10_000;

    private final List<Integer> clientCounts = Arrays.stream(System.getProperty("bench.clients", "32,64").split(","))
            .map(String::trim).map(Integer::parseInt).toList();
    private final List<StrategyName> strategyList = Arrays.stream(System.getProperty("bench.strategies", "ROW_LOCK,ATOMIC,SHARDED").split(","))
            .map(String::trim).map(StrategyName::valueOf).toList();
    private final int measureSeconds = Integer.getInteger("bench.seconds", 15);
    private final int warmupSeconds = Integer.getInteger("bench.warmup", 5);
    private final int userCount = Integer.getInteger("bench.users", 1000);
    private final int hotShards = Integer.getInteger("bench.shards", 16);

    @Autowired JdbcTemplate jdbc;
    @Autowired AccountService accounts;
    @Autowired TransferService transfers;
    @Autowired StrategyRegistry strategies;
    @Autowired TransientRetry retry;
    @Autowired OutboxRelay relay;
    @Autowired OutboxRepository outbox;
    @Autowired Reconciler reconciler;
    @Autowired ObjectMapper json;

    @Test
    void run() throws Exception {
        List<Result> results = new ArrayList<>();
        for (Workload workload : Workload.values()) {
            for (StrategyName strategy : strategyList) {
                for (int clients : clientCounts) {
                    Result r = runOne(strategy, workload, clients);
                    System.out.printf(Locale.ROOT, "%-8s %-9s %3d clients: %8.0f tps  p50 %.2f ms  p99 %.2f ms  rejected %d  errors %d  retries %d  violations %d%n",
                            workload, strategy, clients, r.tps(), r.p50Ms(), r.p99Ms(), r.rejected(), r.errors(), r.retries(), r.violations());
                    results.add(r);
                }
            }
        }
        write(results);
        assertThat(results).allSatisfy(r -> assertThat(r.violations()).isZero());
    }

    private Result runOne(StrategyName strategy, Workload workload, int clients) throws Exception {
        jdbc.execute("TRUNCATE statement_lines, processed_events, outbox, entries, balances, transfers, accounts RESTART IDENTITY CASCADE");
        accounts.clearCache();
        strategies.use(strategy);

        // Merchant is created before the users, so it has the lowest id among them and its row
        // lock is taken first in every transaction that touches it (the unfavourable case).
        Account funding = accounts.create("funding", "USD", true, 1);
        Account merchant = accounts.create("merchant", "USD", false, strategy == StrategyName.SHARDED ? hotShards : 1);
        List<Account> users = new ArrayList<>(userCount);
        for (int i = 0; i < userCount; i++) {
            users.add(accounts.create("user-" + i, "USD", false, 1));
        }
        seed(funding, users);
        waitForOutbox();

        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch go = new CountDownLatch(1);
        long start = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200);
        long measureFrom = start + TimeUnit.SECONDS.toNanos(warmupSeconds);
        long measureTo = measureFrom + TimeUnit.SECONDS.toNanos(measureSeconds);
        ConcurrentLinkedQueue<String> errorSamples = new ConcurrentLinkedQueue<>();
        List<Future<long[]>> futures = new ArrayList<>();
        List<Histogram> histograms = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            Histogram histogram = new Histogram(TimeUnit.SECONDS.toMicros(60), 3);
            histograms.add(histogram);
            futures.add(pool.submit(() -> {
                go.await();
                long ok = 0;
                long rejected = 0;
                long errors = 0;
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                while (true) {
                    long t0 = System.nanoTime();
                    if (t0 >= measureTo) {
                        break;
                    }
                    boolean measured = t0 >= measureFrom;
                    Account from = users.get(rnd.nextInt(userCount));
                    Account to;
                    if (workload == Workload.SKEWED && rnd.nextBoolean()) {
                        to = merchant;
                    } else {
                        do {
                            to = users.get(rnd.nextInt(userCount));
                        } while (to.id() == from.id());
                    }
                    long amount = 1 + rnd.nextLong(MAX_AMOUNT);
                    try {
                        transfers.create(UUID.randomUUID().toString(),
                                new TransferCommand(from.id(), to.id(), amount, "USD", false));
                        if (measured) {
                            histogram.recordValue(Math.min((System.nanoTime() - t0) / 1_000, histogram.getHighestTrackableValue()));
                            ok++;
                        }
                    } catch (InsufficientFundsException e) {
                        if (measured) {
                            rejected++;
                        }
                    } catch (RuntimeException e) {
                        if (measured) {
                            errors++;
                        }
                        if (errorSamples.size() < 5) {
                            errorSamples.add(e.toString());
                        }
                    }
                }
                return new long[] {ok, rejected, errors};
            }));
        }
        go.countDown();
        sleepUntil(measureFrom);
        long retriesAtStart = retry.retries();
        sleepUntil(measureTo);
        long ok = 0;
        long rejected = 0;
        long errors = 0;
        for (Future<long[]> f : futures) {
            long[] counts = f.get();
            ok += counts[0];
            rejected += counts[1];
            errors += counts[2];
        }
        long retries = retry.retries() - retriesAtStart;
        pool.shutdown();

        Histogram all = new Histogram(TimeUnit.SECONDS.toMicros(60), 3);
        histograms.forEach(all::add);
        waitForOutbox();
        Reconciler.Report report = reconciler.run();
        return new Result(workload.name(), strategy.name(), clients, measureSeconds, ok, ok / (double) measureSeconds,
                ms(all.getValueAtPercentile(50)), ms(all.getValueAtPercentile(95)), ms(all.getValueAtPercentile(99)),
                ms(all.getMaxValue()), rejected, errors, retries, report.violations().size(), report.readModelChecked(),
                List.copyOf(errorSamples));
    }

    private void seed(Account funding, List<Account> users) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        for (Account user : users) {
            futures.add(pool.submit(() -> transfers.create(UUID.randomUUID().toString(),
                    new TransferCommand(funding.id(), user.id(), USER_SEED, "USD", false))));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
    }

    private void waitForOutbox() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (outbox.unpublishedCount() > 0 && System.nanoTime() < deadline) {
            relay.drainAll();
            Thread.sleep(20);
        }
    }

    private static void sleepUntil(long nanoTime) throws InterruptedException {
        long remaining;
        while ((remaining = nanoTime - System.nanoTime()) > 0) {
            TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)));
        }
    }

    private static double ms(long micros) {
        return micros / 1000.0;
    }

    // ---------- report ----------

    private void write(List<Result> results) throws Exception {
        Map<String, Object> setup = new LinkedHashMap<>();
        setup.put("ranAt", Instant.now().toString());
        setup.put("command", "mvnw -B -Pbench test" + (System.getProperty("bench.clients") == null ? "" : " -Dbench.clients=" + System.getProperty("bench.clients")));
        setup.put("cpu", cpuName());
        setup.put("logicalCpus", Runtime.getRuntime().availableProcessors());
        setup.put("memoryGiB", Math.round(((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getTotalMemorySize() / (1024.0 * 1024 * 1024)));
        setup.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        setup.put("java", System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        setup.put("postgres", jdbc.queryForObject("SELECT version()", String.class));
        Map<String, String> pgSettings = new LinkedHashMap<>();
        for (String name : List.of("synchronous_commit", "fsync", "shared_buffers", "max_connections", "wal_level")) {
            pgSettings.put(name, jdbc.queryForObject("SHOW " + name, String.class));
        }
        setup.put("postgresSettings", pgSettings);
        setup.put("connectionPool", 80);
        setup.put("users", userCount);
        setup.put("hotAccountShardsUnderSharded", hotShards);
        setup.put("warmupSeconds", warmupSeconds);
        setup.put("measureSeconds", measureSeconds);
        setup.put("amountMinor", "uniform 1.." + MAX_AMOUNT);

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("setup", setup);
        doc.put("results", results);
        Path dir = Path.of("docs");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("benchmark-results.json"),
                json.copy().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(doc), StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("BENCHMARKS.md"), markdown(setup, pgSettings, results), StandardCharsets.UTF_8);
    }

    private String markdown(Map<String, Object> setup, Map<String, String> pg, List<Result> results) {
        StringBuilder md = new StringBuilder();
        md.append("# Benchmarks\n\n");
        md.append("Generated by `LedgerBenchmarkTest` (`src/test/java/com/arjunbj/ledger/bench`). Every number below was measured on the machine listed, in one run on ")
                .append(setup.get("ranAt")).append(". Raw data: [`benchmark-results.json`](benchmark-results.json).\n\n");
        md.append("## Setup\n\n");
        md.append("| | |\n|---|---|\n");
        md.append("| Command | `").append(setup.get("command")).append("` |\n");
        md.append("| CPU | ").append(setup.get("cpu")).append(", ").append(setup.get("logicalCpus")).append(" logical CPUs |\n");
        md.append("| Memory | ").append(setup.get("memoryGiB")).append(" GiB |\n");
        md.append("| OS | ").append(setup.get("os")).append(" |\n");
        md.append("| Java | ").append(setup.get("java")).append(" |\n");
        md.append("| Database | ").append(setup.get("postgres")).append(" |\n");
        md.append("| Postgres settings | embedded (zonky) defaults on a laptop: ");
        pg.forEach((k, v) -> md.append('`').append(k).append('=').append(v).append("` "));
        md.append("|\n");
        md.append("| Clients | threads calling `TransferService` in the same JVM as Postgres; no HTTP. Connection pool ").append(setup.get("connectionPool")).append(" |\n");
        md.append("| Accounts | 1 funding account, 1 merchant, ").append(setup.get("users")).append(" users seeded with ").append(USER_SEED).append(" minor units each |\n");
        md.append("| Transfers | posted (not two-phase), amount uniform 1..").append(MAX_AMOUNT).append(" minor units, fresh idempotency key each |\n");
        md.append("| Workloads | **SKEWED**: 50% user to merchant, 50% user to user. **UNIFORM**: 100% user to user |\n");
        md.append("| SHARDED | merchant has ").append(setup.get("hotAccountShardsUnderSharded")).append(" balance rows; ROW_LOCK and ATOMIC use 1 |\n");
        md.append("| Timing | ").append(setup.get("warmupSeconds")).append(" s warm-up, then ").append(setup.get("measureSeconds"))
                .append(" s measured; tables truncated before each run; reconciler run after each run once the outbox is drained |\n\n");
        md.append("## Results\n\n");
        md.append("| Workload | Strategy | Clients | Transfers/s | p50 ms | p95 ms | p99 ms | Rejected | Errors | Retries | Reconciler violations |\n");
        md.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (Result r : results) {
            md.append(String.format(Locale.ROOT, "| %s | %s | %d | %,.0f | %.2f | %.2f | %.2f | %d | %d | %d | %d |%n",
                    r.workload(), r.strategy(), r.clients(), r.tps(), r.p50Ms(), r.p95Ms(), r.p99Ms(), r.rejected(),
                    r.errors(), r.retries(), r.violations()));
        }
        md.append("\nLatency is per transfer, measured around `TransferService.create` (one database transaction, including any retry). ")
                .append("Rejected means refused for insufficient funds; Errors means any other failure.\n\n");
        md.append("## Caveats\n\n");
        md.append("- One run per cell on a laptop, with Postgres and the load generator sharing the CPU. Treat differences under about 10% as noise.\n");
        md.append("- `synchronous_commit=").append(pg.get("synchronous_commit"))
                .append("` is the embedded default: a commit does not wait for the WAL flush. With it on, every commit waits for the disk, which lengthens how long the hot row stays locked.\n");
        md.append("- The merchant has a lower account id than every user, so under lock ordering its row is locked first and held for the rest of the transaction.\n");
        return md.toString();
    }

    private static String cpuName() {
        try {
            String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
            if (os.contains("win")) {
                Process p = new ProcessBuilder("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name")
                        .redirectErrorStream(true).start();
                try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                    String line = r.readLine();
                    p.waitFor(10, TimeUnit.SECONDS);
                    if (line != null && !line.isBlank()) {
                        return line.trim();
                    }
                }
            } else if (Files.exists(Path.of("/proc/cpuinfo"))) {
                return Files.readAllLines(Path.of("/proc/cpuinfo")).stream().filter(l -> l.startsWith("model name"))
                        .map(l -> l.substring(l.indexOf(':') + 1).trim()).findFirst().orElse("unknown");
            }
        } catch (Exception e) {
            // fall through
        }
        return "unknown";
    }
}
