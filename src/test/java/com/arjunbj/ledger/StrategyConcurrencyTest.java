package com.arjunbj.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.balance.StrategyName;
import com.arjunbj.ledger.common.InsufficientFundsException;
import com.arjunbj.ledger.recon.Reconciler;
import com.arjunbj.ledger.transfer.Transfer;
import com.arjunbj.ledger.transfer.TransferCommand;
import com.arjunbj.ledger.transfer.TransferView;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Many threads move money at random among users and one hot account (credited and debited), with
 * amounts large enough that some transfers are refused and some two-phase transfers are posted or
 * voided. Whatever the interleaving, no money may appear or vanish and the reconciler must be clean.
 */
class StrategyConcurrencyTest extends IntegrationTest {

    private static final int THREADS = 32;
    private static final int OPS_PER_THREAD = 150;
    private static final int USERS = 20;
    private static final long USER_SEED = 10_000;
    private static final long HOT_SEED = 5_000;

    @ParameterizedTest
    @EnumSource(StrategyName.class)
    void moneyIsConservedAndReconcilerIsCleanUnderContention(StrategyName strategy) throws Exception {
        strategies.use(strategy);
        Account funding = account("funding", true, 1);
        Account hot = account("merchant", false, strategy == StrategyName.SHARDED ? 8 : 1);
        List<Account> users = new ArrayList<>();
        for (int i = 0; i < USERS; i++) {
            Account user = account("user-" + i, false, 1);
            users.add(user);
            transfer(funding.id(), user.id(), USER_SEED);
        }
        transfer(funding.id(), hot.id(), HOT_SEED);
        long seeded = USERS * USER_SEED + HOT_SEED;

        AtomicLong completed = new AtomicLong();
        AtomicLong refused = new AtomicLong();
        Queue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < THREADS; t++) {
            pool.submit(() -> {
                start.await();
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                for (int op = 0; op < OPS_PER_THREAD; op++) {
                    Account from;
                    Account to;
                    double roll = rnd.nextDouble();
                    if (roll < 0.4) {
                        from = users.get(rnd.nextInt(USERS));
                        to = hot;
                    } else if (roll < 0.6) {
                        from = hot;
                        to = users.get(rnd.nextInt(USERS));
                    } else {
                        from = users.get(rnd.nextInt(USERS));
                        do {
                            to = users.get(rnd.nextInt(USERS));
                        } while (to.id() == from.id());
                    }
                    long amount = 1 + rnd.nextLong(3_000);
                    boolean twoPhase = rnd.nextDouble() < 0.1;
                    try {
                        TransferView view = transfers.create(UUID.randomUUID().toString(),
                                new TransferCommand(from.id(), to.id(), amount, "USD", twoPhase)).transfer();
                        if (twoPhase) {
                            if (rnd.nextBoolean()) {
                                transfers.post(view.id());
                            } else {
                                transfers.voidTransfer(view.id());
                            }
                        }
                        completed.incrementAndGet();
                    } catch (InsufficientFundsException e) {
                        refused.incrementAndGet();
                    } catch (Throwable e) {
                        unexpected.add(e);
                    }
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(3, TimeUnit.MINUTES)).isTrue();

        assertThat(unexpected).as("unexpected errors").isEmpty();
        assertThat(completed.get() + refused.get()).isEqualTo((long) THREADS * OPS_PER_THREAD);
        assertThat(completed.get()).isPositive();

        // Conservation: users plus the hot account still hold exactly what was seeded, and
        // every account together (funding is negative) sums to zero.
        long held = users.stream().mapToLong(u -> balance(u.id())).sum() + balance(hot.id());
        assertThat(held).isEqualTo(seeded);
        assertThat(jdbc.queryForObject("SELECT sum(balance_minor) FROM balances", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM transfers WHERE status = ?", Long.class,
                Transfer.Status.PENDING.name())).isZero();

        relay.drainAll();
        Reconciler.Report report = reconciler.run();
        assertThat(report.violations()).isEmpty();
        assertThat(report.readModelChecked()).isTrue();
    }
}
