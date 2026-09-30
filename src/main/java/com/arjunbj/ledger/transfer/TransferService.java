package com.arjunbj.ledger.transfer;

import com.arjunbj.ledger.account.Account;
import com.arjunbj.ledger.account.AccountService;
import com.arjunbj.ledger.account.EntryRepository;
import com.arjunbj.ledger.balance.BalanceStrategy;
import com.arjunbj.ledger.balance.StrategyRegistry;
import com.arjunbj.ledger.common.FailPoints;
import com.arjunbj.ledger.common.InsufficientFundsException;
import com.arjunbj.ledger.common.LedgerException;
import com.arjunbj.ledger.common.TransientRetry;
import com.arjunbj.ledger.outbox.OutboxRepository;
import com.arjunbj.ledger.outbox.TransferEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Creates, posts and voids transfers. Each operation is one database transaction wrapped in a
 * bounded retry for deadlock and serialization errors. Inside the transaction the journal rows
 * (transfer, entries, outbox) are written first and the balance rows last, in ascending account
 * id order: the balance rows are the contended ones, so their locks are taken late and held for
 * the shortest possible time, and the fixed order rules out lock cycles between transfers.
 */
@Service
public class TransferService {

    public record CreateResult(TransferView transfer, boolean replayed) {
    }

    private static final Logger log = LoggerFactory.getLogger(TransferService.class);
    private static final int MAX_KEY_LENGTH = 255;

    private final AccountService accounts;
    private final TransferRepository transfers;
    private final EntryRepository entries;
    private final OutboxRepository outbox;
    private final StrategyRegistry strategies;
    private final TransientRetry retry;
    private final FailPoints failPoints;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;

    public TransferService(AccountService accounts, TransferRepository transfers, EntryRepository entries,
                           OutboxRepository outbox, StrategyRegistry strategies, TransientRetry retry,
                           FailPoints failPoints, TransactionTemplate tx, MeterRegistry meters) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.entries = entries;
        this.outbox = outbox;
        this.strategies = strategies;
        this.retry = retry;
        this.failPoints = failPoints;
        this.tx = tx;
        this.meters = meters;
    }

    public CreateResult create(String idempotencyKey, TransferCommand cmd) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > MAX_KEY_LENGTH) {
            throw LedgerException.badRequest("Idempotency-Key header must be 1 to " + MAX_KEY_LENGTH + " characters");
        }
        if (cmd.amountMinor() <= 0) {
            throw LedgerException.badRequest("amountMinor must be positive");
        }
        if (cmd.debitAccountId() == cmd.creditAccountId()) {
            throw LedgerException.badRequest("debit and credit account must differ");
        }
        Account debit = accounts.get(cmd.debitAccountId());
        Account credit = accounts.get(cmd.creditAccountId());
        if (!debit.currency().equals(cmd.currency()) || !credit.currency().equals(cmd.currency())) {
            throw LedgerException.currencyMismatch("transfer is " + cmd.currency() + " but account " + debit.id() + " is "
                    + debit.currency() + " and account " + credit.id() + " is " + credit.currency());
        }
        String hash = cmd.requestHash();
        BalanceStrategy strategy = strategies.active();
        long start = System.nanoTime();
        String outcome = "error";
        try {
            CreateResult result = retry.run(strategy.name().name(),
                    () -> tx.execute(status -> createInTx(idempotencyKey, hash, cmd, debit, credit, strategy)));
            MDC.put("transferId", result.transfer().id().toString());
            outcome = result.replayed() ? "replayed" : cmd.pending() ? "pending" : "posted";
            log.debug("transfer {} amount={} {}", outcome, cmd.amountMinor(), cmd.currency());
            // Test hook: a crash here means the client never saw the committed result.
            failPoints.hit(FailPoints.Point.AFTER_COMMIT);
            return result;
        } catch (InsufficientFundsException e) {
            outcome = "rejected";
            log.info("transfer rejected: {}", e.getMessage());
            throw e;
        } catch (LedgerException e) {
            outcome = e.type();
            throw e;
        } catch (DataIntegrityViolationException e) {
            if (isOverdraftCheck(e)) {
                // The CHECK constraint caught what the strategy did not: still a refusal, not a crash.
                outcome = "rejected";
                throw new InsufficientFundsException(debit.id(), cmd.amountMinor());
            }
            throw e;
        } finally {
            record(strategy, "create", outcome, start);
            MDC.remove("transferId");
        }
    }

    private CreateResult createInTx(String key, String hash, TransferCommand cmd, Account debit, Account credit,
                                    BalanceStrategy strategy) {
        UUID id = UUID.randomUUID();
        Transfer.Status status = cmd.pending() ? Transfer.Status.PENDING : Transfer.Status.POSTED;
        Optional<Transfer> inserted = transfers.insertIfAbsent(id, key, hash, cmd, status);
        if (inserted.isEmpty()) {
            // The key is taken by a committed transfer: replay it, or refuse a different body.
            Transfer existing = transfers.findByKey(key)
                    .orElseThrow(() -> new IllegalStateException("idempotency key " + key + " conflicted but has no row"));
            if (!existing.requestHash().equals(hash)) {
                throw LedgerException.idempotencyConflict(key);
            }
            return new CreateResult(TransferView.asCreated(existing, cmd.pending()), true);
        }
        Transfer transfer = inserted.get();
        MDC.put("transferId", id.toString());
        if (cmd.pending()) {
            // Reserve only: the debit side's available balance drops now; entries wait for post.
            strategy.debit(debit, cmd.amountMinor());
            failPoints.hit(FailPoints.Point.AFTER_DEBIT_LEG);
        } else {
            entries.insertPair(id, debit.id(), credit.id(), cmd.amountMinor(), cmd.currency());
            outbox.append(TransferEvent.of(TransferEvent.POSTED, transfer));
            applyInLockOrder(strategy, debit, credit, cmd.amountMinor());
        }
        return new CreateResult(TransferView.of(transfer), false);
    }

    private void applyInLockOrder(BalanceStrategy strategy, Account debit, Account credit, long amount) {
        if (debit.id() < credit.id()) {
            strategy.debit(debit, amount);
            failPoints.hit(FailPoints.Point.AFTER_DEBIT_LEG);
            strategy.credit(credit, amount);
        } else {
            strategy.credit(credit, amount);
            strategy.debit(debit, amount);
            failPoints.hit(FailPoints.Point.AFTER_DEBIT_LEG);
        }
    }

    public TransferView post(UUID id) {
        return finish(id, Transfer.Status.POSTED);
    }

    public TransferView voidTransfer(UUID id) {
        return finish(id, Transfer.Status.VOIDED);
    }

    public TransferView get(UUID id) {
        return transfers.find(id).map(TransferView::of).orElseThrow(() -> LedgerException.notFound("transfer", id));
    }

    private TransferView finish(UUID id, Transfer.Status target) {
        BalanceStrategy strategy = strategies.active();
        MDC.put("transferId", id.toString());
        long start = System.nanoTime();
        String outcome = "error";
        try {
            TransferView view = retry.run(strategy.name().name(), () -> tx.execute(status -> finishInTx(id, target, strategy)));
            outcome = target.name().toLowerCase();
            log.info("transfer {}", outcome);
            return view;
        } catch (LedgerException e) {
            outcome = e.type();
            throw e;
        } finally {
            record(strategy, target == Transfer.Status.POSTED ? "post" : "void", outcome, start);
            MDC.remove("transferId");
        }
    }

    private TransferView finishInTx(UUID id, Transfer.Status target, BalanceStrategy strategy) {
        Transfer transfer = transfers.findForUpdate(id).orElseThrow(() -> LedgerException.notFound("transfer", id));
        if (transfer.status() == target) {
            return TransferView.of(transfer); // repeated post or void: a no-op, safe for client retries
        }
        if (transfer.status() != Transfer.Status.PENDING) {
            throw LedgerException.invalidState(id, transfer.status(), target);
        }
        Transfer updated = transfers.updateStatus(id, target);
        if (target == Transfer.Status.POSTED) {
            entries.insertPair(id, transfer.debitAccountId(), transfer.creditAccountId(), transfer.amountMinor(),
                    transfer.currency());
            outbox.append(TransferEvent.of(TransferEvent.POSTED, updated));
            // The debit side was reserved at creation; posting only adds the credit.
            strategy.credit(accounts.get(transfer.creditAccountId()), transfer.amountMinor());
        } else {
            outbox.append(TransferEvent.of(TransferEvent.VOIDED, updated));
            // Release the hold back to the account it was reserved from.
            strategy.credit(accounts.get(transfer.debitAccountId()), transfer.amountMinor());
        }
        return TransferView.of(updated);
    }

    private static boolean isOverdraftCheck(DataIntegrityViolationException e) {
        return e.getMessage() != null && e.getMessage().contains("balances_no_overdraft");
    }

    private void record(BalanceStrategy strategy, String operation, String outcome, long startNanos) {
        Timer.builder("ledger.transfer.latency")
                .description("Transfer operation latency, including retries")
                .tag("strategy", strategy.name().name())
                .tag("operation", operation)
                .tag("outcome", outcome)
                .register(meters)
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }
}
