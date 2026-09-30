package com.arjunbj.ledger.account;

import com.arjunbj.ledger.common.LedgerException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AccountService {

    private final AccountRepository repository;
    // Account rows never change after creation, so a read-through cache saves a round trip per
    // transfer without any invalidation logic. Only committed rows are cached (see get()).
    private final Map<Long, Account> cache = new ConcurrentHashMap<>();

    public AccountService(AccountRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Account create(String name, String currency, boolean allowOverdraft, int shards) {
        if (shards < 1 || shards > 64) {
            throw LedgerException.badRequest("shards must be between 1 and 64");
        }
        return repository.insert(name, currency, allowOverdraft, shards);
    }

    public Account get(long id) {
        Account cached = cache.get(id);
        if (cached != null) {
            return cached;
        }
        Account account = repository.find(id).orElseThrow(() -> LedgerException.notFound("account", id));
        cache.put(id, account);
        return account;
    }

    public long balance(long id) {
        get(id);
        return repository.balance(id);
    }

    /** Tests and the benchmark truncate tables and reuse ids; production never deletes accounts. */
    public void clearCache() {
        cache.clear();
    }
}
