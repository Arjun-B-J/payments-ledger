package com.arjunbj.ledger.account;

import com.arjunbj.ledger.account.EntryRepository.EntryPage;
import com.arjunbj.ledger.common.LedgerException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.time.Instant;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/accounts")
public class AccountController {

    public record CreateAccountRequest(
            @NotBlank @Size(max = 200) String name,
            @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
            Boolean allowOverdraft,
            @Min(1) @Max(64) Integer shards) {
    }

    /** balanceMinor is the available balance: posted entries minus pending holds. */
    public record AccountView(long id, String name, String currency, boolean allowOverdraft, int shards,
                              long balanceMinor, Instant createdAt) {
        static AccountView of(Account a, long balance) {
            return new AccountView(a.id(), a.name(), a.currency(), a.allowOverdraft(), a.shards(), balance, a.createdAt());
        }
    }

    private final AccountService accounts;
    private final EntryRepository entries;

    public AccountController(AccountService accounts, EntryRepository entries) {
        this.accounts = accounts;
        this.entries = entries;
    }

    @PostMapping
    public ResponseEntity<AccountView> create(@Valid @RequestBody CreateAccountRequest request) {
        Account account = accounts.create(request.name(), request.currency(),
                Boolean.TRUE.equals(request.allowOverdraft()), request.shards() == null ? 1 : request.shards());
        return ResponseEntity.created(URI.create("/accounts/" + account.id())).body(AccountView.of(account, 0));
    }

    @GetMapping("/{id}")
    public AccountView get(@PathVariable long id) {
        Account account = accounts.get(id);
        return AccountView.of(account, accounts.balance(id));
    }

    @GetMapping("/{id}/entries")
    public EntryPage entries(@PathVariable long id,
                             @RequestParam(required = false) Long before,
                             @RequestParam(defaultValue = "50") int limit) {
        if (limit < 1 || limit > 200) {
            throw LedgerException.badRequest("limit must be between 1 and 200");
        }
        accounts.get(id);
        return entries.page(id, before, limit);
    }
}
