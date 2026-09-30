package com.arjunbj.ledger.transfer;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transfers")
public class TransferController {

    public record TransferRequest(
            @NotNull @Positive Long debitAccountId,
            @NotNull @Positive Long creditAccountId,
            @NotNull @Positive Long amountMinor,
            @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
            Boolean pending) {

        TransferCommand toCommand() {
            return new TransferCommand(debitAccountId, creditAccountId, amountMinor, currency, Boolean.TRUE.equals(pending));
        }
    }

    private final TransferService service;

    public TransferController(TransferService service) {
        this.service = service;
    }

    /** A replay returns the original status code and body, flagged with Idempotent-Replayed: true. */
    @PostMapping
    public ResponseEntity<TransferView> create(@RequestHeader("Idempotency-Key") String idempotencyKey,
                                               @Valid @RequestBody TransferRequest request) {
        TransferService.CreateResult result = service.create(idempotencyKey, request.toCommand());
        return ResponseEntity.created(URI.create("/transfers/" + result.transfer().id()))
                .header("Idempotent-Replayed", String.valueOf(result.replayed()))
                .body(result.transfer());
    }

    @GetMapping("/{id}")
    public TransferView get(@PathVariable UUID id) {
        return service.get(id);
    }

    @PostMapping("/{id}/post")
    public TransferView post(@PathVariable UUID id) {
        return service.post(id);
    }

    @PostMapping("/{id}/void")
    public TransferView voidTransfer(@PathVariable UUID id) {
        return service.voidTransfer(id);
    }
}
