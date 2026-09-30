package com.arjunbj.ledger.transfer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** A validated transfer request. pending=true reserves funds now and posts or voids later. */
public record TransferCommand(long debitAccountId, long creditAccountId, long amountMinor, String currency,
                              boolean pending) {

    /**
     * SHA-256 over the semantic fields, not the raw body bytes, so whitespace or JSON field order
     * in a retry does not count as a different request.
     */
    public String requestHash() {
        String canonical = "debit=" + debitAccountId + ";credit=" + creditAccountId + ";amount=" + amountMinor
                + ";currency=" + currency + ";pending=" + pending;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is always available", e);
        }
    }
}
