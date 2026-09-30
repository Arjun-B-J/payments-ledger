package com.arjunbj.ledger.common;

import org.springframework.http.HttpStatus;

/** A business error. Each one maps to a single RFC 7807 problem type. */
public class LedgerException extends RuntimeException {

    private final HttpStatus status;
    private final String type;
    private final String title;

    public LedgerException(HttpStatus status, String type, String title, String detail) {
        super(detail);
        this.status = status;
        this.type = type;
        this.title = title;
    }

    public HttpStatus status() {
        return status;
    }

    public String type() {
        return type;
    }

    public String title() {
        return title;
    }

    public static LedgerException notFound(String what, Object id) {
        return new LedgerException(HttpStatus.NOT_FOUND, "not-found", "Not found", what + " " + id + " does not exist");
    }

    public static LedgerException badRequest(String detail) {
        return new LedgerException(HttpStatus.BAD_REQUEST, "bad-request", "Bad request", detail);
    }

    public static LedgerException currencyMismatch(String detail) {
        return new LedgerException(HttpStatus.UNPROCESSABLE_ENTITY, "currency-mismatch", "Currency mismatch", detail);
    }

    public static LedgerException idempotencyConflict(String key) {
        return new LedgerException(HttpStatus.CONFLICT, "idempotency-key-reused", "Idempotency key reused",
                "Idempotency-Key " + key + " was already used with a different request body");
    }

    public static LedgerException invalidState(Object transferId, Object current, Object target) {
        return new LedgerException(HttpStatus.CONFLICT, "invalid-transfer-state", "Invalid transfer state",
                "transfer " + transferId + " is " + current + " and cannot become " + target);
    }
}
