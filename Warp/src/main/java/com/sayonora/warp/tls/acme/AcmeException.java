package com.sayonora.warp.tls.acme;

import java.time.Duration;

/** An ACME failure: an RFC 7807 problem document from the CA (type/detail/status) or a local/transport error. */
public class AcmeException extends RuntimeException {

    public static final String URN = "urn:ietf:params:acme:error:";

    private final int status;
    private final String type;
    private final Duration retryAfter;

    public AcmeException(String message) {
        this(message, 0, null, null, null);
    }

    public AcmeException(String message, Throwable cause) {
        this(message, 0, null, null, cause);
    }

    public AcmeException(String message, int status, String type, Duration retryAfter, Throwable cause) {
        super(message, cause);
        this.status = status;
        this.type = type;
        this.retryAfter = retryAfter;
    }

    public int status() {
        return status;
    }

    /** Problem type URN, e.g. {@code urn:ietf:params:acme:error:rateLimited}; null for local errors. */
    public String type() {
        return type;
    }

    /** Parsed {@code Retry-After}, or null. */
    public Duration retryAfter() {
        return retryAfter;
    }

    public boolean is(String shortType) {
        return type != null && type.equals(URN + shortType);
    }

    public boolean isRateLimited() {
        return is("rateLimited") || status == 429;
    }

    public boolean isBadNonce() {
        return is("badNonce");
    }
}
