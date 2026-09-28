package com.sayonora.warp.tls;

/** A TLS configuration or key-material problem, with a message written for the operator. */
public final class TlsException extends RuntimeException {
    public TlsException(String message) {
        super(message);
    }

    public TlsException(String message, Throwable cause) {
        super(message, cause);
    }
}
