package com.sayonora.wire.s3wire;

/** A protocol-level failure that maps directly onto an S3 XML error response. */
final class S3WireException extends RuntimeException {
    final int status;
    final String code;

    S3WireException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }
}
