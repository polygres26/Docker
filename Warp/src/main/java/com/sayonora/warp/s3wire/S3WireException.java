package com.sayonora.warp.s3wire;

import java.util.Map;

/** A protocol-level failure that maps directly onto an S3 XML error response. */
final class S3WireException extends RuntimeException {
    final int status;
    final String code;
    /** Extra response headers (e.g. {@code x-amz-delete-marker}); may be null. */
    final Map<String, String> headers;
    /** Extra XML elements inside {@code <Error>} (name -> value, e.g. {@code Key}, {@code UploadId}); may be null. */
    final Map<String, String> details;

    S3WireException(int status, String code, String message) {
        this(status, code, message, null, null);
    }

    S3WireException(int status, String code, String message, Map<String, String> headers, Map<String, String> details) {
        super(message);
        this.status = status;
        this.code = code;
        this.headers = headers;
        this.details = details;
    }
}
