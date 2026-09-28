package com.sayonora.warp.awswire;

/** An AWS API error: HTTP status, error code (as the SDKs expose it) and message. */
public final class AwsException extends RuntimeException {

    public final int status;
    public final String code;
    public final boolean senderFault;

    public AwsException(int status, String code, String message) {
        super(message, null, false, false);
        this.status = status;
        this.code = code;
        this.senderFault = status < 500;
    }

    public static AwsException bad(String code, String message) {
        return new AwsException(400, code, message);
    }

    public static AwsException notFound(String code, String message) {
        return new AwsException(404, code, message);
    }

    public static AwsException validation(String message) {
        return new AwsException(400, "ValidationException", message);
    }

    public static AwsException invalidParameter(String message) {
        return new AwsException(400, "InvalidParameter", message);
    }

    public static AwsException internal(String message) {
        return new AwsException(500, "InternalFailure", message);
    }
}
