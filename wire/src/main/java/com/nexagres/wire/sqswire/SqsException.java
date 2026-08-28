package com.nexagres.wire.sqswire;

public final class SqsException extends RuntimeException {

    public final int status;
    public final String sqsErrorType;

    public SqsException(int status, String sqsErrorType, String message) {
        super(message);
        this.status = status;
        this.sqsErrorType = sqsErrorType;
    }
}
