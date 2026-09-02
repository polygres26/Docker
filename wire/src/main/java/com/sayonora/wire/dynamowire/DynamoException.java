package com.sayonora.wire.dynamowire;

public final class DynamoException extends RuntimeException {

    public final String dynamoErrorType;

    public DynamoException(String dynamoErrorType, String message) {
        super(message);
        this.dynamoErrorType = dynamoErrorType;
    }
}
