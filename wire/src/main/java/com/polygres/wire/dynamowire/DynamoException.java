package com.polygres.wire.dynamowire;

/**
 * A DynamoDB-shaped error: real clients (AWS SDKs) key error handling off the {@code __type}
 * field in the JSON error body (e.g. {@code com.amazonaws.dynamodb.v20120810#ResourceNotFoundException})
 * plus the HTTP status code, not just the message text. {@link #dynamoErrorType} carries the short
 * name (e.g. {@code "ResourceNotFoundException"}); {@link DynamoWireServer} qualifies it with the
 * real service prefix and picks the matching HTTP status when writing the response.
 */
public final class DynamoException extends RuntimeException {

    public final String dynamoErrorType;

    public DynamoException(String dynamoErrorType, String message) {
        super(message);
        this.dynamoErrorType = dynamoErrorType;
    }
}
