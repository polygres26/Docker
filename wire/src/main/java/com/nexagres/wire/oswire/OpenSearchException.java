package com.nexagres.wire.oswire;

/** A request that fails in a way OpenSearch itself has a name for -- {@code errorType} becomes
 * the {@code type} field of the OpenSearch-shaped error response {@code OpenSearchWireServer}
 * writes back, same pattern as dynamowire's {@code DynamoException}/sqswire's {@code SqsException}. */
public final class OpenSearchException extends RuntimeException {

    public final String errorType;

    public OpenSearchException(String errorType, String message) {
        super(message);
        this.errorType = errorType;
    }
}
