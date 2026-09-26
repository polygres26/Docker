package com.sayonora.warp.dynamowire;

import com.google.gson.JsonObject;

public class DynamoException extends RuntimeException {

    public final String dynamoErrorType;

    /** Extra top-level fields of the error body (e.g. {@code Item} on ConditionalCheckFailed,
     * {@code CancellationReasons} on TransactionCanceled); null when none. */
    public JsonObject extraBody;

    public DynamoException(String dynamoErrorType, String message) {
        super(message);
        this.dynamoErrorType = dynamoErrorType;
    }

    public DynamoException withExtra(String field, com.google.gson.JsonElement value) {
        if (extraBody == null) {
            extraBody = new JsonObject();
        }
        extraBody.add(field, value);
        return this;
    }

    public static DynamoException validation(String message) {
        return new DynamoException("ValidationException", message);
    }
}
