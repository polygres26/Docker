package com.sayonora.wire.sqswire;

/**
 * A service-level SQS error. {@link #sqsErrorType} is the JSON-protocol type (the Smithy shape
 * name, e.g. {@code QueueDoesNotExist}); {@link #queryCode} is the AWS Query-protocol / awsQuery
 * compatible code real SQS sends for the same error (e.g. {@code AWS.SimpleQueueService.NonExistentQueue}).
 */
public final class SqsException extends RuntimeException {

    public final int status;
    public final String sqsErrorType;
    public final String queryCode;

    public SqsException(int status, String sqsErrorType, String message) {
        this(status, sqsErrorType, queryCodeFor(sqsErrorType), message);
    }

    public SqsException(int status, String sqsErrorType, String queryCode, String message) {
        super(message);
        this.status = status;
        this.sqsErrorType = sqsErrorType;
        this.queryCode = queryCode;
    }

    /** JSON error type to the legacy dotted Query-protocol code real SQS uses. */
    static String queryCodeFor(String jsonType) {
        return switch (jsonType) {
            case "QueueDoesNotExist" -> "AWS.SimpleQueueService.NonExistentQueue";
            case "QueueDeletedRecently" -> "AWS.SimpleQueueService.QueueDeletedRecently";
            case "QueueNameExists" -> "QueueAlreadyExists";
            case "BatchEntryIdsNotDistinct" -> "AWS.SimpleQueueService.BatchEntryIdsNotDistinct";
            case "BatchRequestTooLong" -> "AWS.SimpleQueueService.BatchRequestTooLong";
            case "EmptyBatchRequest" -> "AWS.SimpleQueueService.EmptyBatchRequest";
            case "InvalidBatchEntryId" -> "AWS.SimpleQueueService.InvalidBatchEntryId";
            case "TooManyEntriesInBatchRequest" -> "AWS.SimpleQueueService.TooManyEntriesInBatchRequest";
            case "MessageNotInflight" -> "AWS.SimpleQueueService.MessageNotInflight";
            case "PurgeQueueInProgress" -> "AWS.SimpleQueueService.PurgeQueueInProgress";
            case "UnsupportedOperation" -> "AWS.SimpleQueueService.UnsupportedOperation";
            default -> jsonType;
        };
    }

    public static SqsException invalidParam(String message) {
        return new SqsException(400, "InvalidParameterValue", message);
    }

    public static SqsException missing(String param) {
        return new SqsException(400, "MissingParameter", "The request must contain the parameter " + param + ".");
    }

    public static SqsException noQueue() {
        return new SqsException(400, "QueueDoesNotExist", "The specified queue does not exist.");
    }
}
