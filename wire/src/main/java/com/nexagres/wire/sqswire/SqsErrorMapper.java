package com.nexagres.wire.sqswire;

import java.util.Map;

/**
 * Translates a real Postgres backend error (SQLSTATE) into a genuine SQS service error type +
 * HTTP status -- the SQS-protocol counterpart to {@code SqlStateErrorMapper}/{@code
 * DynamoDbErrorMapper}. {@code PgQueueStore} backs each queue with its own real Postgres table
 * ({@code sqs_queue_<name>}) -- a queue whose table has genuinely disappeared underneath sqswire
 * (dropped directly against Postgres, a real outage, etc.) previously surfaced as the same generic
 * {@code InternalError} as any other failure; this gives it SQS's own real {@code
 * QueueDoesNotExist} name instead.
 *
 * <p>The JSON-protocol {@code __type} value and the legacy Query/XML protocol's {@code &lt;Code&gt;}
 * value are NOT always the same string for this specific error. AWS's own documentation and a
 * filed AWS-SDK GitHub issue describe a real inconsistency where SQS's live JSON-protocol service
 * has, at times, sent the legacy dotted form ({@code AWS.SimpleQueueService.NonExistentQueue}) --
 * but that turned out to be describing a genuine AWS-side BUG that breaks client-side
 * unmarshalling, not the working case: verified empirically against a real AWS SDK v2 client (see
 * {@code SqsErrorMappingIntegrationTest}), the short Smithy shape name ({@code
 * com.amazonaws.sqs#QueueDoesNotExist}) is what this SDK version actually needs to resolve its own
 * typed {@code QueueDoesNotExistException} -- so that's what {@link #jsonErrorType} returns. The
 * legacy protocol's {@code <Code>} element genuinely does use the dotted form, unrelated to the
 * JSON-protocol quirk above -- both are exposed here separately rather than assuming one name
 * works for both wire formats.
 */
public final class SqsErrorMapper {

    public static final String DEFAULT_ERROR_TYPE = "InternalError";
    public static final int DEFAULT_STATUS = 500;

    private record NativeError(String jsonType, String legacyCode, int status) {
    }

    private static final Map<String, NativeError> TABLE = Map.ofEntries(

            // undefined_table -- the queue's own backing table is gone (dropped directly against
            // Postgres, or never created). Real SQS's own name for "the referenced queue doesn't
            // exist" -- see the class javadoc for why the JSON and legacy protocol columns below
            // are genuinely different strings, both verified empirically against a real AWS SDK
            // v2 client, not assumed from documentation.
            Map.entry("42P01", new NativeError(
                    "QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue", 400)),

            // insufficient_privilege -- matches the short form this file's own hand-thrown
            // AccessDenied errors already use elsewhere (SqsWireServer.java), same string for
            // both protocols per AWS's common-errors convention.
            Map.entry("42501", new NativeError("AccessDenied", "AccessDenied", 400)),

            // admin_shutdown / connection_failure / too_many_connections -- the backend is
            // genuinely unreachable. SQS has no more specific real name than the common-AWS
            // InternalError already used as this table's own default -- these entries exist so
            // the mapping is explicit and documented rather than an accident of falling through,
            // not because the string actually changes.
            Map.entry("57P01", new NativeError(DEFAULT_ERROR_TYPE, DEFAULT_ERROR_TYPE, DEFAULT_STATUS)),
            Map.entry("08006", new NativeError(DEFAULT_ERROR_TYPE, DEFAULT_ERROR_TYPE, DEFAULT_STATUS)),
            Map.entry("53300", new NativeError(DEFAULT_ERROR_TYPE, DEFAULT_ERROR_TYPE, DEFAULT_STATUS)),

            // sqlclient_unable_to_establish_sqlconnection -- a NEW connection attempt failing to
            // establish (distinct from 08006/57P01's already-open-connection-dying), confirmed
            // live via mongowire's retryable-reads hitting this specific code on its retry attempt
            // once the backend is genuinely down -- see SqlStateErrorMapper's matching entry.
            Map.entry("08001", new NativeError(DEFAULT_ERROR_TYPE, DEFAULT_ERROR_TYPE, DEFAULT_STATUS)));

    public static String jsonErrorType(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_ERROR_TYPE : n.jsonType();
    }

    public static String legacyErrorCode(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_ERROR_TYPE : n.legacyCode();
    }

    public static int status(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_STATUS : n.status();
    }

    private SqsErrorMapper() {
    }
}
