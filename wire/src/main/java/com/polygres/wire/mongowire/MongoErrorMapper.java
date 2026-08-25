package com.polygres.wire.mongowire;

import java.util.Map;

/**
 * Translates a real Postgres backend error (SQLSTATE) into a genuine MongoDB error code +
 * codeName -- the mongowire counterpart to {@code SqlStateErrorMapper}/{@code DynamoDbErrorMapper}/
 * {@code SqsErrorMapper}/{@code OpenSearchErrorMapper}. Until now, ANY real Postgres backend
 * failure (missing collection, duplicate key, permission denied, a genuinely dead connection)
 * collapsed to the same generic {@code 8 UnknownError}, regardless of what actually went wrong.
 *
 * <p>Real MongoDB command-error replies carry both a numeric {@code code} and a string {@code
 * codeName} -- e.g. {@code {ok: 0, errmsg: "...", code: 11000, codeName: "DuplicateKey"}} -- and a
 * real driver's own typed exception classes (pymongo's {@code DuplicateKeyError}, the Java
 * driver's {@code MongoWriteException} with a matching {@code getError().getCategory()}) key off
 * the numeric code specifically. Both fields are supplied here since real MongoDB always sends
 * both.
 */
public final class MongoErrorMapper {

    public static final int DEFAULT_CODE = 8;
    public static final String DEFAULT_CODE_NAME = "UnknownError";

    private record NativeError(int code, String codeName) {
    }

    private static final Map<String, NativeError> TABLE = Map.ofEntries(

            // unique_violation / duplicate_table -- real MongoDB's own name for exactly this,
            // the single most common real-world Mongo error code.
            Map.entry("23505", new NativeError(11000, "DuplicateKey")),
            Map.entry("42P07", new NativeError(11000, "DuplicateKey")),

            // undefined_table -- mongowire's collections are real Postgres tables; a query
            // against one that's genuinely gone (dropped directly against Postgres) is real
            // MongoDB's own NamespaceNotFound.
            Map.entry("42P01", new NativeError(26, "NamespaceNotFound")),

            // serialization_failure / deadlock_detected -- real MongoDB's own name for a write
            // that lost a concurrent conflict and should be retried.
            Map.entry("40001", new NativeError(112, "WriteConflict")),
            Map.entry("40P01", new NativeError(112, "WriteConflict")),

            // Invalid input of various shapes -- real MongoDB's own name for a document that
            // failed schema/type validation.
            Map.entry("23502", new NativeError(121, "DocumentValidationFailure")),
            Map.entry("22P02", new NativeError(121, "DocumentValidationFailure")),
            Map.entry("22001", new NativeError(121, "DocumentValidationFailure")),
            Map.entry("22003", new NativeError(121, "DocumentValidationFailure")),

            // insufficient_privilege.
            Map.entry("42501", new NativeError(13, "Unauthorized")),

            // admin_shutdown / connection_failure / too_many_connections / sqlclient_unable_to_
            // establish_sqlconnection -- the backend is genuinely unreachable. Real MongoDB's own
            // name for a node shutting down/unreachable. 08001 (in addition to the other three) is
            // real, not speculative -- confirmed live: a MongoDB client's own retryable-reads
            // feature (on by default) retries once after the first 57P01, and that retry lands on
            // Hikari's own connection-pool-exhaustion exception once the backend is genuinely
            // down, which carries 08001 rather than 57P01. Without this entry the client-visible
            // error was whichever attempt happened to be last (flaky-looking, not actually
            // random), landing on the generic default instead of a real MongoDB error either way.
            Map.entry("57P01", new NativeError(91, "ShutdownInProgress")),
            Map.entry("08006", new NativeError(91, "ShutdownInProgress")),
            Map.entry("53300", new NativeError(91, "ShutdownInProgress")),
            Map.entry("08001", new NativeError(91, "ShutdownInProgress")));

    public static int code(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_CODE : n.code();
    }

    public static String codeName(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_CODE_NAME : n.codeName();
    }

    private MongoErrorMapper() {
    }
}
