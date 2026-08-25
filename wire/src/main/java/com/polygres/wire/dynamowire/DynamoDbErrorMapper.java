package com.polygres.wire.dynamowire;

import java.util.Map;

/**
 * Translates a real Postgres backend error (SQLSTATE) into a genuine DynamoDB service error type
 * + HTTP status -- the DynamoDB-protocol counterpart to {@code SqlStateErrorMapper} (which does
 * the same job for orawire/mywire/mssqlwire's numeric error codes). Every mapping here was
 * verified against AWS's own DynamoDB API reference and common-errors documentation, not guessed
 * (see the class-level notes on individual entries for the specific verification).
 *
 * <p>{@code DynamoWireServer}'s existing {@code DynamoException}/{@code statusForError} handles
 * genuinely application-level validation failures (bad request shape, missing key, etc.) that are
 * detected before ever reaching Postgres -- this class is strictly for the OTHER path: a real
 * {@code SQLException} that came back from the shared pipeline/backend, which today gets uniformly
 * collapsed into the common AWS {@code InternalFailure} regardless of what actually went wrong in
 * Postgres (unique violation, missing table, permission denied, a genuinely dead connection --
 * all indistinguishable to the client before this).
 */
public final class DynamoDbErrorMapper {

    /** Common AWS error type (not DynamoDB-specific), HTTP 500 -- see AWS's own Common Errors
     * reference. This is the pre-existing default {@code DynamoWireServer} already used for any
     * unmapped/unexpected failure; kept as the fallback here too rather than introduced. */
    public static final String DEFAULT_ERROR_TYPE = "InternalFailure";
    public static final int DEFAULT_STATUS = 500;

    private record NativeError(String errorType, int status) {
    }

    private static final Map<String, NativeError> TABLE = Map.ofEntries(

            // unique_violation / duplicate_table. Real DynamoDB has no server-enforced secondary
            // uniqueness beyond the primary key (PutItem without a ConditionExpression silently
            // upserts) -- a real client only ever sees this shape of failure as a
            // ConditionalCheckFailedException from an explicit conditional write, which is exactly
            // what this maps to: the closest genuine DynamoDB vocabulary for "this would create a
            // duplicate," per AWS's own API reference.
            Map.entry("23505", new NativeError("ConditionalCheckFailedException", 400)),

            // duplicate_table (CreateTable on a name that already exists) -- a real, specific
            // DynamoDB exception for exactly this.
            Map.entry("42P07", new NativeError("ResourceInUseException", 400)),

            // undefined_table -- the operation named a table (or, per AWS's docs, a GSI) that
            // doesn't exist, or isn't ACTIVE yet.
            Map.entry("42P01", new NativeError("ResourceNotFoundException", 400)),

            // Invalid input of various shapes -- all become the same real DynamoDB
            // ValidationException, matching how DynamoDB itself doesn't further subdivide "the
            // input doesn't meet the required format or constraints."
            Map.entry("23502", new NativeError("ValidationException", 400)),
            Map.entry("22P02", new NativeError("ValidationException", 400)),
            Map.entry("22001", new NativeError("ValidationException", 400)),
            Map.entry("22003", new NativeError("ValidationException", 400)),

            // insufficient_privilege.
            Map.entry("42501", new NativeError("AccessDeniedException", 403)),

            // serialization_failure / deadlock_detected -- DynamoDB's real, specific exception
            // for a transaction that couldn't complete because of a conflicting concurrent
            // operation on the same item(s).
            Map.entry("40001", new NativeError("TransactionConflictException", 400)),
            Map.entry("40P01", new NativeError("TransactionConflictException", 400)),

            // admin_shutdown / connection_failure / too_many_connections -- the backend is
            // genuinely unreachable. DynamoDB's own (not just common-AWS) InternalServerError is
            // the real, specific name for this, per AWS's SDK documentation -- more accurate here
            // than falling through to the generic common-AWS InternalFailure default.
            Map.entry("57P01", new NativeError("InternalServerError", 500)),
            Map.entry("08006", new NativeError("InternalServerError", 500)),
            Map.entry("53300", new NativeError("InternalServerError", 500)));

    public static String errorType(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_ERROR_TYPE : n.errorType();
    }

    public static int status(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_STATUS : n.status();
    }

    private DynamoDbErrorMapper() {
    }
}
