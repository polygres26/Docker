package com.nexagres.wire.oswire;

import java.util.Map;

/**
 * Translates a real Postgres backend error (SQLSTATE) into a genuine OpenSearch/Elasticsearch
 * error type -- the oswire counterpart to {@code SqlStateErrorMapper}/{@code DynamoDbErrorMapper}/
 * {@code SqsErrorMapper}. {@code PostgresSearchStore} backs each index with a real Postgres table
 * -- an index whose table has genuinely disappeared underneath oswire (dropped directly against
 * Postgres, a real outage, etc.) previously surfaced as the same generic {@code postgres_exception}
 * as any other failure, regardless of what actually went wrong.
 */
public final class OpenSearchErrorMapper {

    public static final String DEFAULT_ERROR_TYPE = "postgres_exception";
    public static final int DEFAULT_STATUS = 500;

    private record NativeError(String type, int status) {
    }

    private static final Map<String, NativeError> TABLE = Map.ofEntries(

            // undefined_table -- the index's own backing table is gone. Real OpenSearch/
            // Elasticsearch name for "the referenced index doesn't exist," already used elsewhere
            // in this file for the app-level check (OpenSearchWireServer.java:116) -- this just
            // extends the same real name to the SQLException path too.
            Map.entry("42P01", new NativeError("index_not_found_exception", 404)),

            // unique_violation / duplicate_table -- a create/index operation collided with an
            // existing document or index. Real OpenSearch/Elasticsearch name for a version/
            // create conflict.
            Map.entry("23505", new NativeError("version_conflict_engine_exception", 409)),
            Map.entry("42P07", new NativeError("resource_already_exists_exception", 400)),

            // Invalid input of various shapes -- OpenSearch's real name for a document that
            // failed to parse/validate against the index mapping.
            Map.entry("23502", new NativeError("mapper_parsing_exception", 400)),
            Map.entry("22P02", new NativeError("mapper_parsing_exception", 400)),
            Map.entry("22001", new NativeError("mapper_parsing_exception", 400)),
            Map.entry("22003", new NativeError("mapper_parsing_exception", 400)),

            // insufficient_privilege.
            Map.entry("42501", new NativeError("security_exception", 403)),

            // admin_shutdown / connection_failure / too_many_connections -- the backend is
            // genuinely unreachable. Real OpenSearch/Elasticsearch name for a cluster/shard that
            // can't be reached to service the request.
            Map.entry("57P01", new NativeError("no_shard_available_action_exception", 503)),
            Map.entry("08006", new NativeError("no_shard_available_action_exception", 503)),
            Map.entry("53300", new NativeError("no_shard_available_action_exception", 503)),

            // sqlclient_unable_to_establish_sqlconnection -- a NEW connection attempt failing to
            // establish (distinct from 08006/57P01's already-open-connection-dying), confirmed
            // live via mongowire's retryable-reads hitting this specific code on its retry attempt
            // once the backend is genuinely down -- see SqlStateErrorMapper's matching entry.
            Map.entry("08001", new NativeError("no_shard_available_action_exception", 503)));

    public static String errorType(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_ERROR_TYPE : n.type();
    }

    public static int status(String sqlState) {
        NativeError n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? DEFAULT_STATUS : n.status();
    }

    private OpenSearchErrorMapper() {
    }
}
