package com.sayonora.warp.oswire;

import com.google.gson.JsonObject;
import java.util.Map;

/** A request that fails in a way OpenSearch itself has a name for -- {@code errorType} becomes
 * the {@code type} field of the OpenSearch-shaped error response {@code OpenSearchWireServer}
 * writes back, same pattern as dynamowire's {@code DynamoException}/sqswire's {@code SqsException}.
 *
 * <p>{@link #status} is the HTTP status real OpenSearch uses for that error type; {@link #extra}
 * carries the additional error fields real OpenSearch adds ({@code index}, {@code index_uuid},
 * {@code resource.type}, {@code caused_by}, {@code line}/{@code col}, ...). {@link #shardLevel}
 * errors (raised while executing a search on a shard, e.g. sorting on a text field) are wrapped in
 * the {@code search_phase_execution_exception} envelope real OpenSearch uses for them.
 */
public final class OpenSearchException extends RuntimeException {

    private static final Map<String, Integer> STATUS = Map.ofEntries(
            Map.entry("index_not_found_exception", 404),
            Map.entry("document_missing_exception", 404),
            Map.entry("resource_not_found_exception", 404),
            Map.entry("aliases_not_found_exception", 404),
            Map.entry("index_template_missing_exception", 404),
            Map.entry("search_context_missing_exception", 404),
            Map.entry("version_conflict_engine_exception", 409),
            Map.entry("security_exception", 403),
            Map.entry("too_many_buckets_exception", 503),
            Map.entry("script_exception", 400),
            Map.entry("no_shard_available_action_exception", 503),
            Map.entry("illegal_state_exception", 500),
            Map.entry("postgres_exception", 500));

    public final String errorType;
    public final int status;
    public final JsonObject extra = new JsonObject();
    public boolean shardLevel;

    public OpenSearchException(String errorType, String message) {
        this(errorType, message, STATUS.getOrDefault(errorType, 400));
    }

    public OpenSearchException(String errorType, String message, int status) {
        super(message);
        this.errorType = errorType;
        this.status = status;
    }

    public OpenSearchException with(String key, String value) {
        extra.addProperty(key, value);
        return this;
    }

    public OpenSearchException asShardLevel() {
        this.shardLevel = true;
        return this;
    }

    public static OpenSearchException indexNotFound(String index) {
        return new OpenSearchException("index_not_found_exception", "no such index [" + index + "]")
                .with("resource.type", "index_or_alias").with("resource.id", index)
                .with("index_uuid", "_na_").with("index", index);
    }

    public static OpenSearchException illegalArgument(String message) {
        return new OpenSearchException("illegal_argument_exception", message);
    }

    public static OpenSearchException parsing(String message) {
        return new OpenSearchException("parsing_exception", message);
    }

    public static OpenSearchException validation(String message) {
        return new OpenSearchException("action_request_validation_exception", "Validation Failed: 1: " + message + ";");
    }
}
