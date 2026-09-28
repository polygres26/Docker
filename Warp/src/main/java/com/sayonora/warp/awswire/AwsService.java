package com.sayonora.warp.awswire;

import com.google.gson.JsonObject;
import com.sayonora.warp.core.SqlMetricsCollector;
import java.util.Set;

/**
 * One AWS API implemented on Postgres. A service works on a protocol-neutral request {@link JsonObject}: {@link AwsHttp}
 * decodes the JSON protocol (1.0/1.1), CBOR (Kinesis) or the AWS Query protocol into that shape and encodes the result
 * back, so an operation is written once.
 */
public abstract class AwsService {

    /** Per-request context. */
    public record Call(String baseUrl, String accessKey, String protocol, jakarta.servlet.http.HttpServletRequest http) {
    }

    /** Short id: {@code sns}, {@code kinesis}, {@code secretsmanager}, {@code ssm}, {@code kms}, {@code sts}. */
    public abstract String id();

    /** Metrics protocol label: {@code snswire}, {@code kinesiswire}, ... */
    public abstract String metricsProtocol();

    /** Names the SigV4 credential scope uses for this service. */
    public abstract Set<String> signingNames();

    /** {@code X-Amz-Target} prefixes (including the dot) of the JSON protocol; empty when Query only. */
    public abstract Set<String> targetPrefixes();

    /** JSON protocol version: {@code 1.0} or {@code 1.1}. */
    public String jsonVersion() {
        return "1.1";
    }

    public boolean supportsQuery() {
        return false;
    }

    public boolean supportsCbor() {
        return false;
    }

    public String xmlNamespace() {
        return null;
    }

    /** Names whose JSON object value is a Query-protocol map ({@code <entry><key/><value/></entry>}). */
    public Set<String> mapFields() {
        return Set.of();
    }

    /** CBOR: field names that are blobs (byte strings) and timestamps (tag 1). */
    public Set<String> blobFields() {
        return Set.of();
    }

    public Set<String> timestampFields() {
        return Set.of();
    }

    /** Query protocol: operation names in this service's action list, used by the unified endpoint for unsigned requests. */
    public Set<String> queryActions() {
        return Set.of();
    }

    public boolean available() {
        return true;
    }

    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return op.startsWith("Get") || op.startsWith("List") || op.startsWith("Describe")
                ? SqlMetricsCollector.StatementKind.READ : SqlMetricsCollector.StatementKind.WRITE;
    }

    /** Metrics backend label for the request (the owning shard where cheaply known). */
    public String backendLabel(String op, JsonObject req) {
        return "default";
    }

    public abstract JsonObject invoke(String op, JsonObject req, Call call) throws Exception;

    /** Error body content type wrapper hook: the {@code __type} a JSON-protocol error carries for {@code code}. */
    public String jsonErrorType(String code) {
        return code;
    }

    public void start() {
    }

    public void stop() {
    }
}
