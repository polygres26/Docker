package com.polygres.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DynamoWireServer {

    private static final Logger log = LoggerFactory.getLogger(DynamoWireServer.class);
    private static final String TARGET_PREFIX = "DynamoDB_20120810.";

    private final Server server;
    private final PgItemStore store;
    private final OperationHandlers handlers;
    private final com.polygres.wire.core.SqlMetricsCollector sqlMetrics;

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, null);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, cache, com.polygres.wire.acl.ConnectionGate.DISABLED);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache, com.polygres.wire.acl.ConnectionGate connectionGate) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, cache, connectionGate,
                com.polygres.wire.http.auth.AccessContextResolver.DISABLED,
                com.polygres.wire.dynamowire.auth.AwsIamCredentialStore.DISABLED);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, cache, connectionGate, oauth,
                com.polygres.wire.dynamowire.auth.AwsIamCredentialStore.DISABLED);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth,
            com.polygres.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, cache, connectionGate, oauth, awsIamCredentials, null);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth,
            com.polygres.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials,
            com.polygres.wire.core.SqlMetricsCollector sqlMetrics) {
        this(port, new PgItemStore(pgHost, pgPort, pgDatabase, pgUser, pgPassword), cache, connectionGate, oauth,
                awsIamCredentials, sqlMetrics);
    }

    /**
     * Sharded mode -- {@code backendRegistry.shardGroup()} (the same shard group SQL's
     * value-shard rules already route across) becomes the set of Postgres backends item storage
     * is split over, hashed by each item's own partition-key value. An empty shard group
     * (nothing configured) behaves exactly like the single-backend constructor above, pointed at
     * the registry's default target -- see {@link PgItemStore}'s javadoc for the full story.
     */
    public DynamoWireServer(int port, com.polygres.wire.core.BackendRegistry backendRegistry, DynamoCache cache,
            com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth,
            com.polygres.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials,
            com.polygres.wire.core.SqlMetricsCollector sqlMetrics) {
        this(port, new PgItemStore(backendRegistry), cache, connectionGate, oauth, awsIamCredentials, sqlMetrics);
    }

    private DynamoWireServer(int port, PgItemStore store, DynamoCache cache,
            com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth,
            com.polygres.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials,
            com.polygres.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = store;
        this.handlers = new OperationHandlers(store, cache, sqlMetrics);
        this.sqlMetrics = sqlMetrics;
        com.polygres.wire.dynamowire.auth.SigV4Verifier sigV4Verifier =
                new com.polygres.wire.dynamowire.auth.SigV4Verifier(awsIamCredentials);
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                if (!connectionGate.acceptHttp(request)) {
                    writeError(response, 403, "AccessDeniedException", "forbidden");
                    return;
                }
                String body = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (awsIamCredentials.isEnabled()) {
                    com.polygres.wire.dynamowire.auth.SigV4Verifier.Result sigResult = sigV4Verifier.verify(request, body);
                    if (!sigResult.valid()) {
                        log.warn("SigV4: rejecting request -- {}", sigResult.reason());
                        writeError(response, 401, "UnrecognizedClientException", sigResult.reason());
                        return;
                    }
                } else if (oauth.enforce(request, response) == null) {
                    return;
                }
                handleRequest(request, response, body);
            }
        });
    }

    private void handleRequest(HttpServletRequest request, HttpServletResponse response, String body) throws IOException {
        response.setContentType("application/x-amz-json-1.0");
        String amzTarget = request.getHeader("X-Amz-Target");
        if (amzTarget == null || !amzTarget.startsWith(TARGET_PREFIX)) {
            writeError(response, 400, "UnknownOperationException", "Missing or unrecognized X-Amz-Target header: " + amzTarget);
            return;
        }
        String operation = amzTarget.substring(TARGET_PREFIX.length());
        JsonObject requestJson;
        try {
            requestJson = body.isBlank() ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
        } catch (RuntimeException e) {
            writeError(response, 400, "SerializationException", "Could not parse request body as JSON: " + e.getMessage());
            return;
        }
        long start = System.nanoTime();
        try {
            JsonObject responseJson = handlers.dispatch(operation, requestJson);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(responseJson.toString());
        } catch (DynamoException e) {
            writeError(response, statusForError(e.dynamoErrorType), e.dynamoErrorType, e.getMessage());
        } catch (RuntimeException e) {
            log.error("dynamowire operation {} failed", operation, e);
            // PgItemStore wraps every real backend SQLException in a plain RuntimeException (e.g.
            // "CreateTable failed for foo", cause = the real SQLException) rather than letting it
            // propagate directly -- unwrap it here so a real Postgres failure (unique violation,
            // missing table, permission denied, a genuinely dead connection) gets translated to
            // real DynamoDB vocabulary via DynamoDbErrorMapper instead of collapsing to the same
            // generic InternalFailure regardless of what actually went wrong.
            if (e.getCause() instanceof java.sql.SQLException sqlEx) {
                String errorType = DynamoDbErrorMapper.errorType(sqlEx.getSQLState());
                writeError(response, DynamoDbErrorMapper.status(sqlEx.getSQLState()), errorType, e.getMessage());
            } else {
                writeError(response, DynamoDbErrorMapper.DEFAULT_STATUS, DynamoDbErrorMapper.DEFAULT_ERROR_TYPE,
                        String.valueOf(e.getMessage()));
            }
        } finally {
            // Skip schema operations (CreateTable/DeleteTable/DescribeTable/ListTables) --
            // they're not "traffic" in the sense the dashboard means, and would otherwise show up
            // as one-off top-cost entries unrelated to actual read/write volume.
            if (sqlMetrics != null) {
                var kind = switch (operation) {
                    case "GetItem", "Query", "Scan", "BatchGetItem", "TransactGetItems" ->
                            com.polygres.wire.core.SqlMetricsCollector.StatementKind.READ;
                    case "PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem", "TransactWriteItems" ->
                            com.polygres.wire.core.SqlMetricsCollector.StatementKind.WRITE;
                    default -> null;
                };
                if (kind != null) {
                    // This span already covers the full request-to-response-write cycle (the
                    // response.getWriter().write(...) above runs inside the timed try block), so
                    // the same duration is valid as both exec time and RTT -- see
                    // SqlMetricsCollector's RTT javadoc.
                    long elapsedNanos = System.nanoTime() - start;
                    sqlMetrics.recordOperation("dynamowire", resolveBackendLabel(operation, requestJson), kind,
                            operation, elapsedNanos, elapsedNanos);
                }
            }
        }
    }

    /**
     * Best-effort, metrics-label-only re-derivation of which shard an item-level operation
     * landed on -- PgItemStore doesn't hand the resolved backend name back from its own
     * operations (it isn't needed for correctness, only for this label), so this repeats the
     * same "find the partition key, ask the store which shard it hashes to" lookup independently.
     * Any failure (unknown table, malformed key, a Scan with no key at all) just falls back to
     * "default" -- getting the metrics label right is never worth failing the actual request.
     */
    private String resolveBackendLabel(String operation, JsonObject requestJson) {
        try {
            if (!requestJson.has("TableName")) {
                return "default";
            }
            TableSchema schema = store.describeTable(requestJson.get("TableName").getAsString());
            JsonObject keySource = requestJson.has("Key") ? requestJson.getAsJsonObject("Key")
                    : requestJson.has("Item") ? requestJson.getAsJsonObject("Item") : null;
            if (keySource == null) {
                return "default";
            }
            AttributeValue pk = PgItemStore.jsonToItem(keySource).get(schema.partitionKeyName());
            return pk == null ? "default" : store.resolveBackendFor(pk.scalar);
        } catch (RuntimeException e) {
            return "default";
        }
    }

    private static int statusForError(String type) {
        return switch (type) {
            case "ResourceNotFoundException", "TableNotFoundException" -> 400;
            case "ResourceInUseException", "ConditionalCheckFailedException", "ValidationException",
                    "TransactionCanceledException" -> 400;
            default -> 400;
        };
    }

    private void writeError(HttpServletResponse response, int status, String errorType, String message) throws IOException {
        JsonObject err = new JsonObject();
        err.addProperty("__type", "com.amazonaws.dynamodb.v20120810#" + errorType);
        err.addProperty("message", message);
        response.setStatus(status);
        response.getWriter().write(err.toString());
    }

    public void start() throws Exception {
        server.start();
        log.info("polywire dynamowire (DynamoDB HTTP/JSON) listening on port {}",
                ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
        store.close();
    }
}
