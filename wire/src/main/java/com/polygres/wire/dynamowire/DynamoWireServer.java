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

/**
 * Embedded Jetty HTTP server speaking DynamoDB's real client protocol: AWS SDKs, the CLI, and
 * every DynamoDB tool POST SigV4-signed JSON requests to a single endpoint, naming the operation
 * via the {@code X-Amz-Target: DynamoDB_20120810.<Operation>} header (e.g. {@code GetItem}), not
 * via the URL path — this is why dynamowire, unlike orawire/pgwire/mywire/mssqlwire, is HTTP/JSON
 * rather than another raw-TCP frontend. Follows the same raw-{@code Handler}-API pattern as
 * {@link com.polygres.wire.http.admin.MetricsServer}.
 *
 * <h2>Auth — explicitly NOT real SigV4 verification</h2>
 * Real DynamoDB requires AWS SigV4 request signing. ExtendDB implements full SigV4 verification
 * with an IAM-like policy store (see its docs/manuals/10-security-model.md, referenced from its
 * architecture guide) — that is a substantial subsystem on its own and is out of scope for this
 * pass. This server accepts any {@code Authorization} header (SigV4-shaped or not) without
 * verifying the signature: the AWS SDK client still signs every request exactly as it would
 * against real AWS (nothing about the client changes), so this is purely a server-side
 * simplification for a local-dev/test posture, not a client-visible protocol deviation. This is a
 * deliberate, documented gap — a real production target would need real SigV4 verification before
 * being reachable outside a trusted network.
 *
 * <h2>Execution path — bypasses the SQL pipeline entirely</h2>
 * orawire/pgwire/mywire/mssqlwire all funnel through {@code StatementPipeline}/{@code RouterStage}/
 * {@code QosControlStage}/{@code CacheStage}, whose core abstraction ({@code Statement} with a
 * {@code sqlText} field) assumes a SQL statement string. DynamoDB operations are structured
 * JSON RPCs with their own semantics (conditional writes, key-value lookups, expression languages)
 * that don't map onto "a SQL string to route/cache/QoS-gate" without a bad, lossy fit. This
 * frontend therefore talks straight to {@link PgItemStore} (itself talking to Postgres via a plain
 * JDBC/HikariCP pool) rather than forcing itself through that pipeline — a parallel, simpler
 * execution path scoped to this one frontend, matching the same reasoning mssqlwire's NATIVE mode
 * javadoc gives for bypassing machinery that doesn't fit a given wire protocol's real shape.
 *
 * <h2>Scope</h2>
 * Implemented and live-verified: CreateTable, DeleteTable, DescribeTable, ListTables, PutItem,
 * GetItem, DeleteItem, UpdateItem (SET/REMOVE/ADD/DELETE, ConditionExpression), Query, Scan
 * (KeyConditionExpression, FilterExpression, ProjectionExpression, Limit/ExclusiveStartKey
 * pagination), BatchGetItem, BatchWriteItem, TransactGetItems, TransactWriteItems. Not
 * implemented: UpdateTable, Streams, TTL, Import/Export, GSI/LSI — see this class's and
 * {@link PgItemStore}'s javadoc for exactly what's deferred and why.
 */
public final class DynamoWireServer {

    private static final Logger log = LoggerFactory.getLogger(DynamoWireServer.class);
    private static final String TARGET_PREFIX = "DynamoDB_20120810.";

    private final Server server;
    private final PgItemStore store;
    private final OperationHandlers handlers;

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, null);
    }

    /**
     * {@code cache}: nullable — an exact-key ({@code GetItem}-only) cache-aside cache; see
     * {@link DynamoCache}'s class javadoc for scope and default-on rationale. {@code null} when
     * {@code POLYWIRE_DYNAMOWIRE_CACHE_ENABLED=false}, in which case every {@code GetItem} goes
     * straight to Postgres exactly as before this pass.
     */
    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache) {
        this.store = new PgItemStore(pgHost, pgPort, pgDatabase, pgUser, pgPassword);
        this.handlers = new OperationHandlers(store, cache);
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                handleRequest(request, response);
            }
        });
    }

    private void handleRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/x-amz-json-1.0");
        String amzTarget = request.getHeader("X-Amz-Target");
        if (amzTarget == null || !amzTarget.startsWith(TARGET_PREFIX)) {
            writeError(response, 400, "UnknownOperationException", "Missing or unrecognized X-Amz-Target header: " + amzTarget);
            return;
        }
        String operation = amzTarget.substring(TARGET_PREFIX.length());
        String body = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        JsonObject requestJson;
        try {
            requestJson = body.isBlank() ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
        } catch (RuntimeException e) {
            writeError(response, 400, "SerializationException", "Could not parse request body as JSON: " + e.getMessage());
            return;
        }
        try {
            JsonObject responseJson = handlers.dispatch(operation, requestJson);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(responseJson.toString());
        } catch (DynamoException e) {
            writeError(response, statusForError(e.dynamoErrorType), e.dynamoErrorType, e.getMessage());
        } catch (RuntimeException e) {
            log.error("dynamowire operation {} failed", operation, e);
            writeError(response, 500, "InternalFailure", String.valueOf(e.getMessage()));
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
