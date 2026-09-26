package com.sayonora.warp.sqswire;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sqswire -- Amazon SQS wire-protocol compatibility. Speaks both protocols real SQS does: the JSON
 * protocol ({@code X-Amz-Target: AmazonSQS.<Action>}, {@code application/x-amz-json-1.0}) current AWS
 * SDKs (boto3, SDK v2/v3) use, and the AWS Query protocol (form-encoded {@code Action=...}, XML
 * responses) older SDKs, AWS CLI v1 and Terraform still speak -- see {@link SqsQueryProtocol}. Both share
 * {@link SqsOperations}, backed by {@link PgQueueStore} (a pgmq-style Postgres table per queue,
 * reimplemented in plain SQL so no extension is needed; queues shard by name across the backends of the
 * set, see the store's javadoc).
 *
 * <p>Requests are not signature-verified (any SigV4 credentials are accepted; access is controlled by the
 * connection gate), exactly as before. The region and account id used in ARNs come from
 * {@code WARP_SQSWIRE_REGION} (default {@code us-east-1}) and {@code WARP_SQSWIRE_ACCOUNT_ID} (default
 * {@code 000000000000}). A background sweeper deletes messages older than each queue's retention period
 * ({@code WARP_SQSWIRE_SWEEP_SECONDS}, default 30).
 */
public final class SqsWireServer {

    private static final Logger log = LoggerFactory.getLogger(SqsWireServer.class);
    private static final String TARGET_PREFIX = "AmazonSQS.";

    private final Server server;
    private final PgQueueStore store;
    private final SqsOperations ops;
    private final int port;
    private final com.sayonora.warp.core.SqlMetricsCollector sqlMetrics;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "sqswire-sweeper");
        t.setDaemon(true);
        return t;
    });

    public SqsWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword) {
        this(port, new PgQueueStore(pgHost, pgPort, pgDatabase, pgUser, pgPassword),
                com.sayonora.warp.acl.ConnectionGate.DISABLED, null);
    }

    /**
     * Sharded mode -- {@code backendRegistry.shardGroup()} becomes the set of Postgres backends
     * queue storage splits across, hashed by queue name -- see {@link PgQueueStore}'s javadoc.
     */
    public SqsWireServer(int port, com.sayonora.warp.core.BackendRegistry backendRegistry,
            com.sayonora.warp.acl.ConnectionGate connectionGate, com.sayonora.warp.core.SqlMetricsCollector sqlMetrics) {
        this(port, new PgQueueStore(backendRegistry), connectionGate, sqlMetrics);
    }

    private SqsWireServer(int port, PgQueueStore store, com.sayonora.warp.acl.ConnectionGate connectionGate,
            com.sayonora.warp.core.SqlMetricsCollector sqlMetrics) {
        this.store = store;
        this.sqlMetrics = sqlMetrics;
        this.port = port;
        String region = System.getenv("WARP_SQSWIRE_REGION");
        String account = System.getenv("WARP_SQSWIRE_ACCOUNT_ID");
        store.configureIdentity(region == null || region.isBlank() ? "us-east-1" : region,
                account == null || account.isBlank() ? "000000000000" : account);
        this.ops = new SqsOperations(store);
        // long polls park a request thread for up to 20s each (no database connection is held meanwhile)
        this.server = new Server(new QueuedThreadPool(threadPoolSize()));
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);
        server.setHandler(com.sayonora.warp.ab.AbRouting.wrap("sqs", new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                    throws IOException {
                baseRequest.setHandled(true);
                String requestId = UUID.randomUUID().toString();
                response.setHeader("x-amzn-RequestId", requestId);
                response.setCharacterEncoding("UTF-8");
                if (!connectionGate.acceptHttp(request)) {
                    writeJsonError(response, requestId, 403, "AccessDenied", "AccessDenied", "forbidden");
                    return;
                }
                String body = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (request.getHeader("X-Amz-Target") != null) {
                    handleJson(request, response, body, requestId);
                } else {
                    handleQuery(request, response, body, requestId);
                }
            }
        }, (request, response, bodyBytes) -> connectionGate.acceptHttp(request)
                ? com.sayonora.warp.ab.AbRouting.AuthResult.ok(null)
                : com.sayonora.warp.ab.AbRouting.AuthResult.denied(403, "AccessDenied", "forbidden")));
    }

    private static int threadPoolSize() {
        String v = System.getenv("WARP_SQSWIRE_MAX_THREADS");
        try {
            return v == null || v.isBlank() ? 400 : Math.max(16, Integer.parseInt(v.trim()));
        } catch (NumberFormatException e) {
            return 400;
        }
    }

    private String baseUrl(HttpServletRequest request) {
        String host = request.getHeader("Host");
        if (host == null || host.isBlank()) {
            host = "localhost:" + port;
        }
        return request.getScheme() + "://" + host;
    }

    // ------------------------------------------------------------------------------------------
    // JSON protocol
    // ------------------------------------------------------------------------------------------

    private void handleJson(HttpServletRequest request, HttpServletResponse response, String body, String requestId)
            throws IOException {
        response.setContentType("application/x-amz-json-1.0");
        String amzTarget = request.getHeader("X-Amz-Target");
        if (!amzTarget.startsWith(TARGET_PREFIX)) {
            writeJsonError(response, requestId, 400, "UnknownOperationException", "InvalidAction",
                    "Missing or unrecognized X-Amz-Target header: " + amzTarget);
            return;
        }
        String operation = amzTarget.substring(TARGET_PREFIX.length());
        JsonObject requestJson;
        try {
            requestJson = body.isBlank() ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
        } catch (RuntimeException e) {
            writeJsonError(response, requestId, 400, "SerializationException", "SerializationException",
                    "Could not parse request body as JSON: " + e.getMessage());
            return;
        }
        long start = System.nanoTime();
        String queueName = ops.queueNameOf(requestJson);
        try {
            JsonObject responseJson = ops.dispatch(operation, requestJson, new SqsOperations.Ctx(baseUrl(request)));
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(responseJson.toString());
        } catch (SqsException e) {
            writeJsonError(response, requestId, e.status, e.sqsErrorType, e.queryCode, e.getMessage());
        } catch (SQLException e) {
            log.warn("sqswire: Postgres error servicing {}: {}", operation, e.getMessage());
            writeJsonError(response, requestId, SqsErrorMapper.status(e.getSQLState()),
                    SqsErrorMapper.jsonErrorType(e.getSQLState()), SqsErrorMapper.legacyErrorCode(e.getSQLState()),
                    "Postgres error: " + e.getMessage());
        } catch (RuntimeException e) {
            log.error("sqswire operation {} failed", operation, e);
            writeJsonError(response, requestId, SqsErrorMapper.DEFAULT_STATUS, SqsErrorMapper.DEFAULT_ERROR_TYPE,
                    SqsErrorMapper.DEFAULT_ERROR_TYPE, String.valueOf(e.getMessage()));
        } finally {
            recordMetrics(operation, queueName, start);
        }
    }

    private void writeJsonError(HttpServletResponse response, String requestId, int status, String errorType,
            String queryCode, String message) throws IOException {
        JsonObject err = new JsonObject();
        err.addProperty("__type", "com.amazonaws.sqs#" + errorType);
        err.addProperty("message", message);
        response.setContentType("application/x-amz-json-1.0");
        // awsQueryCompatible: SDKs map this header to the legacy dotted error code (e.g. AWS.SimpleQueueService.NonExistentQueue)
        response.setHeader("x-amzn-query-error", queryCode + (status >= 500 ? ";Receiver" : ";Sender"));
        response.setStatus(status);
        response.getWriter().write(err.toString());
    }

    // ------------------------------------------------------------------------------------------
    // Query protocol
    // ------------------------------------------------------------------------------------------

    private void handleQuery(HttpServletRequest request, HttpServletResponse response, String body, String requestId)
            throws IOException {
        response.setContentType("text/xml;charset=UTF-8");
        Map<String, String> params = new LinkedHashMap<>();
        parseForm(request.getQueryString(), params);
        parseForm(body, params);
        String action = params.get("Action");
        if (action == null) {
            writeQueryError(response, requestId, 400, "MissingAction", "Missing Action parameter");
            return;
        }
        long start = System.nanoTime();
        String queueName = null;
        try {
            JsonObject req = SqsQueryProtocol.toJson(params);
            // Query-protocol SDKs address a queue by POSTing to its URL, often without repeating it as a parameter
            String path = request.getRequestURI();
            if (!req.has("QueueUrl") && !req.has("QueueName") && path != null && path.length() > 1) {
                req.addProperty("QueueUrl", baseUrl(request) + path);
            }
            queueName = ops.queueNameOf(req);
            JsonObject result = ops.dispatch(action, req, new SqsOperations.Ctx(baseUrl(request)));
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(SqsQueryProtocol.renderResponse(action, result, requestId));
        } catch (SqsException e) {
            writeQueryError(response, requestId, e.status, e.queryCode, e.getMessage());
        } catch (SQLException e) {
            log.warn("sqswire (query protocol): Postgres error servicing {}: {}", action, e.getMessage());
            writeQueryError(response, requestId, SqsErrorMapper.status(e.getSQLState()),
                    SqsErrorMapper.legacyErrorCode(e.getSQLState()), "Postgres error: " + e.getMessage());
        } catch (RuntimeException e) {
            log.error("sqswire (query protocol) operation {} failed", action, e);
            writeQueryError(response, requestId, SqsErrorMapper.DEFAULT_STATUS, SqsErrorMapper.DEFAULT_ERROR_TYPE,
                    String.valueOf(e.getMessage()));
        } finally {
            recordMetrics(action, queueName, start);
        }
    }

    private static void parseForm(String source, Map<String, String> into) {
        if (source == null || source.isBlank()) {
            return;
        }
        for (String pair : source.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String key = eq < 0 ? pair : pair.substring(0, eq);
            String value = eq < 0 ? "" : pair.substring(eq + 1);
            into.put(URLDecoder.decode(key, StandardCharsets.UTF_8), URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
    }

    private void writeQueryError(HttpServletResponse response, String requestId, int status, String code, String message)
            throws IOException {
        response.setContentType("text/xml;charset=UTF-8");
        response.setStatus(status);
        response.getWriter().write(SqsQueryProtocol.renderError(code, message, requestId, status < 500));
    }

    // ------------------------------------------------------------------------------------------
    // metrics
    // ------------------------------------------------------------------------------------------

    private void recordMetrics(String operation, String queueName, long startNanos) {
        if (sqlMetrics == null) {
            return;
        }
        var kind = switch (operation) {
            case "ReceiveMessage", "GetQueueAttributes", "ListQueues", "GetQueueUrl", "ListQueueTags",
                    "ListDeadLetterSourceQueues", "ListMessageMoveTasks" ->
                    com.sayonora.warp.core.SqlMetricsCollector.StatementKind.READ;
            case "SendMessage", "SendMessageBatch", "DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility",
                    "ChangeMessageVisibilityBatch", "CreateQueue", "DeleteQueue", "SetQueueAttributes", "PurgeQueue", "TagQueue",
                    "UntagQueue", "AddPermission", "RemovePermission", "StartMessageMoveTask", "CancelMessageMoveTask" ->
                    com.sayonora.warp.core.SqlMetricsCollector.StatementKind.WRITE;
            default -> null;
        };
        // time parked in a long poll is waiting, not work -- exclude it from RTT
        long elapsedNanos = Math.max(0, System.nanoTime() - startNanos - SqsOperations.consumeWaitedNanos());
        if (kind != null) {
            String backendLabel = queueName == null ? "default" : store.resolveBackendFor(queueName);
            // This span covers the full request-to-response-write cycle, so the same duration is valid as
            // both exec time and RTT -- see SqlMetricsCollector's RTT javadoc.
            sqlMetrics.recordOperation("sqswire", backendLabel, kind, operation, elapsedNanos, elapsedNanos);
        }
        // sqswire has no cache layer, so instead of cache_hit/pg_read/pg_write it reports the queue-specific
        // split: SendMessage is the enqueue side, ReceiveMessage the dequeue side.
        String outcome = switch (operation) {
            case "SendMessage", "SendMessageBatch" -> com.sayonora.warp.core.SqlMetricsCollector.OUTCOME_ENQUEUE;
            case "ReceiveMessage" -> com.sayonora.warp.core.SqlMetricsCollector.OUTCOME_DEQUEUE;
            default -> null;
        };
        if (outcome != null) {
            sqlMetrics.recordRttOutcome("sqswire", outcome, elapsedNanos);
        }
    }

    /** The request handler this server's own listener uses, so the unified AWS endpoint can dispatch to it in process. */
    public org.eclipse.jetty.server.Handler handler() {
        return server.getHandler();
    }

    /** The in-process operations, for services (SNS) that deliver into queues without a network hop. */
    public SqsOperations operations() {
        return ops;
    }

    public void start() throws Exception {
        server.start();
        ops.moveTasks().resumeRunning();
        long every = 30;
        String env = System.getenv("WARP_SQSWIRE_SWEEP_SECONDS");
        if (env != null && !env.isBlank()) {
            try {
                every = Math.max(1, Long.parseLong(env.trim()));
            } catch (NumberFormatException ignored) {
                // keep the default
            }
        }
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                long n = store.sweepExpired();
                if (n > 0) {
                    log.info("sqswire: retention sweeper removed {} expired message(s)", n);
                }
            } catch (SQLException | RuntimeException e) {
                log.debug("sqswire: retention sweep failed: {}", e.getMessage());
            }
        }, every, every, TimeUnit.SECONDS);
        log.info("warp sqswire (Amazon SQS, JSON + Query protocols) listening on port {}",
                ((ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        sweeper.shutdownNow();
        ops.moveTasks().shutdown();
        server.stop();
        store.close();
    }
}
