package com.sayonora.wire.sqswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sqswire -- Amazon SQS wire-protocol compatibility. Speaks both protocols real SQS does:
 * the JSON-1.1 protocol (X-Amz-Target: AmazonSQS.&lt;Action&gt;) current AWS SDKs (boto3, SDK
 * v2/v3) use by default, and the legacy query-string/XML protocol ({@code Action=...} form or
 * query parameters, XML responses) older tools and some SQS-compatible clients still speak --
 * see {@link #handleLegacyRequest} for the latter. Both are backed by the same
 * {@link PgQueueStore} -- a pgmq-style (github.com/pgmq/pgmq) Postgres table per queue,
 * reimplemented in plain SQL so no {@code pgmq} extension is required on the backend.
 *
 * <p>Covers CreateQueue/DeleteQueue/GetQueueUrl/ListQueues/SendMessage/ReceiveMessage/
 * DeleteMessage/ChangeMessageVisibility/GetQueueAttributes/SetQueueAttributes -- the core queue
 * lifecycle, FIFO queues (message-group ordering + content dedup), and dead-letter/redrive
 * policies. See {@link PgQueueStore}'s javadoc for how each of those is implemented.
 */
public final class SqsWireServer {

    private static final Logger log = LoggerFactory.getLogger(SqsWireServer.class);
    private static final String TARGET_PREFIX = "AmazonSQS.";

    private final Server server;
    private final PgQueueStore store;
    private final String queueUrlBase;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;

    public SqsWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword) {
        this(port, new PgQueueStore(pgHost, pgPort, pgDatabase, pgUser, pgPassword),
                com.sayonora.wire.acl.ConnectionGate.DISABLED, null);
    }

    /**
     * Sharded mode -- {@code backendRegistry.shardGroup()} becomes the set of Postgres backends
     * queue storage splits across, hashed by queue name -- see {@link PgQueueStore}'s javadoc.
     */
    public SqsWireServer(int port, com.sayonora.wire.core.BackendRegistry backendRegistry,
            com.sayonora.wire.acl.ConnectionGate connectionGate, com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
        this(port, new PgQueueStore(backendRegistry), connectionGate, sqlMetrics);
    }

    private SqsWireServer(int port, PgQueueStore store, com.sayonora.wire.acl.ConnectionGate connectionGate,
            com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = store;
        this.sqlMetrics = sqlMetrics;
        this.queueUrlBase = "http://localhost:" + port + "/queue";
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                    throws IOException {
                baseRequest.setHandled(true);
                if (!connectionGate.acceptHttp(request)) {
                    writeError(response, 403, "AccessDenied", "forbidden");
                    return;
                }
                String body = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (request.getHeader("X-Amz-Target") != null) {
                    handleRequest(request, response, body);
                } else {
                    handleLegacyRequest(request, response, body);
                }
            }
        });
    }

    private void handleRequest(HttpServletRequest request, HttpServletResponse response, String body) throws IOException {
        response.setContentType("application/x-amz-json-1.1");
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
            writeError(response, 400, "InvalidAction", "Could not parse request body as JSON: " + e.getMessage());
            return;
        }
        long start = System.nanoTime();
        String queueName = queueNameFromRequest(requestJson);
        try {
            JsonObject responseJson = dispatch(operation, requestJson);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(responseJson.toString());
        } catch (SqsException e) {
            writeError(response, e.status, e.sqsErrorType, e.getMessage());
        } catch (SQLException e) {
            log.warn("sqswire: Postgres error servicing {}: {}", operation, e.getMessage());
            writeError(response, SqsErrorMapper.status(e.getSQLState()),
                    SqsErrorMapper.jsonErrorType(e.getSQLState()), "Postgres error: " + e.getMessage());
        } catch (RuntimeException e) {
            log.error("sqswire operation {} failed", operation, e);
            writeError(response, SqsErrorMapper.DEFAULT_STATUS, SqsErrorMapper.DEFAULT_ERROR_TYPE,
                    String.valueOf(e.getMessage()));
        } finally {
            if (sqlMetrics != null) {
                var kind = switch (operation) {
                    case "ReceiveMessage", "GetQueueAttributes", "ListQueues", "GetQueueUrl" ->
                            com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ;
                    case "SendMessage", "DeleteMessage", "ChangeMessageVisibility", "CreateQueue", "DeleteQueue",
                            "SetQueueAttributes" ->
                            com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE;
                    default -> null;
                };
                if (kind != null) {
                    String backendLabel = queueName == null ? "default" : store.resolveBackendFor(queueName);
                    // This span already covers the full request-to-response-write cycle (the
                    // response.getWriter().write(...) above runs inside the timed try block), so
                    // the same duration is valid as both exec time and RTT -- see
                    // SqlMetricsCollector's RTT javadoc.
                    long elapsedNanos = System.nanoTime() - start;
                    sqlMetrics.recordOperation("sqswire", backendLabel, kind, operation, elapsedNanos, elapsedNanos);
                }
                recordEnqueueDequeue(operation, System.nanoTime() - start);
            }
        }
    }

    /**
     * sqswire has no cache layer (see SqlMetricsCollector.OUTCOME_ENQUEUE/OUTCOME_DEQUEUE's
     * javadoc), so instead of the cache_hit/pg_read/pg_write breakdown the other protocols get,
     * it reports the queue-specific split the caller asked for: SendMessage is the enqueue side,
     * ReceiveMessage is the dequeue side. Everything else (DeleteMessage, queue admin ops, etc.)
     * is intentionally left out of this breakdown -- it's neither an enqueue nor a dequeue, it's
     * still tracked via the plain READ/WRITE kind above.
     */
    private void recordEnqueueDequeue(String operation, long elapsedNanos) {
        if (sqlMetrics == null) {
            return;
        }
        String outcome = switch (operation) {
            case "SendMessage" -> com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_ENQUEUE;
            case "ReceiveMessage" -> com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_DEQUEUE;
            default -> null;
        };
        if (outcome != null) {
            sqlMetrics.recordRttOutcome("sqswire", outcome, elapsedNanos);
        }
    }

    private JsonObject dispatch(String operation, JsonObject req) throws SQLException {
        return switch (operation) {
            case "CreateQueue" -> createQueue(req);
            case "DeleteQueue" -> deleteQueue(req);
            case "GetQueueUrl" -> getQueueUrl(req);
            case "ListQueues" -> listQueues();
            case "SendMessage" -> sendMessage(req);
            case "ReceiveMessage" -> receiveMessage(req);
            case "DeleteMessage" -> deleteMessage(req);
            case "ChangeMessageVisibility" -> changeMessageVisibility(req);
            case "GetQueueAttributes" -> getQueueAttributes(req);
            case "SetQueueAttributes" -> setQueueAttributes(req);
            default -> throw new SqsException(400, "UnknownOperationException", "sqswire does not implement " + operation);
        };
    }

    /** Every action's request carries either a queue name or a queue URL ending in it. */
    private String queueNameFromRequest(JsonObject req) {
        if (req.has("QueueName")) {
            return req.get("QueueName").getAsString();
        }
        if (req.has("QueueUrl")) {
            String url = req.get("QueueUrl").getAsString();
            int slash = url.lastIndexOf('/');
            return slash < 0 ? url : url.substring(slash + 1);
        }
        return null;
    }

    private String requireQueueName(JsonObject req) {
        String name = queueNameFromRequest(req);
        if (name == null) {
            throw new SqsException(400, "MissingParameter", "QueueName or QueueUrl is required");
        }
        return name;
    }

    private JsonObject createQueue(JsonObject req) throws SQLException {
        String name = req.has("QueueName") ? req.get("QueueName").getAsString()
                : throwMissing("QueueName");
        store.createQueue(name, attributesFromRequest(req));
        JsonObject resp = new JsonObject();
        resp.addProperty("QueueUrl", queueUrlBase + "/" + name);
        return resp;
    }

    private JsonObject setQueueAttributes(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes current = store.queueAttributes(name);
        store.setQueueAttributes(name, mergeAttributes(current, req));
        return new JsonObject();
    }

    /**
     * Parses the {@code Attributes} map both CreateQueue and SetQueueAttributes send:
     * {@code VisibilityTimeout}, {@code FifoQueue} ("true"/"false"), and {@code RedrivePolicy}
     * (a JSON string -- real SQS -- holding {@code deadLetterTargetArn} and
     * {@code maxReceiveCount}). No real ARNs/accounts are emulated, so the DLQ target is read as
     * whatever queue name follows the ARN's last {@code :} or {@code /} -- a caller can also just
     * pass the plain queue name there directly.
     */
    private PgQueueStore.QueueAttributes attributesFromRequest(JsonObject req) {
        return mergeAttributes(PgQueueStore.QueueAttributes.DEFAULTS, req);
    }

    private PgQueueStore.QueueAttributes mergeAttributes(PgQueueStore.QueueAttributes base, JsonObject req) {
        if (!req.has("Attributes") || !req.get("Attributes").isJsonObject()) {
            return base;
        }
        JsonObject attrs = req.getAsJsonObject("Attributes");
        int visibilityTimeout = attrs.has("VisibilityTimeout")
                ? Integer.parseInt(attrs.get("VisibilityTimeout").getAsString()) : base.visibilityTimeout();
        boolean fifo = attrs.has("FifoQueue")
                ? Boolean.parseBoolean(attrs.get("FifoQueue").getAsString()) : base.fifo();
        String dlqName = base.dlqQueueName();
        Integer maxReceiveCount = base.maxReceiveCount();
        if (attrs.has("RedrivePolicy")) {
            try {
                JsonObject redrive = JsonParser.parseString(attrs.get("RedrivePolicy").getAsString()).getAsJsonObject();
                if (redrive.has("deadLetterTargetArn")) {
                    String arn = redrive.get("deadLetterTargetArn").getAsString();
                    int cut = Math.max(arn.lastIndexOf(':'), arn.lastIndexOf('/'));
                    dlqName = cut < 0 ? arn : arn.substring(cut + 1);
                }
                if (redrive.has("maxReceiveCount")) {
                    maxReceiveCount = redrive.get("maxReceiveCount").getAsInt();
                }
            } catch (RuntimeException e) {
                throw new SqsException(400, "InvalidAttributeValue", "RedrivePolicy is not valid JSON: " + e.getMessage());
            }
        }
        return new PgQueueStore.QueueAttributes(visibilityTimeout, fifo, dlqName, maxReceiveCount);
    }

    private JsonObject deleteQueue(JsonObject req) throws SQLException {
        store.deleteQueue(requireQueueName(req));
        return new JsonObject();
    }

    private JsonObject getQueueUrl(JsonObject req) {
        String name = req.has("QueueName") ? req.get("QueueName").getAsString() : throwMissing("QueueName");
        JsonObject resp = new JsonObject();
        resp.addProperty("QueueUrl", queueUrlBase + "/" + name);
        return resp;
    }

    private JsonObject listQueues() throws SQLException {
        List<String> names = store.listQueues();
        JsonArray urls = new JsonArray();
        for (String name : names) {
            urls.add(queueUrlBase + "/" + name);
        }
        JsonObject resp = new JsonObject();
        resp.add("QueueUrls", urls);
        return resp;
    }

    private JsonObject sendMessage(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        String body = req.has("MessageBody") ? req.get("MessageBody").getAsString() : throwMissing("MessageBody");
        String groupId = req.has("MessageGroupId") ? req.get("MessageGroupId").getAsString() : null;
        String dedupId = req.has("MessageDeduplicationId") ? req.get("MessageDeduplicationId").getAsString() : null;
        long msgId = store.sendMessage(name, body, groupId, dedupId);
        JsonObject resp = new JsonObject();
        resp.addProperty("MessageId", String.valueOf(msgId));
        resp.addProperty("MD5OfMessageBody", md5Hex(body));
        return resp;
    }

    private JsonObject receiveMessage(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        int maxMessages = req.has("MaxNumberOfMessages") ? req.get("MaxNumberOfMessages").getAsInt() : 1;
        Integer visibilityTimeout = req.has("VisibilityTimeout") ? req.get("VisibilityTimeout").getAsInt() : null;
        List<PgQueueStore.ReceivedMessage> messages = store.receiveMessages(name, maxMessages, visibilityTimeout);
        JsonArray arr = new JsonArray();
        for (PgQueueStore.ReceivedMessage m : messages) {
            JsonObject msgJson = new JsonObject();
            msgJson.addProperty("MessageId", String.valueOf(m.msgId()));
            msgJson.addProperty("ReceiptHandle", m.receiptHandle());
            msgJson.addProperty("Body", m.body());
            msgJson.addProperty("MD5OfBody", md5Hex(m.body()));
            JsonObject attrs = new JsonObject();
            attrs.addProperty("ApproximateReceiveCount", String.valueOf(m.readCt()));
            msgJson.add("Attributes", attrs);
            arr.add(msgJson);
        }
        JsonObject resp = new JsonObject();
        resp.add("Messages", arr);
        return resp;
    }

    private JsonObject deleteMessage(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        String receiptHandle = req.has("ReceiptHandle") ? req.get("ReceiptHandle").getAsString() : throwMissing("ReceiptHandle");
        boolean deleted = store.deleteMessage(name, receiptHandle);
        if (!deleted) {
            throw new SqsException(400, "ReceiptHandleIsInvalid", "The receipt handle is invalid or the message no longer exists");
        }
        return new JsonObject();
    }

    private JsonObject changeMessageVisibility(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        String receiptHandle = req.has("ReceiptHandle") ? req.get("ReceiptHandle").getAsString() : throwMissing("ReceiptHandle");
        int visibilityTimeout = req.has("VisibilityTimeout") ? req.get("VisibilityTimeout").getAsInt() : 30;
        boolean changed = store.changeMessageVisibility(name, receiptHandle, visibilityTimeout);
        if (!changed) {
            throw new SqsException(400, "ReceiptHandleIsInvalid", "The receipt handle is invalid or the message no longer exists");
        }
        return new JsonObject();
    }

    private JsonObject getQueueAttributes(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueCounts counts = store.countMessages(name);
        PgQueueStore.QueueAttributes queueAttrs = store.queueAttributes(name);
        JsonObject attrs = new JsonObject();
        attrs.addProperty("ApproximateNumberOfMessages", String.valueOf(counts.visible()));
        attrs.addProperty("ApproximateNumberOfMessagesNotVisible", String.valueOf(counts.inFlight()));
        attrs.addProperty("VisibilityTimeout", String.valueOf(queueAttrs.visibilityTimeout()));
        attrs.addProperty("FifoQueue", String.valueOf(queueAttrs.fifo()));
        if (queueAttrs.dlqQueueName() != null && queueAttrs.maxReceiveCount() != null) {
            JsonObject redrive = new JsonObject();
            redrive.addProperty("deadLetterTargetArn", queueAttrs.dlqQueueName());
            redrive.addProperty("maxReceiveCount", queueAttrs.maxReceiveCount());
            attrs.addProperty("RedrivePolicy", redrive.toString());
        }
        JsonObject resp = new JsonObject();
        resp.add("Attributes", attrs);
        return resp;
    }

    private static String throwMissing(String param) {
        throw new SqsException(400, "MissingParameter", param + " is required");
    }

    private static String md5Hex(String s) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void writeError(HttpServletResponse response, int status, String errorType, String message) throws IOException {
        JsonObject err = new JsonObject();
        err.addProperty("__type", "com.amazonaws.sqs#" + errorType);
        err.addProperty("message", message);
        response.setStatus(status);
        response.getWriter().write(err.toString());
    }

    /**
     * The legacy query-string protocol: {@code Action=<Action>&Param=Value&...}, form-encoded in
     * the POST body (what old SQS SDKs/tools send) or as query parameters on a GET. Translated
     * into the same JSON shape {@link #dispatch} already accepts -- including
     * {@code Attribute.<N>.Name}/{@code Attribute.<N>.Value} pairs (how this protocol expresses
     * CreateQueue/SetQueueAttributes' {@code Attributes} map) -- so every action shares one real
     * implementation; only request parsing and response rendering differ per protocol.
     */
    private void handleLegacyRequest(HttpServletRequest request, HttpServletResponse response, String body) throws IOException {
        response.setContentType("text/xml;charset=UTF-8");
        Map<String, String> params = parseFormParams(request, body);
        String action = params.get("Action");
        if (action == null) {
            writeLegacyError(response, 400, "MissingAction", "Missing Action parameter");
            return;
        }
        JsonObject req = legacyParamsToJson(params);
        long start = System.nanoTime();
        String queueName = queueNameFromRequest(req);
        try {
            JsonObject resultJson = dispatch(action, req);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(renderLegacyResponse(action, resultJson));
        } catch (SqsException e) {
            writeLegacyError(response, e.status, e.sqsErrorType, e.getMessage());
        } catch (SQLException e) {
            log.warn("sqswire (legacy protocol): Postgres error servicing {}: {}", action, e.getMessage());
            writeLegacyError(response, SqsErrorMapper.status(e.getSQLState()),
                    SqsErrorMapper.legacyErrorCode(e.getSQLState()), "Postgres error: " + e.getMessage());
        } catch (RuntimeException e) {
            log.error("sqswire (legacy protocol) operation {} failed", action, e);
            writeLegacyError(response, SqsErrorMapper.DEFAULT_STATUS, SqsErrorMapper.DEFAULT_ERROR_TYPE,
                    String.valueOf(e.getMessage()));
        } finally {
            if (sqlMetrics != null) {
                var kind = switch (action) {
                    case "ReceiveMessage", "GetQueueAttributes", "ListQueues", "GetQueueUrl" ->
                            com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ;
                    case "SendMessage", "DeleteMessage", "ChangeMessageVisibility", "CreateQueue", "DeleteQueue",
                            "SetQueueAttributes" -> com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE;
                    default -> null;
                };
                if (kind != null) {
                    String backendLabel = queueName == null ? "default" : store.resolveBackendFor(queueName);
                    long elapsedNanos = System.nanoTime() - start;
                    sqlMetrics.recordOperation("sqswire", backendLabel, kind, action, elapsedNanos, elapsedNanos);
                }
                recordEnqueueDequeue(action, System.nanoTime() - start);
            }
        }
    }

    private Map<String, String> parseFormParams(HttpServletRequest request, String body) {
        String query = request.getQueryString();
        String source = (body != null && !body.isBlank()) ? body : query;
        Map<String, String> params = new LinkedHashMap<>();
        if (source == null || source.isBlank()) {
            return params;
        }
        for (String pair : source.split("&")) {
            int eq = pair.indexOf('=');
            String key = eq < 0 ? pair : pair.substring(0, eq);
            String value = eq < 0 ? "" : pair.substring(eq + 1);
            params.put(urlDecode(key), urlDecode(value));
        }
        return params;
    }

    private static String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return s;
        }
    }

    private JsonObject legacyParamsToJson(Map<String, String> params) {
        JsonObject req = new JsonObject();
        for (String simple : List.of("QueueName", "QueueUrl", "MessageBody", "ReceiptHandle",
                "MessageGroupId", "MessageDeduplicationId")) {
            if (params.containsKey(simple)) {
                req.addProperty(simple, params.get(simple));
            }
        }
        if (params.containsKey("MaxNumberOfMessages")) {
            req.addProperty("MaxNumberOfMessages", Integer.parseInt(params.get("MaxNumberOfMessages")));
        }
        if (params.containsKey("VisibilityTimeout")) {
            req.addProperty("VisibilityTimeout", Integer.parseInt(params.get("VisibilityTimeout")));
        }
        // Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=30&Attribute.2.Name=... -> {"Attributes": {...}}
        JsonObject attributes = new JsonObject();
        for (int i = 1; params.containsKey("Attribute." + i + ".Name"); i++) {
            attributes.addProperty(params.get("Attribute." + i + ".Name"), params.get("Attribute." + i + ".Value"));
        }
        if (attributes.size() > 0) {
            req.add("Attributes", attributes);
        }
        return req;
    }

    private String renderLegacyResponse(String action, JsonObject result) {
        String requestId = UUID.randomUUID().toString();
        StringBuilder body = new StringBuilder();
        body.append("<").append(action).append("Response>");
        switch (action) {
            case "CreateQueue", "GetQueueUrl" -> {
                body.append("<").append(action).append("Result>");
                appendTag(body, "QueueUrl", result.get("QueueUrl"));
                body.append("</").append(action).append("Result>");
            }
            case "ListQueues" -> {
                body.append("<ListQueuesResult>");
                if (result.has("QueueUrls")) {
                    for (JsonElement url : result.getAsJsonArray("QueueUrls")) {
                        appendTag(body, "QueueUrl", url);
                    }
                }
                body.append("</ListQueuesResult>");
            }
            case "SendMessage" -> {
                body.append("<SendMessageResult>");
                appendTag(body, "MD5OfMessageBody", result.get("MD5OfMessageBody"));
                appendTag(body, "MessageId", result.get("MessageId"));
                body.append("</SendMessageResult>");
            }
            case "ReceiveMessage" -> {
                body.append("<ReceiveMessageResult>");
                if (result.has("Messages")) {
                    for (JsonElement m : result.getAsJsonArray("Messages")) {
                        JsonObject msg = m.getAsJsonObject();
                        body.append("<Message>");
                        appendTag(body, "MessageId", msg.get("MessageId"));
                        appendTag(body, "ReceiptHandle", msg.get("ReceiptHandle"));
                        appendTag(body, "MD5OfBody", msg.get("MD5OfBody"));
                        appendTag(body, "Body", msg.get("Body"));
                        if (msg.has("Attributes")) {
                            for (var entry : msg.getAsJsonObject("Attributes").entrySet()) {
                                body.append("<Attribute><Name>").append(escapeXml(entry.getKey())).append("</Name><Value>")
                                        .append(escapeXml(entry.getValue().getAsString())).append("</Value></Attribute>");
                            }
                        }
                        body.append("</Message>");
                    }
                }
                body.append("</ReceiveMessageResult>");
            }
            case "GetQueueAttributes" -> {
                body.append("<GetQueueAttributesResult>");
                if (result.has("Attributes")) {
                    for (var entry : result.getAsJsonObject("Attributes").entrySet()) {
                        body.append("<Attribute><Name>").append(escapeXml(entry.getKey())).append("</Name><Value>")
                                .append(escapeXml(entry.getValue().getAsString())).append("</Value></Attribute>");
                    }
                }
                body.append("</GetQueueAttributesResult>");
            }
            default -> { /* DeleteQueue/DeleteMessage/ChangeMessageVisibility/SetQueueAttributes have no *Result payload */ }
        }
        body.append("<ResponseMetadata><RequestId>").append(requestId).append("</RequestId></ResponseMetadata>");
        body.append("</").append(action).append("Response>");
        return body.toString();
    }

    private static void appendTag(StringBuilder body, String tag, JsonElement value) {
        if (value != null) {
            body.append("<").append(tag).append(">").append(escapeXml(value.getAsString())).append("</").append(tag).append(">");
        }
    }

    private static String escapeXml(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private void writeLegacyError(HttpServletResponse response, int status, String errorType, String message) throws IOException {
        String requestId = UUID.randomUUID().toString();
        String xml = "<ErrorResponse><Error><Type>Sender</Type><Code>" + escapeXml(errorType) + "</Code><Message>"
                + escapeXml(message) + "</Message></Error><RequestId>" + requestId + "</RequestId></ErrorResponse>";
        response.setStatus(status);
        response.getWriter().write(xml);
    }

    public void start() throws Exception {
        server.start();
        log.info("warp sqswire (Amazon SQS HTTP/JSON) listening on port {}",
                ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
        store.close();
    }
}
