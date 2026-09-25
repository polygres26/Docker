// Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors.
// See Warp/NOTICE for the licence text.
package com.sayonora.wire.sqswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * The SQS operations, protocol independent: every action takes and returns the JSON-protocol shaped
 * request/response object (the Query protocol front-end converts to and from that shape), so the JSON
 * and Query/XML frontends share one implementation. Validation rules, limits and error codes follow the
 * real SQS service; parts of the validation wording follow Floci's SqsService
 * (https://github.com/floci-io/floci, MIT License).
 *
 * <p>Long polling never holds a database connection while waiting: {@link #receiveMessage} performs a
 * short claim attempt, releases the connection, waits on an in-process wake-up (or a short poll timer, to
 * notice visibility-timeout expiry and sends from other Warp nodes) and tries again.
 */
public final class SqsOperations {

    static final int MAX_BATCH = 10;
    static final int MAX_BATCH_BYTES = 262144;
    private static final long POLL_INTERVAL_MS = 500;
    private static final Pattern QUEUE_NAME = Pattern.compile("[A-Za-z0-9_-]{1,80}");
    private static final Pattern FIFO_QUEUE_NAME = Pattern.compile("[A-Za-z0-9_-]{1,75}\\.fifo");
    private static final Pattern BATCH_ID = Pattern.compile("[A-Za-z0-9_-]{1,80}");
    private static final Pattern ID_CHARS = Pattern.compile("[A-Za-z0-9!\"#$%&'()*+,\\-./:;<=>?@\\[\\\\\\]^_`{|}~]{1,128}");
    private static final Pattern LABEL = Pattern.compile("[A-Za-z0-9_-]{1,80}");

    static final Set<String> SETTABLE = Set.of("VisibilityTimeout", "DelaySeconds", "MaximumMessageSize",
            "MessageRetentionPeriod", "ReceiveMessageWaitTimeSeconds", "Policy", "RedrivePolicy", "RedriveAllowPolicy",
            "FifoQueue", "ContentBasedDeduplication", "DeduplicationScope", "FifoThroughputLimit", "KmsMasterKeyId",
            "KmsDataKeyReusePeriodSeconds", "SqsManagedSseEnabled");
    static final Set<String> FIFO_ONLY = Set.of("ContentBasedDeduplication", "DeduplicationScope", "FifoThroughputLimit");
    static final List<String> ALL_ATTRIBUTES = List.of("QueueArn", "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed", "CreatedTimestamp",
            "LastModifiedTimestamp", "VisibilityTimeout", "MaximumMessageSize", "MessageRetentionPeriod", "DelaySeconds",
            "ReceiveMessageWaitTimeSeconds", "Policy", "RedrivePolicy", "RedriveAllowPolicy", "FifoQueue",
            "ContentBasedDeduplication", "DeduplicationScope", "FifoThroughputLimit", "SqsManagedSseEnabled",
            "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds");
    static final Set<String> SYSTEM_ATTRIBUTES = Set.of("All", "SenderId", "SentTimestamp", "ApproximateReceiveCount",
            "ApproximateFirstReceiveTimestamp", "SequenceNumber", "MessageDeduplicationId", "MessageGroupId",
            "AWSTraceHeader", "DeadLetterQueueSourceArn");

    private static final ThreadLocal<long[]> WAITED_NANOS = ThreadLocal.withInitial(() -> new long[1]);

    /** Nanoseconds this thread spent parked in long polls since the last call (not a database cost); resets. */
    static long consumeWaitedNanos() {
        long[] w = WAITED_NANOS.get();
        long v = w[0];
        w[0] = 0;
        return v;
    }

    /** Per-request context: the externally visible base URL used to build queue URLs. */
    public record Ctx(String baseUrl) {
    }

    /** Per-queue wake-up counters so a parked long poll notices a send without waiting for its poll timer. */
    static final class QueueWaiters {
        private final ConcurrentHashMap<String, Slot> slots = new ConcurrentHashMap<>();

        private static final class Slot {
            final AtomicLong version = new AtomicLong();
        }

        long version(String queue) {
            return slots.computeIfAbsent(queue, k -> new Slot()).version.get();
        }

        void signal(String queue) {
            Slot s = slots.get(queue);
            if (s != null) {
                s.version.incrementAndGet();
                synchronized (s) {
                    s.notifyAll();
                }
            }
        }

        void await(String queue, long seenVersion, long maxMs) throws InterruptedException {
            Slot s = slots.computeIfAbsent(queue, k -> new Slot());
            synchronized (s) {
                if (s.version.get() == seenVersion) {
                    s.wait(maxMs);
                }
            }
        }
    }

    private final PgQueueStore store;
    private final QueueWaiters waiters = new QueueWaiters();
    private final SqsMoveTasks moveTasks;

    public SqsOperations(PgQueueStore store) {
        this.store = store;
        // the waiters map only grows for queues that see a long poll; a signal for an untracked queue is a no-op
        store.setEnqueueListener(waiters::signal);
        this.moveTasks = new SqsMoveTasks(store);
    }

    SqsMoveTasks moveTasks() {
        return moveTasks;
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    /** The queue name inside a queue URL (last path segment). */
    static String queueNameFromUrl(String url) {
        String u = url;
        int q = u.indexOf('?');
        if (q >= 0) {
            u = u.substring(0, q);
        }
        while (u.endsWith("/")) {
            u = u.substring(0, u.length() - 1);
        }
        int slash = u.lastIndexOf('/');
        return slash < 0 ? u : u.substring(slash + 1);
    }

    /** The queue an operation targets, from its {@code QueueUrl} (or {@code QueueName}); {@code null} if none. */
    public String queueNameOf(JsonObject req) {
        if (req.has("QueueName") && !req.get("QueueName").isJsonNull()) {
            return req.get("QueueName").getAsString();
        }
        if (req.has("QueueUrl") && !req.get("QueueUrl").isJsonNull()) {
            return queueNameFromUrl(req.get("QueueUrl").getAsString());
        }
        return null;
    }

    private String requireQueueName(JsonObject req) {
        String name = queueNameOf(req);
        if (name == null || name.isEmpty()) {
            throw SqsException.missing("QueueUrl");
        }
        return name;
    }

    private String queueUrl(Ctx ctx, String name) {
        return ctx.baseUrl() + "/" + store.accountId() + "/" + name;
    }

    private static String str(JsonObject o, String key) {
        return o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsString() : null;
    }

    private static Integer integer(JsonObject o, String key) {
        if (!o.has(key) || o.get(key).isJsonNull()) {
            return null;
        }
        try {
            return o.get(key).getAsBigDecimal().intValueExact();
        } catch (RuntimeException e) {
            throw SqsException.invalidParam("Value " + o.get(key).getAsString() + " for parameter " + key
                    + " is invalid. Reason: Must be an integer.");
        }
    }

    private static List<String> stringList(JsonObject o, String key) {
        List<String> out = new ArrayList<>();
        if (o.has(key) && o.get(key).isJsonArray()) {
            for (JsonElement e : o.getAsJsonArray(key)) {
                out.add(e.getAsString());
            }
        }
        return out;
    }

    private static JsonObject object(JsonObject o, String key) {
        return o.has(key) && o.get(key).isJsonObject() ? o.getAsJsonObject(key) : null;
    }

    private static void requireRange(String param, Integer v, int min, int max) {
        if (v != null && (v < min || v > max)) {
            throw SqsException.invalidParam("Value " + v + " for parameter " + param + " is invalid. Reason: Must be between "
                    + min + " and " + max + ", if provided.");
        }
    }

    // ------------------------------------------------------------------------------------------
    // dispatch
    // ------------------------------------------------------------------------------------------

    public JsonObject dispatch(String operation, JsonObject req, Ctx ctx) throws SQLException {
        return switch (operation) {
            case "CreateQueue" -> createQueue(req, ctx);
            case "DeleteQueue" -> deleteQueue(req);
            case "GetQueueUrl" -> getQueueUrl(req, ctx);
            case "ListQueues" -> listQueues(req, ctx);
            case "GetQueueAttributes" -> getQueueAttributes(req);
            case "SetQueueAttributes" -> setQueueAttributes(req);
            case "SendMessage" -> sendMessage(req);
            case "SendMessageBatch" -> sendMessageBatch(req);
            case "ReceiveMessage" -> receiveMessage(req);
            case "DeleteMessage" -> deleteMessage(req);
            case "DeleteMessageBatch" -> deleteMessageBatch(req);
            case "ChangeMessageVisibility" -> changeMessageVisibility(req);
            case "ChangeMessageVisibilityBatch" -> changeMessageVisibilityBatch(req);
            case "PurgeQueue" -> purgeQueue(req);
            case "TagQueue" -> tagQueue(req);
            case "UntagQueue" -> untagQueue(req);
            case "ListQueueTags" -> listQueueTags(req);
            case "AddPermission" -> addPermission(req);
            case "RemovePermission" -> removePermission(req);
            case "ListDeadLetterSourceQueues" -> listDeadLetterSourceQueues(req, ctx);
            case "StartMessageMoveTask" -> startMessageMoveTask(req);
            case "CancelMessageMoveTask" -> cancelMessageMoveTask(req);
            case "ListMessageMoveTasks" -> listMessageMoveTasks(req);
            default -> throw new SqsException(400, "UnknownOperationException", "InvalidAction",
                    "The action or operation requested is invalid: " + operation);
        };
    }

    // ------------------------------------------------------------------------------------------
    // queue lifecycle
    // ------------------------------------------------------------------------------------------

    private JsonObject createQueue(JsonObject req, Ctx ctx) throws SQLException {
        String name = str(req, "QueueName");
        if (name == null || name.isEmpty()) {
            throw SqsException.missing("QueueName");
        }
        JsonObject rawAttrs = object(req, "Attributes");
        Map<String, String> requested = new LinkedHashMap<>();
        if (rawAttrs != null) {
            for (var e : rawAttrs.entrySet()) {
                requested.put(e.getKey(), e.getValue().getAsString());
            }
        }
        boolean fifo = "true".equalsIgnoreCase(requested.get("FifoQueue"));
        if (requested.containsKey("FifoQueue") && !requested.get("FifoQueue").equalsIgnoreCase("true")
                && !requested.get("FifoQueue").equalsIgnoreCase("false")) {
            throw new SqsException(400, "InvalidAttributeValue", "Invalid value for the parameter FifoQueue.");
        }
        if (fifo ? !FIFO_QUEUE_NAME.matcher(name).matches() : !QUEUE_NAME.matcher(name).matches()) {
            throw SqsException.invalidParam(fifo
                    ? "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with "
                            + ".fifo suffix and be 1 to 80 in length"
                    : "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length");
        }
        Map<String, String> tags = new LinkedHashMap<>();
        JsonObject rawTags = object(req, "tags") != null ? object(req, "tags") : object(req, "Tags");
        if (rawTags != null) {
            for (var e : rawTags.entrySet()) {
                tags.put(e.getKey(), e.getValue().getAsString());
            }
            validateTags(tags);
        }
        Map<String, String> validated = validateAttributes(requested, fifo, true);
        PgQueueStore.QueueAttributes existing = store.findQueueFresh(name);
        if (existing != null) {
            compareWithExisting(existing, validated);
            return urlResponse(queueUrl(ctx, name));
        }
        Map<String, String> stored = new LinkedHashMap<>(validated);
        stored.remove("FifoQueue");
        stored.putIfAbsent("VisibilityTimeout", "30");
        PgQueueStore.QueueAttributes attrs = build(stored, tags, fifo);
        if (!store.createQueueIfAbsent(name, attrs)) {
            existing = store.findQueueFresh(name);
            if (existing != null) {
                compareWithExisting(existing, validated);
            }
        }
        return urlResponse(queueUrl(ctx, name));
    }

    /** Builds the catalog record from a validated attribute map, deriving the redrive columns from RedrivePolicy. */
    private PgQueueStore.QueueAttributes build(Map<String, String> attrs, Map<String, String> tags, boolean fifo)
            throws SQLException {
        String dlq = null;
        Integer max = null;
        String redrive = attrs.get("RedrivePolicy");
        if (redrive != null && !redrive.isEmpty()) {
            JsonObject p = JsonParser.parseString(redrive).getAsJsonObject();
            dlq = PgQueueStore.queueNameFromArn(p.get("deadLetterTargetArn").getAsString());
            max = p.get("maxReceiveCount").getAsBigDecimal().intValue();
        }
        int vt = Integer.parseInt(attrs.getOrDefault("VisibilityTimeout", "30"));
        return new PgQueueStore.QueueAttributes(vt, fifo, dlq, max, attrs, tags, 0L, 0L, null);
    }

    private void compareWithExisting(PgQueueStore.QueueAttributes existing, Map<String, String> requested) {
        Map<String, String> effective = effectiveAttributes(existing);
        for (var e : requested.entrySet()) {
            String have = effective.get(e.getKey());
            if (e.getKey().equals("FifoQueue")) {
                have = existing.fifo() ? "true" : null;
                if (have == null && e.getValue().equals("false")) {
                    continue;
                }
            }
            if (have == null || !have.equals(e.getValue())) {
                throw new SqsException(400, "QueueNameExists",
                        "A queue already exists with the same name and a different value for attribute " + e.getKey());
            }
        }
    }

    private JsonObject deleteQueue(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        store.requireQueue(name);
        store.deleteQueue(name);
        waiters.signal(name);
        return new JsonObject();
    }

    private JsonObject getQueueUrl(JsonObject req, Ctx ctx) throws SQLException {
        String name = str(req, "QueueName");
        if (name == null || name.isEmpty()) {
            throw SqsException.missing("QueueName");
        }
        if (store.findQueue(name) == null) {
            throw SqsException.noQueue();
        }
        return urlResponse(queueUrl(ctx, name));
    }

    private static JsonObject urlResponse(String url) {
        JsonObject resp = new JsonObject();
        resp.addProperty("QueueUrl", url);
        return resp;
    }

    private JsonObject listQueues(JsonObject req, Ctx ctx) throws SQLException {
        String prefix = str(req, "QueueNamePrefix");
        Integer maxResults = integer(req, "MaxResults");
        requireRange("MaxResults", maxResults, 1, 1000);
        String token = str(req, "NextToken");
        String after = decodeToken(token);
        int limit = maxResults != null ? maxResults : 1000;
        List<String> names = store.listQueues(prefix, limit + 1, after);
        JsonObject resp = new JsonObject();
        boolean more = names.size() > limit;
        if (more) {
            names = names.subList(0, limit);
        }
        JsonArray urls = new JsonArray();
        for (String n : names) {
            urls.add(queueUrl(ctx, n));
        }
        if (urls.size() > 0) {
            resp.add("QueueUrls", urls);
        }
        if (more) {
            resp.addProperty("NextToken", encodeToken(names.get(names.size() - 1)));
        }
        return resp;
    }

    private static String encodeToken(String lastName) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(lastName.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeToken(String token) {
        if (token == null || token.isEmpty()) {
            return null;
        }
        try {
            return new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw SqsException.invalidParam("Value " + token + " for parameter NextToken is invalid.");
        }
    }

    private JsonObject listDeadLetterSourceQueues(JsonObject req, Ctx ctx) throws SQLException {
        String name = requireQueueName(req);
        store.requireQueue(name);
        Integer maxResults = integer(req, "MaxResults");
        requireRange("MaxResults", maxResults, 1, 1000);
        int limit = maxResults != null ? maxResults : 1000;
        List<String> names = store.listDeadLetterSources(name, limit + 1, decodeToken(str(req, "NextToken")));
        boolean more = names.size() > limit;
        if (more) {
            names = names.subList(0, limit);
        }
        JsonArray urls = new JsonArray();
        for (String n : names) {
            urls.add(queueUrl(ctx, n));
        }
        JsonObject resp = new JsonObject();
        // the JSON protocol spells this member in lower case (botocore/SDK models: "queueUrls")
        resp.add("queueUrls", urls);
        if (more) {
            resp.addProperty("NextToken", encodeToken(names.get(names.size() - 1)));
        }
        return resp;
    }

    // ------------------------------------------------------------------------------------------
    // attributes
    // ------------------------------------------------------------------------------------------

    /** Validates an attribute map (create or set); returns canonical values. Empty values (set only) clear. */
    Map<String, String> validateAttributes(Map<String, String> in, boolean fifo, boolean creating) {
        Map<String, String> out = new LinkedHashMap<>();
        for (var e : in.entrySet()) {
            String k = e.getKey();
            String v = e.getValue() == null ? "" : e.getValue();
            if (!SETTABLE.contains(k)) {
                throw new SqsException(400, "InvalidAttributeName", "Unknown Attribute " + k + ".");
            }
            if (k.equals("FifoQueue")) {
                if (!creating) {
                    throw new SqsException(400, "InvalidAttributeName",
                            "Unknown Attribute FifoQueue. The queue type can't be changed after the queue is created.");
                }
                out.put(k, v.toLowerCase(java.util.Locale.ROOT));
                continue;
            }
            if (FIFO_ONLY.contains(k) && !fifo) {
                throw new SqsException(400, "InvalidAttributeName", "Unknown Attribute " + k + ".");
            }
            if (v.isEmpty() && !creating) {
                out.put(k, "");
                continue;
            }
            switch (k) {
                case "VisibilityTimeout" -> out.put(k, intInRange(k, v, 0, 43200));
                case "DelaySeconds" -> out.put(k, intInRange(k, v, 0, 900));
                case "MaximumMessageSize" -> out.put(k, intInRange(k, v, 1024, 262144));
                case "MessageRetentionPeriod" -> out.put(k, intInRange(k, v, 60, 1209600));
                case "ReceiveMessageWaitTimeSeconds" -> out.put(k, intInRange(k, v, 0, 20));
                case "KmsDataKeyReusePeriodSeconds" -> out.put(k, intInRange(k, v, 60, 86400));
                case "ContentBasedDeduplication", "SqsManagedSseEnabled" -> {
                    if (!v.equalsIgnoreCase("true") && !v.equalsIgnoreCase("false")) {
                        throw new SqsException(400, "InvalidAttributeValue", "Invalid value for the parameter " + k + ".");
                    }
                    out.put(k, v.toLowerCase(java.util.Locale.ROOT));
                }
                case "DeduplicationScope" -> out.put(k, oneOf(k, v, "messageGroup", "queue"));
                case "FifoThroughputLimit" -> out.put(k, oneOf(k, v, "perQueue", "perMessageGroupId"));
                case "RedrivePolicy" -> out.put(k, validateRedrivePolicy(v, fifo));
                case "RedriveAllowPolicy" -> out.put(k, validateRedriveAllowPolicy(v));
                case "Policy" -> {
                    try {
                        JsonParser.parseString(v).getAsJsonObject();
                    } catch (RuntimeException ex) {
                        throw new SqsException(400, "InvalidAttributeValue", "Invalid value for the parameter Policy.");
                    }
                    out.put(k, v);
                }
                default -> out.put(k, v);
            }
        }
        return out;
    }

    private static String intInRange(String name, String v, int min, int max) {
        try {
            int n = Integer.parseInt(v.trim());
            if (n >= min && n <= max) {
                return String.valueOf(n);
            }
        } catch (NumberFormatException ignored) {
            // fall through
        }
        throw new SqsException(400, "InvalidAttributeValue", "Invalid value for the parameter " + name + ".");
    }

    private static String oneOf(String name, String v, String... allowed) {
        for (String a : allowed) {
            if (a.equals(v)) {
                return v;
            }
        }
        throw new SqsException(400, "InvalidAttributeValue", "Invalid value for the parameter " + name + ".");
    }

    private String validateRedrivePolicy(String v, boolean fifo) {
        JsonObject p;
        try {
            p = JsonParser.parseString(v).getAsJsonObject();
        } catch (RuntimeException e) {
            throw SqsException.invalidParam("Value " + v + " for parameter RedrivePolicy is invalid. Reason: Invalid value for "
                    + "the parameter RedrivePolicy.");
        }
        String arn = p.has("deadLetterTargetArn") ? p.get("deadLetterTargetArn").getAsString() : null;
        String dlqName = PgQueueStore.queueNameFromArn(arn);
        if (dlqName == null) {
            throw SqsException.invalidParam("Value " + v + " for parameter RedrivePolicy is invalid. Reason: Dead letter target "
                    + "is not a valid Amazon SQS queue ARN.");
        }
        int max;
        try {
            max = p.get("maxReceiveCount").getAsBigDecimal().intValueExact();
        } catch (RuntimeException e) {
            throw SqsException.invalidParam("Value " + v + " for parameter RedrivePolicy is invalid. Reason: Invalid value for "
                    + "maxReceiveCount: must be an integer between 1 and 1000.");
        }
        if (max < 1 || max > 1000) {
            throw SqsException.invalidParam("Value " + v + " for parameter RedrivePolicy is invalid. Reason: Invalid value for "
                    + "maxReceiveCount: " + max + ", valid values are from 1 to 1000 both inclusive.");
        }
        try {
            PgQueueStore.QueueAttributes dlq = store.findQueueFresh(dlqName);
            if (dlq == null) {
                throw SqsException.invalidParam("Value " + v + " for parameter RedrivePolicy is invalid. Reason: Dead letter "
                        + "target does not exist.");
            }
            if (dlq.fifo() != fifo) {
                throw SqsException.invalidParam("Value " + v + " for parameter RedrivePolicy is invalid. Reason: Dead-letter "
                        + "queue must be the same type of queue as the source.");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return v;
    }

    private static String validateRedriveAllowPolicy(String v) {
        try {
            JsonObject p = JsonParser.parseString(v).getAsJsonObject();
            String perm = p.get("redrivePermission").getAsString();
            if (!Set.of("allowAll", "denyAll", "byQueue").contains(perm)) {
                throw new IllegalArgumentException(perm);
            }
            if (perm.equals("byQueue") && (!p.has("sourceQueueArns") || p.getAsJsonArray("sourceQueueArns").size() > 10)) {
                throw new IllegalArgumentException("sourceQueueArns");
            }
        } catch (RuntimeException e) {
            throw SqsException.invalidParam("Value " + v + " for parameter RedriveAllowPolicy is invalid. Reason: Invalid value "
                    + "for the parameter RedriveAllowPolicy.");
        }
        return v;
    }

    /** DLQ-side check: does the target queue's RedriveAllowPolicy let {@code sourceName} use it? */
    private void checkRedriveAllowed(String dlqName, String sourceName) throws SQLException {
        PgQueueStore.QueueAttributes dlq = store.findQueueFresh(dlqName);
        String policy = dlq == null ? null : dlq.attr("RedriveAllowPolicy");
        if (policy == null || policy.isEmpty()) {
            return;
        }
        JsonObject p = JsonParser.parseString(policy).getAsJsonObject();
        String perm = p.get("redrivePermission").getAsString();
        boolean ok = perm.equals("allowAll");
        if (perm.equals("byQueue")) {
            for (JsonElement e : p.getAsJsonArray("sourceQueueArns")) {
                ok |= store.queueArn(sourceName).equals(e.getAsString());
            }
        }
        if (!ok) {
            throw SqsException.invalidParam("Value for parameter RedrivePolicy is invalid. Reason: The dead-letter queue's "
                    + "RedriveAllowPolicy does not allow this source queue.");
        }
    }

    /** All attributes as GetQueueAttributes reports them (counts excluded), defaults filled in. */
    Map<String, String> effectiveAttributes(PgQueueStore.QueueAttributes a) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("VisibilityTimeout", String.valueOf(a.visibilityTimeout()));
        m.put("MaximumMessageSize", String.valueOf(a.maxMessageSize()));
        m.put("MessageRetentionPeriod", String.valueOf(a.retentionSeconds()));
        m.put("DelaySeconds", String.valueOf(a.delaySeconds()));
        m.put("ReceiveMessageWaitTimeSeconds", String.valueOf(a.waitSeconds()));
        for (String k : List.of("Policy", "RedrivePolicy", "RedriveAllowPolicy", "KmsMasterKeyId",
                "KmsDataKeyReusePeriodSeconds")) {
            String v = a.attr(k);
            if (v != null && !v.isEmpty()) {
                m.put(k, v);
            }
        }
        if (a.attr("KmsMasterKeyId") != null && !a.attr("KmsMasterKeyId").isEmpty()) {
            m.putIfAbsent("KmsDataKeyReusePeriodSeconds", "300");
            m.put("SqsManagedSseEnabled", "false");
        } else {
            m.put("SqsManagedSseEnabled", a.attr("SqsManagedSseEnabled") != null ? a.attr("SqsManagedSseEnabled") : "true");
        }
        if (a.fifo()) {
            m.put("FifoQueue", "true");
            m.put("ContentBasedDeduplication", a.boolAttr("ContentBasedDeduplication") ? "true" : "false");
            m.put("DeduplicationScope", a.attr("DeduplicationScope") != null ? a.attr("DeduplicationScope") : "queue");
            m.put("FifoThroughputLimit", a.attr("FifoThroughputLimit") != null ? a.attr("FifoThroughputLimit") : "perQueue");
        }
        return m;
    }

    private JsonObject getQueueAttributes(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes a = store.requireQueue(name);
        List<String> wanted = stringList(req, "AttributeNames");
        for (String w : wanted) {
            if (!w.equals("All") && !ALL_ATTRIBUTES.contains(w)) {
                throw new SqsException(400, "InvalidAttributeName", "Unknown Attribute " + w + ".");
            }
        }
        JsonObject resp = new JsonObject();
        if (wanted.isEmpty()) {
            return resp;
        }
        boolean all = wanted.contains("All");
        Map<String, String> eff = effectiveAttributes(a);
        eff.put("QueueArn", store.queueArn(name));
        long created = a.createdMs() > 0 ? a.createdMs() : System.currentTimeMillis();
        long modified = a.modifiedMs() > 0 ? a.modifiedMs() : created;
        eff.put("CreatedTimestamp", String.valueOf(created / 1000));
        eff.put("LastModifiedTimestamp", String.valueOf(modified / 1000));
        if (all || wanted.stream().anyMatch(w -> w.startsWith("ApproximateNumberOfMessages"))) {
            PgQueueStore.QueueCounts c = store.countMessages(name, a);
            eff.put("ApproximateNumberOfMessages", String.valueOf(c.visible()));
            eff.put("ApproximateNumberOfMessagesNotVisible", String.valueOf(c.inFlight()));
            eff.put("ApproximateNumberOfMessagesDelayed", String.valueOf(c.delayed()));
        }
        JsonObject attrs = new JsonObject();
        for (String k : ALL_ATTRIBUTES) {
            if ((all || wanted.contains(k)) && eff.containsKey(k)) {
                attrs.addProperty(k, eff.get(k));
            }
        }
        if (attrs.size() > 0) {
            resp.add("Attributes", attrs);
        }
        return resp;
    }

    private JsonObject setQueueAttributes(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        JsonObject raw = object(req, "Attributes");
        if (raw == null || raw.size() == 0) {
            throw SqsException.missing("Attribute");
        }
        PgQueueStore.QueueAttributes current = store.findQueueFresh(name);
        if (current == null) {
            throw SqsException.noQueue();
        }
        Map<String, String> requested = new LinkedHashMap<>();
        for (var e : raw.entrySet()) {
            requested.put(e.getKey(), e.getValue().getAsString());
        }
        Map<String, String> changes = validateAttributes(requested, current.fifo(), false);
        Map<String, String> merged = new LinkedHashMap<>(current.attributes());
        for (var e : changes.entrySet()) {
            if (e.getValue().isEmpty()) {
                merged.remove(e.getKey());
            } else {
                merged.put(e.getKey(), e.getValue());
            }
        }
        merged.putIfAbsent("VisibilityTimeout", String.valueOf(current.visibilityTimeout()));
        PgQueueStore.QueueAttributes next = build(merged, current.tags(), current.fifo());
        next = new PgQueueStore.QueueAttributes(next.visibilityTimeout(), next.fifo(), next.dlqQueueName(),
                next.maxReceiveCount(), next.attributes(), next.tags(), current.createdMs(), current.modifiedMs(),
                current.tableName());
        if (next.dlqQueueName() != null && changes.containsKey("RedrivePolicy")) {
            checkRedriveAllowed(next.dlqQueueName(), name);
        }
        store.updateQueue(name, next);
        waiters.signal(name);
        return new JsonObject();
    }

    // ------------------------------------------------------------------------------------------
    // tags and permissions
    // ------------------------------------------------------------------------------------------

    private static void validateTags(Map<String, String> tags) {
        if (tags.size() > 50) {
            throw SqsException.invalidParam("Too many tags: a queue can have at most 50 tags.");
        }
        for (var e : tags.entrySet()) {
            if (e.getKey().isEmpty() || e.getKey().length() > 128 || e.getKey().toLowerCase(java.util.Locale.ROOT).startsWith("aws:")) {
                throw SqsException.invalidParam("Invalid tag key: " + e.getKey()
                        + ". Keys must be 1-128 characters and must not start with aws:.");
            }
            if (e.getValue().length() > 256) {
                throw SqsException.invalidParam("Invalid tag value for key " + e.getKey() + ": at most 256 characters.");
            }
        }
    }

    private JsonObject tagQueue(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        JsonObject raw = object(req, "Tags");
        if (raw == null || raw.size() == 0) {
            throw SqsException.missing("Tag");
        }
        PgQueueStore.QueueAttributes a = store.requireQueue(name);
        a = store.findQueueFresh(name);
        Map<String, String> tags = new LinkedHashMap<>(a.tags());
        Map<String, String> add = new LinkedHashMap<>();
        raw.entrySet().forEach(e -> add.put(e.getKey(), e.getValue().getAsString()));
        validateTags(add);
        tags.putAll(add);
        validateTags(tags);
        store.updateQueue(name, withTags(a, tags));
        return new JsonObject();
    }

    private JsonObject untagQueue(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        List<String> keys = stringList(req, "TagKeys");
        if (keys.isEmpty()) {
            throw SqsException.missing("TagKey");
        }
        store.requireQueue(name);
        PgQueueStore.QueueAttributes a = store.findQueueFresh(name);
        Map<String, String> tags = new LinkedHashMap<>(a.tags());
        keys.forEach(tags::remove);
        store.updateQueue(name, withTags(a, tags));
        return new JsonObject();
    }

    private static PgQueueStore.QueueAttributes withTags(PgQueueStore.QueueAttributes a, Map<String, String> tags) {
        return new PgQueueStore.QueueAttributes(a.visibilityTimeout(), a.fifo(), a.dlqQueueName(), a.maxReceiveCount(),
                a.attributes(), tags, a.createdMs(), a.modifiedMs(), a.tableName());
    }

    private JsonObject listQueueTags(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes a = store.requireQueue(name);
        JsonObject resp = new JsonObject();
        if (!a.tags().isEmpty()) {
            JsonObject tags = new JsonObject();
            a.tags().forEach(tags::addProperty);
            resp.add("Tags", tags);
        }
        return resp;
    }

    private JsonObject addPermission(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        String label = str(req, "Label");
        List<String> accounts = stringList(req, "AWSAccountIds");
        List<String> actions = stringList(req, "Actions");
        if (label == null) {
            throw SqsException.missing("Label");
        }
        if (!LABEL.matcher(label).matches()) {
            throw SqsException.invalidParam("Value " + label + " for parameter Label is invalid. Reason: Must be 1-80 "
                    + "alphanumeric characters, hyphens or underscores.");
        }
        if (accounts.isEmpty()) {
            throw SqsException.missing("AWSAccountId");
        }
        if (actions.isEmpty()) {
            throw SqsException.missing("Action");
        }
        Set<String> valid = Set.of("*", "SendMessage", "ReceiveMessage", "DeleteMessage", "ChangeMessageVisibility",
                "GetQueueAttributes", "GetQueueUrl", "ListDeadLetterSourceQueues", "PurgeQueue");
        for (String act : actions) {
            if (!valid.contains(act)) {
                throw SqsException.invalidParam("Value SQS:" + act + " for parameter ActionName is invalid. Reason: Please refer "
                        + "to the appropriate WSDL for a list of valid actions.");
            }
        }
        for (String acct : accounts) {
            if (!acct.matches("\\d{12}")) {
                throw SqsException.invalidParam("Value " + acct + " for parameter AWSAccountId is invalid. Reason: Must be "
                        + "specified with a 12 digit account ID.");
            }
        }
        store.requireQueue(name);
        PgQueueStore.QueueAttributes a = store.findQueueFresh(name);
        JsonObject policy = a.attr("Policy") != null && !a.attr("Policy").isEmpty()
                ? JsonParser.parseString(a.attr("Policy")).getAsJsonObject() : new JsonObject();
        if (!policy.has("Statement")) {
            policy.addProperty("Version", "2012-10-17");
            policy.addProperty("Id", store.queueArn(name) + "/SQSDefaultPolicy");
            policy.add("Statement", new JsonArray());
        }
        for (JsonElement s : policy.getAsJsonArray("Statement")) {
            if (label.equals(str(s.getAsJsonObject(), "Sid"))) {
                throw SqsException.invalidParam("Value " + label + " for parameter Label is invalid. Reason: Already exists.");
            }
        }
        JsonObject stmt = new JsonObject();
        stmt.addProperty("Sid", label);
        stmt.addProperty("Effect", "Allow");
        JsonObject principal = new JsonObject();
        JsonArray arns = new JsonArray();
        accounts.forEach(acct -> arns.add("arn:aws:iam::" + acct + ":root"));
        principal.add("AWS", arns);
        stmt.add("Principal", principal);
        JsonArray acts = new JsonArray();
        actions.forEach(act -> acts.add("SQS:" + act));
        stmt.add("Action", acts.size() == 1 ? acts.get(0) : acts);
        stmt.addProperty("Resource", store.queueArn(name));
        policy.getAsJsonArray("Statement").add(stmt);
        Map<String, String> attrs = new LinkedHashMap<>(a.attributes());
        attrs.put("Policy", policy.toString());
        store.updateQueue(name, withAttributes(a, attrs));
        return new JsonObject();
    }

    private JsonObject removePermission(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        String label = str(req, "Label");
        if (label == null) {
            throw SqsException.missing("Label");
        }
        store.requireQueue(name);
        PgQueueStore.QueueAttributes a = store.findQueueFresh(name);
        String raw = a.attr("Policy");
        boolean removed = false;
        JsonObject policy = raw == null || raw.isEmpty() ? null : JsonParser.parseString(raw).getAsJsonObject();
        if (policy != null && policy.has("Statement")) {
            JsonArray kept = new JsonArray();
            for (JsonElement s : policy.getAsJsonArray("Statement")) {
                if (label.equals(str(s.getAsJsonObject(), "Sid"))) {
                    removed = true;
                } else {
                    kept.add(s);
                }
            }
            policy.add("Statement", kept);
        }
        if (!removed) {
            throw SqsException.invalidParam("Value " + label + " for parameter Label is invalid. Reason: can't find label.");
        }
        Map<String, String> attrs = new LinkedHashMap<>(a.attributes());
        if (policy.getAsJsonArray("Statement").size() == 0) {
            attrs.remove("Policy");
        } else {
            attrs.put("Policy", policy.toString());
        }
        store.updateQueue(name, withAttributes(a, attrs));
        return new JsonObject();
    }

    private static PgQueueStore.QueueAttributes withAttributes(PgQueueStore.QueueAttributes a, Map<String, String> attrs) {
        return new PgQueueStore.QueueAttributes(a.visibilityTimeout(), a.fifo(), a.dlqQueueName(), a.maxReceiveCount(),
                attrs, a.tags(), a.createdMs(), a.modifiedMs(), a.tableName());
    }

    // ------------------------------------------------------------------------------------------
    // messages
    // ------------------------------------------------------------------------------------------

    /** A validated, ready-to-store message plus the checksums the response carries. */
    private record Prepared(PgQueueStore.NewMessage message, String md5Body, String md5Attrs, String md5Sys, int size) {
    }

    private static void validateBodyChars(String body) {
        for (int i = 0; i < body.length(); ) {
            int cp = body.codePointAt(i);
            boolean ok = cp == 0x9 || cp == 0xA || cp == 0xD || (cp >= 0x20 && cp <= 0xD7FF) || (cp >= 0xE000 && cp <= 0xFFFD)
                    || (cp >= 0x10000 && cp <= 0x10FFFF);
            if (!ok) {
                throw new SqsException(400, "InvalidMessageContents", "Invalid characters found. Valid unicode characters are "
                        + "#x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF");
            }
            i += Character.charCount(cp);
        }
    }

    /** Validates one message (SendMessage or a batch entry) against the queue and prepares it. */
    private Prepared prepare(PgQueueStore.QueueAttributes q, JsonObject m) {
        String body = str(m, "MessageBody");
        if (body == null) {
            throw SqsException.missing("MessageBody");
        }
        if (body.isEmpty()) {
            throw SqsException.invalidParam("One or more parameters are invalid. Reason: Message must be shorter than "
                    + q.maxMessageSize() + " bytes and not empty.");
        }
        validateBodyChars(body);
        Integer delay = integer(m, "DelaySeconds");
        requireRange("DelaySeconds", delay, 0, 900);
        String group = str(m, "MessageGroupId");
        String dedup = str(m, "MessageDeduplicationId");
        if (group != null && !ID_CHARS.matcher(group).matches()) {
            throw SqsException.invalidParam("Value " + group + " for parameter MessageGroupId is invalid. Reason: MessageGroupId "
                    + "can only include alphanumeric and punctuation characters. 1 to 128 in length.");
        }
        if (dedup != null && !ID_CHARS.matcher(dedup).matches()) {
            throw SqsException.invalidParam("Value " + dedup + " for parameter MessageDeduplicationId is invalid. Reason: "
                    + "MessageDeduplicationId can only include alphanumeric and punctuation characters. 1 to 128 in length.");
        }
        JsonObject attrs = SqsMessageAttributes.validateAndNormalise(object(m, "MessageAttributes"));
        String trace = SqsMessageAttributes.traceHeader(object(m, "MessageSystemAttributes"));
        int size = SqsMessageAttributes.payloadSize(body, attrs);
        if (size > q.maxMessageSize()) {
            throw SqsException.invalidParam("One or more parameters are invalid. Reason: Message must be shorter than "
                    + q.maxMessageSize() + " bytes.");
        }
        int effectiveDelay;
        if (q.fifo()) {
            if (delay != null) {
                throw SqsException.invalidParam("Value " + delay + " for parameter DelaySeconds is invalid. Reason: The request "
                        + "include parameter that is not valid for this queue type.");
            }
            if (group == null || group.isEmpty()) {
                throw SqsException.missing("MessageGroupId");
            }
            if (dedup == null) {
                if (q.boolAttr("ContentBasedDeduplication")) {
                    dedup = SqsMessageAttributes.sha256Hex(body);
                } else {
                    throw SqsException.invalidParam("The queue should either have ContentBasedDeduplication enabled or "
                            + "MessageDeduplicationId provided explicitly");
                }
            }
            effectiveDelay = q.delaySeconds();
        } else {
            if (dedup != null) {
                throw SqsException.invalidParam("Value " + dedup + " for parameter MessageDeduplicationId is invalid. Reason: The "
                        + "request include parameter that is not valid for this queue type.");
            }
            effectiveDelay = delay != null ? delay : q.delaySeconds();
        }
        return new Prepared(new PgQueueStore.NewMessage(body, group, dedup, effectiveDelay,
                attrs.size() == 0 ? null : attrs.toString(), trace),
                SqsMessageAttributes.md5Hex(body), SqsMessageAttributes.md5OfAttributes(attrs),
                SqsMessageAttributes.md5OfTraceHeader(trace), size);
    }

    private static void putSendResult(JsonObject o, Prepared p, PgQueueStore.SendResult r, boolean fifo) {
        o.addProperty("MessageId", r.messageId());
        o.addProperty("MD5OfMessageBody", p.md5Body());
        if (p.md5Attrs() != null) {
            o.addProperty("MD5OfMessageAttributes", p.md5Attrs());
        }
        if (p.md5Sys() != null) {
            o.addProperty("MD5OfMessageSystemAttributes", p.md5Sys());
        }
        if (fifo) {
            o.addProperty("SequenceNumber", String.format("%018d", r.sequence()));
        }
    }

    private JsonObject sendMessage(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        Prepared p = prepare(q, req);
        PgQueueStore.SendResult r = store.send(name, q, p.message());
        JsonObject resp = new JsonObject();
        putSendResult(resp, p, r, q.fifo());
        return resp;
    }

    /** Common batch-request checks; returns the entry objects. */
    private List<JsonObject> batchEntries(JsonObject req, String entryName) {
        JsonArray arr = req.has("Entries") && req.get("Entries").isJsonArray() ? req.getAsJsonArray("Entries") : new JsonArray();
        if (arr.size() == 0) {
            throw new SqsException(400, "EmptyBatchRequest", "There should be at least one " + entryName + " in the request.");
        }
        if (arr.size() > MAX_BATCH) {
            throw new SqsException(400, "TooManyEntriesInBatchRequest",
                    "Maximum number of entries per request are " + MAX_BATCH + ". You have sent " + arr.size() + ".");
        }
        List<JsonObject> out = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        for (JsonElement e : arr) {
            JsonObject o = e.getAsJsonObject();
            String id = str(o, "Id");
            if (id == null || !BATCH_ID.matcher(id).matches()) {
                throw new SqsException(400, "InvalidBatchEntryId", "A batch entry id can only contain alphanumeric characters, "
                        + "hyphens and underscores. It can be at most 80 letters long.");
            }
            if (!ids.add(id)) {
                throw new SqsException(400, "BatchEntryIdsNotDistinct", "Id " + id + " repeated.");
            }
            out.add(o);
        }
        return out;
    }

    private static JsonObject failure(String id, SqsException e) {
        JsonObject f = new JsonObject();
        f.addProperty("Id", id);
        f.addProperty("SenderFault", true);
        f.addProperty("Code", e.sqsErrorType);
        f.addProperty("Message", e.getMessage());
        return f;
    }

    private JsonObject sendMessageBatch(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        List<JsonObject> entries = batchEntries(req, "SendMessageBatchRequestEntry");
        JsonArray failed = new JsonArray();
        List<JsonObject> okEntries = new ArrayList<>();
        List<Prepared> prepared = new ArrayList<>();
        long total = 0;
        for (JsonObject e : entries) {
            String id = str(e, "Id");
            try {
                Prepared p = prepare(q, e);
                total += p.size();
                okEntries.add(e);
                prepared.add(p);
            } catch (SqsException ex) {
                failed.add(failure(id, ex));
            }
        }
        if (total > MAX_BATCH_BYTES) {
            throw new SqsException(400, "BatchRequestTooLong", "Batch requests cannot be longer than " + MAX_BATCH_BYTES
                    + " bytes. You have sent " + total + " bytes.");
        }
        JsonArray successful = new JsonArray();
        if (!prepared.isEmpty()) {
            List<PgQueueStore.SendResult> results = store.sendMany(name, q, prepared.stream().map(Prepared::message).toList());
            for (int i = 0; i < prepared.size(); i++) {
                JsonObject o = new JsonObject();
                o.addProperty("Id", str(okEntries.get(i), "Id"));
                putSendResult(o, prepared.get(i), results.get(i), q.fifo());
                successful.add(o);
            }
        }
        JsonObject resp = new JsonObject();
        resp.add("Successful", successful);
        resp.add("Failed", failed);
        return resp;
    }

    private JsonObject receiveMessage(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        Integer max = integer(req, "MaxNumberOfMessages");
        requireRange("MaxNumberOfMessages", max, 1, 10);
        Integer vt = integer(req, "VisibilityTimeout");
        requireRange("VisibilityTimeout", vt, 0, 43200);
        Integer wait = integer(req, "WaitTimeSeconds");
        requireRange("WaitTimeSeconds", wait, 0, 20);
        String attemptId = str(req, "ReceiveRequestAttemptId");
        if (attemptId != null && !ID_CHARS.matcher(attemptId).matches()) {
            throw SqsException.invalidParam("Value " + attemptId + " for parameter ReceiveRequestAttemptId is invalid.");
        }
        Set<String> sysNames = new java.util.LinkedHashSet<>(stringList(req, "AttributeNames"));
        sysNames.addAll(stringList(req, "MessageSystemAttributeNames"));
        for (String s : sysNames) {
            if (!SYSTEM_ATTRIBUTES.contains(s)) {
                throw SqsException.invalidParam("Value " + s + " for parameter AttributeName is invalid. Reason: Must be one of "
                        + "the message system attribute names.");
            }
        }
        List<String> attrNames = stringList(req, "MessageAttributeNames");
        int waitSeconds = wait != null ? wait : q.waitSeconds();
        int limit = max != null ? max : 1;
        long deadline = System.currentTimeMillis() + waitSeconds * 1000L;
        List<PgQueueStore.Received> got;
        while (true) {
            long version = waiters.version(name);
            got = store.receive(name, q, limit, vt, attemptId);
            if (!got.isEmpty() || waitSeconds == 0) {
                break;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            long parkedAt = System.nanoTime();
            try {
                waiters.await(name, version, Math.min(remaining, POLL_INTERVAL_MS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } finally {
                WAITED_NANOS.get()[0] += System.nanoTime() - parkedAt;
            }
            // pick up attribute changes (visibility timeout, redrive policy) made while parked; a deleted queue ends the poll
            q = store.findQueue(name);
            if (q == null) {
                throw SqsException.noQueue();
            }
        }
        JsonArray messages = new JsonArray();
        for (PgQueueStore.Received r : got) {
            messages.add(renderMessage(q, r, sysNames, attrNames));
        }
        JsonObject resp = new JsonObject();
        if (messages.size() > 0) {
            resp.add("Messages", messages);
        }
        return resp;
    }

    private JsonObject renderMessage(PgQueueStore.QueueAttributes q, PgQueueStore.Received r, Set<String> sysNames,
            List<String> attrNames) {
        JsonObject m = new JsonObject();
        m.addProperty("MessageId", r.messageId());
        m.addProperty("ReceiptHandle", r.receiptHandle());
        m.addProperty("MD5OfBody", SqsMessageAttributes.md5Hex(r.body()));
        m.addProperty("Body", r.body());
        boolean all = sysNames.contains("All");
        JsonObject sys = new JsonObject();
        if (all || sysNames.contains("SenderId")) {
            sys.addProperty("SenderId", store.accountId());
        }
        if (all || sysNames.contains("SentTimestamp")) {
            sys.addProperty("SentTimestamp", String.valueOf(r.sentMs()));
        }
        if (all || sysNames.contains("ApproximateReceiveCount")) {
            sys.addProperty("ApproximateReceiveCount", String.valueOf(r.receiveCount()));
        }
        if ((all || sysNames.contains("ApproximateFirstReceiveTimestamp")) && r.firstReceiveMs() != null) {
            sys.addProperty("ApproximateFirstReceiveTimestamp", String.valueOf(r.firstReceiveMs()));
        }
        if (q.fifo() && (all || sysNames.contains("SequenceNumber"))) {
            sys.addProperty("SequenceNumber", r.sequenceNumber());
        }
        if (q.fifo() && r.dedupId() != null && (all || sysNames.contains("MessageDeduplicationId"))) {
            sys.addProperty("MessageDeduplicationId", r.dedupId());
        }
        if (r.groupId() != null && (all || sysNames.contains("MessageGroupId"))) {
            sys.addProperty("MessageGroupId", r.groupId());
        }
        if (r.traceHeader() != null && (all || sysNames.contains("AWSTraceHeader"))) {
            sys.addProperty("AWSTraceHeader", r.traceHeader());
        }
        if (r.dlqSourceArn() != null && (all || sysNames.contains("DeadLetterQueueSourceArn"))) {
            sys.addProperty("DeadLetterQueueSourceArn", r.dlqSourceArn());
        }
        if (sys.size() > 0) {
            m.add("Attributes", sys);
        }
        if (r.attrsJson() != null) {
            JsonObject filtered = SqsMessageAttributes.filter(JsonParser.parseString(r.attrsJson()).getAsJsonObject(), attrNames);
            if (filtered.size() > 0) {
                m.addProperty("MD5OfMessageAttributes", SqsMessageAttributes.md5OfAttributes(filtered));
                m.add("MessageAttributes", filtered);
            }
        }
        return m;
    }

    private JsonObject deleteMessage(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        String handle = str(req, "ReceiptHandle");
        if (handle == null) {
            throw SqsException.missing("ReceiptHandle");
        }
        store.deleteMessage(name, q, handle);
        return new JsonObject();
    }

    private JsonObject deleteMessageBatch(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        JsonArray ok = new JsonArray();
        JsonArray failed = new JsonArray();
        for (JsonObject e : batchEntries(req, "DeleteMessageBatchRequestEntry")) {
            String id = str(e, "Id");
            String handle = str(e, "ReceiptHandle");
            try {
                if (handle == null) {
                    throw SqsException.missing("ReceiptHandle");
                }
                store.deleteMessage(name, q, handle);
                JsonObject o = new JsonObject();
                o.addProperty("Id", id);
                ok.add(o);
            } catch (SqsException ex) {
                failed.add(failure(id, ex));
            }
        }
        JsonObject resp = new JsonObject();
        resp.add("Successful", ok);
        resp.add("Failed", failed);
        return resp;
    }

    private void changeVisibility(String name, PgQueueStore.QueueAttributes q, String handle, Integer timeout)
            throws SQLException {
        if (handle == null) {
            throw SqsException.missing("ReceiptHandle");
        }
        if (timeout == null) {
            throw SqsException.missing("VisibilityTimeout");
        }
        requireRange("VisibilityTimeout", timeout, 0, 43200);
        switch (store.changeMessageVisibility(name, q, handle, timeout)) {
            case CHANGED -> { }
            case NOT_IN_FLIGHT -> throw new SqsException(400, "MessageNotInflight", "Message is not in flight.");
            case INVALID -> throw new SqsException(400, "ReceiptHandleIsInvalid",
                    "The input receipt handle \"" + handle + "\" is not a valid receipt handle.");
        }
    }

    private JsonObject changeMessageVisibility(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        changeVisibility(name, q, str(req, "ReceiptHandle"), integer(req, "VisibilityTimeout"));
        return new JsonObject();
    }

    private JsonObject changeMessageVisibilityBatch(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        JsonArray ok = new JsonArray();
        JsonArray failed = new JsonArray();
        for (JsonObject e : batchEntries(req, "ChangeMessageVisibilityBatchRequestEntry")) {
            String id = str(e, "Id");
            try {
                changeVisibility(name, q, str(e, "ReceiptHandle"), integer(e, "VisibilityTimeout"));
                JsonObject o = new JsonObject();
                o.addProperty("Id", id);
                ok.add(o);
            } catch (SqsException ex) {
                failed.add(failure(id, ex));
            }
        }
        JsonObject resp = new JsonObject();
        resp.add("Successful", ok);
        resp.add("Failed", failed);
        return resp;
    }

    private JsonObject purgeQueue(JsonObject req) throws SQLException {
        String name = requireQueueName(req);
        PgQueueStore.QueueAttributes q = store.requireQueue(name);
        store.purgeQueue(name, q);
        return new JsonObject();
    }

    // ------------------------------------------------------------------------------------------
    // message move tasks
    // ------------------------------------------------------------------------------------------

    private JsonObject startMessageMoveTask(JsonObject req) throws SQLException {
        JsonObject resp = new JsonObject();
        resp.addProperty("TaskHandle", moveTasks.start(str(req, "SourceArn"), str(req, "DestinationArn"),
                integer(req, "MaxNumberOfMessagesPerSecond")));
        return resp;
    }

    private JsonObject cancelMessageMoveTask(JsonObject req) throws SQLException {
        JsonObject resp = new JsonObject();
        resp.addProperty("ApproximateNumberOfMessagesMoved", moveTasks.cancel(str(req, "TaskHandle")));
        return resp;
    }

    private JsonObject listMessageMoveTasks(JsonObject req) throws SQLException {
        Integer maxResults = integer(req, "MaxResults");
        requireRange("MaxResults", maxResults, 1, 10);
        JsonArray results = new JsonArray();
        for (PgQueueStore.MoveTask t : moveTasks.list(str(req, "SourceArn"), maxResults != null ? maxResults : 1)) {
            JsonObject o = new JsonObject();
            o.addProperty("TaskHandle", t.handle());
            o.addProperty("Status", t.status());
            o.addProperty("SourceArn", t.sourceArn());
            if (t.destinationArn() != null) {
                o.addProperty("DestinationArn", t.destinationArn());
            }
            if (t.maxPerSecond() > 0) {
                o.addProperty("MaxNumberOfMessagesPerSecond", t.maxPerSecond());
            }
            o.addProperty("ApproximateNumberOfMessagesMoved", t.moved());
            o.addProperty("ApproximateNumberOfMessagesToMove", t.toMove());
            if (t.failureReason() != null) {
                o.addProperty("FailureReason", t.failureReason());
            }
            o.addProperty("StartedTimestamp", t.startedMs());
            results.add(o);
        }
        JsonObject resp = new JsonObject();
        resp.add("Results", results);
        return resp;
    }
}
