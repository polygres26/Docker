// Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors.
// See Warp/NOTICE for the licence text.
package com.sayonora.warp.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Amazon SNS on Postgres (the {@code sns} store): topics, subscriptions (with filter policies, raw delivery, redrive
 * policies), Publish/PublishBatch, FIFO topics with the 5 minute deduplication window, platform applications/endpoints and
 * SMS settings. Speaks the Query protocol (what every current SDK uses for SNS) and the JSON protocol.
 *
 * <p><b>Sharding</b>: a topic, its subscriptions, its FIFO dedup rows and its recorded deliveries live on the host owning
 * hash(topic name); ListTopics/ListSubscriptions fan out over all hosts and merge by ARN. Platform applications, endpoints
 * and SMS settings live on the first host.
 *
 * <p><b>Delivery</b> (fan-out happens after the publish transaction commits, never inside it, and never holds a
 * database connection across I/O): {@code sqs} subscriptions are delivered synchronously, in process, through the
 * sqswire operations of this Warp (stronger than AWS, which is asynchronous; a message is in the queue before Publish
 * returns); {@code http}/{@code https} subscriptions are POSTed asynchronously with SNS headers, the confirmation flow
 * ({@code SubscriptionConfirmation} then {@code ConfirmSubscription} with the token) and retries, then the subscription's
 * redrive policy; {@code lambda}, {@code email}, {@code email-json}, {@code sms}, {@code application} and {@code firehose}
 * are accepted and recorded in {@code warp_sns_deliveries} without being delivered anywhere.
 */
public final class SnsService extends AwsService {

    private static final Logger log = LoggerFactory.getLogger(SnsService.class);
    private static final Pattern TOPIC_NAME = Pattern.compile("^[A-Za-z0-9_-]{1,256}(\\.fifo)?$");
    private static final Set<String> PROTOCOLS = Set.of("sqs", "http", "https", "email", "email-json", "sms", "lambda",
            "application", "firehose");
    private static final Set<String> SUB_ATTRS = Set.of("RawMessageDelivery", "FilterPolicy", "FilterPolicyScope", "RedrivePolicy",
            "DeliveryPolicy", "SubscriptionRoleArn");
    private static final Set<String> TOPIC_ATTRS = Set.of("DeliveryPolicy", "DisplayName", "Policy", "KmsMasterKeyId",
            "FifoTopic", "ContentBasedDeduplication", "SignatureVersion", "TracingConfig", "ArchivePolicy",
            "FifoThroughputScope", "HTTPSuccessFeedbackRoleArn", "HTTPSuccessFeedbackSampleRate", "HTTPFailureFeedbackRoleArn",
            "LambdaSuccessFeedbackRoleArn", "LambdaSuccessFeedbackSampleRate", "LambdaFailureFeedbackRoleArn",
            "SQSSuccessFeedbackRoleArn", "SQSSuccessFeedbackSampleRate", "SQSFailureFeedbackRoleArn",
            "ApplicationSuccessFeedbackRoleArn", "ApplicationSuccessFeedbackSampleRate", "ApplicationFailureFeedbackRoleArn",
            "FirehoseSuccessFeedbackRoleArn", "FirehoseSuccessFeedbackSampleRate", "FirehoseFailureFeedbackRoleArn",
            "DataProtectionPolicy");
    private static final int MAX_MESSAGE_BYTES = 262144;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final AtomicLong SEQUENCE = new AtomicLong(System.currentTimeMillis() * 1_000_000L);

    private final AwsRuntime rt;
    private final AwsShards sh;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(8, r -> {
        Thread t = new Thread(r, "snswire-delivery");
        t.setDaemon(true);
        return t;
    });
    private final int httpAttempts = intEnv("WARP_SNSWIRE_HTTP_ATTEMPTS", 3);
    private final long httpBackoffMs = intEnv("WARP_SNSWIRE_HTTP_BACKOFF_MS", 1000);

    public SnsService(AwsRuntime rt) {
        this.rt = rt;
        this.sh = new AwsShards(rt.registry, StoreType.SNS);
    }

    private static int intEnv(String n, int d) {
        try {
            String v = System.getenv(n);
            return v == null || v.isBlank() ? d : Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return d;
        }
    }

    @Override
    public String id() {
        return "sns";
    }

    @Override
    public String metricsProtocol() {
        return "snswire";
    }

    @Override
    public Set<String> signingNames() {
        return Set.of("sns");
    }

    @Override
    public Set<String> targetPrefixes() {
        return Set.of("SNS_20100331.", "AmazonSimpleNotificationService.");
    }

    @Override
    public String jsonVersion() {
        return "1.1";
    }

    @Override
    public boolean supportsQuery() {
        return true;
    }

    @Override
    public String xmlNamespace() {
        return "http://sns.amazonaws.com/doc/2010-03-31/";
    }

    @Override
    public Set<String> mapFields() {
        return Set.of("Attributes", "attributes");
    }

    @Override
    public Set<String> queryActions() {
        return Set.of("CreateTopic", "DeleteTopic", "ListTopics", "GetTopicAttributes", "SetTopicAttributes", "Subscribe",
                "ConfirmSubscription", "Unsubscribe", "Publish", "PublishBatch", "ListSubscriptions", "ListSubscriptionsByTopic",
                "GetSubscriptionAttributes", "SetSubscriptionAttributes", "AddPermission", "RemovePermission",
                "CreatePlatformApplication", "GetPlatformApplicationAttributes", "SetPlatformApplicationAttributes",
                "DeletePlatformApplication", "ListPlatformApplications", "CreatePlatformEndpoint", "GetEndpointAttributes",
                "SetEndpointAttributes", "DeleteEndpoint", "ListEndpointsByPlatformApplication", "SetSMSAttributes",
                "GetSMSAttributes", "CheckIfPhoneNumberIsOptedOut", "ListPhoneNumbersOptedOut", "OptInPhoneNumber",
                "PutDataProtectionPolicy", "GetDataProtectionPolicy");
    }

    @Override
    public boolean available() {
        return sh.available();
    }

    @Override
    public String jsonErrorType(String code) {
        return code.equals("NotFound") ? "NotFoundException" : code;
    }

    @Override
    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return switch (op) {
            case "GetTopicAttributes", "ListTopics", "ListSubscriptions", "ListSubscriptionsByTopic", "GetSubscriptionAttributes",
                    "ListTagsForResource", "GetPlatformApplicationAttributes", "ListPlatformApplications", "GetEndpointAttributes",
                    "ListEndpointsByPlatformApplication", "GetSMSAttributes", "CheckIfPhoneNumberIsOptedOut",
                    "ListPhoneNumbersOptedOut", "GetDataProtectionPolicy" -> SqlMetricsCollector.StatementKind.READ;
            default -> SqlMetricsCollector.StatementKind.WRITE;
        };
    }

    @Override
    public String backendLabel(String op, JsonObject req) {
        try {
            String arn = req == null ? null : Args.str(req, "TopicArn");
            if (arn == null && req != null) {
                arn = Args.str(req, "SubscriptionArn");
            }
            return arn == null || !sh.available() ? "default" : sh.owner(nameOfArn(arn));
        } catch (RuntimeException e) {
            return "default";
        }
    }

    @Override
    public void start() {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                if (sh.available()) {
                    for (String h : sh.hosts()) {
                        sh.conn(h, c -> {
                            try (var st = c.createStatement()) {
                                st.executeUpdate("DELETE FROM warp_sns_deliveries WHERE created_at < now() - interval '1 day'");
                                st.executeUpdate("DELETE FROM warp_sns_dedup WHERE created_at < now() - interval '10 minutes'");
                            }
                            return null;
                        });
                    }
                }
            } catch (RuntimeException e) {
                log.debug("snswire sweep failed: {}", e.getMessage());
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduler.shutdownNow();
    }

    // ---------------------------------------------------------------------------------------------- helpers

    private static AwsException notFound(String what) {
        return new AwsException(404, "NotFound", what + " does not exist");
    }

    private static AwsException invalid(String msg) {
        return new AwsException(400, "InvalidParameter", "Invalid parameter: " + msg);
    }

    private static AwsException invalidValue(String msg) {
        return new AwsException(400, "InvalidParameterValue", "Invalid parameter: " + msg);
    }

    /** The resource name (6th ARN field) of {@code arn:aws:sns:region:account:name[:suffix]}. */
    static String nameOfArn(String arn) {
        String[] p = arn.split(":");
        if (p.length < 6 || !arn.startsWith("arn:")) {
            throw invalid("TopicArn");
        }
        return p[5];
    }

    private String topicArn(String name) {
        return rt.config.arn("sns", name);
    }

    private static Map<String, String> strMap(JsonObject o) {
        Map<String, String> m = new LinkedHashMap<>();
        if (o != null) {
            for (var e : o.entrySet()) {
                m.put(e.getKey(), e.getValue().isJsonPrimitive() ? e.getValue().getAsString() : e.getValue().toString());
            }
        }
        return m;
    }

    private static JsonObject toJson(Map<String, String> m) {
        JsonObject o = new JsonObject();
        m.forEach(o::addProperty);
        return o;
    }

    private static Map<String, String> parseMap(String json) {
        Map<String, String> m = new LinkedHashMap<>();
        JsonObject o = JsonParser.parseString(json == null || json.isBlank() ? "{}" : json).getAsJsonObject();
        for (var e : o.entrySet()) {
            m.put(e.getKey(), e.getValue().getAsString());
        }
        return m;
    }

    private static Map<String, String> tagMap(JsonObject req) {
        Map<String, String> m = new LinkedHashMap<>();
        for (JsonObject t : Args.objects(req, "Tags")) {
            String k = Args.str(t, "Key");
            if (k == null) {
                throw invalid("Tags");
            }
            m.put(k, Args.str(t, "Value") == null ? "" : Args.str(t, "Value"));
        }
        return m;
    }

    private static String hex(int bytes) {
        byte[] b = new byte[bytes];
        RANDOM.nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    private static String sha256Hex(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private record Topic(String name, String arn, boolean fifo, Map<String, String> attrs, Map<String, String> tags) {
    }

    private record Sub(String arn, String topicName, String topicArn, String protocol, String endpoint, String owner,
            Map<String, String> attrs, boolean confirmed, String token) {
    }

    private Topic readTopic(Connection c, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT arn, fifo, attributes::text, tags::text FROM warp_sns_topics WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new Topic(name, rs.getString(1), rs.getBoolean(2), parseMap(rs.getString(3)),
                        parseMap(rs.getString(4))) : null;
            }
        }
    }

    private static Sub readSub(ResultSet rs) throws SQLException {
        return new Sub(rs.getString("arn"), rs.getString("topic"), rs.getString("topic_arn"), rs.getString("protocol"),
                rs.getString("endpoint"), rs.getString("owner"), parseMap(rs.getString("attrs")), rs.getBoolean("confirmed"),
                rs.getString("token"));
    }

    private static final String SUB_COLS = "arn, topic, topic_arn, protocol, endpoint, owner, attributes::text AS attrs, confirmed, token";

    // ---------------------------------------------------------------------------------------------- dispatch

    @Override
    public JsonObject invoke(String op, JsonObject req, Call call) throws Exception {
        return switch (op) {
            case "CreateTopic" -> createTopic(req);
            case "DeleteTopic" -> deleteTopic(req);
            case "ListTopics" -> listTopics(req);
            case "GetTopicAttributes" -> getTopicAttributes(req);
            case "SetTopicAttributes" -> setTopicAttributes(req);
            case "TagResource" -> tagResource(req);
            case "UntagResource" -> untagResource(req);
            case "ListTagsForResource" -> listTags(req);
            case "AddPermission" -> addPermission(req);
            case "RemovePermission" -> removePermission(req);
            case "PutDataProtectionPolicy" -> putDataProtection(req);
            case "GetDataProtectionPolicy" -> getDataProtection(req);
            case "Subscribe" -> subscribe(req, call);
            case "ConfirmSubscription" -> confirmSubscription(req);
            case "Unsubscribe" -> unsubscribe(req, call);
            case "ListSubscriptions" -> listSubscriptions(req, null);
            case "ListSubscriptionsByTopic" -> listSubscriptions(req, Args.req(req, "TopicArn"));
            case "GetSubscriptionAttributes" -> getSubscriptionAttributes(req);
            case "SetSubscriptionAttributes" -> setSubscriptionAttributes(req);
            case "Publish" -> publish(req, call);
            case "PublishBatch" -> publishBatch(req, call);
            case "CreatePlatformApplication" -> createPlatformApplication(req);
            case "GetPlatformApplicationAttributes" -> getPlatformApplicationAttributes(req);
            case "SetPlatformApplicationAttributes" -> setPlatformApplicationAttributes(req);
            case "DeletePlatformApplication" -> deleteByArn("warp_sns_platform_apps", Args.req(req, "PlatformApplicationArn"));
            case "ListPlatformApplications" -> listPlatformApplications();
            case "CreatePlatformEndpoint" -> createPlatformEndpoint(req);
            case "GetEndpointAttributes" -> getEndpointAttributes(req);
            case "SetEndpointAttributes" -> setEndpointAttributes(req);
            case "DeleteEndpoint" -> deleteByArn("warp_sns_platform_endpoints", Args.req(req, "EndpointArn"));
            case "ListEndpointsByPlatformApplication" -> listEndpoints(req);
            case "SetSMSAttributes" -> setSmsAttributes(req);
            case "GetSMSAttributes" -> getSmsAttributes(req);
            case "CheckIfPhoneNumberIsOptedOut" -> checkOptedOut(req);
            case "ListPhoneNumbersOptedOut" -> listOptedOut();
            case "OptInPhoneNumber" -> optIn(req);
            default -> throw new AwsException(400, "InvalidAction", "Unknown operation " + op);
        };
    }

    // ---------------------------------------------------------------------------------------------- topics

    private JsonObject createTopic(JsonObject req) {
        String name = Args.req(req, "Name");
        Map<String, String> attrs = strMap(Args.obj(req, "Attributes"));
        boolean fifoAttr = "true".equalsIgnoreCase(attrs.get("FifoTopic"));
        if (!TOPIC_NAME.matcher(name).matches()) {
            throw invalid("Topic Name" + (name.endsWith(".fifo") ? "" : "") + ": Topic names must be made up of only uppercase and "
                    + "lowercase ASCII letters, numbers, underscores, and hyphens, and must be between 1 and 256 characters long.");
        }
        if (name.endsWith(".fifo") != fifoAttr) {
            throw invalid(fifoAttr ? "Topic Name: Fifo topic names must end with .fifo"
                    : "Attributes Reason: Topic names ending in .fifo require the FifoTopic attribute set to true");
        }
        for (String k : attrs.keySet()) {
            if (!TOPIC_ATTRS.contains(k)) {
                throw invalid("Attributes Reason: Invalid attribute name " + k);
            }
        }
        Map<String, String> tags = tagMap(req);
        String arn = topicArn(name);
        return sh.tx(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t != null) {
                for (var e : attrs.entrySet()) {
                    String have = t.attrs.get(e.getKey());
                    if (have == null ? !isDefaultAttr(e.getKey(), e.getValue()) : !have.equals(e.getValue())) {
                        throw invalid("Attributes Reason: Topic already exists with different attributes");
                    }
                }
                JsonObject same = new JsonObject();
                same.addProperty("TopicArn", arn);
                return same;
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_topics (name, arn, fifo, attributes, tags) "
                    + "VALUES (?, ?, ?, ?::jsonb, ?::jsonb)")) {
                ps.setString(1, name);
                ps.setString(2, arn);
                ps.setBoolean(3, fifoAttr);
                ps.setString(4, toJson(attrs).toString());
                ps.setString(5, toJson(tags).toString());
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.addProperty("TopicArn", arn);
            return out;
        });
    }

    private static boolean isDefaultAttr(String k, String v) {
        return (k.equals("DisplayName") && v.isEmpty()) || (k.equals("ContentBasedDeduplication") && v.equals("false"));
    }

    private JsonObject deleteTopic(JsonObject req) {
        String arn = Args.req(req, "TopicArn");
        String name = nameOfArn(arn);
        sh.tx(sh.owner(name), c -> {
            for (String sql : new String[] {"DELETE FROM warp_sns_subscriptions WHERE topic = ?",
                "DELETE FROM warp_sns_dedup WHERE topic = ?", "DELETE FROM warp_sns_topics WHERE name = ?"}) {
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setString(1, name);
                    ps.executeUpdate();
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private static <T extends Comparable<T>> JsonObject pageOf(String listKey, List<JsonObject> sorted, String keyField,
            String token, int size) {
        int start = 0;
        if (token != null && !token.isEmpty()) {
            String last = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
            while (start < sorted.size() && sorted.get(start).get(keyField).getAsString().compareTo(last) <= 0) {
                start++;
            }
        }
        int end = Math.min(sorted.size(), start + size);
        JsonArray a = new JsonArray();
        for (int i = start; i < end; i++) {
            a.add(sorted.get(i));
        }
        JsonObject out = new JsonObject();
        out.add(listKey, a);
        if (end < sorted.size()) {
            out.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(
                    sorted.get(end - 1).get(keyField).getAsString().getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    private JsonObject listTopics(JsonObject req) {
        List<JsonObject> all = new ArrayList<>();
        for (List<String> arns : sh.<List<String>>onAll(c -> {
            List<String> l = new ArrayList<>();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT arn FROM warp_sns_topics ORDER BY arn")) {
                while (rs.next()) {
                    l.add(rs.getString(1));
                }
            }
            return l;
        })) {
            for (String a : arns) {
                JsonObject o = new JsonObject();
                o.addProperty("TopicArn", a);
                all.add(o);
            }
        }
        all.sort((a, b) -> a.get("TopicArn").getAsString().compareTo(b.get("TopicArn").getAsString()));
        return pageOf("Topics", all, "TopicArn", Args.str(req, "NextToken"), 100);
    }

    private static String defaultPolicy(String arn, String account) {
        return "{\"Version\":\"2008-10-17\",\"Id\":\"__default_policy_ID\",\"Statement\":[{\"Sid\":\"__default_statement_ID\","
                + "\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":[\"SNS:GetTopicAttributes\","
                + "\"SNS:SetTopicAttributes\",\"SNS:AddPermission\",\"SNS:RemovePermission\",\"SNS:DeleteTopic\","
                + "\"SNS:Subscribe\",\"SNS:ListSubscriptionsByTopic\",\"SNS:Publish\"],\"Resource\":\"" + arn
                + "\",\"Condition\":{\"StringEquals\":{\"AWS:SourceOwner\":\"" + account + "\"}}}]}";
    }

    private JsonObject getTopicAttributes(JsonObject req) {
        String arn = Args.req(req, "TopicArn");
        String name = nameOfArn(arn);
        return sh.conn(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw notFound("Topic");
            }
            int confirmed = 0, pending = 0;
            try (PreparedStatement ps = c.prepareStatement("SELECT confirmed, count(*) FROM warp_sns_subscriptions WHERE topic = ? GROUP BY confirmed")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        if (rs.getBoolean(1)) {
                            confirmed = rs.getInt(2);
                        } else {
                            pending = rs.getInt(2);
                        }
                    }
                }
            }
            Map<String, String> a = new TreeMap<>();
            a.put("TopicArn", t.arn);
            a.put("Owner", rt.config.accountId);
            a.put("DisplayName", "");
            a.put("SubscriptionsConfirmed", String.valueOf(confirmed));
            a.put("SubscriptionsPending", String.valueOf(pending));
            a.put("SubscriptionsDeleted", "0");
            a.put("Policy", defaultPolicy(t.arn, rt.config.accountId));
            a.put("EffectiveDeliveryPolicy", "{\"http\":{\"defaultHealthyRetryPolicy\":{\"minDelayTarget\":20,\"maxDelayTarget\":20,"
                    + "\"numRetries\":3,\"numMaxDelayRetries\":0,\"numNoDelayRetries\":0,\"numMinDelayRetries\":0,"
                    + "\"backoffFunction\":\"linear\"},\"disableSubscriptionOverrides\":false}}");
            if (t.fifo) {
                a.put("FifoTopic", "true");
                a.put("ContentBasedDeduplication", "false");
            }
            a.putAll(t.attrs);
            JsonObject out = new JsonObject();
            out.add("Attributes", toJson(a));
            return out;
        });
    }

    private JsonObject setTopicAttributes(JsonObject req) {
        String arn = Args.req(req, "TopicArn");
        String attr = Args.req(req, "AttributeName");
        String value = Args.str(req, "AttributeValue");
        if (!TOPIC_ATTRS.contains(attr) || attr.equals("FifoTopic")) {
            throw invalid("AttributeName");
        }
        if (attr.equals("Policy") || attr.equals("DeliveryPolicy")) {
            try {
                JsonParser.parseString(value == null ? "" : value).getAsJsonObject();
            } catch (RuntimeException e) {
                throw invalid(attr + ": Invalid JSON");
            }
        }
        String name = nameOfArn(arn);
        sh.tx(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw notFound("Topic");
            }
            if (attr.equals("ContentBasedDeduplication") && !t.fifo) {
                throw invalid("AttributeName Reason: ContentBasedDeduplication is only valid for FIFO topics");
            }
            t.attrs.put(attr, value == null ? "" : value);
            updateTopicJson(c, name, "attributes", t.attrs);
            return null;
        });
        return new JsonObject();
    }

    private static void updateTopicJson(Connection c, String name, String col, Map<String, String> m) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_sns_topics SET " + col + " = ?::jsonb WHERE name = ?")) {
            ps.setString(1, toJson(m).toString());
            ps.setString(2, name);
            ps.executeUpdate();
        }
    }

    private JsonObject tagResource(JsonObject req) {
        String arn = Args.req(req, "ResourceArn");
        Map<String, String> add = tagMap(req);
        String name = nameOfArn(arn);
        sh.tx(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw new AwsException(404, "ResourceNotFound", "Resource does not exist");
            }
            t.tags.putAll(add);
            if (t.tags.size() > 50) {
                throw new AwsException(400, "TagLimitExceeded", "Could not complete request: tag quota of per resource exceeded");
            }
            updateTopicJson(c, name, "tags", t.tags);
            return null;
        });
        return new JsonObject();
    }

    private JsonObject untagResource(JsonObject req) {
        String arn = Args.req(req, "ResourceArn");
        List<String> keys = Args.strings(req, "TagKeys");
        String name = nameOfArn(arn);
        sh.tx(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw new AwsException(404, "ResourceNotFound", "Resource does not exist");
            }
            keys.forEach(t.tags::remove);
            updateTopicJson(c, name, "tags", t.tags);
            return null;
        });
        return new JsonObject();
    }

    private JsonObject listTags(JsonObject req) {
        String name = nameOfArn(Args.req(req, "ResourceArn"));
        return sh.conn(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw new AwsException(404, "ResourceNotFound", "Resource does not exist");
            }
            JsonArray a = new JsonArray();
            t.tags.forEach((k, v) -> {
                JsonObject o = new JsonObject();
                o.addProperty("Key", k);
                o.addProperty("Value", v);
                a.add(o);
            });
            JsonObject out = new JsonObject();
            out.add("Tags", a);
            return out;
        });
    }

    private JsonObject addPermission(JsonObject req) {
        String arn = Args.req(req, "TopicArn");
        String label = Args.req(req, "Label");
        List<String> accounts = Args.strings(req, "AWSAccountId");
        List<String> actions = Args.strings(req, "ActionName");
        String name = nameOfArn(arn);
        sh.tx(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw notFound("Topic");
            }
            JsonObject policy = JsonParser.parseString(t.attrs.getOrDefault("Policy", defaultPolicy(arn, rt.config.accountId)))
                    .getAsJsonObject();
            JsonArray st = policy.getAsJsonArray("Statement");
            for (JsonElement e : st) {
                if (label.equals(e.getAsJsonObject().get("Sid").getAsString())) {
                    throw invalid("Statement already exists");
                }
            }
            JsonObject s = new JsonObject();
            s.addProperty("Sid", label);
            s.addProperty("Effect", "Allow");
            JsonObject principal = new JsonObject();
            JsonArray aws = new JsonArray();
            accounts.forEach(a -> aws.add("arn:aws:iam::" + a + ":root"));
            principal.add("AWS", aws);
            s.add("Principal", principal);
            JsonArray acts = new JsonArray();
            actions.forEach(a -> acts.add("SNS:" + a));
            s.add("Action", acts);
            s.addProperty("Resource", arn);
            st.add(s);
            t.attrs.put("Policy", policy.toString());
            updateTopicJson(c, name, "attributes", t.attrs);
            return null;
        });
        return new JsonObject();
    }

    private JsonObject removePermission(JsonObject req) {
        String arn = Args.req(req, "TopicArn");
        String label = Args.req(req, "Label");
        String name = nameOfArn(arn);
        sh.tx(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw notFound("Topic");
            }
            JsonObject policy = JsonParser.parseString(t.attrs.getOrDefault("Policy", defaultPolicy(arn, rt.config.accountId)))
                    .getAsJsonObject();
            JsonArray st = policy.getAsJsonArray("Statement");
            JsonArray kept = new JsonArray();
            boolean found = false;
            for (JsonElement e : st) {
                if (label.equals(e.getAsJsonObject().get("Sid").getAsString())) {
                    found = true;
                } else {
                    kept.add(e);
                }
            }
            if (!found) {
                throw notFound("Statement");
            }
            policy.add("Statement", kept);
            t.attrs.put("Policy", policy.toString());
            updateTopicJson(c, name, "attributes", t.attrs);
            return null;
        });
        return new JsonObject();
    }

    private JsonObject putDataProtection(JsonObject req) {
        String name = nameOfArn(Args.req(req, "ResourceArn"));
        String policy = Args.req(req, "DataProtectionPolicy");
        sh.tx(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw notFound("Topic");
            }
            t.attrs.put("DataProtectionPolicy", policy);
            updateTopicJson(c, name, "attributes", t.attrs);
            return null;
        });
        return new JsonObject();
    }

    private JsonObject getDataProtection(JsonObject req) {
        String name = nameOfArn(Args.req(req, "ResourceArn"));
        return sh.conn(sh.owner(name), c -> {
            Topic t = readTopic(c, name);
            if (t == null) {
                throw notFound("Topic");
            }
            JsonObject out = new JsonObject();
            if (t.attrs.containsKey("DataProtectionPolicy")) {
                out.addProperty("DataProtectionPolicy", t.attrs.get("DataProtectionPolicy"));
            }
            return out;
        });
    }

    // ---------------------------------------------------------------------------------------------- subscriptions

    private void validateEndpoint(String protocol, String endpoint) {
        switch (protocol) {
            case "sqs" -> {
                if (endpoint == null || !endpoint.matches("^arn:aws:sqs:[a-z0-9-]+:\\d{12}:[A-Za-z0-9_.-]{1,80}$")) {
                    throw invalid("SQS endpoint ARN");
                }
            }
            case "http", "https" -> {
                if (endpoint == null || !endpoint.startsWith(protocol + "://") || endpoint.length() < protocol.length() + 4) {
                    throw invalid("Endpoint Reason: Endpoint must match the specified protocol");
                }
            }
            case "email", "email-json" -> {
                if (endpoint == null || !endpoint.contains("@")) {
                    throw invalid("Email address");
                }
            }
            case "sms" -> {
                if (endpoint == null || !endpoint.matches("^\\+?[0-9-]{5,20}$")) {
                    throw invalid("Invalid SMS endpoint: " + endpoint);
                }
            }
            case "lambda" -> {
                if (endpoint == null || !endpoint.startsWith("arn:aws:lambda:")) {
                    throw invalid("Lambda endpoint ARN");
                }
            }
            case "firehose" -> {
                if (endpoint == null || !endpoint.startsWith("arn:aws:firehose:")) {
                    throw invalid("Firehose endpoint ARN");
                }
            }
            default -> {
                if (endpoint == null || endpoint.isEmpty()) {
                    throw invalid("Endpoint");
                }
            }
        }
    }

    private static void validateSubAttribute(String name, String value) {
        if (!SUB_ATTRS.contains(name)) {
            throw invalid("AttributeName");
        }
        switch (name) {
            case "RawMessageDelivery" -> {
                if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                    throw invalid("Attributes Reason: RawMessageDelivery must be true or false");
                }
            }
            case "FilterPolicyScope" -> {
                if (!"MessageAttributes".equals(value) && !"MessageBody".equals(value)) {
                    throw invalid("Attributes Reason: FilterPolicyScope: Invalid value [" + value
                            + "]. Please use either MessageBody or MessageAttributes");
                }
            }
            case "RedrivePolicy" -> {
                try {
                    JsonObject o = JsonParser.parseString(value).getAsJsonObject();
                    if (!o.has("deadLetterTargetArn")) {
                        throw invalid("Attributes Reason: RedrivePolicy: deadLetterTargetArn is required");
                    }
                } catch (IllegalStateException | com.google.gson.JsonParseException e) {
                    throw invalid("Attributes Reason: RedrivePolicy: failed to parse JSON");
                }
            }
            case "DeliveryPolicy" -> {
                try {
                    JsonParser.parseString(value).getAsJsonObject();
                } catch (IllegalStateException | com.google.gson.JsonParseException e) {
                    throw invalid("Attributes Reason: DeliveryPolicy: failed to parse JSON");
                }
            }
            default -> {
            }
        }
    }

    private static void validateFilterPolicy(Map<String, String> attrs) {
        String fp = attrs.get("FilterPolicy");
        if (fp != null && !fp.isEmpty()) {
            try {
                SnsFilterPolicy.parse(fp, "MessageBody".equals(attrs.get("FilterPolicyScope")));
            } catch (IllegalArgumentException e) {
                throw invalid("FilterPolicy: " + e.getMessage());
            }
        }
    }

    private JsonObject subscribe(JsonObject req, Call call) {
        String topicArn = Args.req(req, "TopicArn");
        String protocol = Args.req(req, "Protocol").toLowerCase();
        String endpoint = Args.str(req, "Endpoint");
        if (!PROTOCOLS.contains(protocol)) {
            throw invalid("Protocol");
        }
        validateEndpoint(protocol, endpoint);
        Map<String, String> attrs = strMap(Args.obj(req, "Attributes"));
        attrs.forEach(SnsService::validateSubAttribute);
        validateFilterPolicy(attrs);
        boolean returnArn = Boolean.TRUE.equals(Args.bool(req, "ReturnSubscriptionArn"));
        String topicName = nameOfArn(topicArn);
        boolean needsConfirm = protocol.equals("http") || protocol.equals("https");
        String token = needsConfirm ? hex(32) : null;
        String subArn = topicArn + ":" + UUID.randomUUID();
        Object[] res = sh.tx(sh.owner(topicName), c -> {
            Topic t = readTopic(c, topicName);
            if (t == null) {
                throw notFound("Topic");
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT arn, confirmed, attributes::text FROM warp_sns_subscriptions "
                    + "WHERE topic = ? AND protocol = ? AND endpoint = ?")) {
                ps.setString(1, topicName);
                ps.setString(2, protocol);
                ps.setString(3, endpoint);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return new Object[] {rs.getString(1), rs.getBoolean(2), false};
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_subscriptions (arn, topic, topic_arn, protocol, "
                    + "endpoint, owner, attributes, confirmed, token) VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)")) {
                ps.setString(1, subArn);
                ps.setString(2, topicName);
                ps.setString(3, topicArn);
                ps.setString(4, protocol);
                ps.setString(5, endpoint);
                ps.setString(6, rt.config.accountId);
                ps.setString(7, toJson(attrs).toString());
                ps.setBoolean(8, !needsConfirm);
                ps.setString(9, token);
                ps.executeUpdate();
            }
            return new Object[] {subArn, !needsConfirm, true};
        });
        boolean confirmed = (Boolean) res[1];
        if (needsConfirm && (Boolean) res[2]) {
            String base = call == null ? "http://localhost" : call.baseUrl();
            String subscribeUrl = base + "/?Action=ConfirmSubscription&TopicArn=" + topicArn + "&Token=" + token;
            String body = SnsEnvelope.subscriptionConfirmation(rt.config.region, topicArn, UUID.randomUUID().toString(), token,
                    subscribeUrl, Instant.now());
            httpSend(endpoint, body, "SubscriptionConfirmation", topicArn, "PendingConfirmation", null, 1, null, null);
        }
        JsonObject out = new JsonObject();
        out.addProperty("SubscriptionArn", confirmed || returnArn ? (String) res[0] : "pending confirmation");
        return out;
    }

    private JsonObject confirmSubscription(JsonObject req) {
        String topicArn = Args.req(req, "TopicArn");
        String token = Args.req(req, "Token");
        String name = nameOfArn(topicArn);
        return sh.tx(sh.owner(name), c -> {
            if (readTopic(c, name) == null) {
                throw notFound("Topic");
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_sns_subscriptions SET confirmed = true WHERE topic = ? "
                    + "AND token = ? RETURNING arn")) {
                ps.setString(1, name);
                ps.setString(2, token);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw invalid("Token");
                    }
                    JsonObject out = new JsonObject();
                    out.addProperty("SubscriptionArn", rs.getString(1));
                    return out;
                }
            }
        });
    }

    private JsonObject unsubscribe(JsonObject req, Call call) {
        String arn = Args.req(req, "SubscriptionArn");
        if (arn.equals("PendingConfirmation") || arn.equals("pending confirmation")) {
            throw invalid("SubscriptionArn Reason: An ARN must have at least 6 elements, not 1");
        }
        String name = nameOfArn(arn);
        Sub removed = sh.tx(sh.owner(name), c -> {
            Sub s = null;
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_sns_subscriptions WHERE arn = ? RETURNING " + SUB_COLS)) {
                ps.setString(1, arn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        s = readSub(rs);
                    }
                }
            }
            return s;
        });
        if (removed != null && removed.confirmed && (removed.protocol.equals("http") || removed.protocol.equals("https"))) {
            String base = call == null ? "http://localhost" : call.baseUrl();
            String body = SnsEnvelope.unsubscribeConfirmation(rt.config.region, removed.topicArn, UUID.randomUUID().toString(),
                    hex(32), base + "/?Action=ConfirmSubscription&TopicArn=" + removed.topicArn + "&Token=" + hex(8), Instant.now());
            httpSend(removed.endpoint, body, "UnsubscribeConfirmation", removed.topicArn, removed.arn, null, 1, null, null);
        }
        return new JsonObject();
    }

    private JsonObject subJson(Sub s) {
        JsonObject o = new JsonObject();
        o.addProperty("SubscriptionArn", s.confirmed ? s.arn : "PendingConfirmation");
        o.addProperty("Owner", s.owner);
        o.addProperty("Protocol", s.protocol);
        o.addProperty("Endpoint", s.endpoint);
        o.addProperty("TopicArn", s.topicArn);
        return o;
    }

    private JsonObject listSubscriptions(JsonObject req, String topicArn) {
        List<JsonObject> all = new ArrayList<>();
        java.util.function.Function<Connection, List<Sub>> reader = c -> {
            List<Sub> l = new ArrayList<>();
            try {
                String sql = "SELECT " + SUB_COLS + " FROM warp_sns_subscriptions" + (topicArn != null ? " WHERE topic = ?" : "")
                        + " ORDER BY arn";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    if (topicArn != null) {
                        ps.setString(1, nameOfArn(topicArn));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            l.add(readSub(rs));
                        }
                    }
                }
            } catch (SQLException e) {
                throw AwsShards.storage(e);
            }
            return l;
        };
        if (topicArn != null) {
            String name = nameOfArn(topicArn);
            String host = sh.owner(name);
            boolean exists = sh.conn(host, c -> readTopic(c, name) != null);
            if (!exists) {
                throw notFound("Topic");
            }
            for (Sub s : sh.conn(host, c -> reader.apply(c))) {
                all.add(subJson(s));
            }
        } else {
            for (List<Sub> l : sh.onAll(c -> reader.apply(c))) {
                l.forEach(s -> all.add(subJson(s)));
            }
        }
        // page key must be the real arn, not "PendingConfirmation"
        List<JsonObject> keyed = new ArrayList<>();
        for (JsonObject o : all) {
            keyed.add(o);
        }
        all.sort((a, b) -> keyOf(a).compareTo(keyOf(b)));
        JsonObject page = pageOfSubs(all, Args.str(req, "NextToken"));
        return page;
    }

    private static String keyOf(JsonObject o) {
        return o.get("TopicArn").getAsString() + "|" + o.get("Endpoint").getAsString() + "|" + o.get("Protocol").getAsString()
                + "|" + o.get("SubscriptionArn").getAsString();
    }

    private static JsonObject pageOfSubs(List<JsonObject> sorted, String token) {
        int start = 0;
        if (token != null && !token.isEmpty()) {
            String last = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
            while (start < sorted.size() && keyOf(sorted.get(start)).compareTo(last) <= 0) {
                start++;
            }
        }
        int end = Math.min(sorted.size(), start + 100);
        JsonArray a = new JsonArray();
        for (int i = start; i < end; i++) {
            a.add(sorted.get(i));
        }
        JsonObject out = new JsonObject();
        out.add("Subscriptions", a);
        if (end < sorted.size()) {
            out.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(
                    keyOf(sorted.get(end - 1)).getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    private Sub findSub(String arn) {
        String name = nameOfArn(arn);
        Sub s = sh.conn(sh.owner(name), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT " + SUB_COLS + " FROM warp_sns_subscriptions WHERE arn = ?")) {
                ps.setString(1, arn);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? readSub(rs) : null;
                }
            }
        });
        if (s == null) {
            throw notFound("Subscription");
        }
        return s;
    }

    private JsonObject getSubscriptionAttributes(JsonObject req) {
        Sub s = findSub(Args.req(req, "SubscriptionArn"));
        Map<String, String> a = new TreeMap<>();
        a.put("SubscriptionArn", s.arn);
        a.put("TopicArn", s.topicArn);
        a.put("Protocol", s.protocol);
        a.put("Endpoint", s.endpoint);
        a.put("Owner", s.owner);
        a.put("ConfirmationWasAuthenticated", "true");
        a.put("PendingConfirmation", s.confirmed ? "false" : "true");
        a.put("RawMessageDelivery", "false");
        a.put("SubscriptionPrincipal", "arn:aws:iam::" + rt.config.accountId + ":root");
        a.putAll(s.attrs);
        JsonObject out = new JsonObject();
        out.add("Attributes", toJson(a));
        return out;
    }

    private JsonObject setSubscriptionAttributes(JsonObject req) {
        String arn = Args.req(req, "SubscriptionArn");
        String name = Args.req(req, "AttributeName");
        String value = Args.str(req, "AttributeValue");
        validateSubAttribute(name, value == null ? "" : value);
        String topic = nameOfArn(arn);
        sh.tx(sh.owner(topic), c -> {
            Sub s;
            try (PreparedStatement ps = c.prepareStatement("SELECT " + SUB_COLS + " FROM warp_sns_subscriptions WHERE arn = ? FOR UPDATE")) {
                ps.setString(1, arn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw notFound("Subscription");
                    }
                    s = readSub(rs);
                }
            }
            Map<String, String> a = new LinkedHashMap<>(s.attrs);
            if (value == null || (value.isEmpty() && (name.equals("FilterPolicy") || name.equals("RedrivePolicy")))) {
                a.remove(name);
            } else {
                a.put(name, value);
            }
            if (name.equals("FilterPolicy") || name.equals("FilterPolicyScope")) {
                validateFilterPolicy(a);
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_sns_subscriptions SET attributes = ?::jsonb WHERE arn = ?")) {
                ps.setString(1, toJson(a).toString());
                ps.setString(2, arn);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    // ---------------------------------------------------------------------------------------------- publish

    private record Msg(String topicArn, String topicName, boolean fifo, String subject, String message, boolean jsonStructure,
            Map<String, SnsFilterPolicy.Attr> attrs, JsonObject attrsRaw, String group, String dedup, String messageId) {
    }

    private record Plan(Topic topic, List<Sub> subs, String messageId, String sequence, boolean duplicate) {
    }

    private JsonObject publish(JsonObject req, Call call) {
        String phone = Args.str(req, "PhoneNumber");
        String target = Args.str(req, "TopicArn") != null ? Args.str(req, "TopicArn") : Args.str(req, "TargetArn");
        if (target == null && phone == null) {
            throw invalid("TopicArn or TargetArn or PhoneNumber");
        }
        if (target != null && phone != null) {
            throw invalid("PhoneNumber, TopicArn and TargetArn: only one may be specified");
        }
        String message = Args.str(req, "Message");
        if (message == null || message.isEmpty()) {
            throw invalid("Empty message");
        }
        String subject = Args.str(req, "Subject");
        validateSubject(subject);
        boolean structured = "json".equals(Args.str(req, "MessageStructure"));
        if (structured) {
            try {
                JsonObject o = JsonParser.parseString(message).getAsJsonObject();
                if (!o.has("default")) {
                    throw invalidValue("Message Structure - No default entry in JSON message body");
                }
            } catch (IllegalStateException | com.google.gson.JsonParseException e) {
                throw invalidValue("Message Structure - JSON message body failed to parse");
            }
        }
        JsonObject attrsRaw = Args.obj(req, "MessageAttributes");
        Map<String, SnsFilterPolicy.Attr> attrs = parseAttributes(attrsRaw);
        checkSize(message, attrsRaw);
        String group = Args.str(req, "MessageGroupId");
        String dedup = Args.str(req, "MessageDeduplicationId");
        String messageId = UUID.randomUUID().toString();
        JsonObject out = new JsonObject();
        if (phone != null) {
            recordSms(phone, message, messageId);
            out.addProperty("MessageId", messageId);
            return out;
        }
        if (target.contains(":endpoint/")) {
            recordEndpoint(target, message, messageId);
            out.addProperty("MessageId", messageId);
            return out;
        }
        String name = nameOfArn(target);
        Msg m = new Msg(target, name, false, subject, message, structured, attrs, attrsRaw, group, dedup, messageId);
        Plan plan = plan(m, call);
        out.addProperty("MessageId", plan.messageId);
        if (plan.topic.fifo) {
            out.addProperty("SequenceNumber", plan.sequence);
        }
        if (!plan.duplicate) {
            Msg real = new Msg(target, name, plan.topic.fifo, subject, message, structured, attrs, attrsRaw, group,
                    plan.topic.fifo ? dedupOf(plan.topic, dedup, message) : null, plan.messageId);
            fanOut(plan, real, call);
        }
        return out;
    }

    private static String dedupOf(Topic t, String dedup, String message) {
        return dedup != null ? dedup : sha256Hex(message);
    }

    private static void validateSubject(String subject) {
        if (subject == null) {
            return;
        }
        if (subject.length() > 100) {
            throw invalidValue("Subject must be shorter than 100 characters");
        }
        for (int i = 0; i < subject.length(); i++) {
            char c = subject.charAt(i);
            if (c < 32 || c > 126) {
                throw invalidValue("Subject must be ASCII text that begins with a letter, number, or punctuation mark; must not "
                        + "include line breaks or control characters; and must be less than 100 characters long");
            }
        }
        if (subject.isEmpty()) {
            throw invalidValue("Subject must not be empty");
        }
    }

    private static void checkSize(String message, JsonObject attrsRaw) {
        int total = message.getBytes(StandardCharsets.UTF_8).length;
        if (attrsRaw != null) {
            for (var e : attrsRaw.entrySet()) {
                total += e.getKey().getBytes(StandardCharsets.UTF_8).length;
                if (e.getValue().isJsonObject()) {
                    for (var f : e.getValue().getAsJsonObject().entrySet()) {
                        if (!f.getKey().equals("BinaryValue")) {
                            total += f.getValue().getAsString().getBytes(StandardCharsets.UTF_8).length;
                        } else {
                            total += Base64.getDecoder().decode(f.getValue().getAsString()).length;
                        }
                    }
                }
            }
        }
        if (total > MAX_MESSAGE_BYTES) {
            throw invalid("Message too long");
        }
    }

    private static Map<String, SnsFilterPolicy.Attr> parseAttributes(JsonObject raw) {
        Map<String, SnsFilterPolicy.Attr> out = new LinkedHashMap<>();
        if (raw == null) {
            return out;
        }
        if (raw.size() > 10) {
            throw invalid("Number of message attributes [" + raw.size() + "] exceeds the allowed maximum [10]");
        }
        for (var e : raw.entrySet()) {
            if (!e.getValue().isJsonObject()) {
                throw invalid("MessageAttributes");
            }
            JsonObject a = e.getValue().getAsJsonObject();
            String type = Args.str(a, "DataType");
            if (type == null || !(type.equals("String") || type.startsWith("String.") || type.equals("Number")
                    || type.startsWith("Number.") || type.equals("Binary") || type.startsWith("Binary."))) {
                throw invalidValue("The message attribute '" + e.getKey() + "' has an invalid message attribute type, the set of "
                        + "supported type prefixes is Binary, Number, and String.");
            }
            String sv = Args.str(a, "StringValue");
            String bv = Args.str(a, "BinaryValue");
            if (type.startsWith("Binary")) {
                if (bv == null) {
                    throw invalidValue("The message attribute '" + e.getKey() + "' with type 'Binary' must use field 'Binary'.");
                }
            } else if (sv == null || sv.isEmpty()) {
                throw invalidValue("The message attribute '" + e.getKey() + "' must contain non-empty message attribute value for "
                        + "message attribute type '" + type + "'.");
            }
            out.put(e.getKey(), new SnsFilterPolicy.Attr(type, type.startsWith("Binary") ? bv : sv));
        }
        return out;
    }

    private Plan plan(Msg m, Call call) {
        return sh.tx(sh.owner(m.topicName), c -> {
            Topic t = readTopic(c, m.topicName);
            if (t == null) {
                throw notFound("Topic");
            }
            if (t.fifo) {
                if (m.group == null || m.group.isEmpty()) {
                    throw invalid("The MessageGroupId parameter is required for FIFO topics");
                }
                boolean cbd = "true".equalsIgnoreCase(t.attrs.get("ContentBasedDeduplication"));
                if (m.dedup == null && !cbd) {
                    throw invalid("The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId "
                            + "provided explicitly");
                }
                String dedupId = m.dedup != null ? m.dedup : sha256Hex(m.message);
                String seq = String.valueOf(SEQUENCE.incrementAndGet());
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_sns_dedup WHERE topic = ? AND dedup_id = ? "
                        + "AND created_at < now() - interval '5 minutes'")) {
                    ps.setString(1, m.topicName);
                    ps.setString(2, dedupId);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_dedup (topic, dedup_id, message_id, sequence) "
                        + "VALUES (?, ?, ?, ?) ON CONFLICT (topic, dedup_id) DO NOTHING")) {
                    ps.setString(1, m.topicName);
                    ps.setString(2, dedupId);
                    ps.setString(3, m.messageId);
                    ps.setString(4, seq);
                    if (ps.executeUpdate() == 0) {
                        try (PreparedStatement q = c.prepareStatement("SELECT message_id, sequence FROM warp_sns_dedup WHERE topic = ? AND dedup_id = ?")) {
                            q.setString(1, m.topicName);
                            q.setString(2, dedupId);
                            try (ResultSet rs = q.executeQuery()) {
                                rs.next();
                                return new Plan(t, List.of(), rs.getString(1), rs.getString(2), true);
                            }
                        }
                    }
                }
                return new Plan(t, subsOf(c, m.topicName), m.messageId, seq, false);
            }
            if (m.group != null) {
                throw invalid("The request includes MessageGroupId parameter that is not valid for this topic type");
            }
            if (m.dedup != null) {
                throw invalid("The request includes MessageDeduplicationId parameter that is not valid for this topic type");
            }
            return new Plan(t, subsOf(c, m.topicName), m.messageId, null, false);
        });
    }

    private List<Sub> subsOf(Connection c, String topic) throws SQLException {
        List<Sub> subs = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT " + SUB_COLS + " FROM warp_sns_subscriptions WHERE topic = ? "
                + "AND confirmed = true ORDER BY created_at, arn")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    subs.add(readSub(rs));
                }
            }
        }
        return subs;
    }

    private JsonObject publishBatch(JsonObject req, Call call) {
        String topicArn = Args.req(req, "TopicArn");
        List<JsonObject> entries = Args.objects(req, "PublishBatchRequestEntries");
        if (entries.isEmpty()) {
            throw new AwsException(400, "EmptyBatchRequest", "The batch request doesn't contain any entries.");
        }
        if (entries.size() > 10) {
            throw new AwsException(400, "TooManyEntriesInBatchRequest", "The batch request contains more entries than permissible.");
        }
        java.util.Set<String> ids = new java.util.HashSet<>();
        long total = 0;
        for (JsonObject e : entries) {
            String id = Args.str(e, "Id");
            if (id == null || !ids.add(id)) {
                throw new AwsException(400, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.");
            }
            total += Args.str(e, "Message") == null ? 0 : Args.str(e, "Message").getBytes(StandardCharsets.UTF_8).length;
        }
        if (total > MAX_MESSAGE_BYTES) {
            throw new AwsException(400, "BatchRequestTooLong", "The length of all the messages put together is more than the limit.");
        }
        JsonArray ok = new JsonArray();
        JsonArray failed = new JsonArray();
        for (JsonObject e : entries) {
            JsonObject one = new JsonObject();
            one.addProperty("TopicArn", topicArn);
            for (String k : new String[] {"Message", "Subject", "MessageStructure", "MessageAttributes", "MessageGroupId",
                "MessageDeduplicationId"}) {
                if (e.has(k)) {
                    one.add(k, e.get(k));
                }
            }
            try {
                JsonObject r = publish(one, call);
                JsonObject s = new JsonObject();
                s.addProperty("Id", Args.str(e, "Id"));
                s.addProperty("MessageId", Args.str(r, "MessageId"));
                if (r.has("SequenceNumber")) {
                    s.addProperty("SequenceNumber", Args.str(r, "SequenceNumber"));
                }
                ok.add(s);
            } catch (AwsException ex) {
                if (ex.status == 404) {
                    throw ex;
                }
                JsonObject f = new JsonObject();
                f.addProperty("Id", Args.str(e, "Id"));
                f.addProperty("Code", ex.code);
                f.addProperty("Message", ex.getMessage());
                f.addProperty("SenderFault", ex.senderFault);
                failed.add(f);
            }
        }
        JsonObject out = new JsonObject();
        out.add("Successful", ok);
        out.add("Failed", failed);
        return out;
    }

    // ---------------------------------------------------------------------------------------------- delivery

    private static String protocolMessage(Msg m, String protocol) {
        if (!m.jsonStructure) {
            return m.message;
        }
        JsonObject o = JsonParser.parseString(m.message).getAsJsonObject();
        JsonElement e = o.has(protocol) ? o.get(protocol) : o.get("default");
        return e.isJsonPrimitive() ? e.getAsString() : e.toString();
    }

    private boolean passesFilter(Sub s, Msg m, String body) {
        String fp = s.attrs.get("FilterPolicy");
        if (fp == null || fp.isEmpty()) {
            return true;
        }
        boolean bodyScope = "MessageBody".equals(s.attrs.get("FilterPolicyScope"));
        try {
            JsonObject policy = SnsFilterPolicy.parse(fp, bodyScope);
            return bodyScope ? SnsFilterPolicy.matchesBody(policy, body) : SnsFilterPolicy.matchesAttributes(policy, m.attrs);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void fanOut(Plan plan, Msg m, Call call) {
        List<Object[]> records = new ArrayList<>();
        for (Sub s : plan.subs) {
            String body = protocolMessage(m, s.protocol);
            if (!passesFilter(s, m, body)) {
                continue;
            }
            boolean raw = "true".equalsIgnoreCase(s.attrs.get("RawMessageDelivery"));
            try {
                switch (s.protocol) {
                    case "sqs" -> deliverSqs(s, m, body, raw);
                    case "http", "https" -> {
                        String base = call == null ? "http://localhost" : call.baseUrl();
                        JsonObject attrs = envelopeAttrs(m);
                        String payload = raw ? body : SnsEnvelope.notification(rt.config.region, m.topicArn, m.messageId, m.subject,
                                body, attrs, base + "/?Action=Unsubscribe&SubscriptionArn=" + s.arn, Instant.now());
                        httpSend(s.endpoint, payload, "Notification", m.topicArn, s.arn, raw ? m.attrsRaw : null, 1, s, m);
                    }
                    default -> {
                        JsonObject attrs = envelopeAttrs(m);
                        String base = call == null ? "http://localhost" : call.baseUrl();
                        String payload = raw ? body : SnsEnvelope.notification(rt.config.region, m.topicArn, m.messageId, m.subject,
                                body, attrs, base + "/?Action=Unsubscribe&SubscriptionArn=" + s.arn, Instant.now());
                        if (s.protocol.equals("lambda")) {
                            payload = SnsEnvelope.lambdaEvent(s.arn, JsonParser.parseString(SnsEnvelope.notification(
                                    rt.config.region, m.topicArn, m.messageId, m.subject, body, attrs,
                                    base + "/?Action=Unsubscribe&SubscriptionArn=" + s.arn, Instant.now())).getAsJsonObject());
                        }
                        records.add(new Object[] {m.topicArn, s.arn, s.protocol, s.endpoint, m.messageId, payload});
                    }
                }
            } catch (RuntimeException e) {
                log.warn("snswire: delivery to {} {} failed: {}", s.protocol, s.endpoint, e.getMessage());
                redrive(s, m, body, raw);
            }
        }
        if (!records.isEmpty()) {
            record(plan.topic.name, records);
        }
    }

    private static JsonObject envelopeAttrs(Msg m) {
        JsonObject o = new JsonObject();
        for (var e : m.attrs.entrySet()) {
            JsonObject a = new JsonObject();
            a.addProperty("Type", e.getValue().dataType());
            a.addProperty("Value", e.getValue().stringValue());
            o.add(e.getKey(), a);
        }
        return o;
    }

    private void record(String topicName, List<Object[]> rows) {
        try {
            sh.conn(sh.owner(topicName), c -> {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_deliveries (topic_arn, subscription_arn, "
                        + "protocol, endpoint, message_id, payload) VALUES (?, ?, ?, ?, ?, ?)")) {
                    for (Object[] r : rows) {
                        for (int i = 0; i < 6; i++) {
                            ps.setString(i + 1, (String) r[i]);
                        }
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                return null;
            });
        } catch (RuntimeException e) {
            log.warn("snswire: could not record deliveries: {}", e.getMessage());
        }
    }

    private void deliverSqs(Sub s, Msg m, String body, boolean raw) {
        var sqs = rt.sqs();
        if (sqs == null) {
            throw new IllegalStateException("sqswire is not running in this Warp process, cannot deliver to " + s.endpoint);
        }
        String queue = s.endpoint.substring(s.endpoint.lastIndexOf(':') + 1);
        JsonObject req = new JsonObject();
        req.addProperty("QueueUrl", "http://sns.local/" + rt.config.accountId + "/" + queue);
        if (raw) {
            req.addProperty("MessageBody", body);
            if (m.attrsRaw != null && m.attrsRaw.size() > 0) {
                req.add("MessageAttributes", m.attrsRaw.deepCopy());
            }
        } else {
            req.addProperty("MessageBody", SnsEnvelope.notification(rt.config.region, m.topicArn, m.messageId, m.subject, body,
                    envelopeAttrs(m), "https://sns." + rt.config.region + ".amazonaws.com/?Action=Unsubscribe&SubscriptionArn="
                            + s.arn, Instant.now()));
        }
        if (m.fifo && queue.endsWith(".fifo")) {
            req.addProperty("MessageGroupId", m.group);
            req.addProperty("MessageDeduplicationId", m.dedup);
        }
        try {
            sqs.dispatch("SendMessage", req, new com.sayonora.warp.sqswire.SqsOperations.Ctx("http://sns.local"));
        } catch (SQLException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void redrive(Sub s, Msg m, String body, boolean raw) {
        String rp = s.attrs.get("RedrivePolicy");
        if (rp == null || rt.sqs() == null) {
            return;
        }
        try {
            String dlq = JsonParser.parseString(rp).getAsJsonObject().get("deadLetterTargetArn").getAsString();
            String queue = dlq.substring(dlq.lastIndexOf(':') + 1);
            JsonObject req = new JsonObject();
            req.addProperty("QueueUrl", "http://sns.local/" + rt.config.accountId + "/" + queue);
            req.addProperty("MessageBody", raw ? body : SnsEnvelope.notification(rt.config.region, m.topicArn, m.messageId,
                    m.subject, body, envelopeAttrs(m), "https://sns." + rt.config.region + ".amazonaws.com/?Action=Unsubscribe",
                    Instant.now()));
            if (raw && m.attrsRaw != null && m.attrsRaw.size() > 0) {
                req.add("MessageAttributes", m.attrsRaw.deepCopy());
            }
            if (m.fifo && queue.endsWith(".fifo")) {
                req.addProperty("MessageGroupId", m.group);
                req.addProperty("MessageDeduplicationId", m.dedup + "-dlq");
            }
            rt.sqs().dispatch("SendMessage", req, new com.sayonora.warp.sqswire.SqsOperations.Ctx("http://sns.local"));
        } catch (Exception e) {
            log.warn("snswire: redrive to the subscription's dead-letter queue failed: {}", e.getMessage());
        }
    }

    /** POSTs an SNS document to an HTTP(S) endpoint asynchronously, retrying, then falling back to the redrive policy. */
    private void httpSend(String endpoint, String body, String type, String topicArn, String subArn, JsonObject rawAttrs,
            int attempt, Sub s, Msg m) {
        String messageId = JsonParser.parseString(body.startsWith("{") ? body : "{}").getAsJsonObject().has("MessageId")
                ? JsonParser.parseString(body).getAsJsonObject().get("MessageId").getAsString() : UUID.randomUUID().toString();
        HttpRequest.Builder b;
        try {
            b = HttpRequest.newBuilder().uri(URI.create(endpoint)).timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "text/plain; charset=UTF-8")
                    .header("x-amz-sns-message-type", type)
                    .header("x-amz-sns-message-id", messageId)
                    .header("x-amz-sns-topic-arn", topicArn)
                    .header("x-amz-sns-subscription-arn", subArn)
                    .header("User-Agent", "Amazon Simple Notification Service Agent");
        } catch (IllegalArgumentException e) {
            log.warn("snswire: invalid HTTP endpoint {}", endpoint);
            return;
        }
        if (rawAttrs != null || (s != null && "true".equalsIgnoreCase(s.attrs.get("RawMessageDelivery")))) {
            b.header("x-amz-sns-rawdelivery", "true");
        }
        http.sendAsync(b.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.discarding())
                .whenComplete((resp, err) -> {
                    boolean ok = err == null && resp.statusCode() >= 200 && resp.statusCode() < 300;
                    if (ok) {
                        return;
                    }
                    if (attempt < httpAttempts) {
                        scheduler.schedule(() -> httpSend(endpoint, body, type, topicArn, subArn, rawAttrs, attempt + 1, s, m),
                                httpBackoffMs * attempt, TimeUnit.MILLISECONDS);
                    } else {
                        log.warn("snswire: delivery of {} to {} failed after {} attempts ({})", type, endpoint, attempt,
                                err != null ? err.toString() : "HTTP " + resp.statusCode());
                        if (s != null && m != null) {
                            redrive(s, m, m.message, true);
                        }
                    }
                });
    }

    // ---------------------------------------------------------------------------------------------- platform / SMS

    private JsonObject createPlatformApplication(JsonObject req) {
        String name = Args.req(req, "Name");
        String platform = Args.req(req, "Platform");
        Map<String, String> attrs = strMap(Args.obj(req, "Attributes"));
        String arn = rt.config.arn("sns", "app/" + platform + "/" + name);
        sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_platform_apps (arn, name, platform, attributes) "
                    + "VALUES (?, ?, ?, ?::jsonb) ON CONFLICT (arn) DO UPDATE SET attributes = EXCLUDED.attributes")) {
                ps.setString(1, arn);
                ps.setString(2, name);
                ps.setString(3, platform);
                ps.setString(4, toJson(attrs).toString());
                ps.executeUpdate();
            }
            return null;
        });
        JsonObject out = new JsonObject();
        out.addProperty("PlatformApplicationArn", arn);
        return out;
    }

    private JsonObject attrRow(String table, String arn, String what) {
        return sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT attributes::text FROM " + table + " WHERE arn = ?")) {
                ps.setString(1, arn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw notFound(what);
                    }
                    JsonObject out = new JsonObject();
                    out.add("Attributes", JsonParser.parseString(rs.getString(1)));
                    return out;
                }
            }
        });
    }

    private JsonObject getPlatformApplicationAttributes(JsonObject req) {
        return attrRow("warp_sns_platform_apps", Args.req(req, "PlatformApplicationArn"), "PlatformApplication");
    }

    private JsonObject setAttrRow(String table, String arn, JsonObject req, String what) {
        Map<String, String> attrs = strMap(Args.obj(req, "Attributes"));
        sh.tx(sh.home(), c -> {
            Map<String, String> cur;
            try (PreparedStatement ps = c.prepareStatement("SELECT attributes::text FROM " + table + " WHERE arn = ? FOR UPDATE")) {
                ps.setString(1, arn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw notFound(what);
                    }
                    cur = parseMap(rs.getString(1));
                }
            }
            cur.putAll(attrs);
            try (PreparedStatement ps = c.prepareStatement("UPDATE " + table + " SET attributes = ?::jsonb WHERE arn = ?")) {
                ps.setString(1, toJson(cur).toString());
                ps.setString(2, arn);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject setPlatformApplicationAttributes(JsonObject req) {
        return setAttrRow("warp_sns_platform_apps", Args.req(req, "PlatformApplicationArn"), req, "PlatformApplication");
    }

    private JsonObject deleteByArn(String table, String arn) {
        sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + table + " WHERE arn = ?")) {
                ps.setString(1, arn);
                ps.executeUpdate();
            }
            if (table.equals("warp_sns_platform_apps")) {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_sns_platform_endpoints WHERE app_arn = ?")) {
                    ps.setString(1, arn);
                    ps.executeUpdate();
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject listPlatformApplications() {
        return sh.conn(sh.home(), c -> {
            JsonArray a = new JsonArray();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery(
                    "SELECT arn, attributes::text FROM warp_sns_platform_apps ORDER BY arn")) {
                while (rs.next()) {
                    JsonObject o = new JsonObject();
                    o.addProperty("PlatformApplicationArn", rs.getString(1));
                    o.add("Attributes", JsonParser.parseString(rs.getString(2)));
                    a.add(o);
                }
            }
            JsonObject out = new JsonObject();
            out.add("PlatformApplications", a);
            return out;
        });
    }

    private JsonObject createPlatformEndpoint(JsonObject req) {
        String appArn = Args.req(req, "PlatformApplicationArn");
        String token = Args.req(req, "Token");
        Map<String, String> attrs = strMap(Args.obj(req, "Attributes"));
        return sh.tx(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM warp_sns_platform_apps WHERE arn = ?")) {
                ps.setString(1, appArn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw notFound("PlatformApplication");
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT arn FROM warp_sns_platform_endpoints WHERE app_arn = ? AND token = ?")) {
                ps.setString(1, appArn);
                ps.setString(2, token);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        JsonObject out = new JsonObject();
                        out.addProperty("EndpointArn", rs.getString(1));
                        return out;
                    }
                }
            }
            String[] p = appArn.split(":");
            String res = p.length > 5 ? p[5] : "app/GCM/x";
            String arn = rt.config.arn("sns", "endpoint/" + res.substring(res.indexOf('/') + 1) + "/" + UUID.randomUUID());
            attrs.putIfAbsent("Enabled", "true");
            attrs.put("Token", token);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_platform_endpoints (arn, app_arn, token, attributes) "
                    + "VALUES (?, ?, ?, ?::jsonb)")) {
                ps.setString(1, arn);
                ps.setString(2, appArn);
                ps.setString(3, token);
                ps.setString(4, toJson(attrs).toString());
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.addProperty("EndpointArn", arn);
            return out;
        });
    }

    private JsonObject getEndpointAttributes(JsonObject req) {
        return attrRow("warp_sns_platform_endpoints", Args.req(req, "EndpointArn"), "Endpoint");
    }

    private JsonObject setEndpointAttributes(JsonObject req) {
        return setAttrRow("warp_sns_platform_endpoints", Args.req(req, "EndpointArn"), req, "Endpoint");
    }

    private JsonObject listEndpoints(JsonObject req) {
        String app = Args.req(req, "PlatformApplicationArn");
        return sh.conn(sh.home(), c -> {
            JsonArray a = new JsonArray();
            try (PreparedStatement ps = c.prepareStatement("SELECT arn, attributes::text FROM warp_sns_platform_endpoints "
                    + "WHERE app_arn = ? ORDER BY arn")) {
                ps.setString(1, app);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject o = new JsonObject();
                        o.addProperty("EndpointArn", rs.getString(1));
                        o.add("Attributes", JsonParser.parseString(rs.getString(2)));
                        a.add(o);
                    }
                }
            }
            JsonObject out = new JsonObject();
            out.add("Endpoints", a);
            return out;
        });
    }

    private void recordEndpoint(String endpointArn, String message, String messageId) {
        boolean enabled = sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT attributes::text FROM warp_sns_platform_endpoints WHERE arn = ?")) {
                ps.setString(1, endpointArn);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw notFound("Endpoint");
                    }
                    return !"false".equalsIgnoreCase(parseMap(rs.getString(1)).get("Enabled"));
                }
            }
        });
        if (!enabled) {
            throw new AwsException(400, "EndpointDisabled", "Endpoint is disabled");
        }
        sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_deliveries (topic_arn, subscription_arn, protocol, "
                    + "endpoint, message_id, payload) VALUES (?, NULL, 'application', ?, ?, ?)")) {
                ps.setString(1, endpointArn);
                ps.setString(2, endpointArn);
                ps.setString(3, messageId);
                ps.setString(4, message);
                ps.executeUpdate();
            }
            return null;
        });
    }

    private void recordSms(String phone, String message, String messageId) {
        sh.conn(sh.home(), c -> {
            try (PreparedStatement q = c.prepareStatement("SELECT opted_out FROM warp_sns_optouts WHERE phone = ?")) {
                q.setString(1, phone);
                try (ResultSet rs = q.executeQuery()) {
                    if (rs.next() && rs.getBoolean(1)) {
                        throw new AwsException(400, "OptedOut", "Phone number is opted out");
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_deliveries (topic_arn, subscription_arn, protocol, "
                    + "endpoint, message_id, payload) VALUES ('sms', NULL, 'sms', ?, ?, ?)")) {
                ps.setString(1, phone);
                ps.setString(2, messageId);
                ps.setString(3, message);
                ps.executeUpdate();
            }
            return null;
        });
    }

    private JsonObject setSmsAttributes(JsonObject req) {
        Map<String, String> attrs = strMap(Args.obj(req, "attributes"));
        if (attrs.isEmpty()) {
            attrs = strMap(Args.obj(req, "Attributes"));
        }
        Map<String, String> fin = attrs;
        sh.tx(sh.home(), c -> {
            for (var e : fin.entrySet()) {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_sns_sms (k, v) VALUES (?, ?) "
                        + "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v")) {
                    ps.setString(1, e.getKey());
                    ps.setString(2, e.getValue());
                    ps.executeUpdate();
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject getSmsAttributes(JsonObject req) {
        List<String> want = Args.strings(req, "attributes");
        if (want.isEmpty()) {
            want = Args.strings(req, "Attributes");
        }
        List<String> w = want;
        return sh.conn(sh.home(), c -> {
            JsonObject m = new JsonObject();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT k, v FROM warp_sns_sms")) {
                while (rs.next()) {
                    if (w.isEmpty() || w.contains(rs.getString(1))) {
                        m.addProperty(rs.getString(1), rs.getString(2));
                    }
                }
            }
            JsonObject out = new JsonObject();
            out.add("attributes", m);
            return out;
        });
    }

    private JsonObject checkOptedOut(JsonObject req) {
        String phone = Args.req(req, "phoneNumber");
        return sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT opted_out FROM warp_sns_optouts WHERE phone = ?")) {
                ps.setString(1, phone);
                try (ResultSet rs = ps.executeQuery()) {
                    JsonObject out = new JsonObject();
                    out.addProperty("isOptedOut", rs.next() && rs.getBoolean(1));
                    return out;
                }
            }
        });
    }

    private JsonObject listOptedOut() {
        return sh.conn(sh.home(), c -> {
            JsonArray a = new JsonArray();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT phone FROM warp_sns_optouts WHERE opted_out ORDER BY phone")) {
                while (rs.next()) {
                    a.add(rs.getString(1));
                }
            }
            JsonObject out = new JsonObject();
            out.add("phoneNumbers", a);
            return out;
        });
    }

    private JsonObject optIn(JsonObject req) {
        String phone = Args.req(req, "phoneNumber");
        sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_sns_optouts WHERE phone = ?")) {
                ps.setString(1, phone);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    static JsonElement nullSafe(JsonElement e) {
        return e == null ? JsonNull.INSTANCE : e;
    }
}
