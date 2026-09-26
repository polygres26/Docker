package com.sayonora.wire.mcp;

import static com.sayonora.wire.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.pubsubwire.PubsubEmbedded;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Google Cloud Pub/Sub vocabulary (pubsub_* tools: topics, publish, subscriptions, pull, acknowledge, modify ack deadline).
 * Each tool calls the same Publisher/Subscriber RPC implementation ({@code PsService}) pubsubwire serves over gRPC and REST,
 * with proto3 JSON requests, so ordering keys, filters, ack deadlines, dead-lettering and error statuses are the wire
 * protocol's. {@code pubsub_pull} never waits (returnImmediately) and, because it leases messages, counts as a write tool.
 * Names may be short ({@code my-topic}, with {@code project}) or full ({@code projects/p/topics/my-topic}).
 */
final class PubsubToolProvider extends StoreToolProvider {

    private static final String PUB = "google.pubsub.v1.Publisher/";
    private static final String SUB = "google.pubsub.v1.Subscriber/";
    private static final int MAX_PULL = 100;

    PubsubToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.PUBSUB, describer, stores);
    }

    private PubsubEmbedded ps() {
        return stores.engine("pubsub", PubsubEmbedded::new);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject project = str("Project id (default: WARP_MCP_GCP_PROJECT, else \"warp-project\")");
        JsonObject topic = str("Topic: short name or projects/<p>/topics/<t>");
        JsonObject sub = str("Subscription: short name or projects/<p>/subscriptions/<s>");
        JsonObject ackIds = strings("ackIds from pubsub_pull");
        return List.of(
                new Tool("pubsub_list_topics", "List the topics of a project.", schema(List.of(), "project", project,
                        "pageSize", num("Max topics (default 100)"), "pageToken", str("nextPageToken of the previous page")), false),
                new Tool("pubsub_get_topic", "Topic configuration.", schema(List.of("topic"), "project", project, "topic", topic), false),
                new Tool("pubsub_create_topic", "Create a topic.", schema(List.of("topic"), "project", project, "topic", topic,
                        "labels", obj("Labels"), "messageRetentionDuration", str("e.g. 86400s")), true),
                new Tool("pubsub_delete_topic", "Delete a topic.", schema(List.of("topic"), "project", project, "topic", topic), true),
                new Tool("pubsub_publish", "Publish one message (data as text, or dataBase64) or several (messages: [{data|dataBase64, attributes, orderingKey}]).",
                        schema(List.of("topic"), "project", project, "topic", topic, "data", str("UTF-8 text payload"),
                                "dataBase64", str("Base64 payload"), "attributes", obj("String attributes"), "orderingKey", str("Ordering key"),
                                "messages", arr("Several messages instead of data")), true),
                new Tool("pubsub_list_topic_subscriptions", "Subscriptions attached to a topic.", schema(List.of("topic"), "project", project, "topic", topic), false),
                new Tool("pubsub_list_subscriptions", "List the subscriptions of a project.", schema(List.of(), "project", project,
                        "pageSize", num("Max subscriptions (default 100)"), "pageToken", str("nextPageToken of the previous page")), false),
                new Tool("pubsub_get_subscription", "Subscription configuration.", schema(List.of("subscription"), "project", project, "subscription", sub), false),
                new Tool("pubsub_create_subscription", "Create a pull subscription on a topic.", schema(List.of("subscription", "topic"),
                        "project", project, "subscription", sub, "topic", topic, "ackDeadlineSeconds", num("10-600 (default 10)"),
                        "filter", str("Message filter expression"), "enableMessageOrdering", bool("Deliver by ordering key")), true),
                new Tool("pubsub_delete_subscription", "Delete a subscription.", schema(List.of("subscription"), "project", project, "subscription", sub), true),
                new Tool("pubsub_pull", "Pull available messages without waiting; they are leased until acknowledged or the ack deadline passes.",
                        schema(List.of("subscription"), "project", project, "subscription", sub, "maxMessages", num("1-100 (default 10)")), true),
                new Tool("pubsub_ack", "Acknowledge pulled messages.", schema(List.of("subscription", "ackIds"), "project", project,
                        "subscription", sub, "ackIds", ackIds), true),
                new Tool("pubsub_modify_ack_deadline", "Extend (or with 0, release) the lease of pulled messages.", schema(
                        List.of("subscription", "ackIds", "ackDeadlineSeconds"), "project", project, "subscription", sub, "ackIds", ackIds,
                        "ackDeadlineSeconds", num("0-600")), true));
    }

    private static String full(JsonObject a, String key, String kind) {
        String v = requireString(a, key);
        return v.startsWith("projects/") ? v : "projects/" + gcpProject(a) + "/" + kind + "/" + v;
    }

    private JsonObject rpc(String method, JsonObject body) {
        return JsonParser.parseString(ps().call(method, body.toString())).getAsJsonObject();
    }

    private static JsonObject page(JsonObject a, JsonObject b) {
        b.addProperty("pageSize", limit(a, "pageSize", 100, 1000));
        if (optString(a, "pageToken") != null) {
            b.addProperty("pageToken", optString(a, "pageToken"));
        }
        return b;
    }

    private static JsonObject message(JsonObject m) {
        JsonObject out = new JsonObject();
        out.addProperty("data", Base64.getEncoder().encodeToString(bodyArg(m, "data", "dataBase64")));
        if (m.has("attributes") && m.get("attributes").isJsonObject()) {
            JsonObject attrs = new JsonObject();
            m.getAsJsonObject("attributes").entrySet().forEach(e -> attrs.addProperty(e.getKey(), e.getValue().getAsString()));
            out.add("attributes", attrs);
        }
        if (optString(m, "orderingKey") != null) {
            out.addProperty("orderingKey", optString(m, "orderingKey"));
        }
        return out;
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        JsonObject b = new JsonObject();
        switch (tool) {
            case "pubsub_list_topics":
                b.addProperty("project", "projects/" + gcpProject(a));
                return json(rpc(PUB + "ListTopics", page(a, b)));
            case "pubsub_get_topic":
                b.addProperty("topic", full(a, "topic", "topics"));
                return json(rpc(PUB + "GetTopic", b));
            case "pubsub_create_topic":
                b.addProperty("name", full(a, "topic", "topics"));
                if (a.has("labels") && a.get("labels").isJsonObject()) {
                    b.add("labels", a.get("labels"));
                }
                if (optString(a, "messageRetentionDuration") != null) {
                    b.addProperty("messageRetentionDuration", optString(a, "messageRetentionDuration"));
                }
                return json(rpc(PUB + "CreateTopic", b));
            case "pubsub_delete_topic":
                b.addProperty("topic", full(a, "topic", "topics"));
                rpc(PUB + "DeleteTopic", b);
                return json(ok());
            case "pubsub_publish": {
                b.addProperty("topic", full(a, "topic", "topics"));
                JsonArray msgs = new JsonArray();
                if (a.has("messages") && a.get("messages").isJsonArray()) {
                    for (JsonElement e : a.getAsJsonArray("messages")) {
                        msgs.add(message(e.getAsJsonObject()));
                    }
                } else {
                    msgs.add(message(a));
                }
                b.add("messages", msgs);
                return json(rpc(PUB + "Publish", b));
            }
            case "pubsub_list_topic_subscriptions":
                b.addProperty("topic", full(a, "topic", "topics"));
                return json(rpc(PUB + "ListTopicSubscriptions", b));
            case "pubsub_list_subscriptions":
                b.addProperty("project", "projects/" + gcpProject(a));
                return json(rpc(SUB + "ListSubscriptions", page(a, b)));
            case "pubsub_get_subscription":
                b.addProperty("subscription", full(a, "subscription", "subscriptions"));
                return json(rpc(SUB + "GetSubscription", b));
            case "pubsub_create_subscription":
                b.addProperty("name", full(a, "subscription", "subscriptions"));
                b.addProperty("topic", full(a, "topic", "topics"));
                if (optInt(a, "ackDeadlineSeconds") != null) {
                    b.addProperty("ackDeadlineSeconds", optInt(a, "ackDeadlineSeconds"));
                }
                if (optString(a, "filter") != null) {
                    b.addProperty("filter", optString(a, "filter"));
                }
                if (optBool(a, "enableMessageOrdering", false)) {
                    b.addProperty("enableMessageOrdering", true);
                }
                return json(rpc(SUB + "CreateSubscription", b));
            case "pubsub_delete_subscription":
                b.addProperty("subscription", full(a, "subscription", "subscriptions"));
                rpc(SUB + "DeleteSubscription", b);
                return json(ok());
            case "pubsub_pull": {
                b.addProperty("subscription", full(a, "subscription", "subscriptions"));
                b.addProperty("maxMessages", limit(a, "maxMessages", 10, MAX_PULL));
                b.addProperty("returnImmediately", true);
                JsonObject res = rpc(SUB + "Pull", b);
                JsonArray out = new JsonArray();
                if (res.has("receivedMessages")) {
                    for (JsonElement e : res.getAsJsonArray("receivedMessages")) {
                        JsonObject r = e.getAsJsonObject();
                        JsonObject m = r.has("message") ? r.getAsJsonObject("message") : new JsonObject();
                        JsonObject o = new JsonObject();
                        o.add("ackId", r.get("ackId"));
                        o.add("messageId", m.get("messageId"));
                        o.add("publishTime", m.get("publishTime"));
                        o.add("attributes", m.has("attributes") ? m.get("attributes") : new JsonObject());
                        o.add("orderingKey", m.get("orderingKey"));
                        o.add("deliveryAttempt", r.get("deliveryAttempt"));
                        byte[] data = m.has("data") ? Base64.getDecoder().decode(m.get("data").getAsString()) : new byte[0];
                        byte[] cut = data.length > MAX_TEXT ? java.util.Arrays.copyOf(data, MAX_TEXT) : data;
                        putBody(o, cut, data.length);
                        out.add(o);
                    }
                }
                JsonObject o = new JsonObject();
                o.add("messages", out);
                o.addProperty("count", out.size());
                return json(o);
            }
            case "pubsub_ack": {
                b.addProperty("subscription", full(a, "subscription", "subscriptions"));
                b.add("ackIds", a.get("ackIds"));
                rpc(SUB + "Acknowledge", b);
                return json(ok());
            }
            case "pubsub_modify_ack_deadline": {
                b.addProperty("subscription", full(a, "subscription", "subscriptions"));
                b.add("ackIds", a.get("ackIds"));
                b.addProperty("ackDeadlineSeconds", a.get("ackDeadlineSeconds").getAsInt());
                rpc(SUB + "ModifyAckDeadline", b);
                return json(ok());
            }
            default:
                return Outcome.error("unknown pubsub tool: " + tool);
        }
    }

    private static JsonObject ok() {
        JsonObject o = new JsonObject();
        o.addProperty("ok", true);
        return o;
    }
}
