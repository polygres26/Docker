package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.amqpwire.AmqpEmbedded;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * AMQP (RabbitMQ) vocabulary (amqp_* tools: exchanges, queues, bindings, publish, look at messages, purge). Each tool calls the
 * same broker core amqpwire serves 0-9-1 clients with, so routing, queue arguments, dead-lettering and RabbitMQ's error texts are
 * the wire protocol's. {@code amqp_get_messages} never leases or removes anything. Write tools are hidden under WARP_MCP_READ_ONLY.
 */
final class AmqpToolProvider extends StoreToolProvider {

    AmqpToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.AMQP, describer, stores);
    }

    private AmqpEmbedded amqp() {
        return stores.engine("amqp", AmqpEmbedded::new);
    }

    private static String vhost(JsonObject a) {
        String v = optString(a, "vhost");
        return v == null ? "/" : v;
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject vhost = str("Virtual host (default \"/\")");
        JsonObject args = obj("Arguments table, e.g. {\"x-message-ttl\": 60000, \"x-dead-letter-exchange\": \"dlx\"}");
        return List.of(
                new Tool("amqp_list_exchanges", "List the exchanges of a vhost.", schema(List.of(), "vhost", vhost), false),
                new Tool("amqp_list_queues", "List the queues of a vhost with ready and unacknowledged message counts and the host each lives on.",
                        schema(List.of(), "vhost", vhost), false),
                new Tool("amqp_list_bindings", "List the bindings of a vhost.", schema(List.of(), "vhost", vhost), false),
                new Tool("amqp_get_messages", "Look at the messages of a queue (ready and unacknowledged, in delivery order) without consuming them.",
                        schema(List.of("queue"), "vhost", vhost, "queue", str("Queue name"), "count", num("1-100 (default 10)")), false),
                new Tool("amqp_declare_exchange", "Declare an exchange.", schema(List.of("exchange", "type"), "vhost", vhost,
                        "exchange", str("Exchange name"), "type", str("direct, fanout, topic or headers"), "durable", bool("Default true"),
                        "autoDelete", bool("Default false"), "internal", bool("Default false"), "arguments", args), true),
                new Tool("amqp_delete_exchange", "Delete an exchange.", schema(List.of("exchange"), "vhost", vhost, "exchange", str("Exchange name"),
                        "ifUnused", bool("Fail when it has bindings")), true),
                new Tool("amqp_declare_queue", "Declare a queue.", schema(List.of("queue"), "vhost", vhost, "queue", str("Queue name"),
                        "durable", bool("Default true"), "autoDelete", bool("Default false"), "arguments", args), true),
                new Tool("amqp_delete_queue", "Delete a queue and its messages.", schema(List.of("queue"), "vhost", vhost, "queue", str("Queue name"),
                        "ifUnused", bool("Fail when it has consumers"), "ifEmpty", bool("Fail when it has messages")), true),
                new Tool("amqp_bind", "Bind a queue (or, with destinationType exchange, an exchange) to an exchange.",
                        schema(List.of("source", "destination"), "vhost", vhost, "source", str("Source exchange"), "destination", str("Queue or exchange"),
                                "destinationType", str("queue (default) or exchange"), "routingKey", str("Routing key / topic pattern"), "arguments", args), true),
                new Tool("amqp_unbind", "Remove a binding.", schema(List.of("source", "destination"), "vhost", vhost, "source", str("Source exchange"),
                        "destination", str("Queue or exchange"), "destinationType", str("queue (default) or exchange"),
                        "routingKey", str("Routing key"), "arguments", args), true),
                new Tool("amqp_publish", "Publish one message (data as text, or dataBase64) to an exchange; the default exchange \"\" routes by queue name. "
                        + "Returns how many queues it reached.", schema(List.of("routingKey"), "vhost", vhost, "exchange", str("Exchange (default \"\")"),
                        "routingKey", str("Routing key"), "data", str("UTF-8 text body"), "dataBase64", str("Base64 body"),
                        "properties", obj("content_type, headers, delivery_mode, priority, correlation_id, reply_to, expiration (ms), message_id, type, app_id")), true),
                new Tool("amqp_purge_queue", "Delete every ready message of a queue.", schema(List.of("queue"), "vhost", vhost, "queue", str("Queue name")), true));
    }

    static Object plain(JsonElement e) {
        if (e == null || e.isJsonNull()) {
            return null;
        }
        if (e.isJsonPrimitive()) {
            var p = e.getAsJsonPrimitive();
            if (p.isBoolean()) {
                return p.getAsBoolean();
            }
            if (p.isNumber()) {
                double d = p.getAsDouble();
                return d == Math.rint(d) && Math.abs(d) < 1e18 ? (Object) p.getAsLong() : (Object) d;
            }
            return p.getAsString();
        }
        if (e.isJsonArray()) {
            List<Object> l = new ArrayList<>();
            e.getAsJsonArray().forEach(x -> l.add(plain(x)));
            return l;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        e.getAsJsonObject().entrySet().forEach(x -> m.put(x.getKey(), plain(x.getValue())));
        return m;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> table(JsonObject a, String key) {
        return a.has(key) && a.get(key).isJsonObject() ? (Map<String, Object>) plain(a.get(key)) : new LinkedHashMap<>();
    }

    static JsonElement toJson(Object v) {
        if (v == null) {
            return com.google.gson.JsonNull.INSTANCE;
        }
        if (v instanceof Map<?, ?> m) {
            JsonObject o = new JsonObject();
            m.forEach((k, x) -> o.add(String.valueOf(k), toJson(x)));
            return o;
        }
        if (v instanceof List<?> l) {
            JsonArray arr = new JsonArray();
            l.forEach(x -> arr.add(toJson(x)));
            return arr;
        }
        if (v instanceof Boolean b) {
            return GSON.toJsonTree(b);
        }
        if (v instanceof Number n) {
            return GSON.toJsonTree(n);
        }
        if (v instanceof byte[] b) {
            return GSON.toJsonTree(Base64.getEncoder().encodeToString(b));
        }
        return GSON.toJsonTree(v.toString());
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        String vh = vhost(a);
        try {
            switch (tool) {
                case "amqp_list_exchanges":
                    return json(toJson(amqp().exchanges(vh)));
                case "amqp_list_queues":
                    return json(toJson(amqp().queues(vh)));
                case "amqp_list_bindings":
                    return json(toJson(amqp().bindings(vh)));
                case "amqp_get_messages": {
                    List<Map<String, Object>> msgs = amqp().peek(vh, requireString(a, "queue"), limit(a, "count", 10, 100));
                    JsonArray out = new JsonArray();
                    for (Map<String, Object> m : msgs) {
                        JsonObject o = new JsonObject();
                        o.addProperty("exchange", (String) m.get("exchange"));
                        o.addProperty("routing_key", (String) m.get("routing_key"));
                        o.addProperty("redelivered", (Boolean) m.get("redelivered"));
                        o.add("properties", toJson(m.get("properties")));
                        putBody(o, (byte[]) m.get("body"), ((byte[]) m.get("body")).length);
                        out.add(o);
                    }
                    return json(out);
                }
                case "amqp_declare_exchange":
                    amqp().declareExchange(vh, requireString(a, "exchange"), requireString(a, "type"), optBool(a, "durable", true),
                            optBool(a, "autoDelete", false), optBool(a, "internal", false), table(a, "arguments"));
                    return Outcome.ok("{\"declared\":true}");
                case "amqp_delete_exchange":
                    amqp().deleteExchange(vh, requireString(a, "exchange"), optBool(a, "ifUnused", false));
                    return Outcome.ok("{\"deleted\":true}");
                case "amqp_declare_queue":
                    amqp().declareQueue(vh, requireString(a, "queue"), optBool(a, "durable", true), optBool(a, "autoDelete", false), table(a, "arguments"));
                    return Outcome.ok("{\"declared\":true}");
                case "amqp_delete_queue": {
                    long n = amqp().deleteQueue(vh, requireString(a, "queue"), optBool(a, "ifUnused", false), optBool(a, "ifEmpty", false));
                    return Outcome.ok("{\"deleted\":true,\"messages\":" + n + "}");
                }
                case "amqp_bind":
                case "amqp_unbind": {
                    boolean ex = "exchange".equals(optString(a, "destinationType"));
                    String rk = optString(a, "routingKey") == null ? "" : optString(a, "routingKey");
                    if (tool.equals("amqp_bind")) {
                        amqp().bind(vh, requireString(a, "source"), requireString(a, "destination"), ex, rk, table(a, "arguments"));
                    } else {
                        amqp().unbind(vh, requireString(a, "source"), requireString(a, "destination"), ex, rk, table(a, "arguments"));
                    }
                    return Outcome.ok("{\"ok\":true}");
                }
                case "amqp_publish": {
                    byte[] body = a.has("data") || a.has("dataBase64") ? bodyArg(a, "data", "dataBase64") : "".getBytes(StandardCharsets.UTF_8);
                    String ex = optString(a, "exchange") == null ? "" : optString(a, "exchange");
                    int n = amqp().publish(vh, ex, requireString(a, "routingKey"), table(a, "properties"), body);
                    return Outcome.ok("{\"routedToQueues\":" + n + ",\"routed\":" + (n > 0) + "}");
                }
                case "amqp_purge_queue":
                    return Outcome.ok("{\"purged\":" + amqp().purge(vh, requireString(a, "queue")) + "}");
                default:
                    return Outcome.error("unknown amqp tool: " + tool);
            }
        } catch (AmqpEmbedded.ApiError e) {
            return Outcome.error(e.getMessage());
        }
    }
}
