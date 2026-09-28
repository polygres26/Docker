package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.kafkawire.KafkaEmbedded;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Apache Kafka vocabulary (kafka_* tools: topics, produce, fetch, consumer groups and lag) over the Warp-hosted Kafka store. Each tool
 * uses the same log tables, record batches and validation the kafkawire wire protocol serves ({@code KafkaEmbedded}). Writes
 * (create/delete topic, produce) are hidden under {@code WARP_MCP_READ_ONLY}. {@code kafka_fetch} reads by offset and never joins a consumer
 * group or commits anything.
 */
final class KafkaToolProvider extends StoreToolProvider {

    private static final int MAX_MESSAGES = 500;

    KafkaToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.KAFKASTORE, describer, stores);
    }

    private KafkaEmbedded kafka() {
        return stores.engine("kafka", KafkaEmbedded::new);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject topic = str("Topic name");
        return List.of(
                new Tool("kafka_list_topics", "List the topics with their partition counts and retained message counts.", schema(List.of()), false),
                new Tool("kafka_describe_topic", "Partitions (log start offset, high watermark, hosting backend) and configuration of a topic.",
                        schema(List.of("topic"), "topic", topic), false),
                new Tool("kafka_create_topic", "Create a topic.", schema(List.of("topic"), "topic", topic, "partitions", num("Partition count (default 1)"),
                        "config", obj("Topic configs, e.g. {\"retention.ms\":\"3600000\",\"cleanup.policy\":\"delete\"}")), true),
                new Tool("kafka_delete_topic", "Delete a topic and its messages.", schema(List.of("topic"), "topic", topic), true),
                new Tool("kafka_produce", "Produce one message (key/value as text, or keyBase64/valueBase64) or several (messages: [{key, value, headers, timestamp}]). "
                        + "Without partition the key is hashed like Kafka's default partitioner (no key: round robin).",
                        schema(List.of("topic"), "topic", topic, "partition", num("Partition (default: by key)"), "key", str("Key text"),
                                "keyBase64", str("Key bytes"), "value", str("Value text"), "valueBase64", str("Value bytes"), "headers", obj("Header name -> text"),
                                "messages", arr("Several messages instead of key/value")), true),
                new Tool("kafka_fetch", "Read messages of one partition from an offset (default: the log start) without joining a group. Values are text when UTF-8, else base64.",
                        schema(List.of("topic"), "topic", topic, "partition", num("Partition (default 0)"), "offset", num("First offset (default: log start)"),
                                "maxMessages", num("1-" + MAX_MESSAGES + " (default 10)"), "maxBytes", num("Byte budget (default 1048576)")), false),
                new Tool("kafka_list_groups", "List the consumer groups with state and generation.", schema(List.of()), false),
                new Tool("kafka_group_lag", "Committed offsets, log end offsets and lag per partition of a consumer group.",
                        schema(List.of("group"), "group", str("Consumer group id")), false));
    }

    private static byte[] bytes(JsonObject o, String text, String b64) {
        String s64 = optString(o, b64);
        if (s64 != null) {
            return Base64.getDecoder().decode(s64);
        }
        String t = optString(o, text);
        return t == null ? null : t.getBytes(StandardCharsets.UTF_8);
    }

    private static KafkaEmbedded.Rec rec(JsonObject m) {
        List<byte[][]> hs = new ArrayList<>();
        if (m.has("headers") && m.get("headers").isJsonObject()) {
            for (Map.Entry<String, JsonElement> e : m.getAsJsonObject("headers").entrySet()) {
                hs.add(new byte[][] {e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue().getAsString().getBytes(StandardCharsets.UTF_8)});
            }
        }
        Long ts = optLong(m, "timestamp");
        return new KafkaEmbedded.Rec(bytes(m, "key", "keyBase64"), bytes(m, "value", "valueBase64"), hs, ts == null ? 0 : ts);
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        try {
            switch (tool) {
                case "kafka_list_topics":
                    return json(kafka().listTopics());
                case "kafka_describe_topic":
                    return json(kafka().describeTopic(requireString(a, "topic")));
                case "kafka_create_topic": {
                    Map<String, String> cfg = new LinkedHashMap<>();
                    if (a.has("config") && a.get("config").isJsonObject()) {
                        a.getAsJsonObject("config").entrySet().forEach(e -> cfg.put(e.getKey(), e.getValue().getAsString()));
                    }
                    Long p = optLong(a, "partitions");
                    return json(kafka().createTopic(requireString(a, "topic"), p == null ? 1 : p.intValue(), cfg));
                }
                case "kafka_delete_topic":
                    return json(kafka().deleteTopic(requireString(a, "topic")));
                case "kafka_produce": {
                    List<KafkaEmbedded.Rec> recs = new ArrayList<>();
                    if (a.has("messages") && a.get("messages").isJsonArray()) {
                        JsonArray arr = a.getAsJsonArray("messages");
                        for (JsonElement e : arr) {
                            recs.add(rec(e.getAsJsonObject()));
                        }
                    } else {
                        recs.add(rec(a));
                    }
                    Long p = optLong(a, "partition");
                    return json(kafka().produce(requireString(a, "topic"), p == null ? null : p.intValue(), recs));
                }
                case "kafka_fetch": {
                    Long p = optLong(a, "partition");
                    Long off = optLong(a, "offset");
                    Long max = optLong(a, "maxMessages");
                    Long bytes = optLong(a, "maxBytes");
                    int n = (int) Math.max(1, Math.min(MAX_MESSAGES, max == null ? 10 : max));
                    return json(kafka().fetch(requireString(a, "topic"), p == null ? 0 : p.intValue(), off == null ? -1 : off, n,
                            bytes == null ? 1_048_576 : bytes.intValue()));
                }
                case "kafka_list_groups":
                    return json(kafka().listGroups());
                case "kafka_group_lag":
                    return json(kafka().groupLag(requireString(a, "group")));
                default:
                    return Outcome.error("unknown kafka tool: " + tool);
            }
        } catch (IllegalArgumentException e) {
            return Outcome.error(e.getMessage());
        }
    }
}
