package com.sayonora.wire.kafkawire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.wire.core.BackendRegistry;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The in-process face of the Kafka store for the MCP tools (and tests): the same tables, batches and validation the wire protocol
 * uses, without a socket. Every method returns JSON ready for a tool result and throws {@link IllegalArgumentException} for bad input.
 */
public final class KafkaEmbedded {

    private final KafkaStore store;
    private final AtomicInteger roundRobin = new AtomicInteger();

    public KafkaEmbedded(BackendRegistry registry) {
        this.store = new KafkaStore(registry, 0);
    }

    private KafkaStore.Topic need(String topic) {
        KafkaStore.Topic t = store.topic(topic);
        if (t == null) {
            throw new IllegalArgumentException("UNKNOWN_TOPIC_OR_PARTITION: topic '" + topic + "' does not exist");
        }
        return t;
    }

    private static IllegalArgumentException bad(KafkaError e) {
        return new IllegalArgumentException(e.getMessage());
    }

    public JsonObject listTopics() {
        JsonArray arr = new JsonArray();
        for (KafkaStore.Topic t : store.topics().values()) {
            JsonObject o = new JsonObject();
            o.addProperty("name", t.name());
            o.addProperty("partitions", t.partitions());
            o.addProperty("topicId", t.id().toString());
            long messages = 0;
            for (int p = 0; p < t.partitions(); p++) {
                long[] b = store.bounds(t.name(), p);
                if (b != null) {
                    messages += b[0] - b[1];
                }
            }
            o.addProperty("messages", messages);
            arr.add(o);
        }
        JsonObject out = new JsonObject();
        out.add("topics", arr);
        return out;
    }

    public JsonObject describeTopic(String topic) {
        KafkaStore.Topic t = need(topic);
        JsonObject o = new JsonObject();
        o.addProperty("name", t.name());
        o.addProperty("topicId", t.id().toString());
        o.addProperty("partitions", t.partitions());
        JsonArray ps = new JsonArray();
        for (int p = 0; p < t.partitions(); p++) {
            long[] b = store.bounds(t.name(), p);
            JsonObject po = new JsonObject();
            po.addProperty("partition", p);
            po.addProperty("logStartOffset", b == null ? -1 : b[1]);
            po.addProperty("highWatermark", b == null ? -1 : b[0]);
            po.addProperty("host", store.owner(t.name(), p));
            ps.add(po);
        }
        o.add("partitionDetails", ps);
        JsonObject cfg = new JsonObject();
        for (KafkaConfigs.Def d : KafkaConfigs.TOPIC) {
            cfg.addProperty(d.name(), t.config().getOrDefault(d.name(), d.def()));
        }
        o.add("config", cfg);
        return o;
    }

    public JsonObject createTopic(String topic, int partitions, Map<String, String> config) {
        String badName = KafkaBroker.validateTopicName(topic);
        if (badName != null) {
            throw new IllegalArgumentException("INVALID_TOPIC_EXCEPTION: " + badName);
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("INVALID_PARTITIONS: partitions must be positive");
        }
        for (Map.Entry<String, String> c : config.entrySet()) {
            String m = KafkaConfigs.validate(c.getKey(), c.getValue());
            if (m != null) {
                throw new IllegalArgumentException("INVALID_CONFIG: " + m);
            }
        }
        try {
            KafkaStore.Topic t = store.createTopic(topic, partitions, new LinkedHashMap<>(config));
            JsonObject o = new JsonObject();
            o.addProperty("name", t.name());
            o.addProperty("partitions", t.partitions());
            o.addProperty("topicId", t.id().toString());
            return o;
        } catch (KafkaError e) {
            throw bad(e);
        }
    }

    public JsonObject deleteTopic(String topic) {
        need(topic);
        store.deleteTopic(topic);
        JsonObject o = new JsonObject();
        o.addProperty("deleted", topic);
        return o;
    }

    /** Kafka's default partitioner hash (murmur2 of the key, positive). */
    static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;
        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
            default:
                break;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    /** One record to produce; key/value are UTF-8 text unless the matching *Base64 is set. */
    public record Rec(byte[] key, byte[] value, List<byte[][]> headers, long timestamp) {
    }

    public JsonObject produce(String topic, Integer partition, List<Rec> recs) {
        KafkaStore.Topic t = need(topic);
        if (recs.isEmpty()) {
            throw new IllegalArgumentException("no records to produce");
        }
        Map<Integer, List<KBatch.Rec>> byPart = new LinkedHashMap<>();
        for (Rec r : recs) {
            int p;
            if (partition != null) {
                p = partition;
            } else if (r.key() != null) {
                p = (murmur2(r.key()) & 0x7fffffff) % t.partitions();
            } else {
                p = Math.floorMod(roundRobin.getAndIncrement(), t.partitions());
            }
            if (p < 0 || p >= t.partitions()) {
                throw new IllegalArgumentException("UNKNOWN_TOPIC_OR_PARTITION: partition " + p + " of '" + topic + "' does not exist");
            }
            byPart.computeIfAbsent(p, k -> new ArrayList<>()).add(new KBatch.Rec(0, r.timestamp(), r.key(), r.value(), r.headers()));
        }
        long now = System.currentTimeMillis();
        JsonArray results = new JsonArray();
        for (Map.Entry<Integer, List<KBatch.Rec>> e : byPart.entrySet()) {
            byte[] batch = KBatch.build(e.getValue(), now);
            KafkaStore.AppendResult a = store.append(topic, e.getKey(), KBatch.parse(batch));
            if (a.error() != 0) {
                throw new IllegalArgumentException("Kafka error " + a.error() + (a.message() == null ? "" : ": " + a.message()));
            }
            JsonObject o = new JsonObject();
            o.addProperty("partition", e.getKey());
            o.addProperty("baseOffset", a.baseOffset());
            o.addProperty("count", e.getValue().size());
            results.add(o);
        }
        JsonObject out = new JsonObject();
        out.addProperty("topic", topic);
        out.add("results", results);
        return out;
    }

    private static void put(JsonObject o, String name, byte[] b) {
        if (b == null) {
            o.add(name, com.google.gson.JsonNull.INSTANCE);
            return;
        }
        try {
            String s = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(b)).toString();
            o.addProperty(name, s);
        } catch (CharacterCodingException e) {
            o.addProperty(name, Base64.getEncoder().encodeToString(b));
            o.addProperty(name + "Encoding", "base64");
        }
    }

    public JsonObject fetch(String topic, int partition, long offset, int maxMessages, int maxBytes) {
        KafkaStore.Topic t = need(topic);
        if (partition < 0 || partition >= t.partitions()) {
            throw new IllegalArgumentException("UNKNOWN_TOPIC_OR_PARTITION: partition " + partition + " of '" + topic + "' does not exist");
        }
        long[] bd = store.bounds(topic, partition);
        long from = offset < 0 ? bd[1] : offset;
        JsonArray msgs = new JsonArray();
        long next = from;
        long fetchedBytes = 0;
        int guard = 0;
        while (msgs.size() < maxMessages && fetchedBytes < maxBytes && guard++ < 1000) {
            KafkaStore.FetchResult fr = store.fetch(topic, partition, next, Math.max(1, maxBytes - (int) fetchedBytes), true);
            if (fr.error() != 0) {
                throw new IllegalArgumentException(fr.error() == KafkaError.OFFSET_OUT_OF_RANGE
                        ? "OFFSET_OUT_OF_RANGE: offset " + from + " is outside [" + fr.logStart() + ", " + fr.highWatermark() + "]"
                        : "Kafka error " + fr.error());
            }
            if (fr.records().length == 0) {
                break;
            }
            fetchedBytes += fr.records().length;
            for (KBatch b : KBatch.parse(fr.records())) {
                List<KBatch.Rec> recs = b.records();
                if (recs == null) {
                    JsonObject o = new JsonObject();
                    o.addProperty("baseOffset", b.baseOffset());
                    o.addProperty("count", b.recordCount());
                    o.addProperty("note", "batch compressed with snappy, lz4 or zstd: not decoded by Warp");
                    o.addProperty("batchBase64", Base64.getEncoder().encodeToString(b.copy()));
                    msgs.add(o);
                    next = b.baseOffset() + b.lastOffsetDelta() + 1;
                    continue;
                }
                for (KBatch.Rec r : recs) {
                    next = Math.max(next, r.offset() + 1);
                    if (r.offset() < from || msgs.size() >= maxMessages) {
                        continue;
                    }
                    JsonObject o = new JsonObject();
                    o.addProperty("offset", r.offset());
                    o.addProperty("timestamp", r.timestamp());
                    put(o, "key", r.key());
                    put(o, "value", r.value());
                    if (!r.headers().isEmpty()) {
                        JsonArray hs = new JsonArray();
                        for (byte[][] h : r.headers()) {
                            JsonObject ho = new JsonObject();
                            ho.addProperty("key", new String(h[0], StandardCharsets.UTF_8));
                            put(ho, "value", h[1]);
                            hs.add(ho);
                        }
                        o.add("headers", hs);
                    }
                    msgs.add(o);
                }
            }
            if (next >= bd[0] && fr.highWatermark() <= next) {
                break;
            }
        }
        JsonObject out = new JsonObject();
        out.addProperty("topic", topic);
        out.addProperty("partition", partition);
        out.addProperty("logStartOffset", bd[1]);
        out.addProperty("highWatermark", bd[0]);
        out.add("messages", msgs);
        return out;
    }

    public JsonObject listGroups() {
        JsonArray arr = new JsonArray();
        for (String[] row : store.groupIds()) {
            KafkaStore.GroupRow g = store.loadGroup(row[0]);
            JsonObject o = new JsonObject();
            o.addProperty("groupId", row[0]);
            o.addProperty("protocolType", row[1]);
            o.addProperty("state", g == null ? GroupCoordinator.EMPTY : g.state());
            o.addProperty("generation", g == null ? 0 : g.generation());
            arr.add(o);
        }
        JsonObject out = new JsonObject();
        out.add("groups", arr);
        return out;
    }

    public JsonObject groupLag(String group) {
        List<KafkaStore.Committed> committed = store.committed(group, null);
        KafkaStore.GroupRow g = store.loadGroup(group);
        if (committed.isEmpty() && g == null) {
            throw new IllegalArgumentException("GROUP_ID_NOT_FOUND: group '" + group + "' has no committed offsets");
        }
        JsonArray parts = new JsonArray();
        long total = 0;
        for (KafkaStore.Committed c : committed) {
            long[] b = store.topic(c.topic()) == null ? null : store.bounds(c.topic(), c.partition());
            JsonObject o = new JsonObject();
            o.addProperty("topic", c.topic());
            o.addProperty("partition", c.partition());
            o.addProperty("committedOffset", c.offset());
            if (b == null) {
                o.add("logEndOffset", com.google.gson.JsonNull.INSTANCE);
                o.add("lag", com.google.gson.JsonNull.INSTANCE);
            } else {
                long lag = Math.max(0, b[0] - Math.max(c.offset(), b[1]));
                total += lag;
                o.addProperty("logEndOffset", b[0]);
                o.addProperty("lag", lag);
            }
            parts.add(o);
        }
        JsonObject out = new JsonObject();
        out.addProperty("groupId", group);
        out.addProperty("state", g == null ? GroupCoordinator.EMPTY : g.state());
        out.addProperty("totalLag", total);
        out.add("partitions", parts);
        return out;
    }

}
