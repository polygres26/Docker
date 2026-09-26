package com.sayonora.wire.kafkawire;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ShardingStrategy;
import com.sayonora.wire.core.StoreBootstrap;
import com.sayonora.wire.core.StoreType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Postgres side of kafkawire. The hosts are the Postgres backends of the frontend's set that enable the {@code kafka} store
 * (declaration order == hash order); the first host is the "home" of topic metadata, the broker registry, producer ids, group state
 * and committed offsets, and the log of a partition lives on {@code hash(topic + "-" + partition)}. Every unit of work borrows a pooled
 * connection for one statement or one short transaction and returns it: nothing is pinned while a Fetch waits.
 */
public final class KafkaStore implements GroupStore {

    private static final Logger log = LoggerFactory.getLogger(KafkaStore.class);
    private static final Gson GSON = new Gson();

    public record Topic(String name, UUID id, int partitions, Map<String, String> config, long createdMs) {
    }

    public record Broker(int id, String host, int port) {
    }

    record AppendResult(int error, long baseOffset, long logStart, String message) {
    }

    record FetchResult(int error, long highWatermark, long logStart, byte[] records, int batches) {
    }

    record Committed(String topic, int partition, long offset, int leaderEpoch, String metadata, long commitMs) {
    }

    record GroupRow(String id, String state, String protocolType, String protocolName, int generation, String leader, String members) {
    }

    private final BackendRegistry registry;
    private final long cacheTtlMs;
    private volatile Map<String, Topic> topicCache = Map.of();
    private volatile long topicCacheAt;
    private volatile List<Broker> brokerCache = List.of();
    private volatile long brokerCacheAt;
    private volatile String clusterId;
    private final Object appendSignal = new Object();
    private long appendVersion;
    private final Map<String, Boolean> ensured = new ConcurrentHashMap<>();

    public KafkaStore(BackendRegistry registry, long cacheTtlMs) {
        this.registry = registry;
        this.cacheTtlMs = cacheTtlMs;
    }

    // ------------------------------------------------------------------ hosts and connections

    public boolean available() {
        return !registry.storeHosts(StoreType.KAFKA).isEmpty();
    }

    List<String> hosts() {
        List<String> h = registry.storeHosts(StoreType.KAFKA);
        if (h.isEmpty()) {
            throw new KafkaError(KafkaError.COORDINATOR_NOT_AVAILABLE, "No Postgres backend of this set has the kafka store enabled");
        }
        return h;
    }

    String home() {
        return hosts().get(0);
    }

    String owner(String topic, int part) {
        List<String> hosts = hosts();
        if (hosts.size() == 1) {
            return hosts.get(0);
        }
        return ShardingStrategy.hash(hosts).resolve(topic + "-" + part);
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("kafkawire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, StoreType.KAFKA);
        return t;
    }

    @FunctionalInterface
    interface SqlFn<T> {
        T apply(Connection c) throws SQLException;
    }

    <T> T conn(String host, SqlFn<T> fn) {
        try (Connection c = target(host).open()) {
            return fn.apply(c);
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    <T> T tx(String host, SqlFn<T> fn) {
        try (Connection c = target(host).openManualCommit()) {
            try {
                T out = fn.apply(c);
                c.commit();
                return out;
            } catch (SQLException | RuntimeException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // the pool discards the connection
                }
                throw e;
            } finally {
                try {
                    c.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // pool resets on return
                }
            }
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    static KafkaError storage(SQLException e) {
        log.error("kafkawire: Postgres store error", e);
        return new KafkaError(KafkaError.UNKNOWN_SERVER_ERROR, "The store is currently unavailable (" + e.getMessage() + ")");
    }

    <T> T home(SqlFn<T> fn) {
        return conn(home(), fn);
    }

    // ------------------------------------------------------------------ append signal (wakes long polls of this node)

    long appendVersion() {
        synchronized (appendSignal) {
            return appendVersion;
        }
    }

    /** Waits until an append happened after {@code seen} or {@code ms} elapsed. */
    void awaitAppend(long seen, long ms) {
        synchronized (appendSignal) {
            if (appendVersion == seen && ms > 0) {
                try {
                    appendSignal.wait(ms);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void signalAppend() {
        synchronized (appendSignal) {
            appendVersion++;
            appendSignal.notifyAll();
        }
    }

    // ------------------------------------------------------------------ cluster: id and brokers

    public String clusterId() {
        String id = clusterId;
        if (id == null) {
            id = home(c -> {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kafka_meta (k, v) VALUES ('cluster_id', ?) ON CONFLICT DO NOTHING")) {
                    UUID u = UUID.randomUUID();
                    ps.setString(1, uuidB64(u));
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement("SELECT v FROM warp_kafka_meta WHERE k = 'cluster_id'");
                        ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getString(1);
                }
            });
            clusterId = id;
        }
        return id;
    }

    static String uuidB64(UUID u) {
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(16);
        bb.putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits());
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());
    }

    /** Registers (or re-uses the row of) this node's advertised endpoint and returns its node id. */
    int registerBroker(String host, int port) {
        return home(c -> {
            for (int attempt = 0; attempt < 5; attempt++) {
                try (PreparedStatement ps = c.prepareStatement("SELECT node_id FROM warp_kafka_brokers WHERE host = ? AND port = ?")) {
                    ps.setString(1, host);
                    ps.setInt(2, port);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            int id = rs.getInt(1);
                            beat(c, id);
                            return id;
                        }
                    }
                }
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kafka_brokers (node_id, host, port) "
                        + "VALUES ((SELECT coalesce(max(node_id) + 1, 0) FROM warp_kafka_brokers), ?, ?) ON CONFLICT DO NOTHING")) {
                    ps.setString(1, host);
                    ps.setInt(2, port);
                    ps.executeUpdate();
                }
            }
            throw new SQLException("could not register the broker");
        });
    }

    private static void beat(Connection c, int id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_brokers SET beat = now() WHERE node_id = ?")) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }
    }

    void heartbeat(int id) {
        home(c -> {
            beat(c, id);
            return null;
        });
        brokerCacheAt = 0;
    }

    void unregister(int id) {
        try {
            home(c -> {
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_brokers SET beat = now() - interval '1 hour' WHERE node_id = ?")) {
                    ps.setInt(1, id);
                    ps.executeUpdate();
                }
                return null;
            });
        } catch (RuntimeException ignored) {
            // shutting down
        }
    }

    List<Broker> liveBrokers() {
        long now = System.currentTimeMillis();
        List<Broker> b = brokerCache;
        if (!b.isEmpty() && now - brokerCacheAt < 2000) {
            return b;
        }
        b = home(c -> {
            List<Broker> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT node_id, host, port FROM warp_kafka_brokers "
                    + "WHERE beat > now() - interval '15 seconds' ORDER BY node_id");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Broker(rs.getInt(1), rs.getString(2), rs.getInt(3)));
                }
            }
            return out;
        });
        brokerCache = b;
        brokerCacheAt = now;
        return b;
    }

    // ------------------------------------------------------------------ topics

    /** All topics (cached briefly; local writes invalidate). */
    Map<String, Topic> topics() {
        long now = System.currentTimeMillis();
        if (now - topicCacheAt < cacheTtlMs) {
            return topicCache;
        }
        Map<String, Topic> m = home(c -> {
            Map<String, Topic> out = new TreeMap<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT name, topic_id, partitions, config::text, created_ms FROM warp_kafka_topics ORDER BY name");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), new Topic(rs.getString(1), (UUID) rs.getObject(2), rs.getInt(3), parseConfig(rs.getString(4)), rs.getLong(5)));
                }
            }
            return out;
        });
        topicCache = m;
        topicCacheAt = System.currentTimeMillis();
        return m;
    }

    Topic topic(String name) {
        Topic t = topics().get(name);
        if (t == null && System.currentTimeMillis() - topicCacheAt > 25) {
            // another Warp node may have created it within the cache TTL: look once more (at most every 25 ms) before answering "unknown"
            invalidateTopics();
            t = topics().get(name);
        }
        return t;
    }

    void invalidateTopics() {
        topicCacheAt = 0;
    }

    static Map<String, String> parseConfig(String json) {
        Map<String, String> out = new LinkedHashMap<>();
        if (json != null && !json.isBlank()) {
            JsonObject o = GSON.fromJson(json, JsonObject.class);
            o.entrySet().forEach(e -> out.put(e.getKey(), e.getValue().getAsString()));
        }
        return out;
    }

    static String configJson(Map<String, String> cfg) {
        JsonObject o = new JsonObject();
        cfg.forEach(o::addProperty);
        return o.toString();
    }

    /** Creates a topic; {@code TOPIC_ALREADY_EXISTS} when the name is taken. */
    Topic createTopic(String name, int partitions, Map<String, String> config) {
        Topic t = new Topic(name, UUID.randomUUID(), partitions, config, System.currentTimeMillis());
        int n = home(c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kafka_topics (name, topic_id, partitions, config, created_ms) "
                    + "VALUES (?, ?, ?, ?::jsonb, ?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, name);
                ps.setObject(2, t.id());
                ps.setInt(3, partitions);
                ps.setString(4, configJson(config));
                ps.setLong(5, t.createdMs());
                return ps.executeUpdate();
            }
        });
        if (n == 0) {
            throw new KafkaError(KafkaError.TOPIC_ALREADY_EXISTS, "Topic '" + name + "' already exists.");
        }
        try {
            createPartitions(name, 0, partitions);
        } catch (RuntimeException e) {
            deleteTopic(name);
            throw e;
        }
        invalidateTopics();
        return t;
    }

    private void createPartitions(String name, int from, int to) {
        Map<String, List<Integer>> byHost = new LinkedHashMap<>();
        for (int p = from; p < to; p++) {
            byHost.computeIfAbsent(owner(name, p), h -> new ArrayList<>()).add(p);
        }
        for (Map.Entry<String, List<Integer>> e : byHost.entrySet()) {
            tx(e.getKey(), c -> {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kafka_parts (topic, part) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
                    for (int p : e.getValue()) {
                        ps.setString(1, name);
                        ps.setInt(2, p);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                return null;
            });
        }
    }

    boolean deleteTopic(String name) {
        int n = home(c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kafka_topics WHERE name = ?")) {
                ps.setString(1, name);
                int r = ps.executeUpdate();
                try (PreparedStatement o = c.prepareStatement("DELETE FROM warp_kafka_offsets WHERE topic = ?")) {
                    o.setString(1, name);
                    o.executeUpdate();
                }
                return r;
            }
        });
        for (String h : hosts()) {
            tx(h, c -> {
                for (String t : new String[] {"warp_kafka_log", "warp_kafka_pseq", "warp_kafka_parts"}) {
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + t + " WHERE topic = ?")) {
                        ps.setString(1, name);
                        ps.executeUpdate();
                    }
                }
                return null;
            });
        }
        invalidateTopics();
        return n > 0;
    }

    void addPartitions(String name, int newCount) {
        Topic t = topic(name);
        home(c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_topics SET partitions = ? WHERE name = ?")) {
                ps.setInt(1, newCount);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
        createPartitions(name, t == null ? 0 : t.partitions(), newCount);
        invalidateTopics();
    }

    void setConfig(String name, Map<String, String> config) {
        home(c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_topics SET config = ?::jsonb WHERE name = ?")) {
                ps.setString(1, configJson(config));
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
        invalidateTopics();
    }

    // ------------------------------------------------------------------ produce

    /** Appends already validated batches to one partition (one transaction on its host). */
    AppendResult append(String topic, int part, List<KBatch> batches) {
        String host = owner(topic, part);
        AppendResult r = tx(host, c -> {
            long logStart;
            long next;
            try (PreparedStatement ps = c.prepareStatement("SELECT log_start, next_off FROM warp_kafka_parts WHERE topic = ? AND part = ? FOR UPDATE")) {
                ps.setString(1, topic);
                ps.setInt(2, part);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return new AppendResult(KafkaError.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, null);
                    }
                    logStart = rs.getLong(1);
                    next = rs.getLong(2);
                }
            }
            long first = -1;
            boolean appended = false;
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO warp_kafka_log (topic, part, base_off, last_off, nbytes, first_ts, max_ts, batch) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                for (KBatch b : batches) {
                    int count = b.lastOffsetDelta() + 1;
                    if (b.producerId() >= 0) {
                        int[] st = null;
                        try (PreparedStatement ps = c.prepareStatement("SELECT epoch, first_seq, last_seq, base_off FROM warp_kafka_pseq "
                                + "WHERE topic = ? AND part = ? AND pid = ?")) {
                            ps.setString(1, topic);
                            ps.setInt(2, part);
                            ps.setLong(3, b.producerId());
                            try (ResultSet rs = ps.executeQuery()) {
                                if (rs.next()) {
                                    st = new int[] {rs.getInt(1), rs.getInt(2), rs.getInt(3)};
                                    long lastBase = rs.getLong(4);
                                    int epoch = b.producerEpoch();
                                    if (epoch < st[0]) {
                                        return new AppendResult(KafkaError.INVALID_PRODUCER_EPOCH, -1, logStart,
                                                "Producer's epoch " + epoch + " is older than the current epoch " + st[0]);
                                    }
                                    if (epoch == st[0]) {
                                        if (b.baseSequence() == st[1] && b.baseSequence() + count - 1 == st[2]) {
                                            if (first < 0) {
                                                first = lastBase; // a retry of the last batch: answer with its offset, append nothing
                                            }
                                            continue;
                                        }
                                        int expected = st[2] == Integer.MAX_VALUE ? 0 : st[2] + 1;
                                        if (b.baseSequence() != expected) {
                                            return new AppendResult(KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER, -1,
                                                    logStart, "Out of order sequence number for producer " + b.producerId() + " at offset " + next
                                                            + " in partition " + topic + "-" + part + ": " + b.baseSequence() + " (incoming seq. number), "
                                                            + st[2] + " (current end sequence number)");
                                        }
                                    } else if (b.baseSequence() != 0) {
                                        return new AppendResult(KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER, -1, logStart,
                                                "Invalid sequence number for new epoch of producer " + b.producerId() + ": " + b.baseSequence());
                                    }
                                }
                                // a producer this partition has no state for is accepted at any sequence, as Kafka 4 does
                            }
                        }
                    }
                    byte[] bytes = b.copy();
                    KBatch.setBaseOffset(bytes, next);
                    KBatch.setLeaderEpoch(bytes, 0);
                    ins.setString(1, topic);
                    ins.setInt(2, part);
                    ins.setLong(3, next);
                    ins.setLong(4, next + count - 1);
                    ins.setInt(5, bytes.length);
                    ins.setLong(6, b.firstTimestamp());
                    ins.setLong(7, b.maxTimestamp());
                    ins.setBytes(8, bytes);
                    ins.executeUpdate();
                    if (b.producerId() >= 0) {
                        try (PreparedStatement up = c.prepareStatement("INSERT INTO warp_kafka_pseq (topic, part, pid, epoch, first_seq, last_seq, base_off) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (topic, part, pid) DO UPDATE SET epoch = EXCLUDED.epoch, "
                                + "first_seq = EXCLUDED.first_seq, last_seq = EXCLUDED.last_seq, base_off = EXCLUDED.base_off")) {
                            up.setString(1, topic);
                            up.setInt(2, part);
                            up.setLong(3, b.producerId());
                            up.setInt(4, b.producerEpoch());
                            up.setInt(5, b.baseSequence());
                            up.setInt(6, b.baseSequence() + count - 1);
                            up.setLong(7, next);
                            up.executeUpdate();
                        }
                    }
                    if (first < 0) {
                        first = next;
                    }
                    next += count;
                    appended = true;
                }
            }
            if (appended) {
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_parts SET next_off = ? WHERE topic = ? AND part = ?")) {
                    ps.setLong(1, next);
                    ps.setString(2, topic);
                    ps.setInt(3, part);
                    ps.executeUpdate();
                }
            }
            return new AppendResult(KafkaError.NONE, first, logStart, null);
        });
        if (r.error() == KafkaError.NONE) {
            signalAppend();
        }
        return r;
    }

    // ------------------------------------------------------------------ fetch and offsets

    /** {highWatermark, logStart} of a partition, or null when it does not exist. */
    long[] bounds(String topic, int part) {
        return conn(owner(topic, part), c -> bounds(c, topic, part));
    }

    private static long[] bounds(Connection c, String topic, int part) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT next_off, log_start FROM warp_kafka_parts WHERE topic = ? AND part = ?")) {
            ps.setString(1, topic);
            ps.setInt(2, part);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new long[] {rs.getLong(1), rs.getLong(2)} : null;
            }
        }
    }

    FetchResult fetch(String topic, int part, long offset, int maxBytes, boolean allowFirstOversize) {
        return conn(owner(topic, part), c -> {
            long[] bd = bounds(c, topic, part);
            if (bd == null) {
                return new FetchResult(KafkaError.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, new byte[0], 0);
            }
            long hw = bd[0];
            long ls = bd[1];
            if (offset < ls || offset > hw) {
                return new FetchResult(KafkaError.OFFSET_OUT_OF_RANGE, hw, ls, new byte[0], 0);
            }
            if (offset == hw) {
                return new FetchResult(KafkaError.NONE, hw, ls, new byte[0], 0);
            }
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            int batches = 0;
            long from = offset;
            boolean more = true;
            while (more) {
                more = false;
                int rows = 0;
                try (PreparedStatement ps = c.prepareStatement("SELECT last_off, batch FROM warp_kafka_log WHERE topic = ? AND part = ? AND last_off >= ? "
                        + "ORDER BY base_off LIMIT 16")) {
                    ps.setString(1, topic);
                    ps.setInt(2, part);
                    ps.setLong(3, from);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            rows++;
                            byte[] b = rs.getBytes(2);
                            if (out.size() + b.length > maxBytes && !(batches == 0 && allowFirstOversize)) {
                                return new FetchResult(KafkaError.NONE, hw, ls, out.toByteArray(), batches);
                            }
                            out.write(b, 0, b.length);
                            batches++;
                            from = rs.getLong(1) + 1;
                            if (out.size() >= maxBytes) {
                                return new FetchResult(KafkaError.NONE, hw, ls, out.toByteArray(), batches);
                            }
                        }
                    }
                }
                more = rows == 16;
            }
            return new FetchResult(KafkaError.NONE, hw, ls, out.toByteArray(), batches);
        });
    }

    /** {offset, timestamp} of the first record at or after {@code ts}; {-1,-1} when none. Batch granularity for compressed batches. */
    long[] offsetForTimestamp(String topic, int part, long ts) {
        return conn(owner(topic, part), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT base_off, last_off, max_ts, batch FROM warp_kafka_log WHERE topic = ? AND part = ? AND max_ts >= ? "
                    + "ORDER BY base_off LIMIT 1")) {
                ps.setString(1, topic);
                ps.setInt(2, part);
                ps.setLong(3, ts);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return new long[] {-1, -1};
                    }
                    byte[] b = rs.getBytes(4);
                    List<KBatch.Rec> recs = new KBatch(b, 0, b.length).records();
                    if (recs != null) {
                        for (KBatch.Rec r : recs) {
                            if (r.timestamp() >= ts) {
                                return new long[] {r.offset(), r.timestamp()};
                            }
                        }
                    }
                    return new long[] {rs.getLong(1), rs.getLong(3)};
                }
            }
        });
    }

    /** {offset, timestamp} of the record with the largest timestamp (first such); {-1,-1} for an empty log. */
    long[] offsetOfMaxTimestamp(String topic, int part) {
        return conn(owner(topic, part), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT base_off, last_off, max_ts, batch FROM warp_kafka_log WHERE topic = ? AND part = ? "
                    + "ORDER BY max_ts DESC, base_off ASC LIMIT 1")) {
                ps.setString(1, topic);
                ps.setInt(2, part);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return new long[] {-1, -1};
                    }
                    byte[] b = rs.getBytes(4);
                    List<KBatch.Rec> recs = new KBatch(b, 0, b.length).records();
                    if (recs != null) {
                        for (KBatch.Rec r : recs) {
                            if (r.timestamp() == rs.getLong(3)) {
                                return new long[] {r.offset(), r.timestamp()};
                            }
                        }
                    }
                    return new long[] {rs.getLong(2), rs.getLong(3)};
                }
            }
        });
    }

    /** DeleteRecords: moves the log start to {@code offset} (-1 = high watermark). Returns the new low watermark or throws. */
    long deleteRecords(String topic, int part, long offset) {
        return tx(owner(topic, part), c -> {
            long hw;
            long ls;
            try (PreparedStatement ps = c.prepareStatement("SELECT next_off, log_start FROM warp_kafka_parts WHERE topic = ? AND part = ? FOR UPDATE")) {
                ps.setString(1, topic);
                ps.setInt(2, part);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PARTITION, "unknown partition");
                    }
                    hw = rs.getLong(1);
                    ls = rs.getLong(2);
                }
            }
            long target = offset == -1 ? hw : offset;
            if (target > hw || target < 0 && offset != -1) {
                throw new KafkaError(KafkaError.OFFSET_OUT_OF_RANGE, "The offset " + offset + " is out of range");
            }
            if (target <= ls) {
                return ls; // deleting up to or below the current low watermark is a no-op that reports the low watermark
            }
            if (target > ls) {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kafka_log WHERE topic = ? AND part = ? AND last_off < ?")) {
                    ps.setString(1, topic);
                    ps.setInt(2, part);
                    ps.setLong(3, target);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_parts SET log_start = ? WHERE topic = ? AND part = ?")) {
                    ps.setLong(1, target);
                    ps.setString(2, topic);
                    ps.setInt(3, part);
                    ps.executeUpdate();
                }
            }
            return target;
        });
    }

    // ------------------------------------------------------------------ producer ids

    long[] newProducer() {
        return home(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT nextval('warp_kafka_pid_seq')");
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                long pid = rs.getLong(1);
                try (PreparedStatement ins = c.prepareStatement("INSERT INTO warp_kafka_producers (pid, epoch, created_ms) VALUES (?, 0, ?)")) {
                    ins.setLong(1, pid);
                    ins.setLong(2, System.currentTimeMillis());
                    ins.executeUpdate();
                }
                return new long[] {pid, 0};
            }
        });
    }

    /** Epoch bump for a producer that recovers; {@code null} when the pid is unknown, {-1} when the given epoch is stale. */
    long[] bumpProducer(long pid, int epoch) {
        return home(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT epoch FROM warp_kafka_producers WHERE pid = ? FOR UPDATE")) {
                ps.setLong(1, pid);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    int cur = rs.getInt(1);
                    if (cur != epoch) {
                        return new long[] {-1};
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_producers SET epoch = epoch + 1 WHERE pid = ?")) {
                ps.setLong(1, pid);
                ps.executeUpdate();
            }
            return new long[] {pid, epoch + 1};
        });
    }

    // ------------------------------------------------------------------ groups and committed offsets (home)

    @Override
    public void saveGroup(GroupRow g) {
        home(c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kafka_groups (grp, state, protocol_type, protocol_name, generation, leader, members, updated_ms) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?) ON CONFLICT (grp) DO UPDATE SET state = EXCLUDED.state, protocol_type = EXCLUDED.protocol_type, "
                    + "protocol_name = EXCLUDED.protocol_name, generation = EXCLUDED.generation, leader = EXCLUDED.leader, members = EXCLUDED.members, "
                    + "updated_ms = EXCLUDED.updated_ms")) {
                ps.setString(1, g.id());
                ps.setString(2, g.state());
                ps.setString(3, g.protocolType());
                ps.setString(4, g.protocolName());
                ps.setInt(5, g.generation());
                ps.setString(6, g.leader());
                ps.setString(7, g.members());
                ps.setLong(8, System.currentTimeMillis());
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public GroupRow loadGroup(String id) {
        return home(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT state, protocol_type, protocol_name, generation, leader, members::text FROM warp_kafka_groups WHERE grp = ?")) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? new GroupRow(id, rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getString(5), rs.getString(6)) : null;
                }
            }
        });
    }

    /** Ids of every group known to the store: rows of the group table plus groups that only have committed offsets. */
    List<String[]> groupIds() {
        return home(c -> {
            List<String[]> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT grp, protocol_type FROM warp_kafka_groups UNION "
                    + "SELECT DISTINCT grp, '' FROM warp_kafka_offsets WHERE grp NOT IN (SELECT grp FROM warp_kafka_groups) ORDER BY 1");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new String[] {rs.getString(1), rs.getString(2)});
                }
            }
            return out;
        });
    }

    @Override
    public boolean groupExists(String id) {
        return home(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM warp_kafka_groups WHERE grp = ? UNION ALL SELECT 1 FROM warp_kafka_offsets WHERE grp = ? LIMIT 1")) {
                ps.setString(1, id);
                ps.setString(2, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        });
    }

    @Override
    public boolean deleteGroup(String id) {
        return home(c -> {
            int n;
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kafka_groups WHERE grp = ?")) {
                ps.setString(1, id);
                n = ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kafka_offsets WHERE grp = ?")) {
                ps.setString(1, id);
                n += ps.executeUpdate();
            }
            return n > 0;
        });
    }

    void commitOffsets(String group, List<Committed> offsets) {
        home(c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_kafka_offsets (grp, topic, part, committed, leader_epoch, metadata, commit_ms) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (grp, topic, part) DO UPDATE SET committed = EXCLUDED.committed, "
                    + "leader_epoch = EXCLUDED.leader_epoch, metadata = EXCLUDED.metadata, commit_ms = EXCLUDED.commit_ms")) {
                for (Committed o : offsets) {
                    ps.setString(1, group);
                    ps.setString(2, o.topic());
                    ps.setInt(3, o.partition());
                    ps.setLong(4, o.offset());
                    ps.setInt(5, o.leaderEpoch());
                    ps.setString(6, o.metadata());
                    ps.setLong(7, o.commitMs());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return null;
        });
    }

    /** Committed offsets of a group; {@code topic == null} means every topic. */
    List<Committed> committed(String group, String topic) {
        return home(c -> {
            List<Committed> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT topic, part, committed, leader_epoch, metadata, commit_ms FROM warp_kafka_offsets "
                    + "WHERE grp = ?" + (topic == null ? "" : " AND topic = ?") + " ORDER BY topic, part")) {
                ps.setString(1, group);
                if (topic != null) {
                    ps.setString(2, topic);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Committed(rs.getString(1), rs.getInt(2), rs.getLong(3), rs.getInt(4), rs.getString(5), rs.getLong(6)));
                    }
                }
            }
            return out;
        });
    }

    // ------------------------------------------------------------------ retention

    /** Applies retention.ms / retention.bytes of every topic on every host; returns the number of batches deleted. */
    int sweep(long now) {
        int deleted = 0;
        Map<String, Topic> ts = topics();
        for (Topic t : ts.values()) {
            long retMs = KafkaConfigs.longOf(t.config(), "retention.ms");
            long retBytes = KafkaConfigs.longOf(t.config(), "retention.bytes");
            String cleanup = t.config().getOrDefault("cleanup.policy", "delete");
            if (!cleanup.contains("delete")) {
                continue;
            }
            for (int p = 0; p < t.partitions(); p++) {
                final int part = p;
                if (retMs < 0 && retBytes < 0) {
                    continue;
                }
                deleted += tx(owner(t.name(), p), c -> {
                    int n = 0;
                    if (retMs >= 0) {
                        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kafka_log WHERE topic = ? AND part = ? AND max_ts < ?")) {
                            ps.setString(1, t.name());
                            ps.setInt(2, part);
                            ps.setLong(3, now - retMs);
                            n += ps.executeUpdate();
                        }
                    }
                    if (retBytes >= 0) {
                        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_kafka_log WHERE topic = ? AND part = ? AND base_off IN ("
                                + "SELECT base_off FROM (SELECT base_off, sum(nbytes) OVER (ORDER BY base_off DESC) AS s FROM warp_kafka_log WHERE topic = ? AND part = ?) q "
                                + "WHERE s > ?)")) {
                            ps.setString(1, t.name());
                            ps.setInt(2, part);
                            ps.setString(3, t.name());
                            ps.setInt(4, part);
                            ps.setLong(5, retBytes);
                            n += ps.executeUpdate();
                        }
                    }
                    if (n > 0) {
                        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_kafka_parts SET log_start = greatest(log_start, coalesce("
                                + "(SELECT min(base_off) FROM warp_kafka_log WHERE topic = ? AND part = ?), next_off)) WHERE topic = ? AND part = ?")) {
                            ps.setString(1, t.name());
                            ps.setInt(2, part);
                            ps.setString(3, t.name());
                            ps.setInt(4, part);
                            ps.executeUpdate();
                        }
                    }
                    return n;
                });
            }
        }
        return deleted;
    }
}
