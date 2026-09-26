package com.sayonora.wire.pubsubwire;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.Snapshot;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * All SQL of the Pub/Sub store. Catalog methods run on the project's home host; queue methods take the host that owns the
 * subscription (see {@link PsShards}). Nothing here blocks: waiting for messages is done by the callers between calls.
 */
final class PsStore {

    final PsShards shards;

    PsStore(PsShards shards) {
        this.shards = shards;
    }

    // ------------------------------------------------------------------------------------------ helpers

    static Timestamp ts(Instant i) {
        return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
    }

    static Instant instant(Timestamp t) {
        return Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
    }

    static OffsetDateTime odt(Instant i) {
        return i.atOffset(ZoneOffset.UTC);
    }

    static Instant micros(Instant i) {
        return Instant.ofEpochSecond(i.getEpochSecond(), i.getNano() / 1000 * 1000);
    }

    static Instant readInstant(ResultSet rs, String col) throws SQLException {
        OffsetDateTime o = rs.getObject(col, OffsetDateTime.class);
        return o == null ? null : o.toInstant();
    }

    // ------------------------------------------------------------------------------------------ topics

    void insertTopic(String project, Topic t) {
        String home = shards.home(project);
        shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_topics (name, project, doc) VALUES (?,?,?)")) {
                ps.setString(1, t.getName());
                ps.setString(2, project);
                ps.setBytes(3, t.toByteArray());
                ps.executeUpdate();
            }
            return null;
        });
    }

    Topic getTopic(String project, String name) {
        return shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT doc FROM warp_pubsub_topics WHERE name=?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? parse(Topic.parser(), rs.getBytes(1)) : null;
                }
            }
        });
    }

    void updateTopic(String project, Topic t) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_pubsub_topics SET doc=? WHERE name=?")) {
                ps.setBytes(1, t.toByteArray());
                ps.setString(2, t.getName());
                ps.executeUpdate();
            }
            return null;
        });
    }

    List<Topic> listTopics(String project, String after, int limit) {
        return shards.conn(shards.home(project), c -> {
            List<Topic> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT doc FROM warp_pubsub_topics WHERE project=? AND name > ? ORDER BY name LIMIT ?")) {
                ps.setString(1, project);
                ps.setString(2, after);
                ps.setInt(3, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(parse(Topic.parser(), rs.getBytes(1)));
                    }
                }
            }
            return out;
        });
    }

    /** Deletes a topic; its subscriptions stay, pointing at "_deleted-topic_". Returns false when it did not exist. */
    boolean deleteTopic(String project, String name) {
        return shards.tx(shards.home(project), c -> {
            int n;
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_topics WHERE name=?")) {
                ps.setString(1, name);
                n = ps.executeUpdate();
            }
            if (n == 0) {
                return false;
            }
            Map<String, byte[]> docs = new LinkedHashMap<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT name, doc FROM warp_pubsub_subs WHERE topic=?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        docs.put(rs.getString(1), parse(Subscription.parser(), rs.getBytes(2)).toBuilder()
                                .setTopic("_deleted-topic_").build().toByteArray());
                    }
                }
            }
            for (var e : docs.entrySet()) {
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_pubsub_subs SET topic='_deleted-topic_', doc=? WHERE name=?")) {
                    ps.setBytes(1, e.getValue());
                    ps.setString(2, e.getKey());
                    ps.executeUpdate();
                }
            }
            for (String sql : new String[] {
                "DELETE FROM warp_pubsub_snapshots WHERE topic=?",
                "DELETE FROM warp_pubsub_iam WHERE resource=?"}) {
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setString(1, name);
                    ps.executeUpdate();
                }
            }
            return true;
        });
    }

    // ------------------------------------------------------------------------------------------ subscriptions

    record SubRow(String name, String project, String topic, Subscription doc, String filter, boolean detached) {
    }

    private static SubRow subRow(ResultSet rs) throws SQLException {
        return new SubRow(rs.getString("name"), rs.getString("project"), rs.getString("topic"),
                parse(Subscription.parser(), rs.getBytes("doc")), rs.getString("filter"), rs.getBoolean("detached"));
    }

    void insertSub(String project, Subscription s, String filter) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_pubsub_subs (name, project, topic, doc, filter, push) VALUES (?,?,?,?,?,?)")) {
                ps.setString(1, s.getName());
                ps.setString(2, project);
                ps.setString(3, s.getTopic());
                ps.setBytes(4, s.toByteArray());
                ps.setString(5, filter);
                ps.setBoolean(6, !s.getPushConfig().getPushEndpoint().isEmpty());
                ps.executeUpdate();
            }
            return null;
        });
    }

    SubRow getSub(String project, String name) {
        return shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_pubsub_subs WHERE name=?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? subRow(rs) : null;
                }
            }
        });
    }

    void updateSub(String project, Subscription s) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE warp_pubsub_subs SET doc=?, push=?, detached=? WHERE name=?")) {
                ps.setBytes(1, s.toByteArray());
                ps.setBoolean(2, !s.getPushConfig().getPushEndpoint().isEmpty());
                ps.setBoolean(3, s.getDetached());
                ps.setString(4, s.getName());
                ps.executeUpdate();
            }
            return null;
        });
    }

    List<SubRow> listSubs(String project, String after, int limit) {
        return shards.conn(shards.home(project), c -> {
            List<SubRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT * FROM warp_pubsub_subs WHERE project=? AND name > ? ORDER BY name LIMIT ?")) {
                ps.setString(1, project);
                ps.setString(2, after);
                ps.setInt(3, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(subRow(rs));
                    }
                }
            }
            return out;
        });
    }

    /** Subscriptions of a topic (not detached), in name order, for listing and for Publish fan-out. */
    List<SubRow> topicSubs(String project, String topic, String after, int limit, boolean includeDetached) {
        return shards.conn(shards.home(project), c -> {
            List<SubRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_pubsub_subs WHERE topic=? AND name > ? "
                    + (includeDetached ? "" : "AND NOT detached ") + "ORDER BY name LIMIT ?")) {
                ps.setString(1, topic);
                ps.setString(2, after);
                ps.setInt(3, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(subRow(rs));
                    }
                }
            }
            return out;
        });
    }

    /** All push subscriptions of every project on the home host(s). */
    List<SubRow> pushSubs() {
        List<SubRow> out = new ArrayList<>();
        for (String h : shards.allHosts().subList(0, Math.min(1, shards.allHosts().size()))) {
            out.addAll(shards.conn(h, c -> {
                List<SubRow> l = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_pubsub_subs WHERE push AND NOT detached");
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        l.add(subRow(rs));
                    }
                }
                return l;
            }));
        }
        return out;
    }

    List<SubRow> allSubs() {
        List<SubRow> out = new ArrayList<>();
        List<String> hosts = shards.allHosts();
        if (hosts.isEmpty()) {
            return out;
        }
        out.addAll(shards.conn(hosts.get(0), c -> {
            List<SubRow> l = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_pubsub_subs");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    l.add(subRow(rs));
                }
            }
            return l;
        }));
        return out;
    }

    boolean deleteSub(String project, String name) {
        boolean existed = shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_subs WHERE name=?")) {
                ps.setString(1, name);
                return ps.executeUpdate() > 0;
            } finally {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_iam WHERE resource=?")) {
                    ps.setString(1, name);
                    ps.executeUpdate();
                }
            }
        });
        if (existed) {
            shards.conn(shards.owner(project, name), c -> {
                for (String sql : new String[] {"DELETE FROM warp_pubsub_msgs WHERE sub=?", "DELETE FROM warp_pubsub_hold WHERE sub=?",
                    "DELETE FROM warp_pubsub_snapmsgs WHERE sub=?"}) {
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        ps.setString(1, name);
                        ps.executeUpdate();
                    }
                }
                return null;
            });
        }
        return existed;
    }

    // ------------------------------------------------------------------------------------------ snapshots

    record SnapRow(String name, String topic, String originSub, Snapshot doc, Instant createdAt, Instant expireAt) {
    }

    private static SnapRow snapRow(ResultSet rs) throws SQLException {
        return new SnapRow(rs.getString("name"), rs.getString("topic"), rs.getString("origin_sub"),
                parse(Snapshot.parser(), rs.getBytes("doc")), readInstant(rs, "created_at"), readInstant(rs, "expire_at"));
    }

    void insertSnapshot(String project, SnapRow s) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_snapshots "
                    + "(name, project, topic, origin_sub, doc, created_at, expire_at) VALUES (?,?,?,?,?,?,?)")) {
                ps.setString(1, s.name());
                ps.setString(2, project);
                ps.setString(3, s.topic());
                ps.setString(4, s.originSub());
                ps.setBytes(5, s.doc().toByteArray());
                ps.setObject(6, odt(s.createdAt()));
                ps.setObject(7, odt(s.expireAt()));
                ps.executeUpdate();
            }
            return null;
        });
    }

    SnapRow getSnapshot(String project, String name) {
        return shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_pubsub_snapshots WHERE name=? AND expire_at > now()")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? snapRow(rs) : null;
                }
            }
        });
    }

    void updateSnapshot(String project, SnapRow s) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_pubsub_snapshots SET doc=?, expire_at=? WHERE name=?")) {
                ps.setBytes(1, s.doc().toByteArray());
                ps.setObject(2, odt(s.expireAt()));
                ps.setString(3, s.name());
                ps.executeUpdate();
            }
            return null;
        });
    }

    List<SnapRow> listSnapshots(String project, String topic, String after, int limit) {
        return shards.conn(shards.home(project), c -> {
            List<SnapRow> out = new ArrayList<>();
            String sql = topic == null
                    ? "SELECT * FROM warp_pubsub_snapshots WHERE project=? AND name > ? AND expire_at > now() ORDER BY name LIMIT ?"
                    : "SELECT * FROM warp_pubsub_snapshots WHERE topic=? AND name > ? AND expire_at > now() ORDER BY name LIMIT ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, topic == null ? project : topic);
                ps.setString(2, after);
                ps.setInt(3, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(snapRow(rs));
                    }
                }
            }
            return out;
        });
    }

    boolean deleteSnapshot(String project, SnapRow s) {
        boolean ok = shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_snapshots WHERE name=?")) {
                ps.setString(1, s.name());
                return ps.executeUpdate() > 0;
            }
        });
        if (ok) {
            shards.conn(shards.owner(project, s.originSub()), c -> {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_snapmsgs WHERE snapshot=?")) {
                    ps.setString(1, s.name());
                    ps.executeUpdate();
                }
                return null;
            });
        }
        return ok;
    }

    /** Expired snapshots are removed by the sweeper. */
    void sweepSnapshots() {
        for (String h : shards.allHosts()) {
            shards.conn(h, c -> {
                List<String> gone = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_snapshots WHERE expire_at <= now() RETURNING name");
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        gone.add(rs.getString(1));
                    }
                }
                for (String g : gone) {
                    for (String h2 : shards.allHosts()) {
                        shards.conn(h2, c2 -> {
                            try (PreparedStatement ps = c2.prepareStatement("DELETE FROM warp_pubsub_snapmsgs WHERE snapshot=?")) {
                                ps.setString(1, g);
                                ps.executeUpdate();
                            }
                            return null;
                        });
                    }
                }
                return null;
            });
        }
    }

    // ------------------------------------------------------------------------------------------ IAM & schemas

    byte[] getIam(String project, String resource) {
        return shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT policy FROM warp_pubsub_iam WHERE resource=?")) {
                ps.setString(1, resource);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getBytes(1) : null;
                }
            }
        });
    }

    void setIam(String project, String resource, byte[] policy) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_iam (resource, policy) VALUES (?,?) "
                    + "ON CONFLICT (resource) DO UPDATE SET policy = EXCLUDED.policy")) {
                ps.setString(1, resource);
                ps.setBytes(2, policy);
                ps.executeUpdate();
            }
            return null;
        });
    }

    void insertSchema(String project, Schema s) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_schemas (name, project, doc) VALUES (?,?,?)")) {
                ps.setString(1, s.getName());
                ps.setString(2, project);
                ps.setBytes(3, s.toByteArray());
                ps.executeUpdate();
            }
            return null;
        });
    }

    Schema getSchema(String project, String name) {
        return shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT doc FROM warp_pubsub_schemas WHERE name=?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? parse(Schema.parser(), rs.getBytes(1)) : null;
                }
            }
        });
    }

    List<Schema> listSchemas(String project, String after, int limit) {
        return shards.conn(shards.home(project), c -> {
            List<Schema> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT doc FROM warp_pubsub_schemas WHERE project=? AND name > ? ORDER BY name LIMIT ?")) {
                ps.setString(1, project);
                ps.setString(2, after);
                ps.setInt(3, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(parse(Schema.parser(), rs.getBytes(1)));
                    }
                }
            }
            return out;
        });
    }

    boolean deleteSchema(String project, String name) {
        return shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_schemas WHERE name=?")) {
                ps.setString(1, name);
                return ps.executeUpdate() > 0;
            }
        });
    }

    // ------------------------------------------------------------------------------------------ outbox

    long outboxInsert(String project, String topic, byte[] payload) {
        return shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_outbox (topic, payload) VALUES (?,?) RETURNING id")) {
                ps.setString(1, topic);
                ps.setBytes(2, payload);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    void outboxDelete(String project, long id) {
        shards.conn(shards.home(project), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_outbox WHERE id=?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
            return null;
        });
    }

    record OutboxRow(long id, String topic, byte[] payload) {
    }

    List<OutboxRow> outboxStale(String host, int limit) {
        return shards.conn(host, c -> {
            List<OutboxRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT id, topic, payload FROM warp_pubsub_outbox "
                    + "WHERE created_at < now() - interval '5 seconds' ORDER BY id LIMIT ?")) {
                ps.setInt(1, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new OutboxRow(rs.getLong(1), rs.getString(2), rs.getBytes(3)));
                    }
                }
            }
            return out;
        });
    }

    void outboxDeleteOn(String host, long id) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_outbox WHERE id=?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
            return null;
        });
    }

    // ------------------------------------------------------------------------------------------ queues

    /** One message destined for one subscription's queue. */
    record Enqueue(String sub, PubsubMessage msg) {
    }

    /** Inserts the messages (idempotent on (sub, message id)); one transaction per call. */
    int insertMessages(String host, List<Enqueue> batch) {
        if (batch.isEmpty()) {
            return 0;
        }
        return shards.tx(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_msgs "
                    + "(sub, msg_id, data, attrs, ordering_key, publish_time) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING")) {
                for (Enqueue e : batch) {
                    PubsubMessage m = e.msg();
                    ps.setString(1, e.sub());
                    ps.setString(2, m.getMessageId());
                    ps.setBytes(3, m.getData().toByteArray());
                    ps.setString(4, attrsJson(m.getAttributesMap()));
                    ps.setString(5, m.getOrderingKey());
                    ps.setObject(6, odt(instant(m.getPublishTime())));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return batch.size();
        });
    }

    static String attrsJson(Map<String, String> m) {
        if (m.isEmpty()) {
            return "{}";
        }
        com.google.gson.JsonObject o = new com.google.gson.JsonObject();
        new java.util.TreeMap<>(m).forEach(o::addProperty);
        return o.toString();
    }

    static Map<String, String> attrsFrom(String json) {
        Map<String, String> out = new LinkedHashMap<>();
        if (json != null && json.length() > 2) {
            com.google.gson.JsonObject o = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
            o.entrySet().forEach(e -> out.put(e.getKey(), e.getValue().getAsString()));
        }
        return out;
    }

    record Delivered(long seq, int attempt, String token, PubsubMessage msg) {
    }

    /**
     * Leases up to {@code max} deliverable messages of a queue. {@code ordered}: only the oldest unacked message of each
     * ordering key is eligible (one outstanding message per key; redelivery keeps the order). {@code maxAttempts > 0}: with
     * {@code deadMode} false skips messages that already used their attempts, with {@code deadMode} true returns exactly
     * those (to be forwarded to the dead-letter topic) without counting a delivery.
     */
    List<Delivered> claim(String host, String sub, int max, long leaseMs, boolean ordered, int maxAttempts, boolean deadMode,
            long backoffMinMs, long backoffMaxMs) {
        String order = ordered ? " AND (c.ordering_key = '' OR NOT EXISTS (SELECT 1 FROM warp_pubsub_msgs o WHERE o.sub = c.sub "
                + "AND o.ordering_key = c.ordering_key AND NOT o.acked AND o.seq < c.seq))" : "";
        String attempts = maxAttempts > 0 ? (deadMode ? " AND c.delivery_attempt >= ?" : " AND c.delivery_attempt < ?") : "";
        String bump = deadMode ? "0" : "1";
        String extra = backoffMinMs >= 0 ? " + (LEAST(?::float8, ?::float8 * power(2, LEAST(GREATEST(m.delivery_attempt + " + bump
                + " - 1, 0), 40))) * interval '1 millisecond')" : "";
        String sql = "UPDATE warp_pubsub_msgs m SET visible_at = now() + (?::bigint * interval '1 millisecond')" + extra
                + ", delivery_attempt = m.delivery_attempt + " + bump
                + ", ack_token = substr(md5(random()::text || m.seq::text), 1, 12) "
                + "FROM (SELECT c.sub, c.seq FROM warp_pubsub_msgs c WHERE c.sub = ? AND NOT c.acked AND c.visible_at <= now()"
                + order + attempts + " ORDER BY c.seq LIMIT ? FOR UPDATE SKIP LOCKED) x "
                + "WHERE m.sub = x.sub AND m.seq = x.seq "
                + "RETURNING m.seq, m.msg_id, m.data, m.attrs, m.ordering_key, m.publish_time, m.delivery_attempt, m.ack_token";
        return shards.conn(host, c -> {
            List<Delivered> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                int i = 1;
                ps.setLong(i++, leaseMs);
                if (backoffMinMs >= 0) {
                    ps.setDouble(i++, backoffMaxMs);
                    ps.setDouble(i++, backoffMinMs);
                }
                ps.setString(i++, sub);
                if (maxAttempts > 0) {
                    ps.setInt(i++, maxAttempts);
                }
                ps.setInt(i, max);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        PubsubMessage.Builder b = PubsubMessage.newBuilder().setMessageId(rs.getString(2))
                                .setData(ByteString.copyFrom(rs.getBytes(3))).putAllAttributes(attrsFrom(rs.getString(4)))
                                .setOrderingKey(rs.getString(5)).setPublishTime(ts(readInstant(rs, "publish_time")));
                        out.add(new Delivered(rs.getLong(1), rs.getInt(7), rs.getString(8), b.build()));
                    }
                }
            }
            out.sort((a, b) -> Long.compare(a.seq(), b.seq()));
            return out;
        });
    }

    enum AckResult { OK, INVALID }

    /** Acknowledges by sequence number. With {@code strict} (exactly-once) an id only counts when its token is the current one and
     * its lease has not expired. Ids that are already gone succeed silently on at-least-once subscriptions. */
    Map<String, AckResult> ack(String host, String sub, List<String> ackIds, boolean strict, boolean retain) {
        Map<String, AckResult> out = new LinkedHashMap<>();
        List<PsAckId> ids = new ArrayList<>();
        List<String> raw = new ArrayList<>();
        for (String s : ackIds) {
            PsAckId d = PsAckId.decode(s);
            if (d == null) {
                out.put(s, AckResult.INVALID);
            } else {
                ids.add(d);
                raw.add(s);
            }
        }
        if (ids.isEmpty()) {
            return out;
        }
        String done = retain ? "UPDATE warp_pubsub_msgs SET acked = TRUE, acked_at = now()"
                : "DELETE FROM warp_pubsub_msgs";
        shards.conn(host, c -> {
            if (!strict) {
                Long[] seqs = ids.stream().map(PsAckId::seq).toArray(Long[]::new);
                try (PreparedStatement ps = c.prepareStatement(done + " WHERE sub = ? AND seq = ANY(?) AND NOT acked")) {
                    ps.setString(1, sub);
                    ps.setArray(2, c.createArrayOf("bigint", seqs));
                    ps.executeUpdate();
                }
                for (String r : raw) {
                    out.put(r, AckResult.OK);
                }
            } else {
                try (PreparedStatement ps = c.prepareStatement(done
                        + " WHERE sub = ? AND seq = ? AND ack_token = ? AND delivery_attempt = ? AND visible_at > now() AND NOT acked")) {
                    for (PsAckId d : ids) {
                        ps.setString(1, sub);
                        ps.setLong(2, d.seq());
                        ps.setString(3, d.token());
                        ps.setInt(4, d.attempt());
                        ps.addBatch();
                    }
                    int[] r = ps.executeBatch();
                    for (int i = 0; i < r.length; i++) {
                        out.put(raw.get(i), r[i] > 0 || r[i] == java.sql.Statement.SUCCESS_NO_INFO ? AckResult.OK : AckResult.INVALID);
                    }
                }
            }
            return null;
        });
        return out;
    }

    /** Changes leases: {@code leaseMs == 0} makes the messages deliverable again (after the retry backoff, if any). */
    Map<String, AckResult> modack(String host, String sub, List<String> ackIds, long leaseMs, boolean strict, long backoffMinMs,
            long backoffMaxMs) {
        Map<String, AckResult> out = new LinkedHashMap<>();
        List<PsAckId> ids = new ArrayList<>();
        List<String> raw = new ArrayList<>();
        for (String s : ackIds) {
            PsAckId d = PsAckId.decode(s);
            if (d == null) {
                out.put(s, AckResult.INVALID);
            } else {
                ids.add(d);
                raw.add(s);
            }
        }
        if (ids.isEmpty()) {
            return out;
        }
        String extra = backoffMinMs >= 0 ? " + (LEAST(?::float8, ?::float8 * power(2, LEAST(GREATEST(delivery_attempt - 1, 0), 40)))"
                + " * interval '1 millisecond')" : "";
        String sql = "UPDATE warp_pubsub_msgs SET visible_at = now() + (?::bigint * interval '1 millisecond')" + extra
                + " WHERE sub = ? AND seq = ?" + (strict ? " AND ack_token = ? AND delivery_attempt = ? AND visible_at > now()" : "")
                + " AND NOT acked";
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                for (PsAckId d : ids) {
                    int i = 1;
                    ps.setLong(i++, leaseMs);
                    if (backoffMinMs >= 0) {
                        ps.setDouble(i++, backoffMaxMs);
                        ps.setDouble(i++, backoffMinMs);
                    }
                    ps.setString(i++, sub);
                    ps.setLong(i++, d.seq());
                    if (strict) {
                        ps.setString(i++, d.token());
                        ps.setInt(i, d.attempt());
                    }
                    ps.addBatch();
                }
                int[] r = ps.executeBatch();
                for (int i = 0; i < r.length; i++) {
                    boolean ok = !strict || r[i] > 0 || r[i] == java.sql.Statement.SUCCESS_NO_INFO;
                    out.put(raw.get(i), ok ? AckResult.OK : AckResult.INVALID);
                }
            }
            return null;
        });
        return out;
    }

    /** Acknowledges by sequence number without token checks (used after forwarding to the dead-letter topic). */
    void dropSeqs(String host, String sub, List<Long> seqs) {
        if (seqs.isEmpty()) {
            return;
        }
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_msgs WHERE sub = ? AND seq = ANY(?)")) {
                ps.setString(1, sub);
                ps.setArray(2, c.createArrayOf("bigint", seqs.toArray(new Long[0])));
                ps.executeUpdate();
            }
            return null;
        });
    }

    void dropAll(String host, String sub) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_pubsub_msgs WHERE sub = ?")) {
                ps.setString(1, sub);
                ps.executeUpdate();
            }
            return null;
        });
    }

    boolean hasHold(String host, String sub) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM warp_pubsub_hold WHERE sub=?")) {
                ps.setString(1, sub);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        });
    }

    void setHold(String host, String sub) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_hold (sub) VALUES (?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, sub);
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** Records the unacked message ids of a queue for a snapshot; returns the publish time of the oldest one (or null). */
    Instant snapshotState(String host, String snapshot, String sub) {
        return shards.tx(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_pubsub_snapmsgs (snapshot, sub, msg_id) "
                    + "SELECT ?, sub, msg_id FROM warp_pubsub_msgs WHERE sub = ? AND NOT acked ON CONFLICT DO NOTHING")) {
                ps.setString(1, snapshot);
                ps.setString(2, sub);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT min(publish_time) FROM warp_pubsub_msgs WHERE sub = ? AND NOT acked")) {
                ps.setString(1, sub);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    OffsetDateTime o = rs.getObject(1, OffsetDateTime.class);
                    return o == null ? null : o.toInstant();
                }
            }
        });
    }

    List<String> snapshotUnacked(String host, String snapshot) {
        return shards.conn(host, c -> {
            List<String> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT msg_id FROM warp_pubsub_snapmsgs WHERE snapshot = ?")) {
                ps.setString(1, snapshot);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString(1));
                    }
                }
            }
            return out;
        });
    }

    /**
     * Seek: messages published before {@code time} become acknowledged, the others (and, when {@code replayIds} is given, those
     * ids regardless of age) become deliverable again with their leases cleared.
     */
    void seek(String host, String sub, Instant time, List<String> replayIds, boolean retain) {
        shards.tx(host, c -> {
            String ackSql = retain ? "UPDATE warp_pubsub_msgs SET acked = TRUE, acked_at = now()" : "DELETE FROM warp_pubsub_msgs";
            Array arr = replayIds == null ? null : c.createArrayOf("text", replayIds.toArray(new String[0]));
            try (PreparedStatement ps = c.prepareStatement(ackSql + " WHERE sub = ? AND NOT acked AND publish_time <= ?"
                    + (arr != null ? " AND msg_id <> ALL(?)" : ""))) {
                ps.setString(1, sub);
                ps.setObject(2, odt(time));
                if (arr != null) {
                    ps.setArray(3, arr);
                }
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_pubsub_msgs SET acked = FALSE, acked_at = NULL, "
                    + "visible_at = now(), delivery_attempt = 0, ack_token = NULL WHERE sub = ? AND (publish_time > ?"
                    + (arr != null ? " OR msg_id = ANY(?)" : "") + ")")) {
                ps.setString(1, sub);
                ps.setObject(2, odt(time));
                if (arr != null) {
                    ps.setArray(3, arr);
                }
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** Seek to a timestamp: acks strictly older messages, replays the rest. */
    void seekTime(String host, String sub, Instant time, boolean retain) {
        Instant t = time.minusNanos(1000); // publish_time < time  <=>  publish_time <= time - 1us
        seek(host, sub, t, null, retain);
    }

    long backlog(String host, String sub) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_pubsub_msgs WHERE sub = ? AND NOT acked")) {
                ps.setString(1, sub);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    /** Retention sweep of one queue; returns rows removed. */
    int sweepQueue(String host, String sub, long retentionSeconds, boolean retainAcked) {
        return shards.conn(host, c -> {
            int n = 0;
            try (PreparedStatement ps = c.prepareStatement(
                    "DELETE FROM warp_pubsub_msgs WHERE sub = ? AND publish_time < now() - (?::bigint * interval '1 second')")) {
                ps.setString(1, sub);
                ps.setLong(2, retentionSeconds);
                n += ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "DELETE FROM warp_pubsub_msgs WHERE sub = ? AND acked AND acked_at < now() - interval '1 hour' AND NOT ?")) {
                ps.setString(1, sub);
                ps.setBoolean(2, retainAcked);
                n += ps.executeUpdate();
            }
            return n;
        });
    }

    static <T extends com.google.protobuf.Message> T parse(com.google.protobuf.Parser<T> p, byte[] b) {
        try {
            return p.parseFrom(b);
        } catch (InvalidProtocolBufferException e) {
            throw PsException.internal("corrupt stored document: " + e.getMessage());
        }
    }
}
