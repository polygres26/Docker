package com.sayonora.wire.amqpwire;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** All SQL of the AMQP store. Exchanges/bindings/nodes/outbox are read and written on the home host; queues and messages on the owner host. */
final class AmqpStore {

    record ExchangeDef(String vhost, String name, String type, boolean durable, boolean autoDelete, boolean internal,
            Map<String, Object> args) {
    }

    record BindingDef(String vhost, String source, String dest, char destType, String rkey, Map<String, Object> args) {
        String argsKey() {
            return AmqpCodec.canonical(args);
        }
    }

    record QueueDef(String vhost, String name, boolean durable, String exclOwner, boolean autoDelete, Map<String, Object> args,
            int maxPrio, Long ttlMs, Long maxLen, Long maxBytes, String overflow, String dlx, String dlxRkey, Long expiresMs) {
    }

    record MsgRow(long seq, String vhost, String queue, String msgId, String exchange, String rkey, byte[] props, byte[] body,
            int priority, boolean persistent, boolean redelivered, long enqMs, byte[] deaths, byte[] raw10, int dcount) {
    }

    record NewMsg(String msgId, String exchange, String rkey, byte[] props, byte[] body, Long msgTtlMs, int prio, boolean persistent,
            boolean redelivered, byte[] deaths, byte[] raw10) {
    }

    enum Ins { INSERTED, MISSING, REJECTED }

    record OutboxRow(long id, byte[] payload) {
    }

    final AmqpShards shards;

    AmqpStore(AmqpShards shards) {
        this.shards = shards;
    }

    // ------------------------------------------------------------------------------------------ exchanges (home)

    private static final String XCOLS = "vhost, name, type, durable, auto_delete, internal, args";

    private static ExchangeDef exchangeOf(ResultSet rs) throws SQLException {
        return new ExchangeDef(rs.getString(1), rs.getString(2), rs.getString(3), rs.getBoolean(4), rs.getBoolean(5), rs.getBoolean(6),
                AmqpCodec.decodeTable(rs.getBytes(7)));
    }

    ExchangeDef exchange(String vhost, String name) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT " + XCOLS + " FROM warp_amqp_exchanges WHERE vhost=? AND name=?")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? exchangeOf(rs) : null;
                }
            }
        });
    }

    List<ExchangeDef> exchanges(String vhost) {
        return shards.conn(shards.home(), c -> {
            List<ExchangeDef> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + XCOLS + " FROM warp_amqp_exchanges WHERE vhost=? ORDER BY name")) {
                ps.setString(1, vhost);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(exchangeOf(rs));
                    }
                }
            }
            return out;
        });
    }

    boolean insertExchange(ExchangeDef d) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_amqp_exchanges (" + XCOLS + ") VALUES (?,?,?,?,?,?,?) "
                    + "ON CONFLICT (vhost, name) DO NOTHING")) {
                ps.setString(1, d.vhost());
                ps.setString(2, d.name());
                ps.setString(3, d.type());
                ps.setBoolean(4, d.durable());
                ps.setBoolean(5, d.autoDelete());
                ps.setBoolean(6, d.internal());
                ps.setBytes(7, AmqpCodec.encodeTable(d.args()));
                return ps.executeUpdate() == 1;
            }
        });
    }

    /** Deletes the exchange and every binding it is the source or an exchange destination of. */
    boolean deleteExchange(String vhost, String name) {
        return shards.tx(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_bindings WHERE vhost=? AND (source=? OR (dest=? AND dest_type='e'))")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                ps.setString(3, name);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_exchanges WHERE vhost=? AND name=?")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                return ps.executeUpdate() == 1;
            }
        });
    }

    void ensureVhost(String vhost, List<ExchangeDef> defaults) {
        for (ExchangeDef d : defaults) {
            insertExchange(d);
        }
    }

    // ------------------------------------------------------------------------------------------ bindings (home)

    List<BindingDef> bindings(String vhost) {
        return shards.conn(shards.home(), c -> {
            List<BindingDef> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT source, dest, dest_type, rkey, args FROM warp_amqp_bindings WHERE vhost=? ORDER BY id")) {
                ps.setString(1, vhost);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new BindingDef(vhost, rs.getString(1), rs.getString(2), rs.getString(3).charAt(0), rs.getString(4),
                                AmqpCodec.decodeTable(rs.getBytes(5))));
                    }
                }
            }
            return out;
        });
    }

    boolean addBinding(BindingDef b) {
        return shards.tx(shards.home(), c -> {
            boolean ins;
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_amqp_bindings (vhost, source, dest, dest_type, rkey, args, args_key) "
                    + "VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, b.vhost());
                ps.setString(2, b.source());
                ps.setString(3, b.dest());
                ps.setString(4, String.valueOf(b.destType()));
                ps.setString(5, b.rkey());
                ps.setBytes(6, AmqpCodec.encodeTable(b.args()));
                ps.setString(7, b.argsKey());
                ins = ps.executeUpdate() == 1;
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_amqp_exchanges SET had_binding=TRUE WHERE vhost=? AND name=?")) {
                ps.setString(1, b.vhost());
                ps.setString(2, b.source());
                ps.executeUpdate();
            }
            return ins;
        });
    }

    boolean removeBinding(BindingDef b) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_bindings WHERE vhost=? AND source=? AND dest=? AND dest_type=? AND rkey=? AND args_key=?")) {
                ps.setString(1, b.vhost());
                ps.setString(2, b.source());
                ps.setString(3, b.dest());
                ps.setString(4, String.valueOf(b.destType()));
                ps.setString(5, b.rkey());
                ps.setString(6, b.argsKey());
                return ps.executeUpdate() > 0;
            }
        });
    }

    /** Auto-delete exchange check: true when the exchange had a binding once and has none now. */
    boolean autoDeletable(String vhost, String name) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT e.auto_delete AND e.had_binding AND NOT EXISTS (SELECT 1 FROM warp_amqp_bindings b "
                    + "WHERE b.vhost=e.vhost AND b.source=e.name) FROM warp_amqp_exchanges e WHERE e.vhost=? AND e.name=?")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() && rs.getBoolean(1);
                }
            }
        });
    }

    /** Removes every binding to a queue; returns the source exchanges that had one. */
    Set<String> deleteQueueBindings(String vhost, String queue) {
        return shards.conn(shards.home(), c -> {
            Set<String> out = new HashSet<>();
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_bindings WHERE vhost=? AND dest=? AND dest_type='q' RETURNING source")) {
                ps.setString(1, vhost);
                ps.setString(2, queue);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString(1));
                    }
                }
            }
            return out;
        });
    }

    // ------------------------------------------------------------------------------------------ nodes / outbox (home)

    void beat(String node) {
        shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_amqp_nodes (node, beat) VALUES (?, now()) "
                    + "ON CONFLICT (node) DO UPDATE SET beat = now()")) {
                ps.setString(1, node);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_nodes WHERE beat < now() - interval '10 minutes'")) {
                ps.executeUpdate();
            }
            return null;
        });
    }

    void bye(String node) {
        shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_nodes WHERE node=?")) {
                ps.setString(1, node);
                ps.executeUpdate();
            }
            return null;
        });
    }

    Set<String> liveNodes(long graceSeconds) {
        return shards.conn(shards.home(), c -> {
            Set<String> out = new HashSet<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT node FROM warp_amqp_nodes WHERE beat > now() - (? * interval '1 second')")) {
                ps.setLong(1, graceSeconds);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString(1));
                    }
                }
            }
            return out;
        });
    }

    long outboxInsert(byte[] payload) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_amqp_outbox (payload) VALUES (?) RETURNING id")) {
                ps.setBytes(1, payload);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    void outboxDelete(long id) {
        shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_outbox WHERE id=?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
            return null;
        });
    }

    List<OutboxRow> outboxStale(int limit, long olderThanSeconds) {
        return shards.tx(shards.home(), c -> {
            List<OutboxRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT id, payload FROM warp_amqp_outbox WHERE created_at < now() - (? * interval '1 second') "
                    + "ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED")) {
                ps.setLong(1, olderThanSeconds);
                ps.setInt(2, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new OutboxRow(rs.getLong(1), rs.getBytes(2)));
                    }
                }
            }
            return out;
        });
    }

    // ------------------------------------------------------------------------------------------ queues (owner)

    private static final String QCOLS = "vhost, name, durable, excl_owner, auto_delete, args, max_prio, ttl_ms, max_len, max_bytes, overflow, dlx, dlx_rkey, expires_ms";

    private static Long nl(ResultSet rs, int i) throws SQLException {
        long v = rs.getLong(i);
        return rs.wasNull() ? null : v;
    }

    private static QueueDef queueOf(ResultSet rs) throws SQLException {
        return new QueueDef(rs.getString(1), rs.getString(2), rs.getBoolean(3), rs.getString(4), rs.getBoolean(5),
                AmqpCodec.decodeTable(rs.getBytes(6)), rs.getInt(7), nl(rs, 8), nl(rs, 9), nl(rs, 10), rs.getString(11), rs.getString(12),
                rs.getString(13), nl(rs, 14));
    }

    QueueDef queue(String vhost, String name) {
        return shards.conn(shards.owner(vhost, name), c -> queue(c, vhost, name));
    }

    private QueueDef queue(Connection c, String vhost, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + QCOLS + " FROM warp_amqp_queues WHERE vhost=? AND name=?")) {
            ps.setString(1, vhost);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? queueOf(rs) : null;
            }
        }
    }

    List<QueueDef> queuesOn(String host, String vhost) {
        return shards.conn(host, c -> {
            List<QueueDef> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + QCOLS + " FROM warp_amqp_queues WHERE vhost=? ORDER BY name")) {
                ps.setString(1, vhost);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(queueOf(rs));
                    }
                }
            }
            return out;
        });
    }

    boolean insertQueue(QueueDef q) {
        return shards.conn(shards.owner(q.vhost(), q.name()), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_amqp_queues (" + QCOLS + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (vhost, name) DO NOTHING")) {
                ps.setString(1, q.vhost());
                ps.setString(2, q.name());
                ps.setBoolean(3, q.durable());
                ps.setString(4, q.exclOwner());
                ps.setBoolean(5, q.autoDelete());
                ps.setBytes(6, AmqpCodec.encodeTable(q.args()));
                ps.setInt(7, q.maxPrio());
                setLong(ps, 8, q.ttlMs());
                setLong(ps, 9, q.maxLen());
                setLong(ps, 10, q.maxBytes());
                ps.setString(11, q.overflow());
                ps.setString(12, q.dlx());
                ps.setString(13, q.dlxRkey());
                setLong(ps, 14, q.expiresMs());
                return ps.executeUpdate() == 1;
            }
        });
    }

    private static void setLong(PreparedStatement ps, int i, Long v) throws SQLException {
        if (v == null) {
            ps.setNull(i, Types.BIGINT);
        } else {
            ps.setLong(i, v);
        }
    }

    /** Deletes the queue and its messages; returns the number of messages that were in it, or -1 when it did not exist. */
    long deleteQueue(String vhost, String name) {
        return shards.tx(shards.owner(vhost, name), c -> {
            long n = 0;
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_msgs WHERE vhost=? AND queue=? RETURNING holder IS NULL")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        n += rs.getBoolean(1) ? 1 : 0; // RabbitMQ reports the ready messages only
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_queues WHERE vhost=? AND name=?")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                return ps.executeUpdate() == 1 ? n : -1;
            }
        });
    }

    void touch(String vhost, String name) {
        shards.conn(shards.owner(vhost, name), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_amqp_queues SET last_used = now() WHERE vhost=? AND name=?")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
    }

    long ready(String vhost, String name) {
        return shards.conn(shards.owner(vhost, name), c -> ready(c, vhost, name));
    }

    private static long ready(Connection c, String vhost, String name) throws SQLException {
        // like RabbitMQ, expired messages behind a live head still count until they reach the head and are dropped
        try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_amqp_msgs WHERE vhost=? AND queue=? AND holder IS NULL")) {
            ps.setString(1, vhost);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    long unacked(String vhost, String name) {
        return shards.conn(shards.owner(vhost, name), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_amqp_msgs WHERE vhost=? AND queue=? AND holder IS NOT NULL")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    long purge(String vhost, String name) {
        return shards.conn(shards.owner(vhost, name), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_msgs WHERE vhost=? AND queue=? AND holder IS NULL")) {
                ps.setString(1, vhost);
                ps.setString(2, name);
                return (long) ps.executeUpdate();
            }
        });
    }

    // ------------------------------------------------------------------------------------------ messages (owner)

    private static final String INSERT_MSG = "INSERT INTO warp_amqp_msgs (vhost, queue, msg_id, exchange, rkey, props, body, priority, persistent, expires_at, redelivered, deaths, raw10) "
            + "SELECT q.vhost, q.name, ?, ?, ?, ?, ?, CASE WHEN q.max_prio > 0 THEN least(?, q.max_prio) ELSE 0 END, ?, "
            + "CASE WHEN q.ttl_ms IS NULL AND ?::bigint IS NULL THEN NULL "
            + "ELSE now() + (least(coalesce(q.ttl_ms, ?::bigint), coalesce(?::bigint, q.ttl_ms)) * interval '1 millisecond') END, ?, ?, ? "
            + "FROM warp_amqp_queues q WHERE q.vhost = ? AND q.name = ? "
            + "AND (q.max_len IS NULL OR q.overflow NOT LIKE 'reject-publish%' OR (SELECT count(*) FROM warp_amqp_msgs m WHERE m.vhost = q.vhost "
            + "AND m.queue = q.name AND m.holder IS NULL) < q.max_len) "
            + "AND (q.max_bytes IS NULL OR q.overflow NOT LIKE 'reject-publish%' OR (SELECT coalesce(sum(octet_length(m.body)), 0) FROM warp_amqp_msgs m "
            + "WHERE m.vhost = q.vhost AND m.queue = q.name AND m.holder IS NULL) + ? <= q.max_bytes) "
            + "ON CONFLICT (vhost, queue, msg_id) DO NOTHING";

    /** Inserts one message into each of {@code queues} (all owned by {@code host}) in one transaction. Idempotent on (queue, msg id). */
    Map<String, Ins> insertMessages(String host, String vhost, Collection<String> queues, NewMsg m) {
        return shards.tx(host, c -> {
            Map<String, Ins> out = new LinkedHashMap<>();
            try (PreparedStatement ps = c.prepareStatement(INSERT_MSG)) {
                for (String q : queues) {
                    ps.setString(1, m.msgId());
                    ps.setString(2, m.exchange());
                    ps.setString(3, m.rkey());
                    ps.setBytes(4, m.props());
                    ps.setBytes(5, m.body());
                    ps.setInt(6, m.prio());
                    ps.setBoolean(7, m.persistent());
                    setLong(ps, 8, m.msgTtlMs());
                    setLong(ps, 9, m.msgTtlMs());
                    setLong(ps, 10, m.msgTtlMs());
                    ps.setBoolean(11, m.redelivered());
                    ps.setBytes(12, m.deaths());
                    ps.setBytes(13, m.raw10());
                    ps.setString(14, vhost);
                    ps.setString(15, q);
                    ps.setLong(16, m.body().length);
                    if (ps.executeUpdate() == 1) {
                        out.put(q, Ins.INSERTED);
                        continue;
                    }
                    // nothing inserted: queue missing, already inserted (idempotent replay) or rejected by overflow=reject-publish
                    try (PreparedStatement chk = c.prepareStatement("SELECT (SELECT count(*) FROM warp_amqp_queues WHERE vhost=? AND name=?), "
                            + "(SELECT count(*) FROM warp_amqp_msgs WHERE vhost=? AND queue=? AND msg_id=?)")) {
                        chk.setString(1, vhost);
                        chk.setString(2, q);
                        chk.setString(3, vhost);
                        chk.setString(4, q);
                        chk.setString(5, m.msgId());
                        try (ResultSet rs = chk.executeQuery()) {
                            rs.next();
                            out.put(q, rs.getLong(1) == 0 ? Ins.MISSING : rs.getLong(2) > 0 ? Ins.INSERTED : Ins.REJECTED);
                        }
                    }
                }
            }
            return out;
        });
    }

    private static MsgRow msgOf(ResultSet rs) throws SQLException {
        return new MsgRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getBytes(7),
                rs.getBytes(8), rs.getInt(9), rs.getBoolean(10), rs.getBoolean(11), rs.getTimestamp(12).getTime(), rs.getBytes(13), rs.getBytes(14), rs.getInt(15));
    }

    private static final String MCOLS = "seq, vhost, queue, msg_id, exchange, rkey, props, body, priority, persistent, redelivered, enq_at, deaths, raw10, dcount";

    /** Leases up to {@code limit} ready messages to {@code holder} (FOR UPDATE SKIP LOCKED), in queue order (priority, then arrival). */
    List<MsgRow> claim(String vhost, String queue, String holder, int limit) {
        return shards.conn(shards.owner(vhost, queue), c -> {
            List<MsgRow> out = new ArrayList<>();
            // MATERIALIZED: a plain IN (subquery ... LIMIT ... SKIP LOCKED) may be re-planned as a semi-join and lease more rows than the limit
            try (PreparedStatement ps = c.prepareStatement("WITH c AS MATERIALIZED (SELECT seq FROM warp_amqp_msgs WHERE vhost = ? AND queue = ? "
                    + "AND holder IS NULL AND (expires_at IS NULL OR expires_at > now()) ORDER BY priority DESC, seq LIMIT ? FOR UPDATE SKIP LOCKED) "
                    + "UPDATE warp_amqp_msgs m SET holder = ?, holder_at = now() FROM c WHERE m.seq = c.seq RETURNING "
                    + "m.seq, m.vhost, m.queue, m.msg_id, m.exchange, m.rkey, m.props, m.body, m.priority, m.persistent, m.redelivered, m.enq_at, m.deaths, m.raw10, m.dcount")) {
                ps.setString(1, vhost);
                ps.setString(2, queue);
                ps.setInt(3, limit);
                ps.setString(4, holder);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(msgOf(rs));
                    }
                }
            }
            out.sort((a, b) -> a.priority() != b.priority() ? Integer.compare(b.priority(), a.priority()) : Long.compare(a.seq(), b.seq()));
            return out;
        });
    }

    void ack(String vhost, String queue, Collection<Long> seqs) {
        if (seqs.isEmpty()) {
            return;
        }
        shards.conn(shards.owner(vhost, queue), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_msgs WHERE seq = ANY(?)")) {
                ps.setArray(1, c.createArrayOf("bigint", seqs.toArray()));
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** Returns leased messages to the queue; {@code failed} counts the return as a failed delivery (AMQP 1.0 modified with delivery-failed). */
    void requeue(String vhost, String queue, Collection<Long> seqs, boolean failed) {
        if (seqs.isEmpty()) {
            return;
        }
        shards.conn(shards.owner(vhost, queue), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_amqp_msgs SET holder = NULL, holder_at = NULL, redelivered = TRUE, dcount = dcount + ? "
                    + "WHERE seq = ANY(?)")) {
                ps.setInt(1, failed ? 1 : 0);
                ps.setArray(2, c.createArrayOf("bigint", seqs.toArray()));
                ps.executeUpdate();
            }
            return null;
        });
    }

    void requeue(String vhost, String queue, Collection<Long> seqs) {
        if (seqs.isEmpty()) {
            return;
        }
        shards.conn(shards.owner(vhost, queue), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_amqp_msgs SET holder = NULL, holder_at = NULL, redelivered = TRUE WHERE seq = ANY(?)")) {
                ps.setArray(1, c.createArrayOf("bigint", seqs.toArray()));
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** Messages that expired while ready, with the dead-letter setting of their queue. */
    record Expired(MsgRow msg, QueueDef queue) {
    }

    List<Expired> lockExpired(Connection c, int limit) throws SQLException {
        return lockExpired(c, limit, null, null);
    }

    /** Expired head messages of every queue, or of one queue when {@code vhost}/{@code queue} are given. */
    List<Expired> lockExpired(Connection c, int limit, String vhost, String queue) throws SQLException {
        List<Expired> out = new ArrayList<>();
        Map<String, QueueDef> qs = new LinkedHashMap<>();
        // like RabbitMQ, a message expires only at the head of its queue: nothing that is still alive may be ahead of it
        try (PreparedStatement ps = c.prepareStatement("SELECT " + MCOLS + " FROM warp_amqp_msgs m WHERE m.holder IS NULL AND m.expires_at IS NOT NULL AND m.expires_at <= now() "
                + "AND NOT EXISTS (SELECT 1 FROM warp_amqp_msgs h WHERE h.vhost = m.vhost AND h.queue = m.queue AND h.holder IS NULL "
                + "AND (h.expires_at IS NULL OR h.expires_at > now()) AND (h.priority > m.priority OR (h.priority = m.priority AND h.seq < m.seq))) "
                + (queue == null ? "" : "AND m.vhost = ? AND m.queue = ? ")
                + "ORDER BY m.seq LIMIT ? FOR UPDATE OF m SKIP LOCKED")) {
            int i = 1;
            if (queue != null) {
                ps.setString(i++, vhost);
                ps.setString(i++, queue);
            }
            ps.setInt(i, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    MsgRow m = msgOf(rs);
                    QueueDef q = qs.computeIfAbsent(m.vhost() + "\u0000" + m.queue(), k -> {
                        try {
                            return queue(c, m.vhost(), m.queue());
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                    out.add(new Expired(m, q));
                }
            }
        }
        return out;
    }

    /** The ready messages beyond {@code maxLen}: the head of the queue (drop-head), locked for the caller's transaction. */
    List<MsgRow> lockOverflow(Connection c, QueueDef q) throws SQLException {
        List<MsgRow> out = new ArrayList<>();
        if (q.maxLen() != null) {
            try (PreparedStatement ps = c.prepareStatement("SELECT " + MCOLS + " FROM warp_amqp_msgs WHERE seq IN (SELECT seq FROM warp_amqp_msgs "
                    + "WHERE vhost=? AND queue=? AND holder IS NULL ORDER BY priority DESC, seq LIMIT greatest((SELECT count(*) FROM warp_amqp_msgs "
                    + "WHERE vhost=? AND queue=? AND holder IS NULL) - ?, 0)) ORDER BY priority DESC, seq FOR UPDATE SKIP LOCKED")) {
                ps.setString(1, q.vhost());
                ps.setString(2, q.name());
                ps.setString(3, q.vhost());
                ps.setString(4, q.name());
                ps.setLong(5, q.maxLen());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(msgOf(rs));
                    }
                }
            }
        }
        return out;
    }

    /** Sum of ready body bytes above {@code maxBytes}: the messages (oldest first) to drop. */
    List<MsgRow> lockByteOverflow(Connection c, QueueDef q) throws SQLException {
        List<MsgRow> out = new ArrayList<>();
        if (q.maxBytes() == null) {
            return out;
        }
        List<MsgRow> all = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT " + MCOLS + " FROM warp_amqp_msgs WHERE vhost=? AND queue=? AND holder IS NULL "
                + "ORDER BY priority DESC, seq FOR UPDATE SKIP LOCKED")) {
            ps.setString(1, q.vhost());
            ps.setString(2, q.name());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    all.add(msgOf(rs));
                }
            }
        }
        long total = 0;
        for (MsgRow m : all) {
            total += m.body().length;
        }
        for (MsgRow m : all) {
            if (total <= q.maxBytes()) {
                break;
            }
            total -= m.body().length;
            out.add(m);
        }
        return out;
    }

    void deleteRows(Connection c, Collection<MsgRow> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        List<Long> seqs = new ArrayList<>();
        rows.forEach(r -> seqs.add(r.seq()));
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_amqp_msgs WHERE seq = ANY(?)")) {
            ps.setArray(1, c.createArrayOf("bigint", seqs.toArray()));
            ps.executeUpdate();
        }
    }

    /** Frees messages leased by nodes that are no longer alive. */
    int releaseDead(String host, Set<String> live) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_amqp_msgs SET holder = NULL, holder_at = NULL, redelivered = TRUE "
                    + "WHERE holder IS NOT NULL AND NOT (split_part(holder, '/', 1) = ANY(?))")) {
                ps.setArray(1, c.createArrayOf("text", live.toArray()));
                return ps.executeUpdate();
            }
        });
    }

    /** Frees every message leased by this node (used at start-up: nothing of a previous run of the same node id can still be held). */
    List<String> deadExclusive(String host, Set<String> live) {
        return shards.conn(host, c -> {
            List<String> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT vhost || E'\\u0001' || name FROM warp_amqp_queues WHERE excl_owner IS NOT NULL "
                    + "AND NOT (split_part(excl_owner, '/', 1) = ANY(?))")) {
                ps.setArray(1, c.createArrayOf("text", live.toArray()));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString(1));
                    }
                }
            }
            return out;
        });
    }

    /** Queues whose x-expires elapsed without use (the caller decides whether they have local consumers). */
    List<QueueDef> expiredQueues(String host) {
        return shards.conn(host, c -> {
            List<QueueDef> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + QCOLS + " FROM warp_amqp_queues WHERE expires_ms IS NOT NULL "
                    + "AND last_used < now() - (expires_ms * interval '1 millisecond')")) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(queueOf(rs));
                    }
                }
            }
            return out;
        });
    }

    /** Messages of a queue for inspection (MCP), oldest first, without leasing them. */
    List<MsgRow> peek(String vhost, String queue, int limit) {
        return shards.conn(shards.owner(vhost, queue), c -> {
            List<MsgRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + MCOLS + " FROM warp_amqp_msgs WHERE vhost=? AND queue=? "
                    + "ORDER BY priority DESC, seq LIMIT ?")) {
                ps.setString(1, vhost);
                ps.setString(2, queue);
                ps.setInt(3, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(msgOf(rs));
                    }
                }
            }
            return out;
        });
    }

    static String b64(byte[] b) {
        return Base64.getEncoder().encodeToString(b);
    }

    static Timestamp ts(long ms) {
        return new Timestamp(ms);
    }
}
