package com.sayonora.warp.azurewire;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Queue storage: the queue catalog (metadata + stored access policies) on the home host, a queue's messages wholly on
 * the shard owning hash(account/queue). Dequeue is a single {@code UPDATE ... FOR UPDATE SKIP LOCKED} that bumps the
 * dequeue count, hides the message until the visibility deadline and issues a fresh pop receipt, so concurrent
 * consumers never receive the same message twice.
 */
final class QueueStore {

    private static final Gson GSON = new Gson();
    private static final TypeToken<Map<String, String>> MAP_T = new TypeToken<>() { };
    static final Instant NEVER = Instant.parse("9999-12-31T23:59:59Z");

    final AzShards shards;

    QueueStore(AzShards shards) {
        this.shards = shards;
    }

    record Queue(String account, String name, Instant createdAt, Map<String, String> metadata,
            List<AzureAuth.Policy> acl) {
    }

    record Message(long seq, String id, Instant insertedAt, Instant expiresAt, Instant visibleAt, int dequeueCount,
            String popReceipt, String body) {
    }

    String ownerHost(String account, String queue) {
        return shards.owner(account + "/" + queue);
    }

    private static Timestamp ts(Instant i) {
        return Timestamp.from(i);
    }

    Queue get(String account, String name) {
        return shards.conn(shards.home(), c -> select(c, account, name, false));
    }

    static Queue select(Connection c, String account, String name, boolean lock) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT account, name, created_at, metadata::text, acl::text FROM "
                + "warp_azqueue_queues WHERE account=? AND name=?" + (lock ? " FOR UPDATE" : ""))) {
            ps.setString(1, account);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                Map<String, String> md = GSON.fromJson(rs.getString(4), MAP_T);
                return new Queue(rs.getString(1), rs.getString(2), rs.getTimestamp(3).toInstant(),
                        md == null ? new LinkedHashMap<>() : new LinkedHashMap<>(md),
                        BlobStore.class == null ? List.of() : parseAcl(rs.getString(5)));
            }
        }
    }

    private static List<AzureAuth.Policy> parseAcl(String json) {
        List<Map<String, String>> l = GSON.fromJson(json, new TypeToken<List<Map<String, String>>>() { });
        List<AzureAuth.Policy> out = new ArrayList<>();
        if (l != null) {
            for (Map<String, String> m : l) {
                out.add(new AzureAuth.Policy(m.get("id"), m.get("start"), m.get("expiry"), m.get("permission")));
            }
        }
        return out;
    }

    /** @return true when created, false when it already existed with identical metadata; throws 409 otherwise */
    boolean create(String account, String name, Map<String, String> metadata) {
        return shards.tx(shards.home(), c -> {
            Queue q = select(c, account, name, true);
            if (q != null) {
                if (q.metadata().equals(metadata)) {
                    return false;
                }
                throw new AzureException(409, "QueueAlreadyExists", "The specified queue already exists.");
            }
            BlobStore.exec(c, "INSERT INTO warp_azqueue_queues (account, name, created_at, metadata) VALUES (?,?,?,?::jsonb)",
                    account, name, ts(Instant.now()), GSON.toJson(metadata));
            return true;
        });
    }

    void setMetadata(String account, String name, Map<String, String> md) {
        shards.tx(shards.home(), c -> {
            if (select(c, account, name, true) == null) {
                throw queueNotFound();
            }
            BlobStore.exec(c, "UPDATE warp_azqueue_queues SET metadata=?::jsonb WHERE account=? AND name=?", GSON.toJson(md),
                    account, name);
            return null;
        });
    }

    void setAcl(String account, String name, List<AzureAuth.Policy> acl) {
        shards.tx(shards.home(), c -> {
            if (select(c, account, name, true) == null) {
                throw queueNotFound();
            }
            BlobStore.exec(c, "UPDATE warp_azqueue_queues SET acl=?::jsonb WHERE account=? AND name=?", BlobStore.aclJson(acl),
                    account, name);
            return null;
        });
    }

    void delete(String account, String name) {
        shards.tx(shards.home(), c -> {
            if (BlobStore.exec(c, "DELETE FROM warp_azqueue_queues WHERE account=? AND name=?", account, name) == 0) {
                throw queueNotFound();
            }
            return null;
        });
        shards.conn(ownerHost(account, name), c -> BlobStore.exec(c, "DELETE FROM warp_azqueue_messages WHERE account=? "
                + "AND queue=?", account, name));
    }

    static AzureException queueNotFound() {
        return new AzureException(404, "QueueNotFound", "The specified queue does not exist.");
    }

    List<Queue> list(String account, String prefix, String marker, int limit) {
        return shards.conn(shards.home(), c -> {
            StringBuilder sql = new StringBuilder("SELECT account, name, created_at, metadata::text, acl::text FROM "
                    + "warp_azqueue_queues WHERE account=?");
            List<Object> args = new ArrayList<>();
            args.add(account);
            if (!prefix.isEmpty()) {
                sql.append(" AND name >= ? AND name < ?");
                args.add(prefix);
                args.add(BlobStore.prefixEnd(prefix));
            }
            if (!marker.isEmpty()) {
                sql.append(" AND name >= ?");
                args.add(marker);
            }
            sql.append(" ORDER BY name LIMIT ").append(limit);
            List<Queue> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                for (int i = 0; i < args.size(); i++) {
                    ps.setObject(i + 1, args.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, String> md = GSON.fromJson(rs.getString(4), MAP_T);
                        out.add(new Queue(rs.getString(1), rs.getString(2), rs.getTimestamp(3).toInstant(),
                                md == null ? new LinkedHashMap<>() : new LinkedHashMap<>(md), parseAcl(rs.getString(5))));
                    }
                }
            }
            return out;
        });
    }

    String serviceProperties(String account) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT properties::text FROM warp_azqueue_service WHERE account=?")) {
                ps.setString(1, account);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : "{}";
                }
            }
        });
    }

    void setServiceProperties(String account, String json) {
        shards.conn(shards.home(), c -> BlobStore.exec(c, "INSERT INTO warp_azqueue_service (account, properties) VALUES "
                + "(?, ?::jsonb) ON CONFLICT (account) DO UPDATE SET properties = EXCLUDED.properties", account, json));
    }

    // ---------------------------------------------------------------------------------------------- messages

    Message put(String account, String queue, String body, Instant now, int visibilitySeconds, long ttlSeconds) {
        String id = UUID.randomUUID().toString();
        Instant expires = ttlSeconds < 0 ? NEVER : now.plusSeconds(ttlSeconds);
        Instant visible = now.plusSeconds(visibilitySeconds);
        String pop = newPopReceipt();
        long seq = shards.conn(ownerHost(account, queue), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_azqueue_messages (account, queue, message_id, "
                    + "inserted_at, expires_at, visible_at, dequeue_count, pop_receipt, body) VALUES "
                    + "(?,?,?::uuid,?,?,?,0,?,?) RETURNING seq")) {
                ps.setString(1, account);
                ps.setString(2, queue);
                ps.setString(3, id);
                ps.setTimestamp(4, ts(now));
                ps.setTimestamp(5, ts(expires));
                ps.setTimestamp(6, ts(visible));
                ps.setString(7, pop);
                ps.setString(8, body);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
        return new Message(seq, id, now, expires, visible, 0, pop, body);
    }

    static String newPopReceipt() {
        byte[] b = new byte[16];
        new java.security.SecureRandom().nextBytes(b);
        return java.util.Base64.getEncoder().encodeToString(b);
    }

    /** Dequeues up to {@code max} visible messages, hiding them until now+visibilitySeconds. */
    List<Message> get(String account, String queue, int max, Instant now, int visibilitySeconds) {
        Instant next = now.plusSeconds(visibilitySeconds);
        return shards.conn(ownerHost(account, queue), c -> {
            List<Message> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("WITH picked AS (SELECT message_id FROM warp_azqueue_messages "
                    + "WHERE account=? AND queue=? AND visible_at <= ? AND expires_at > ? ORDER BY seq LIMIT ? "
                    + "FOR UPDATE SKIP LOCKED) "
                    + "UPDATE warp_azqueue_messages m SET visible_at=?, dequeue_count=m.dequeue_count+1, "
                    + "pop_receipt=encode(decode(replace(gen_random_uuid()::text,'-',''),'hex'),'base64') "
                    + "FROM picked WHERE m.account=? AND m.queue=? AND m.message_id=picked.message_id "
                    + "RETURNING m.seq, m.message_id::text, m.inserted_at, m.expires_at, m.visible_at, m.dequeue_count, "
                    + "m.pop_receipt, m.body")) {
                int i = 1;
                ps.setString(i++, account);
                ps.setString(i++, queue);
                ps.setTimestamp(i++, ts(now));
                ps.setTimestamp(i++, ts(now));
                ps.setInt(i++, max);
                ps.setTimestamp(i++, ts(next));
                ps.setString(i++, account);
                ps.setString(i, queue);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(read(rs));
                    }
                }
            }
            out.sort(java.util.Comparator.comparingLong(Message::seq));
            return out;
        });
    }

    private static Message read(ResultSet rs) throws SQLException {
        return new Message(rs.getLong(1), rs.getString(2), rs.getTimestamp(3).toInstant(), rs.getTimestamp(4).toInstant(),
                rs.getTimestamp(5).toInstant(), rs.getInt(6), rs.getString(7), rs.getString(8));
    }

    List<Message> peek(String account, String queue, int max, Instant now) {
        return shards.conn(ownerHost(account, queue), c -> {
            List<Message> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT seq, message_id::text, inserted_at, expires_at, visible_at, "
                    + "dequeue_count, pop_receipt, body FROM warp_azqueue_messages WHERE account=? AND queue=? AND "
                    + "visible_at <= ? AND expires_at > ? ORDER BY seq LIMIT ?")) {
                ps.setString(1, account);
                ps.setString(2, queue);
                ps.setTimestamp(3, ts(now));
                ps.setTimestamp(4, ts(now));
                ps.setInt(5, max);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(read(rs));
                    }
                }
            }
            return out;
        });
    }

    long approximateCount(String account, String queue, Instant now) {
        return shards.conn(ownerHost(account, queue), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_azqueue_messages WHERE account=? AND "
                    + "queue=? AND expires_at > ?")) {
                ps.setString(1, account);
                ps.setString(2, queue);
                ps.setTimestamp(3, ts(now));
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    /** Deletes a message with a matching pop receipt. */
    void delete(String account, String queue, String messageId, String popReceipt, Instant now) {
        UUID id = parseId(messageId);
        shards.tx(ownerHost(account, queue), c -> {
            Message m = lock(c, account, queue, id, now);
            if (m == null) {
                throw new AzureException(404, "MessageNotFound", "The specified message does not exist.");
            }
            if (!popReceipt.equals(m.popReceipt())) {
                throw popMismatch();
            }
            BlobStore.exec(c, "DELETE FROM warp_azqueue_messages WHERE account=? AND queue=? AND message_id=?::uuid", account,
                    queue, id.toString());
            return null;
        });
    }

    static AzureException popMismatch() {
        return new AzureException(400, "PopReceiptMismatch", "The specified pop receipt did not match the pop receipt for a "
                + "dequeued message.");
    }

    private static UUID parseId(String s) {
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            throw new AzureException(404, "MessageNotFound", "The specified message does not exist.");
        }
    }

    private static Message lock(Connection c, String account, String queue, UUID id, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT seq, message_id::text, inserted_at, expires_at, visible_at, "
                + "dequeue_count, pop_receipt, body FROM warp_azqueue_messages WHERE account=? AND queue=? AND "
                + "message_id=?::uuid AND expires_at > ? FOR UPDATE")) {
            ps.setString(1, account);
            ps.setString(2, queue);
            ps.setString(3, id.toString());
            ps.setTimestamp(4, ts(now));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? read(rs) : null;
            }
        }
    }

    /** Update Message: new visibility timeout, optional new content; returns (new pop receipt, next visible). */
    Message update(String account, String queue, String messageId, String popReceipt, Instant now, int visibilitySeconds,
            String newBody) {
        UUID id = parseId(messageId);
        return shards.tx(ownerHost(account, queue), c -> {
            Message m = lock(c, account, queue, id, now);
            if (m == null) {
                throw new AzureException(404, "MessageNotFound", "The specified message does not exist.");
            }
            if (!popReceipt.equals(m.popReceipt())) {
                throw popMismatch();
            }
            Instant visible = now.plusSeconds(visibilitySeconds);
            String pop = newPopReceipt();
            BlobStore.exec(c, "UPDATE warp_azqueue_messages SET visible_at=?, pop_receipt=?" + (newBody != null ? ", body=?" : "")
                    + " WHERE account=? AND queue=? AND message_id=?::uuid",
                    newBody != null ? new Object[] {ts(visible), pop, newBody, account, queue, id.toString()}
                            : new Object[] {ts(visible), pop, account, queue, id.toString()});
            return new Message(m.seq(), m.id(), m.insertedAt(), m.expiresAt(), visible, m.dequeueCount(), pop,
                    newBody != null ? newBody : m.body());
        });
    }

    void clear(String account, String queue) {
        shards.conn(ownerHost(account, queue), c -> BlobStore.exec(c, "DELETE FROM warp_azqueue_messages WHERE account=? "
                + "AND queue=?", account, queue));
    }

    /** Removes expired messages on every shard (the sweeper); returns how many. */
    long sweepExpired() {
        long n = 0;
        for (String h : shards.hosts()) {
            n += shards.conn(h, c -> (long) BlobStore.exec(c, "DELETE FROM warp_azqueue_messages WHERE expires_at <= now()"));
        }
        return n;
    }

    /** MCP/describe: queue names with approximate message counts. */
    Map<String, Long> counts(String account) {
        Map<String, Long> out = new LinkedHashMap<>();
        for (Queue q : list(account, "", "", 1000)) {
            out.put(q.name(), approximateCount(account, q.name(), Instant.now()));
        }
        return out;
    }
}
