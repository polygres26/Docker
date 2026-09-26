package com.sayonora.warp.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ShardingStrategy;
import com.sayonora.warp.core.StoreBootstrap;
import com.sayonora.warp.core.StoreType;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres storage of cosmoswire (see {@code ddl/postgres/cosmoswire_store.sql}). Sharding: a document lives on the host owning
 * hash(database/container/first partition key value); the catalog (databases, containers, scripts) lives on the first host. Every
 * operation borrows a pooled connection for one short statement or transaction; nothing is held across client I/O or between the
 * pages of a scan (scans are keyset-paged).
 */
final class CosmosStore {

    private static final Logger log = LoggerFactory.getLogger(CosmosStore.class);
    static final int PAGE = 500;
    private static final SecureRandom RND = new SecureRandom();

    private final BackendRegistry registry;
    private final StoreType type = StoreType.COSMOS;
    private final ConcurrentHashMap<String, Cached> collCache = new ConcurrentHashMap<>();
    private static final long CACHE_MS = 1000;

    private record Cached(Coll coll, long at) {
    }

    CosmosStore(BackendRegistry registry) {
        this.registry = registry;
    }

    // ------------------------------------------------------------------------------------------ model

    record DbRow(String id, String rid, String etag, long ts, Integer throughput) {

        JsonObject toJson() {
            JsonObject o = new JsonObject();
            o.addProperty("id", id);
            o.addProperty("_rid", rid);
            o.addProperty("_self", "dbs/" + rid + "/");
            o.addProperty("_etag", etag);
            o.addProperty("_colls", "colls/");
            o.addProperty("_users", "users/");
            o.addProperty("_ts", ts);
            return o;
        }
    }

    record Coll(String db, String dbRid, String id, String rid, String etag, long ts, JsonObject def, Integer throughput) {

        List<String> pkPaths() {
            List<String> out = new ArrayList<>();
            JsonObject pk = def.has("partitionKey") ? def.getAsJsonObject("partitionKey") : null;
            if (pk != null && pk.has("paths")) {
                pk.getAsJsonArray("paths").forEach(p -> out.add(p.getAsString()));
            }
            return out;
        }

        boolean hierarchical() {
            return pkPaths().size() > 1;
        }

        Integer defaultTtl() {
            return def.has("defaultTtl") && !def.get("defaultTtl").isJsonNull() ? def.get("defaultTtl").getAsInt() : null;
        }

        boolean ttlEnabled() {
            return defaultTtl() != null;
        }

        /** Unique key constraints: each a list of property paths. */
        List<List<String>> uniqueKeys() {
            List<List<String>> out = new ArrayList<>();
            if (def.has("uniqueKeyPolicy") && def.getAsJsonObject("uniqueKeyPolicy").has("uniqueKeys")) {
                for (JsonElement k : def.getAsJsonObject("uniqueKeyPolicy").getAsJsonArray("uniqueKeys")) {
                    List<String> ps = new ArrayList<>();
                    k.getAsJsonObject().getAsJsonArray("paths").forEach(p -> ps.add(p.getAsString()));
                    out.add(ps);
                }
            }
            return out;
        }

        String selfLink() {
            return "dbs/" + dbRid + "/colls/" + rid + "/";
        }

        JsonObject toJson() {
            JsonObject o = new JsonObject();
            o.addProperty("id", id);
            for (Map.Entry<String, JsonElement> e : def.entrySet()) {
                o.add(e.getKey(), e.getValue());
            }
            o.addProperty("_rid", rid);
            o.addProperty("_ts", ts);
            o.addProperty("_self", selfLink());
            o.addProperty("_etag", etag);
            o.addProperty("_docs", "docs/");
            o.addProperty("_sprocs", "sprocs/");
            o.addProperty("_triggers", "triggers/");
            o.addProperty("_udfs", "udfs/");
            o.addProperty("_conflicts", "conflicts/");
            return o;
        }
    }

    /** A stored document version. {@code body} holds the user properties only (with id). */
    record Doc(String pkh, String id, JsonObject body, String etag, long ts, long lsn, String rid, Integer ttl) {

        /** The document as returned to clients: user properties then _rid, _self, _etag, _attachments, _ts. */
        JsonObject toJson(Coll c, boolean withLsn) {
            JsonObject o = body.deepCopy();
            for (String sys : new String[] {"_rid", "_self", "_etag", "_attachments", "_ts", "_lsn"}) {
                o.remove(sys);
            }
            o.addProperty("_rid", rid);
            o.addProperty("_self", c.selfLink() + "docs/" + rid + "/");
            o.addProperty("_etag", etag);
            o.addProperty("_attachments", "attachments/");
            o.addProperty("_ts", ts);
            if (withLsn) {
                o.addProperty("_lsn", lsn);
            }
            return o;
        }

        boolean expired(Coll c, long nowSec) {
            Integer d = c.defaultTtl();
            if (d == null) {
                return false;
            }
            int eff = ttl != null ? ttl : d;
            return eff > 0 && ts + eff <= nowSec;
        }
    }

    // ------------------------------------------------------------------------------------------ hosts and connections

    List<String> hosts() {
        List<String> h = registry.storeHosts(type);
        if (h.isEmpty()) {
            throw new CosmosException(503, "ServiceUnavailable", "No Postgres backend of this set has the cosmos store enabled");
        }
        return h;
    }

    boolean available() {
        return !registry.storeHosts(type).isEmpty();
    }

    String home() {
        return hosts().get(0);
    }

    /** Index in {@link #hosts()} of the host owning a partition (by the hash of its first key value). */
    int ownerIndex(String db, String coll, List<JsonElement> pk) {
        List<String> h = hosts();
        if (h.size() == 1) {
            return 0;
        }
        return h.indexOf(ShardingStrategy.hash(h).resolve(db + "/" + coll + "/" + CosmosPk.hostKey(pk)));
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("cosmoswire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, type);
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

    static CosmosException storage(SQLException e) {
        log.error("cosmoswire: Postgres store error", e);
        return new CosmosException(503, "ServiceUnavailable", "The store is currently unavailable (" + e.getMessage() + ")");
    }

    // ------------------------------------------------------------------------------------------ ids and etags

    static String etag() {
        return "\"" + UUID.randomUUID() + "\"";
    }

    private static String b64(byte[] b) {
        return Base64.getEncoder().encodeToString(b).replace('/', '-').replace('+', '_');
    }

    private static byte[] unb64(String s) {
        return Base64.getDecoder().decode(s.replace('-', '/').replace('_', '+'));
    }

    static String newRid(int bytes) {
        byte[] b = new byte[bytes];
        RND.nextBytes(b);
        b[0] &= 0x7F;
        return b64(b);
    }

    static String childRid(String parentRid, int extra) {
        byte[] p = unb64(parentRid);
        byte[] b = new byte[p.length + extra];
        System.arraycopy(p, 0, b, 0, p.length);
        byte[] r = new byte[extra];
        RND.nextBytes(r);
        System.arraycopy(r, 0, b, p.length, extra);
        return b64(b);
    }

    /** Document resource id: the container rid then 1 byte of host index and 7 bytes of lsn (unique per container). */
    static String docRid(String collRid, int hostIdx, long lsn) {
        byte[] p = unb64(collRid);
        byte[] b = new byte[p.length + 8];
        System.arraycopy(p, 0, b, 0, p.length);
        b[p.length] = (byte) hostIdx;
        for (int i = 0; i < 7; i++) {
            b[p.length + 1 + i] = (byte) (lsn >>> (8 * (6 - i)));
        }
        return b64(b);
    }

    static long nowSec() {
        return System.currentTimeMillis() / 1000;
    }

    // ------------------------------------------------------------------------------------------ databases

    List<DbRow> listDbs() {
        return conn(home(), c -> {
            List<DbRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT id, rid, etag, ts, throughput FROM warp_cosmos_dbs ORDER BY id");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(db(rs));
                }
            }
            return out;
        });
    }

    private static Integer nullableInt(ResultSet rs, int col) throws SQLException {
        int v = rs.getInt(col);
        return rs.wasNull() ? null : v;
    }

    private static DbRow db(ResultSet rs) throws SQLException {
        Integer t = nullableInt(rs, 5);
        return new DbRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4), t);
    }

    DbRow getDb(String id) {
        return conn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT id, rid, etag, ts, throughput FROM warp_cosmos_dbs WHERE id = ?")) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? db(rs) : null;
                }
            }
        });
    }

    DbRow needDb(String id) {
        DbRow d = getDb(id);
        if (d == null) {
            throw notFound();
        }
        return d;
    }

    static CosmosException notFound() {
        return CosmosException.notFound("{\"Errors\":[\"Resource Not Found. Learn more: https://aka.ms/cosmosdb-tsg-not-found\"]}");
    }

    DbRow createDb(String id, Integer throughput) {
        DbRow d = new DbRow(id, newRid(4), etag(), nowSec(), throughput);
        int n = conn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_cosmos_dbs (id, rid, etag, ts, throughput) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, d.id());
                ps.setString(2, d.rid());
                ps.setString(3, d.etag());
                ps.setLong(4, d.ts());
                if (throughput == null) {
                    ps.setNull(5, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(5, throughput);
                }
                return ps.executeUpdate();
            }
        });
        if (n == 0) {
            throw CosmosException.conflict("Entity with the specified id already exists in the system.");
        }
        return d;
    }

    void deleteDb(String id) {
        needDb(id);
        for (Coll c : listColls(id)) {
            deleteColl(id, c.id());
        }
        conn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cosmos_dbs WHERE id = ?")) {
                ps.setString(1, id);
                return ps.executeUpdate();
            }
        });
        collCache.keySet().removeIf(k -> k.startsWith(id + "\u0000"));
    }

    void setDbThroughput(String id, Integer throughput) {
        conn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_cosmos_dbs SET throughput = ? WHERE id = ?")) {
                if (throughput == null) {
                    ps.setNull(1, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(1, throughput);
                }
                ps.setString(2, id);
                return ps.executeUpdate();
            }
        });
    }

    // ------------------------------------------------------------------------------------------ containers

    private static final String COLL_SQL = "SELECT c.db, d.rid, c.id, c.rid, c.etag, c.ts, c.def, c.throughput FROM warp_cosmos_colls c "
            + "JOIN warp_cosmos_dbs d ON d.id = c.db";

    private static Coll coll(ResultSet rs) throws SQLException {
        Integer t = nullableInt(rs, 8);
        return new Coll(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getLong(6),
                JsonParser.parseString(rs.getString(7)).getAsJsonObject(), t);
    }

    List<Coll> listColls(String db) {
        return conn(home(), c -> {
            List<Coll> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(COLL_SQL + " WHERE c.db = ? ORDER BY c.id")) {
                ps.setString(1, db);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(coll(rs));
                    }
                }
            }
            return out;
        });
    }

    /** The container, or null. Cached for a second (catalog reads happen on every document operation). */
    Coll getColl(String db, String id) {
        String key = db + "\u0000" + id;
        Cached ch = collCache.get(key);
        long now = System.currentTimeMillis();
        if (ch != null && now - ch.at() < CACHE_MS) {
            return ch.coll();
        }
        Coll c = conn(home(), cn -> {
            try (PreparedStatement ps = cn.prepareStatement(COLL_SQL + " WHERE c.db = ? AND c.id = ?")) {
                ps.setString(1, db);
                ps.setString(2, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? coll(rs) : null;
                }
            }
        });
        if (c == null) {
            collCache.remove(key);
        } else {
            collCache.put(key, new Cached(c, now));
        }
        return c;
    }

    Coll needColl(String db, String id) {
        Coll c = getColl(db, id);
        if (c == null) {
            throw notFound();
        }
        return c;
    }

    Coll createColl(String db, String id, JsonObject def, Integer throughput) {
        DbRow d = needDb(db);
        Coll c = new Coll(db, d.rid(), id, childRid(d.rid(), 4), etag(), nowSec(), def, throughput);
        int n = conn(home(), cn -> {
            try (PreparedStatement ps = cn.prepareStatement(
                    "INSERT INTO warp_cosmos_colls (db, id, rid, etag, ts, def, throughput) VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, db);
                ps.setString(2, id);
                ps.setString(3, c.rid());
                ps.setString(4, c.etag());
                ps.setLong(5, c.ts());
                ps.setString(6, def.toString());
                if (throughput == null) {
                    ps.setNull(7, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(7, throughput);
                }
                return ps.executeUpdate();
            }
        });
        if (n == 0) {
            throw CosmosException.conflict("Entity with the specified id already exists in the system.");
        }
        collCache.remove(db + "\u0000" + id);
        return c;
    }

    Coll replaceColl(Coll old, JsonObject def) {
        String et = etag();
        long ts = nowSec();
        conn(home(), cn -> {
            try (PreparedStatement ps = cn.prepareStatement("UPDATE warp_cosmos_colls SET def = ?, etag = ?, ts = ? WHERE db = ? AND id = ?")) {
                ps.setString(1, def.toString());
                ps.setString(2, et);
                ps.setLong(3, ts);
                ps.setString(4, old.db());
                ps.setString(5, old.id());
                return ps.executeUpdate();
            }
        });
        collCache.remove(old.db() + "\u0000" + old.id());
        return new Coll(old.db(), old.dbRid(), old.id(), old.rid(), et, ts, def, old.throughput());
    }

    void setCollThroughput(String db, String id, Integer throughput) {
        conn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_cosmos_colls SET throughput = ? WHERE db = ? AND id = ?")) {
                if (throughput == null) {
                    ps.setNull(1, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(1, throughput);
                }
                ps.setString(2, db);
                ps.setString(3, id);
                return ps.executeUpdate();
            }
        });
        collCache.remove(db + "\u0000" + id);
    }

    void deleteColl(String db, String id) {
        needColl(db, id);
        conn(home(), cn -> {
            try (PreparedStatement ps = cn.prepareStatement("DELETE FROM warp_cosmos_colls WHERE db = ? AND id = ?")) {
                ps.setString(1, db);
                ps.setString(2, id);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = cn.prepareStatement("DELETE FROM warp_cosmos_scripts WHERE db = ? AND coll = ?")) {
                ps.setString(1, db);
                ps.setString(2, id);
                ps.executeUpdate();
            }
            return 0;
        });
        for (String h : hosts()) {
            conn(h, cn -> {
                for (String t : new String[] {"warp_cosmos_docs", "warp_cosmos_seq", "warp_cosmos_uniq"}) {
                    try (PreparedStatement ps = cn.prepareStatement("DELETE FROM " + t + " WHERE db = ? AND coll = ?")) {
                        ps.setString(1, db);
                        ps.setString(2, id);
                        ps.executeUpdate();
                    }
                }
                return 0;
            });
        }
        collCache.remove(db + "\u0000" + id);
    }

    // ------------------------------------------------------------------------------------------ scripts (stored, never executed)

    record Script(String kind, String id, String rid, String etag, long ts, JsonObject def) {

        JsonObject toJson(Coll c) {
            JsonObject o = def.deepCopy();
            o.addProperty("id", id);
            o.addProperty("_rid", rid);
            o.addProperty("_self", c.selfLink() + kind + "/" + rid + "/");
            o.addProperty("_etag", etag);
            o.addProperty("_ts", ts);
            return o;
        }
    }

    List<Script> listScripts(Coll cl, String kind) {
        return conn(home(), c -> {
            List<Script> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT id, rid, etag, ts, def FROM warp_cosmos_scripts WHERE db = ? AND coll = ? AND kind = ? ORDER BY id")) {
                ps.setString(1, cl.db());
                ps.setString(2, cl.id());
                ps.setString(3, kind);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Script(kind, rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4),
                                JsonParser.parseString(rs.getString(5)).getAsJsonObject()));
                    }
                }
            }
            return out;
        });
    }

    Script getScript(Coll cl, String kind, String id) {
        return listScripts(cl, kind).stream().filter(s -> s.id().equals(id)).findFirst().orElse(null);
    }

    Script putScript(Coll cl, String kind, String id, JsonObject def, boolean mustNotExist) {
        Script old = getScript(cl, kind, id);
        if (old != null && mustNotExist) {
            throw CosmosException.conflict("Entity with the specified id already exists in the system.");
        }
        if (old == null && !mustNotExist) {
            throw notFound();
        }
        Script s = new Script(kind, id, old != null ? old.rid() : childRid(cl.rid(), 4), etag(), nowSec(), def);
        conn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cosmos_scripts (db, coll, kind, id, rid, etag, ts, def) VALUES (?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (db, coll, kind, id) DO UPDATE SET etag = EXCLUDED.etag, ts = EXCLUDED.ts, def = EXCLUDED.def")) {
                ps.setString(1, cl.db());
                ps.setString(2, cl.id());
                ps.setString(3, kind);
                ps.setString(4, id);
                ps.setString(5, s.rid());
                ps.setString(6, s.etag());
                ps.setLong(7, s.ts());
                ps.setString(8, def.toString());
                return ps.executeUpdate();
            }
        });
        return s;
    }

    void deleteScript(Coll cl, String kind, String id) {
        int n = conn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cosmos_scripts WHERE db = ? AND coll = ? AND kind = ? AND id = ?")) {
                ps.setString(1, cl.db());
                ps.setString(2, cl.id());
                ps.setString(3, kind);
                ps.setString(4, id);
                return ps.executeUpdate();
            }
        });
        if (n == 0) {
            throw notFound();
        }
    }

    // ------------------------------------------------------------------------------------------ documents (inside a transaction / connection)

    private static final String DOC_COLS = "pkh, id, body, etag, ts, lsn, rid, ttl";

    private static Doc doc(ResultSet rs) throws SQLException {
        Integer t = nullableInt(rs, 8);
        return new Doc(rs.getString(1), rs.getString(2), JsonParser.parseString(rs.getString(3)).getAsJsonObject(), rs.getString(4),
                rs.getLong(5), rs.getLong(6), rs.getString(7), t);
    }

    private static long nextLsn(Connection c, Coll cl) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cosmos_seq (db, coll, n) VALUES (?,?,1) "
                + "ON CONFLICT (db, coll) DO UPDATE SET n = warp_cosmos_seq.n + 1 RETURNING n")) {
            ps.setString(1, cl.db());
            ps.setString(2, cl.id());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /** The live (non-expired) document, or null; an expired one is deleted on the spot. */
    static Doc read(Connection c, Coll cl, String pkh, String id, boolean forUpdate) throws SQLException {
        Doc d;
        try (PreparedStatement ps = c.prepareStatement("SELECT " + DOC_COLS + " FROM warp_cosmos_docs WHERE db = ? AND coll = ? AND pkh = ? AND id = ?"
                + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, cl.db());
            ps.setString(2, cl.id());
            ps.setString(3, pkh);
            ps.setString(4, id);
            try (ResultSet rs = ps.executeQuery()) {
                d = rs.next() ? doc(rs) : null;
            }
        }
        if (d != null && d.expired(cl, nowSec())) {
            remove(c, cl, pkh, id, null);
            return null;
        }
        return d;
    }

    private static int remove(Connection c, Coll cl, String pkh, String id, String ifMatch) throws SQLException {
        int n;
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cosmos_docs WHERE db = ? AND coll = ? AND pkh = ? AND id = ?"
                + (ifMatch != null ? " AND etag = ?" : ""))) {
            ps.setString(1, cl.db());
            ps.setString(2, cl.id());
            ps.setString(3, pkh);
            ps.setString(4, id);
            if (ifMatch != null) {
                ps.setString(5, ifMatch);
            }
            n = ps.executeUpdate();
        }
        if (n > 0 && !cl.uniqueKeys().isEmpty()) {
            dropUnique(c, cl, pkh, id);
        }
        return n;
    }

    private static void dropUnique(Connection c, Coll cl, String pkh, String id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cosmos_uniq WHERE db = ? AND coll = ? AND pkh = ? AND id = ?")) {
            ps.setString(1, cl.db());
            ps.setString(2, cl.id());
            ps.setString(3, pkh);
            ps.setString(4, id);
            ps.executeUpdate();
        }
    }

    /** Inserts the unique-key entries of a document; a clash within the logical partition is a 409. */
    private static void putUnique(Connection c, Coll cl, String pkh, String id, JsonObject body) throws SQLException {
        List<List<String>> keys = cl.uniqueKeys();
        for (int i = 0; i < keys.size(); i++) {
            JsonArray vals = new JsonArray();
            for (String p : keys.get(i)) {
                JsonElement v = CosmosJson.path(body, p);
                vals.add(v == null ? new JsonObject() : v); // a missing value is a value too ({} = undefined)
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_cosmos_uniq (db, coll, pkh, ux, k, id) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, cl.db());
                ps.setString(2, cl.id());
                ps.setString(3, pkh);
                ps.setInt(4, i);
                ps.setString(5, CosmosJson.canon(vals));
                ps.setString(6, id);
                if (ps.executeUpdate() == 0) {
                    throw CosmosException.conflict("Unique index constraint violation.");
                }
            }
        }
    }

    private static void bind(PreparedStatement ps, int idx, Integer v) throws SQLException {
        if (v == null) {
            ps.setNull(idx, java.sql.Types.INTEGER);
        } else {
            ps.setInt(idx, v);
        }
    }

    /** Creates a document; 409 when the id exists in the partition. */
    Doc create(Connection c, Coll cl, int hostIdx, String pkh, JsonObject body, Integer ttl) throws SQLException {
        String id = body.get("id").getAsString();
        if (cl.ttlEnabled()) {
            read(c, cl, pkh, id, true); // an expired document does not block the id
        }
        long lsn = nextLsn(c, cl);
        Doc d = new Doc(pkh, id, body, etag(), nowSec(), lsn, docRid(cl.rid(), hostIdx, lsn), ttl);
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cosmos_docs (db, coll, pkh, id, body, etag, ts, lsn, rid, ttl) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING")) {
            fill(ps, cl, d);
            if (ps.executeUpdate() == 0) {
                throw CosmosException.conflict("Entity with the specified id already exists in the system.");
            }
        }
        if (!cl.uniqueKeys().isEmpty()) {
            putUnique(c, cl, pkh, id, body);
        }
        return d;
    }

    private static void fill(PreparedStatement ps, Coll cl, Doc d) throws SQLException {
        ps.setString(1, cl.db());
        ps.setString(2, cl.id());
        ps.setString(3, d.pkh());
        ps.setString(4, d.id());
        ps.setString(5, d.body().toString());
        ps.setString(6, d.etag());
        ps.setLong(7, d.ts());
        ps.setLong(8, d.lsn());
        ps.setString(9, d.rid());
        bind(ps, 10, d.ttl());
    }

    /** Result of an upsert: the new version and whether it was inserted. */
    record Upserted(Doc doc, boolean created) {
    }

    Upserted upsert(Connection c, Coll cl, int hostIdx, String pkh, JsonObject body, Integer ttl, String ifMatch) throws SQLException {
        String id = body.get("id").getAsString();
        Doc cur = cl.ttlEnabled() || ifMatch != null || !cl.uniqueKeys().isEmpty() ? read(c, cl, pkh, id, true) : null;
        if (ifMatch != null && cur != null && !ifMatch.equals(cur.etag())) {
            throw CosmosException.precondition("Operation cannot be performed because one of the specified precondition is not met.");
        }
        long lsn = nextLsn(c, cl);
        Doc d = new Doc(pkh, id, body, etag(), nowSec(), lsn, cur != null ? cur.rid() : docRid(cl.rid(), hostIdx, lsn), ttl);
        boolean created;
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cosmos_docs (db, coll, pkh, id, body, etag, ts, lsn, rid, ttl) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (db, coll, pkh, id) DO UPDATE SET body = EXCLUDED.body, etag = EXCLUDED.etag, "
                + "ts = EXCLUDED.ts, lsn = EXCLUDED.lsn, ttl = EXCLUDED.ttl RETURNING (xmax = 0), rid")) {
            fill(ps, cl, d);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                created = rs.getBoolean(1);
                if (!created) {
                    d = new Doc(d.pkh(), d.id(), d.body(), d.etag(), d.ts(), d.lsn(), rs.getString(2), d.ttl());
                }
            }
        }
        if (!cl.uniqueKeys().isEmpty()) {
            dropUnique(c, cl, pkh, id);
            putUnique(c, cl, pkh, id, body);
        }
        return new Upserted(d, created);
    }

    Doc replace(Connection c, Coll cl, String pkh, JsonObject body, Integer ttl, String ifMatch) throws SQLException {
        String id = body.get("id").getAsString();
        Doc cur = read(c, cl, pkh, id, true);
        if (cur == null) {
            throw notFound();
        }
        if (ifMatch != null && !ifMatch.equals(cur.etag())) {
            throw CosmosException.precondition("Operation cannot be performed because one of the specified precondition is not met.");
        }
        return update(c, cl, cur, body, ttl);
    }

    private Doc update(Connection c, Coll cl, Doc cur, JsonObject body, Integer ttl) throws SQLException {
        long lsn = nextLsn(c, cl);
        Doc d = new Doc(cur.pkh(), cur.id(), body, etag(), nowSec(), lsn, cur.rid(), ttl);
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_cosmos_docs SET body = ?, etag = ?, ts = ?, lsn = ?, ttl = ? "
                + "WHERE db = ? AND coll = ? AND pkh = ? AND id = ?")) {
            ps.setString(1, body.toString());
            ps.setString(2, d.etag());
            ps.setLong(3, d.ts());
            ps.setLong(4, lsn);
            bind(ps, 5, ttl);
            ps.setString(6, cl.db());
            ps.setString(7, cl.id());
            ps.setString(8, cur.pkh());
            ps.setString(9, cur.id());
            ps.executeUpdate();
        }
        if (!cl.uniqueKeys().isEmpty()) {
            dropUnique(c, cl, cur.pkh(), cur.id());
            putUnique(c, cl, cur.pkh(), cur.id(), body);
        }
        return d;
    }

    /** Reads the document, lets {@code fn} compute its new body, and writes it back (patch). */
    Doc patch(Connection c, Coll cl, String pkh, String id, String ifMatch, java.util.function.Function<Doc, JsonObject> fn) throws SQLException {
        Doc cur = read(c, cl, pkh, id, true);
        if (cur == null) {
            throw notFound();
        }
        if (ifMatch != null && !ifMatch.equals(cur.etag())) {
            throw CosmosException.precondition("Operation cannot be performed because one of the specified precondition is not met.");
        }
        JsonObject nb = fn.apply(cur);
        return update(c, cl, cur, nb, CosmosService.ttlOf(nb));
    }

    void delete(Connection c, Coll cl, String pkh, String id, String ifMatch) throws SQLException {
        Doc cur = read(c, cl, pkh, id, true);
        if (cur == null) {
            throw notFound();
        }
        if (ifMatch != null && !ifMatch.equals(cur.etag())) {
            throw CosmosException.precondition("Operation cannot be performed because one of the specified precondition is not met.");
        }
        remove(c, cl, pkh, id, null);
    }

    // ------------------------------------------------------------------------------------------ scans

    /** Filters of a scan: a partition (exact or prefix) and/or an id. */
    record Filter(String pkh, String pkhPrefix, String id) {
        static final Filter NONE = new Filter(null, null, null);
    }

    private static String like(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%";
    }

    /** One keyset page of documents ordered by (pkh, id) after ({@code afterPkh}, {@code afterId}); expired ones are skipped. */
    List<Doc> scan(String host, Coll cl, Filter f, String afterPkh, String afterId, boolean inclusive, int limit) {
        return conn(host, c -> {
            StringBuilder sql = new StringBuilder("SELECT " + DOC_COLS + " FROM warp_cosmos_docs WHERE db = ? AND coll = ?");
            List<Object> args = new ArrayList<>(List.of(cl.db(), cl.id()));
            if (f.pkh() != null) {
                sql.append(" AND pkh = ?");
                args.add(f.pkh());
            } else if (f.pkhPrefix() != null) {
                sql.append(" AND pkh LIKE ? ESCAPE '\\'");
                args.add(like(f.pkhPrefix()));
            }
            if (f.id() != null) {
                sql.append(" AND id = ?");
                args.add(f.id());
            }
            if (afterPkh != null) {
                sql.append(inclusive ? " AND (pkh, id) >= (?, ?)" : " AND (pkh, id) > (?, ?)");
                args.add(afterPkh);
                args.add(afterId);
            }
            sql.append(" ORDER BY pkh, id LIMIT ?");
            args.add(limit);
            List<Doc> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                for (int i = 0; i < args.size(); i++) {
                    Object a = args.get(i);
                    if (a instanceof Integer n) {
                        ps.setInt(i + 1, n);
                    } else {
                        ps.setString(i + 1, (String) a);
                    }
                }
                long now = nowSec();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Doc d = doc(rs);
                        out.add(d.expired(cl, now) ? new Doc(d.pkh(), d.id(), null, null, 0, 0, null, null) : d);
                    }
                }
            }
            return out;
        });
    }

    /** Documents changed after {@code afterLsn} on one host in _lsn order (the change feed). */
    List<Doc> feed(String host, Coll cl, long afterLsn, String pkh, long modifiedSinceSec, int limit) {
        return conn(host, c -> {
            StringBuilder sql = new StringBuilder("SELECT " + DOC_COLS + " FROM warp_cosmos_docs WHERE db = ? AND coll = ? AND lsn > ?");
            if (pkh != null) {
                sql.append(" AND pkh = ?");
            }
            if (modifiedSinceSec > 0) {
                sql.append(" AND ts >= ?");
            }
            sql.append(" ORDER BY lsn LIMIT ?");
            List<Doc> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                int i = 1;
                ps.setString(i++, cl.db());
                ps.setString(i++, cl.id());
                ps.setLong(i++, afterLsn);
                if (pkh != null) {
                    ps.setString(i++, pkh);
                }
                if (modifiedSinceSec > 0) {
                    ps.setLong(i++, modifiedSinceSec);
                }
                ps.setInt(i, limit);
                long now = nowSec();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Doc d = doc(rs);
                        if (!d.expired(cl, now)) {
                            out.add(d);
                        }
                    }
                }
            }
            return out;
        });
    }

    long maxLsn(String host, Coll cl) {
        return conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT n FROM warp_cosmos_seq WHERE db = ? AND coll = ?")) {
                ps.setString(1, cl.db());
                ps.setString(2, cl.id());
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong(1) : 0L;
                }
            }
        });
    }

    long count(String host, Coll cl) {
        return conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_cosmos_docs WHERE db = ? AND coll = ?")) {
                ps.setString(1, cl.db());
                ps.setString(2, cl.id());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    /** Finds a document by its _rid within a container (any host). */
    Doc byRid(Coll cl, String rid) {
        for (String h : hosts()) {
            Doc d = conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT " + DOC_COLS + " FROM warp_cosmos_docs WHERE db = ? AND coll = ? AND rid = ?")) {
                    ps.setString(1, cl.db());
                    ps.setString(2, cl.id());
                    ps.setString(3, rid);
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next() ? doc(rs) : null;
                    }
                }
            });
            if (d != null) {
                return d;
            }
        }
        return null;
    }

    /** Deletes expired documents of every container with a default TTL; returns the number removed. */
    int sweepExpired() {
        int total = 0;
        List<Coll> colls = new ArrayList<>();
        for (DbRow d : listDbs()) {
            for (Coll c : listColls(d.id())) {
                if (c.ttlEnabled()) {
                    colls.add(c);
                }
            }
        }
        long now = nowSec();
        for (Coll cl : colls) {
            for (String h : hosts()) {
                total += tx(h, c -> {
                    try (PreparedStatement ps = c.prepareStatement("WITH d AS (DELETE FROM warp_cosmos_docs WHERE db = ? AND coll = ? "
                            + "AND COALESCE(ttl, ?) > 0 AND ts + COALESCE(ttl, ?) <= ? RETURNING pkh, id), "
                            + "u AS (DELETE FROM warp_cosmos_uniq x USING d WHERE x.db = ? AND x.coll = ? AND x.pkh = d.pkh AND x.id = d.id RETURNING 1) "
                            + "SELECT count(*) FROM d")) {
                        ps.setString(1, cl.db());
                        ps.setString(2, cl.id());
                        ps.setInt(3, cl.defaultTtl());
                        ps.setInt(4, cl.defaultTtl());
                        ps.setLong(5, now);
                        ps.setString(6, cl.db());
                        ps.setString(7, cl.id());
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                            return rs.getInt(1);
                        }
                    }
                });
            }
        }
        return total;
    }
}
