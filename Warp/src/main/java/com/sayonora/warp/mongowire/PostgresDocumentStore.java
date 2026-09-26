package com.sayonora.warp.mongowire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ShardingStrategy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Document storage for mongowire on Postgres. Every collection is a table
 * {@code "<db>"."<collection>"(id text PRIMARY KEY, doc jsonb, bson bytea, seq bigint identity)}:
 * {@code bson} is the authoritative, type-exact BSON document (field order and int32/int64/double/
 * decimal/date/binary types survive), {@code doc} is a relaxed-JSON mirror for SQL/MCP readers and
 * {@code seq} gives natural (insertion) order. {@code id} is a canonical key of {@code _id} (numbers are
 * normalised so 1, 1L and 1.0 collide, as they do in MongoDB).
 *
 * <p>With several backends in the set the document is stored on the shard picked by hashing its id key;
 * queries scan every shard and are evaluated in Java (MongoMatcher / MongoAgg), so results are exact
 * whatever the topology. Unique secondary indexes are enforced through a key-ownership table
 * ({@code "<db>"."__warp_uk"}) whose rows live on the shard chosen by hashing the index key, so two
 * documents that would collide always meet on one shard.
 */
final class PostgresDocumentStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresDocumentStore.class);
    static final String CATALOG = "__warp_catalog";
    static final String UNIQUE_KEYS = "__warp_uk";
    private static final long META_TTL_MS = 3000;

    private final ConnectionSupplier legacyConnections;
    private final BackendRegistry backendRegistry;
    private static final ConcurrentHashMap<String, Boolean> ENSURED = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, CachedMeta> META = new ConcurrentHashMap<>();
    private volatile List<String> lastLoggedShardGroup = null;

    private static final java.util.Set<String> knownPhysicalTables = ConcurrentHashMap.newKeySet();

    static boolean isKnownPhysicalTable(String physicalTableLower) {
        return knownPhysicalTables.contains(physicalTableLower);
    }

    interface ConnectionSupplier {
        Connection get() throws SQLException;
    }

    PostgresDocumentStore(ConnectionSupplier connections) {
        this.legacyConnections = connections;
        this.backendRegistry = null;
    }

    PostgresDocumentStore(BackendRegistry backendRegistry) {
        this.legacyConnections = null;
        this.backendRegistry = backendRegistry;
        logShardGroupIfChanged();
    }

    private volatile List<String> routedGroup;

    void routeTo(List<String> group) {
        this.routedGroup = group;
    }

    boolean isRouted() {
        return routedGroup != null;
    }

    private List<String> currentShardGroup() {
        List<String> routed = routedGroup;
        if (routed != null) {
            return routed;
        }
        return backendRegistry == null ? List.of()
                : backendRegistry.storeShardGroup(com.sayonora.warp.core.StoreType.MONGODB);
    }

    private void logShardGroupIfChanged() {
        List<String> group = currentShardGroup();
        if (!group.equals(lastLoggedShardGroup)) {
            lastLoggedShardGroup = group;
            if (!group.isEmpty()) {
                log.info("mongowire: sharding document storage across {} backend(s) by _id: {}", group.size(), group);
            }
        }
    }

    boolean isSharded() {
        return currentShardGroup().size() > 1;
    }

    String resolveBackendFor(String idKey) {
        if (legacyConnections != null || idKey == null) {
            return "default";
        }
        List<String> group = currentShardGroup();
        return group.isEmpty() ? BackendRegistry.DEFAULT_BACKEND_NAME : ShardingStrategy.hash(group).resolve(idKey);
    }

    // ------------------------------------------------------------------ shards / connections

    /** A backend a document may live on. */
    private final class Shard {
        final String name;

        Shard(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Shard other && other.name.equals(name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        Connection open() throws SQLException {
            if (legacyConnections != null) {
                return legacyConnections.get();
            }
            BackendTarget target = backendRegistry.resolveForRouting(name);
            if (target == null) {
                throw new IllegalStateException("mongowire: resolved shard backend \"" + name + "\" is not configured");
            }
            return target.open();
        }
    }

    private List<Shard> shards() {
        if (legacyConnections != null) {
            return List.of(new Shard("default"));
        }
        logShardGroupIfChanged();
        List<String> group = currentShardGroup();
        List<String> names = group.isEmpty() ? List.of(BackendRegistry.DEFAULT_BACKEND_NAME) : group;
        List<Shard> out = new ArrayList<>();
        for (String n : names) {
            out.add(new Shard(n));
        }
        return out;
    }

    private Shard shardForKey(String key) {
        List<Shard> all = shards();
        if (all.size() == 1) {
            return all.get(0);
        }
        List<String> names = new ArrayList<>();
        all.forEach(s -> names.add(s.name));
        String n = ShardingStrategy.hash(names).resolve(key);
        for (Shard s : all) {
            if (s.name.equals(n)) {
                return s;
            }
        }
        return all.get(0);
    }

    private String shardScope(Shard s) throws SQLException {
        if (legacyConnections != null) {
            try (Connection c = s.open()) {
                return c.getMetaData().getURL();
            }
        }
        return s.name;
    }

    // ------------------------------------------------------------------ names

    static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    static void validateDb(String db) {
        if (db.isEmpty()) {
            throw new MongoCmdException(73, "Invalid database name: ''");
        }
        for (char c : db.toCharArray()) {
            if (c == '/' || c == '\\' || c == '.' || c == ' ' || c == '"' || c == '$' || c == '*' || c == '<' || c == '>' || c == ':'
                    || c == '|' || c == '?' || c == 0) {
                throw new MongoCmdException(73, "Invalid database name: '" + db + "'");
            }
        }
        if (db.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 63) {
            throw new MongoCmdException(73, "database name must be at most 63 bytes on this Postgres-backed server: '" + db + "'");
        }
        String l = db.toLowerCase(java.util.Locale.ROOT);
        if (l.startsWith("pg_") || l.equals("information_schema") || l.equals("public")) {
            throw new MongoCmdException(73, "Invalid database name: '" + db + "' (reserved by the Postgres backend)");
        }
    }

    static void validateColl(String coll) {
        if (coll.isEmpty()) {
            throw new MongoCmdException(73, "Invalid namespace specified '.'");
        }
        if (coll.indexOf('$') >= 0 || coll.indexOf('\0') >= 0) {
            throw new MongoCmdException(73, "Invalid collection name: " + coll);
        }
        if (coll.startsWith("__warp_")) {
            throw new MongoCmdException(73, "Invalid collection name (reserved prefix __warp_): " + coll);
        }
        if (coll.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 63) {
            throw new MongoCmdException(73, "collection name must be at most 63 bytes on this Postgres-backed server: " + coll);
        }
    }

    static String qualifiedTable(String db, String collection) {
        return quoteIdent(db) + "." + quoteIdent(collection);
    }

    private static String qualified(String db, String name) {
        return quoteIdent(db) + "." + quoteIdent(name);
    }

    // ------------------------------------------------------------------ ids / json

    static String idKey(BsonValue v) {
        switch (v.getBsonType()) {
            case INT32: case INT64: case DOUBLE: case DECIMAL128: {
                String k = BsonCmp.key(v);
                return k.substring(2);
            }
            case STRING: return jsonString(v.asString().getValue());
            case OBJECT_ID: return "{\"$oid\":\"" + v.asObjectId().getValue().toHexString() + "\"}";
            default: return BsonJson.valueToJson(v);
        }
    }

    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20 || c == 0x7f) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static String docJson(BsonDocument doc) {
        String j = doc.toJson(org.bson.json.JsonWriterSettings.builder().outputMode(org.bson.json.JsonMode.RELAXED).build());
        return j.indexOf("\\u0000") >= 0 ? j.replace("\\u0000", "\\ufffd") : j;
    }

    private static PGobject jsonb(String json) throws SQLException {
        PGobject obj = new PGobject();
        obj.setType("jsonb");
        obj.setValue(json);
        return obj;
    }

    // ------------------------------------------------------------------ schema / catalog

    /** Result of {@link #meta}. */
    static final class CollMeta {
        final boolean exists;
        final BsonDocument options;
        final List<BsonDocument> indexes; // excluding the implicit _id_ index
        final BsonBinary uuid;
        volatile MongoMatcher.Pred validator;
        final List<UniqueIndex> uniques = new ArrayList<>();

        CollMeta(boolean exists, BsonDocument options, List<BsonDocument> indexes, BsonBinary uuid) {
            this.exists = exists;
            this.options = options;
            this.indexes = indexes;
            this.uuid = uuid;
            for (BsonDocument ix : indexes) {
                if (ix.containsKey("unique") && ix.getBoolean("unique").getValue()) {
                    uniques.add(new UniqueIndex(ix));
                }
            }
        }

        MongoMatcher.Pred validator() {
            if (!options.containsKey("validator") || options.getDocument("validator").isEmpty()) {
                return null;
            }
            MongoMatcher.Pred v = validator;
            if (v == null) {
                v = MongoMatcher.compile(options.getDocument("validator"));
                validator = v;
            }
            return v;
        }
    }

    static final class UniqueIndex {
        final String name;
        final BsonDocument key;
        final boolean sparse;
        final MongoMatcher.Pred partial;
        final BsonCmp.Collation collation;

        UniqueIndex(BsonDocument spec) {
            this.collation = spec.containsKey("collation") ? MongoCrud.collationFrom(spec.getDocument("collation")) : null;
            this.name = spec.getString("name").getValue();
            this.key = spec.getDocument("key");
            this.sparse = spec.containsKey("sparse") && spec.getBoolean("sparse").getValue();
            this.partial = spec.containsKey("partialFilterExpression")
                    ? MongoMatcher.compile(spec.getDocument("partialFilterExpression")) : null;
        }
    }

    private record CachedMeta(CollMeta meta, long expires) {
    }

    private static String metaKey(String scope, String db, String coll) {
        return scope + "|" + db + "." + coll;
    }

    void invalidateMeta(String db, String coll) {
        META.keySet().removeIf(k -> k.endsWith("|" + db + "." + coll));
    }

    private void ensureSchemaObjects(Connection conn, String db) throws SQLException {
        try (var st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS " + quoteIdent(db));
            st.execute("CREATE TABLE IF NOT EXISTS " + qualified(db, CATALOG) + " (name text PRIMARY KEY, entry bytea NOT NULL)");
            st.execute("CREATE TABLE IF NOT EXISTS " + qualified(db, UNIQUE_KEYS)
                    + " (coll text NOT NULL, idx text NOT NULL, k text NOT NULL, id text NOT NULL, PRIMARY KEY (coll, idx, k))");
            st.execute("CREATE INDEX IF NOT EXISTS " + quoteIdent("__warp_uk_id_" + Integer.toHexString(db.hashCode())) + " ON "
                    + qualified(db, UNIQUE_KEYS) + " (coll, id)");
        }
    }

    private void createTable(Connection conn, String db, String coll) throws SQLException {
        String t = qualified(db, coll);
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS " + t
                    + " (id text PRIMARY KEY, doc jsonb NOT NULL, bson bytea, seq bigint GENERATED ALWAYS AS IDENTITY)");
            st.execute("ALTER TABLE " + t + " ADD COLUMN IF NOT EXISTS bson bytea");
            st.execute("ALTER TABLE " + t + " ADD COLUMN IF NOT EXISTS seq bigint GENERATED ALWAYS AS IDENTITY");
            st.execute("CREATE INDEX IF NOT EXISTS " + quoteIdent("wseq_" + Integer.toHexString((db + "." + coll).hashCode()))
                    + " ON " + t + " (seq)");
        }
    }

    private boolean tableExists(Connection conn, String db, String coll) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
            ps.setString(1, qualified(db, coll));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /** Creates the collection's table, catalog row and support tables on every shard. Returns true when it was created now. */
    boolean createCollection(String db, String coll, BsonDocument options) throws SQLException {
        validateDb(db);
        validateColl(coll);
        boolean created = false;
        BsonBinary uuid = new BsonBinary(java.util.UUID.randomUUID());
        for (int attempt = 0; ; attempt++) {
            try {
                for (Shard s : shards()) {
                    try (Connection conn = s.open()) {
                        boolean existed = tableExists(conn, db, coll);
                        ensureSchemaObjects(conn, db);
                        createTable(conn, db, coll);
                        BsonDocument entry = catalogEntry(options == null ? new BsonDocument() : options, List.of(), uuid);
                        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + qualified(db, CATALOG)
                                + " (name, entry) VALUES (?, ?) ON CONFLICT (name) DO NOTHING")) {
                            ps.setString(1, coll);
                            ps.setBytes(2, MongoBson.encode(entry));
                            ps.executeUpdate();
                        }
                        if (!existed) {
                            created = true;
                        }
                        ENSURED.put(shardScope(s) + "|" + db + "." + coll, Boolean.TRUE);
                    }
                }
                break;
            } catch (SQLException e) {
                if (attempt < 3 && ("23505".equals(e.getSQLState()) || "42P07".equals(e.getSQLState()) || "40P01".equals(e.getSQLState())
                        || "XX000".equals(e.getSQLState()))) {
                    continue;
                }
                throw e;
            }
        }
        knownPhysicalTables.add((db + "." + coll).toLowerCase(java.util.Locale.ROOT));
        invalidateMeta(db, coll);
        return created;
    }

    private static BsonDocument catalogEntry(BsonDocument options, List<BsonDocument> indexes, BsonBinary uuid) {
        return new BsonDocument("options", options).append("indexes", new BsonArray(new ArrayList<BsonValue>(indexes)))
                .append("uuid", uuid).append("created", new BsonDateTime(System.currentTimeMillis()));
    }

    /** Makes sure the collection exists (implicit creation on first write). */
    void ensureCollection(String db, String coll) throws SQLException {
        validateDb(db);
        validateColl(coll);
        List<Shard> all = shards();
        boolean allEnsured = true;
        for (Shard s : all) {
            if (!ENSURED.containsKey(shardScope(s) + "|" + db + "." + coll)) {
                allEnsured = false;
                break;
            }
        }
        knownPhysicalTables.add((db + "." + coll).toLowerCase(java.util.Locale.ROOT));
        if (allEnsured) {
            return;
        }
        createCollection(db, coll, new BsonDocument());
    }

    CollMeta meta(String db, String coll) throws SQLException {
        validateDb(db);
        List<Shard> all = shards();
        Shard first = all.get(0);
        String mk = metaKey(shardScope(first), db, coll);
        CachedMeta cm = META.get(mk);
        long now = System.currentTimeMillis();
        if (cm != null && cm.expires > now) {
            return cm.meta;
        }
        CollMeta m = loadMeta(first, db, coll);
        META.put(mk, new CachedMeta(m, now + META_TTL_MS));
        return m;
    }

    private CollMeta loadMeta(Shard shard, String db, String coll) throws SQLException {
        try (Connection conn = shard.open()) {
            if (!tableExists(conn, db, coll)) {
                return new CollMeta(false, new BsonDocument(), List.of(), new BsonBinary(new byte[16]));
            }
            byte[] entry = null;
            try (PreparedStatement ps = conn.prepareStatement("SELECT entry FROM " + qualified(db, CATALOG) + " WHERE name = ?")) {
                ps.setString(1, coll);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        entry = rs.getBytes(1);
                    }
                }
            } catch (SQLException e) {
                if (!"42P01".equals(e.getSQLState())) {
                    throw e;
                }
            }
            if (entry == null) {
                // a collection created outside Warp's catalog (legacy table): synthesize a stable identity
                return new CollMeta(true, new BsonDocument(), List.of(), new BsonBinary(
                        java.util.UUID.nameUUIDFromBytes((db + "." + coll).getBytes(java.nio.charset.StandardCharsets.UTF_8))));
            }
            BsonDocument e = MongoBson.decode(entry);
            List<BsonDocument> ix = new ArrayList<>();
            for (BsonValue v : e.getArray("indexes")) {
                ix.add(v.asDocument());
            }
            return new CollMeta(true, e.getDocument("options"), ix, e.getBinary("uuid"));
        }
    }

    void saveMeta(String db, String coll, BsonDocument options, List<BsonDocument> indexes, BsonBinary uuid) throws SQLException {
        byte[] bytes = MongoBson.encode(catalogEntry(options, indexes, uuid));
        for (Shard s : shards()) {
            try (Connection conn = s.open(); PreparedStatement ps = conn.prepareStatement("INSERT INTO " + qualified(db, CATALOG)
                    + " (name, entry) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET entry = excluded.entry")) {
                ps.setString(1, coll);
                ps.setBytes(2, bytes);
                ps.executeUpdate();
            }
        }
        invalidateMeta(db, coll);
    }

    List<String> listCollections(String db) throws SQLException {
        java.util.TreeSet<String> names = new java.util.TreeSet<>();
        for (Shard s : shards()) {
            try (Connection conn = s.open(); PreparedStatement ps = conn.prepareStatement(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' "
                            + "AND table_name NOT LIKE '\\_\\_warp\\_%'")) {
                ps.setString(1, db);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        names.add(rs.getString(1));
                    }
                }
            }
        }
        return new ArrayList<>(names);
    }

    List<String> listDatabases() throws SQLException {
        java.util.TreeSet<String> names = new java.util.TreeSet<>();
        for (Shard s : shards()) {
            try (Connection conn = s.open(); PreparedStatement ps = conn.prepareStatement(
                    "SELECT DISTINCT c.table_schema FROM information_schema.columns c JOIN information_schema.columns d "
                            + "ON d.table_schema = c.table_schema AND d.table_name = c.table_name AND d.column_name = 'doc' "
                            + "AND d.data_type = 'jsonb' WHERE c.column_name = 'id' AND c.data_type = 'text' "
                            + "AND c.table_schema NOT IN ('public', 'pg_catalog', 'information_schema') AND c.table_name NOT LIKE '\\_\\_warp\\_%'");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        }
        return new ArrayList<>(names);
    }

    boolean dropCollection(String db, String coll) throws SQLException {
        validateDb(db);
        boolean existed = false;
        for (Shard s : shards()) {
            try (Connection conn = s.open()) {
                if (tableExists(conn, db, coll)) {
                    existed = true;
                }
                try (var st = conn.createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + qualified(db, coll));
                }
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + qualified(db, CATALOG) + " WHERE name = ?")) {
                    ps.setString(1, coll);
                    ps.executeUpdate();
                } catch (SQLException e) {
                    if (!"42P01".equals(e.getSQLState())) {
                        throw e;
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + qualified(db, UNIQUE_KEYS) + " WHERE coll = ?")) {
                    ps.setString(1, coll);
                    ps.executeUpdate();
                } catch (SQLException e) {
                    if (!"42P01".equals(e.getSQLState())) {
                        throw e;
                    }
                }
                ENSURED.remove(shardScope(s) + "|" + db + "." + coll);
            }
        }
        invalidateMeta(db, coll);
        return existed;
    }

    boolean dropDatabase(String db) throws SQLException {
        validateDb(db);
        boolean existed = false;
        for (Shard s : shards()) {
            try (Connection conn = s.open()) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM information_schema.schemata WHERE schema_name = ?")) {
                    ps.setString(1, db);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            existed = true;
                        }
                    }
                }
                try (var st = conn.createStatement()) {
                    st.execute("DROP SCHEMA IF EXISTS " + quoteIdent(db) + " CASCADE");
                }
            }
        }
        ENSURED.keySet().removeIf(k -> k.contains("|" + db + "."));
        META.keySet().removeIf(k -> k.contains("|" + db + "."));
        knownPhysicalTables.removeIf(t -> t.startsWith(db.toLowerCase(java.util.Locale.ROOT) + "."));
        return existed;
    }

    void renameCollection(String db, String from, String to, boolean dropTarget) throws SQLException {
        validateDb(db);
        validateColl(to);
        for (Shard s : shards()) {
            try (Connection conn = s.open()) {
                conn.setAutoCommit(false);
                try (var st = conn.createStatement()) {
                    if (dropTarget) {
                        st.execute("DROP TABLE IF EXISTS " + qualified(db, to));
                        st.execute("DELETE FROM " + qualified(db, CATALOG) + " WHERE name = " + literal(to));
                        st.execute("DELETE FROM " + qualified(db, UNIQUE_KEYS) + " WHERE coll = " + literal(to));
                    }
                    st.execute("ALTER TABLE " + qualified(db, from) + " RENAME TO " + quoteIdent(to));
                    st.execute("UPDATE " + qualified(db, CATALOG) + " SET name = " + literal(to) + " WHERE name = " + literal(from));
                    st.execute("UPDATE " + qualified(db, UNIQUE_KEYS) + " SET coll = " + literal(to) + " WHERE coll = " + literal(from));
                    createTable(conn, db, to);
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
            ENSURED.remove(shardScope(s) + "|" + db + "." + from);
            ENSURED.put(shardScope(s) + "|" + db + "." + to, Boolean.TRUE);
        }
        invalidateMeta(db, from);
        invalidateMeta(db, to);
        knownPhysicalTables.add((db + "." + to).toLowerCase(java.util.Locale.ROOT));
    }

    private static String literal(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    // ------------------------------------------------------------------ reads

    /**
     * Natural-order scan by keyset pagination over {@code seq}: each chunk is one short query on a pooled connection
     * that is returned immediately, so an open MongoDB cursor never pins a Postgres connection.
     */
    private final class RowIterator implements Iterator<BsonDocument>, AutoCloseable {
        private static final int CHUNK = 500;
        private final List<Shard> shards;
        private final String db;
        private final String coll;
        private final List<String> idKeys;
        private int shardIdx = 0;
        private long lastSeq = Long.MIN_VALUE;
        private java.util.ArrayDeque<BsonDocument> buffer = new java.util.ArrayDeque<>();
        private boolean shardDone;
        private boolean closed;
        private boolean repaired;

        RowIterator(List<Shard> shards, String db, String coll, List<String> idKeys) {
            this.shards = shards;
            this.db = db;
            this.coll = coll;
            this.idKeys = idKeys;
        }

        private boolean fill() {
            while (buffer.isEmpty()) {
                if (closed || shardIdx >= shards.size()) {
                    return false;
                }
                if (shardDone) {
                    shardIdx++;
                    lastSeq = Long.MIN_VALUE;
                    shardDone = false;
                    continue;
                }
                String sql = "SELECT seq, bson, CASE WHEN bson IS NULL THEN doc::text END FROM " + qualified(db, coll)
                        + " WHERE seq > ?" + (idKeys != null ? " AND id = ANY(?)" : "") + " ORDER BY seq LIMIT " + CHUNK;
                try (Connection conn = shards.get(shardIdx).open(); PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, lastSeq);
                    if (idKeys != null) {
                        ps.setArray(2, conn.createArrayOf("text", idKeys.toArray()));
                    }
                    int n = 0;
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            n++;
                            lastSeq = rs.getLong(1);
                            byte[] bson = rs.getBytes(2);
                            buffer.add(bson != null ? MongoBson.decode(bson) : BsonDocument.parse(rs.getString(3)));
                        }
                    }
                    if (n < CHUNK) {
                        shardDone = true;
                    }
                } catch (SQLException e) {
                    if ("42P01".equals(e.getSQLState()) || "3F000".equals(e.getSQLState())) {
                        shardDone = true;
                        continue;
                    }
                    if ("42703".equals(e.getSQLState()) && !repaired) {
                        repaired = true;
                        try (Connection conn = shards.get(shardIdx).open()) {
                            createTable(conn, db, coll);
                        } catch (SQLException e2) {
                            throw new MongoCrud.SqlFailure(e2);
                        }
                        continue;
                    }
                    throw new MongoCrud.SqlFailure(e);
                }
            }
            return true;
        }

        @Override
        public boolean hasNext() {
            return !buffer.isEmpty() || fill();
        }

        @Override
        public BsonDocument next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buffer.poll();
        }

        @Override
        public void close() {
            closed = true;
            buffer.clear();
        }
    }

    /** Documents of a collection in natural order (each shard in turn); the stream must be closed. */
    Stream<BsonDocument> scan(String db, String coll, List<String> idKeys) {
        validateDb(db);
        List<Shard> targets = shards();
        if (idKeys != null && targets.size() > 1) {
            java.util.LinkedHashSet<Shard> chosen = new java.util.LinkedHashSet<>();
            for (String k : idKeys) {
                chosen.add(shardForKey(k));
            }
            targets = new ArrayList<>(chosen);
        }
        RowIterator it = new RowIterator(targets, db, coll, idKeys);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false).onClose(it::close);
    }

    // ------------------------------------------------------------------ unique key ownership

    static String ukSep() {
        return "\u0001";
    }

    /** Index-entry keys a document contributes to a unique index (empty when skipped by sparse/partial). */
    static List<String> uniqueKeys(BsonDocument doc, UniqueIndex ix) {
        return uniqueEntries(doc, ix).stream().map(IndexKeys.Entry::text).toList();
    }

    static List<IndexKeys.Entry> uniqueEntries(BsonDocument doc, UniqueIndex ix) {
        if (ix.partial != null && !ix.partial.test(doc)) {
            return List.of();
        }
        return IndexKeys.keys(doc, ix.key, ix.sparse, ix.collation);
    }

    private MongoCmdException duplicate(String db, String coll, String indexName, BsonDocument keyPattern, BsonDocument doc) {
        return duplicate(db, coll, indexName, keyPattern, doc, null);
    }

    private MongoCmdException duplicate(String db, String coll, String indexName, BsonDocument keyPattern, BsonDocument doc,
            List<BsonValue> values) {
        BsonDocument keyValue = new BsonDocument();
        int i = 0;
        for (String f : keyPattern.keySet()) {
            BsonValue v = values != null && i < values.size() ? values.get(i) : firstResolved(doc, f);
            keyValue.put(f, v);
            i++;
        }
        BsonDocument extra = new BsonDocument("keyPattern", keyPattern.clone()).append("keyValue", keyValue);
        return new MongoCmdException(11000, "E11000 duplicate key error collection: " + db + "." + coll + " index: " + indexName
                + " dup key: " + MongoFmt.doc(keyValue), extra);
    }

    private static BsonValue firstResolved(BsonDocument doc, String path) {
        for (BsonValue v : MongoMatcher.resolve(doc, path)) {
            return v == MongoMatcher.MISSING ? BsonNull.VALUE : v;
        }
        return BsonNull.VALUE;
    }

    private record Claim(Shard shard, String coll, String idx, String key) {
    }

    private List<Claim> claimUniqueKeys(String db, String coll, CollMeta meta, BsonDocument doc, String id) throws SQLException {
        List<Claim> claimed = new ArrayList<>();
        try {
            for (UniqueIndex ix : meta.uniques) {
                for (IndexKeys.Entry entry : uniqueEntries(doc, ix)) {
                    String k = entry.text();
                    Shard home = shards().size() == 1 ? shards().get(0) : shardForKey("uk:" + coll + ":" + ix.name + ":" + k);
                    try (Connection conn = home.open()) {
                        int n;
                        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + qualified(db, UNIQUE_KEYS)
                                + " (coll, idx, k, id) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
                            ps.setString(1, coll);
                            ps.setString(2, ix.name);
                            ps.setString(3, k);
                            ps.setString(4, id);
                            n = ps.executeUpdate();
                        }
                        if (n == 0) {
                            String owner = null;
                            try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM " + qualified(db, UNIQUE_KEYS)
                                    + " WHERE coll = ? AND idx = ? AND k = ?")) {
                                ps.setString(1, coll);
                                ps.setString(2, ix.name);
                                ps.setString(3, k);
                                try (ResultSet rs = ps.executeQuery()) {
                                    if (rs.next()) {
                                        owner = rs.getString(1);
                                    }
                                }
                            }
                            if (!id.equals(owner)) {
                                throw duplicate(db, coll, ix.name, ix.key, doc, entry.values());
                            }
                        } else {
                            claimed.add(new Claim(home, coll, ix.name, k));
                        }
                    }
                }
            }
        } catch (RuntimeException | SQLException e) {
            releaseClaims(db, claimed);
            throw e;
        }
        return claimed;
    }

    private void releaseClaims(String db, List<Claim> claims) {
        for (Claim c : claims) {
            try (Connection conn = c.shard.open(); PreparedStatement ps = conn.prepareStatement("DELETE FROM "
                    + qualified(db, UNIQUE_KEYS) + " WHERE coll = ? AND idx = ? AND k = ?")) {
                ps.setString(1, c.coll);
                ps.setString(2, c.idx);
                ps.setString(3, c.key);
                ps.executeUpdate();
            } catch (SQLException e) {
                log.warn("mongowire: could not release unique key claim: {}", e.getMessage());
            }
        }
    }

    private void releaseDocKeys(String db, String coll, CollMeta meta, BsonDocument doc) {
        List<Claim> claims = new ArrayList<>();
        for (UniqueIndex ix : meta.uniques) {
            for (String k : uniqueKeys(doc, ix)) {
                Shard home = shards().size() == 1 ? shards().get(0) : shardForKey("uk:" + coll + ":" + ix.name + ":" + k);
                claims.add(new Claim(home, coll, ix.name, k));
            }
        }
        releaseClaims(db, claims);
    }

    // ------------------------------------------------------------------ writes

    /** Inserts a document (must carry _id). Duplicate keys raise a MongoCmdException(11000). */
    void insert(String db, String coll, BsonDocument doc, CollMeta meta) throws SQLException {
        String id = idKey(doc.get("_id"));
        byte[] bson = MongoBson.encode(doc);
        String json = docJson(doc);
        List<Claim> claims = meta.uniques.isEmpty() ? List.of() : claimUniqueKeys(db, coll, meta, doc, id);
        String sql = "INSERT INTO " + qualified(db, coll) + " (id, doc, bson) VALUES (?, ?, ?)";
        try {
            for (int attempt = 0; ; attempt++) {
                try (Connection conn = shardForKey(id).open(); PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, id);
                    ps.setObject(2, jsonb(json));
                    ps.setBytes(3, bson);
                    ps.executeUpdate();
                    return;
                } catch (SQLException e) {
                    if ("42P01".equals(e.getSQLState()) || "3F000".equals(e.getSQLState())) {
                        if (attempt == 0) {
                            ENSURED.keySet().removeIf(k -> k.endsWith("|" + db + "." + coll));
                            createCollection(db, coll, new BsonDocument());
                            continue;
                        }
                    }
                    if ("23505".equals(e.getSQLState())) {
                        throw duplicate(db, coll, "_id_", new BsonDocument("_id", new BsonInt32(1)), doc);
                    }
                    throw e;
                }
            }
        } catch (RuntimeException | SQLException e) {
            releaseClaims(db, claims);
            throw e;
        }
    }

    /** Convenience used by tests and simple callers. */
    Document insertOne(String db, String collection, Document document) throws SQLException {
        if (!document.containsKey("_id")) {
            document.put("_id", new ObjectId());
        }
        BsonDocument bd = BsonJson.toBson(document);
        ensureCollection(db, collection);
        insert(db, collection, bd, meta(db, collection));
        return document;
    }

    /** Replaces {@code oldDoc} with {@code newDoc} (same _id); returns false if the stored document changed meanwhile. */
    boolean replace(String db, String coll, BsonDocument oldDoc, BsonDocument newDoc, CollMeta meta) throws SQLException {
        String id = idKey(oldDoc.get("_id"));
        byte[] oldBytes = MongoBson.encode(oldDoc);
        byte[] newBytes = MongoBson.encode(newDoc);
        List<Claim> newClaims = List.of();
        boolean keysChanged = false;
        if (!meta.uniques.isEmpty()) {
            for (UniqueIndex ix : meta.uniques) {
                if (!uniqueKeys(oldDoc, ix).equals(uniqueKeys(newDoc, ix))) {
                    keysChanged = true;
                }
            }
            if (keysChanged) {
                newClaims = claimUniqueKeys(db, coll, meta, newDoc, id);
            }
        }
        boolean updated;
        try (Connection conn = shardForKey(id).open(); PreparedStatement ps = conn.prepareStatement("UPDATE " + qualified(db, coll)
                + " SET doc = ?, bson = ? WHERE id = ? AND bson = ?")) {
            ps.setObject(1, jsonb(docJson(newDoc)));
            ps.setBytes(2, newBytes);
            ps.setString(3, id);
            ps.setBytes(4, oldBytes);
            updated = ps.executeUpdate() > 0;
            if (!updated) {
                // legacy row (bson column not yet populated): fall back to id-only match
                try (PreparedStatement ps2 = conn.prepareStatement("UPDATE " + qualified(db, coll)
                        + " SET doc = ?, bson = ? WHERE id = ? AND bson IS NULL")) {
                    ps2.setObject(1, jsonb(docJson(newDoc)));
                    ps2.setBytes(2, newBytes);
                    ps2.setString(3, id);
                    updated = ps2.executeUpdate() > 0;
                }
            }
        } catch (SQLException | RuntimeException e) {
            releaseClaims(db, newClaims);
            throw e;
        }
        if (!updated) {
            releaseClaims(db, newClaims);
            return false;
        }
        if (keysChanged) {
            // drop old claims that the new document no longer holds
            List<Claim> stale = new ArrayList<>();
            for (UniqueIndex ix : meta.uniques) {
                List<String> keep = uniqueKeys(newDoc, ix);
                for (String k : uniqueKeys(oldDoc, ix)) {
                    if (!keep.contains(k)) {
                        Shard home = shards().size() == 1 ? shards().get(0) : shardForKey("uk:" + coll + ":" + ix.name + ":" + k);
                        stale.add(new Claim(home, coll, ix.name, k));
                    }
                }
            }
            releaseClaims(db, stale);
        }
        return true;
    }

    /** Deletes the document if it still equals {@code doc}; returns whether a row was removed. */
    boolean delete(String db, String coll, BsonDocument doc, CollMeta meta) throws SQLException {
        String id = idKey(doc.get("_id"));
        boolean removed;
        try (Connection conn = shardForKey(id).open(); PreparedStatement ps = conn.prepareStatement("DELETE FROM " + qualified(db, coll)
                + " WHERE id = ?")) {
            ps.setString(1, id);
            removed = ps.executeUpdate() > 0;
        }
        if (removed && !meta.uniques.isEmpty()) {
            releaseDocKeys(db, coll, meta, doc);
        }
        return removed;
    }

    /** Deletes every document (used by $out/$merge replacement and TTL sweeps do targeted deletes instead). */
    void truncate(String db, String coll) throws SQLException {
        for (Shard s : shards()) {
            try (Connection conn = s.open(); var st = conn.createStatement()) {
                st.execute("DELETE FROM " + qualified(db, coll));
                st.execute("DELETE FROM " + qualified(db, UNIQUE_KEYS) + " WHERE coll = " + literal(coll));
            }
        }
    }

    /** Fresh copy of one document by _id (used by optimistic-retry loops). */
    BsonDocument fetch(String db, String coll, BsonValue idValue) throws SQLException {
        String id = idKey(idValue);
        try (Connection conn = shardForKey(id).open(); PreparedStatement ps = conn.prepareStatement(
                "SELECT bson, CASE WHEN bson IS NULL THEN doc::text END FROM " + qualified(db, coll) + " WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                byte[] b = rs.getBytes(1);
                return b != null ? MongoBson.decode(b) : BsonDocument.parse(rs.getString(2));
            }
        } catch (SQLException e) {
            if ("42P01".equals(e.getSQLState())) {
                return null;
            }
            throw e;
        }
    }

    /** Claims unique keys for every existing document when a unique index is built; returns the failing document or null. */
    void buildUniqueIndex(String db, String coll, UniqueIndex ix, CollMeta metaWithIndex) throws SQLException {
        List<Claim> done = new ArrayList<>();
        try (Stream<BsonDocument> docs = scan(db, coll, null)) {
            Iterator<BsonDocument> it = docs.iterator();
            while (it.hasNext()) {
                BsonDocument d = it.next();
                String id = idKey(d.get("_id"));
                for (String k : uniqueKeys(d, ix)) {
                    Shard home = shards().size() == 1 ? shards().get(0) : shardForKey("uk:" + coll + ":" + ix.name + ":" + k);
                    try (Connection conn = home.open(); PreparedStatement ps = conn.prepareStatement("INSERT INTO " + qualified(db, UNIQUE_KEYS)
                            + " (coll, idx, k, id) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
                        ps.setString(1, coll);
                        ps.setString(2, ix.name);
                        ps.setString(3, k);
                        ps.setString(4, id);
                        if (ps.executeUpdate() == 0) {
                            releaseClaims(db, done);
                            throw duplicate(db, coll, ix.name, ix.key, d);
                        }
                        done.add(new Claim(home, coll, ix.name, k));
                    }
                }
            }
        }
    }

    void dropUniqueKeys(String db, String coll, String idxName) throws SQLException {
        for (Shard s : shards()) {
            try (Connection conn = s.open(); PreparedStatement ps = conn.prepareStatement("DELETE FROM " + qualified(db, UNIQUE_KEYS)
                    + " WHERE coll = ? AND idx = ?")) {
                ps.setString(1, coll);
                ps.setString(2, idxName);
                ps.executeUpdate();
            } catch (SQLException e) {
                if (!"42P01".equals(e.getSQLState())) {
                    throw e;
                }
            }
        }
    }

    // ------------------------------------------------------------------ stats

    long[] storageStats(String db, String coll) throws SQLException {
        long count = 0;
        long size = 0;
        for (Shard s : shards()) {
            try (Connection conn = s.open(); PreparedStatement ps = conn.prepareStatement("SELECT count(*), coalesce(sum(length(bson)), 0)"
                    + " FROM " + qualified(db, coll)); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    count += rs.getLong(1);
                    size += rs.getLong(2);
                }
            } catch (SQLException e) {
                if (!"42P01".equals(e.getSQLState())) {
                    throw e;
                }
            }
        }
        return new long[] {count, size};
    }

    long countAll(String db, String coll) throws SQLException {
        return storageStats(db, coll)[0];
    }

    /** Removes documents whose TTL index date field is older than the index's expireAfterSeconds. */
    int sweepExpired(String db, String coll, CollMeta meta) throws SQLException {
        int removed = 0;
        for (BsonDocument ix : meta.indexes) {
            if (!ix.containsKey("expireAfterSeconds") || ix.getDocument("key").size() != 1) {
                continue;
            }
            String field = ix.getDocument("key").getFirstKey();
            long secs = MongoNum.truncLong(ix.get("expireAfterSeconds"));
            long cutoff = System.currentTimeMillis() - secs * 1000;
            List<BsonDocument> victims = new ArrayList<>();
            try (Stream<BsonDocument> docs = scan(db, coll, null)) {
                Iterator<BsonDocument> it = docs.iterator();
                while (it.hasNext()) {
                    BsonDocument d = it.next();
                    boolean expired = false;
                    for (BsonValue v : MongoMatcher.resolve(d, field)) {
                        List<BsonValue> leaves = v.isArray() ? v.asArray().getValues() : List.of(v);
                        for (BsonValue l : leaves) {
                            if (l != MongoMatcher.MISSING && l.isDateTime() && l.asDateTime().getValue() <= cutoff) {
                                expired = true;
                            }
                        }
                    }
                    if (expired) {
                        victims.add(d);
                    }
                }
            }
            for (BsonDocument d : victims) {
                if (delete(db, coll, d, meta)) {
                    removed++;
                }
            }
        }
        return removed;
    }

    static BsonValue newId() {
        return new BsonObjectId(new ObjectId());
    }
}
