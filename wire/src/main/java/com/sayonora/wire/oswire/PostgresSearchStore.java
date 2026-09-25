package com.sayonora.wire.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ShardingStrategy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index catalog + document storage for oswire, backed by plain Postgres (no OpenSearch, no extension).
 *
 * <p><b>Storage.</b> One table per index, {@code warp_search_<index>} (names that are not plain lower-case
 * identifiers are sanitised and suffixed with a hash), on EVERY host of the backend set that has the
 * {@code opensearch} store enabled:
 * <pre>
 *   doc_id TEXT PRIMARY KEY, source JSONB NOT NULL, embedding JSONB (legacy, unused),
 *   updated_at TIMESTAMPTZ, seq_no BIGINT (per-index sequence, like a shard's seq_no), version BIGINT
 * </pre>
 * Index metadata (settings, mappings, aliases, uuid, creation date) lives in {@code warp_os_catalog} on the first
 * host; index/composable/component templates in {@code warp_os_templates}. A document lives on exactly one host,
 * chosen by hashing its {@code _id} (point operations touch one host; searches read every host).
 *
 * <p><b>Search</b> is executed by {@link SearchEngine} in the JVM over the documents of every host ({@link #scan}):
 * that gives Lucene-faithful analysis, BM25 scoring (per host, like per-shard scoring) and OpenSearch aggregation
 * semantics that Postgres text search cannot reproduce. The price is that a search reads the whole index (or the
 * subset an optional SQL pre-filter narrows it to) -- see docs/WARP_GUIDE.md.
 *
 * <p>Optimistic concurrency ({@code if_seq_no}/{@code if_primary_term}, {@code version}/{@code version_type}) and
 * {@code op_type=create} are single conditional SQL statements, so they are atomic across concurrent writers.
 */
public final class PostgresSearchStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresSearchStore.class);
    private static final String TABLE_PREFIX = "warp_search_";
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]*");
    private static final long META_TTL_NANOS = 2_000_000_000L;

    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> ensured = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IndexMeta> metaCache = new ConcurrentHashMap<>();
    private volatile List<IndexMeta> allCache;
    private volatile long allCacheAt;

    public PostgresSearchStore(BackendRegistry backendRegistry) {
        this.backendRegistry = backendRegistry;
    }

    // ------------------------------------------------------------------ types

    static final class IndexMeta {
        final String name;
        final String uuid;
        final String table;
        final long created;
        final JsonObject settings;
        final JsonObject aliases;
        final Mappings mappings;
        final boolean closed;
        final JsonObject refreshState;
        private final boolean gated;
        final long loadedAt = System.nanoTime();

        IndexMeta(String name, String uuid, String table, long created, JsonObject settings, JsonObject mappings,
                JsonObject aliases, boolean closed) {
            this(name, uuid, table, created, settings, mappings, aliases, closed, new JsonObject());
        }

        IndexMeta(String name, String uuid, String table, long created, JsonObject settings, JsonObject mappings,
                JsonObject aliases, boolean closed, JsonObject refreshState) {
            this.refreshState = refreshState;
            this.gated = computeGated(settings);
            this.name = name;
            this.uuid = uuid;
            this.table = table;
            this.created = created;
            this.settings = settings;
            this.aliases = aliases;
            this.mappings = new Mappings(mappings);
            this.closed = closed;
        }

        IndexMeta withMappings(JsonObject m) {
            return new IndexMeta(name, uuid, table, created, settings, m, aliases, closed, refreshState);
        }

        IndexMeta withRefreshState(JsonObject st) {
            return new IndexMeta(name, uuid, table, created, settings, mappings.raw, aliases, closed, st);
        }

        /** Searches only see documents up to the last explicit refresh when index.refresh_interval is -1. */
        boolean gated() {
            return gated;
        }

        private static boolean computeGated(JsonObject settings) {
            JsonElement idx = settings.get("index");
            if (idx == null || !idx.isJsonObject()) {
                return false;
            }
            JsonElement ri = idx.getAsJsonObject().get("refresh_interval");
            return ri != null && ri.isJsonPrimitive() && "-1".equals(ri.getAsString());
        }

        boolean hasAlias(String a) {
            return aliases.has(a);
        }
    }

    static final class Doc {
        final String index;
        final String id;
        final JsonObject source;
        final long seqNo;
        final long version;
        final int shard;

        Doc(String index, String id, JsonObject source, long seqNo, long version, int shard) {
            this.index = index;
            this.id = id;
            this.source = source;
            this.seqNo = seqNo;
            this.version = version;
            this.shard = shard;
        }
    }

    record WriteResult(long seqNo, long version, boolean created, boolean noop) {
    }

    /** Conditions of a write (all optional). */
    static final class WriteOpts {
        boolean create;
        Long ifSeqNo;
        Long ifPrimaryTerm;
        Long version;
        String versionType = "internal";
    }

    // ------------------------------------------------------------------ targets / sharding

    private BackendTarget defaultTarget() {
        List<String> group = shardGroup();
        BackendTarget target = backendRegistry.resolveForRouting(
                group.isEmpty() ? BackendRegistry.DEFAULT_BACKEND_NAME : group.get(0));
        if (target == null) {
            throw new IllegalStateException("oswire: no default backend configured");
        }
        return target;
    }

    private List<String> shardGroup() {
        return backendRegistry == null ? List.of()
                : backendRegistry.storeShardGroup(com.sayonora.wire.core.StoreType.OPENSEARCH);
    }

    List<BackendTarget> allShardTargets() {
        List<String> group = shardGroup();
        if (group.isEmpty()) {
            return List.of(defaultTarget());
        }
        List<BackendTarget> targets = new ArrayList<>();
        for (String name : group) {
            BackendTarget target = backendRegistry.resolveForRouting(name);
            if (target == null) {
                throw new IllegalStateException("oswire: shard group references unknown backend \"" + name + "\"");
            }
            targets.add(target);
        }
        return targets;
    }

    int shardCount() {
        return Math.max(1, shardGroup().size());
    }

    private int shardOf(String docId) {
        List<String> group = shardGroup();
        if (group.size() <= 1) {
            return 0;
        }
        return group.indexOf(ShardingStrategy.hash(group).resolve(docId));
    }

    private BackendTarget targetForDoc(String docId) {
        List<String> group = shardGroup();
        if (group.isEmpty()) {
            return defaultTarget();
        }
        String shardName = ShardingStrategy.hash(group).resolve(docId);
        BackendTarget target = backendRegistry.resolveForRouting(shardName);
        if (target == null) {
            throw new IllegalStateException("oswire: shard group references unknown backend \"" + shardName + "\"");
        }
        return target;
    }

    // ------------------------------------------------------------------ names

    static String pgTableName(String index) {
        String lower = index.toLowerCase(Locale.ROOT);
        if (IDENTIFIER.matcher(lower).matches() && lower.length() <= 50) {
            return TABLE_PREFIX + lower;
        }
        String sane = lower.replaceAll("[^a-z0-9_]", "_");
        if (sane.length() > 40) {
            sane = sane.substring(0, 40);
        }
        return TABLE_PREFIX + sane + "_" + hash8(index);
    }

    private static String hash8(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d, 0, 4);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static void validateIndexName(String name) {
        String reason = null;
        if (name.equals(".") || name.equals("..")) {
            reason = "must not be '.' or '..'";
        } else if (name.startsWith("_") || name.startsWith("-") || name.startsWith("+")) {
            reason = "must not start with '_', '-', or '+'";
        } else if (!name.equals(name.toLowerCase(Locale.ROOT))) {
            reason = "must be lowercase";
        } else if (name.chars().anyMatch(c -> " \",*\\<|>/?#:".indexOf(c) >= 0)) {
            reason = "must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]";
        } else if (name.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new OpenSearchException("invalid_index_name_exception", "Invalid index name [" + name
                    + "], index name is too long, (" + name.getBytes(StandardCharsets.UTF_8).length + " > 255)")
                    .with("index", name).with("index_uuid", "_na_");
        }
        if (reason != null) {
            throw new OpenSearchException("invalid_index_name_exception", "Invalid index name [" + name + "], " + reason)
                    .with("index", name).with("index_uuid", "_na_");
        }
    }

    // ------------------------------------------------------------------ catalog

    private void ensureCatalog() throws SQLException {
        BackendTarget t = defaultTarget();
        String key = "catalog@" + t.jdbcUrl();
        if (ensured.putIfAbsent(key, Boolean.TRUE) != null) {
            return;
        }
        try (Connection c = t.open(); var st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS warp_os_catalog (name TEXT PRIMARY KEY, uuid TEXT NOT NULL, "
                    + "table_name TEXT NOT NULL, settings JSONB NOT NULL, mappings JSONB NOT NULL, "
                    + "aliases JSONB NOT NULL DEFAULT '{}', created BIGINT NOT NULL, closed BOOLEAN NOT NULL DEFAULT FALSE)");
            st.execute("ALTER TABLE warp_os_catalog ADD COLUMN IF NOT EXISTS refresh_state JSONB NOT NULL DEFAULT '{}'");
            st.execute("CREATE TABLE IF NOT EXISTS warp_os_templates (kind TEXT NOT NULL, name TEXT NOT NULL, "
                    + "body JSONB NOT NULL, PRIMARY KEY (kind, name))");
        } catch (SQLException e) {
            ensured.remove(key);
            throw e;
        }
    }

    private IndexMeta readMeta(ResultSet rs) throws SQLException {
        return new IndexMeta(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(7),
                JsonParser.parseString(rs.getString(4)).getAsJsonObject(),
                JsonParser.parseString(rs.getString(5)).getAsJsonObject(),
                JsonParser.parseString(rs.getString(6)).getAsJsonObject(), rs.getBoolean(8),
                JsonParser.parseString(rs.getString(9)).getAsJsonObject());
    }

    private static final String META_COLS = "name, uuid, table_name, settings::text, mappings::text, aliases::text, created, closed, refresh_state::text";

    /** Every index in the catalog (short TTL cache). */
    List<IndexMeta> allMetas() throws SQLException {
        List<IndexMeta> cached = allCache;
        if (cached != null && System.nanoTime() - allCacheAt < META_TTL_NANOS) {
            return cached;
        }
        ensureCatalog();
        List<IndexMeta> out = new ArrayList<>();
        try (Connection c = defaultTarget().open(); var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT " + META_COLS + " FROM warp_os_catalog ORDER BY name")) {
            while (rs.next()) {
                IndexMeta m = readMeta(rs);
                out.add(m);
                metaCache.put(m.name, m);
            }
        }
        allCache = out;
        allCacheAt = System.nanoTime();
        return out;
    }

    private void invalidate() {
        allCache = null;
    }

    /** Index by exact name (no alias resolution); null when missing. Legacy tables with no catalog row are adopted. */
    IndexMeta meta(String name) throws SQLException {
        IndexMeta cached = metaCache.get(name);
        if (cached != null && System.nanoTime() - cached.loadedAt < META_TTL_NANOS) {
            return cached;
        }
        ensureCatalog();
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("SELECT " + META_COLS + " FROM warp_os_catalog WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    IndexMeta m = readMeta(rs);
                    metaCache.put(name, m);
                    return m;
                }
            }
        }
        metaCache.remove(name);
        return adoptLegacy(name);
    }

    /** A table created by an older oswire (no catalog row): register it and infer its mapping from the documents. */
    private IndexMeta adoptLegacy(String name) throws SQLException {
        if (!IDENTIFIER.matcher(name).matches()) {
            return null;
        }
        String table = pgTableName(name);
        boolean exists;
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("SELECT 1 FROM information_schema.tables WHERE table_name = ?")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                exists = rs.next();
            }
        }
        if (!exists) {
            return null;
        }
        IndexMeta created = insertCatalog(name, new JsonObject(), new JsonObject(), new JsonObject());
        upgradeTables(created);
        // rows written by an older oswire have no sequence numbers: number them in insertion order and continue after them
        for (BackendTarget target : allShardTargets()) {
            try (Connection c = target.open(); var st = c.createStatement()) {
                st.execute("UPDATE " + created.table + " t SET seq_no = r.rn - 1 FROM (SELECT doc_id, row_number() OVER "
                        + "(ORDER BY updated_at, doc_id) AS rn FROM " + created.table + ") r WHERE t.doc_id = r.doc_id");
                st.execute("SELECT setval('" + created.table + "_seq', greatest((SELECT count(*) FROM " + created.table
                        + ") - 1, 0), (SELECT count(*) > 0 FROM " + created.table + "))");
            } catch (SQLException e) {
                log.warn("oswire: could not renumber adopted table {}: {}", created.table, e.getMessage());
            }
        }
        Mappings m = new Mappings(new JsonObject());
        for (Doc d : scan(created, null)) {
            m = m.applyDocument(d.source, d.id, created.settings);
        }
        if (m.raw.size() > 0) {
            created = persistMappings(created, m.raw);
        }
        return created;
    }

    private JsonObject defaultSettings(String name, String uuid, long created, JsonObject requested) {
        JsonObject index = new JsonObject();
        JsonObject req = requested.has("index") && requested.get("index").isJsonObject() ? requested.getAsJsonObject("index") : new JsonObject();
        for (var e : req.entrySet()) {
            index.add(e.getKey(), e.getValue());
        }
        setIfAbsent(index, "number_of_shards", "1");
        setIfAbsent(index, "number_of_replicas", "1");
        index.addProperty("provided_name", name);
        index.addProperty("creation_date", Long.toString(created));
        index.addProperty("uuid", uuid);
        JsonObject version = new JsonObject();
        version.addProperty("created", "136408427");
        index.add("version", version);
        JsonObject out = new JsonObject();
        out.add("index", index);
        return out;
    }

    private static void setIfAbsent(JsonObject o, String k, String v) {
        if (!o.has(k)) {
            o.addProperty(k, v);
        }
    }

    /** Normalises create-index / put-settings bodies to {"index": {k: "string"}} with nested objects kept. */
    static JsonObject normalizeSettings(JsonObject body) {
        JsonObject index = new JsonObject();
        if (body == null) {
            JsonObject o = new JsonObject();
            o.add("index", index);
            return o;
        }
        for (var e : body.entrySet()) {
            String k = e.getKey();
            JsonElement v = e.getValue();
            if (k.equals("index") && v.isJsonObject()) {
                for (var ie : v.getAsJsonObject().entrySet()) {
                    putNested(index, ie.getKey(), ie.getValue());
                }
            } else if (k.startsWith("index.")) {
                putNested(index, k.substring(6), v);
            } else {
                putNested(index, k, v);
            }
        }
        JsonObject o = new JsonObject();
        o.add("index", index);
        return o;
    }

    private static void putNested(JsonObject root, String dottedKey, JsonElement v) {
        String[] parts = dottedKey.split("\\.");
        JsonObject cur = root;
        for (int i = 0; i < parts.length - 1; i++) {
            if (!cur.has(parts[i]) || !cur.get(parts[i]).isJsonObject()) {
                cur.add(parts[i], new JsonObject());
            }
            cur = cur.getAsJsonObject(parts[i]);
        }
        String last = parts[parts.length - 1];
        if (v.isJsonObject()) {
            JsonObject target = cur.has(last) && cur.get(last).isJsonObject() ? cur.getAsJsonObject(last) : new JsonObject();
            for (var e : v.getAsJsonObject().entrySet()) {
                putNested(target, e.getKey(), e.getValue());
            }
            cur.add(last, target);
        } else if (v.isJsonPrimitive()) {
            cur.addProperty(last, v.getAsString());
        } else if (v.isJsonArray()) {
            com.google.gson.JsonArray arr = new com.google.gson.JsonArray();
            for (JsonElement el : v.getAsJsonArray()) {
                arr.add(el.isJsonPrimitive() ? new com.google.gson.JsonPrimitive(el.getAsString()) : el);
            }
            cur.add(last, arr);
        } else {
            cur.add(last, v);
        }
    }

    private IndexMeta insertCatalog(String name, JsonObject settings, JsonObject mappings, JsonObject aliases) throws SQLException {
        ensureCatalog();
        String uuid = newUuid();
        long created = System.currentTimeMillis();
        JsonObject full = defaultSettings(name, uuid, created, settings);
        String table = pgTableName(name);
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("INSERT INTO warp_os_catalog (name, uuid, table_name, settings, mappings, aliases, created) "
                        + "VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?)")) {
            ps.setString(1, name);
            ps.setString(2, uuid);
            ps.setString(3, table);
            ps.setString(4, full.toString());
            ps.setString(5, mappings.toString());
            ps.setString(6, aliases.toString());
            ps.setLong(7, created);
            ps.executeUpdate();
        } catch (SQLException e) {
            if ("23505".equals(e.getSQLState())) {
                throw new OpenSearchException("resource_already_exists_exception", "index [" + name + "/" + uuid + "] already exists")
                        .with("index", name).with("index_uuid", uuid);
            }
            throw e;
        }
        invalidate();
        IndexMeta m = new IndexMeta(name, uuid, table, created, full, mappings, aliases, false);
        metaCache.put(name, m);
        return m;
    }

    private static String newUuid() {
        UUID u = UUID.randomUUID();
        java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(16);
        b.putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array()).substring(0, 22);
    }

    /** Creates the index (catalog row + a table on every host). {@code resource_already_exists_exception} if present. */
    IndexMeta createIndex(String name, JsonObject settings, JsonObject mappings, JsonObject aliases) throws SQLException {
        validateIndexName(name);
        for (IndexMeta m : allMetas()) {
            if (m.hasAlias(name)) {
                throw new OpenSearchException("invalid_index_name_exception", "Invalid index name [" + name
                        + "], already exists as alias").with("index", name).with("index_uuid", "_na_");
            }
        }
        IndexMeta m = insertCatalog(name, settings, mappings, aliases);
        try {
            upgradeTables(m);
        } catch (SQLException e) {
            deleteCatalog(name);
            throw e;
        }
        return m;
    }

    IndexMeta ensureIndex(String name) throws SQLException {
        IndexMeta m = meta(name);
        if (m != null) {
            return m;
        }
        Templates.Applied t = Templates.forNewIndex(this, name);
        try {
            return createIndex(name, t.settings, t.mappings, t.aliases);
        } catch (OpenSearchException e) {
            if (e.errorType.equals("resource_already_exists_exception")) {
                IndexMeta again = meta(name);
                if (again != null) {
                    return again;
                }
            }
            throw e;
        }
    }

    private void deleteCatalog(String name) throws SQLException {
        try (Connection c = defaultTarget().open(); var ps = c.prepareStatement("DELETE FROM warp_os_catalog WHERE name = ?")) {
            ps.setString(1, name);
            ps.executeUpdate();
        }
        metaCache.remove(name);
        invalidate();
    }

    void deleteIndex(IndexMeta m) throws SQLException {
        deleteCatalog(m.name);
        for (BackendTarget t : allShardTargets()) {
            try (Connection c = t.open(); var st = c.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + m.table);
                st.execute("DROP SEQUENCE IF EXISTS " + m.table + "_seq");
            }
            ensured.remove(m.table + "@" + t.jdbcUrl());
        }
    }

    private void upgradeTables(IndexMeta m) throws SQLException {
        for (BackendTarget target : allShardTargets()) {
            String key = m.table + "@" + target.jdbcUrl();
            if (ensured.putIfAbsent(key, Boolean.TRUE) != null) {
                continue;
            }
            try (Connection c = target.open(); var st = c.createStatement()) {
                st.execute("CREATE SEQUENCE IF NOT EXISTS " + m.table + "_seq MINVALUE 0 START 0");
                st.execute("CREATE TABLE IF NOT EXISTS " + m.table + " (doc_id TEXT PRIMARY KEY, source JSONB NOT NULL, "
                        + "embedding JSONB, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                        + "seq_no BIGINT NOT NULL DEFAULT 0, version BIGINT NOT NULL DEFAULT 1)");
                st.execute("ALTER TABLE " + m.table + " ADD COLUMN IF NOT EXISTS seq_no BIGINT NOT NULL DEFAULT 0");
                st.execute("ALTER TABLE " + m.table + " ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1");
                st.execute("ALTER TABLE " + m.table + " ADD COLUMN IF NOT EXISTS ins_seq BIGINT");
            } catch (SQLException e) {
                ensured.remove(key);
                throw e;
            }
        }
    }

    /** Ensures the tables exist on every host (a host added later, or a table dropped underneath). */
    private void ensureTables(IndexMeta m) throws SQLException {
        upgradeTables(m);
    }

    IndexMeta persistMappings(IndexMeta m, JsonObject newRaw) throws SQLException {
        try (Connection c = defaultTarget().open()) {
            c.setAutoCommit(false);
            try (var sel = c.prepareStatement("SELECT mappings::text FROM warp_os_catalog WHERE name = ? FOR UPDATE")) {
                sel.setString(1, m.name);
                JsonObject stored = null;
                try (ResultSet rs = sel.executeQuery()) {
                    if (rs.next()) {
                        stored = JsonParser.parseString(rs.getString(1)).getAsJsonObject();
                    }
                }
                if (stored == null) {
                    c.rollback();
                    throw OpenSearchException.indexNotFound(m.name);
                }
                JsonObject merged = stored.deepCopy();
                // fields another writer added concurrently are kept; ours win for fields present in both
                Mappings.merge(merged, newRaw, "");
                try (var up = c.prepareStatement("UPDATE warp_os_catalog SET mappings = ?::jsonb WHERE name = ?")) {
                    up.setString(1, merged.toString());
                    up.setString(2, m.name);
                    up.executeUpdate();
                }
                c.commit();
                IndexMeta nm = m.withMappings(merged);
                metaCache.put(m.name, nm);
                invalidate();
                return nm;
            } catch (SQLException | RuntimeException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // connection is going back to the pool anyway
                }
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        }
    }

    void updateCatalogColumn(IndexMeta m, String column, JsonObject value) throws SQLException {
        if (!column.equals("settings") && !column.equals("aliases") && !column.equals("mappings")) {
            throw new IllegalArgumentException(column);
        }
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("UPDATE warp_os_catalog SET " + column + " = ?::jsonb WHERE name = ?")) {
            ps.setString(1, value.toString());
            ps.setString(2, m.name);
            ps.executeUpdate();
        }
        metaCache.remove(m.name);
        invalidate();
    }

    /** Makes everything written so far visible to search (only meaningful when {@code index.refresh_interval} is -1). */
    IndexMeta refresh(IndexMeta m) throws SQLException {
        if (!m.gated()) {
            return m;
        }
        JsonObject st = new JsonObject();
        List<BackendTarget> targets = allShardTargets();
        for (int s = 0; s < targets.size(); s++) {
            try (Connection c = targets.get(s).open(); var stmt = c.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT CASE WHEN is_called THEN last_value ELSE -1 END FROM " + m.table + "_seq")) {
                rs.next();
                st.addProperty(String.valueOf(s), rs.getLong(1));
            } catch (SQLException e) {
                // no sequence yet: nothing written on this host
            }
        }
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("UPDATE warp_os_catalog SET refresh_state = ?::jsonb WHERE name = ?")) {
            ps.setString(1, st.toString());
            ps.setString(2, m.name);
            ps.executeUpdate();
        }
        IndexMeta nm = m.withRefreshState(st);
        metaCache.put(m.name, nm);
        invalidate();
        return nm;
    }

    void setClosed(IndexMeta m, boolean closed) throws SQLException {
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("UPDATE warp_os_catalog SET closed = ? WHERE name = ?")) {
            ps.setBoolean(1, closed);
            ps.setString(2, m.name);
            ps.executeUpdate();
        }
        metaCache.remove(m.name);
        invalidate();
    }

    // ------------------------------------------------------------------ templates

    Map<String, JsonObject> templates(String kind) throws SQLException {
        ensureCatalog();
        Map<String, JsonObject> out = new LinkedHashMap<>();
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("SELECT name, body::text FROM warp_os_templates WHERE kind = ? ORDER BY name")) {
            ps.setString(1, kind);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), JsonParser.parseString(rs.getString(2)).getAsJsonObject());
                }
            }
        }
        return out;
    }

    void putTemplate(String kind, String name, JsonObject body) throws SQLException {
        ensureCatalog();
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("INSERT INTO warp_os_templates (kind, name, body) VALUES (?, ?, ?::jsonb) "
                        + "ON CONFLICT (kind, name) DO UPDATE SET body = EXCLUDED.body")) {
            ps.setString(1, kind);
            ps.setString(2, name);
            ps.setString(3, body.toString());
            ps.executeUpdate();
        }
    }

    boolean deleteTemplate(String kind, String name) throws SQLException {
        ensureCatalog();
        try (Connection c = defaultTarget().open();
                var ps = c.prepareStatement("DELETE FROM warp_os_templates WHERE kind = ? AND name = ?")) {
            ps.setString(1, kind);
            ps.setString(2, name);
            return ps.executeUpdate() > 0;
        }
    }

    // ------------------------------------------------------------------ resolution

    /** A concrete index plus the alias filters (if it was reached through filtered aliases). */
    record Resolved(IndexMeta index, List<JsonObject> aliasFilters) {
    }

    static final class ResolveOpts {
        boolean allowNoIndices = true;
        boolean ignoreUnavailable;
        boolean expandOpen = true;
        boolean expandClosed;
        boolean requireAlias;
    }

    /** Resolves an index expression (comma list, wildcards, {@code _all}, aliases, {@code -exclusion}). */
    List<Resolved> resolve(String expr, ResolveOpts opts) throws SQLException {
        List<IndexMeta> all = allMetas();
        Map<String, Resolved> out = new LinkedHashMap<>();
        boolean wildcardOnly = true;
        boolean any = false;
        String[] parts = expr == null || expr.isBlank() || expr.equals("_all") ? new String[] {"*"} : expr.split(",");
        for (String raw : parts) {
            String p = raw.trim();
            if (p.isEmpty()) {
                continue;
            }
            boolean exclude = p.startsWith("-") && any;
            if (exclude) {
                p = p.substring(1);
            }
            boolean wildcard = p.contains("*") || p.equals("_all");
            if (!wildcard) {
                wildcardOnly = false;
                if (p.startsWith("_") || p.startsWith("+")) {
                    throw new OpenSearchException("invalid_index_name_exception", "Invalid index name [" + p + "], must not start with '_', '-', or '+'")
                            .with("index", p).with("index_uuid", "_na_");
                }
            }
            List<Resolved> found = new ArrayList<>();
            if (wildcard && !opts.expandOpen && !opts.expandClosed) {
                continue; // expand_wildcards=none
            }
            for (IndexMeta m : all) {
                if (p.equals("_all") || (wildcard ? Mappings.wildcardMatch(p, m.name) : m.name.equals(p))) {
                    found.add(new Resolved(m, new ArrayList<>()));
                }
            }
            for (IndexMeta m : all) {
                for (var a : m.aliases.entrySet()) {
                    if (wildcard ? Mappings.wildcardMatch(p, a.getKey()) : a.getKey().equals(p)) {
                        List<JsonObject> f = new ArrayList<>();
                        JsonObject def = a.getValue().isJsonObject() ? a.getValue().getAsJsonObject() : new JsonObject();
                        if (def.has("filter")) {
                            f.add(def.getAsJsonObject("filter"));
                        }
                        boolean dup = false;
                        for (Resolved r : found) {
                            if (r.index().name.equals(m.name)) {
                                dup = true;
                            }
                        }
                        if (!dup) {
                            found.add(new Resolved(m, f));
                        }
                    }
                }
            }
            if (!wildcard && found.isEmpty()) {
                // legacy table without a catalog row?
                IndexMeta adopted = meta(p);
                if (adopted != null) {
                    found.add(new Resolved(adopted, new ArrayList<>()));
                }
            }
            if (found.isEmpty() && !wildcard && !opts.ignoreUnavailable) {
                throw OpenSearchException.indexNotFound(p);
            }
            for (Resolved r : found) {
                if (r.index().closed && !opts.expandClosed) {
                    if (!wildcard && !opts.ignoreUnavailable) {
                        throw new OpenSearchException("index_closed_exception", "closed").with("index", r.index().name).with("index_uuid", r.index().uuid);
                    }
                    continue;
                }
                if (exclude) {
                    out.remove(r.index().name);
                } else {
                    Resolved prev = out.get(r.index().name);
                    if (prev == null) {
                        out.put(r.index().name, r);
                    } else if (r.aliasFilters().isEmpty()) {
                        out.put(r.index().name, r);
                    } else if (!prev.aliasFilters().isEmpty()) {
                        List<JsonObject> merged = new ArrayList<>(prev.aliasFilters());
                        merged.addAll(r.aliasFilters());
                        out.put(r.index().name, new Resolved(r.index(), merged));
                    }
                }
            }
            any = true;
        }
        if (out.isEmpty() && !opts.allowNoIndices) {
            throw OpenSearchException.indexNotFound(expr == null ? "_all" : expr);
        }
        return new ArrayList<>(out.values());
    }

    /** The single concrete index a write addresses: an index name, or an alias with exactly one (or a write) index. */
    IndexMeta resolveWriteTarget(String name, boolean autoCreate) throws SQLException {
        IndexMeta direct = meta(name);
        if (direct != null) {
            if (direct.closed) {
                throw new OpenSearchException("index_closed_exception", "closed").with("index", name).with("index_uuid", direct.uuid);
            }
            return direct;
        }
        List<IndexMeta> viaAlias = new ArrayList<>();
        IndexMeta writeIndex = null;
        for (IndexMeta m : allMetas()) {
            if (m.aliases.has(name)) {
                viaAlias.add(m);
                JsonElement def = m.aliases.get(name);
                if (def.isJsonObject() && def.getAsJsonObject().has("is_write_index")
                        && def.getAsJsonObject().get("is_write_index").getAsBoolean()) {
                    writeIndex = m;
                }
            }
        }
        if (writeIndex != null) {
            return writeIndex;
        }
        if (viaAlias.size() == 1) {
            return viaAlias.get(0);
        }
        if (viaAlias.size() > 1) {
            throw OpenSearchException.illegalArgument("no write index is defined for alias [" + name
                    + "]. The write index may be explicitly disabled using is_write_index=false or the alias points to multiple indices without one being designated as a write index");
        }
        if (!autoCreate) {
            throw OpenSearchException.indexNotFound(name);
        }
        return ensureIndex(name);
    }

    // ------------------------------------------------------------------ documents

    static void validateId(String id) {
        if (id.getBytes(StandardCharsets.UTF_8).length > 512) {
            throw OpenSearchException.validation("id [" + id + "] is too long, must be no longer than 512 bytes but was: "
                    + id.getBytes(StandardCharsets.UTF_8).length);
        }
    }

    static String autoId() {
        byte[] b = new byte[15];
        new java.security.SecureRandom().nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    /** Compatibility entry point (id + optional legacy vector). */
    public void indexDocument(String collection, String docId, JsonObject source, float[] vector) throws SQLException {
        IndexMeta m = resolveWriteTarget(collection, true);
        write(m, docId, source, new WriteOpts());
    }

    /** Indexes (or, with {@code opts.create}, creates) one document; applies dynamic mapping first. */
    WriteResult write(IndexMeta meta, String id, JsonObject source, WriteOpts opts) throws SQLException {
        validateId(id);
        Mappings updated = meta.mappings.applyDocument(source, id, meta.settings);
        if (updated != meta.mappings) {
            meta = persistMappings(meta, updated.raw);
        }
        String t = meta.table;
        String seq = "nextval('" + t + "_seq')";
        BackendTarget target = targetForDoc(id);
        String src = source.toString();
        if (opts.ifPrimaryTerm != null && opts.ifPrimaryTerm != 1L) {
            try (Connection c = target.open()) {
                throw conflict(c, t, id, opts);
            }
        }
        for (int attempt = 0; ; attempt++) {
            try (Connection c = target.open()) {
                if (opts.ifSeqNo != null || (opts.version != null && opts.versionType.equals("internal"))) {
                    String cond = opts.ifSeqNo != null ? "seq_no = ?" : "version = ?";
                    long condVal = opts.ifSeqNo != null ? opts.ifSeqNo : opts.version;
                    try (var ps = c.prepareStatement("UPDATE " + t + " SET source = ?::jsonb, updated_at = now(), seq_no = " + seq
                            + ", version = version + 1 WHERE doc_id = ? AND " + cond + " RETURNING seq_no, version")) {
                        ps.setString(1, src);
                        ps.setString(2, id);
                        ps.setLong(3, condVal);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                return new WriteResult(rs.getLong(1), rs.getLong(2), false, false);
                            }
                        }
                    }
                    throw conflict(c, t, id, opts);
                }
                if (opts.version != null) {
                    boolean gte = opts.versionType.equals("external_gte");
                    try (var ps = c.prepareStatement("" + insertHead(t, seq, meta.gated(), "?") + " ON CONFLICT (doc_id) DO UPDATE SET source = EXCLUDED.source, updated_at = now(), seq_no = EXCLUDED.seq_no, "
                            + "version = EXCLUDED.version WHERE x.version " + (gte ? "<=" : "<") + " EXCLUDED.version RETURNING seq_no, version, (xmax = 0)")) {
                        ps.setString(1, id);
                        ps.setString(2, src);
                        ps.setLong(3, opts.version);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                return new WriteResult(rs.getLong(1), rs.getLong(2), rs.getBoolean(3), false);
                            }
                        }
                    }
                    throw conflict(c, t, id, opts);
                }
                if (opts.create) {
                    try (var ps = c.prepareStatement("" + insertHead(t, seq, meta.gated(), "1") + " ON CONFLICT (doc_id) DO NOTHING RETURNING seq_no, version")) {
                        ps.setString(1, id);
                        ps.setString(2, src);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                return new WriteResult(rs.getLong(1), rs.getLong(2), true, false);
                            }
                        }
                    }
                    throw conflict(c, t, id, opts);
                }
                try (var ps = c.prepareStatement("" + insertHead(t, seq, meta.gated(), "1") + " ON CONFLICT (doc_id) DO UPDATE SET source = EXCLUDED.source, updated_at = now(), "
                        + "seq_no = EXCLUDED.seq_no, version = x.version + 1 RETURNING seq_no, version, (xmax = 0)")) {
                    ps.setString(1, id);
                    ps.setString(2, src);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        return new WriteResult(rs.getLong(1), rs.getLong(2), rs.getBoolean(3), false);
                    }
                }
            } catch (SQLException e) {
                if (attempt == 0 && "42P01".equals(e.getSQLState())) {
                    ensured.remove(t + "@" + target.jdbcUrl());
                    ensureTables(meta);
                    continue;
                }
                throw e;
            }
        }
    }

    /** The INSERT head (binds doc_id, source[, version]). Gated indexes (refresh_interval -1) also record the first sequence
     * number of the row ({@code ins_seq}) so an update does not hide a document that was already visible. */
    private static String insertHead(String t, String seq, boolean gated, String version) {
        if (gated) {
            return "WITH n AS (SELECT " + seq + " AS s) INSERT INTO " + t + " AS x (doc_id, source, updated_at, seq_no, version, ins_seq) "
                    + "SELECT ?, ?::jsonb, now(), n.s, " + version + ", n.s FROM n";
        }
        return "INSERT INTO " + t + " AS x (doc_id, source, updated_at, seq_no, version) VALUES (?, ?::jsonb, now(), " + seq + ", " + version + ")";
    }

    private OpenSearchException conflict(Connection c, String table, String id, WriteOpts opts) throws SQLException {
        long curSeq = -2;
        long curVer = -1;
        try (var ps = c.prepareStatement("SELECT seq_no, version FROM " + table + " WHERE doc_id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    curSeq = rs.getLong(1);
                    curVer = rs.getLong(2);
                }
            }
        }
        String msg;
        if (opts.ifSeqNo != null) {
            msg = "[" + id + "]: version conflict, required seqNo [" + opts.ifSeqNo + "], primary term [" + opts.ifPrimaryTerm
                    + "]. " + (curVer < 0 ? "document does not exist (expected)" : "current document has seqNo [" + curSeq + "] and primary term [1]");
        } else if (opts.create) {
            msg = "[" + id + "]: version conflict, document already exists (current version [" + curVer + "])";
        } else if (opts.version != null && opts.versionType.equals("internal")) {
            msg = "[" + id + "]: version conflict, current version [" + curVer + "] is different than the one provided [" + opts.version + "]";
        } else {
            msg = "[" + id + "]: version conflict, current version [" + curVer + "] is higher or equal to the one provided [" + opts.version + "]";
        }
        return new OpenSearchException("version_conflict_engine_exception", msg);
    }

    Doc get(IndexMeta meta, String id) throws SQLException {
        try (Connection c = targetForDoc(id).open();
                var ps = c.prepareStatement("SELECT source::text, seq_no, version FROM " + meta.table + " WHERE doc_id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new Doc(meta.name, id, JsonParser.parseString(rs.getString(1)).getAsJsonObject(),
                        rs.getLong(2), rs.getLong(3), shardOf(id)) : null;
            }
        }
    }

    /** Compatibility (older callers). */
    public JsonObject getDocument(String collection, String docId) throws SQLException {
        IndexMeta m = meta(collection);
        if (m == null) {
            return null;
        }
        Doc d = get(m, docId);
        return d == null ? null : d.source;
    }

    /** @return version/seq_no of the delete, or {@code null} when the document did not exist. */
    WriteResult delete(IndexMeta meta, String id, WriteOpts opts) throws SQLException {
        String seq = "nextval('" + meta.table + "_seq')";
        if (opts != null && opts.ifPrimaryTerm != null && opts.ifPrimaryTerm != 1L) {
            try (Connection c = targetForDoc(id).open()) {
                throw conflict(c, meta.table, id, opts);
            }
        }
        try (Connection c = targetForDoc(id).open()) {
            String cond = "";
            List<Object> args = new ArrayList<>();
            boolean external = opts != null && opts.version != null && opts.versionType.startsWith("external");
            if (opts != null && opts.ifSeqNo != null) {
                cond = " AND seq_no = ?";
                args.add(opts.ifSeqNo);
            } else if (opts != null && opts.version != null) {
                cond = external ? (opts.versionType.equals("external_gte") ? " AND version <= ?" : " AND version < ?") : " AND version = ?";
                args.add(opts.version);
            }
            try (var ps = c.prepareStatement("DELETE FROM " + meta.table + " WHERE doc_id = ?" + cond + " RETURNING version, " + seq)) {
                ps.setString(1, id);
                for (int i = 0; i < args.size(); i++) {
                    ps.setObject(i + 2, args.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return new WriteResult(rs.getLong(2), external ? opts.version : rs.getLong(1) + 1, false, false);
                    }
                }
            }
            if (!cond.isEmpty()) {
                try (var ps = c.prepareStatement("SELECT 1 FROM " + meta.table + " WHERE doc_id = ?")) {
                    ps.setString(1, id);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            throw conflict(c, meta.table, id, opts);
                        }
                    }
                }
                if (opts.ifSeqNo != null) {
                    throw conflict(c, meta.table, id, opts);
                }
            }
            return null;
        }
    }

    /** Compatibility. */
    public boolean deleteDocument(String collection, String docId) throws SQLException {
        IndexMeta m = meta(collection);
        return m != null && delete(m, docId, null) != null;
    }

    // ------------------------------------------------------------------ scans

    /** Reads every document of the index on every host (optionally narrowed by a SQL predicate over {@code source}/{@code doc_id}). */
    List<Doc> scan(IndexMeta meta, SqlFilter filter) throws SQLException {
        List<Doc> out = new ArrayList<>();
        List<BackendTarget> targets = allShardTargets();
        for (int s = 0; s < targets.size(); s++) {
            try (Connection c = targets.get(s).open()) {
                List<Object> params = new ArrayList<>();
                StringBuilder where = new StringBuilder();
                if (filter != null) {
                    where.append('(').append(filter.sql()).append(')');
                    params.addAll(filter.params());
                }
                if (meta.gated()) {
                    if (where.length() > 0) {
                        where.append(" AND ");
                    }
                    where.append("COALESCE(ins_seq, seq_no) <= ?");
                    params.add(meta.refreshState.has(String.valueOf(s)) ? meta.refreshState.get(String.valueOf(s)).getAsLong() : -1L);
                }
                String sql = "SELECT doc_id, source::text, seq_no, version FROM " + meta.table
                        + (where.length() == 0 ? "" : " WHERE " + where) + " ORDER BY seq_no";
                try (var ps = c.prepareStatement(sql)) {
                    for (int i = 0; i < params.size(); i++) {
                        ps.setObject(i + 1, params.get(i));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(new Doc(meta.name, rs.getString(1), JsonParser.parseString(rs.getString(2)).getAsJsonObject(),
                                    rs.getLong(3), rs.getLong(4), s));
                        }
                    }
                }
            } catch (SQLException e) {
                if ("42P01".equals(e.getSQLState()) && meta(meta.name) != null && s > 0) {
                    ensured.remove(meta.table + "@" + targets.get(s).jdbcUrl());
                    ensureTables(meta);
                    continue;
                }
                throw e;
            }
        }
        return out;
    }

    /** A SQL predicate that is a SUPERSET of the documents a query can match (never drops a real match). */
    record SqlFilter(String sql, List<Object> params) {
    }

    long count(IndexMeta meta) throws SQLException {
        long n = 0;
        for (BackendTarget t : allShardTargets()) {
            try (Connection c = t.open(); var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT count(*) FROM " + meta.table)) {
                rs.next();
                n += rs.getLong(1);
            } catch (SQLException e) {
                if (!"42P01".equals(e.getSQLState())) {
                    throw e;
                }
            }
        }
        return n;
    }

    long storeSizeBytes(IndexMeta meta) throws SQLException {
        long n = 0;
        for (BackendTarget t : allShardTargets()) {
            try (Connection c = t.open(); var st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT pg_total_relation_size('" + meta.table + "')")) {
                rs.next();
                n += rs.getLong(1);
            } catch (SQLException e) {
                // table not on this host yet
            }
        }
        return n;
    }

    /** Test/diagnostic hook: forget every cached decision (used after the tables are dropped underneath). */
    void clearCaches() {
        ensured.clear();
        metaCache.clear();
        invalidate();
    }
}
