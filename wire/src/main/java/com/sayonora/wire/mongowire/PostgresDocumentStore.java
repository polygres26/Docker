package com.sayonora.wire.mongowire;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ShardingStrategy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Document storage for mongowire, backed by Postgres. Two modes, same shape as dynamowire's
 * {@code PgItemStore}:
 *
 * <p><b>Single-backend (legacy):</b> {@link #PostgresDocumentStore(ConnectionSupplier)}
 * -- one fixed connection supplier, exactly as before this class supported sharding at all.
 *
 * <p><b>Sharded:</b> {@link #PostgresDocumentStore(BackendRegistry)} routes by
 * hashing the document's {@code _id} across {@code backendRegistry.shardGroup()} -- read fresh on
 * every call (not captured once), so a config change takes effect immediately, no restart,
 * matching {@code PgItemStore}'s dynamowire sharding and {@code RouterStage}'s SQL sharding.
 *
 * <p>{@code _id} was the obvious choice where dynamowire had an explicit, schema-declared
 * partition key to reuse: every Mongo document already has one (generated on insert if the
 * caller didn't supply one), it's the one field every collection is guaranteed to have, and
 * {@code MongoQueryTranslator.exactIdEquality} already existed (for GetItem-style cache lookups)
 * to detect exactly the query shape -- {@code find/updateMany/deleteMany} filtered on nothing but
 * {@code {_id: <value>}} -- that resolves to one shard unambiguously. Any other filter (an
 * arbitrary field, a range, no filter at all) can't be reduced to "which one shard", so it fans
 * out across every shard backend and concatenates -- same scatter-gather tradeoff as dynamowire's
 * {@code Scan}, and the same one real sharded MongoDB has scanning across shards that don't share
 * the query's shard key.
 *
 * <p>Unlike dynamowire, there's no separate catalog table here to keep stable -- {@code
 * ensureTable} (CREATE SCHEMA/TABLE) just runs on every shard backend up front, since a
 * collection needs to exist wherever a document might land, and there's no metadata row whose
 * "home" could move under a shard-group change.
 */
final class PostgresDocumentStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresDocumentStore.class);
    private static final Pattern SAFE_NAME = Pattern.compile("^[A-Za-z0-9_]+$");

    private final ConnectionSupplier legacyConnections;
    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> ensuredTables = new ConcurrentHashMap<>();
    private volatile List<String> lastLoggedShardGroup = null;

    // Reverse index (lowercased "db.collection" -> present), alongside ensuredTables' own
    // per-backend DDL dedup -- lets CacheStage recognize "is this table a mongowire collection"
    // from the bare table name a SQL FROM/WHERE clause actually contains, the same role
    // PgItemStore.physicalTableIndex plays for dynamowire. Lowercased because Postgres folds an
    // unquoted identifier to lowercase; a genuinely mixed-case collection name (quoted in
    // Postgres to preserve case) won't match here and simply won't get the SQL-side cache
    // fast path -- a missed optimization, not a correctness bug, same tradeoff CacheStage's own
    // normalizeTable() already makes everywhere else.
    //
    // static, unlike ensuredTables above -- PostgresDocumentStore itself is constructed FRESH per
    // client session (see MongoWireSessionHandler), so an instance-level set would never be
    // visible to CacheStage's shared, cross-session, cross-protocol lookup.
    private static final java.util.Set<String> knownPhysicalTables = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Nullable in spirit only -- returns false, never null, since "not sharing the row cache"
     * and "not a known table" collapse to the same answer for CacheStage's purposes. */
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

    private List<String> currentShardGroup() {
        return backendRegistry == null ? List.of() : backendRegistry.shardGroup();
    }

    private void logShardGroupIfChanged() {
        List<String> group = currentShardGroup();
        if (!group.equals(lastLoggedShardGroup)) {
            lastLoggedShardGroup = group;
            if (!group.isEmpty()) {
                log.info("mongowire: sharding document storage across {} backend(s) by _id: {}", group.size(), group);
            } else if (backendRegistry != null) {
                log.info("mongowire: no shard group configured -- document storage on the single default backend");
            }
        }
    }

    boolean isSharded() {
        return !currentShardGroup().isEmpty();
    }

    /** Metrics-label-only: which backend a document with this _id would route to, no connection opened. */
    String resolveBackendFor(String idJson) {
        if (legacyConnections != null || idJson == null) {
            return "default";
        }
        List<String> group = currentShardGroup();
        return group.isEmpty() ? BackendRegistry.DEFAULT_BACKEND_NAME : ShardingStrategy.hash(group).resolve(idJson);
    }

    /** All currently-live backend connections a document might need to be found on. */
    private List<Connection> allBackendConnections() throws SQLException {
        if (legacyConnections != null) {
            return List.of(legacyConnections.get());
        }
        List<Connection> connections = new ArrayList<>();
        List<String> group = currentShardGroup();
        List<String> names = group.isEmpty() ? List.of(BackendRegistry.DEFAULT_BACKEND_NAME) : group;
        for (String name : names) {
            // resolveForRouting, not get -- lets a DRAINING/DOWN shard transparently redirect to
            // its configured fallback, same switchover/failover mechanism pgwire/mssqlwire/
            // mywire/orawire already use (see BackendRegistry.resolveForRouting's javadoc).
            BackendTarget target = backendRegistry.resolveForRouting(name);
            if (target != null) {
                connections.add(target.open());
            }
        }
        return connections;
    }

    /** The one backend a document with this _id (as its Mongo Extended JSON string) belongs on. */
    private Connection shardConnectionFor(String idJson) throws SQLException {
        if (legacyConnections != null) {
            return legacyConnections.get();
        }
        logShardGroupIfChanged();
        List<String> group = currentShardGroup();
        String backendName = group.isEmpty() ? BackendRegistry.DEFAULT_BACKEND_NAME : ShardingStrategy.hash(group).resolve(idJson);
        BackendTarget target = backendRegistry.resolveForRouting(backendName);
        if (target == null) {
            throw new IllegalStateException("mongowire: resolved shard backend \"" + backendName + "\" is not configured");
        }
        return target.open();
    }

    private static String quoteIdent(String name) {
        if (!SAFE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("mongowire: database/collection names must match [A-Za-z0-9_]+, got \""
                    + name + "\"");
        }
        return "\"" + name + "\"";
    }

    static String qualifiedTable(String db, String collection) {
        return quoteIdent(db) + "." + quoteIdent(collection);
    }

    /** Ensures the collection's table exists on every backend a document could currently land on.
     * Cached per (collection, physical backend URL) pair, not per collection alone -- a flat
     * per-collection cache would mean a backend that only starts receiving this collection's
     * writes LATER (a switchover's fallback taking over after this collection was already ensured
     * against its primary, or a shard added to an existing group) never gets the table created on
     * it at all, and every write there would fail with "relation does not exist". */
    private void ensureTable(String db, String collection) throws SQLException {
        knownPhysicalTables.add((db + "." + collection).toLowerCase(java.util.Locale.ROOT));
        for (Connection conn : allBackendConnections()) {
            try (conn) {
                String key = db + "." + collection + "@" + conn.getMetaData().getURL();
                if (ensuredTables.putIfAbsent(key, Boolean.TRUE) != null) {
                    continue;
                }
                try (var st = conn.createStatement()) {
                    st.execute("CREATE SCHEMA IF NOT EXISTS " + quoteIdent(db));
                    st.execute("CREATE TABLE IF NOT EXISTS " + qualifiedTable(db, collection)
                            + " (id text PRIMARY KEY, doc jsonb NOT NULL)");
                }
            }
        }
    }

    private static PGobject jsonb(String json) throws SQLException {
        PGobject obj = new PGobject();
        obj.setType("jsonb");
        obj.setValue(json);
        return obj;
    }

    record WriteResult(int count, List<String> ids) {}

    static String idJsonFor(Object idValue) {
        return BsonJson.valueToJson(new BsonObjectIdOrPassthrough(idValue).toBson());
    }

    Document insertOne(String db, String collection, Document document) throws SQLException {
        if (!document.containsKey("_id")) {
            document.put("_id", new ObjectId());
        }
        String idJson = idJsonFor(document.get("_id"));
        String docJson = BsonJson.toJson(document);
        ensureTable(db, collection);
        try (Connection conn = shardConnectionFor(idJson);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + qualifiedTable(db, collection) + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, idJson);
            ps.setObject(2, jsonb(docJson));
            ps.executeUpdate();
        }
        return document;
    }


    /** Real {@code listCollections} support -- a genuine, high-impact gap found auditing this
     * frontend for GA transparency: mongoose's default {@code autoIndex} startup behavior (and
     * plenty of admin tooling) calls this before ever running a query, so its absence broke
     * CONNECTION setup for a typical app, not just a later query. Queries the real Postgres
     * {@code information_schema} directly rather than the in-process, this-instance-only {@code
     * knownPhysicalTables} cache -- a durable, schema-derived answer that also sees collections
     * created by an earlier process or before this one started. */
    List<String> listCollections(String db) throws SQLException {
        List<Connection> targets = allBackendConnections();
        if (targets.isEmpty()) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        try (Connection conn = targets.get(0)) {
            for (int i = 1; i < targets.size(); i++) {
                targets.get(i).close();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name")) {
                ps.setString(1, db);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        names.add(rs.getString(1));
                    }
                }
            }
        }
        return names;
    }

    /** Real {@code count}/{@code countDocuments} support -- an ordinary filtered row count, same
     * {@code WHERE} translation {@link #find} already uses, just {@code count(*)} instead of
     * fetching rows. Scatter-gathers and sums across shards, same honestly-scoped limitation
     * every other multi-shard operation here already has. */
    long count(String db, String collection, MongoQueryTranslator.Where where) throws SQLException {
        ensureTable(db, collection);
        long total = 0;
        String sql = "SELECT count(*) FROM " + qualifiedTable(db, collection) + where.sql();
        for (Connection conn : allBackendConnections()) {
            try (conn; PreparedStatement ps = conn.prepareStatement(sql)) {
                bindParams(ps, where.jsonbParams());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        total += rs.getLong(1);
                    }
                }
            }
        }
        return total;
    }

    /** Real {@code distinct} support -- the values of one top-level field across every matching
     * document, deduplicated. {@code field} is validated the same way {@link
     * MongoAggregationTranslator}'s own field references are (top-level only, no dotted path). */
    List<org.bson.BsonValue> distinct(String db, String collection, String field, MongoQueryTranslator.Where where)
            throws SQLException {
        ensureTable(db, collection);
        if (field.contains(".") || field.isEmpty()) {
            throw new IllegalArgumentException("distinct: only a top-level field name is supported, not \"" + field + "\"");
        }
        java.util.LinkedHashSet<String> distinctJson = new java.util.LinkedHashSet<>();
        String sql = "SELECT DISTINCT doc->" + quoteLiteral(field) + " FROM " + qualifiedTable(db, collection)
                + where.sql();
        for (Connection conn : allBackendConnections()) {
            try (conn; PreparedStatement ps = conn.prepareStatement(sql)) {
                bindParams(ps, where.jsonbParams());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String json = rs.getString(1);
                        if (json != null) {
                            distinctJson.add(json);
                        }
                    }
                }
            }
        }
        List<org.bson.BsonValue> values = new ArrayList<>();
        for (String json : distinctJson) {
            values.add(BsonJson.fromJson("{\"v\":" + json + "}").toBsonDocument().get("v"));
        }
        return values;
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** Real {@code findAndModify} support -- the single wire command real drivers/ODMs use for
     * BOTH {@code findOneAndUpdate} and {@code findOneAndDelete} (distinguished by whether the
     * caller sent an {@code update} or a {@code remove: true}). Only an exact-{@code _id} filter
     * routes to one shard the way {@link #find}/{@link #updateMany} do; anything else finds the
     * first match by scanning shards in order (no cross-shard "first by original insertion order"
     * guarantee beyond that -- same honestly-scoped limitation the rest of this class already
     * has). Returns the matched document as it was BEFORE the update (real MongoDB's own default,
     * {@code new: false}) unless {@code returnNew} is set.
     */
    Document findAndModify(String db, String collection, BsonDocument filter, MongoQueryTranslator.Where where,
            Document updateMerger, boolean remove, boolean returnNew) throws SQLException {
        ensureTable(db, collection);
        String idJson = MongoQueryTranslator.exactIdEquality(filter);
        List<Connection> targets = idJson != null ? List.of(shardConnectionFor(idJson)) : allBackendConnections();
        String selectSql = "SELECT id, doc FROM " + qualifiedTable(db, collection) + where.sql() + " LIMIT 1";
        for (Connection conn : targets) {
            try (conn) {
                String matchedId = null;
                Document before = null;
                try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                    bindParams(ps, where.jsonbParams());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            matchedId = rs.getString(1);
                            before = BsonJson.fromJson(rs.getString(2));
                        }
                    }
                }
                if (matchedId == null) {
                    continue;
                }
                if (remove) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "DELETE FROM " + qualifiedTable(db, collection) + " WHERE id = ?")) {
                        ps.setString(1, matchedId);
                        ps.executeUpdate();
                    }
                    return before;
                }
                Document after = new Document(before);
                UpdateApplier.apply(after, updateMerger);
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE " + qualifiedTable(db, collection) + " SET doc = ? WHERE id = ?")) {
                    ps.setObject(1, jsonb(BsonJson.toJson(after)));
                    ps.setString(2, matchedId);
                    ps.executeUpdate();
                }
                return returnNew ? after : before;
            }
        }
        return null;
    }

    /** Runs a real {@code aggregate} pipeline's already-translated SQL (see {@link
     * MongoAggregationTranslator}) and reads back one document per result row -- same shape as
     * {@link #find}. Note this does NOT re-aggregate across shards the way a true distributed
     * aggregation would: with more than one backend, this runs the SAME grouped query on each and
     * concatenates the results, so a {@code $group}'s sums/counts are only correct when every
     * matching document lives on the SAME shard (true for the common single-backend deployment,
     * and for a sharded one whenever the group key IS the shard key) -- same honestly-scoped
     * limitation {@link #find}'s own multi-shard concatenation already has for an unsorted,
     * unlimited scatter-gather. */
    List<Document> aggregate(String db, String collection, MongoAggregationTranslator.AggregateQuery query)
            throws SQLException {
        ensureTable(db, collection);
        List<Document> results = new ArrayList<>();
        for (Connection conn : allBackendConnections()) {
            try (conn; PreparedStatement ps = conn.prepareStatement(query.sql())) {
                bindParams(ps, query.jsonbParams());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(BsonJson.fromJson(rs.getString(1)));
                    }
                }
            }
        }
        return results;
    }

    List<Document> find(String db, String collection, BsonDocument filter, MongoQueryTranslator.Where where, int limit) throws SQLException {
        ensureTable(db, collection);
        String idJson = MongoQueryTranslator.exactIdEquality(filter);
        List<Connection> targets = idJson != null ? List.of(shardConnectionFor(idJson)) : allBackendConnections();
        List<Document> results = new ArrayList<>();
        String sql = "SELECT doc FROM " + qualifiedTable(db, collection) + where.sql()
                + (limit > 0 ? " LIMIT " + limit : "");
        for (Connection conn : targets) {
            try (conn; PreparedStatement ps = conn.prepareStatement(sql)) {
                bindParams(ps, where.jsonbParams());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(BsonJson.fromJson(rs.getString(1)));
                        if (limit > 0 && results.size() >= limit) {
                            return results;
                        }
                    }
                }
            }
        }
        return results;
    }

    WriteResult updateMany(String db, String collection, BsonDocument filter, MongoQueryTranslator.Where where,
            Document merger, int limit) throws SQLException {
        ensureTable(db, collection);
        String idJson = MongoQueryTranslator.exactIdEquality(filter);
        List<Connection> targets = idJson != null ? List.of(shardConnectionFor(idJson)) : allBackendConnections();
        int totalCount = 0;
        List<String> allIds = new ArrayList<>();
        String selectSql = "SELECT id, doc FROM " + qualifiedTable(db, collection) + where.sql()
                + (limit > 0 ? " LIMIT " + limit : "");
        for (Connection conn : targets) {
            try (conn) {
                List<String> ids = new ArrayList<>();
                List<String> newDocs = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                    bindParams(ps, where.jsonbParams());
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Document existing = BsonJson.fromJson(rs.getString(2));
                            UpdateApplier.apply(existing, merger);
                            ids.add(rs.getString(1));
                            newDocs.add(BsonJson.toJson(existing));
                        }
                    }
                }
                if (!ids.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE " + qualifiedTable(db, collection) + " SET doc = ? WHERE id = ?")) {
                        for (int i = 0; i < ids.size(); i++) {
                            ps.setObject(1, jsonb(newDocs.get(i)));
                            ps.setString(2, ids.get(i));
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                }
                totalCount += ids.size();
                allIds.addAll(ids);
            }
        }
        return new WriteResult(totalCount, allIds);
    }

    WriteResult deleteMany(String db, String collection, BsonDocument filter, MongoQueryTranslator.Where where, int limit) throws SQLException {
        ensureTable(db, collection);
        String idJson = MongoQueryTranslator.exactIdEquality(filter);
        List<Connection> targets = idJson != null ? List.of(shardConnectionFor(idJson)) : allBackendConnections();
        int totalCount = 0;
        List<String> allIds = new ArrayList<>();
        String selectSql = "SELECT id FROM " + qualifiedTable(db, collection) + where.sql()
                + (limit > 0 ? " LIMIT " + limit : "");
        for (Connection conn : targets) {
            try (conn) {
                List<String> ids = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                    bindParams(ps, where.jsonbParams());
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getString(1));
                        }
                    }
                }
                if (!ids.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "DELETE FROM " + qualifiedTable(db, collection) + " WHERE id = ANY(?)")) {
                        ps.setArray(1, conn.createArrayOf("text", ids.toArray()));
                        ps.executeUpdate();
                    }
                }
                totalCount += ids.size();
                allIds.addAll(ids);
            }
        }
        return new WriteResult(totalCount, allIds);
    }

    private void bindParams(PreparedStatement ps, List<String> jsonbParams) throws SQLException {
        int i = 1;
        for (String p : jsonbParams) {
            ps.setObject(i++, jsonb(p));
        }
    }

    private record BsonObjectIdOrPassthrough(Object value) {
        org.bson.BsonValue toBson() {
            if (value instanceof ObjectId oid) {
                return new BsonObjectId(oid);
            }
            return new Document("_id", value).toBsonDocument().get("_id");
        }
    }
}
