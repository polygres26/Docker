package com.sayonora.warp.dynamowire;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.DdlTemplates;
import com.sayonora.warp.core.ShardingStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Item storage for dynamowire, backed by Postgres. Two modes, chosen by which constructor is
 * used:
 *
 * <p><b>Single-backend (legacy):</b> {@link #PgItemStore(String, int, String, String, String)}
 * owns one fixed {@link HikariDataSource}: every table lives on the one Postgres instance
 * dynamowire was pointed at.
 *
 * <p><b>Sharded:</b> {@link #PgItemStore(BackendRegistry)} routes every item-level operation to
 * one of the backends of the DynamoDB store's shard group by hashing the item's partition-key
 * value (the same {@link ShardingStrategy} SQL's value-shard rules use). Table catalog metadata
 * ({@code _dynamo_tables}) lives on one fixed home backend; table/index DDL runs on every shard
 * backend. Scans, secondary-index queries, counts and TTL sweeps scatter to every shard and merge
 * the shards' already-ordered streams (k-way) so limits and pagination are exact.
 *
 * <p><b>Layout:</b> one Postgres table per DynamoDB table ({@code pk_value}, {@code sk_value},
 * {@code sk_num}, {@code item jsonb}). Global/local secondary indexes are Postgres expression
 * indexes over the stored item, so an index needs no separate storage, is always consistent with
 * the table (real DynamoDB GSIs are only eventually consistent) and adding/removing one is DDL
 * only. The table's DynamoDB-level metadata (attribute definitions, indexes, billing, TTL, tags,
 * ...) is one JSON document in the catalog row.
 *
 * <p><b>Atomicity:</b> conditional writes lock the item row ({@code SELECT ... FOR UPDATE}; when the
 * row does not exist yet, a transaction-scoped advisory lock on the key), so concurrent
 * {@code attribute_not_exists} puts, arithmetic updates and transactions serialise per item exactly
 * like DynamoDB's per-item linearisability.
 */
public final class PgItemStore {

    private static final Logger log = LoggerFactory.getLogger(PgItemStore.class);
    private static final Pattern SAFE_IDENT = Pattern.compile("[^a-zA-Z0-9_]");

    /** How long a cached table schema is trusted before it is re-read from the catalog (picks up
     * UpdateTable / DeleteTable performed through another Warp node). */
    private static final long SCHEMA_TTL_NANOS = 10_000_000_000L;

    private final HikariDataSource legacyDs;
    private final BackendRegistry backendRegistry;
    private volatile List<String> lastLoggedShardGroup = null;

    private record CachedSchema(TableSchema schema, long loadedNanos) {}

    private final ConcurrentHashMap<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();

    /** Reverse index (physical Postgres table name -> schema) so CacheStage can recognise a
     * dynamowire table from the bare table name in a SQL statement. */
    private final ConcurrentHashMap<String, TableSchema> physicalTableIndex = new ConcurrentHashMap<>();

    /** Nullable: null means "not a dynamowire-backed table" (or not loaded in this process yet). */
    public TableSchema lookupByPhysicalTable(String physicalTableName) {
        return physicalTableIndex.get(physicalTableName);
    }

    private final ConcurrentHashMap<String, Boolean> catalogEnsured = new ConcurrentHashMap<>();

    public PgItemStore(String host, int port, String database, String user, String password) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + database);
        if (user != null) cfg.setUsername(user);
        if (password != null) cfg.setPassword(password);
        cfg.setPoolName("dynamowire-pg-pool");
        cfg.setMaximumPoolSize(8);
        this.legacyDs = new HikariDataSource(cfg);
        this.backendRegistry = null;
        ensureCatalogEagerly();
    }

    /** Sharded mode -- see the class javadoc. */
    public PgItemStore(BackendRegistry backendRegistry) {
        this.legacyDs = null;
        this.backendRegistry = backendRegistry;
        logShardGroupIfChanged();
        ensureCatalogEagerly();
    }

    private void ensureCatalogEagerly() {
        try (Connection conn = legacyDs != null ? legacyDs.getConnection() : borrowCatalogConnection()) {
            ensureCatalog(conn);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize _dynamo_tables catalog", e);
        }
    }

    private List<String> currentShardGroup() {
        return backendRegistry == null ? List.of()
                : backendRegistry.storeShardGroup(com.sayonora.warp.core.StoreType.DYNAMODB);
    }

    /** The fixed home of {@code _dynamo_tables}: the DynamoDB store's home host of the backend
     * set, else the registry's {@code default} backend (falling back, with a warning, to the first
     * shard-group member). */
    private String currentCatalogBackendName() {
        String home = backendRegistry.storeHome(com.sayonora.warp.core.StoreType.DYNAMODB);
        if (home != null) {
            return home;
        }
        List<String> group = currentShardGroup();
        if (backendRegistry.get(BackendRegistry.DEFAULT_BACKEND_NAME) != null) {
            return BackendRegistry.DEFAULT_BACKEND_NAME;
        }
        if (!group.isEmpty()) {
            log.warn("dynamowire: \"{}\" is not a configured backend -- falling back to \"{}\" (the first "
                    + "shard-group member) for the _dynamo_tables catalog. Add a \"default=...\" entry to "
                    + "WARP_BACKENDS to give the catalog a stable home that survives shard-group changes.",
                    BackendRegistry.DEFAULT_BACKEND_NAME, group.get(0));
            return group.get(0);
        }
        return BackendRegistry.DEFAULT_BACKEND_NAME;
    }

    private void logShardGroupIfChanged() {
        List<String> group = currentShardGroup();
        if (!group.equals(lastLoggedShardGroup)) {
            lastLoggedShardGroup = group;
            if (!group.isEmpty()) {
                log.info("dynamowire: sharding item storage across {} backend(s) by partition key: {}",
                        group.size(), group);
            } else {
                log.info("dynamowire: no shard group configured -- item storage on the single default backend");
            }
        }
    }

    public boolean isSharded() {
        return currentShardGroup().size() > 1;
    }

    /** Which backend name a given partition-key value would route to -- no connection opened. */
    public String resolveBackendFor(String pkValue) {
        if (legacyDs != null) {
            return "default";
        }
        List<String> group = currentShardGroup();
        return group.isEmpty() ? currentCatalogBackendName() : ShardingStrategy.hash(group).resolve(pkValue);
    }

    private Connection borrowCatalogConnection() throws RuntimeException {
        try {
            if (legacyDs != null) {
                return legacyDs.getConnection();
            }
            String catalogBackendName = currentCatalogBackendName();
            BackendTarget target = backendRegistry.resolveForRouting(catalogBackendName);
            if (target == null) {
                throw new IllegalStateException("dynamowire: catalog backend \"" + catalogBackendName
                        + "\" is not a configured backend");
            }
            Connection conn = target.open();
            ensureCatalog(conn);
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to open catalog connection", e);
        }
    }

    /** Resolves the shard connection an item with this partition-key value belongs on. */
    Connection borrowShardConnection(String pkValue) throws SQLException {
        if (legacyDs != null) {
            return legacyDs.getConnection();
        }
        logShardGroupIfChanged();
        return borrowBackend(resolveBackendFor(pkValue));
    }

    private Connection borrowBackend(String backendName) throws SQLException {
        if (legacyDs != null) {
            return legacyDs.getConnection();
        }
        BackendTarget target = backendRegistry.resolveForRouting(backendName);
        if (target == null) {
            throw new IllegalStateException("dynamowire: resolved shard backend \"" + backendName
                    + "\" is not configured");
        }
        return target.open();
    }

    /** One connection per shard backend (just the one, when unsharded). */
    List<Connection> borrowAllShardConnections() throws SQLException {
        if (legacyDs != null) {
            return new ArrayList<>(List.of(legacyDs.getConnection()));
        }
        List<Connection> connections = new ArrayList<>();
        List<String> group = currentShardGroup();
        List<String> names = group.isEmpty() ? List.of(currentCatalogBackendName()) : group;
        try {
            for (String name : names) {
                BackendTarget target = backendRegistry.resolveForRouting(name);
                if (target == null) {
                    continue;
                }
                connections.add(target.open());
            }
        } catch (SQLException | RuntimeException e) {
            for (Connection c : connections) {
                try { c.close(); } catch (SQLException ignored) { }
            }
            throw e;
        }
        return connections;
    }

    static boolean isPostgres(Connection c) throws SQLException {
        String url = c.getMetaData().getURL();
        return url != null && url.startsWith("jdbc:postgresql:");
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        } catch (SQLException ignored) {
            // nothing useful to do
        }
    }

    // ------------------------------------------------------------------------------- catalog

    private void ensureCatalog(Connection c) throws SQLException {
        String url = c.getMetaData().getURL();
        if (Boolean.TRUE.equals(catalogEnsured.get(url))) {
            return;
        }
        String engine = DdlTemplates.engineDirFor(url);
        if (engine == null) {
            throw new IllegalStateException("dynamowire: no DDL support for this backend engine's own "
                    + "JDBC URL (" + url + ") -- see DdlTemplates.engineDirFor for the currently supported set");
        }
        List<String> statements = DdlTemplates.loadStatements(engine, "dynamowire_catalog", Map.of());
        if (statements == null) {
            throw new IllegalStateException("dynamowire: no dynamowire_catalog DDL for engine \"" + engine
                    + "\" -- see ddl/" + engine + "/ for what's actually implemented");
        }
        try (var st = c.createStatement()) {
            for (String statement : statements) {
                st.execute(statement);
            }
            if ("postgres".equals(engine)) {
                // Table metadata (attribute definitions, secondary indexes, billing, TTL, tags) as a JSON
                // document; added in place so catalogs created by older versions keep working.
                st.execute("ALTER TABLE _dynamo_tables ADD COLUMN IF NOT EXISTS meta text");
                st.execute("CREATE TABLE IF NOT EXISTS _dynamo_txn_tokens (token text PRIMARY KEY, request_hash text NOT NULL, "
                        + "created_millis bigint NOT NULL)");
            }
        }
        catalogEnsured.put(url, Boolean.TRUE);
    }

    /** Oracle rejects an unquoted identifier starting with {@code _}; see the catalog DDL. */
    private static String catalogTableName(String engine) {
        return "oracle".equals(engine) ? "dynamo_tables_catalog" : "_dynamo_tables";
    }

    static String pgTableName(String dynamoTableName) {
        return "dynamo_item_" + SAFE_IDENT.matcher(dynamoTableName.toLowerCase()).replaceAll("_");
    }

    public String tableToPgName(String tableName) {
        return pgTableName(tableName);
    }

    // ------------------------------------------------------------------------------- tables

    /** Convenience for a table with only a primary key (on-demand billing, no indexes). */
    public TableSchema createTable(String tableName, String pkName, String pkType, String skName, String skType) {
        return createTable(new TableSchema(tableName, pkName, pkType, skName, skType, "ACTIVE", System.currentTimeMillis()));
    }

    /** Creates the physical tables/indexes on every shard, then the catalog row. */
    public TableSchema createTable(TableSchema schema) {
        String tableName = schema.tableName();
        String pg = pgTableName(tableName);
        try (Connection c = borrowCatalogConnection()) {
            String engine = DdlTemplates.engineDirFor(c.getMetaData().getURL());
            String catalogTable = catalogTableName(engine);
            try (var ps = c.prepareStatement("SELECT table_name FROM " + catalogTable + " WHERE table_name = ? OR pg_table = ?")) {
                ps.setString(1, tableName);
                ps.setString(2, pg);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        if (rs.getString(1).equals(tableName)) {
                            throw new DynamoException("ResourceInUseException", "Cannot create preexisting table");
                        }
                        throw DynamoException.validation("Table name " + tableName + " collides with existing table "
                                + rs.getString(1) + ": Warp stores tables in Postgres tables named after the lower-cased "
                                + "name with every character outside [a-z0-9_] replaced by '_'");
                    }
                }
            }
            if (!schema.meta().indexes().isEmpty() && !"postgres".equals(engine)) {
                throw DynamoException.validation("Secondary indexes require the DynamoDB store to live on a PostgreSQL backend");
            }
            // Physical DDL first: the catalog row is only written once every shard's table exists, so a
            // failed shard never leaves a catalog row claiming a table that was never created.
            for (Connection shardConn : borrowAllShardConnections()) {
                try (shardConn) {
                    createPhysicalTable(shardConn, schema);
                }
            }
            try {
                insertCatalogRow(c, engine, catalogTable, schema, pg);
            } catch (SQLException e) {
                if ("23505".equals(e.getSQLState())) { // a concurrent CreateTable of the same name won
                    throw new DynamoException("ResourceInUseException", "Cannot create preexisting table");
                }
                throw e;
            }
            schemaCache.put(tableName, new CachedSchema(schema, System.nanoTime()));
            physicalTableIndex.put(pg, schema);
            return schema;
        } catch (SQLException e) {
            throw new RuntimeException("CreateTable failed for " + tableName, e);
        }
    }

    private void insertCatalogRow(Connection c, String engine, String catalogTable, TableSchema schema, String pg)
            throws SQLException {
        boolean withMeta = "postgres".equals(engine);
        try (var ps = c.prepareStatement("INSERT INTO " + catalogTable
                + " (table_name, pg_table, pk_name, pk_type, sk_name, sk_type, status, creation_time_millis"
                + (withMeta ? ", meta" : "") + ") VALUES (?,?,?,?,?,?,?,?" + (withMeta ? ",?" : "") + ")")) {
            ps.setString(1, schema.tableName());
            ps.setString(2, pg);
            ps.setString(3, schema.partitionKeyName());
            ps.setString(4, schema.partitionKeyType());
            ps.setString(5, schema.sortKeyName());
            ps.setString(6, schema.sortKeyType());
            ps.setString(7, "ACTIVE");
            ps.setLong(8, schema.creationTimeEpochMillis());
            if (withMeta) ps.setString(9, schema.meta().toJson().toString());
            ps.executeUpdate();
        }
    }

    /** Creates the item table and every secondary index of {@code schema} on one shard connection. */
    void createPhysicalTable(Connection shardConn, TableSchema schema) throws SQLException {
        String pg = pgTableName(schema.tableName());
        String engine = DdlTemplates.engineDirFor(shardConn.getMetaData().getURL());
        List<String> itemDdl = engine == null ? null
                : DdlTemplates.loadStatements(engine, "dynamowire_item_table", Map.of("table", pg));
        if (itemDdl == null) {
            throw new SQLException("dynamowire: no real DDL template for this backend's own engine "
                    + "(jdbcUrl=" + shardConn.getMetaData().getURL() + ") -- see BackendDriverRegistry "
                    + "for the currently-supported engine list");
        }
        try (var st = shardConn.createStatement()) {
            for (String statement : itemDdl) {
                st.execute(statement);
            }
        }
        if ("postgres".equals(engine)) {
            for (TableSchema.IndexDef idx : schema.meta().indexes()) {
                createIndexDdl(shardConn, schema, idx);
            }
            if (schema.meta().ttlEnabled() && schema.meta().ttlAttribute() != null) {
                createTtlIndexDdl(shardConn, schema);
            }
        }
    }

    void createIndexDdl(Connection c, TableSchema schema, TableSchema.IndexDef idx) throws SQLException {
        String pg = pgTableName(schema.tableName());
        StringBuilder cols = new StringBuilder();
        if (idx.local()) {
            cols.append("pk_value, ");
        } else {
            for (TableSchema.KeyAttr k : idx.hash()) cols.append(IndexSql.keyExpr(k)).append(", ");
        }
        for (TableSchema.KeyAttr k : idx.range()) cols.append(IndexSql.keyExpr(k)).append(", ");
        cols.append("pk_value, sk_value");
        StringBuilder where = new StringBuilder();
        for (TableSchema.KeyAttr k : idx.presenceKeys()) {
            if (where.length() > 0) where.append(" AND ");
            where.append(IndexSql.presence(k));
        }
        try (var st = c.createStatement()) {
            st.execute("CREATE INDEX IF NOT EXISTS " + idx.pgName() + " ON " + pg + " (" + cols + ") WHERE " + where);
        }
    }

    void dropIndexDdl(Connection c, TableSchema.IndexDef idx) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("DROP INDEX IF EXISTS " + idx.pgName());
        }
    }

    private static String ttlIndexName(TableSchema schema) {
        return pgTableName(schema.tableName()) + "_ttl_idx";
    }

    void createTtlIndexDdl(Connection c, TableSchema schema) throws SQLException {
        String attr = IndexSql.lit(schema.meta().ttlAttribute());
        try (var st = c.createStatement()) {
            st.execute("CREATE INDEX IF NOT EXISTS " + shorten(ttlIndexName(schema)) + " ON " + pgTableName(schema.tableName())
                    + " (((item->" + attr + "->>'N')::numeric)) WHERE item->" + attr + "->>'N' IS NOT NULL");
        }
    }

    void dropTtlIndexDdl(Connection c, TableSchema schema) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("DROP INDEX IF EXISTS " + shorten(ttlIndexName(schema)));
        }
    }

    private static String shorten(String ident) {
        return ident.length() <= 60 ? ident : ident.substring(0, 50) + Integer.toHexString(ident.hashCode());
    }

    /** Runs {@code ddl} on every shard (index create/drop for UpdateTable). */
    void onAllShards(SqlConsumer<Connection> ddl) {
        try {
            for (Connection c : borrowAllShardConnections()) {
                try (c) {
                    ddl.accept(c);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DDL failed on a shard", e);
        }
    }

    @FunctionalInterface
    interface SqlConsumer<T> {
        void accept(T t) throws SQLException;
    }

    @FunctionalInterface
    interface SqlFunction<T, R> {
        R apply(T t) throws SQLException;
    }

    public void deleteTable(String tableName) {
        try {
            for (Connection shardConn : borrowAllShardConnections()) {
                try (shardConn; var st = shardConn.createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + pgTableName(tableName));
                }
            }
            try (Connection c = borrowCatalogConnection()) {
                String catalogTable = catalogTableName(DdlTemplates.engineDirFor(c.getMetaData().getURL()));
                try (var ps = c.prepareStatement("DELETE FROM " + catalogTable + " WHERE table_name = ?")) {
                    ps.setString(1, tableName);
                    ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DeleteTable failed for " + tableName, e);
        } finally {
            schemaCache.remove(tableName);
            physicalTableIndex.remove(pgTableName(tableName));
        }
    }

    /** Persists new metadata for a table (UpdateTable / TTL / tags / PITR) and refreshes the cache. */
    public TableSchema updateMeta(TableSchema schema, TableSchema.Meta meta) {
        TableSchema updated = schema.withMeta(meta);
        try (Connection c = borrowCatalogConnection()) {
            String engine = DdlTemplates.engineDirFor(c.getMetaData().getURL());
            if (!"postgres".equals(engine)) {
                throw DynamoException.validation("This operation requires the DynamoDB store to live on a PostgreSQL backend");
            }
            try (var ps = c.prepareStatement("UPDATE " + catalogTableName(engine) + " SET meta = ? WHERE table_name = ?")) {
                ps.setString(1, meta.toJson().toString());
                ps.setString(2, schema.tableName());
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("UpdateTable failed for " + schema.tableName(), e);
        }
        schemaCache.put(schema.tableName(), new CachedSchema(updated, System.nanoTime()));
        physicalTableIndex.put(pgTableName(schema.tableName()), updated);
        return updated;
    }

    public TableSchema describeTable(String tableName) {
        CachedSchema cached = schemaCache.get(tableName);
        if (cached != null && System.nanoTime() - cached.loadedNanos < SCHEMA_TTL_NANOS) {
            return cached.schema;
        }
        TableSchema loaded;
        try {
            loaded = loadTableSchema(tableName);
        } catch (DynamoException e) {
            schemaCache.remove(tableName);
            physicalTableIndex.remove(pgTableName(tableName));
            throw e;
        }
        schemaCache.put(tableName, new CachedSchema(loaded, System.nanoTime()));
        physicalTableIndex.put(pgTableName(tableName), loaded);
        return loaded;
    }

    private TableSchema loadTableSchema(String tableName) {
        try (Connection c = borrowCatalogConnection()) {
            String engine = DdlTemplates.engineDirFor(c.getMetaData().getURL());
            String catalogTable = catalogTableName(engine);
            boolean withMeta = "postgres".equals(engine);
            try (var ps = c.prepareStatement("SELECT pk_name, pk_type, sk_name, sk_type, status, creation_time_millis"
                    + (withMeta ? ", meta" : "") + " FROM " + catalogTable + " WHERE table_name = ?")) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new DynamoException("ResourceNotFoundException", "Cannot do operations on a non-existent table");
                    }
                    String pkName = rs.getString(1), pkType = rs.getString(2), skName = rs.getString(3), skType = rs.getString(4);
                    TableSchema.Meta meta = TableSchema.Meta.defaults(tableName, pkName, pkType, skName, skType, rs.getLong(6));
                    String metaJson = withMeta ? rs.getString(7) : null;
                    if (metaJson != null) {
                        meta = TableSchema.Meta.fromJson(JsonParser.parseString(metaJson).getAsJsonObject(), meta);
                    }
                    return new TableSchema(tableName, pkName, pkType, skName, skType, rs.getString(5), rs.getLong(6), meta);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DescribeTable failed for " + tableName, e);
        }
    }

    /** Item count of the table and (for each index, in {@code meta().indexes()} order) how many
     * items it contains, summed over every shard. */
    public long[] counts(TableSchema schema) {
        List<TableSchema.IndexDef> indexes = schema.meta().indexes();
        long[] total = new long[1 + indexes.size()];
        StringBuilder sql = new StringBuilder("SELECT count(*)");
        for (TableSchema.IndexDef idx : indexes) {
            StringBuilder w = new StringBuilder();
            for (TableSchema.KeyAttr k : idx.presenceKeys()) {
                if (w.length() > 0) w.append(" AND ");
                w.append(IndexSql.presence(k));
            }
            sql.append(", count(*) FILTER (WHERE ").append(w).append(")");
        }
        sql.append(" FROM ").append(pgTableName(schema.tableName()));
        try {
            for (Connection c : borrowAllShardConnections()) {
                try (c; var st = c.createStatement()) {
                    String text = isPostgres(c) ? sql.toString() : "SELECT count(*) FROM " + pgTableName(schema.tableName());
                    try (ResultSet rs = st.executeQuery(text)) {
                        rs.next();
                        for (int i = 0; i < total.length; i++) {
                            if (i == 0 || isPostgres(c)) total[i] += rs.getLong(i + 1);
                        }
                    } catch (SQLException e) {
                        if (!"42P01".equals(e.getSQLState())) throw e;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count items in " + schema.tableName(), e);
        }
        return total;
    }

    public long itemCount(TableSchema schema) {
        return counts(schema)[0];
    }

    /** Approximate table size in bytes (sum of item JSON lengths), for DescribeTable. */
    public long sizeBytes(TableSchema schema) {
        long total = 0;
        try {
            for (Connection c : borrowAllShardConnections()) {
                try (c; var st = c.createStatement();
                        ResultSet rs = st.executeQuery("SELECT COALESCE(sum(length(item::text)), 0) FROM " + pgTableName(schema.tableName()))) {
                    rs.next();
                    total += rs.getLong(1);
                } catch (SQLException e) {
                    if (!"42P01".equals(e.getSQLState())) throw e;
                }
            }
        } catch (SQLException e) {
            return 0;
        }
        return total;
    }

    /** Table names in ascending order, starting after {@code exclusiveStart}; at most {@code limit}. */
    public List<String> listTables(String exclusiveStart, int limit) {
        List<String> out = new ArrayList<>();
        try (Connection c = borrowCatalogConnection()) {
            String catalogTable = catalogTableName(DdlTemplates.engineDirFor(c.getMetaData().getURL()));
            try (var st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT table_name FROM " + catalogTable + " ORDER BY table_name")) {
                while (rs.next()) out.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("ListTables failed", e);
        }
        out.sort((a, b) -> java.util.Arrays.compareUnsigned(a.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                b.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        int from = 0;
        if (exclusiveStart != null) {
            while (from < out.size() && compareBytes(out.get(from), exclusiveStart) <= 0) from++;
        }
        int to = Math.min(out.size(), from + limit);
        return new ArrayList<>(out.subList(from, to));
    }

    public List<String> listTables() {
        return listTables(null, Integer.MAX_VALUE);
    }

    static int compareBytes(String a, String b) {
        return java.util.Arrays.compareUnsigned(a.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                b.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** Tables (freshly read from the catalog) that have TTL enabled -- the TTL sweeper's work list. */
    public List<TableSchema> tablesWithTtl() {
        List<TableSchema> out = new ArrayList<>();
        try (Connection c = borrowCatalogConnection()) {
            String engine = DdlTemplates.engineDirFor(c.getMetaData().getURL());
            if (!"postgres".equals(engine)) return out;
            List<String> names = new ArrayList<>();
            try (var st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT table_name FROM _dynamo_tables WHERE meta LIKE '%\"ttlOn\":true%'")) {
                while (rs.next()) names.add(rs.getString(1));
            }
            for (String n : names) {
                try {
                    out.add(describeTable(n));
                } catch (DynamoException ignored) {
                    // dropped concurrently
                }
            }
        } catch (SQLException e) {
            log.warn("dynamowire: TTL sweeper could not list tables: {}", e.getMessage());
        }
        return out;
    }

    // ------------------------------------------------------------------------------- items

    public record WriteResult(Map<String, AttributeValue> old, Map<String, AttributeValue> neu, List<Expr.Path> touched) {}

    /** ConditionalCheckFailedException carrying the item that failed the condition. */
    public static final class ConditionalCheckFailed extends DynamoException {
        public final Map<String, AttributeValue> item;

        public ConditionalCheckFailed(Map<String, AttributeValue> item) {
            super("ConditionalCheckFailedException", "The conditional request failed");
            this.item = item;
        }
    }

    /**
     * Reads the item under a lock, in one round trip. On Postgres a transaction-scoped advisory lock on
     * the item's key is taken first and then the row is read {@code FOR UPDATE} (two statements pipelined in
     * a single request; each takes its own snapshot, so the read sees whatever the previous lock holder
     * committed). The advisory lock is what makes a missing row lockable -- there is no row to lock yet --
     * so concurrent creators ({@code attribute_not_exists} puts, upserting updates) serialise per item.
     */
    Map<String, AttributeValue> readLocked(Connection c, String pg, String pk, String sk) throws SQLException {
        if (!isPostgres(c)) {
            return selectForUpdate(c, pg, pk, sk);
        }
        try (var ps = c.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0)); SELECT item FROM " + pg
                + " WHERE pk_value = ? AND sk_value = ? FOR UPDATE")) {
            ps.setString(1, pg + '|' + pk + '|' + sk);
            ps.setString(2, pk);
            ps.setString(3, sk);
            boolean isResultSet = ps.execute();
            while (true) {
                if (isResultSet) {
                    try (ResultSet rs = ps.getResultSet()) {
                        if (rs.getMetaData().getColumnCount() == 1 && "item".equalsIgnoreCase(rs.getMetaData().getColumnLabel(1))) {
                            if (!rs.next()) return null;
                            return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
                        }
                    }
                } else if (ps.getUpdateCount() == -1) {
                    return null;
                }
                isResultSet = ps.getMoreResults();
            }
        }
    }

    private Map<String, AttributeValue> selectForUpdate(Connection c, String pg, String pk, String sk) throws SQLException {
        try (var ps = c.prepareStatement("SELECT item FROM " + pg + " WHERE pk_value = ? AND sk_value = ? FOR UPDATE")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
            }
        }
    }

    Map<String, AttributeValue> readLocked(Connection c, TableSchema s, Map<String, AttributeValue> keyOrItem) throws SQLException {
        return readLocked(c, pgTableName(s.tableName()), KeyCodec.partitionToken(s, keyOrItem), KeyCodec.sortToken(s, keyOrItem));
    }

    void writeRow(Connection c, TableSchema s, Map<String, AttributeValue> item) throws SQLException {
        String pg = pgTableName(s.tableName());
        String pk = KeyCodec.partitionToken(s, item);
        String sk = KeyCodec.sortToken(s, item);
        BigDecimal skNum = s.hasSortKey() && "N".equals(s.sortKeyType()) ? new BigDecimal(sk) : null;
        String engine = DdlTemplates.engineDirFor(c.getMetaData().getURL());
        try (var ps = c.prepareStatement(PgItemStoreDialect.upsertSql(engine, pg))) {
            PgItemStoreDialect.bindUpsert(ps, engine, pk, sk, skNum, itemToJson(item).toString());
            ps.executeUpdate();
        }
    }

    void deleteRow(Connection c, TableSchema s, Map<String, AttributeValue> key) throws SQLException {
        try (var ps = c.prepareStatement("DELETE FROM " + pgTableName(s.tableName()) + " WHERE pk_value = ? AND sk_value = ?")) {
            ps.setString(1, KeyCodec.partitionToken(s, key));
            ps.setString(2, KeyCodec.sortToken(s, key));
            ps.executeUpdate();
        }
    }

    /** Runs {@code fn} on the connection for {@code pk}'s shard; if the shard has no such table yet
     * (a backend added to the shard group after the table was created) the table and its indexes
     * are created there and the operation retried once. */
    private <T> T onShard(TableSchema s, String pk, SqlFunction<Connection, T> fn) {
        try (Connection c = borrowShardConnection(pk)) {
            try {
                return fn.apply(c);
            } catch (SQLException e) {
                if (!"42P01".equals(e.getSQLState()) || !mayCreateMissingTable(c)) throw e;
                try {
                    if (!c.getAutoCommit()) c.rollback();
                } catch (SQLException ignored) {
                    // best effort
                }
                createPhysicalTable(c, s);
                return fn.apply(c);
            }
        } catch (SQLException e) {
            throw new RuntimeException(s.tableName() + ": operation failed", e);
        }
    }

    /**
     * A shard that lacks the item table because it joined the shard group after the table was created
     * gets the table on first use. The catalog's home backend never does: there a missing table means
     * it was dropped behind dynamowire's back, which must surface as an error, not be papered over.
     */
    boolean mayCreateMissingTable(Connection c) throws SQLException {
        if (legacyDs != null || !isSharded()) return false;
        BackendTarget home = backendRegistry.resolveForRouting(currentCatalogBackendName());
        String url = c.getMetaData().getURL();
        return home == null || url == null || !url.equals(home.jdbcUrl());
    }

    public WriteResult put(TableSchema s, Map<String, AttributeValue> item, Expr.Cond cond, boolean needOld) {
        ItemValidator.validateItem(s, item, false);
        String pk = KeyCodec.partitionToken(s, item);
        boolean needsTransaction = cond != null || needOld;
        return onShard(s, pk, c -> {
            if (!needsTransaction) {
                writeRow(c, s, item);
                return new WriteResult(null, item, List.of());
            }
            c.setAutoCommit(false);
            try {
                Map<String, AttributeValue> existing = readLocked(c, s, item);
                if (cond != null && !ExprEval.test(cond, existing)) {
                    c.rollback();
                    throw new ConditionalCheckFailed(existing);
                }
                writeRow(c, s, item);
                c.commit();
                return new WriteResult(existing, item, List.of());
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        });
    }

    public Map<String, AttributeValue> getItem(TableSchema s, Map<String, AttributeValue> key) {
        String pg = pgTableName(s.tableName());
        String pk = KeyCodec.partitionToken(s, key);
        String sk = KeyCodec.sortToken(s, key);
        return onShard(s, pk, c -> {
            try (var ps = c.prepareStatement("SELECT item FROM " + pg + " WHERE pk_value = ? AND sk_value = ?")) {
                ps.setString(1, pk);
                ps.setString(2, sk);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
                }
            }
        });
    }

    public WriteResult delete(TableSchema s, Map<String, AttributeValue> key, Expr.Cond cond, boolean needOld) {
        String pk = KeyCodec.partitionToken(s, key);
        boolean needsTransaction = cond != null || needOld;
        return onShard(s, pk, c -> {
            if (!needsTransaction) {
                deleteRow(c, s, key);
                return new WriteResult(null, null, List.of());
            }
            c.setAutoCommit(false);
            try {
                Map<String, AttributeValue> existing = readLocked(c, s, key);
                if (cond != null && !ExprEval.test(cond, existing)) {
                    c.rollback();
                    throw new ConditionalCheckFailed(existing);
                }
                if (existing != null) deleteRow(c, s, key);
                c.commit();
                return new WriteResult(existing, null, List.of());
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        });
    }

    public WriteResult update(TableSchema s, Map<String, AttributeValue> key, Expr.UpdatePlan plan, Expr.Cond cond) {
        String pk = KeyCodec.partitionToken(s, key);
        return onShard(s, pk, c -> {
            c.setAutoCommit(false);
            try {
                Map<String, AttributeValue> existing = readLocked(c, s, key);
                if (cond != null && !ExprEval.test(cond, existing)) {
                    c.rollback();
                    throw new ConditionalCheckFailed(existing);
                }
                WriteResult r = computeUpdate(s, key, existing, plan);
                writeRow(c, s, r.neu());
                c.commit();
                return r;
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        });
    }

    /** The item that results from applying {@code plan} to {@code existing} (or to a new item
     * holding just the key), validated as a written item. */
    WriteResult computeUpdate(TableSchema s, Map<String, AttributeValue> key, Map<String, AttributeValue> existing,
            Expr.UpdatePlan plan) {
        for (Expr.UpdateAction a : plan.actions()) {
            String top = a.path().top();
            if (top.equals(s.partitionKeyName()) || top.equals(s.sortKeyName())) {
                throw DynamoException.validation("One or more parameter values were invalid: Cannot update attribute "
                        + top + ". This attribute is part of the key");
            }
        }
        Map<String, AttributeValue> base = existing != null ? existing : new LinkedHashMap<>(key);
        ExprEval.UpdateResult res = ExprEval.apply(plan, base);
        Map<String, AttributeValue> neu = res.item();
        neu.putAll(key);
        ItemValidator.validateItem(s, neu, true);
        return new WriteResult(existing, neu, res.touched());
    }

    // ------------------------------------------------------------------------------- transactions

    /** Transactions span shards: one database transaction per involved shard, all prepared
     * (rows locked, conditions evaluated, writes applied) before the first commit. */
    public final class TxnScope implements AutoCloseable {
        private final Map<String, Connection> conns = new LinkedHashMap<>();
        private final boolean snapshot;
        private boolean finished;

        TxnScope(boolean snapshot) {
            this.snapshot = snapshot;
        }

        public Connection forPartition(String pkToken) throws SQLException {
            String backend = resolveBackendFor(pkToken);
            Connection c = conns.get(backend);
            if (c == null) {
                c = legacyDs != null ? legacyDs.getConnection() : borrowBackend(backend);
                c.setAutoCommit(false);
                if (snapshot) {
                    c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                }
                conns.put(backend, c);
            }
            return c;
        }

        public void commit() throws SQLException {
            finished = true;
            SQLException first = null;
            boolean committedAny = false;
            for (Connection c : conns.values()) {
                try {
                    if (first == null) {
                        c.commit();
                        committedAny = true;
                    } else {
                        c.rollback();
                    }
                } catch (SQLException e) {
                    first = e;
                }
            }
            if (first != null) {
                if (committedAny) {
                    log.error("dynamowire: a multi-shard transaction committed on some shards but failed on another: {}",
                            first.getMessage());
                }
                throw first;
            }
        }

        public void rollback() {
            finished = true;
            for (Connection c : conns.values()) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // nothing to do
                }
            }
        }

        @Override
        public void close() {
            if (!finished) rollback();
            for (Connection c : conns.values()) {
                try {
                    if (snapshot) c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    c.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // connection is going back to the pool regardless
                }
                closeQuietly(c);
            }
            conns.clear();
        }
    }

    /** @param snapshot REPEATABLE READ (a consistent snapshot per shard) for TransactGetItems. */
    public TxnScope beginTransaction(boolean snapshot) {
        return new TxnScope(snapshot);
    }

    /** Plain read inside a transaction (no locks). */
    Map<String, AttributeValue> readUnlocked(Connection c, TableSchema s, Map<String, AttributeValue> key) throws SQLException {
        try (var ps = c.prepareStatement("SELECT item FROM " + pgTableName(s.tableName()) + " WHERE pk_value = ? AND sk_value = ?")) {
            ps.setString(1, KeyCodec.partitionToken(s, key));
            ps.setString(2, KeyCodec.sortToken(s, key));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
            }
        }
    }

    /** Idempotency-token lookup for TransactWriteItems: returns the stored request hash if the token was already used. */
    public String findTransactionToken(String token) {
        try (Connection c = borrowCatalogConnection()) {
            if (!isPostgres(c)) return null;
            try (var ps = c.prepareStatement("SELECT request_hash FROM _dynamo_txn_tokens WHERE token = ? AND created_millis > ?")) {
                ps.setString(1, token);
                ps.setLong(2, System.currentTimeMillis() - 10 * 60_000L);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Transaction token lookup failed", e);
        }
    }

    public void recordTransactionToken(String token, String requestHash) {
        try (Connection c = borrowCatalogConnection()) {
            if (!isPostgres(c)) return;
            try (var st = c.prepareStatement("DELETE FROM _dynamo_txn_tokens WHERE created_millis < ?")) {
                st.setLong(1, System.currentTimeMillis() - 10 * 60_000L);
                st.executeUpdate();
            }
            try (var ps = c.prepareStatement("INSERT INTO _dynamo_txn_tokens (token, request_hash, created_millis) VALUES (?,?,?) "
                    + "ON CONFLICT (token) DO UPDATE SET request_hash = EXCLUDED.request_hash, created_millis = EXCLUDED.created_millis")) {
                ps.setString(1, token);
                ps.setString(2, requestHash);
                ps.setLong(3, System.currentTimeMillis());
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Transaction token store failed", e);
        }
    }

    // ------------------------------------------------------------------------------- TTL

    /** Deletes up to {@code max} expired items of {@code schema} on every shard; returns the deleted
     * (pk, sk) tokens so the caller can invalidate its caches. */
    public List<String[]> sweepExpired(TableSchema schema, long nowEpochSeconds, int max) {
        List<String[]> deleted = new ArrayList<>();
        if (!schema.meta().ttlEnabled() || schema.meta().ttlAttribute() == null) return deleted;
        String pg = pgTableName(schema.tableName());
        String attr = IndexSql.lit(schema.meta().ttlAttribute());
        try {
            for (Connection c : borrowAllShardConnections()) {
                try (c) {
                    if (!isPostgres(c)) continue;
                    String sql = "DELETE FROM " + pg + " WHERE ctid = ANY(ARRAY(SELECT ctid FROM " + pg
                            + " WHERE item->" + attr + "->>'N' IS NOT NULL AND (item->" + attr + "->>'N')::numeric <= ? LIMIT ?)) "
                            + "RETURNING pk_value, sk_value";
                    try (var ps = c.prepareStatement(sql)) {
                        ps.setBigDecimal(1, BigDecimal.valueOf(nowEpochSeconds));
                        ps.setInt(2, max);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) deleted.add(new String[] {rs.getString(1), rs.getString(2)});
                        }
                    } catch (SQLException e) {
                        if (!"42P01".equals(e.getSQLState())) throw e;
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("dynamowire: TTL sweep of {} failed: {}", schema.tableName(), e.getMessage());
        }
        return deleted;
    }

    // ------------------------------------------------------------------------------- JSON helpers

    public static JsonObject itemToJson(Map<String, AttributeValue> item) {
        JsonObject obj = new JsonObject();
        for (var e : item.entrySet()) obj.add(e.getKey(), e.getValue().toJson());
        return obj;
    }

    public static Map<String, AttributeValue> jsonToItem(JsonObject obj) {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        for (var e : obj.entrySet()) item.put(e.getKey(), AttributeValue.fromJson(e.getValue()));
        return item;
    }

    public void close() {
        if (legacyDs != null) {
            legacyDs.close();
        }
    }
}
