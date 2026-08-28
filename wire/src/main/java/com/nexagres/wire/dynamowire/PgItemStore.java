package com.nexagres.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.core.BackendTarget;
import com.nexagres.wire.core.DdlTemplates;
import com.nexagres.wire.core.ShardingStrategy;
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
 * owns one fixed {@link HikariDataSource}, exactly as before this class supported sharding at
 * all -- every table lives on the one Postgres instance dynamowire was pointed at.
 *
 * <p><b>Sharded:</b> {@link #PgItemStore(BackendRegistry)} routes every item-level operation
 * (PutItem/GetItem/UpdateItem/DeleteItem/Query) to one of {@code backendRegistry.shardGroup()}'s
 * backends by hashing the item's own partition-key value -- the same {@link ShardingStrategy}
 * SQL's value-shard rules already use, applied to the one key DynamoDB itself would use to
 * distribute partitions in real DynamoDB. This is opt-in: a registry with an empty shard group
 * behaves exactly like the legacy single-backend mode (routes everything to the registry's
 * {@code DEFAULT_BACKEND_NAME} target). Table catalog metadata ({@code _dynamo_tables}) and
 * DDL are NOT shardable by definition -- {@link #createTable}/{@link #deleteTable} run their DDL
 * on every shard backend (so every shard has the physical table an item might land on) but keep
 * exactly one catalog row, always on {@code DEFAULT_BACKEND_NAME} regardless of shard group
 * membership (that name has to stay fixed and stable -- see {@link #currentCatalogBackendName}'s
 * javadoc for why using a shard-group member here broke DescribeTable the first time this was
 * tried). A deployment that shards needs {@code POLYWIRE_BACKENDS} to include a
 * {@code default=...} entry alongside the shard backends for exactly this reason.
 *
 * <p><b>Live-reloadable:</b> the shard group is re-read from {@code backendRegistry} on every
 * call, not captured once at construction -- a {@code POLYWIRE_BACKENDS}/
 * {@code POLYWIRE_SHARD_BACKENDS} config change takes effect on this store's very next
 * operation, same as {@code RouterStage}'s SQL sharding. What it does NOT do is migrate data:
 * turning sharding on after a table already has rows leaves those rows on whatever backend they
 * were written to (the old single default, most likely) -- reads for those specific keys will
 * miss if they now hash to a different shard. This mirrors how a real distributed store needs an
 * explicit resharding/migration step; it isn't attempted automatically here.
 *
 * <p><b>Known limitation:</b> {@link #scan} has no partition key to hash on, so a sharded store
 * fans it out across every shard backend and concatenates results -- correct in the sense that
 * every item is seen, but each shard's own page boundary is respected rather than one globally
 * consistent page across all shards. Real DynamoDB has the same practical scan-across-partitions
 * cost; this doesn't attempt to hide it.
 */
public final class PgItemStore {

    private static final Logger log = LoggerFactory.getLogger(PgItemStore.class);
    private static final Pattern SAFE_IDENT = Pattern.compile("[^a-zA-Z0-9_]");

    private final HikariDataSource legacyDs;
    private final BackendRegistry backendRegistry;
    private volatile List<String> lastLoggedShardGroup = null;

    // describeTable() used to hit Postgres (a real network round trip against _dynamo_tables) on
    // EVERY GetItem/PutItem/etc call, including cache hits that never touch the item table at
    // all -- found live while chasing a sub-0.3ms cache-hit RTT target: this was the actual
    // dominant cost, not JSON parsing or the item cache itself. Table schema is set once at
    // CreateTable and Dynamo's API has no way to change a table's key schema afterward, so it's
    // safe to cache indefinitely until DeleteTable (or CreateTable itself, which populates it
    // fresh). Keyed by table name only -- schema is identical regardless of which shard backend
    // ends up serving a given item.
    private final ConcurrentHashMap<String, TableSchema> schemaCache = new ConcurrentHashMap<>();
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

    /**
     * Sharded mode -- routes by {@code backendRegistry.shardGroup()} when non-empty, otherwise
     * behaves like a single-backend store pointed at {@code backendRegistry}'s default target.
     * The shard group is read fresh from {@code backendRegistry} on every call (see {@link
     * #currentShardGroup()}), not captured once here -- {@code backendRegistry} is the same
     * mutable, hot-reloaded instance {@code RouterStage} already reads live, so a
     * POLYWIRE_BACKENDS/POLYWIRE_SHARD_BACKENDS config change takes effect on this store's very
     * next operation, no process restart needed, matching how every other router-backed feature
     * in PolyWire already reloads.
     */
    public PgItemStore(BackendRegistry backendRegistry) {
        this.legacyDs = null;
        this.backendRegistry = backendRegistry;
        logShardGroupIfChanged();
        // Registry mode's own borrowCatalogConnection() already calls ensureCatalog on every
        // resolution (see its javadoc) -- this eager call is only load-bearing for legacy mode,
        // where borrowCatalogConnection() never calls it since there's only ever one fixed
        // backend. Harmless (and cheap -- ensureCatalog is cached per URL) either way, so both
        // constructors share it rather than duplicating the branch.
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
        return backendRegistry == null ? List.of() : backendRegistry.shardGroup();
    }

    /**
     * The registry's stable {@code DEFAULT_BACKEND_NAME} when it's actually registered --
     * {@code _dynamo_tables} needs one fixed home that doesn't move when the shard group is
     * reconfigured. (Earlier this was {@code shardGroup.get(0)}, which broke DescribeTable for
     * every table created before sharding was turned on: the catalog "moved" to a shard backend
     * that had never seen that table's row.) A deployment that shards needs {@code
     * POLYWIRE_BACKENDS} to keep a {@code default=...} entry alongside the shard backends for
     * exactly this reason -- but if an operator forgets that and {@code default} isn't
     * registered, falling back to the first shard-group member (the old behavior) and logging a
     * warning is far better than what this did the first time: an unhandled
     * {@code IllegalStateException} out of the constructor that took the entire PolyWire process
     * down at startup, every wire protocol included, over a dynamowire-only config detail.
     */
    private String currentCatalogBackendName() {
        List<String> group = currentShardGroup();
        if (backendRegistry.get(BackendRegistry.DEFAULT_BACKEND_NAME) != null) {
            return BackendRegistry.DEFAULT_BACKEND_NAME;
        }
        if (!group.isEmpty()) {
            log.warn("dynamowire: \"{}\" is not a configured backend -- falling back to \"{}\" (the first "
                    + "shard-group member) for the _dynamo_tables catalog. Add a \"default=...\" entry to "
                    + "POLYWIRE_BACKENDS to give the catalog a stable home that survives shard-group changes.",
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
        return !currentShardGroup().isEmpty();
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
            // resolveForRouting, not get -- see BackendRegistry.resolveForRouting's javadoc.
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
    private Connection borrowShardConnection(String pkValue) throws SQLException {
        if (legacyDs != null) {
            return legacyDs.getConnection();
        }
        logShardGroupIfChanged();
        String backendName = resolveBackendFor(pkValue);
        BackendTarget target = backendRegistry.resolveForRouting(backendName);
        if (target == null) {
            throw new IllegalStateException("dynamowire: resolved shard backend \"" + backendName
                    + "\" is not configured");
        }
        return target.open();
    }

    private List<Connection> borrowAllShardConnections() throws SQLException {
        List<Connection> connections = new ArrayList<>();
        List<String> group = currentShardGroup();
        List<String> names = group.isEmpty() ? List.of(currentCatalogBackendName()) : group;
        for (String name : names) {
            BackendTarget target = backendRegistry.resolveForRouting(name);
            if (target == null) {
                continue;
            }
            connections.add(target.open());
        }
        return connections;
    }

    /** Idempotent per physical backend (cached by JDBC URL, same technique as sqswire's {@code
     * PgQueueStore#ensureTable}) -- called from {@link #borrowCatalogConnection} on every
     * resolution, not just once at construction, so a switchover's fallback (a genuinely separate
     * Postgres, not necessarily a replica sharing the primary's schema) gets the catalog table the
     * first time routing actually lands there, not never. Does NOT close {@code c} -- the caller
     * owns and returns it for real use. */
    private void ensureCatalog(Connection c) throws SQLException {
        String key = c.getMetaData().getURL();
        if (Boolean.TRUE.equals(catalogEnsured.get(key))) {
            return;
        }
        // Real DDL, loaded from ddl/postgres/dynamowire_catalog.sql -- see DdlTemplates' own
        // javadoc. Postgres-only today: the catalog connection is always the control-plane
        // backend, which is always Postgres in every deployment shape this project supports.
        List<String> statements = DdlTemplates.loadStatements("postgres", "dynamowire_catalog", Map.of());
        try (var st = c.createStatement()) {
            for (String statement : statements) {
                st.execute(statement);
            }
        }
        catalogEnsured.put(key, Boolean.TRUE);
    }

    private static String pgTableName(String dynamoTableName) {
        return "dynamo_item_" + SAFE_IDENT.matcher(dynamoTableName.toLowerCase()).replaceAll("_");
    }

    public TableSchema createTable(String tableName, String pkName, String pkType, String skName, String skType) {
        String pg = pgTableName(tableName);
        try (Connection c = borrowCatalogConnection()) {
            try (var ps = c.prepareStatement("SELECT 1 FROM _dynamo_tables WHERE table_name = ?")) {
                ps.setString(1, tableName);
                if (ps.executeQuery().next()) {
                    throw new DynamoException("ResourceInUseException", "Table already exists: " + tableName);
                }
            }
            // Real bug, found live testing a MySQL/MariaDB shard backend: this used to INSERT the
            // catalog metadata row FIRST, then create the real physical table(s) below -- a real
            // DDL failure on any one shard (a jsonb-column syntax error on a non-Postgres engine,
            // but just as real on Postgres itself for a transient disk-full/permission failure)
            // left the metadata row committed and orphaned, silently claiming a table exists that
            // was never actually created anywhere. Every later CreateTable attempt for that same
            // name then permanently, incorrectly reports ResourceInUseException instead of the
            // real underlying failure -- confirmed live: the client only ever saw the misleading
            // "already exists" from a RETRY of the original failed call, not the real error, which
            // only appeared in this process's own log. Real physical DDL now runs FIRST; the
            // catalog row is only ever written once every shard's own table genuinely exists.
            for (Connection shardConn : shardConnectionsForDdl()) {
                try (shardConn) {
                    String engine = DdlTemplates.engineDirFor(shardConn.getMetaData().getURL());
                    // Real DDL, loaded from ddl/<engine>/dynamowire_item_table.sql -- see
                    // DdlTemplates' own javadoc for the full reasoning and the real per-engine
                    // differences (Postgres jsonb -> Oracle CLOB+IS JSON / SQL Server
                    // NVARCHAR(MAX)+ISJSON() / MySQL JSON, etc.).
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
                }
            }
            long now = System.currentTimeMillis();
            try (var ps = c.prepareStatement(
                    "INSERT INTO _dynamo_tables (table_name, pg_table, pk_name, pk_type, sk_name, sk_type, status, creation_time_millis) VALUES (?,?,?,?,?,?,?,?)")) {
                ps.setString(1, tableName);
                ps.setString(2, pg);
                ps.setString(3, pkName);
                ps.setString(4, pkType);
                ps.setString(5, skName);
                ps.setString(6, skType);
                ps.setString(7, "ACTIVE");
                ps.setLong(8, now);
                ps.executeUpdate();
            }
            TableSchema schema = new TableSchema(tableName, pkName, pkType, skName, skType, "ACTIVE", now);
            schemaCache.put(tableName, schema);
            return schema;
        } catch (SQLException e) {
            throw new RuntimeException("CreateTable failed for " + tableName, e);
        }
    }

    private List<Connection> shardConnectionsForDdl() throws SQLException {
        if (legacyDs != null) {
            return List.of(legacyDs.getConnection());
        }
        return borrowAllShardConnections();
    }

    public void deleteTable(String tableName) {
        TableSchema schema = describeTable(tableName);
        try {
            for (Connection shardConn : shardConnectionsForDdl()) {
                try (shardConn; var st = shardConn.createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + pgTableName(tableName));
                }
            }
            try (Connection c = borrowCatalogConnection();
                    var ps = c.prepareStatement("DELETE FROM _dynamo_tables WHERE table_name = ?")) {
                ps.setString(1, tableName);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("DeleteTable failed for " + tableName, e);
        } finally {
            schemaCache.remove(tableName);
        }
    }

    public TableSchema describeTable(String tableName) {
        TableSchema cached = schemaCache.get(tableName);
        if (cached != null) {
            return cached;
        }
        TableSchema loaded = loadTableSchema(tableName);
        // A benign race with a concurrent DeleteTable is fine either way here: if delete's
        // invalidation (schemaCache.remove) already ran, this put just re-adds a schema for a
        // table that's mid-delete -- the next DeleteTable-triggered invalidation (or this same
        // one, if it hasn't happened yet) cleans it up; nothing reads a torn/partial value.
        schemaCache.put(tableName, loaded);
        return loaded;
    }

    private TableSchema loadTableSchema(String tableName) {
        try (Connection c = borrowCatalogConnection();
                var ps = c.prepareStatement("SELECT pk_name, pk_type, sk_name, sk_type, status, creation_time_millis FROM _dynamo_tables WHERE table_name = ?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new DynamoException("ResourceNotFoundException", "Table not found: " + tableName);
                return new TableSchema(tableName, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getLong(6));
            }
        } catch (SQLException e) {
            throw new RuntimeException("DescribeTable failed for " + tableName, e);
        }
    }

    public long itemCount(TableSchema schema) {
        long total = 0;
        try {
            for (Connection c : shardConnectionsForDdl()) {
                try (c; var st = c.createStatement();
                        ResultSet rs = st.executeQuery("SELECT count(*) FROM " + pgTableName(schema.tableName()))) {
                    rs.next();
                    total += rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count items in " + schema.tableName(), e);
        }
        return total;
    }

    public List<String> listTables() {
        List<String> out = new ArrayList<>();
        try (Connection c = borrowCatalogConnection(); var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT table_name FROM _dynamo_tables ORDER BY table_name")) {
            while (rs.next()) out.add(rs.getString(1));
        } catch (SQLException e) {
            throw new RuntimeException("ListTables failed", e);
        }
        return out;
    }

    private static String keyToken(Map<String, AttributeValue> item, String attr) {
        AttributeValue v = item.get(attr);
        if (v == null) throw new DynamoException("ValidationException", "Missing required key attribute: " + attr);
        return v.scalar;
    }

    public Map<String, AttributeValue> putItem(TableSchema schema, Map<String, AttributeValue> item, String conditionExpr, ExpressionContext ctx) {
        return putItem(schema, item, conditionExpr, ctx, true);
    }

    /**
     * @param needExisting whether the caller actually wants the pre-write item back (real
     *      DynamoDB only returns it when {@code ReturnValues=ALL_OLD} is requested). When a
     *      caller doesn't need it AND there's no condition expression to evaluate against it
     *      (the common case -- plain PutItem/BatchWriteItem with no ReturnValues), this skips the
     *      {@code SELECT ... FOR UPDATE} entirely: it was running unconditionally before, on
     *      every single PutItem, whether or not anything downstream ever looked at the result --
     *      found live comparing PutItem's cost against a plain SQL INSERT's (PutItem was running
     *      2 real round trips to INSERT's 1). A condition expression still forces the read, since
     *      evaluating it needs the existing row regardless of whether the caller asked for it back.
     */
    public Map<String, AttributeValue> putItem(TableSchema schema, Map<String, AttributeValue> item, String conditionExpr,
            ExpressionContext ctx, boolean needExisting) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(item, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(item, schema.sortKeyName()) : "";
        // A plain unconditional PutItem with nothing needing the pre-write row is a single
        // statement with no cross-statement consistency to protect -- explicit
        // setAutoCommit(false)+commit() around it was costing a real BEGIN/COMMIT round trip pair
        // for nothing, on top of the SELECT this same investigation already removed above. Only
        // pay for a real transaction when there's an actual read-then-write dependency (a
        // condition to evaluate, or the caller wants the old value back atomically with the write).
        boolean needsTransaction = conditionExpr != null || needExisting;
        try (Connection c = borrowShardConnection(pk)) {
            if (needsTransaction) {
                c.setAutoCommit(false);
            }
            try {
                Map<String, AttributeValue> existing = needsTransaction ? readForUpdate(c, pg, pk, sk) : null;
                if (conditionExpr != null && !ConditionExpressionEvaluator.evaluate(conditionExpr, existing, ctx)) {
                    c.rollback();
                    throw new DynamoException("ConditionalCheckFailedException", "The conditional request failed");
                }
                String json = itemToJson(item).toString();
                BigDecimal skNum = schema.hasSortKey() && "N".equals(schema.sortKeyType()) ? new BigDecimal(sk) : null;
                try (var ps = c.prepareStatement(
                        "INSERT INTO " + pg + " (pk_value, sk_value, sk_num, item) VALUES (?,?,?,?::jsonb) " +
                        "ON CONFLICT (pk_value, sk_value) DO UPDATE SET sk_num = EXCLUDED.sk_num, item = EXCLUDED.item")) {
                    ps.setString(1, pk);
                    ps.setString(2, sk);
                    if (skNum != null) ps.setBigDecimal(3, skNum); else ps.setNull(3, java.sql.Types.NUMERIC);
                    ps.setString(4, json);
                    ps.executeUpdate();
                }
                if (needsTransaction) {
                    c.commit();
                }
                return existing;
            } catch (RuntimeException | SQLException e) {
                if (needsTransaction) {
                    c.rollback();
                }
                if (e instanceof DynamoException de) throw de;
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("PutItem failed", e);
        }
    }

    private Map<String, AttributeValue> readForUpdate(Connection c, String pg, String pk, String sk) throws SQLException {
        try (var ps = c.prepareStatement("SELECT item FROM " + pg + " WHERE pk_value = ? AND sk_value = ? FOR UPDATE")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
            }
        }
    }

    public Map<String, AttributeValue> getItem(TableSchema schema, Map<String, AttributeValue> key) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(key, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(key, schema.sortKeyName()) : "";
        try (Connection c = borrowShardConnection(pk);
                var ps = c.prepareStatement("SELECT item FROM " + pg + " WHERE pk_value = ? AND sk_value = ?")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
            }
        } catch (SQLException e) {
            throw new RuntimeException("GetItem failed", e);
        }
    }

    public Map<String, AttributeValue> deleteItem(TableSchema schema, Map<String, AttributeValue> key, String conditionExpr, ExpressionContext ctx) {
        return deleteItem(schema, key, conditionExpr, ctx, true);
    }

    /** @param needExisting see {@link #putItem}'s javadoc -- same reasoning, same fix. */
    public Map<String, AttributeValue> deleteItem(TableSchema schema, Map<String, AttributeValue> key, String conditionExpr,
            ExpressionContext ctx, boolean needExisting) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(key, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(key, schema.sortKeyName()) : "";
        boolean needsTransaction = conditionExpr != null || needExisting;
        try (Connection c = borrowShardConnection(pk)) {
            if (needsTransaction) {
                c.setAutoCommit(false);
            }
            try {
                Map<String, AttributeValue> existing = needsTransaction ? readForUpdate(c, pg, pk, sk) : null;
                if (conditionExpr != null && !ConditionExpressionEvaluator.evaluate(conditionExpr, existing, ctx)) {
                    c.rollback();
                    throw new DynamoException("ConditionalCheckFailedException", "The conditional request failed");
                }
                try (var ps = c.prepareStatement("DELETE FROM " + pg + " WHERE pk_value = ? AND sk_value = ?")) {
                    ps.setString(1, pk);
                    ps.setString(2, sk);
                    ps.executeUpdate();
                }
                if (needsTransaction) {
                    c.commit();
                }
                return existing;
            } catch (RuntimeException | SQLException e) {
                if (needsTransaction) {
                    c.rollback();
                }
                if (e instanceof DynamoException de) throw de;
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("DeleteItem failed", e);
        }
    }

    public Map<String, AttributeValue> updateItem(TableSchema schema, Map<String, AttributeValue> key, String updateExpr,
            String conditionExpr, ExpressionContext ctx) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(key, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(key, schema.sortKeyName()) : "";
        try (Connection c = borrowShardConnection(pk)) {
            c.setAutoCommit(false);
            try {
                Map<String, AttributeValue> existing = readForUpdate(c, pg, pk, sk);
                if (conditionExpr != null && !ConditionExpressionEvaluator.evaluate(conditionExpr, existing, ctx)) {
                    c.rollback();
                    throw new DynamoException("ConditionalCheckFailedException", "The conditional request failed");
                }
                Map<String, AttributeValue> item = existing != null ? new LinkedHashMap<>(existing) : new LinkedHashMap<>();

                item.putAll(key);
                UpdateExpressionParser.apply(updateExpr, item, ctx);
                item.putAll(key);
                String json = itemToJson(item).toString();
                BigDecimal skNum = schema.hasSortKey() && "N".equals(schema.sortKeyType()) ? new BigDecimal(sk) : null;
                try (var ps = c.prepareStatement(
                        "INSERT INTO " + pg + " (pk_value, sk_value, sk_num, item) VALUES (?,?,?,?::jsonb) " +
                        "ON CONFLICT (pk_value, sk_value) DO UPDATE SET sk_num = EXCLUDED.sk_num, item = EXCLUDED.item")) {
                    ps.setString(1, pk);
                    ps.setString(2, sk);
                    if (skNum != null) ps.setBigDecimal(3, skNum); else ps.setNull(3, java.sql.Types.NUMERIC);
                    ps.setString(4, json);
                    ps.executeUpdate();
                }
                c.commit();
                return item;
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                if (e instanceof DynamoException de) throw de;
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("UpdateItem failed", e);
        }
    }

    public record PageResult(List<Map<String, AttributeValue>> items, Map<String, AttributeValue> lastEvaluatedKey) {}

    public PageResult query(TableSchema schema, KeyConditionParser keyCond, ExpressionContext ctx,
            String filterExpr, Integer limit, Map<String, AttributeValue> exclusiveStartKey, boolean scanForward) {
        String pg = pgTableName(schema.tableName());
        AttributeValue pkVal = ctx.resolveValue(keyCond.partitionValueToken);
        StringBuilder sql = new StringBuilder("SELECT item, pk_value, sk_value FROM " + pg + " WHERE pk_value = ?");
        List<Object> params = new ArrayList<>();
        params.add(pkVal.scalar);
        appendSortCondition(sql, params, schema, keyCond, ctx);
        boolean numericSort = schema.hasSortKey() && "N".equals(schema.sortKeyType());
        String orderCol = numericSort ? "sk_num" : "sk_value";
        if (exclusiveStartKey != null && schema.hasSortKey()) {
            AttributeValue startSk = exclusiveStartKey.get(schema.sortKeyName());
            sql.append(scanForward ? " AND " + orderCol + " > ?" : " AND " + orderCol + " < ?");
            params.add(sortParam(schema, startSk));
        }
        sql.append(" ORDER BY ").append(orderCol).append(scanForward ? " ASC" : " DESC");
        // Query always has an equality partition-key condition -- it belongs on exactly one
        // shard, the same one PutItem for that partition key would have landed on.
        try {
            Connection c = borrowShardConnection(pkVal.scalar);
            return runAndFilter(List.of(c), pg, schema, sql.toString(), params, filterExpr, ctx, limit);
        } catch (SQLException e) {
            throw new RuntimeException("Query failed", e);
        }
    }

    private void appendSortCondition(StringBuilder sql, List<Object> params, TableSchema schema, KeyConditionParser kc, ExpressionContext ctx) {
        String skCol = schema.hasSortKey() && "N".equals(schema.sortKeyType()) ? "sk_num" : "sk_value";
        switch (kc.sortOp) {
            case NONE -> {}
            case EQ -> { sql.append(" AND ").append(skCol).append(" = ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case LT -> { sql.append(" AND ").append(skCol).append(" < ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case LE -> { sql.append(" AND ").append(skCol).append(" <= ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case GT -> { sql.append(" AND ").append(skCol).append(" > ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case GE -> { sql.append(" AND ").append(skCol).append(" >= ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case BETWEEN -> {
                sql.append(" AND ").append(skCol).append(" BETWEEN ? AND ?");
                params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken)));
                params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken2)));
            }
            case BEGINS_WITH -> { sql.append(" AND sk_value LIKE ?"); params.add(escapeLike(ctx.resolveValue(kc.sortValueToken).scalar) + "%"); }
        }
    }

    private Object sortParam(TableSchema schema, AttributeValue v) {
        return "N".equals(schema.sortKeyType()) ? new BigDecimal(v.scalar) : v.scalar;
    }

    private static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    /**
     * No partition key to route on -- scans every shard backend (just the one backend, in
     * unsharded mode) and concatenates. See class javadoc for the pagination caveat this implies.
     */
    public PageResult scan(TableSchema schema, String filterExpr, ExpressionContext ctx, Integer limit, Map<String, AttributeValue> exclusiveStartKey) {
        String pg = pgTableName(schema.tableName());
        StringBuilder sql = new StringBuilder("SELECT item, pk_value, sk_value FROM " + pg);
        List<Object> params = new ArrayList<>();
        if (exclusiveStartKey != null) {
            sql.append(" WHERE (pk_value, sk_value) > (?, ?)");
            params.add(exclusiveStartKey.get(schema.partitionKeyName()).scalar);
            params.add(schema.hasSortKey() ? exclusiveStartKey.get(schema.sortKeyName()).scalar : "");
        }
        sql.append(" ORDER BY pk_value, sk_value");
        try {
            List<Connection> connections = shardConnectionsForDdl();
            return runAndFilter(connections, pg, schema, sql.toString(), params, filterExpr, ctx, limit);
        } catch (SQLException e) {
            throw new RuntimeException("Scan failed", e);
        }
    }

    private PageResult runAndFilter(List<Connection> connections, String pg, TableSchema schema, String sql, List<Object> params, String filterExpr,
            ExpressionContext ctx, Integer limit) {
        List<Map<String, AttributeValue>> results = new ArrayList<>();
        Map<String, AttributeValue> lastKey = null;
        try {
            for (Connection c : connections) {
                try (c; PreparedStatement ps = c.prepareStatement(sql)) {
                    for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Map<String, AttributeValue> item = jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
                            if (filterExpr != null && !ConditionExpressionEvaluator.evaluate(filterExpr, item, ctx)) continue;
                            results.add(item);
                            if (limit != null && results.size() >= limit) {
                                lastKey = keyOf(schema, item);
                                if (!rs.next()) {
                                    lastKey = null;
                                }
                                return new PageResult(results, lastKey);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Query/Scan failed", e);
        }
        return new PageResult(results, lastKey);
    }

    private Map<String, AttributeValue> keyOf(TableSchema schema, Map<String, AttributeValue> item) {
        Map<String, AttributeValue> k = new LinkedHashMap<>();
        k.put(schema.partitionKeyName(), item.get(schema.partitionKeyName()));
        if (schema.hasSortKey()) k.put(schema.sortKeyName(), item.get(schema.sortKeyName()));
        return k;
    }

    /** Transactions stay single-shard -- resolved by the first write's partition key. */
    public <T> T runInTransaction(String anyPartitionKeyInTransaction, java.util.function.Function<Connection, T> work) {
        try (Connection c = borrowShardConnection(anyPartitionKeyInTransaction)) {
            c.setAutoCommit(false);
            try {
                T result = work.apply(c);
                c.commit();
                return result;
            } catch (RuntimeException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Transaction failed", e);
        }
    }

    public String tableToPgName(String tableName) {
        return pgTableName(tableName);
    }

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
