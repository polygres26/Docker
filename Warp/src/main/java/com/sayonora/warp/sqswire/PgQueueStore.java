package com.sayonora.warp.sqswire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.DdlTemplates;
import com.sayonora.warp.core.ShardingStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue storage for sqswire -- the same pgmq (github.com/pgmq/pgmq) table shape (a {@code vt}
 * visibility-timeout column plus {@code FOR UPDATE SKIP LOCKED} for ReceiveMessage) reimplemented
 * in plain SQL, so no {@code pgmq} extension needs to be installed on a Postgres backend, exactly
 * as dynamowire/mongowire don't require DynamoDB/MongoDB to exist.
 *
 * <p><b>Real backend engines beyond Postgres</b>: a shard group member can now genuinely be
 * Oracle, SQL Server, or MySQL/MariaDB too (see {@link com.sayonora.warp.core.BackendDriverRegistry}
 * for the currently-supported engine list) -- live-verified, full CreateQueue/SendMessage/
 * ReceiveMessage/DeleteMessage/ChangeMessageVisibility/GetQueueAttributes, including FIFO
 * group-exclusion and dedup, against real instances of all three. {@link SqswireDialect} carries
 * every real per-engine SQL difference (Postgres's own single-statement {@code UPDATE ...
 * RETURNING} claim stays untouched; the other three engines share one real, portable two-statement
 * claim pattern instead) -- see that class's own javadoc for the full design reasoning.
 *
 * <p><b>Sharding:</b> queues route by name across {@code backendRegistry.shardGroup()} -- the
 * same shard group SQL's value-shard rules and dynamowire/mongowire already use -- hashed via
 * {@link ShardingStrategy#hash}. One queue's table lives entirely on one resolved backend (unlike
 * dynamowire/mongowire, almost every SQS operation is already scoped to a single queue, so there's
 * no per-message shard key to hash independently). {@code ListQueues} is the one operation with no
 * single queue to route by, but unlike dynamowire's {@code Scan} / mongowire's non-{@code _id}
 * {@code find} it doesn't need to fan out across shard backends at all: the queue-attributes
 * catalog (below) is already the one stable, cluster-wide list of every queue name, so
 * {@code ListQueues} just reads that. A registry with an empty shard group behaves like a
 * single-backend store pointed at {@code BackendRegistry.DEFAULT_BACKEND_NAME}.
 *
 * <p><b>Queue attributes catalog:</b> {@code sqs_queues_catalog} holds each queue's visibility timeout
 * default, FIFO flag, and redrive policy (DLQ target + max receive count). Like dynamowire's
 * {@code _dynamo_tables}, this metadata needs one fixed home independent of shard-group
 * membership, so it always lives on {@code BackendRegistry.DEFAULT_BACKEND_NAME} -- see
 * {@link #currentCatalogBackendName()}'s javadoc for why using a shard-group member here broke
 * DescribeTable-equivalent lookups in dynamowire the first time that was tried.
 *
 * <p><b>Real bug, found live against Oracle</b>: this catalog table's name was originally
 * {@code _sqs_queues} (matching dynamowire's own {@code _dynamo_tables} convention) -- Oracle
 * rejects an unquoted identifier starting with {@code _} outright ({@code ORA-00911: invalid
 * character after TABLE}), and quoting it consistently isn't a safe portable fix either: MySQL
 * without {@code ANSI_QUOTES} (not in its own default {@code sql_mode}) treats a double-quoted
 * token as a string literal, not an identifier, so a quoted name that works on Oracle/Postgres/
 * SQL Server breaks MySQL. Renamed to {@code sqs_queues_catalog} instead -- valid, unquoted, on
 * all four engines -- rather than adding a fourth per-engine quoting/naming special case.
 *
 * <p><b>FIFO queues</b> (name ends {@code .fifo}): {@code SendMessage}'s {@code dedup_id} is
 * deduplicated against the last 5 minutes (SQS's own dedup window) for the same queue -- a resend
 * with the same id returns the original message id rather than inserting a duplicate.
 * {@code ReceiveMessage} additionally never claims a message from a {@code message_group_id} that
 * already has another message in flight (visibility timeout hasn't expired), matching SQS FIFO's
 * per-group ordering guarantee.
 *
 * <p><b>Dead-letter queues:</b> when a queue's redrive policy sets a {@code maxReceiveCount}, a
 * message that would be claimed again after already having been received that many times is
 * instead moved to the configured DLQ's table (never returned to the caller) -- the same redrive
 * semantics real SQS provides, driven by the {@code read_ct} column ReceiveMessage already
 * maintains.
 *
 * <p><b>Live-reloadable:</b> the shard group is re-read from {@code backendRegistry} on every
 * call, not captured once at construction, so a {@code WARP_BACKENDS}/
 * {@code WARP_SHARD_BACKENDS} change takes effect on this store's very next operation.
 *
 * <p><b>Known limitation:</b> turning sharding on (or changing the shard group) does not migrate
 * existing queues' physical tables -- a queue keeps living on whichever backend it was created on
 * until it's recreated. This mirrors the same caveat already documented for dynamowire/mongowire.
 */
public final class PgQueueStore {

    private static final Logger log = LoggerFactory.getLogger(PgQueueStore.class);
    // Postgres unquoted identifiers can't contain '-' or '.' (both legal in SQS queue names,
    // e.g. "my-queue" or "my-queue.fifo") -- fold them to '_' rather than quoting the identifier,
    // to keep every generated SQL string above simple and injection-safe by construction.
    private static final Pattern SAFE_QUEUE_NAME = Pattern.compile("[^a-zA-Z0-9_]");
    private static final int FIFO_DEDUP_WINDOW_SECONDS = 300;

    private final HikariDataSource legacyDs;
    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> tableEnsured = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> catalogEnsured = new ConcurrentHashMap<>();
    private volatile List<String> lastLoggedShardGroup = null;


    public PgQueueStore(String host, int port, String database, String user, String password) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + database);
        if (user != null) cfg.setUsername(user);
        if (password != null) cfg.setPassword(password);
        cfg.setPoolName("sqswire-pg-pool");
        cfg.setMaximumPoolSize(8);
        this.legacyDs = new HikariDataSource(cfg);
        this.backendRegistry = null;
        ensureCatalogEagerly();
    }

    public PgQueueStore(BackendRegistry backendRegistry) {
        this.legacyDs = null;
        this.backendRegistry = backendRegistry;
        logShardGroupIfChanged();
        // Registry mode's own borrowCatalogConnection() already calls ensureCatalog on every
        // resolution (see its javadoc) -- this eager call is only load-bearing for legacy mode,
        // where borrowCatalogConnection() never calls it since there's only ever one fixed
        // backend. Harmless (and cheap -- ensureCatalog is cached per URL) to call unconditionally
        // either way, so both constructors share it rather than duplicating the branch.
        ensureCatalogEagerly();
    }

    private void ensureCatalogEagerly() {
        try (Connection conn = legacyDs != null ? legacyDs.getConnection() : borrowCatalogConnection()) {
            ensureCatalog(conn);
        } catch (SQLException e) {
            throw new RuntimeException("sqswire: failed to create sqs_queues_catalog catalog", e);
        }
    }

    private List<String> currentShardGroup() {
        return backendRegistry == null ? List.of()
                : backendRegistry.storeShardGroup(com.sayonora.warp.core.StoreType.SQS);
    }

    private void logShardGroupIfChanged() {
        List<String> group = currentShardGroup();
        if (!group.equals(lastLoggedShardGroup)) {
            lastLoggedShardGroup = group;
            if (group.isEmpty()) {
                log.info("sqswire: no shard group configured -- every queue lives on the default backend");
            } else {
                log.info("sqswire: sharding queues by name across {} backend(s): {}", group.size(), group);
            }
        }
    }

    /** Which backend a given queue name currently routes to -- no connection opened. */
    public String resolveBackendFor(String queueName) {
        List<String> group = currentShardGroup();
        if (group.isEmpty()) {
            return BackendRegistry.DEFAULT_BACKEND_NAME;
        }
        return ShardingStrategy.hash(group).resolve(queueName);
    }

    private Connection connectionFor(String queueName) throws SQLException {
        if (legacyDs != null) {
            return legacyDs.getConnection();
        }
        logShardGroupIfChanged();
        String backendName = resolveBackendFor(queueName);
        // resolveForRouting, not get -- see BackendRegistry.resolveForRouting's javadoc.
        BackendTarget target = backendRegistry.resolveForRouting(backendName);
        if (target == null) {
            target = backendRegistry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
        }
        if (target == null) {
            throw new IllegalStateException("sqswire: no backend named \"" + backendName
                    + "\" (or a \"" + BackendRegistry.DEFAULT_BACKEND_NAME + "\" fallback) is registered");
        }
        return target.open();
    }

    /**
     * The registry's stable {@code DEFAULT_BACKEND_NAME} when it's actually registered --
     * {@code sqs_queues_catalog} needs one fixed home that doesn't move when the shard group is
     * reconfigured (mirrors {@code PgItemStore#currentCatalogBackendName}, including the
     * fallback for a deployment that shards without an explicit {@code default=...} entry).
     */
    private Connection borrowCatalogConnection() {
        try {
            if (legacyDs != null) {
                return legacyDs.getConnection();
            }
            // SQS enabled on backend(s) of the frontend's set: the queue catalog lives on the first host
            String home = backendRegistry.storeHome(com.sayonora.warp.core.StoreType.SQS);
            BackendTarget target = backendRegistry.resolveForRouting(
                    home != null ? home : BackendRegistry.DEFAULT_BACKEND_NAME);
            if (target == null) {
                List<String> group = currentShardGroup();
                if (!group.isEmpty()) {
                    log.warn("sqswire: \"{}\" is not a configured backend -- falling back to \"{}\" for the "
                            + "queue-attributes catalog. Configure a \"default=...\" backend entry to avoid this.",
                            BackendRegistry.DEFAULT_BACKEND_NAME, group.get(0));
                    target = backendRegistry.resolveForRouting(group.get(0));
                }
            }
            if (target == null) {
                throw new IllegalStateException("sqswire: no \"" + BackendRegistry.DEFAULT_BACKEND_NAME + "\" backend registered");
            }
            Connection conn = target.open();
            ensureCatalog(conn);
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException("sqswire: failed to open catalog connection", e);
        }
    }

    /** Idempotent per physical backend (cached by JDBC URL, same technique as {@link
     * #ensureTable}) -- called from {@link #borrowCatalogConnection} on every resolution, not just
     * once at construction, so a switchover's fallback (a genuinely separate Postgres, not
     * necessarily a replica sharing the primary's schema) gets the catalog table the first time
     * routing actually lands there, not never. Does NOT close {@code conn} -- unlike the old
     * construction-time-only call, the caller (borrowCatalogConnection) owns and returns it for
     * real use. */
    private void ensureCatalog(Connection conn) throws SQLException {
        String key = conn.getMetaData().getURL();
        if (Boolean.TRUE.equals(catalogEnsured.get(key))) {
            return;
        }
        // Real DDL, loaded from ddl/<engine>/sqswire_catalog.sql -- see DdlTemplates' own javadoc.
        String engine = engineOf(conn);
        try (var st = conn.createStatement()) {
            for (String statement : DdlTemplates.loadStatements(engine, "sqswire_catalog", Map.of())) {
                executeIdempotently(st, statement, engine);
            }
        }
        catalogEnsured.put(key, Boolean.TRUE);
    }

    /** Real bug, found live: {@code catalogEnsured}/{@code tableEnsured} are per-{@code
     * PgQueueStore}-instance caches, not persisted anywhere durable -- a config hot-reload (a
     * fresh {@code PgQueueStore} construction, same as the drain-routing tests exercise) starts
     * both empty again, so the CREATE this method guards can run a second time against a schema
     * that already has the object from a previous instance's run. Harmless on Postgres/MySQL,
     * which use a real {@code CREATE TABLE IF NOT EXISTS}. Oracle (pre-23c) and SQL Server have
     * no {@code IF NOT EXISTS} on {@code CREATE TABLE}/{@code CREATE INDEX} at all -- their own
     * re-run throws a real "already exists" error (Oracle {@code ORA-00955}, SQL Server error
     * 2714), caught here exactly like {@code deleteQueue}'s own {@code ORA-00942} catch for the
     * same underlying "no IF [NOT] EXISTS" gap, just on the create side instead of the drop
     * side. */
    private static void executeIdempotently(java.sql.Statement st, String statement, String engine) throws SQLException {
        if (!"oracle".equals(engine) && !"sqlserver".equals(engine)) {
            st.execute(statement);
            return;
        }
        try {
            st.execute(statement);
        } catch (SQLException e) {
            boolean alreadyExists = "oracle".equals(engine)
                    ? ("955".equals(e.getSQLState()) || e.getErrorCode() == 955)
                    : e.getErrorCode() == 2714;
            if (!alreadyExists) {
                throw e;
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Identity (ARNs), wake-up hook, table naming
    // ------------------------------------------------------------------------------------------

    private volatile String region = "us-east-1";
    private volatile String accountId = "000000000000";
    private volatile Consumer<String> enqueueListener = q -> { };
    private final ConcurrentHashMap<String, String> engineByUrl = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Cached> cache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_NANOS = 5_000_000_000L;

    private record Cached(QueueAttributes attrs, long loadedNanos) {
    }

    public void configureIdentity(String region, String accountId) {
        this.region = region;
        this.accountId = accountId;
    }

    public String region() {
        return region;
    }

    public String accountId() {
        return accountId;
    }

    public String queueArn(String queueName) {
        return "arn:aws:sqs:" + region + ":" + accountId + ":" + queueName;
    }

    /** The queue name inside an SQS ARN, or {@code null} if it is not an SQS queue ARN. */
    public static String queueNameFromArn(String arn) {
        if (arn == null) {
            return null;
        }
        String[] p = arn.split(":", -1);
        if (p.length != 6 || !"arn".equals(p[0]) || !"sqs".equals(p[2]) || p[5].isEmpty()) {
            return null;
        }
        return p[5];
    }

    /** Called with a queue name whenever messages become receivable on it (sends, redrive, moves). */
    public void setEnqueueListener(Consumer<String> listener) {
        this.enqueueListener = listener == null ? q -> { } : listener;
    }

    private static String shortHash(String s) {
        return SqsMessageAttributes.sha256Hex(s).substring(0, 8);
    }

    /** The physical table a queue created before per-queue table names were recorded lives in. */
    private static String legacyTableName(String queueName) {
        return "sqs_queue_" + SAFE_QUEUE_NAME.matcher(queueName).replaceAll("_").toLowerCase(java.util.Locale.ROOT);
    }

    static String baseTableName(String queueName) {
        String s = SAFE_QUEUE_NAME.matcher(queueName).replaceAll("_").toLowerCase(java.util.Locale.ROOT);
        if (s.length() > 40) {
            s = s.substring(0, 31) + "_" + shortHash(queueName);
        }
        return "sqs_queue_" + s;
    }

    private static String safeTableName(String queueName) {
        return legacyTableName(queueName);
    }

    private static String tableOf(String queueName, QueueAttributes a) {
        return a != null && a.tableName() != null ? a.tableName() : legacyTableName(queueName);
    }

    private static String dedupTable(String table) {
        return table + "_dd";
    }

    /**
     * @return the real {@code ddl/<engine>/} directory name for {@code conn}'s own backend --
     *     see {@link DdlTemplates#engineDirFor}/{@link SqswireDialect}'s own javadoc. Throws a
     *     real, clear error for an unrecognized engine rather than silently guessing Postgres.
     */
    private String engineOf(Connection conn) throws SQLException {
        String url = conn.getMetaData().getURL();
        String cached = engineByUrl.get(url);
        if (cached != null) {
            return cached;
        }
        String engine = DdlTemplates.engineDirFor(url);
        if (engine == null) {
            throw new SQLException("sqswire: no real DDL/query support for this backend's own engine "
                    + "(jdbcUrl=" + url + ") -- see BackendDriverRegistry for the currently-supported engine list");
        }
        engineByUrl.put(url, engine);
        return engine;
    }

    private void ensureTable(Connection conn, String queueName, String table) throws SQLException {
        ensureTable(conn, queueName, table, false);
    }

    /**
     * {@code force} (queue creation) re-runs the idempotent DDL even if this process already ensured the table;
     * every other caller trusts the cache, so an operation racing a DeleteQueue fails with "table does not exist"
     * (mapped to QueueDoesNotExist) instead of silently re-creating the dropped table.
     */
    private void ensureTable(Connection conn, String queueName, String table, boolean force) throws SQLException {
        String key = table + "@" + conn.getMetaData().getURL();
        if (!force && Boolean.TRUE.equals(tableEnsured.get(key))) {
            return;
        }
        // Real DDL, loaded from ddl/<engine>/sqswire_queue_table.sql -- see SqswireDialect's own
        // javadoc for the full real per-engine query-portability design this DDL supports. For
        // Postgres the same file also adds (idempotently) the columns/tables newer features need,
        // so queues created by an older Warp are upgraded in place on first use.
        String engine = engineOf(conn);
        try (var st = conn.createStatement()) {
            for (String statement : DdlTemplates.loadStatements(engine, "sqswire_queue_table", Map.of("table", table))) {
                executeIdempotently(st, statement, engine);
            }
        }
        tableEnsured.put(key, Boolean.TRUE);
    }

    // ------------------------------------------------------------------------------------------
    // Queue attributes (catalog)
    // ------------------------------------------------------------------------------------------

    /**
     * Everything the catalog knows about a queue. {@code attributes} holds the settable SQS attributes as
     * their string values (VisibilityTimeout, DelaySeconds, MaximumMessageSize, MessageRetentionPeriod,
     * ReceiveMessageWaitTimeSeconds, RedrivePolicy, RedriveAllowPolicy, Policy, ContentBasedDeduplication,
     * DeduplicationScope, FifoThroughputLimit, SqsManagedSseEnabled, KmsMasterKeyId, ...);
     * {@code tableName} is the physical table ({@code null} for queues created before it was recorded).
     */
    public record QueueAttributes(int visibilityTimeout, boolean fifo, String dlqQueueName, Integer maxReceiveCount,
            Map<String, String> attributes, Map<String, String> tags, long createdMs, long modifiedMs,
            String tableName) {
        static final QueueAttributes DEFAULTS = new QueueAttributes(30, false, null, null);

        public QueueAttributes(int visibilityTimeout, boolean fifo, String dlqQueueName, Integer maxReceiveCount) {
            this(visibilityTimeout, fifo, dlqQueueName, maxReceiveCount, Map.of(), Map.of(), 0L, 0L, null);
        }

        public String attr(String name) {
            return attributes.get(name);
        }

        public int intAttr(String name, int def) {
            String v = attributes.get(name);
            if (v == null || v.isEmpty()) {
                return def;
            }
            try {
                return Integer.parseInt(v.trim());
            } catch (NumberFormatException e) {
                return def;
            }
        }

        public boolean boolAttr(String name) {
            return "true".equalsIgnoreCase(attributes.get(name));
        }

        public int delaySeconds() {
            return intAttr("DelaySeconds", 0);
        }

        public int maxMessageSize() {
            return intAttr("MaximumMessageSize", 262144);
        }

        public int retentionSeconds() {
            return intAttr("MessageRetentionPeriod", 345600);
        }

        public int waitSeconds() {
            return intAttr("ReceiveMessageWaitTimeSeconds", 0);
        }
    }

    /** The queue's attributes, or {@code null} when no such queue exists (cached for a few seconds). */
    public QueueAttributes findQueue(String queueName) throws SQLException {
        Cached c = cache.get(queueName);
        if (c != null && System.nanoTime() - c.loadedNanos() < CACHE_TTL_NANOS) {
            return c.attrs();
        }
        return findQueueFresh(queueName);
    }

    /** Bypasses the cache -- used before read-modify-write of attributes/tags/policy. */
    public QueueAttributes findQueueFresh(String queueName) throws SQLException {
        QueueAttributes loaded = loadQueueAttributes(queueName);
        if (loaded == null) {
            cache.remove(queueName);
        } else {
            cache.put(queueName, new Cached(loaded, System.nanoTime()));
        }
        return loaded;
    }

    public QueueAttributes requireQueue(String queueName) throws SQLException {
        QueueAttributes a = findQueue(queueName);
        if (a == null) {
            throw SqsException.noQueue();
        }
        return a;
    }

    /** Legacy accessor: attributes, or defaults for an unknown queue. */
    public QueueAttributes queueAttributes(String queueName) throws SQLException {
        QueueAttributes a = findQueue(queueName);
        return a == null ? QueueAttributes.DEFAULTS : a;
    }

    private QueueAttributes loadQueueAttributes(String queueName) throws SQLException {
        try (Connection cat = borrowCatalogConnection()) {
            boolean pg = "postgres".equals(engineOf(cat));
            String sql = "SELECT visibility_timeout, is_fifo, dlq_queue_name, max_receive_count"
                    + (pg ? ", attributes_json, tags_json, created_at, modified_at, table_name" : "")
                    + " FROM sqs_queues_catalog WHERE queue_name = ?";
            try (PreparedStatement ps = cat.prepareStatement(sql)) {
                ps.setString(1, queueName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    Integer maxReceive = rs.getObject("max_receive_count") == null ? null : rs.getInt("max_receive_count");
                    int vt = rs.getInt("visibility_timeout");
                    boolean fifo = rs.getBoolean("is_fifo");
                    Map<String, String> attrs = new LinkedHashMap<>();
                    Map<String, String> tags = new LinkedHashMap<>();
                    long created = 0;
                    long modified = 0;
                    String table = null;
                    if (pg) {
                        readStringMap(rs.getString("attributes_json"), attrs);
                        readStringMap(rs.getString("tags_json"), tags);
                        created = rs.getLong("created_at");
                        modified = rs.getLong("modified_at");
                        table = rs.getString("table_name");
                    }
                    attrs.putIfAbsent("VisibilityTimeout", String.valueOf(vt));
                    return new QueueAttributes(vt, fifo, rs.getString("dlq_queue_name"), maxReceive,
                            attrs, tags, created, modified, table);
                }
            }
        }
    }

    private static void readStringMap(String json, Map<String, String> into) {
        if (json == null || json.isBlank()) {
            return;
        }
        for (var e : JsonParser.parseString(json).getAsJsonObject().entrySet()) {
            into.put(e.getKey(), e.getValue().getAsString());
        }
    }

    private static String toJson(Map<String, String> m) {
        JsonObject o = new JsonObject();
        m.forEach(o::addProperty);
        return o.toString();
    }

    /** Picks the physical table for a new queue: the base name unless another queue already claims it. */
    private String chooseTableName(Connection cat, String queueName) throws SQLException {
        String candidate = baseTableName(queueName);
        try (PreparedStatement ps = cat.prepareStatement(
                "SELECT 1 FROM sqs_queues_catalog WHERE queue_name <> ? AND (table_name = ? OR (table_name IS NULL AND "
                        + "'sqs_queue_' || lower(regexp_replace(queue_name, '[^a-zA-Z0-9_]', '_', 'g')) = ?)) LIMIT 1")) {
            ps.setString(1, queueName);
            ps.setString(2, candidate);
            ps.setString(3, candidate);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return candidate + "_" + shortHash(queueName);
                }
            }
        }
        return candidate;
    }

    /**
     * Creates the queue (table + catalog row) unless one with this name already exists.
     *
     * @return true when this call created it, false when it already existed (nothing changed)
     */
    public boolean createQueueIfAbsent(String queueName, QueueAttributes attrs) throws SQLException {
        try (Connection cat = borrowCatalogConnection()) {
            if (!"postgres".equals(engineOf(cat))) {
                if (findQueueFresh(queueName) != null) {
                    return false;
                }
                createQueue(queueName, attrs);
                return true;
            }
            String table = chooseTableName(cat, queueName);
            long now = System.currentTimeMillis();
            boolean inserted;
            try (PreparedStatement ps = cat.prepareStatement(
                    "INSERT INTO sqs_queues_catalog (queue_name, visibility_timeout, is_fifo, dlq_queue_name, max_receive_count, "
                            + "attributes_json, tags_json, created_at, modified_at, table_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                            + "ON CONFLICT (queue_name) DO NOTHING")) {
                bindCatalog(ps, queueName, attrs);
                ps.setLong(8, now);
                ps.setLong(9, now);
                ps.setString(10, table);
                inserted = ps.executeUpdate() > 0;
            }
            if (!inserted) {
                return false;
            }
            try (Connection conn = connectionFor(queueName)) {
                ensureTable(conn, queueName, table, true);
            }
            cache.remove(queueName);
            return true;
        }
    }

    private static void bindCatalog(PreparedStatement ps, String queueName, QueueAttributes attrs) throws SQLException {
        ps.setString(1, queueName);
        ps.setInt(2, attrs.visibilityTimeout());
        ps.setBoolean(3, attrs.fifo());
        ps.setString(4, attrs.dlqQueueName());
        if (attrs.maxReceiveCount() == null) {
            ps.setNull(5, java.sql.Types.INTEGER);
        } else {
            ps.setInt(5, attrs.maxReceiveCount());
        }
        ps.setString(6, toJson(attrs.attributes()));
        ps.setString(7, toJson(attrs.tags()));
    }

    /** Persists changed attributes/tags for an existing queue (keeps its creation time and table). */
    public void updateQueue(String queueName, QueueAttributes attrs) throws SQLException {
        try (Connection cat = borrowCatalogConnection()) {
            if (!"postgres".equals(engineOf(cat))) {
                createQueue(queueName, attrs);
                return;
            }
            try (PreparedStatement ps = cat.prepareStatement(
                    "UPDATE sqs_queues_catalog SET visibility_timeout = ?, is_fifo = ?, dlq_queue_name = ?, max_receive_count = ?, "
                            + "attributes_json = ?, tags_json = ?, modified_at = ? WHERE queue_name = ?")) {
                ps.setInt(1, attrs.visibilityTimeout());
                ps.setBoolean(2, attrs.fifo());
                ps.setString(3, attrs.dlqQueueName());
                if (attrs.maxReceiveCount() == null) {
                    ps.setNull(4, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(4, attrs.maxReceiveCount());
                }
                ps.setString(5, toJson(attrs.attributes()));
                ps.setString(6, toJson(attrs.tags()));
                ps.setLong(7, System.currentTimeMillis());
                ps.setString(8, queueName);
                ps.executeUpdate();
            }
        }
        cache.remove(queueName);
    }

    /**
     * Legacy create/upsert (kept for existing callers and non-Postgres backends): creates the table and
     * upserts the four core catalog columns.
     */
    public void createQueue(String queueName, QueueAttributes attrs) throws SQLException {
        boolean isFifo = attrs.fifo() || queueName.endsWith(".fifo");
        try (Connection cat = borrowCatalogConnection()) {
            String engine = engineOf(cat);
            String table = tableOf(queueName, attrs);
            boolean pg = "postgres".equals(engine);
            if (pg && attrs.tableName() == null) {
                QueueAttributes existing = loadQueueAttributes(queueName);
                table = existing != null ? tableOf(queueName, existing) : chooseTableName(cat, queueName);
            }
            try (Connection conn = connectionFor(queueName)) {
                ensureTable(conn, queueName, table, true);
            }
            String upsertSql = pg
                    ? "INSERT INTO sqs_queues_catalog (queue_name, visibility_timeout, is_fifo, dlq_queue_name, max_receive_count, "
                            + "attributes_json, tags_json, created_at, modified_at, table_name) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (queue_name) DO UPDATE SET "
                            + "visibility_timeout = EXCLUDED.visibility_timeout, is_fifo = EXCLUDED.is_fifo, "
                            + "dlq_queue_name = EXCLUDED.dlq_queue_name, max_receive_count = EXCLUDED.max_receive_count, "
                            + "modified_at = EXCLUDED.modified_at"
                    : SqswireDialect.catalogUpsertSql(engine);
            try (PreparedStatement ps = cat.prepareStatement(upsertSql)) {
                if (pg) {
                    bindCatalog(ps, queueName, attrs);
                    ps.setBoolean(3, isFifo);
                    long now = System.currentTimeMillis();
                    ps.setLong(8, now);
                    ps.setLong(9, now);
                    ps.setString(10, table);
                } else {
                    ps.setString(1, queueName);
                    ps.setInt(2, attrs.visibilityTimeout());
                    SqswireDialect.bindIsFifo(ps, 3, isFifo, engine);
                    ps.setString(4, attrs.dlqQueueName());
                    if (attrs.maxReceiveCount() == null) {
                        ps.setNull(5, java.sql.Types.INTEGER);
                    } else {
                        ps.setInt(5, attrs.maxReceiveCount());
                    }
                }
                ps.executeUpdate();
            }
        }
        cache.remove(queueName);
    }

    public void setQueueAttributes(String queueName, QueueAttributes attrs) throws SQLException {
        createQueue(queueName, attrs);
    }

    public void deleteQueue(String queueName) throws SQLException {
        QueueAttributes attrs = findQueueFresh(queueName);
        String table = tableOf(queueName, attrs);
        try (Connection conn = connectionFor(queueName); var st = conn.createStatement()) {
            // Real per-engine gap: Oracle (pre-23c) has no "DROP TABLE IF EXISTS" at all -- a
            // plain DROP TABLE throws ORA-00942 for a table that's already gone (or was never
            // created, e.g. DeleteQueue on a queue that never had a message sent to it), which
            // this treats the same way IF EXISTS would on the other 3 engines: not an error.
            if ("oracle".equals(engineOf(conn))) {
                try {
                    st.execute("DROP TABLE " + table);
                } catch (SQLException e) {
                    if (!"942".equals(e.getSQLState()) && e.getErrorCode() != 942) {
                        throw e;
                    }
                }
            } else {
                st.execute("DROP TABLE IF EXISTS " + table);
                if ("postgres".equals(engineOf(conn))) {
                    st.execute("DROP TABLE IF EXISTS " + dedupTable(table));
                }
            }
            // tableEnsured is deliberately NOT cleared: creating a queue forces its DDL, and operations racing this
            // delete must fail (undefined_table -> QueueDoesNotExist) rather than re-create the table.
        }
        try (Connection cat = borrowCatalogConnection();
                PreparedStatement ps = cat.prepareStatement("DELETE FROM sqs_queues_catalog WHERE queue_name = ?")) {
            ps.setString(1, queueName);
            ps.executeUpdate();
        } finally {
            cache.remove(queueName);
        }
    }

    /** Every queue name (used by the admin API and the sweeper). */
    public List<String> listQueues() throws SQLException {
        return listQueues(null, Integer.MAX_VALUE, null);
    }

    /**
     * Reads the catalog rather than scanning every shard backend -- the catalog is the one stable,
     * cluster-wide source of queue names (see the class javadoc), and unlike a table-name-derived list
     * it preserves the real queue name. Sorted by name; {@code afterName} is the pagination cursor.
     */
    public List<String> listQueues(String prefix, int limit, String afterName) throws SQLException {
        List<String> names = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT queue_name FROM sqs_queues_catalog WHERE 1 = 1");
        List<String> binds = new ArrayList<>();
        if (afterName != null) {
            sql.append(" AND queue_name > ?");
            binds.add(afterName);
        }
        if (prefix != null && !prefix.isEmpty()) {
            sql.append(" AND queue_name LIKE ? ESCAPE '!'");
            binds.add(prefix.replace("!", "!!").replace("%", "!%").replace("_", "!_") + "%");
        }
        sql.append(" ORDER BY queue_name");
        try (Connection cat = borrowCatalogConnection(); PreparedStatement ps = cat.prepareStatement(sql.toString())) {
            for (int i = 0; i < binds.size(); i++) {
                ps.setString(i + 1, binds.get(i));
            }
            if (limit < Integer.MAX_VALUE) {
                ps.setMaxRows(limit);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        }
        return names;
    }

    /** Names of queues whose redrive policy targets {@code dlqName}, sorted, after the cursor. */
    public List<String> listDeadLetterSources(String dlqName, int limit, String afterName) throws SQLException {
        List<String> names = new ArrayList<>();
        try (Connection cat = borrowCatalogConnection();
                PreparedStatement ps = cat.prepareStatement("SELECT queue_name FROM sqs_queues_catalog WHERE dlq_queue_name = ? "
                        + (afterName != null ? "AND queue_name > ? " : "") + "ORDER BY queue_name")) {
            ps.setString(1, dlqName);
            if (afterName != null) {
                ps.setString(2, afterName);
            }
            ps.setMaxRows(limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        }
        return names;
    }

    // ------------------------------------------------------------------------------------------
    // SendMessage
    // ------------------------------------------------------------------------------------------

    public record NewMessage(String body, String groupId, String dedupId, int delaySeconds, String attrsJson,
            String traceHeader) {
    }

    /** {@code sequence} is the queue's insertion counter (the FIFO SequenceNumber; also the row id of the message). */
    public record SendResult(String messageId, long sequence, boolean duplicate) {
    }

    /** Legacy single-message send (body, optional group/dedup id); returns the message's row id. */
    public long sendMessage(String queueName, String body, String messageGroupId, String dedupId) throws SQLException {
        return send(queueName, queueAttributes(queueName), new NewMessage(body, messageGroupId, dedupId, 0, null, null)).sequence();
    }

    public SendResult send(String queueName, QueueAttributes attrs, NewMessage msg) throws SQLException {
        return sendMany(queueName, attrs, List.of(msg)).get(0);
    }

    /**
     * Sends messages in order on one connection. Standard queues are a single INSERT each. FIFO queues
     * deduplicate against a per-queue dedup table (5-minute window, survives the message being deleted,
     * like real SQS) and run inside a transaction with a per-queue advisory lock so concurrent duplicates
     * cannot both be inserted.
     */
    public List<SendResult> sendMany(String queueName, QueueAttributes attrs, List<NewMessage> msgs) throws SQLException {
        List<SendResult> out = new ArrayList<>(msgs.size());
        String table = tableOf(queueName, attrs);
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName, table);
            String engine = engineOf(conn);
            for (NewMessage m : msgs) {
                if (!"postgres".equals(engine)) {
                    if (m.attrsJson() != null || m.traceHeader() != null || m.delaySeconds() > 0) {
                        throw new SqsException(400, "UnsupportedOperation", "Message attributes, delays and trace headers "
                                + "require a Postgres backend for this queue (backend engine: " + engine + ").");
                    }
                    long id = legacySend(conn, engine, table, m.body(), m.groupId(), m.dedupId());
                    out.add(new SendResult(String.valueOf(id), id, false));
                } else if (attrs.fifo()) {
                    out.add(sendFifo(conn, table, attrs, m));
                } else {
                    out.add(sendStandard(conn, table, m));
                }
            }
        }
        enqueueListener.accept(queueName);
        return out;
    }

    private static SendResult sendStandard(Connection conn, String table, NewMessage m) throws SQLException {
        String messageId = UUID.randomUUID().toString();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + table
                + " (message_id, body, message_group_id, dedup_id, attrs, trace_header, vt) "
                + "VALUES (?, ?, ?, ?, ?, ?, now() + ? * interval '1 second') RETURNING msg_id")) {
            ps.setString(1, messageId);
            ps.setString(2, m.body());
            ps.setString(3, m.groupId());
            ps.setString(4, m.dedupId());
            ps.setString(5, m.attrsJson());
            ps.setString(6, m.traceHeader());
            ps.setInt(7, m.delaySeconds());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return new SendResult(messageId, rs.getLong(1), false);
            }
        }
    }

    private static SendResult sendFifo(Connection conn, String table, QueueAttributes attrs, NewMessage m) throws SQLException {
        boolean groupScoped = "messageGroup".equalsIgnoreCase(attrs.attr("DeduplicationScope"));
        String dedupKey = groupScoped ? m.groupId().length() + ":" + m.groupId() + ":" + m.dedupId() : m.dedupId();
        String dd = dedupTable(table);
        String messageId = UUID.randomUUID().toString();
        boolean auto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            String sql = "WITH n AS (SELECT nextval(pg_get_serial_sequence(?, 'msg_id')) AS id), "
                    + "d AS (INSERT INTO " + dd + " (dedup_key, message_id, seq) SELECT ?, ?, n.id FROM n "
                    + "ON CONFLICT (dedup_key) DO UPDATE SET message_id = EXCLUDED.message_id, seq = EXCLUDED.seq, created_at = now() "
                    + "WHERE " + dd + ".created_at < now() - interval '" + FIFO_DEDUP_WINDOW_SECONDS + " seconds' "
                    + "RETURNING message_id, seq), "
                    + "m AS (INSERT INTO " + table + " (msg_id, message_id, body, message_group_id, dedup_id, attrs, trace_header, vt) "
                    + "SELECT d.seq, d.message_id, ?, ?, ?, ?, ?, now() + ? * interval '1 second' FROM d RETURNING msg_id) "
                    + "SELECT d.message_id, d.seq FROM d";
            SendResult result = null;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, table);
                ps.setString(2, dedupKey);
                ps.setString(3, messageId);
                ps.setString(4, m.body());
                ps.setString(5, m.groupId());
                ps.setString(6, m.dedupId());
                ps.setString(7, m.attrsJson());
                ps.setString(8, m.traceHeader());
                ps.setInt(9, m.delaySeconds());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        result = new SendResult(rs.getString(1), rs.getLong(2), false);
                    }
                }
            }
            if (result == null) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT message_id, seq FROM " + dd + " WHERE dedup_key = ?")) {
                    ps.setString(1, dedupKey);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        result = new SendResult(rs.getString(1), rs.getLong(2), true);
                    }
                }
            }
            conn.commit();
            return result;
        } catch (SQLException | RuntimeException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(auto);
        }
    }

    /** Legacy send (Oracle/SQL Server/MySQL backends, and pre-attribute callers). */
    private long legacySend(Connection conn, String engine, String table, String body, String messageGroupId, String dedupId)
            throws SQLException {
        if (dedupId != null) {
            String dedupSql = "postgres".equals(engine)
                    ? "SELECT msg_id FROM " + table + " WHERE dedup_id = ? AND enqueued_at > now() - (? || ' seconds')::interval "
                            + "ORDER BY msg_id LIMIT 1"
                    : SqswireDialect.dedupLookupSql(engine, table);
            try (PreparedStatement dup = conn.prepareStatement(dedupSql)) {
                dup.setString(1, dedupId);
                dup.setInt(2, FIFO_DEDUP_WINDOW_SECONDS);
                try (ResultSet rs = dup.executeQuery()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            }
        }
        // Real, portable JDBC generated-keys API instead of RETURNING -- see SqswireDialect's javadoc. Naming the
        // generated column explicitly is what makes Oracle's driver return the value rather than a ROWID.
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + table + " (body, message_group_id, dedup_id) VALUES (?, ?, ?)", new String[] {"msg_id"})) {
            ps.setString(1, body);
            ps.setString(2, messageGroupId);
            ps.setString(3, dedupId);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                keys.next();
                return keys.getLong(1);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // ReceiveMessage
    // ------------------------------------------------------------------------------------------

    public record Received(long msgId, String messageId, String receiptHandle, String body, int receiveCount,
            String attrsJson, String traceHeader, long sentMs, Long firstReceiveMs, String groupId, String dedupId,
            String dlqSourceArn) {
        /** FIFO sequence numbers are the per-queue insertion counter, zero-padded like real SQS's 18 digits. */
        public String sequenceNumber() {
            return String.format("%018d", msgId);
        }
    }

    private static final String RETURN_COLS = "msg_id, COALESCE(message_id, msg_id::text) AS message_id, receipt_handle, body, "
            + "read_ct, attrs, trace_header, (extract(epoch FROM enqueued_at) * 1000)::bigint AS sent_ms, "
            + "(extract(epoch FROM first_receive_at) * 1000)::bigint AS first_ms, message_group_id, dedup_id, dlq_source_arn";

    private static Received readReceived(ResultSet rs) throws SQLException {
        long first = rs.getLong("first_ms");
        Long firstMs = rs.wasNull() ? null : first;
        return new Received(rs.getLong("msg_id"), rs.getString("message_id"), rs.getString("receipt_handle"),
                rs.getString("body"), rs.getInt("read_ct"), rs.getString("attrs"), rs.getString("trace_header"),
                rs.getLong("sent_ms"), firstMs, rs.getString("message_group_id"), rs.getString("dedup_id"),
                rs.getString("dlq_source_arn"));
    }

    /**
     * One non-blocking receive attempt (long polling is layered on top by the caller so no pooled
     * connection is ever held while waiting). Claims up to {@code max} visible messages, moving any that
     * exceeded the redrive policy's {@code maxReceiveCount} to the dead-letter queue instead of returning them.
     */
    public List<Received> receive(String queueName, QueueAttributes attrs, int max, Integer visibilityOverride,
            String attemptId) throws SQLException {
        int vt = visibilityOverride != null ? visibilityOverride : attrs.visibilityTimeout();
        String table = tableOf(queueName, attrs);
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName, table);
            String engine = engineOf(conn);
            if (!"postgres".equals(engine)) {
                return legacyReceive(conn, engine, queueName, attrs, table, max, vt);
            }
            int redriveMax = attrs.maxReceiveCount() != null && attrs.dlqQueueName() != null ? attrs.maxReceiveCount() : -1;
            List<Received> out = new ArrayList<>();
            for (int round = 0; round < 5 && out.size() < max; round++) {
                List<Received> claimed = attrs.fifo()
                        ? claimFifo(conn, table, attrs, max - out.size(), vt, attemptId)
                        : claimStandard(conn, table, attrs, max - out.size(), vt);
                if (claimed.isEmpty()) {
                    break;
                }
                List<Received> dead = new ArrayList<>();
                for (Received r : claimed) {
                    if (redriveMax > 0 && r.receiveCount() > redriveMax) {
                        dead.add(r);
                    } else {
                        out.add(r);
                    }
                }
                if (dead.isEmpty()) {
                    break;
                }
                redrive(conn, table, queueName, attrs, dead);
            }
            out.sort(java.util.Comparator.comparingLong(Received::msgId));
            return out;
        }
    }

    private static List<Received> claimStandard(Connection conn, String table, QueueAttributes attrs, int limit, int vt)
            throws SQLException {
        String sql = "UPDATE " + table + " SET vt = now() + ? * interval '1 second', "
                + "receipt_handle = msg_id::text || '-' || replace(gen_random_uuid()::text, '-', ''), "
                + "read_ct = read_ct + 1, first_receive_at = COALESCE(first_receive_at, now()) "
                + "WHERE msg_id IN (SELECT msg_id FROM " + table + " WHERE vt <= now() "
                + "AND enqueued_at > now() - ? * interval '1 second' ORDER BY msg_id FOR UPDATE SKIP LOCKED LIMIT ?) "
                + "RETURNING " + RETURN_COLS;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, vt);
            ps.setInt(2, attrs.retentionSeconds());
            ps.setInt(3, limit);
            List<Received> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(readReceived(rs));
                }
            }
            return out;
        }
    }

    /**
     * FIFO claim: whole-queue advisory lock so the "no in-flight message in this group" check is race free;
     * picks the group holding the oldest eligible message first and returns as many of its messages (in order)
     * as fit, then continues with the next group. A group with any message in flight is skipped entirely.
     */
    private static List<Received> claimFifo(Connection conn, String table, QueueAttributes attrs, int limit, int vt,
            String attemptId) throws SQLException {
        boolean auto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement lock = conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
                lock.setString(1, table);
                lock.executeQuery().close();
            }
            List<Received> out = new ArrayList<>();
            if (attemptId != null) {
                // a retry of a receive that already claimed messages returns the same messages and handles
                try (PreparedStatement ps = conn.prepareStatement("SELECT " + RETURN_COLS + " FROM " + table
                        + " WHERE attempt_id = ? AND attempt_at > now() - interval '" + FIFO_DEDUP_WINDOW_SECONDS
                        + " seconds' AND vt > now() ORDER BY msg_id")) {
                    ps.setString(1, attemptId);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(readReceived(rs));
                        }
                    }
                }
                if (!out.isEmpty()) {
                    conn.commit();
                    return out;
                }
            }
            String sql = "UPDATE " + table + " SET vt = now() + ? * interval '1 second', "
                    + "receipt_handle = msg_id::text || '-' || replace(gen_random_uuid()::text, '-', ''), "
                    + "read_ct = read_ct + 1, first_receive_at = COALESCE(first_receive_at, now())"
                    + (attemptId != null ? ", attempt_id = ?, attempt_at = now()" : "")
                    + " WHERE msg_id IN (SELECT msg_id FROM (SELECT t.msg_id, min(t.msg_id) OVER (PARTITION BY t.message_group_id) AS gfirst "
                    + "FROM " + table + " t WHERE t.vt <= now() AND t.enqueued_at > now() - ? * interval '1 second' "
                    + "AND NOT EXISTS (SELECT 1 FROM " + table + " o WHERE o.message_group_id = t.message_group_id "
                    + "AND o.vt > now() AND o.read_ct > 0)) x ORDER BY gfirst, msg_id LIMIT ?) "
                    + "RETURNING " + RETURN_COLS;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 1;
                ps.setInt(i++, vt);
                if (attemptId != null) {
                    ps.setString(i++, attemptId);
                }
                ps.setInt(i++, attrs.retentionSeconds());
                ps.setInt(i, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(readReceived(rs));
                    }
                }
            }
            conn.commit();
            return out;
        } catch (SQLException | RuntimeException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(auto);
        }
    }

    /**
     * Moves messages that exceeded {@code maxReceiveCount} to the dead-letter queue -- which may live on a
     * different shard backend. The copy lands in the DLQ first, then the source rows are deleted (a crash in
     * between duplicates rather than loses a message).
     */
    private void redrive(Connection sourceConn, String sourceTable, String sourceQueue, QueueAttributes attrs,
            List<Received> dead) throws SQLException {
        String dlq = attrs.dlqQueueName();
        QueueAttributes dlqAttrs = findQueue(dlq);
        if (dlqAttrs == null) {
            // the dead-letter queue was deleted: real SQS keeps the messages in the source queue
            try (PreparedStatement ps = sourceConn.prepareStatement("UPDATE " + sourceTable
                    + " SET vt = now() WHERE msg_id = ANY (?)")) {
                ps.setArray(1, sourceConn.createArrayOf("bigint", dead.stream().map(Received::msgId).toArray()));
                ps.executeUpdate();
            }
            return;
        }
        String dlqTable = tableOf(dlq, dlqAttrs);
        try (Connection dlqConn = connectionFor(dlq)) {
            ensureTable(dlqConn, dlq, dlqTable);
            String sourceArn = queueArn(sourceQueue);
            try (PreparedStatement ins = dlqConn.prepareStatement("INSERT INTO " + dlqTable
                    + " (message_id, body, message_group_id, dedup_id, attrs, trace_header, enqueued_at, dlq_source_arn) "
                    + "VALUES (?, ?, ?, ?, ?, ?, to_timestamp(? / 1000.0), ?)")) {
                for (Received r : dead) {
                    ins.setString(1, r.messageId());
                    ins.setString(2, r.body());
                    ins.setString(3, r.groupId());
                    ins.setString(4, r.dedupId());
                    ins.setString(5, r.attrsJson());
                    ins.setString(6, r.traceHeader());
                    ins.setLong(7, r.sentMs());
                    ins.setString(8, r.dlqSourceArn() != null ? r.dlqSourceArn() : sourceArn);
                    ins.addBatch();
                }
                ins.executeBatch();
            }
        }
        try (PreparedStatement del = sourceConn.prepareStatement("DELETE FROM " + sourceTable + " WHERE msg_id = ANY (?)")) {
            del.setArray(1, sourceConn.createArrayOf("bigint", dead.stream().map(Received::msgId).toArray()));
            del.executeUpdate();
        }
        log.info("sqswire: redrove {} message(s) from \"{}\" to DLQ \"{}\" after exceeding max receive count",
                dead.size(), sourceQueue, dlq);
        enqueueListener.accept(dlq);
    }

    /** Legacy claim (Oracle/SQL Server/MySQL backends): body only, one message per claim. */
    private List<Received> legacyReceive(Connection conn, String engine, String queueName, QueueAttributes attrs,
            String table, int maxMessages, int visibilityTimeoutSeconds) throws SQLException {
        List<Received> results = new ArrayList<>();
        for (int i = 0; i < maxMessages; i++) {
            ReceivedMessage claimed = "sqlserver".equals(engine)
                    ? claimOneSqlServer(conn, table, attrs.fifo(), visibilityTimeoutSeconds)
                    : claimOneNonPostgres(conn, engine, table, attrs.fifo(), visibilityTimeoutSeconds);
            if (claimed == null) {
                break;
            }
            if (attrs.maxReceiveCount() != null && attrs.dlqQueueName() != null
                    && claimed.readCt() > attrs.maxReceiveCount()) {
                legacyRedrive(conn, table, claimed, attrs.dlqQueueName());
                i--;
                continue;
            }
            results.add(new Received(claimed.msgId(), String.valueOf(claimed.msgId()), claimed.receiptHandle(),
                    claimed.body(), claimed.readCt(), null, null, System.currentTimeMillis(), null, null, null, null));
        }
        return results;
    }

    private record ReceivedMessage(long msgId, String receiptHandle, String body, int readCt) {
    }

    /** SQL Server's own real single-statement claim -- see {@link SqswireDialect}'s javadoc. */
    private static ReceivedMessage claimOneSqlServer(Connection conn, String table, boolean fifo, int visibilityTimeoutSeconds)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SqswireDialect.claimSqlServerSql(table, fifo))) {
            ps.setInt(1, visibilityTimeoutSeconds);
            String receiptHandle = UUID.randomUUID().toString();
            ps.setString(2, receiptHandle);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new ReceivedMessage(rs.getLong("msg_id"), receiptHandle, rs.getString("body"), rs.getInt("read_ct"));
            }
        }
    }

    /** The real, portable two-statement claim for Oracle/MySQL -- see {@link SqswireDialect}'s javadoc. */
    private static ReceivedMessage claimOneNonPostgres(Connection conn, String engine, String table, boolean fifo,
            int visibilityTimeoutSeconds) throws SQLException {
        boolean originalAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            long msgId;
            String body;
            int readCt;
            try (PreparedStatement select = conn.prepareStatement(SqswireDialect.claimSelectSql(engine, table, fifo));
                    ResultSet rs = select.executeQuery()) {
                if (!rs.next()) {
                    conn.commit();
                    return null;
                }
                msgId = rs.getLong("msg_id");
                body = rs.getString("body");
                readCt = rs.getInt("read_ct");
            }
            String receiptHandle = UUID.randomUUID().toString();
            try (PreparedStatement update = conn.prepareStatement(SqswireDialect.claimUpdateSql(engine, table))) {
                update.setInt(1, visibilityTimeoutSeconds);
                update.setString(2, receiptHandle);
                update.setLong(3, msgId);
                update.executeUpdate();
            }
            conn.commit();
            return new ReceivedMessage(msgId, receiptHandle, body, readCt + 1);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(originalAutoCommit);
        }
    }

    private void legacyRedrive(Connection sourceConn, String sourceTable, ReceivedMessage msg, String dlqQueueName)
            throws SQLException {
        QueueAttributes dlqAttrs = findQueue(dlqQueueName);
        String dlqTable = tableOf(dlqQueueName, dlqAttrs);
        try (Connection dlqConn = connectionFor(dlqQueueName)) {
            ensureTable(dlqConn, dlqQueueName, dlqTable);
            try (PreparedStatement ins = dlqConn.prepareStatement("INSERT INTO " + dlqTable + " (body) VALUES (?)")) {
                ins.setString(1, msg.body());
                ins.executeUpdate();
            }
        }
        try (PreparedStatement del = sourceConn.prepareStatement("DELETE FROM " + sourceTable + " WHERE msg_id = ?")) {
            del.setLong(1, msg.msgId());
            del.executeUpdate();
        }
    }

    // ------------------------------------------------------------------------------------------
    // DeleteMessage / ChangeMessageVisibility / counts / purge
    // ------------------------------------------------------------------------------------------

    private static final Pattern HANDLE = Pattern.compile("(\\d{1,18})-[0-9a-f]{32}");

    /** Whether {@code handle} has the shape this store hands out (Postgres-backed queues). */
    public static boolean isWellFormedHandle(String handle) {
        return handle != null && HANDLE.matcher(handle).matches();
    }

    /**
     * Deletes the message a receipt handle refers to. Like real SQS the call is idempotent: a handle for a
     * message that is already gone (or was re-received under a newer handle) deletes nothing and still succeeds.
     *
     * @throws SqsException ReceiptHandleIsInvalid for a handle this service could never have issued
     */
    public boolean deleteMessage(String queueName, QueueAttributes attrs, String receiptHandle) throws SQLException {
        String table = tableOf(queueName, attrs);
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName, table);
            if ("postgres".equals(engineOf(conn))) {
                var m = HANDLE.matcher(receiptHandle == null ? "" : receiptHandle);
                if (!m.matches()) {
                    throw new SqsException(400, "ReceiptHandleIsInvalid",
                            "The input receipt handle \"" + receiptHandle + "\" is not a valid receipt handle.");
                }
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + table + " WHERE msg_id = ? AND receipt_handle = ?")) {
                    ps.setLong(1, Long.parseLong(m.group(1)));
                    ps.setString(2, receiptHandle);
                    return ps.executeUpdate() > 0;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + table + " WHERE receipt_handle = ?")) {
                ps.setString(1, receiptHandle);
                return ps.executeUpdate() > 0;
            }
        }
    }

    public enum VisibilityResult { CHANGED, NOT_IN_FLIGHT, INVALID }

    public VisibilityResult changeMessageVisibility(String queueName, QueueAttributes attrs, String receiptHandle,
            int visibilityTimeoutSeconds) throws SQLException {
        String table = tableOf(queueName, attrs);
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName, table);
            String engine = engineOf(conn);
            if (!"postgres".equals(engine)) {
                try (PreparedStatement ps = conn.prepareStatement(SqswireDialect.changeVisibilitySql(engine, table))) {
                    ps.setInt(1, visibilityTimeoutSeconds);
                    ps.setString(2, receiptHandle);
                    return ps.executeUpdate() > 0 ? VisibilityResult.CHANGED : VisibilityResult.INVALID;
                }
            }
            var m = HANDLE.matcher(receiptHandle == null ? "" : receiptHandle);
            if (!m.matches()) {
                throw new SqsException(400, "ReceiptHandleIsInvalid",
                        "The input receipt handle \"" + receiptHandle + "\" is not a valid receipt handle.");
            }
            long msgId = Long.parseLong(m.group(1));
            try (PreparedStatement ps = conn.prepareStatement("UPDATE " + table
                    + " SET vt = now() + ? * interval '1 second' WHERE msg_id = ? AND receipt_handle = ? AND vt > now()")) {
                ps.setInt(1, visibilityTimeoutSeconds);
                ps.setLong(2, msgId);
                ps.setString(3, receiptHandle);
                if (ps.executeUpdate() > 0) {
                    if (visibilityTimeoutSeconds == 0) {
                        enqueueListener.accept(queueName);
                    }
                    return VisibilityResult.CHANGED;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM " + table + " WHERE msg_id = ? AND receipt_handle = ?")) {
                ps.setLong(1, msgId);
                ps.setString(2, receiptHandle);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? VisibilityResult.NOT_IN_FLIGHT : VisibilityResult.INVALID;
                }
            }
        }
    }

    public record QueueCounts(long visible, long inFlight, long delayed) {
        public QueueCounts(long visible, long inFlight) {
            this(visible, inFlight, 0);
        }
    }

    public QueueCounts countMessages(String queueName) throws SQLException {
        return countMessages(queueName, queueAttributes(queueName));
    }

    public QueueCounts countMessages(String queueName, QueueAttributes attrs) throws SQLException {
        String table = tableOf(queueName, attrs);
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName, table);
            String engine = engineOf(conn);
            if (!"postgres".equals(engine)) {
                try (var st = conn.createStatement(); ResultSet rs = st.executeQuery(SqswireDialect.visibleCountSql(engine, table))) {
                    rs.next();
                    return new QueueCounts(rs.getLong("visible"), rs.getLong("in_flight"), 0);
                }
            }
            String sql = "SELECT count(*) FILTER (WHERE vt <= now()) AS visible, "
                    + "count(*) FILTER (WHERE vt > now() AND read_ct > 0) AS in_flight, "
                    + "count(*) FILTER (WHERE vt > now() AND read_ct = 0) AS delayed FROM " + table;
            try (var st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                rs.next();
                return new QueueCounts(rs.getLong("visible"), rs.getLong("in_flight"), rs.getLong("delayed"));
            }
        }
    }

    public void purgeQueue(String queueName, QueueAttributes attrs) throws SQLException {
        String table = tableOf(queueName, attrs);
        try (Connection conn = connectionFor(queueName); var st = conn.createStatement()) {
            ensureTable(conn, queueName, table);
            st.execute("DELETE FROM " + table);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Retention sweeper
    // ------------------------------------------------------------------------------------------

    /** Deletes messages older than each queue's retention period and expired dedup records. */
    public long sweepExpired() throws SQLException {
        long removed = 0;
        for (String name : listQueues()) {
            try {
                QueueAttributes a = findQueue(name);
                if (a == null) {
                    continue;
                }
                String table = tableOf(name, a);
                try (Connection conn = connectionFor(name)) {
                    if (!"postgres".equals(engineOf(conn))) {
                        continue;
                    }
                    // no ensureTable: a queue deleted since it was listed must not have its table re-created here
                    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + table
                            + " WHERE enqueued_at < now() - ? * interval '1 second'")) {
                        ps.setInt(1, a.retentionSeconds());
                        removed += ps.executeUpdate();
                    }
                    if (a.fifo()) {
                        try (var st = conn.createStatement()) {
                            st.execute("DELETE FROM " + dedupTable(table) + " WHERE created_at < now() - interval '"
                                    + FIFO_DEDUP_WINDOW_SECONDS + " seconds'");
                        }
                    }
                }
            } catch (SQLException e) {
                log.debug("sqswire sweeper: skipping queue {}: {}", name, e.getMessage());
            }
        }
        return removed;
    }

    // ------------------------------------------------------------------------------------------
    // Message move tasks (DLQ redrive)
    // ------------------------------------------------------------------------------------------

    public record MoveDestination(String queueName, QueueAttributes attrs) {
    }

    public record MoveResult(int moved, int skipped, int scanned) {
    }

    /**
     * Moves up to {@code limit} visible messages out of {@code sourceQueue}. {@code resolver} maps each message
     * (by its recorded dead-letter source ARN) to a destination queue, or {@code null} to leave it in place.
     * Source and destination may be on different shard backends: each batch copies into the destination first
     * and only then deletes from the source (at-least-once).
     */
    public MoveResult moveBatch(String sourceQueue, QueueAttributes srcAttrs, int limit,
            Function<Received, MoveDestination> resolver) throws SQLException {
        String table = tableOf(sourceQueue, srcAttrs);
        try (Connection src = connectionFor(sourceQueue)) {
            if (!"postgres".equals(engineOf(src))) {
                throw new SqsException(400, "UnsupportedOperation", "Message move tasks require a Postgres backend.");
            }
            ensureTable(src, sourceQueue, table);
            boolean auto = src.getAutoCommit();
            src.setAutoCommit(false);
            try {
                List<Received> rows = new ArrayList<>();
                try (PreparedStatement ps = src.prepareStatement("SELECT msg_id, COALESCE(message_id, msg_id::text) AS message_id, "
                        + "receipt_handle, body, read_ct, attrs, trace_header, (extract(epoch FROM enqueued_at) * 1000)::bigint AS sent_ms, "
                        + "NULL::bigint AS first_ms, message_group_id, dedup_id, dlq_source_arn FROM " + table
                        + " WHERE vt <= now() ORDER BY msg_id LIMIT ? FOR UPDATE SKIP LOCKED")) {
                    ps.setInt(1, limit);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            rows.add(readReceived(rs));
                        }
                    }
                }
                List<Long> movedIds = new ArrayList<>();
                Map<String, List<Received>> byDest = new LinkedHashMap<>();
                Map<String, MoveDestination> dests = new HashMap<>();
                int skipped = 0;
                for (Received r : rows) {
                    MoveDestination d = resolver.apply(r);
                    if (d == null) {
                        skipped++;
                        continue;
                    }
                    dests.put(d.queueName(), d);
                    byDest.computeIfAbsent(d.queueName(), k -> new ArrayList<>()).add(r);
                }
                for (var e : byDest.entrySet()) {
                    MoveDestination d = dests.get(e.getKey());
                    String dTable = tableOf(d.queueName(), d.attrs());
                    try (Connection dc = connectionFor(d.queueName())) {
                        ensureTable(dc, d.queueName(), dTable);
                        try (PreparedStatement ins = dc.prepareStatement("INSERT INTO " + dTable
                                + " (message_id, body, message_group_id, dedup_id, attrs, trace_header) VALUES (?, ?, ?, ?, ?, ?)")) {
                            for (Received r : e.getValue()) {
                                ins.setString(1, r.messageId());
                                ins.setString(2, r.body());
                                ins.setString(3, d.attrs().fifo() && r.groupId() == null ? "moved" : r.groupId());
                                ins.setString(4, r.dedupId());
                                ins.setString(5, r.attrsJson());
                                ins.setString(6, r.traceHeader());
                                ins.addBatch();
                                movedIds.add(r.msgId());
                            }
                            ins.executeBatch();
                        }
                    }
                    enqueueListener.accept(d.queueName());
                }
                if (!movedIds.isEmpty()) {
                    try (PreparedStatement del = src.prepareStatement("DELETE FROM " + table + " WHERE msg_id = ANY (?)")) {
                        del.setArray(1, src.createArrayOf("bigint", movedIds.toArray()));
                        del.executeUpdate();
                    }
                }
                src.commit();
                return new MoveResult(movedIds.size(), skipped, rows.size());
            } catch (SQLException | RuntimeException e) {
                src.rollback();
                throw e;
            } finally {
                src.setAutoCommit(auto);
            }
        }
    }

    public record MoveTask(String handle, String sourceArn, String destinationArn, int maxPerSecond, String status,
            long moved, long toMove, String failureReason, long startedMs) {
    }

    private MoveTask readTask(ResultSet rs) throws SQLException {
        return new MoveTask(rs.getString("task_handle"), rs.getString("source_arn"), rs.getString("destination_arn"),
                rs.getInt("max_per_second"), rs.getString("status"), rs.getLong("moved"), rs.getLong("to_move"),
                rs.getString("failure_reason"), rs.getLong("started_at"));
    }

    public void insertMoveTask(MoveTask t) throws SQLException {
        try (Connection cat = borrowCatalogConnection(); PreparedStatement ps = cat.prepareStatement(
                "INSERT INTO sqs_move_tasks (task_handle, source_arn, destination_arn, max_per_second, status, moved, to_move, "
                        + "failure_reason, started_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, t.handle());
            ps.setString(2, t.sourceArn());
            ps.setString(3, t.destinationArn());
            ps.setInt(4, t.maxPerSecond());
            ps.setString(5, t.status());
            ps.setLong(6, t.moved());
            ps.setLong(7, t.toMove());
            ps.setString(8, t.failureReason());
            ps.setLong(9, t.startedMs());
            ps.setLong(10, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    /**
     * Records progress/terminal state. A task with a pending cancel (CANCELLING) stays CANCELLING while
     * running and ends CANCELLED; finished tasks are never modified. Returns the resulting status, or
     * {@code null} if the task was already finished.
     */
    public String updateMoveTask(String handle, String status, long moved, String failureReason) throws SQLException {
        try (Connection cat = borrowCatalogConnection(); PreparedStatement ps = cat.prepareStatement(
                "UPDATE sqs_move_tasks SET status = CASE WHEN status = 'CANCELLING' THEN "
                        + "(CASE WHEN ? IN ('COMPLETED', 'CANCELLED') THEN 'CANCELLED' ELSE 'CANCELLING' END) ELSE ? END, "
                        + "moved = ?, failure_reason = COALESCE(?, failure_reason), updated_at = ? WHERE task_handle = ? "
                        + "AND status NOT IN ('CANCELLED', 'COMPLETED', 'FAILED') RETURNING status")) {
            ps.setString(1, status);
            ps.setString(2, status);
            ps.setLong(3, moved);
            ps.setString(4, failureReason);
            ps.setLong(5, System.currentTimeMillis());
            ps.setString(6, handle);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /** Marks a RUNNING task CANCELLING; returns false if it was not running. */
    public boolean requestCancelMoveTask(String handle) throws SQLException {
        try (Connection cat = borrowCatalogConnection(); PreparedStatement ps = cat.prepareStatement(
                "UPDATE sqs_move_tasks SET status = 'CANCELLING', updated_at = ? WHERE task_handle = ? AND status = 'RUNNING'")) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, handle);
            return ps.executeUpdate() > 0;
        }
    }

    public MoveTask getMoveTask(String handle) throws SQLException {
        try (Connection cat = borrowCatalogConnection();
                PreparedStatement ps = cat.prepareStatement("SELECT * FROM sqs_move_tasks WHERE task_handle = ?")) {
            ps.setString(1, handle);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readTask(rs) : null;
            }
        }
    }

    public List<MoveTask> listMoveTasks(String sourceArn, int limit) throws SQLException {
        List<MoveTask> out = new ArrayList<>();
        try (Connection cat = borrowCatalogConnection(); PreparedStatement ps = cat.prepareStatement(
                "SELECT * FROM sqs_move_tasks WHERE source_arn = ? ORDER BY started_at DESC, task_handle")) {
            ps.setString(1, sourceArn);
            ps.setMaxRows(limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(readTask(rs));
                }
            }
        }
        return out;
    }

    public List<MoveTask> runningMoveTasks() throws SQLException {
        List<MoveTask> out = new ArrayList<>();
        try (Connection cat = borrowCatalogConnection(); var st = cat.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM sqs_move_tasks WHERE status IN ('RUNNING', 'CANCELLING')")) {
            while (rs.next()) {
                out.add(readTask(rs));
            }
        }
        return out;
    }

    /** True when the catalog backend can host protocol features beyond the legacy core (i.e. is Postgres). */
    public boolean catalogIsPostgres() throws SQLException {
        try (Connection cat = borrowCatalogConnection()) {
            return "postgres".equals(engineOf(cat));
        }
    }

    public void close() {
        if (legacyDs != null) {
            legacyDs.close();
        }
    }
}
