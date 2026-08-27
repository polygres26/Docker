package com.polygres.wire.sqswire;

import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import com.polygres.wire.core.DdlTemplates;
import com.polygres.wire.core.ShardingStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue storage for sqswire, backed by Postgres -- the same pgmq (github.com/pgmq/pgmq) table
 * shape (a {@code vt} visibility-timeout column plus {@code FOR UPDATE SKIP LOCKED} for
 * ReceiveMessage) reimplemented in plain SQL, so no {@code pgmq} extension needs to be installed
 * on the backend, exactly as dynamowire/mongowire don't require DynamoDB/MongoDB to exist.
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
 * <p><b>Queue attributes catalog:</b> {@code _sqs_queues} holds each queue's visibility timeout
 * default, FIFO flag, and redrive policy (DLQ target + max receive count). Like dynamowire's
 * {@code _dynamo_tables}, this metadata needs one fixed home independent of shard-group
 * membership, so it always lives on {@code BackendRegistry.DEFAULT_BACKEND_NAME} -- see
 * {@link #currentCatalogBackendName()}'s javadoc for why using a shard-group member here broke
 * DescribeTable-equivalent lookups in dynamowire the first time that was tried.
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
 * call, not captured once at construction, so a {@code POLYWIRE_BACKENDS}/
 * {@code POLYWIRE_SHARD_BACKENDS} change takes effect on this store's very next operation.
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

    // queueAttributes() used to hit Postgres (a real _sqs_queues round trip) on every single
    // ReceiveMessage call -- found live comparing SendMessage's server-side cost against
    // ReceiveMessage's on an empty queue: ReceiveMessage was ~2x slower despite doing *less* real
    // work (a claim UPDATE that matches nothing, vs. an actual INSERT), the same shape of gap
    // describeTable() had for dynamowire and the translation cache had for orawire/mywire/
    // mssqlwire. Unlike dynamowire's table schema, SQS queue attributes ARE mutable at runtime
    // (SetQueueAttributes), so this cache is invalidated -- not just populated -- on every write:
    // createQueue()/setQueueAttributes() (the latter delegates to the former) refresh it,
    // deleteQueue() evicts it.
    private final ConcurrentHashMap<String, QueueAttributes> attributesCache = new ConcurrentHashMap<>();

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
            throw new RuntimeException("sqswire: failed to create _sqs_queues catalog", e);
        }
    }

    private List<String> currentShardGroup() {
        return backendRegistry == null ? List.of() : backendRegistry.shardGroup();
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
     * {@code _sqs_queues} needs one fixed home that doesn't move when the shard group is
     * reconfigured (mirrors {@code PgItemStore#currentCatalogBackendName}, including the
     * fallback for a deployment that shards without an explicit {@code default=...} entry).
     */
    private Connection borrowCatalogConnection() {
        try {
            if (legacyDs != null) {
                return legacyDs.getConnection();
            }
            BackendTarget target = backendRegistry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
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
        // Real DDL, loaded from ddl/postgres/sqswire_catalog.sql -- see DdlTemplates' own javadoc.
        try (var st = conn.createStatement()) {
            for (String statement : DdlTemplates.loadStatements("postgres", "sqswire_catalog", Map.of())) {
                st.execute(statement);
            }
        }
        catalogEnsured.put(key, Boolean.TRUE);
    }

    private static String safeTableName(String queueName) {
        return "sqs_queue_" + SAFE_QUEUE_NAME.matcher(queueName).replaceAll("_");
    }

    private void ensureTable(Connection conn, String queueName) throws SQLException {
        String key = queueName + "@" + conn.getMetaData().getURL();
        // Cheap idempotent DDL; not worth the complexity of a per-(backend,queue) cache beyond
        // avoiding a repeat CREATE TABLE on every single call for the common case.
        if (Boolean.TRUE.equals(tableEnsured.get(key))) {
            return;
        }
        String table = safeTableName(queueName);
        // Real DDL, loaded from ddl/postgres/sqswire_queue_table.sql -- see that file's own
        // comment for why this store is still Postgres-only end to end (query-level, not just
        // DDL) unlike dynamowire/influxwire's own now-portable table DDL.
        try (var st = conn.createStatement()) {
            for (String statement : DdlTemplates.loadStatements("postgres", "sqswire_queue_table", Map.of("table", table))) {
                st.execute(statement);
            }
        }
        tableEnsured.put(key, Boolean.TRUE);
    }

    public record QueueAttributes(int visibilityTimeout, boolean fifo, String dlqQueueName, Integer maxReceiveCount) {
        static final QueueAttributes DEFAULTS = new QueueAttributes(30, false, null, null);
    }

    public void createQueue(String queueName, QueueAttributes attrs) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
        }
        boolean isFifo = attrs.fifo() || queueName.endsWith(".fifo");
        try (Connection cat = borrowCatalogConnection();
                PreparedStatement ps = cat.prepareStatement(
                        "INSERT INTO _sqs_queues (queue_name, visibility_timeout, is_fifo, dlq_queue_name, max_receive_count) "
                                + "VALUES (?, ?, ?, ?, ?) ON CONFLICT (queue_name) DO UPDATE SET "
                                + "visibility_timeout = EXCLUDED.visibility_timeout, is_fifo = EXCLUDED.is_fifo, "
                                + "dlq_queue_name = EXCLUDED.dlq_queue_name, max_receive_count = EXCLUDED.max_receive_count")) {
            ps.setString(1, queueName);
            ps.setInt(2, attrs.visibilityTimeout());
            ps.setBoolean(3, isFifo);
            ps.setString(4, attrs.dlqQueueName());
            if (attrs.maxReceiveCount() == null) {
                ps.setNull(5, java.sql.Types.INTEGER);
            } else {
                ps.setInt(5, attrs.maxReceiveCount());
            }
            ps.executeUpdate();
        }
        QueueAttributes stored = new QueueAttributes(attrs.visibilityTimeout(), isFifo, attrs.dlqQueueName(), attrs.maxReceiveCount());
        attributesCache.put(queueName, stored);
    }

    public QueueAttributes queueAttributes(String queueName) throws SQLException {
        QueueAttributes cached = attributesCache.get(queueName);
        if (cached != null) {
            return cached;
        }
        QueueAttributes loaded = loadQueueAttributes(queueName);
        attributesCache.put(queueName, loaded);
        return loaded;
    }

    private QueueAttributes loadQueueAttributes(String queueName) throws SQLException {
        try (Connection cat = borrowCatalogConnection();
                PreparedStatement ps = cat.prepareStatement(
                        "SELECT visibility_timeout, is_fifo, dlq_queue_name, max_receive_count FROM _sqs_queues WHERE queue_name = ?")) {
            ps.setString(1, queueName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return QueueAttributes.DEFAULTS;
                }
                Integer maxReceive = rs.getObject("max_receive_count") == null ? null : rs.getInt("max_receive_count");
                return new QueueAttributes(rs.getInt("visibility_timeout"), rs.getBoolean("is_fifo"),
                        rs.getString("dlq_queue_name"), maxReceive);
            }
        }
    }

    public void setQueueAttributes(String queueName, QueueAttributes attrs) throws SQLException {
        createQueue(queueName, attrs);
    }

    public void deleteQueue(String queueName) throws SQLException {
        try (Connection conn = connectionFor(queueName); var st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + safeTableName(queueName));
        }
        try (Connection cat = borrowCatalogConnection();
                PreparedStatement ps = cat.prepareStatement("DELETE FROM _sqs_queues WHERE queue_name = ?")) {
            ps.setString(1, queueName);
            ps.executeUpdate();
        } finally {
            attributesCache.remove(queueName);
        }
    }

    /**
     * Reads the catalog ({@code _sqs_queues}) rather than scanning {@code information_schema}
     * across every shard backend -- the catalog is already the one stable, cluster-wide source
     * of queue names (see the class javadoc), and unlike a table-name-derived list it preserves
     * the real queue name: Postgres identifiers can't hold {@code -}/{@code .} (both legal in SQS
     * queue names, e.g. {@code orders.fifo}), so deriving a name from the sanitized table name
     * would return the wrong string and silently miss that queue's own catalog row (its FIFO
     * flag, DLQ, etc.) on every subsequent lookup -- exactly the bug this replaced.
     */
    public List<String> listQueues() throws SQLException {
        List<String> names = new ArrayList<>();
        try (Connection cat = borrowCatalogConnection();
                var st = cat.createStatement();
                ResultSet rs = st.executeQuery("SELECT queue_name FROM _sqs_queues ORDER BY queue_name")) {
            while (rs.next()) {
                names.add(rs.getString(1));
            }
        }
        return names;
    }

    /**
     * FIFO dedup: a resend within the 5-minute window with the same {@code dedupId} returns the
     * original message's id instead of inserting a duplicate row, matching SQS's own
     * content-based-dedup behavior for {@code .fifo} queues.
     */
    public long sendMessage(String queueName, String body, String messageGroupId, String dedupId) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
            String table = safeTableName(queueName);
            if (dedupId != null) {
                try (PreparedStatement dup = conn.prepareStatement(
                        "SELECT msg_id FROM " + table + " WHERE dedup_id = ? AND enqueued_at > now() - (? || ' seconds')::interval "
                                + "ORDER BY msg_id LIMIT 1")) {
                    dup.setString(1, dedupId);
                    dup.setInt(2, FIFO_DEDUP_WINDOW_SECONDS);
                    try (ResultSet rs = dup.executeQuery()) {
                        if (rs.next()) {
                            return rs.getLong(1);
                        }
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (body, message_group_id, dedup_id) VALUES (?, ?, ?) RETURNING msg_id")) {
                ps.setString(1, body);
                ps.setString(2, messageGroupId);
                ps.setString(3, dedupId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        }
    }

    public record ReceivedMessage(long msgId, String receiptHandle, String body, int readCt) {
    }

    /**
     * {@code UPDATE ... WHERE vt <= now() ... FOR UPDATE SKIP LOCKED} in a single statement --
     * this one statement *is* the visibility-timeout mechanism: it atomically claims up to
     * {@code maxMessages} visible rows, stamps a fresh receipt handle, and pushes {@code vt}
     * forward so no other concurrent receiver can claim the same rows until it expires. No
     * background sweeper needed, and it's safe under concurrent ReceiveMessage callers by
     * construction (the same technique pgmq itself uses).
     *
     * <p>FIFO queues additionally exclude any row whose {@code message_group_id} already has
     * another row in flight, so at most one message per group is ever outstanding at a time.
     * When the queue's redrive policy caps {@code max_receive_count}, a row that would exceed it
     * is moved to the DLQ's table instead of being handed back to the caller, and claiming
     * continues for the remaining requested slots.
     */
    public List<ReceivedMessage> receiveMessages(String queueName, int maxMessages, Integer visibilityTimeoutOverride) throws SQLException {
        QueueAttributes attrs = queueAttributes(queueName);
        int visibilityTimeoutSeconds = visibilityTimeoutOverride != null ? visibilityTimeoutOverride : attrs.visibilityTimeout();
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
            String table = safeTableName(queueName);
            String claimSql = attrs.fifo()
                    ? "UPDATE " + table + " SET vt = now() + (? || ' seconds')::interval, "
                            + "receipt_handle = ?, read_ct = read_ct + 1 "
                            + "WHERE msg_id = (SELECT msg_id FROM " + table + " t WHERE t.vt <= now() "
                            + "AND (t.message_group_id IS NULL OR NOT EXISTS ("
                            + "  SELECT 1 FROM " + table + " o WHERE o.message_group_id = t.message_group_id AND o.vt > now())) "
                            + "ORDER BY t.msg_id FOR UPDATE SKIP LOCKED LIMIT 1) "
                            + "RETURNING msg_id, receipt_handle, body, read_ct"
                    : "UPDATE " + table + " SET vt = now() + (? || ' seconds')::interval, "
                            + "receipt_handle = ?, read_ct = read_ct + 1 "
                            + "WHERE msg_id = (SELECT msg_id FROM " + table + " WHERE vt <= now() "
                            + "ORDER BY msg_id FOR UPDATE SKIP LOCKED LIMIT 1) "
                            + "RETURNING msg_id, receipt_handle, body, read_ct";
            List<ReceivedMessage> results = new ArrayList<>();
            // Each claimed row needs its OWN fresh receipt handle, not one shared across the
            // batch -- do it as N single-row claims rather than one batch UPDATE with one handle.
            for (int i = 0; i < maxMessages; i++) {
                ReceivedMessage claimed;
                try (PreparedStatement ps = conn.prepareStatement(claimSql)) {
                    String receiptHandle = UUID.randomUUID().toString();
                    ps.setInt(1, visibilityTimeoutSeconds);
                    ps.setString(2, receiptHandle);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            break;
                        }
                        claimed = new ReceivedMessage(rs.getLong("msg_id"), rs.getString("receipt_handle"),
                                rs.getString("body"), rs.getInt("read_ct"));
                    }
                }
                if (attrs.maxReceiveCount() != null && attrs.dlqQueueName() != null
                        && claimed.readCt() > attrs.maxReceiveCount()) {
                    redrive(conn, table, claimed, attrs.dlqQueueName());
                    i--; // this slot didn't produce a deliverable message -- try claiming another
                    continue;
                }
                results.add(claimed);
            }
            return results;
        }
    }

    /** Moves a message that exceeded max_receive_count onto its queue's dead-letter queue. */
    private void redrive(Connection sourceConn, String sourceTable, ReceivedMessage msg, String dlqQueueName) throws SQLException {
        try (Connection dlqConn = connectionFor(dlqQueueName)) {
            ensureTable(dlqConn, dlqQueueName);
            String dlqTable = safeTableName(dlqQueueName);
            try (PreparedStatement ins = dlqConn.prepareStatement("INSERT INTO " + dlqTable + " (body) VALUES (?)")) {
                ins.setString(1, msg.body());
                ins.executeUpdate();
            }
        }
        try (PreparedStatement del = sourceConn.prepareStatement("DELETE FROM " + sourceTable + " WHERE msg_id = ?")) {
            del.setLong(1, msg.msgId());
            del.executeUpdate();
        }
        log.info("sqswire: redrove message {} to DLQ \"{}\" after exceeding max receive count", msg.msgId(), dlqQueueName);
    }

    public boolean deleteMessage(String queueName, String receiptHandle) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
            String table = safeTableName(queueName);
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + table + " WHERE receipt_handle = ?")) {
                ps.setString(1, receiptHandle);
                return ps.executeUpdate() > 0;
            }
        }
    }

    public boolean changeMessageVisibility(String queueName, String receiptHandle, int visibilityTimeoutSeconds) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
            String table = safeTableName(queueName);
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE " + table + " SET vt = now() + (? || ' seconds')::interval WHERE receipt_handle = ?")) {
                ps.setInt(1, visibilityTimeoutSeconds);
                ps.setString(2, receiptHandle);
                return ps.executeUpdate() > 0;
            }
        }
    }

    public record QueueCounts(long visible, long inFlight) {
    }

    public QueueCounts countMessages(String queueName) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
            String table = safeTableName(queueName);
            try (var st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT count(*) FILTER (WHERE vt <= now()) AS visible, "
                                    + "count(*) FILTER (WHERE vt > now()) AS in_flight FROM " + table)) {
                rs.next();
                return new QueueCounts(rs.getLong("visible"), rs.getLong("in_flight"));
            }
        }
    }

    public void close() {
        if (legacyDs != null) {
            legacyDs.close();
        }
    }
}
