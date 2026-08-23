package com.polygres.wire.sqswire;

import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import com.polygres.wire.core.ShardingStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
 * no per-message shard key to hash independently). {@code ListQueues} is the one operation that
 * has no single queue to route by, so it fans out across every shard backend and merges -- same
 * pattern as dynamowire's {@code Scan} / mongowire's non-{@code _id} {@code find}. A registry with
 * an empty shard group behaves like a single-backend store pointed at
 * {@code BackendRegistry.DEFAULT_BACKEND_NAME}.
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
    private static final Pattern SAFE_QUEUE_NAME = Pattern.compile("[^a-zA-Z0-9_-]");

    private final HikariDataSource legacyDs;
    private final BackendRegistry backendRegistry;
    private final ConcurrentHashMap<String, Boolean> tableEnsured = new ConcurrentHashMap<>();
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
    }

    public PgQueueStore(BackendRegistry backendRegistry) {
        this.legacyDs = null;
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
        BackendTarget target = backendRegistry.get(backendName);
        if (target == null) {
            target = backendRegistry.get(BackendRegistry.DEFAULT_BACKEND_NAME);
        }
        if (target == null) {
            throw new IllegalStateException("sqswire: no backend named \"" + backendName
                    + "\" (or a \"" + BackendRegistry.DEFAULT_BACKEND_NAME + "\" fallback) is registered");
        }
        return target.open();
    }

    /** Every shard backend's connection, for fan-out operations like ListQueues. */
    private List<Connection> allBackendConnections() throws SQLException {
        List<Connection> connections = new ArrayList<>();
        if (legacyDs != null) {
            connections.add(legacyDs.getConnection());
            return connections;
        }
        logShardGroupIfChanged();
        List<String> group = currentShardGroup();
        if (group.isEmpty()) {
            BackendTarget target = backendRegistry.get(BackendRegistry.DEFAULT_BACKEND_NAME);
            if (target == null) {
                throw new IllegalStateException("sqswire: no \"" + BackendRegistry.DEFAULT_BACKEND_NAME + "\" backend registered");
            }
            connections.add(target.open());
            return connections;
        }
        for (String name : group) {
            BackendTarget target = backendRegistry.get(name);
            if (target == null) {
                log.warn("sqswire: shard group member \"{}\" is not a registered backend -- skipping for this fan-out", name);
                continue;
            }
            connections.add(target.open());
        }
        return connections;
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
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + "msg_id BIGSERIAL PRIMARY KEY, "
                    + "receipt_handle TEXT, "
                    + "vt TIMESTAMPTZ NOT NULL DEFAULT now(), "
                    + "enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                    + "read_ct INT NOT NULL DEFAULT 0, "
                    + "body TEXT NOT NULL, "
                    + "message_group_id TEXT, "
                    + "dedup_id TEXT)");
            st.execute("CREATE INDEX IF NOT EXISTS " + table + "_vt_idx ON " + table + " (vt)");
        }
        tableEnsured.put(key, Boolean.TRUE);
    }

    public void createQueue(String queueName) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
        }
    }

    public void deleteQueue(String queueName) throws SQLException {
        try (Connection conn = connectionFor(queueName); var st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + safeTableName(queueName));
        }
    }

    /** Queue names across every shard backend, deduplicated -- see class javadoc for why this fans out. */
    public List<String> listQueues() throws SQLException {
        List<String> names = new ArrayList<>();
        for (Connection conn : allBackendConnections()) {
            try (conn; var st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'sqs_queue_%'")) {
                while (rs.next()) {
                    String table = rs.getString(1);
                    String name = table.substring("sqs_queue_".length());
                    if (!names.contains(name)) {
                        names.add(name);
                    }
                }
            }
        }
        return names;
    }

    public long sendMessage(String queueName, String body, String messageGroupId, String dedupId) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
            String table = safeTableName(queueName);
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
     */
    public List<ReceivedMessage> receiveMessages(String queueName, int maxMessages, int visibilityTimeoutSeconds) throws SQLException {
        try (Connection conn = connectionFor(queueName)) {
            ensureTable(conn, queueName);
            String table = safeTableName(queueName);
            List<ReceivedMessage> results = new ArrayList<>();
            // Each claimed row needs its OWN fresh receipt handle, not one shared across the
            // batch -- do it as N single-row claims rather than one batch UPDATE with one handle.
            for (int i = 0; i < maxMessages; i++) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE " + table + " SET vt = now() + (? || ' seconds')::interval, "
                                + "receipt_handle = ?, read_ct = read_ct + 1 "
                                + "WHERE msg_id = (SELECT msg_id FROM " + table + " WHERE vt <= now() "
                                + "ORDER BY msg_id FOR UPDATE SKIP LOCKED LIMIT 1) "
                                + "RETURNING msg_id, receipt_handle, body, read_ct")) {
                    String receiptHandle = UUID.randomUUID().toString();
                    ps.setInt(1, visibilityTimeoutSeconds);
                    ps.setString(2, receiptHandle);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            break;
                        }
                        results.add(new ReceivedMessage(rs.getLong("msg_id"), rs.getString("receipt_handle"),
                                rs.getString("body"), rs.getInt("read_ct")));
                    }
                }
            }
            return results;
        }
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
