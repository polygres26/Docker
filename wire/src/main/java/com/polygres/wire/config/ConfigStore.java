package com.polygres.wire.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres-backed config store for PolyWire's hot-reloadable config tier (see {@code
 * PolyWireConfig}'s and {@code Main}'s class javadoc for the full bootstrap-vs-dynamic split).
 * Reuses the cluster's existing Postgres backend as the config store rather than standing up a
 * new subsystem (etcd/Consul/ZooKeeper) — every node already holds a live connection to this same
 * Postgres, which already gives durability and transactional consistency for free.
 *
 * <h2>Schema</h2>
 * <pre>{@code
 * CREATE TABLE polywire_config (
 *     version     bigserial PRIMARY KEY,
 *     payload     jsonb NOT NULL,
 *     created_at  timestamptz NOT NULL DEFAULT now()
 * );
 * }</pre>
 * <b>Insert-only, never updated in place</b>: every config change is a brand-new row. A reader
 * always does a single {@code SELECT ... ORDER BY version DESC LIMIT 1} — one committed row, read
 * whole, under Postgres's normal MVCC snapshot semantics, so a reader can never observe a
 * half-applied ("torn") version, and a row already read by one node is never mutated out from
 * under it later. That's what makes "old and new copies coexist during a rolling patch" safe: a
 * node running version N and a node that already picked up version N+1 both keep operating
 * correctly and independently — neither version is ever changed or deleted after being written.
 *
 * <h2>Writing a new version (supported mechanism for this pass)</h2>
 * There is deliberately no HTTP admin write-endpoint yet (Wire has zero config-write HTTP surface
 * today — {@link com.polygres.wire.http.admin.MetricsServer} only ever served read-only {@code
 * GET} routes before this change, and building a real authenticated write API is a larger, more
 * carefully-scoped follow-up than this pass attempts). The supported way to publish a new config
 * version right now is a plain SQL insert against the table above, e.g.:
 * <pre>{@code
 * INSERT INTO polywire_config (payload) VALUES (
 *   '{"qosRatePerSec":"50","qosBurst":"50","qosMaxWaitMs":null,"qosClassLimits":null,
 *     "qosPoolWaitThreshold":null,"cacheTables":null,"cacheTtlMs":null,"backends":null,
 *     "shardBackends":null,"routerSchemaRules":null,"routerPredicateRules":null,
 *     "routerValueShardRules":null,"routerShardTables":null,"rollupDefinitionsYaml":null}'::jsonb
 * );
 * }</pre>
 * {@link #ensureSchema} additionally installs an {@code AFTER INSERT} trigger that calls {@code
 * pg_notify('polywire_config_changed', ...)} for every new row — so this works identically
 * whether the insert comes from {@link #write}, {@code psql}, or any other SQL client; the trigger
 * is what makes "raw SQL insert" a fully supported publish mechanism rather than one that only
 * works through this class.
 *
 * <h2>LISTEN/NOTIFY propagation</h2>
 * {@link #listen} opens a <b>dedicated, non-pooled</b> JDBC connection via {@link DriverManager}
 * (deliberately not borrowed from a HikariCP pool elsewhere in this codebase) and issues {@code
 * LISTEN polywire_config_changed} on it, then blocks in a background thread on {@code
 * PGConnection.getNotifications(timeoutMs)}. This connection must stay open for the process
 * lifetime of the listener: Postgres's LISTEN/NOTIFY state is per-session, so a pooled connection
 * that gets silently recycled back to the pool (and handed to some unrelated caller, or closed and
 * replaced) would silently drop the subscription with no error raised anywhere — a real, sharp-
 * edged gotcha this class exists specifically to avoid.
 */
public final class ConfigStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConfigStore.class);
    private static final String CHANNEL = "polywire_config_changed";

    public record Version(long version, PolyWireConfig payload, java.time.Instant createdAt) {
    }

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private volatile Connection listenConnection;
    private ExecutorService listenExecutor;

    public ConfigStore(String host, int port, String database, String user, String password) {
        this.jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        this.user = user;
        this.password = password;
    }

    /** Idempotent — safe to call on every node's startup. */
    public void ensureSchema() throws SQLException {
        try (Connection conn = open(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS polywire_config ("
                    + "version bigserial PRIMARY KEY, "
                    + "payload jsonb NOT NULL, "
                    + "created_at timestamptz NOT NULL DEFAULT now())");
            st.execute("CREATE OR REPLACE FUNCTION polywire_config_notify() RETURNS trigger AS $$ "
                    + "BEGIN PERFORM pg_notify('" + CHANNEL + "', NEW.version::text); RETURN NEW; END; "
                    + "$$ LANGUAGE plpgsql");
            st.execute("DROP TRIGGER IF EXISTS polywire_config_notify_trigger ON polywire_config");
            st.execute("CREATE TRIGGER polywire_config_notify_trigger AFTER INSERT ON polywire_config "
                    + "FOR EACH ROW EXECUTE FUNCTION polywire_config_notify()");
        }
    }

    /** Inserts a brand-new version row (never updates an existing one) and returns its assigned version number. The insert trigger notifies every listening node. */
    public long write(PolyWireConfig config) throws SQLException {
        try (Connection conn = open();
                java.sql.PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO polywire_config (payload) VALUES (?::jsonb) RETURNING version")) {
            ps.setString(1, config.toJson());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /**
     * Reads the current latest complete version in one transaction (a single {@code SELECT} is
     * already one atomic MVCC snapshot read in Postgres, so no explicit {@code BEGIN} is needed
     * for this to be torn-read-safe). Empty when the table has no rows yet (a brand-new cluster).
     */
    public Optional<Version> readLatest() throws SQLException {
        try (Connection conn = open(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT version, payload::text, created_at FROM polywire_config "
                                + "ORDER BY version DESC LIMIT 1")) {
            if (!rs.next()) {
                return Optional.empty();
            }
            long version = rs.getLong(1);
            PolyWireConfig payload = PolyWireConfig.fromJson(rs.getString(2));
            java.time.Instant createdAt = rs.getTimestamp(3).toInstant();
            return Optional.of(new Version(version, payload, createdAt));
        }
    }

    /**
     * Starts a background LISTEN loop on a dedicated connection; {@code callback} is invoked
     * (with the newly-read latest {@link Version}) every time a notification arrives. Call at
     * most once per {@code ConfigStore} instance. Idle-polls {@code getNotifications(5000)} rather
     * than an unbounded block so the loop can also periodically notice the connection has died and
     * needs recreating, rather than hanging forever on a dropped socket.
     */
    public void listen(Consumer<Version> callback) throws SQLException {
        if (!listening.compareAndSet(false, true)) {
            throw new IllegalStateException("listen() already called on this ConfigStore");
        }
        listenExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "polywire-config-listen");
            t.setDaemon(true);
            return t;
        });
        listenExecutor.submit(() -> listenLoop(callback));
    }

    private void listenLoop(Consumer<Version> callback) {
        while (listening.get()) {
            try {
                Connection conn = open();
                this.listenConnection = conn;
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                log.info("config: LISTEN {} established on a dedicated connection", CHANNEL);
                PGConnection pgConn = conn.unwrap(PGConnection.class);
                while (listening.get() && !conn.isClosed()) {
                    PGNotification[] notifications = pgConn.getNotifications(5000);
                    if (notifications != null && notifications.length > 0) {
                        log.info("config: received {} notification(s) on {}, re-reading latest version",
                                notifications.length, CHANNEL);
                        try {
                            readLatest().ifPresent(callback::accept);
                        } catch (SQLException e) {
                            log.warn("config: failed to re-read latest version after notification", e);
                        }
                    }
                }
            } catch (SQLException e) {
                if (listening.get()) {
                    log.warn("config: LISTEN connection failed, retrying in 2s", e);
                    sleep(2000);
                }
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Connection open() throws SQLException {
        java.util.Properties props = new java.util.Properties();
        if (user != null) {
            props.setProperty("user", user);
        }
        if (password != null) {
            props.setProperty("password", password);
        }
        return DriverManager.getConnection(jdbcUrl, props);
    }

    @Override
    public void close() {
        listening.set(false);
        Connection conn = listenConnection;
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // best-effort close during shutdown
            }
        }
        if (listenExecutor != null) {
            listenExecutor.shutdownNow();
        }
    }
}
