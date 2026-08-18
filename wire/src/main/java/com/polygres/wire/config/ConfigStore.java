package com.polygres.wire.config;

import java.sql.Connection;
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

public final class ConfigStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConfigStore.class);
    private static final String CHANNEL = "polywire_config_changed";

    public record Version(long version, PolyWireConfig payload, java.time.Instant createdAt) {
    }

    private final com.polygres.wire.server.ServerOptions options;
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private volatile Connection listenConnection;
    private ExecutorService listenExecutor;

    public ConfigStore(com.polygres.wire.server.ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() throws SQLException {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
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

    public long write(PolyWireConfig config) throws SQLException {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options);
                java.sql.PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO polywire_config (payload) VALUES (?::jsonb) RETURNING version")) {
            ps.setString(1, config.toJson());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public Optional<Version> readLatest() throws SQLException {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement();
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
                Connection conn = com.polygres.wire.pgwire.PgConnections.openRaw(options);
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

    @Override
    public void close() {
        listening.set(false);
        Connection conn = listenConnection;
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                
            }
        }
        if (listenExecutor != null) {
            listenExecutor.shutdownNow();
        }
    }
}
