package com.sayonora.wire.ab;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.pgwire.PgConnections;
import com.sayonora.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Control-plane persistence of the A/B routing document: append-only versions in {@code warp_ab_routing}
 * (control-plane Postgres, like {@code warp_config}), a trigger that {@code NOTIFY}s on every insert, and a
 * LISTEN loop on each node so a change made through any node's admin API is applied on every node without a
 * restart -- the same mechanism as {@link com.sayonora.wire.config.ConfigStore}. A slow poll of the latest
 * version backs up a missed notification. Secrets inside the document are encrypted (see {@link AbTarget}).
 */
public final class AbStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AbStore.class);
    private static final String CHANNEL = "warp_ab_routing_changed";

    public record Version(long version, JsonObject doc) {
    }

    private final ServerOptions options;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Connection listenConn;
    private ExecutorService exec;

    public AbStore(ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() throws SQLException {
        try (Connection c = PgConnections.open(options); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(7351001)");
            try {
                st.execute("CREATE TABLE IF NOT EXISTS warp_ab_routing (version bigserial PRIMARY KEY, "
                        + "payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now())");
                st.execute("CREATE OR REPLACE FUNCTION warp_ab_routing_notify() RETURNS trigger AS $$ "
                        + "BEGIN PERFORM pg_notify('" + CHANNEL + "', NEW.version::text); RETURN NEW; END; $$ LANGUAGE plpgsql");
                st.execute("DROP TRIGGER IF EXISTS warp_ab_routing_notify_trigger ON warp_ab_routing");
                st.execute("CREATE TRIGGER warp_ab_routing_notify_trigger AFTER INSERT ON warp_ab_routing "
                        + "FOR EACH ROW EXECUTE FUNCTION warp_ab_routing_notify()");
            } finally {
                st.execute("SELECT pg_advisory_unlock(7351001)");
            }
        }
    }

    public Optional<Version> readLatest() throws SQLException {
        try (Connection c = PgConnections.open(options); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT version, payload::text FROM warp_ab_routing ORDER BY version DESC LIMIT 1")) {
            if (!rs.next()) {
                return Optional.empty();
            }
            return Optional.of(new Version(rs.getLong(1), JsonParser.parseString(rs.getString(2)).getAsJsonObject()));
        }
    }

    /**
     * Read-modify-write of the STORED document (secrets still encrypted), serialised across nodes by an advisory
     * lock. {@code mutator} receives the stored document and returns the new one (or throws to abort).
     */
    public Version mutate(UnaryOperator<JsonObject> mutator) throws SQLException {
        try (Connection c = PgConnections.open(options)) {
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.execute("SELECT pg_advisory_xact_lock(7351002)");
                JsonObject cur = new JsonObject();
                try (ResultSet rs = st.executeQuery("SELECT payload::text FROM warp_ab_routing ORDER BY version DESC LIMIT 1")) {
                    if (rs.next()) {
                        cur = JsonParser.parseString(rs.getString(1)).getAsJsonObject();
                    }
                }
                JsonObject next = mutator.apply(cur);
                long v;
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_ab_routing (payload) VALUES (?::jsonb) RETURNING version")) {
                    ps.setString(1, next.toString());
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        v = rs.getLong(1);
                    }
                }
                c.commit();
                return new Version(v, next);
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                throw e;
            }
        }
    }

    public void listen(Consumer<Version> onChange) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        exec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "warp-ab-routing-listen");
            t.setDaemon(true);
            return t;
        });
        exec.submit(() -> loop(onChange));
    }

    private void loop(Consumer<Version> onChange) {
        long lastSeen = -1;
        while (running.get()) {
            try {
                Connection conn = PgConnections.openRaw(options);
                listenConn = conn;
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                PGConnection pg = conn.unwrap(PGConnection.class);
                int idle = 0;
                while (running.get() && !conn.isClosed()) {
                    PGNotification[] n = pg.getNotifications(2000);
                    boolean poll = n != null && n.length > 0;
                    if (!poll && ++idle >= 5) { // ~10s safety poll in case a notification was missed
                        poll = true;
                    }
                    if (poll) {
                        idle = 0;
                        Optional<Version> v = readLatest();
                        if (v.isPresent() && v.get().version() != lastSeen) {
                            lastSeen = v.get().version();
                            onChange.accept(v.get());
                        }
                    }
                }
            } catch (SQLException | RuntimeException e) {
                if (running.get()) {
                    log.warn("ab-routing: LISTEN connection failed, retrying in 2s: {}", e.getMessage());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        Connection c = listenConn;
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignored) {
                // closing
            }
        }
        if (exec != null) {
            exec.shutdownNow();
        }
    }
}
