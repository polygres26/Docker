package com.polygres.wire.config;

import com.polygres.wire.core.FirewallStage;
import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FirewallRuleStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FirewallRuleStore.class);
    private static final String CHANNEL = "polywire_firewall_rules_changed";

    private final ServerOptions options;
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private volatile Connection listenConnection;
    private ExecutorService listenExecutor;

    public FirewallRuleStore(ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() {
        try (Connection conn = PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS polywire_firewall_rules ("
                    + "id bigserial PRIMARY KEY, "
                    + "priority integer NOT NULL DEFAULT 100, "
                    + "action text NOT NULL CHECK (action IN ('allow', 'deny')), "
                    + "statement_type text, "
                    + "table_pattern text, "
                    + "sql_pattern text, "
                    + "enabled boolean NOT NULL DEFAULT true, "
                    + "description text, "
                    + "created_at timestamptz NOT NULL DEFAULT now())");
            st.execute("CREATE OR REPLACE FUNCTION polywire_firewall_rules_notify() RETURNS trigger AS $notify$ "
                    + "BEGIN PERFORM pg_notify('" + CHANNEL + "', ''); RETURN NULL; END; "
                    + "$notify$ LANGUAGE plpgsql");
            st.execute("DROP TRIGGER IF EXISTS polywire_firewall_rules_notify_trigger ON polywire_firewall_rules");
            st.execute("CREATE TRIGGER polywire_firewall_rules_notify_trigger "
                    + "AFTER INSERT OR UPDATE OR DELETE ON polywire_firewall_rules "
                    + "FOR EACH STATEMENT EXECUTE FUNCTION polywire_firewall_rules_notify()");
        } catch (SQLException e) {
            log.warn("FirewallRuleStore: could not ensure polywire_firewall_rules schema -- "
                    + "the firewall stage will run with zero rules (default ALLOW) until this is fixed: {}",
                    e.getMessage());
        }
    }

    /** Admin-facing row shape (raw DB values, not compiled Patterns) -- for the admin API/UI, not the pipeline. */
    public record AdminRow(long id, int priority, String action, String statementType, String tablePattern,
            String sqlPattern, boolean enabled, String description, String createdAt) {
    }

    public List<AdminRow> listAll() throws SQLException {
        List<AdminRow> rows = new ArrayList<>();
        try (Connection conn = PgConnections.open(options);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT id, priority, action, statement_type, table_pattern, sql_pattern, enabled, "
                                + "description, created_at FROM polywire_firewall_rules ORDER BY priority, id")) {
            while (rs.next()) {
                rows.add(new AdminRow(rs.getLong("id"), rs.getInt("priority"), rs.getString("action"),
                        rs.getString("statement_type"), rs.getString("table_pattern"), rs.getString("sql_pattern"),
                        rs.getBoolean("enabled"), rs.getString("description"),
                        String.valueOf(rs.getTimestamp("created_at"))));
            }
        }
        return rows;
    }

    public long insert(int priority, String action, String statementType, String tablePattern, String sqlPattern,
            boolean enabled, String description) throws SQLException {
        try (Connection conn = PgConnections.open(options);
                java.sql.PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO polywire_firewall_rules (priority, action, statement_type, table_pattern, "
                                + "sql_pattern, enabled, description) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id")) {
            ps.setInt(1, priority);
            ps.setString(2, action);
            ps.setString(3, statementType);
            ps.setString(4, tablePattern);
            ps.setString(5, sqlPattern);
            ps.setBoolean(6, enabled);
            ps.setString(7, description);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public boolean update(long id, int priority, String action, String statementType, String tablePattern,
            String sqlPattern, boolean enabled, String description) throws SQLException {
        try (Connection conn = PgConnections.open(options);
                java.sql.PreparedStatement ps = conn.prepareStatement(
                        "UPDATE polywire_firewall_rules SET priority = ?, action = ?, statement_type = ?, "
                                + "table_pattern = ?, sql_pattern = ?, enabled = ?, description = ? WHERE id = ?")) {
            ps.setInt(1, priority);
            ps.setString(2, action);
            ps.setString(3, statementType);
            ps.setString(4, tablePattern);
            ps.setString(5, sqlPattern);
            ps.setBoolean(6, enabled);
            ps.setString(7, description);
            ps.setLong(8, id);
            return ps.executeUpdate() > 0;
        }
    }

    public boolean delete(long id) throws SQLException {
        try (Connection conn = PgConnections.open(options);
                java.sql.PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM polywire_firewall_rules WHERE id = ?")) {
            ps.setLong(1, id);
            return ps.executeUpdate() > 0;
        }
    }

    public List<FirewallStage.Rule> readRules() throws SQLException {
        List<FirewallStage.Rule> rules = new ArrayList<>();
        try (Connection conn = PgConnections.open(options);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT id, priority, action, statement_type, table_pattern, sql_pattern, description "
                                + "FROM polywire_firewall_rules WHERE enabled ORDER BY priority, id")) {
            while (rs.next()) {
                long id = rs.getLong("id");
                int priority = rs.getInt("priority");
                FirewallStage.Action action = "deny".equalsIgnoreCase(rs.getString("action"))
                        ? FirewallStage.Action.DENY : FirewallStage.Action.ALLOW;
                String statementType = rs.getString("statement_type");
                Pattern tablePattern = globToPattern(rs.getString("table_pattern"));
                String rawSqlPattern = rs.getString("sql_pattern");
                Pattern sqlPattern = rawSqlPattern == null || rawSqlPattern.isBlank()
                        ? null : Pattern.compile(rawSqlPattern, Pattern.CASE_INSENSITIVE);
                String description = rs.getString("description");
                rules.add(new FirewallStage.Rule(id, priority, action, statementType, tablePattern, sqlPattern, description));
            }
        }
        return rules;
    }

    private static Pattern globToPattern(String glob) {
        if (glob == null || glob.isBlank()) {
            return null;
        }
        String[] segments = glob.split("\\.", 2);
        StringBuilder regex = new StringBuilder("^");
        if (segments.length == 2) {
            regex.append("(?:").append(translateGlobSegment(segments[0])).append("\\.)?")
                    .append(translateGlobSegment(segments[1]));
        } else {
            regex.append(translateGlobSegment(glob));
        }
        regex.append("$");
        return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
    }

    private static String translateGlobSegment(String segment) {
        StringBuilder regex = new StringBuilder();
        for (char c : segment.toCharArray()) {
            if (c == '*') {
                regex.append(".*");
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return regex.toString();
    }

    public void listen(Consumer<List<FirewallStage.Rule>> callback) {
        if (!listening.compareAndSet(false, true)) {
            throw new IllegalStateException("listen() already called on this FirewallRuleStore");
        }
        listenExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "polywire-firewall-rules-listen");
            t.setDaemon(true);
            return t;
        });
        listenExecutor.submit(() -> listenLoop(callback));
    }

    private void listenLoop(Consumer<List<FirewallStage.Rule>> callback) {
        while (listening.get()) {
            try {
                Connection conn = PgConnections.openRaw(options);
                this.listenConnection = conn;
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                log.info("firewall: LISTEN {} established on a dedicated connection", CHANNEL);
                PGConnection pgConn = conn.unwrap(PGConnection.class);
                while (listening.get() && !conn.isClosed()) {
                    PGNotification[] notifications = pgConn.getNotifications(5000);
                    if (notifications != null && notifications.length > 0) {
                        log.info("firewall: received {} notification(s) on {}, re-reading rules",
                                notifications.length, CHANNEL);
                        try {
                            callback.accept(readRules());
                        } catch (SQLException e) {
                            log.warn("firewall: failed to re-read rules after notification", e);
                        }
                    }
                }
            } catch (SQLException e) {
                if (listening.get()) {
                    log.warn("firewall: LISTEN connection failed, retrying in 2s", e);
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
        if (listenExecutor != null) {
            listenExecutor.shutdownNow();
        }
        try {
            if (listenConnection != null) {
                listenConnection.close();
            }
        } catch (SQLException ignored) {
        }
    }
}
