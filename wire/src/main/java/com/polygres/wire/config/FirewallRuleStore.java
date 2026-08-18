package com.polygres.wire.config;

import com.polygres.wire.core.FirewallStage;
import java.sql.Connection;
import java.sql.DriverManager;
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

/**
 * The literal "policies come from the Postgres database server itself" answer -- unlike {@code
 * polywire_config} (an insert-only versioned JSON blob, right for config that changes as one
 * atomic unit), firewall rules are naturally a *set* a DBA adds, removes, and toggles
 * independently, so this is a plain mutable table a DBA manages with ordinary {@code INSERT}/
 * {@code UPDATE}/{@code DELETE} -- the same mental model as {@code GRANT}/{@code REVOKE} or
 * {@code pg_hba.conf}, not a config file or env var to learn.
 *
 * <h2>Schema</h2>
 * <pre>{@code
 * CREATE TABLE polywire_firewall_rules (
 *     id             bigserial PRIMARY KEY,
 *     priority       integer NOT NULL DEFAULT 100,  -- lower evaluated first
 *     action         text NOT NULL CHECK (action IN ('allow','deny')),
 *     statement_type text,     -- SELECT/INSERT/UPDATE/DELETE/DROP/TRUNCATE/ALTER/CREATE/GRANT/
 *                               -- REVOKE/..., or NULL/'ANY' for every statement type
 *     table_pattern  text,     -- glob ('*' wildcard), e.g. 'public.orders' or 'public.*';
 *                               -- matched against the statement's real referenced tables, not
 *                               -- raw SQL text -- NULL matches regardless of tables touched
 *     sql_pattern    text,     -- raw regex escape hatch for the rare rule statement_type/
 *                               -- table_pattern alone can't express; NULL unless needed
 *     enabled        boolean NOT NULL DEFAULT true,
 *     description    text,
 *     created_at     timestamptz NOT NULL DEFAULT now()
 * );
 * }</pre>
 *
 * <h2>Example -- exactly the setup a DBA would write</h2>
 * <pre>{@code
 * INSERT INTO polywire_firewall_rules (priority, action, statement_type, description) VALUES
 *   (10, 'deny', 'DROP', 'no DROP from any client, ever'),
 *   (10, 'deny', 'TRUNCATE', 'no TRUNCATE from any client, ever');
 * INSERT INTO polywire_firewall_rules (priority, action, statement_type, table_pattern, description) VALUES
 *   (20, 'deny', 'DELETE', 'public.orders', 'orders are archived, never hard-deleted');
 * }</pre>
 * Takes effect within seconds, no PolyWire restart -- a trigger on the table itself calls {@code
 * pg_notify}, so a plain {@code INSERT}/{@code UPDATE}/{@code DELETE} is all a DBA ever needs to
 * run; they never need to know {@code LISTEN}/{@code NOTIFY} exists.
 *
 * <h2>LISTEN/NOTIFY propagation</h2>
 * Same shape as {@link ConfigStore}'s own LISTEN loop: a dedicated connection held open for the
 * process lifetime (Postgres's LISTEN state is per-session, so this can't be a pooled connection),
 * blocking on {@code PGConnection.getNotifications(5000)} and re-reading the whole rule set on any
 * notification.
 */
public final class FirewallRuleStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FirewallRuleStore.class);
    private static final String CHANNEL = "polywire_firewall_rules_changed";

    private final String pgHost;
    private final int pgPort;
    private final String pgDatabase;
    private final String pgUser;
    private final String pgPassword;
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private volatile Connection listenConnection;
    private ExecutorService listenExecutor;

    public FirewallRuleStore(String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword) {
        this.pgHost = pgHost;
        this.pgPort = pgPort;
        this.pgDatabase = pgDatabase;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
    }

    public void ensureSchema() {
        try (Connection conn = open(); Statement st = conn.createStatement()) {
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

    /** One-shot read of every enabled rule, ordered by {@code priority} then {@code id} -- used for the initial load and after every LISTEN notification. */
    public List<FirewallStage.Rule> readRules() throws SQLException {
        List<FirewallStage.Rule> rules = new ArrayList<>();
        try (Connection conn = open();
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

    /**
     * {@code null} in, {@code null} out (matches regardless of table) -- otherwise a simple glob
     * ({@code *} wildcard only, e.g. {@code "public.*"}), not a full regex; keeps the DBA-facing
     * surface intuitive.
     *
     * <p><b>The schema segment of a {@code schema.table} pattern is optional to match against</b>
     * -- found live: a rule written as {@code table_pattern = 'public.orders'} failed to match a
     * real {@code DELETE FROM orders} statement, since most real SQL doesn't schema-qualify table
     * references at all (it relies on {@code search_path}), so {@link SqlTableReferences}
     * extracts the bare name {@code "orders"}, not {@code "public.orders"} -- an anchored
     * full-string match against the qualified pattern never fired. A DBA writing a schema-
     * qualified pattern almost always means "this table, regardless of whether the client
     * happened to qualify it," not "only when literally schema-qualified in the SQL text" -- so
     * only the first ({@code schema}) segment is made optional, keeping {@code table_pattern}
     * matching either a bare or a qualified reference to the same table.
     */
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

    /** Starts a background LISTEN loop; {@code callback} is invoked with the freshly re-read rule list every time the table changes. Call at most once. */
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
                Connection conn = open();
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

    private Connection open() throws SQLException {
        java.util.Properties props = new java.util.Properties();
        if (pgUser != null) {
            props.setProperty("user", pgUser);
        }
        if (pgPassword != null) {
            props.setProperty("password", pgPassword);
        }
        return DriverManager.getConnection("jdbc:postgresql://" + pgHost + ":" + pgPort + "/" + pgDatabase, props);
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
