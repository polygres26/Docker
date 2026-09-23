package com.sayonora.wire.cluster;

import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.SourceDialect;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Installs (or prints, for an operator to apply by hand) the Postgres triggers that make a write
 * NOT performed through Warp still invalidate Warp's result/row caches -- the {@code NOTIFY} half
 * of {@link CacheInvalidationListener}'s LISTEN/NOTIFY pairing. Postgres-only in v1: {@link
 * #install} refuses any other dialect outright rather than silently doing nothing.
 *
 * <p>Two trigger shapes, decided per table:
 * <ul>
 *   <li><b>table</b> (the default): one {@code AFTER INSERT OR DELETE OR UPDATE OR TRUNCATE ...
 *       FOR EACH STATEMENT} trigger ({@code warp_cache_notify_stmt}) emitting one table-only
 *       notify per statement, however many rows it touched.</li>
 *   <li><b>rows</b> (opt-in per table via {@code WARP_CACHE_INVALIDATION_ROW_TABLES}, meant for
 *       {@link RowCache}-backed tables): statement-level triggers WITH transition tables, whose
 *       function aggregates the affected keys into the notify so only those row-cache entries are
 *       dropped. Postgres refuses transition tables on a multi-event trigger, so this is one
 *       trigger per DML event ({@code warp_cache_notify_ins}/{@code _upd}/{@code _del}) plus a
 *       plain table-level one for TRUNCATE ({@code warp_cache_notify_trunc}), which has no
 *       transition tables at all. Key columns are decided HERE at install time: dynamowire's own
 *       fixed physical shape is {@code (pk_value, sk_value)}, mongowire's is {@code (id)}, and any
 *       other table uses its {@code PRIMARY KEY}; a table with no primary key can't be keyed and is
 *       forced back to table mode with a warning.</li>
 * </ul>
 *
 * <p>Idempotent: the two plpgsql functions are {@code CREATE OR REPLACE}d, and each table's
 * existing {@code warp_cache_%} triggers are compared (via {@code pg_get_triggerdef}) against
 * exactly what would be created -- an identical trigger is left alone (its {@code pg_trigger}
 * OID survives), a different or stray one is dropped and recreated. The whole install runs in
 * one transaction per backend, so a failure part-way leaves the backend exactly as it was.
 * Warp never auto-uninstalls; {@link #uninstallScript} is for the operator.
 */
public final class CacheTriggerInstaller {

    private static final Logger log = LoggerFactory.getLogger(CacheTriggerInstaller.class);

    public static final String CHANNEL = "warp_cache_invalidate";
    static final String STMT_FUNCTION = "warp_cache_notify_stmt";
    static final String ROWS_FUNCTION = "warp_cache_notify_rows";
    private static final String RESOURCE = "/com/sayonora/wire/cluster/warp_cache_notify.sql";
    private static final Pattern MARKER = Pattern.compile("(?m)^--\\s*###.*$");

    /** A cache table resolved against one real backend: its actual schema/name from {@code
     * pg_class} and, for rows mode, the key column(s) the trigger will report. */
    public record ResolvedTable(String schema, String table, boolean rowsMode, String pkColumn, String skColumn) {
        String qualified() {
            return quoteIdent(schema) + "." + quoteIdent(table);
        }
    }

    private final List<String> cacheTables;
    private final Set<String> rowTables;

    /**
     * @param cacheTables the configured result-cache tables ({@code WARP_CACHE_TABLES}, each
     *     {@code orders} or {@code public.orders}), plus dynamowire/mongowire physical tables
     *     when those row caches are on
     * @param rowTables the subset (same spellings) that should get rows-mode triggers
     */
    public CacheTriggerInstaller(Collection<String> cacheTables, Collection<String> rowTables) {
        this.cacheTables = new ArrayList<>(new java.util.LinkedHashSet<>(cacheTables));
        this.rowTables = new java.util.HashSet<>();
        for (String t : rowTables) {
            this.rowTables.add(stripQuotes(t).toLowerCase(Locale.ROOT));
        }
    }

    /** Splits a comma-separated table list the same way {@link CacheStage#fromConfig} does. */
    public static List<String> parseTableList(String spec) {
        List<String> out = new ArrayList<>();
        if (spec != null && !spec.isBlank()) {
            for (String entry : spec.split(",")) {
                String trimmed = entry.trim();
                if (!trimmed.isEmpty()) {
                    out.add(trimmed);
                }
            }
        }
        return out;
    }

    /** The two plpgsql function definitions, verbatim from the shared SQL resource. */
    public static List<String> functionDefinitions() {
        String raw;
        try (InputStream in = CacheTriggerInstaller.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + RESOURCE);
            }
            raw = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read " + RESOURCE, e);
        }
        String[] chunks = MARKER.split(raw);
        List<String> out = new ArrayList<>();
        for (int i = 1; i < chunks.length; i++) {
            String chunk = chunks[i].strip();
            if (!chunk.isEmpty()) {
                out.add(chunk);
            }
        }
        return out;
    }

    // ---- connection ---------------------------------------------------------------------------

    /**
     * A dedicated, NON-pooled JDBC connection to {@code target} -- shared with {@link
     * CacheInvalidationListener}, which holds one open forever per backend. Deliberately not
     * {@code BackendConnectionPools}: a pooled connection parked in LISTEN for the process's
     * whole life would permanently consume one of the pool's slots (see the sizing comment in
     * {@code BackendConnectionPools}). The implicit default backend goes through {@link
     * com.sayonora.wire.pgwire.PgConnections#openRaw} so it inherits failover/SSL settings; a
     * named backend opens its own URL directly, resolving a {@code vault:}/{@code cyberark:}
     * password reference the same way {@link BackendTarget#open} does.
     */
    public static Connection openDedicatedConnection(BackendTarget target) throws SQLException {
        // tcpKeepAlive: a LISTEN connection sends nothing for hours at a time, which is exactly
        // the traffic pattern a NAT/firewall/load balancer silently drops -- without keepalives
        // the listener would sit in getNotifications forever, hearing nothing, believing it's
        // connected, while every notify goes to a socket nobody reads.
        Properties props = new Properties();
        props.setProperty("tcpKeepAlive", "true");
        Connection conn;
        if (target.failoverOptions() != null) {
            conn = com.sayonora.wire.pgwire.PgConnections.openRaw(target.failoverOptions(), props);
        } else {
            if (target.user() != null) {
                props.setProperty("user", target.user());
            }
            if (target.password() != null) {
                props.setProperty("password", com.sayonora.wire.secrets.SecretResolver.resolve(target.password()));
            }
            conn = DriverManager.getConnection(target.jdbcUrl(), props);
        }
        return conn;
    }

    private static void requirePostgres(BackendTarget target) {
        if (target.dialect() != SourceDialect.POSTGRES) {
            throw new IllegalArgumentException("cache invalidation triggers are Postgres-only in this version -- backend \""
                    + target.name() + "\" is " + (target.dialect() == null ? "an unrecognized JDBC URL" : target.dialect())
                    + " (" + target.jdbcUrl() + "). Oracle (DBMS_ALERT/CQN), MySQL (binlog) and SQL Server "
                    + "(Query Notifications) are future work; that backend's cached results expire by TTL only.");
        }
    }

    // ---- resolution ---------------------------------------------------------------------------

    /**
     * Looks each configured table up in {@code pg_class}/{@code pg_namespace} on {@code conn} --
     * an unqualified name resolves through the connection's own {@code search_path}, exactly the
     * way the SQL a client sends through Warp would -- and, for rows-mode tables, picks the key
     * columns. A table that doesn't exist on this backend is skipped with an INFO log (one
     * {@code WARP_CACHE_TABLES} list serves every backend, and not every table lives on every
     * backend).
     */
    public List<ResolvedTable> resolve(Connection conn) throws SQLException {
        List<ResolvedTable> out = new ArrayList<>();
        for (String raw : cacheTables) {
            String[] parts = splitQualified(raw);
            String schema = parts[0];
            String table = parts[1];
            String[] found = lookupTable(conn, schema, table);
            if (found == null) {
                log.info("cache triggers: \"{}\" does not exist on this backend -- skipping it here", raw);
                continue;
            }
            boolean rows = rowTables.contains(stripQuotes(raw).toLowerCase(Locale.ROOT))
                    || rowTables.contains((found[0] + "." + found[1]).toLowerCase(Locale.ROOT))
                    || (schema == null && rowTables.contains(found[1].toLowerCase(Locale.ROOT)));
            String pk = null;
            String sk = null;
            if (rows) {
                String[] key = keyColumns(conn, found[0], found[1]);
                if (key == null) {
                    log.warn("cache triggers: \"{}\" is listed in WARP_CACHE_INVALIDATION_ROW_TABLES but has no "
                            + "PRIMARY KEY (and isn't a dynamowire/mongowire table) -- there's no key to report, "
                            + "so it gets a plain table-level trigger instead", raw);
                    rows = false;
                } else {
                    pk = key[0];
                    sk = key.length > 1 ? key[1] : null;
                }
            }
            out.add(new ResolvedTable(found[0], found[1], rows, pk, sk));
        }
        return out;
    }

    /** @return {@code {schemaOrNull, table}}, quotes stripped, case preserved when quoted and
     *     lower-cased when not -- the same folding Postgres itself applies to an identifier. */
    static String[] splitQualified(String raw) {
        String s = raw.trim();
        int dot = -1;
        boolean inQuote = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                inQuote = !inQuote;
            } else if (c == '.' && !inQuote) {
                dot = i;
                break;
            }
        }
        if (dot < 0) {
            return new String[] {null, foldIdent(s)};
        }
        return new String[] {foldIdent(s.substring(0, dot)), foldIdent(s.substring(dot + 1))};
    }

    private static String foldIdent(String ident) {
        String t = ident.trim();
        if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
            return t.substring(1, t.length() - 1).replace("\"\"", "\"");
        }
        return t.toLowerCase(Locale.ROOT);
    }

    private static String stripQuotes(String s) {
        return s.replace("\"", "").trim();
    }

    private static String[] lookupTable(Connection conn, String schema, String table) throws SQLException {
        String sql = schema == null
                ? "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                        + "WHERE c.relname = ? AND c.relkind IN ('r','p') AND pg_catalog.pg_table_is_visible(c.oid)"
                : "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                        + "WHERE c.relname = ? AND n.nspname = ? AND c.relkind IN ('r','p')";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            if (schema != null) {
                ps.setString(2, schema);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new String[] {rs.getString(1), rs.getString(2)} : null;
            }
        }
    }

    /** dynamowire shape wins, then mongowire's, then the real PRIMARY KEY (single or composite --
     * only its first two columns are reported, since {@link RowCache#key} has exactly pk|sk). */
    private static String[] keyColumns(Connection conn, String schema, String table) throws SQLException {
        Set<String> columns = new java.util.HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString(1));
                }
            }
        }
        if (columns.contains("pk_value") && columns.contains("sk_value") && columns.contains("item")) {
            return new String[] {"pk_value", "sk_value"};
        }
        if (columns.contains("id") && columns.contains("doc") && columns.size() == 2) {
            return new String[] {"id"};
        }
        List<String> pk = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT a.attname FROM pg_catalog.pg_index i "
                        + "JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                        + "WHERE i.indrelid = (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                        + "ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = ?) AND i.indisprimary "
                        + "ORDER BY array_position(i.indkey, a.attnum)")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pk.add(rs.getString(1));
                }
            }
        }
        if (pk.isEmpty()) {
            return null;
        }
        return pk.size() == 1 ? new String[] {pk.get(0)} : new String[] {pk.get(0), pk.get(1)};
    }

    // ---- DDL generation -----------------------------------------------------------------------

    /**
     * Every {@code CREATE TRIGGER} for {@code t}, keyed by trigger name, in EXACTLY the canonical
     * spelling {@code pg_get_triggerdef} produces (event order INSERT/DELETE/UPDATE/TRUNCATE, the
     * relation always schema-qualified, a space after each argument comma) -- so {@link #install}
     * can compare the two strings verbatim to decide whether an existing trigger is already right.
     */
    static Map<String, String> desiredTriggers(ResolvedTable t) {
        Map<String, String> out = new LinkedHashMap<>();
        String on = " ON " + t.qualified();
        if (!t.rowsMode()) {
            out.put(STMT_FUNCTION, "CREATE TRIGGER " + STMT_FUNCTION + " AFTER INSERT OR DELETE OR UPDATE OR TRUNCATE"
                    + on + " FOR EACH STATEMENT EXECUTE FUNCTION " + STMT_FUNCTION + "()");
            return out;
        }
        String args = t.skColumn() == null
                ? "(" + quoteLiteral(t.pkColumn()) + ")"
                : "(" + quoteLiteral(t.pkColumn()) + ", " + quoteLiteral(t.skColumn()) + ")";
        out.put("warp_cache_notify_ins", "CREATE TRIGGER warp_cache_notify_ins AFTER INSERT" + on
                + " REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION " + ROWS_FUNCTION + args);
        out.put("warp_cache_notify_upd", "CREATE TRIGGER warp_cache_notify_upd AFTER UPDATE" + on
                + " REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION " + ROWS_FUNCTION + args);
        out.put("warp_cache_notify_del", "CREATE TRIGGER warp_cache_notify_del AFTER DELETE" + on
                + " REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION " + ROWS_FUNCTION + args);
        out.put("warp_cache_notify_trunc", "CREATE TRIGGER warp_cache_notify_trunc AFTER TRUNCATE" + on
                + " FOR EACH STATEMENT EXECUTE FUNCTION " + STMT_FUNCTION + "()");
        return out;
    }

    /** The full, hand-applicable install script for the tables that exist on {@code conn}. */
    public String installScript(Connection conn) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Warp out-of-band cache invalidation: install script (WARP_CACHE_INVALIDATION=listen)\n");
        sb.append("-- Privileges needed: CREATE on the function schema (search_path's first schema),\n");
        sb.append("-- TRIGGER on each table below. Safe to re-run; Warp never uninstalls these itself.\n");
        sb.append("BEGIN;\n\n");
        for (String fn : functionDefinitions()) {
            sb.append(fn).append(";\n\n");
        }
        for (ResolvedTable t : resolve(conn)) {
            sb.append("-- ").append(t.qualified()).append(" (").append(t.rowsMode() ? "rows" : "table").append(" mode)\n");
            for (String name : allTriggerNames()) {
                sb.append("DROP TRIGGER IF EXISTS ").append(name).append(" ON ").append(t.qualified()).append(";\n");
            }
            for (String ddl : desiredTriggers(t).values()) {
                sb.append(ddl).append(";\n");
            }
            sb.append('\n');
        }
        sb.append("COMMIT;\n");
        return sb.toString();
    }

    /** Drops every trigger this class ever creates on the configured tables, then the two
     * functions. Never run by Warp itself -- {@code --print-cache-triggers-uninstall} only. */
    public String uninstallScript(Connection conn) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Warp out-of-band cache invalidation: uninstall script\n");
        sb.append("BEGIN;\n\n");
        for (ResolvedTable t : resolve(conn)) {
            for (String name : allTriggerNames()) {
                sb.append("DROP TRIGGER IF EXISTS ").append(name).append(" ON ").append(t.qualified()).append(";\n");
            }
        }
        sb.append("\nDROP FUNCTION IF EXISTS ").append(STMT_FUNCTION).append("();\n");
        sb.append("DROP FUNCTION IF EXISTS ").append(ROWS_FUNCTION).append("();\n");
        sb.append("COMMIT;\n");
        return sb.toString();
    }

    private static List<String> allTriggerNames() {
        return List.of(STMT_FUNCTION, "warp_cache_notify_ins", "warp_cache_notify_upd",
                "warp_cache_notify_del", "warp_cache_notify_trunc");
    }

    // ---- install ------------------------------------------------------------------------------

    /**
     * Installs on {@code target}, in one transaction. A failure -- most likely a missing {@code
     * TRIGGER} privilege on a table or {@code CREATE} on the function schema -- surfaces as an
     * {@link SQLException} whose message names exactly those privileges and the failing SQL, so
     * the ERROR {@code Main} logs is actionable without reading this code.
     *
     * @return how many triggers were actually created (0 means everything was already in place)
     */
    public int install(BackendTarget target) throws SQLException {
        requirePostgres(target);
        try (Connection conn = openDedicatedConnection(target)) {
            return install(conn);
        }
    }

    int install(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        String current = null;
        try {
            try (Statement st = conn.createStatement()) {
                for (String fn : functionDefinitions()) {
                    current = fn;
                    st.execute(fn);
                }
            }
            int created = 0;
            for (ResolvedTable t : resolve(conn)) {
                Map<String, String> desired = desiredTriggers(t);
                Map<String, String> existing = existingTriggers(conn, t);
                try (Statement st = conn.createStatement()) {
                    // Anything of ours that's not wanted any more (e.g. a table that moved from
                    // rows mode back to table mode) or that differs from the canonical DDL goes.
                    for (Map.Entry<String, String> e : existing.entrySet()) {
                        String want = desired.get(e.getKey());
                        if (want == null || !want.equals(e.getValue())) {
                            current = "DROP TRIGGER IF EXISTS " + e.getKey() + " ON " + t.qualified();
                            st.execute(current);
                        }
                    }
                    for (Map.Entry<String, String> e : desired.entrySet()) {
                        if (e.getValue().equals(existing.get(e.getKey()))) {
                            continue;
                        }
                        current = e.getValue();
                        st.execute(current);
                        created++;
                    }
                }
            }
            conn.commit();
            return created;
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
                // the original failure is what matters
            }
            throw new SQLException("cache trigger install failed (nothing was changed -- rolled back). The "
                    + "connecting role needs the TRIGGER privilege on every WARP_CACHE_TABLES table and CREATE on the "
                    + "schema the functions land in (first schema of search_path); either grant those, or run the "
                    + "script from `--print-cache-triggers` as a role that has them and switch to "
                    + "WARP_CACHE_INVALIDATION=listen. Failing statement: " + current + " -- " + e.getMessage(),
                    e.getSQLState(), e);
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException ignored) {
                // connection is being closed by the caller anyway
            }
        }
    }

    private static Map<String, String> existingTriggers(Connection conn, ResolvedTable t) throws SQLException {
        Map<String, String> out = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT tgname, pg_catalog.pg_get_triggerdef(oid) FROM pg_catalog.pg_trigger "
                        + "WHERE tgrelid = (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                        + "ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = ?) "
                        + "AND tgname LIKE 'warp\\_cache\\_%' AND NOT tgisinternal")) {
            ps.setString(1, t.schema());
            ps.setString(2, t.table());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getString(2));
                }
            }
        }
        return out;
    }

    // ---- quoting ------------------------------------------------------------------------------

    private static final Pattern SAFE_IDENT = Pattern.compile("[a-z_][a-z0-9_]*");

    /** Quotes only when Postgres itself would (upper-case or non-identifier characters) -- matching
     * {@code pg_get_triggerdef}'s own {@code quote_identifier} behaviour, so the generated DDL
     * compares equal to the catalog's rendering of it. */
    static String quoteIdent(String ident) {
        if (SAFE_IDENT.matcher(ident).matches() && !RESERVED.contains(ident)) {
            return ident;
        }
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    // The handful of reserved words that could plausibly be a table name -- quote_identifier
    // quotes ANY keyword, but a cache table named e.g. "order" or "user" is the realistic case.
    private static final Set<String> RESERVED = new TreeSet<>(List.of(
            "all", "and", "any", "as", "asc", "both", "case", "cast", "check", "column", "constraint", "create",
            "current_date", "current_time", "current_timestamp", "current_user", "default", "desc", "distinct",
            "do", "else", "end", "except", "false", "for", "foreign", "from", "grant", "group", "having", "in",
            "initially", "intersect", "into", "leading", "limit", "not", "null", "offset", "on", "only", "or",
            "order", "primary", "references", "select", "session_user", "some", "table", "then", "to", "trailing",
            "true", "union", "unique", "user", "using", "when", "where", "with"));

    private static String quoteLiteral(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
