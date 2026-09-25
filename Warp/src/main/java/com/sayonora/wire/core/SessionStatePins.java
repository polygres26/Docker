package com.sayonora.wire.core;

import java.util.regex.Pattern;

/**
 * The explicit list of statements that create backend session state and therefore pin a
 * {@link SessionConnectionLease} for the rest of the session (irreversible: Warp cannot prove the state is
 * gone). Everything else -- plain SELECT/INSERT/UPDATE/DELETE, DDL, {@code SET LOCAL}, transaction-scoped
 * locks -- leaves nothing behind once its statement (or transaction) ends.
 *
 * <p>Matching is textual and deliberately conservative: a false positive only costs multiplexing
 * efficiency (the session keeps its connection), a false negative would silently lose state.
 *
 * <p>Pin triggers, by dialect of the CLIENT's SQL:
 * <ul>
 *   <li>Postgres: non-LOCAL {@code SET}, {@code SET ROLE}, {@code SET SESSION AUTHORIZATION},
 *       {@code SET SESSION CHARACTERISTICS}, {@code RESET}, {@code DISCARD}, {@code LISTEN}/{@code UNLISTEN},
 *       SQL-level {@code PREPARE}, {@code CREATE TEMP/TEMPORARY ...}, {@code SELECT ... INTO TEMP},
 *       {@code pg_advisory_lock*} / {@code pg_try_advisory_lock*} (session-level), {@code set_config},
 *       {@code nextval}/{@code currval}/{@code lastval}, {@code LOAD}.</li>
 *   <li>Every dialect: the same function/temp-table triggers, plus MySQL {@code LAST_INSERT_ID()}, SQL Server
 *       {@code #temp} tables / {@code SCOPE_IDENTITY()} / {@code @@IDENTITY}, Oracle {@code seq.NEXTVAL/CURRVAL},
 *       {@code DBMS_OUTPUT}, {@code DBMS_SESSION}, {@code ALTER SESSION}, global temporary tables.</li>
 * </ul>
 * Cursors ({@code DECLARE ... CURSOR}) are reversible and handled by {@link #isDeclareCursor} /
 * {@link #isCloseCursor}, not here.
 */
public final class SessionStatePins {

    private static final Pattern SET_SESSION_STATE = Pattern.compile(
            "^\\s*set\\s+(?!local\\b|transaction\\b|constraints\\b)", Pattern.CASE_INSENSITIVE);
    private static final Pattern VERB = Pattern.compile(
            "^\\s*(?:reset|discard|listen|unlisten|prepare(?!\\s+transaction\\b)|load|alter\\s+session"
                    + "|create\\s+(?:global\\s+|local\\s+)?temp(?:orary)?)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DECLARE_HOLD = Pattern.compile(
            "^\\s*declare\\b[^;]*\\bwith\\s+hold\\b", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SELECT_INTO_TEMP = Pattern.compile(
            "\\binto\\s+(?:temp|temporary|unlogged\\s+temp)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern STATE_FUNCTION = Pattern.compile(
            "\\b(?:nextval|currval|lastval|set_config|pg_advisory_lock|pg_try_advisory_lock"
                    + "|pg_advisory_lock_shared|pg_try_advisory_lock_shared|dbms_output|dbms_session"
                    + "|last_insert_id|scope_identity|ident_current)\\b|@@identity\\b|\\.(?:nextval|currval)\\b"
                    + "|(?<![\\w'\"$])#{1,2}[A-Za-z_]",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DECLARE_CURSOR = Pattern.compile(
            "^\\s*declare\\s+\\w+\\s+(?:\\w+\\s+)*cursor\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern CLOSE_CURSOR = Pattern.compile(
            "^\\s*close\\s+(?:all\\b|\\w+)\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_OR_WITH = Pattern.compile("^\\s*(?:select|with)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern WRITES_OR_LOCKS = Pattern.compile(
            "\\b(?:insert|update|delete|merge|for\\s+update|for\\s+share|for\\s+no\\s+key\\s+update"
                    + "|for\\s+key\\s+share|lock\\s+table)\\b|\\binto\\b", Pattern.CASE_INSENSITIVE);

    /** A plain session setting ({@code SET [SESSION] name = value}, {@code SET TIME ZONE x}) that Warp can
     * replay on whichever physical connection the session gets next, so it does NOT need to pin. */
    public record ReplayableSetting(String name, String sql) {
    }

    private static final Pattern SET_HEAD = Pattern.compile(
            "^\\s*set\\s+(?:session\\s+)?(?!local\\b|role\\b|authorization\\b|characteristics\\b|transaction\\b"
                    + "|constraints\\b|names\\b|schema\\b|seed\\b|tablespace\\b|xml\\b)(.+?)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SET_TIME_ZONE = Pattern.compile("^time\\s+zone\\s+(.+)$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SET_NAME_VALUE = Pattern.compile(
            "^([a-z_][a-z0-9_.]*)\\s*(?:=|\\bto\\b)\\s*(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern RESET_TARGET = Pattern.compile(
            "^\\s*reset\\s+(all|[a-z_][a-z0-9_.]*|time\\s+zone)\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    /** @return the setting {@code sql} changes if it is a replayable session {@code SET}, else {@code null}
     *     (which then falls under {@link #sessionStateReason} and pins). Only meaningful OUTSIDE a transaction:
     *     inside one the change would be rolled back with it, so callers pin instead. */
    public static ReplayableSetting replayableSetting(String sql) {
        if (sql == null) {
            return null;
        }
        java.util.regex.Matcher head = SET_HEAD.matcher(sql);
        if (!head.matches()) {
            return null;
        }
        String rest = head.group(1);
        String name;
        String value;
        java.util.regex.Matcher tz = SET_TIME_ZONE.matcher(rest);
        java.util.regex.Matcher nv = SET_NAME_VALUE.matcher(rest);
        if (tz.matches()) {
            name = "timezone";
            value = tz.group(1);
        } else if (nv.matches()) {
            name = normalizeSettingName(nv.group(1));
            value = nv.group(2);
        } else {
            return null;
        }
        // several statements in one string, or "SET x FROM CURRENT" (depends on the current value): pin instead
        if (value.indexOf(';') >= 0 || value.regionMatches(true, 0, "from ", 0, 5)) {
            return null;
        }
        return new ReplayableSetting(name, sql.strip());
    }

    /** {@code RESET name} / {@code RESET ALL}: the setting name, {@code "*"} for ALL, else {@code null}. */
    public static String resetTarget(String sql) {
        if (sql == null) {
            return null;
        }
        java.util.regex.Matcher m = RESET_TARGET.matcher(sql);
        if (!m.matches()) {
            return null;
        }
        return m.group(1).equalsIgnoreCase("all") ? "*" : normalizeSettingName(m.group(1));
    }

    private static String normalizeSettingName(String raw) {
        String name = raw.trim().toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", " ");
        return name.equals("time zone") ? "timezone" : name;
    }

    /** @return why {@code sql} creates session state that must stay on one physical connection, or
     *     {@code null} if it does not. */
    public static String sessionStateReason(SourceDialect dialect, String sql) {
        if (sql == null) {
            return null;
        }
        if (dialect == SourceDialect.POSTGRES && SET_SESSION_STATE.matcher(sql).find()) {
            return "session SET/SET ROLE";
        }
        if (VERB.matcher(sql).find()) {
            return "session-scoped statement (RESET/DISCARD/LISTEN/PREPARE/temp table/ALTER SESSION)";
        }
        if (SELECT_INTO_TEMP.matcher(sql).find()) {
            return "temp table";
        }
        if (DECLARE_HOLD.matcher(sql).find()) {
            return "cursor WITH HOLD (survives commit)";
        }
        if (STATE_FUNCTION.matcher(sql).find()) {
            return "session-scoped function (sequence currval/nextval, advisory lock, set_config, "
                    + "LAST_INSERT_ID/SCOPE_IDENTITY, temp table, DBMS_*)";
        }
        return null;
    }

    public static boolean isDeclareCursor(String sql) {
        return sql != null && DECLARE_CURSOR.matcher(sql).find();
    }

    public static boolean isCloseCursor(String sql) {
        return sql != null && CLOSE_CURSOR.matcher(sql).matches();
    }

    /** True for a plain read (SELECT/WITH without DML, locking clauses or INTO) that leaves no
     * transaction-scoped effect behind, so it can run in autocommit and release the connection. Anything
     * else is treated as (potentially) writing. */
    public static boolean isPureRead(SourceDialect dialect, String sql) {
        return sql != null && SELECT_OR_WITH.matcher(sql).find() && !WRITES_OR_LOCKS.matcher(sql).find()
                && sessionStateReason(dialect, sql) == null;
    }

    private SessionStatePins() {
    }
}
