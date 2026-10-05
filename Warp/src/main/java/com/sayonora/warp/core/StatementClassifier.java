package com.sayonora.warp.core;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Decides whether a statement is safe to run on a read replica. Deliberately conservative and
 * dialect-agnostic: the only answer that lets a statement leave the primary is "this is a plain
 * read with no locking, no session state and no side effects"; anything unrecognised stays on the
 * primary. {@link SqlMetricsCollector#classify} (first keyword only) is NOT safe for this -- it
 * calls {@code SELECT ... FOR UPDATE}, {@code SELECT nextval(...)} and a data-modifying CTE all
 * "READ".
 *
 * <p>A replica that rejects a statement as a write (Postgres {@code 25006}, MySQL error 1290 /
 * 1836) is a second, independent safety net handled by {@link ReplicaRouter}: the statement is
 * retried on the primary. This classifier exists so that net is almost never needed, and because
 * it cannot catch a side-effecting function that a replica does not reject.
 */
public final class StatementClassifier {

    private StatementClassifier() {
    }

    // Only plain SELECT and WITH ... SELECT can ever be replica-safe. SHOW/EXPLAIN are excluded on
    // purpose: SHOW exposes per-connection settings (the replica's, not the session's) and EXPLAIN
    // ANALYZE executes the statement.
    private static final Pattern LEADING = Pattern.compile("^(select|with)\\b", Pattern.CASE_INSENSITIVE);

    // Locking reads: need the primary's lock manager.
    private static final Pattern LOCKING = Pattern.compile(
            "\\bfor\\s+(no\\s+key\\s+)?update\\b|\\bfor\\s+(key\\s+)?share\\b|\\block\\s+in\\s+share\\s+mode\\b"
                    + "|\\bwith\\s*\\(\\s*(updlock|xlock|holdlock|rowlock)\\b",
            Pattern.CASE_INSENSITIVE);

    // SELECT ... INTO (creates a table / writes a file / assigns a variable), and writable CTEs.
    private static final Pattern INTO = Pattern.compile("\\binto\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern DML_KEYWORD = Pattern.compile(
            "\\b(insert|update|delete|merge|truncate|create|alter|drop|grant|revoke|call|exec|execute|copy|"
                    + "vacuum|analyze|refresh|lock|listen|notify|prepare|declare|set)\\b",
            Pattern.CASE_INSENSITIVE);

    // Functions/pseudo-columns with side effects or session-local results. A read replica may
    // not reject all of these, and the ones it does not reject would silently give wrong answers.
    private static final Pattern SIDE_EFFECT_FUNCTION = Pattern.compile(
            "\\b(nextval|setval|currval|lastval|set_config|txid_current|pg_advisory\\w*|pg_try_advisory\\w*"
                    + "|lo_\\w+|dblink\\w*|pg_notify|pg_sleep_for|pg_terminate_backend|pg_cancel_backend"
                    + "|last_insert_id|get_lock|release_lock|release_all_locks|is_free_lock|sleep_lock"
                    + "|next\\s+value\\s+for|sp_\\w+|xp_\\w+|dbms_\\w+|utl_\\w+)\\b"
                    + "|\\b\\w+\\.(nextval|currval)\\b|@@identity|scope_identity\\s*\\(|\\bsys_context\\b",
            Pattern.CASE_INSENSITIVE);

    /** {@code true} only for a statement that is certainly a plain, side-effect-free read. */
    public static boolean isReplicaSafeRead(String sql) {
        if (sql == null || sql.isBlank()) {
            return false;
        }
        String stripped = stripLiteralsAndComments(sql).strip();
        if (stripped.isEmpty()) {
            return false;
        }
        // More than one statement in one string: cannot reason about the rest.
        String noTrailing = stripped.endsWith(";") ? stripped.substring(0, stripped.length() - 1) : stripped;
        if (noTrailing.indexOf(';') >= 0) {
            return false;
        }
        if (!LEADING.matcher(noTrailing).find()) {
            return false;
        }
        if (LOCKING.matcher(noTrailing).find() || INTO.matcher(noTrailing).find()
                || SIDE_EFFECT_FUNCTION.matcher(noTrailing).find()) {
            return false;
        }
        // WITH ... INSERT/UPDATE/DELETE (writable CTE), and any DDL/DML/utility keyword anywhere.
        // A column or table literally named like a keyword is rare enough that falling back to the
        // primary is the right, safe cost.
        if (DML_KEYWORD.matcher(noTrailing).find()) {
            return false;
        }
        return true;
    }

    /** Replaces string literals, quoted identifiers and comments with spaces so keywords inside
     * them cannot trigger (or hide) a match. Handles '..' with '' escapes, "..", `..`, [..]
     * (T-SQL), -- line comments, and block comments; PostgreSQL dollar-quoting is treated as a
     * string too. An unterminated construct swallows the rest of the statement, which only ever
     * makes the result MORE conservative for the caller's checks on what remains... except that it
     * could hide a trailing keyword -- so an unterminated quote returns the empty string, which
     * the caller treats as "not safe". */
    static String stripLiteralsAndComments(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        int n = sql.length();
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int nl = sql.indexOf('\n', i);
                i = nl < 0 ? n : nl;
                out.append(' ');
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                if (end < 0) {
                    return "";
                }
                i = end + 2;
                out.append(' ');
            } else if (c == '\'' || c == '"' || c == '`') {
                int j = i + 1;
                boolean closed = false;
                while (j < n) {
                    if (sql.charAt(j) == c) {
                        if (j + 1 < n && sql.charAt(j + 1) == c) {
                            j += 2;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    j++;
                }
                if (!closed) {
                    return "";
                }
                i = j + 1;
                out.append(' ');
            } else if (c == '[') {
                int end = sql.indexOf(']', i + 1);
                if (end < 0) {
                    out.append(c);
                    i++;
                } else {
                    i = end + 1;
                    out.append(' ');
                }
            } else if (c == '$') {
                int tagEnd = sql.indexOf('$', i + 1);
                String tag = tagEnd < 0 ? null : sql.substring(i, tagEnd + 1);
                if (tag != null && tag.matches("\\$[A-Za-z_]*\\$")) {
                    int close = sql.indexOf(tag, tagEnd + 1);
                    if (close < 0) {
                        return "";
                    }
                    i = close + tag.length();
                    out.append(' ');
                } else {
                    out.append(c);
                    i++;
                }
            } else {
                out.append(Character.toLowerCase(c) == c ? c : Character.toLowerCase(c));
                i++;
            }
        }
        return out.toString().toLowerCase(Locale.ROOT);
    }
}
