package com.polygres.wire.orawire.translator;

import java.util.regex.Pattern;

/**
 * Strips Oracle's "FROM DUAL" pseudo-table reference, which has no Postgres
 * equivalent (Postgres allows constant SELECTs with no FROM clause at all).
 *
 * Deliberately narrow: this server has no general Oracle-to-Postgres SQL
 * dialect translator (confirmed — searched the whole tree, only
 * {@link BindVariableRewriter} exists, and it only handles bind-placeholder
 * syntax). Full dialect translation is a large, separate feature; this
 * handles just the one idiom virtually every client driver's own
 * validation/health-check query uses ("SELECT 1 FROM DUAL" or equivalent),
 * which was otherwise blocking any query test at all — confirmed live: the
 * Postgres backend rejects it outright with "ERROR: relation \"dual\" does
 * not exist".
 */
public final class DualTableRewriter {

    // Matches "dual" as a whole word (not e.g. "individual"), case-insensitive,
    // immediately preceded by "from" and whitespace. Only strips the "dual"
    // token itself, leaving the FROM keyword and surrounding SQL intact — a
    // bare "SELECT 1 FROM" is valid Postgres, so no further rewrite is needed.
    private static final Pattern FROM_DUAL =
            Pattern.compile("(?i)(\\bfrom\\s+)dual\\b");

    public static String rewrite(String oracleSql) {
        return FROM_DUAL.matcher(oracleSql).replaceAll("$1(select 1) dual_placeholder");
    }

    private DualTableRewriter() {
    }
}
