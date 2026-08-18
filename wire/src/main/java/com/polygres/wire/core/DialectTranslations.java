package com.polygres.wire.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hub-and-spoke rule vocabulary behind {@link DialectTranslationStage} — normalize once per
 * *source* dialect into one shared ANSI-ish canonical form, then render once per *target* dialect,
 * instead of hand-writing a separate translator for every {@code (source, target)} pair. With
 * {@code N} source dialects and {@code M} target dialects that's {@code N + M} functions to
 * maintain instead of {@code N × M} — the difference between adding one function and adding six
 * every time a new backend dialect (Snowflake, Databricks, ...) shows up.
 *
 * <p><b>Four source normalizers today: {@code ORACLE}, {@code POSTGRES}, {@code MYSQL}, {@code
 * SQL_SERVER}</b> — every
 * frontend that stamps a real dialect (see {@link DialectTranslationStage}'s javadoc for which
 * frontends produce which {@code SourceDialect}; {@code grpc} and MCP/HTTP both stamp
 * {@code POLYWIRE_NATIVE}, which has no normalizer and never will — there's no wire-protocol signal
 * telling PolyWire which dialect that SQL text was actually authored in, so nothing principled to
 * normalize from). Six targets have renderers: {@code POSTGRES}, {@code MYSQL},
 * {@code SNOWFLAKE}, {@code REDSHIFT}, {@code BIGQUERY}, {@code DATABRICKS}.
 *
 * <p><b>The {@code POSTGRES}/{@code MYSQL} normalizers reuse the {@code ORACLE} renderers'
 * sequence handling for free</b>, by design: both convert their own real sequence-access syntax
 * ({@code nextval('seq')}/{@code currval('seq')} for Postgres; MariaDB's {@code NEXTVAL(seq)}/
 * {@code LASTVAL(seq)} for MySQL) back into the same Oracle-shaped {@code seq.NEXTVAL}/
 * {@code seq.CURRVAL} dot-syntax the canonical form already uses — so every existing renderer
 * (including the ones for Snowflake/Redshift/BigQuery/Databricks) handles a Postgres- or
 * MySQL-sourced sequence reference correctly with zero new renderer code, the same "N + M, not
 * N × M" property the hub-and-spoke design was built for in the first place.
 *
 * <p><b>Canonical form</b>: Oracle SQL with {@code DUAL} stripped, {@code NVL(...)} →
 * {@code COALESCE(...)}, {@code SYSDATE} → {@code CURRENT_TIMESTAMP}, and a single simple
 * {@code ROWNUM <=/< N} predicate → {@code LIMIT N} — all four already valid, as-is, on every one
 * of the six target dialects, so no renderer needs to touch them. Sequence dot-syntax
 * ({@code seq.NEXTVAL}/{@code seq.CURRVAL}) is deliberately <b>not</b> normalized — it's genuinely
 * valid Snowflake syntax already, and every other target's handling (or honest non-handling) is
 * dialect-specific enough that it belongs in the renderer, not the shared canonical form.
 *
 * <p><b>Per-target sequence handling, honestly uneven because the real platforms are</b>:
 * <ul>
 *   <li>{@code POSTGRES}: {@code nextval('seq')}/{@code currval('seq')} — real function-call
 *   syntax.</li>
 *   <li>{@code MYSQL} (via this project's MariaDB JDBC driver): MariaDB 10.3+'s real
 *   {@code CREATE SEQUENCE} object, function-call form — {@code NEXTVAL(seq)}/{@code LASTVAL(seq)}.
 *   Plain upstream MySQL has no sequence object at all; this project's MySQL wire frontend only
 *   ever talks to a MariaDB-compatible backend (see ARCHITECTURE.md §1's driver-choice note), so
 *   that's the real target being rendered for.</li>
 *   <li>{@code SNOWFLAKE}: no rewrite — {@code seq.NEXTVAL}/{@code seq.CURRVAL} is Snowflake's own
 *   native syntax, identical to Oracle's.</li>
 *   <li>{@code REDSHIFT}/{@code BIGQUERY}/{@code DATABRICKS}: no native sequence-object equivalent
 *   exists on any of the three real platforms — left untranslated deliberately, so a statement
 *   using one fails with that backend's own clear "function/object not found" error instead of a
 *   confident-looking wrong rewrite. Same "loud, not silent" convention as everywhere else in this
 *   project (see {@code DbLinkStage}'s unregistered-link handling).</li>
 * </ul>
 *
 * <p><b>Verification status, stated plainly</b>: {@code ORACLE→POSTGRES} and {@code ORACLE→MYSQL}
 * are live-verified end-to-end against real backends (see ARCHITECTURE.md §5.5b/§5.5c), as is
 * {@code POSTGRES→ORACLE} and {@code MYSQL→ORACLE} (see §5.5f). {@code ORACLE→SNOWFLAKE}/
 * {@code REDSHIFT}/{@code BIGQUERY}/{@code DATABRICKS} (and the {@code POSTGRES}/{@code MYSQL}
 * sources rendered at those same four targets) are unit-tested against the same rule vocabulary
 * but have <b>not</b> been live-verified — no local, free way to run any of those four platforms in
 * this environment (same honest gap already recorded for those backends' own connectivity in
 * ARCHITECTURE.md §5.4c).
 *
 * <p><b>{@code SQL_SERVER→POSTGRES} is also live-verified end-to-end</b>, against real {@code
 * mssql-jdbc} through {@code com.polygres.wire.mssqlwire} onto a real Postgres backend: bracketed
 * identifiers, {@code TOP N}, {@code GETDATE()}, and {@code ISNULL} were each independently
 * confirmed. T-SQL's {@code +} string-concatenation operator is deliberately <b>not</b> translated
 * — see {@link #normalizeSqlServer}'s javadoc for why a regex-only layer can't safely disambiguate
 * it from arithmetic {@code +}. {@code SQL_SERVER→SNOWFLAKE}/{@code REDSHIFT}/{@code BIGQUERY}/
 * {@code DATABRICKS} share the same unverified status as every other source rendered at those four
 * targets, for the same reason.
 */
public final class DialectTranslations {

    private DialectTranslations() {
    }

    private static final Map<SourceDialect, Function<String, String>> NORMALIZERS = Map.of(
            SourceDialect.ORACLE, DialectTranslations::normalizeOracle,
            SourceDialect.POSTGRES, DialectTranslations::normalizePostgres,
            SourceDialect.MYSQL, DialectTranslations::normalizeMysql,
            SourceDialect.SQL_SERVER, DialectTranslations::normalizeSqlServer);

    private static final Map<SourceDialect, Function<String, String>> RENDERERS = Map.of(
            SourceDialect.ORACLE, DialectTranslations::renderOracle,
            SourceDialect.POSTGRES, DialectTranslations::renderPostgres,
            SourceDialect.MYSQL, DialectTranslations::renderMysql,
            SourceDialect.SNOWFLAKE, DialectTranslations::renderIdentity,
            SourceDialect.REDSHIFT, DialectTranslations::renderIdentity,
            SourceDialect.BIGQUERY, DialectTranslations::renderBigQuery,
            SourceDialect.DATABRICKS, DialectTranslations::renderIdentity,
            // Calcite's SQL dialect is ANSI-close enough (real COALESCE/CURRENT_TIMESTAMP/LIMIT
            // support) that the shared canonical form needs no rewriting here either; no sequence
            // concept exists for a REST-backed table at all, so nothing to render for that.
            SourceDialect.GENERIC_REST, DialectTranslations::renderIdentity);

    /** {@code null} means "no normalizer for {@code from}, or no renderer for {@code to}" — caller passes the original SQL through untouched. */
    public static String translate(String sql, SourceDialect from, SourceDialect to) {
        if (from == to) {
            return sql;
        }
        Function<String, String> normalizer = NORMALIZERS.get(from);
        Function<String, String> renderer = RENDERERS.get(to);
        if (normalizer == null || renderer == null) {
            return null;
        }
        return renderer.apply(normalizer.apply(sql));
    }

    // ---- ORACLE normalizer: Oracle SQL -> shared canonical form ----

    private static final Pattern NVL = Pattern.compile("(?i)\\bNVL\\s*\\(");
    private static final Pattern FROM_DUAL = Pattern.compile("(?i)\\s+FROM\\s+DUAL\\b");
    private static final Pattern SYSDATE = Pattern.compile("(?i)\\bSYSDATE\\b");
    private static final Pattern ROWNUM = Pattern.compile("(?i)(\\s+(?:AND|WHERE)\\s+)ROWNUM\\s*(<=|<)\\s*(\\d+)\\b");

    private static String normalizeOracle(String sql) {
        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, NVL, m -> "COALESCE(");
        out = SqlLiterals.replaceOutsideLiterals(out, FROM_DUAL, m -> "");
        out = SqlLiterals.replaceOutsideLiterals(out, SYSDATE, m -> "CURRENT_TIMESTAMP");
        out = applyRownumLimit(out);
        out = rewriteDecodeCalls(out);
        return out;
    }

    private static final Pattern DECODE_CALL = Pattern.compile("(?i)\\bDECODE\\s*\\(");

    /**
     * {@code DECODE(expr, val1, res1, val2, res2, ..., default)} → {@code CASE expr WHEN val1 THEN
     * res1 WHEN val2 THEN res2 ... ELSE default END} (or without {@code ELSE} when the argument
     * count is exactly even — no trailing default). {@code CASE} is real ANSI syntax already valid
     * on every target here (same reasoning as the rest of the canonical form), so this is a
     * one-time rewrite with nothing further needed per-renderer.
     *
     * <p>Real parenthesis/quote-aware splitting, not a regex over the whole call — {@code DECODE}'s
     * arguments routinely nest function calls and string literals containing commas
     * ({@code DECODE(NVL(a,0), 1, 'a, b', 0)}), so a naive comma split would misparse those. One
     * {@code DECODE} match is rewritten per pass; nested/repeated calls are handled by iterating
     * until no more matches remain (outermost-first would also work, but inside-out is simpler to
     * reason about and gives the same result since each rewrite only touches its own balanced span).
     */
    private static String rewriteDecodeCalls(String sql) {
        String out = sql;
        while (true) {
            Matcher m = DECODE_CALL.matcher(out);
            int openParenIdx = -1;
            int searchFrom = 0;
            while (m.find(searchFrom)) {
                if (!SqlLiterals.isInsideStringLiteral(out, m.start())) {
                    openParenIdx = m.end() - 1; // index of the '('
                    break;
                }
                searchFrom = m.end();
            }
            if (openParenIdx < 0) {
                return out;
            }
            int closeParenIdx = matchingCloseParen(out, openParenIdx);
            if (closeParenIdx < 0) {
                return out; // unbalanced -- leave untouched rather than risk a wrong rewrite
            }
            int callStart = out.lastIndexOf("DECODE", openParenIdx);
            if (callStart < 0) {
                callStart = out.lastIndexOf("decode", openParenIdx);
            }
            List<String> args = splitTopLevelArgs(out.substring(openParenIdx + 1, closeParenIdx));
            if (args.size() < 3) {
                return out; // not a real DECODE(expr, val, res, ...) shape -- leave untouched
            }
            String expr = args.get(0);
            StringBuilder caseExpr = new StringBuilder("CASE ").append(expr);
            int i = 1;
            for (; i + 1 < args.size(); i += 2) {
                caseExpr.append(" WHEN ").append(args.get(i)).append(" THEN ").append(args.get(i + 1));
            }
            if (i < args.size()) {
                caseExpr.append(" ELSE ").append(args.get(i));
            }
            caseExpr.append(" END");
            out = out.substring(0, callStart) + caseExpr + out.substring(closeParenIdx + 1);
        }
    }

    /** Index of the {@code )} matching the {@code (} at {@code openIdx}, or -1 if unbalanced. Quote-aware (a paren inside a string literal doesn't count). */
    private static int matchingCloseParen(String sql, int openIdx) {
        int depth = 0;
        boolean inString = false;
        for (int i = openIdx; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
            } else if (!inString && c == '(') {
                depth++;
            } else if (!inString && c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /** Splits {@code argsText} on top-level commas only (not inside nested parens or string literals), trimming each piece. */
    private static java.util.List<String> splitTopLevelArgs(String argsText) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        int start = 0;
        for (int i = 0; i < argsText.length(); i++) {
            char c = argsText.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < argsText.length() && argsText.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
            } else if (!inString && c == '(') {
                depth++;
            } else if (!inString && c == ')') {
                depth--;
            } else if (!inString && depth == 0 && c == ',') {
                parts.add(argsText.substring(start, i).trim());
                start = i + 1;
            }
        }
        parts.add(argsText.substring(start).trim());
        return parts;
    }

    /**
     * Handles exactly one simple {@code WHERE ROWNUM <=/< N} or {@code AND ROWNUM <=/< N}
     * predicate — see {@link DialectTranslationStage}'s original javadoc for the full scope note
     * this carries forward unchanged (a second {@code ROWNUM} reference, or one outside a simple
     * comparison, is not attempted).
     */
    private static String applyRownumLimit(String sql) {
        Matcher matcher = ROWNUM.matcher(sql);
        if (!matcher.find() || SqlLiterals.isInsideStringLiteral(sql, matcher.start())) {
            return sql;
        }
        String operator = matcher.group(2);
        int n = Integer.parseInt(matcher.group(3));
        int limit = operator.equals("<=") ? n : n - 1;
        String withoutClause = sql.substring(0, matcher.start()) + sql.substring(matcher.end());
        String trimmed = withoutClause.stripTrailing();
        boolean hadSemicolon = trimmed.endsWith(";");
        if (hadSemicolon) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).stripTrailing();
        }
        return trimmed + " LIMIT " + limit + (hadSemicolon ? ";" : "");
    }

    // ---- POSTGRES normalizer: Postgres SQL -> shared canonical form ----

    private static final Pattern PG_NEXTVAL_CALL = Pattern.compile("(?i)\\bnextval\\s*\\(\\s*'([^']+)'\\s*\\)");
    private static final Pattern PG_CURRVAL_CALL = Pattern.compile("(?i)\\bcurrval\\s*\\(\\s*'([^']+)'\\s*\\)");
    private static final Pattern PG_NOW_CALL = Pattern.compile("(?i)\\bnow\\s*\\(\\s*\\)");
    // Simple operand only (identifier, dotted identifier, integer/decimal literal, or a quoted
    // string literal) immediately before "::type" -- deliberately not a general expression parser,
    // same "narrow but honest" scope as everywhere else in this file. "(a + b)::int" or
    // "func(x)::text" are left untranslated rather than risk a wrong rewrite.
    private static final Pattern PG_CAST_SHORTHAND =
            Pattern.compile("(?i)([A-Za-z_][\\w.$#]*|'[^']*'|\\d+(?:\\.\\d+)?)::([A-Za-z_][\\w]*)");

    private static String normalizePostgres(String sql) {
        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, PG_NEXTVAL_CALL, m -> m.group(1) + ".NEXTVAL");
        out = SqlLiterals.replaceOutsideLiterals(out, PG_CURRVAL_CALL, m -> m.group(1) + ".CURRVAL");
        out = SqlLiterals.replaceOutsideLiterals(out, PG_NOW_CALL, m -> "CURRENT_TIMESTAMP");
        out = SqlLiterals.replaceOutsideLiterals(out, PG_CAST_SHORTHAND, m -> "CAST(" + m.group(1) + " AS " + m.group(2) + ")");
        return out;
    }

    // ---- MYSQL normalizer: MySQL/MariaDB SQL -> shared canonical form ----

    private static final Pattern MYSQL_NEXTVAL_CALL = Pattern.compile("(?i)\\bnextval\\s*\\(\\s*([A-Za-z_][\\w$#]*)\\s*\\)");
    private static final Pattern MYSQL_LASTVAL_CALL = Pattern.compile("(?i)\\blastval\\s*\\(\\s*([A-Za-z_][\\w$#]*)\\s*\\)");
    private static final Pattern MYSQL_NOW_CALL = Pattern.compile("(?i)\\bnow\\s*\\(\\s*\\)");
    private static final Pattern MYSQL_BACKTICK_IDENTIFIER = Pattern.compile("`([^`]+)`");
    private static final Pattern MYSQL_NVL = Pattern.compile("(?i)\\bNVL\\s*\\(");
    // MySQL's two-arg LIMIT (offset, count) -- distinct from the single-arg LIMIT N form (already
    // valid, unchanged, on every target here) and from the LIMIT N OFFSET M form (already ANSI).
    // Only fires on the plain-number two-arg shape; deliberately not attempted for bind-parameter
    // offsets/counts (?, ? -- StatementPipeline binds those positionally, not textually, so there's
    // nothing to distinguish "two-arg LIMIT" from "single-arg LIMIT with a comma-separated
    // subquery" once the literals are gone).
    private static final Pattern MYSQL_LIMIT_OFFSET_COUNT =
            Pattern.compile("(?i)\\bLIMIT\\s+(\\d+)\\s*,\\s*(\\d+)\\b");
    private static final Pattern SHOW_TABLES = Pattern.compile("(?i)^\\s*SHOW\\s+TABLES\\s*;?\\s*$");
    private static final Pattern SHOW_DATABASES = Pattern.compile("(?i)^\\s*SHOW\\s+DATABASES\\s*;?\\s*$");

    private static String normalizeMysql(String sql) {
        // SHOW TABLES/DATABASES have no canonical-form shape to normalize into -- MySQL's SHOW
        // family isn't real SQL syntax any target dialect recognizes, so these are rewritten
        // directly to a real query here and skip the rest of the pipeline (a single-column result
        // set shaped like MySQL's own SHOW TABLES/DATABASES output, built from Postgres catalog
        // views since that's this project's one real MySQL-wire target -- see class javadoc on
        // MYSQL's own renderer notes for why Postgres is the only real backend mywire talks to in
        // its default, non-native mode).
        if (SHOW_TABLES.matcher(sql).matches()) {
            return "SELECT tablename AS \"Tables\" FROM pg_catalog.pg_tables "
                    + "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY tablename";
        }
        if (SHOW_DATABASES.matcher(sql).matches()) {
            return "SELECT datname AS \"Database\" FROM pg_catalog.pg_database "
                    + "WHERE datistemplate = false ORDER BY datname";
        }
        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, MYSQL_NEXTVAL_CALL, m -> m.group(1) + ".NEXTVAL");
        out = SqlLiterals.replaceOutsideLiterals(out, MYSQL_LASTVAL_CALL, m -> m.group(1) + ".CURRVAL");
        out = SqlLiterals.replaceOutsideLiterals(out, MYSQL_NOW_CALL, m -> "CURRENT_TIMESTAMP");
        out = SqlLiterals.replaceOutsideLiterals(out, MYSQL_BACKTICK_IDENTIFIER, m -> "\"" + m.group(1) + "\"");
        out = SqlLiterals.replaceOutsideLiterals(out, MYSQL_NVL, m -> "COALESCE(");
        out = SqlLiterals.replaceOutsideLiterals(out, MYSQL_LIMIT_OFFSET_COUNT,
                m -> "LIMIT " + m.group(2) + " OFFSET " + m.group(1));
        return out;
    }

    // ---- SQL_SERVER (T-SQL) normalizer: T-SQL -> shared canonical form ----

    // Bracketed identifiers -- T-SQL's own quoting rule, same shape as MySQL's backtick rule
    // (MYSQL_BACKTICK_IDENTIFIER above), different delimiter. A '[' inside a string literal is
    // skipped by replaceOutsideLiterals the same way every other rule here is.
    private static final Pattern MSSQL_BRACKETED_IDENTIFIER = Pattern.compile("\\[([^\\]]+)\\]");
    private static final Pattern MSSQL_GETDATE_CALL = Pattern.compile("(?i)\\bGETDATE\\s*\\(\\s*\\)");
    // ISNULL takes exactly two arguments in T-SQL, unlike Postgres's own variadic COALESCE -- a
    // straight rename is correct here without the arg-restructuring DECODE->CASE rewrite needed
    // (COALESCE(expr, replacement) is already the same shape as ISNULL(expr, replacement)).
    private static final Pattern MSSQL_ISNULL = Pattern.compile("(?i)\\bISNULL\\s*\\(");
    // SELECT [DISTINCT] TOP N ... -- the position is different from Postgres's LIMIT (TOP sits
    // right after SELECT, LIMIT goes at the end of the statement), so this is a real structural
    // rewrite: strip "TOP N" from right after SELECT[/DISTINCT] and append "LIMIT N" to the tail
    // of the statement, same as applyRownumLimit does for Oracle's ROWNUM. Only a bare integer
    // literal is handled (no "TOP (@n)" bind-parameter form, no "TOP N PERCENT") -- narrow but
    // honest, same convention as everywhere else in this file.
    private static final Pattern MSSQL_TOP =
            Pattern.compile("(?i)^(\\s*SELECT\\s+)(DISTINCT\\s+)?TOP\\s+(\\d+)\\s+");

    private static String normalizeSqlServer(String sql) {
        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, MSSQL_GETDATE_CALL, m -> "CURRENT_TIMESTAMP");
        out = SqlLiterals.replaceOutsideLiterals(out, MSSQL_ISNULL, m -> "COALESCE(");
        out = SqlLiterals.replaceOutsideLiterals(out, MSSQL_BRACKETED_IDENTIFIER, m -> "\"" + m.group(1) + "\"");
        out = applyTopLimit(out);
        // Deliberately NOT translated: T-SQL's "+" string concatenation operator (e.g.
        // "SELECT first_name + ' ' + last_name"). T-SQL overloads "+" for both arithmetic and
        // string concatenation with no textual marker distinguishing the two -- telling them apart
        // needs real operand type information (is 'a' a string column, a numeric column, a
        // parameter?), which this regex-only, no-catalog-lookup translation layer genuinely doesn't
        // have (same category of gap already documented for MYSQL_LIMIT_OFFSET_COUNT's bind-
        // parameter carve-out above). Guessing wrong here is worse than not translating: a numeric
        // "+" silently rewritten to string concat (or vice versa) produces a confidently wrong
        // result instead of Postgres's own clear type-mismatch error. Left untouched on purpose --
        // a T-SQL statement relying on "+" concatenation against a Postgres target needs to be
        // rewritten by the caller to use "||" (or CONCAT()) directly.
        return out;
    }

    /**
     * Handles {@code SELECT [DISTINCT] TOP N ...} at the very start of the statement -- strips the
     * {@code TOP N} clause and appends {@code LIMIT N} to the statement's tail (before a trailing
     * semicolon, if any), mirroring {@link #applyRownumLimit}'s ROWNUM handling. A second/nested
     * {@code TOP} (e.g. inside a subquery) isn't attempted, same "one simple case" scope as ROWNUM.
     */
    private static String applyTopLimit(String sql) {
        Matcher matcher = MSSQL_TOP.matcher(sql);
        if (!matcher.find() || SqlLiterals.isInsideStringLiteral(sql, matcher.start())) {
            return sql;
        }
        String n = matcher.group(3);
        String withoutTop = matcher.group(1) + (matcher.group(2) == null ? "" : matcher.group(2))
                + sql.substring(matcher.end());
        String trimmed = withoutTop.stripTrailing();
        boolean hadSemicolon = trimmed.endsWith(";");
        if (hadSemicolon) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).stripTrailing();
        }
        return trimmed + " LIMIT " + n + (hadSemicolon ? ";" : "");
    }

    // ---- Renderers: canonical form -> target-specific concerns the canonical form doesn't already cover ----

    private static final Pattern NEXTVAL = Pattern.compile("(?i)\\b([A-Za-z_][\\w$#]*)\\.NEXTVAL\\b");
    private static final Pattern CURRVAL = Pattern.compile("(?i)\\b([A-Za-z_][\\w$#]*)\\.CURRVAL\\b");
    private static final Pattern CURRENT_TIMESTAMP_NO_PARENS = Pattern.compile("(?i)\\bCURRENT_TIMESTAMP\\b(?!\\s*\\()");

    private static String renderPostgres(String sql) {
        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, NEXTVAL, m -> "nextval('" + m.group(1).toLowerCase() + "')");
        out = SqlLiterals.replaceOutsideLiterals(out, CURRVAL, m -> "currval('" + m.group(1).toLowerCase() + "')");
        return out;
    }

    private static String renderMysql(String sql) {
        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, NEXTVAL, m -> "NEXTVAL(" + m.group(1).toLowerCase() + ")");
        out = SqlLiterals.replaceOutsideLiterals(out, CURRVAL, m -> "LASTVAL(" + m.group(1).toLowerCase() + ")");
        return out;
    }

    /** BigQuery's grammar rejects the bare {@code CURRENT_TIMESTAMP} keyword form -- it needs the call form. */
    private static String renderBigQuery(String sql) {
        return SqlLiterals.replaceOutsideLiterals(sql, CURRENT_TIMESTAMP_NO_PARENS, m -> "CURRENT_TIMESTAMP()");
    }

    // Only a trailing LIMIT clause is handled -- the canonical form only ever produces one there
    // (from the ORACLE normalizer's ROWNUM rewrite, or a Postgres/MySQL source that already wrote
    // one), so this deliberately doesn't attempt LIMIT...OFFSET or any other placement/shape.
    private static final Pattern LIMIT_CLAUSE = Pattern.compile("(?i)\\bLIMIT\\s+(\\d+)\\s*;?\\s*$");

    /**
     * {@code COALESCE}/{@code CURRENT_TIMESTAMP}/{@code seq.NEXTVAL} dot-syntax are all already
     * valid, real Oracle syntax -- nothing to rewrite for any of them. The one real gap: Oracle has
     * no {@code LIMIT} keyword at all, so a canonical-form {@code LIMIT N} (however it got there --
     * an ORACLE-normalized {@code ROWNUM}, or a Postgres/MySQL source that wrote {@code LIMIT}
     * directly) is rewritten to Oracle 12c+'s real {@code FETCH FIRST N ROWS ONLY}.
     */
    private static String renderOracle(String sql) {
        Matcher matcher = LIMIT_CLAUSE.matcher(sql);
        if (!matcher.find() || SqlLiterals.isInsideStringLiteral(sql, matcher.start())) {
            return sql;
        }
        String n = matcher.group(1);
        String withoutLimit = sql.substring(0, matcher.start()).stripTrailing();
        return withoutLimit + " FETCH FIRST " + n + " ROWS ONLY";
    }

    /**
     * No target-specific rewrite needed beyond the shared canonical form: SNOWFLAKE's own sequence
     * dot-syntax already matches Oracle's, and REDSHIFT/DATABRICKS have no native sequence object
     * to translate to at all (see class javadoc) -- either way, nothing left to rewrite here.
     */
    private static String renderIdentity(String sql) {
        return sql;
    }
}
