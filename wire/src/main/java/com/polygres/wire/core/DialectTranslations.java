package com.polygres.wire.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DialectTranslations {

    private DialectTranslations() {
    }

    private static final java.util.concurrent.atomic.AtomicLong CALL_COUNT =
            new java.util.concurrent.atomic.AtomicLong();

    public static long callCount() {
        return CALL_COUNT.get();
    }

    private static final Map<SourceDialect, Function<String, String>> NORMALIZERS = Map.of(
            SourceDialect.ORACLE, DialectTranslations::normalizeOracle,
            SourceDialect.POSTGRES, DialectTranslations::normalizePostgres,
            SourceDialect.MYSQL, DialectTranslations::normalizeMysql,
            SourceDialect.SQL_SERVER, DialectTranslations::normalizeSqlServer,
            
            SourceDialect.POLYWIRE_NATIVE, DialectTranslations::renderIdentity);

    private static final Map<SourceDialect, Function<String, String>> RENDERERS = Map.of(
            SourceDialect.ORACLE, DialectTranslations::renderOracle,
            SourceDialect.POSTGRES, DialectTranslations::renderPostgres,
            SourceDialect.MYSQL, DialectTranslations::renderMysql,
            SourceDialect.SNOWFLAKE, DialectTranslations::renderIdentity,
            SourceDialect.REDSHIFT, DialectTranslations::renderIdentity,
            SourceDialect.BIGQUERY, DialectTranslations::renderBigQuery,
            SourceDialect.DATABRICKS, DialectTranslations::renderIdentity,
            
            SourceDialect.GENERIC_REST, DialectTranslations::renderIdentity);

    public static String translate(String sql, SourceDialect from, SourceDialect to) {
        CALL_COUNT.incrementAndGet();
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

    private static final Pattern NVL = Pattern.compile("(?i)\\bNVL\\s*\\(");
    private static final Pattern FROM_DUAL = Pattern.compile("(?i)\\s+FROM\\s+DUAL\\b");
    private static final Pattern SYSDATE = Pattern.compile("(?i)\\bSYSDATE\\b");
    
    private static final Pattern DBTIMEZONE = Pattern.compile("(?i)\\bDBTIMEZONE\\b");
    private static final Pattern SESSIONTIMEZONE = Pattern.compile("(?i)\\bSESSIONTIMEZONE\\b");
    private static final Pattern ROWNUM = Pattern.compile("(?i)(\\s+(?:AND|WHERE)\\s+)ROWNUM\\s*(<=|<)\\s*(\\d+)\\b");

    private static final Pattern ALTER_SESSION_TIME_ZONE =
            Pattern.compile("(?i)^\\s*ALTER\\s+SESSION\\s+SET\\s+TIME_ZONE\\s*=\\s*('[^']*')\\s*;?\\s*$");

    private static final Pattern NLS_SESSION_PARAMETERS_PROBE =
            Pattern.compile("(?i)\\bFROM\\s+nls_session_parameters\\b");

    private static final Pattern DBMS_METADATA_BLOCK =
            Pattern.compile("(?i)\\bdbms_metadata\\s*\\.\\s*set_transform_param\\s*\\(");

    private static final Pattern PLACEHOLDER = Pattern.compile("\\?");

    private static final Pattern ALL_TABLES_LIKE = Pattern.compile("(?i)\\b(all_tables|user_tables|dba_tables)\\b");
    private static final Pattern DBA_USERS = Pattern.compile("(?i)\\bdba_users\\b");

    private static String normalizeOracle(String sql) {
        
        Matcher alterSessionTz = ALTER_SESSION_TIME_ZONE.matcher(sql);
        if (alterSessionTz.matches()) {
            return "SET TIME ZONE " + alterSessionTz.group(1);
        }
        if (NLS_SESSION_PARAMETERS_PROBE.matcher(sql).find()) {
            return nlsSessionParametersAnswer();
        }
        if (DBMS_METADATA_BLOCK.matcher(sql).find()) {
            return dbmsMetadataNoOp(sql);
        }

        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, NVL, m -> "COALESCE(");
        out = SqlLiterals.replaceOutsideLiterals(out, FROM_DUAL, m -> "");
        out = SqlLiterals.replaceOutsideLiterals(out, SYSDATE, m -> "CURRENT_TIMESTAMP");
        out = SqlLiterals.replaceOutsideLiterals(out, DBTIMEZONE, m -> "current_setting('TimeZone')");
        out = SqlLiterals.replaceOutsideLiterals(out, SESSIONTIMEZONE, m -> "current_setting('TimeZone')");
        out = SqlLiterals.replaceOutsideLiterals(out, ALL_TABLES_LIKE,
                m -> "(SELECT tablename AS table_name, schemaname AS owner FROM pg_catalog.pg_tables "
                        + "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')) " + m.group(1));
        out = SqlLiterals.replaceOutsideLiterals(out, DBA_USERS,
                m -> "(SELECT usename AS username, NULL::timestamp AS last_login FROM pg_catalog.pg_user) dba_users");
        out = applyRownumLimit(out);
        out = rewriteDecodeCalls(out);
        return out;
    }

    private static String nlsSessionParametersAnswer() {
        return "SELECT * FROM (VALUES "
                + "('NLS_LANGUAGE','AMERICAN'),"
                + "('NLS_TERRITORY','AMERICA'),"
                + "('NLS_CURRENCY','$'),"
                + "('NLS_ISO_CURRENCY','AMERICA'),"
                + "('NLS_NUMERIC_CHARACTERS','.,'),"
                + "('NLS_CALENDAR','GREGORIAN'),"
                + "('NLS_DATE_FORMAT','DD-MON-RR'),"
                + "('NLS_DATE_LANGUAGE','AMERICAN'),"
                + "('NLS_SORT','BINARY'),"
                + "('NLS_TIME_FORMAT','HH.MI.SSXFF AM'),"
                + "('NLS_TIMESTAMP_FORMAT','DD-MON-RR HH.MI.SSXFF AM'),"
                + "('NLS_TIME_TZ_FORMAT','HH.MI.SSXFF AM TZR'),"
                + "('NLS_TIMESTAMP_TZ_FORMAT','DD-MON-RR HH.MI.SSXFF AM TZR'),"
                + "('NLS_DUAL_CURRENCY','$'),"
                + "('NLS_COMP','BINARY'),"
                + "('NLS_LENGTH_SEMANTICS','BYTE'),"
                + "('NLS_NCHAR_CONV_EXCP','FALSE'),"
                + "('DB_TIMEZONE', current_setting('TimeZone')),"
                + "('SESSION_TIMEZONE', current_setting('TimeZone')),"
                + "('SESSION_TIMEZONE_OFFSET', to_char(extract(timezone_hour FROM now()), 'S00') "
                + "|| ':' || lpad(abs(extract(timezone_minute FROM now()))::text, 2, '0')),"
                + "('NLS_CHARACTERSET', 'AL32UTF8')"
                + ") AS t(parameter, value)";
    }

    private static String dbmsMetadataNoOp(String sql) {
        long placeholderCount = PLACEHOLDER.matcher(sql).results().count();
        StringBuilder out = new StringBuilder("SELECT 1 WHERE FALSE");
        for (int i = 0; i < placeholderCount; i++) {
            out.append(" AND ?::text IS NOT NULL");
        }
        return out.toString();
    }

    private static final Pattern DECODE_CALL = Pattern.compile("(?i)\\bDECODE\\s*\\(");

    private static String rewriteDecodeCalls(String sql) {
        String out = sql;
        while (true) {
            Matcher m = DECODE_CALL.matcher(out);
            int openParenIdx = -1;
            int searchFrom = 0;
            while (m.find(searchFrom)) {
                if (!SqlLiterals.isInsideStringLiteral(out, m.start())) {
                    openParenIdx = m.end() - 1;
                    break;
                }
                searchFrom = m.end();
            }
            if (openParenIdx < 0) {
                return out;
            }
            int closeParenIdx = matchingCloseParen(out, openParenIdx);
            if (closeParenIdx < 0) {
                return out;
            }
            int callStart = out.lastIndexOf("DECODE", openParenIdx);
            if (callStart < 0) {
                callStart = out.lastIndexOf("decode", openParenIdx);
            }
            List<String> args = splitTopLevelArgs(out.substring(openParenIdx + 1, closeParenIdx));
            if (args.size() < 3) {
                return out;
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

    private static final Pattern PG_NEXTVAL_CALL = Pattern.compile("(?i)\\bnextval\\s*\\(\\s*'([^']+)'\\s*\\)");
    private static final Pattern PG_CURRVAL_CALL = Pattern.compile("(?i)\\bcurrval\\s*\\(\\s*'([^']+)'\\s*\\)");
    private static final Pattern PG_NOW_CALL = Pattern.compile("(?i)\\bnow\\s*\\(\\s*\\)");
    
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

    private static final Pattern MYSQL_NEXTVAL_CALL = Pattern.compile("(?i)\\bnextval\\s*\\(\\s*([A-Za-z_][\\w$#]*)\\s*\\)");
    private static final Pattern MYSQL_LASTVAL_CALL = Pattern.compile("(?i)\\blastval\\s*\\(\\s*([A-Za-z_][\\w$#]*)\\s*\\)");
    private static final Pattern MYSQL_NOW_CALL = Pattern.compile("(?i)\\bnow\\s*\\(\\s*\\)");
    private static final Pattern MYSQL_BACKTICK_IDENTIFIER = Pattern.compile("`([^`]+)`");
    private static final Pattern MYSQL_NVL = Pattern.compile("(?i)\\bNVL\\s*\\(");
    
    private static final Pattern MYSQL_LIMIT_OFFSET_COUNT =
            Pattern.compile("(?i)\\bLIMIT\\s+(\\d+)\\s*,\\s*(\\d+)\\b");
    private static final Pattern SHOW_TABLES = Pattern.compile("(?i)^\\s*SHOW\\s+TABLES\\s*;?\\s*$");
    private static final Pattern SHOW_DATABASES = Pattern.compile("(?i)^\\s*SHOW\\s+DATABASES\\s*;?\\s*$");

    private static String normalizeMysql(String sql) {
        
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

    private static final Pattern MSSQL_BRACKETED_IDENTIFIER = Pattern.compile("\\[([^\\]]+)\\]");
    private static final Pattern MSSQL_GETDATE_CALL = Pattern.compile("(?i)\\bGETDATE\\s*\\(\\s*\\)");
    
    private static final Pattern MSSQL_ISNULL = Pattern.compile("(?i)\\bISNULL\\s*\\(");
    
    private static final Pattern MSSQL_TOP =
            Pattern.compile("(?i)^(\\s*SELECT\\s+)(DISTINCT\\s+)?TOP\\s+(\\d+)\\s+");

    private static final Pattern MSSQL_BEGIN_TRAN =
            Pattern.compile("(?i)^\\s*BEGIN\\s+TRAN(SACTION)?\\b.*$");
    private static final Pattern MSSQL_COMMIT_TRAN =
            Pattern.compile("(?i)^\\s*COMMIT\\s+(TRAN(SACTION)?)?\\b.*$");
    private static final Pattern MSSQL_ROLLBACK_TRAN =
            Pattern.compile("(?i)^\\s*ROLLBACK\\s+TRAN(SACTION)?\\b.*$");

    private static String normalizeSqlServer(String sql) {
        if (MSSQL_BEGIN_TRAN.matcher(sql).matches()) {
            
            return "SET LOCAL lock_timeout TO DEFAULT";
        }
        if (MSSQL_COMMIT_TRAN.matcher(sql).matches()) {
            return "COMMIT";
        }
        if (MSSQL_ROLLBACK_TRAN.matcher(sql).matches()) {
            return "ROLLBACK";
        }
        String out = sql;
        out = SqlLiterals.replaceOutsideLiterals(out, MSSQL_GETDATE_CALL, m -> "CURRENT_TIMESTAMP");
        out = SqlLiterals.replaceOutsideLiterals(out, MSSQL_ISNULL, m -> "COALESCE(");
        out = SqlLiterals.replaceOutsideLiterals(out, MSSQL_BRACKETED_IDENTIFIER, m -> "\"" + m.group(1) + "\"");
        out = applyTopLimit(out);
        return out;
    }

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

    private static String renderBigQuery(String sql) {
        return SqlLiterals.replaceOutsideLiterals(sql, CURRENT_TIMESTAMP_NO_PARENS, m -> "CURRENT_TIMESTAMP()");
    }

    private static final Pattern LIMIT_CLAUSE = Pattern.compile("(?i)\\bLIMIT\\s+(\\d+)\\s*;?\\s*$");

    private static String renderOracle(String sql) {
        Matcher matcher = LIMIT_CLAUSE.matcher(sql);
        if (!matcher.find() || SqlLiterals.isInsideStringLiteral(sql, matcher.start())) {
            return sql;
        }
        String n = matcher.group(1);
        String withoutLimit = sql.substring(0, matcher.start()).stripTrailing();
        return withoutLimit + " FETCH FIRST " + n + " ROWS ONLY";
    }

    private static String renderIdentity(String sql) {
        return sql;
    }
}
