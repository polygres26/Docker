package com.polygres.wire.mywire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Synthesizes a plausible answer for a {@code SELECT @@session.xxx, @@global.yyy, ...} query --
 * MySQL system-variable introspection every real MySQL JDBC client (confirmed live: MySQL
 * Connector/J) sends once, immediately after authenticating, before running any of the caller's
 * own SQL. Found while adding {@code COM_STMT_PREPARE}/{@code EXECUTE} support: without this,
 * {@code DriverManager.getConnection(...)} itself fails before a single application query can run
 * at all, since {@code @@name} has no meaningful Postgres translation and the real value would
 * have to come from a real MySQL server this isn't.
 *
 * <p>Scope: recognizes the query shape and returns a fixed, generic set of common MySQL 8
 * defaults for whichever variable names were actually asked for -- not real values (there's no
 * real MySQL instance backing this), just values plausible enough that a driver's own startup
 * logic (charset/timezone/isolation-level bookkeeping) doesn't choke on them. An unrecognized
 * variable name gets an empty string rather than failing the whole query, matching the "give the
 * driver *something* it can parse" goal here -- this is a compatibility shim for connection setup,
 * not a claim about this server's actual configuration.
 */
final class MySqlSessionVariableQuery {

    private static final Pattern IS_SESSION_VAR_QUERY =
            Pattern.compile("(?i)^\\s*(?:/\\*.*?\\*/\\s*)?SELECT\\s+@@");
    private static final Pattern VAR_ITEM =
            Pattern.compile("(?i)@@(?:session\\.|global\\.)?(\\w+)(?:\\s+AS\\s+(\\w+))?");

    private static final Map<String, String> DEFAULTS = Map.ofEntries(
            Map.entry("auto_increment_increment", "1"),
            Map.entry("character_set_client", "utf8mb4"),
            Map.entry("character_set_connection", "utf8mb4"),
            Map.entry("character_set_results", "utf8mb4"),
            Map.entry("character_set_server", "utf8mb4"),
            Map.entry("collation_server", "utf8mb4_0900_ai_ci"),
            Map.entry("collation_connection", "utf8mb4_0900_ai_ci"),
            Map.entry("init_connect", ""),
            Map.entry("interactive_timeout", "28800"),
            Map.entry("license", "GPL"),
            Map.entry("lower_case_table_names", "0"),
            Map.entry("max_allowed_packet", "67108864"),
            Map.entry("net_write_timeout", "60"),
            Map.entry("performance_schema", "0"),
            Map.entry("sql_mode", ""),
            Map.entry("system_time_zone", "UTC"),
            Map.entry("time_zone", "SYSTEM"),
            Map.entry("transaction_isolation", "REPEATABLE-READ"),
            Map.entry("tx_isolation", "REPEATABLE-READ"),
            // Numeric flags a driver may parse strictly (Integer.parseInt, not just log/ignore) --
            // found live: Connector/J's isReadOnly() parses transaction_read_only as an int and
            // throws NumberFormatException on "", not just misbehaves. Default to "0" (not
            // read-only) rather than "" for anything in this shape.
            Map.entry("transaction_read_only", "0"),
            Map.entry("tx_read_only", "0"),
            Map.entry("autocommit", "1"),
            Map.entry("wait_timeout", "28800"));

    record SyntheticResult(List<String> columnNames, List<Object> row) {
    }

    static boolean matches(String sql) {
        return IS_SESSION_VAR_QUERY.matcher(sql).find();
    }

    static SyntheticResult synthesize(String sql) {
        List<String> columnNames = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        Matcher matcher = VAR_ITEM.matcher(sql);
        while (matcher.find()) {
            String name = matcher.group(1).toLowerCase(java.util.Locale.ROOT);
            String alias = matcher.group(2) != null ? matcher.group(2) : "@@" + matcher.group(1);
            columnNames.add(alias);
            row.add(DEFAULTS.getOrDefault(name, ""));
        }
        return new SyntheticResult(columnNames, row);
    }

    private MySqlSessionVariableQuery() {
    }
}
