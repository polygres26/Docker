package com.polygres.advisor.workload;

import com.polygres.advisor.core.BackendTarget;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Point-in-time snapshot of {@code V$SQL}, scoped to the connecting schema
 * ({@code PARSING_SCHEMA_NAME = USER}). Needs {@code SELECT} on {@code V_$SQL} (commonly granted
 * via the {@code SELECT_CATALOG_ROLE} role) -- a real step up in privilege from the {@code USER_*}
 * views {@link com.polygres.advisor.catalog.OracleCatalogProfiler} uses, so callers should expect
 * this to fail with a permissions error on a locked-down read-only account and treat that as
 * "workload capture unavailable," not a scan-blocking failure.
 *
 * <p>This is a snapshot of Oracle's shared-pool cursor cache, not a continuous trace -- short-lived
 * or infrequently-run statements that have aged out of the cache by the time this runs won't show
 * up. Good enough as a first-pass "what's hot right now" signal; a real continuous capture (AWR/
 * ASH-backed, or an audit-trail-based capture) is future work once this MVP proves the shape is
 * useful.
 */
public class OracleWorkloadCapture implements WorkloadCapture {

    private static final String QUERY = """
        SELECT sql_id, sql_fulltext, executions, elapsed_time, cpu_time, buffer_gets,
               disk_reads, rows_processed, parse_calls, parsing_schema_name, module
        FROM v$sql
        WHERE parsing_schema_name = USER
          AND command_type IS NOT NULL
        ORDER BY elapsed_time DESC
        FETCH FIRST ? ROWS ONLY
        """;

    @Override
    public List<CapturedStatement> capture(BackendTarget target, int limit) throws SQLException {
        List<CapturedStatement> statements = new ArrayList<>();
        try (Connection connection = target.open();
             PreparedStatement ps = connection.prepareStatement(QUERY)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    statements.add(new CapturedStatement(
                        rs.getString("sql_id"),
                        rs.getString("sql_fulltext"),
                        rs.getLong("executions"),
                        rs.getLong("elapsed_time"),
                        rs.getLong("cpu_time"),
                        rs.getLong("buffer_gets"),
                        rs.getLong("disk_reads"),
                        rs.getLong("rows_processed"),
                        rs.getLong("parse_calls"),
                        rs.getString("parsing_schema_name"),
                        rs.getString("module")
                    ));
                }
            }
        }
        return statements;
    }
}
