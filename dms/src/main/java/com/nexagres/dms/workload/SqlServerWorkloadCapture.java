package com.nexagres.dms.workload;

import com.nexagres.dms.core.BackendTarget;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Workload capture for SQL Server, via {@code sys.dm_exec_query_stats} joined to
 * {@code sys.dm_exec_sql_text} -- the closest equivalent to Oracle's {@code V$SQL}: a snapshot of
 * cached execution plans and their aggregate stats since each plan was compiled (not since server
 * start, and plans age out of cache the same way Oracle's shared pool cursors do).
 *
 * <p>Field mapping into {@link CapturedStatement}'s Oracle-native vocabulary:
 * {@code elapsedTimeMicros} &lt;- {@code total_elapsed_time} (already microseconds in this DMV),
 * {@code cpuTimeMicros} &lt;- {@code total_worker_time} (already microseconds), {@code bufferGets}
 * &lt;- {@code total_logical_reads}, {@code diskReads} &lt;- {@code total_physical_reads}. SQL Server
 * has no per-statement "module" concept comparable to Oracle's {@code V$SQL.MODULE}, so that field
 * is left {@code null} here, same as the MySQL/MariaDB capture.
 */
public class SqlServerWorkloadCapture implements WorkloadCapture {

    private static final String QUERY = """
        SELECT TOP (?) CONVERT(VARCHAR(64), qs.query_hash, 1) AS sql_id,
               SUBSTRING(st.text, (qs.statement_start_offset/2) + 1,
                 ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END
                   - qs.statement_start_offset)/2) + 1) AS sql_text,
               qs.execution_count AS executions,
               qs.total_elapsed_time AS elapsed_micros,
               qs.total_worker_time AS cpu_micros,
               qs.total_logical_reads AS logical_reads,
               qs.total_physical_reads AS physical_reads,
               qs.total_rows AS total_rows,
               DB_NAME(st.dbid) AS schema_name
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        WHERE st.dbid = DB_ID()
        ORDER BY qs.total_elapsed_time DESC
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
                        rs.getString("sql_text"),
                        rs.getLong("executions"),
                        rs.getLong("elapsed_micros"),
                        rs.getLong("cpu_micros"),
                        rs.getLong("logical_reads"),
                        rs.getLong("physical_reads"),
                        rs.getLong("total_rows"),
                        rs.getLong("executions"), // SQL Server's DMV doesn't separate parse calls from executions either
                        rs.getString("schema_name"),
                        null
                    ));
                }
            }
        }
        return statements;
    }
}
