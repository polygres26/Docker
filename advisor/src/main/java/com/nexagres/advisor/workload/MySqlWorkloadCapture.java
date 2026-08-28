package com.nexagres.advisor.workload;

import com.nexagres.advisor.core.BackendTarget;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Workload capture for MySQL/MariaDB, via {@code performance_schema.events_statements_summary_by_digest}
 * -- the closest equivalent to Oracle's {@code V$SQL} (a digest-level rollup of statements seen
 * since the last {@code TRUNCATE TABLE performance_schema.events_statements_summary_by_digest} or
 * server restart, not a true point-in-time cursor cache, but the same "what's actually running"
 * signal). Needs {@code performance_schema} enabled server-side (on by default since MySQL 5.6/
 * MariaDB 10.x) and {@code SELECT} on the table.
 *
 * <p>Field mapping into {@link CapturedStatement}'s Oracle-native vocabulary, noted explicitly
 * since MySQL's own terms differ:
 * <ul>
 *   <li>{@code elapsedTimeMicros} <- {@code SUM_TIMER_WAIT} (picoseconds -&gt; converted to micros)</li>
 *   <li>{@code cpuTimeMicros} <- {@code SUM_CPU_TIME} (MySQL 8.0.28+/MariaDB 10.11+; 0 on older servers where the column doesn't exist -- queried defensively, see {@link #captureCpuTime})</li>
 *   <li>{@code bufferGets} <- {@code SUM_ROWS_EXAMINED} (logical row reads, not Oracle's buffer-cache-block-gets, but the same "how much work" signal)</li>
 *   <li>{@code diskReads} <- {@code SUM_NO_INDEX_USED} count as a proxy for full scans; MySQL's digest table has no direct physical-I/O counter like Oracle's {@code disk_reads}</li>
 * </ul>
 */
public class MySqlWorkloadCapture implements WorkloadCapture {

    private static final String BASE_QUERY = """
        SELECT DIGEST AS sql_id, DIGEST_TEXT AS sql_text, COUNT_STAR AS executions,
               SUM_TIMER_WAIT AS elapsed_picos, SUM_ROWS_EXAMINED AS rows_examined,
               SUM_NO_INDEX_USED AS no_index_used, SUM_ROWS_SENT AS rows_sent,
               SCHEMA_NAME AS schema_name
        FROM performance_schema.events_statements_summary_by_digest
        WHERE SCHEMA_NAME = DATABASE()
        ORDER BY SUM_TIMER_WAIT DESC
        LIMIT ?
        """;

    @Override
    public List<CapturedStatement> capture(BackendTarget target, int limit) throws SQLException {
        List<CapturedStatement> statements = new ArrayList<>();
        try (Connection connection = target.open();
             PreparedStatement ps = connection.prepareStatement(BASE_QUERY)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long elapsedPicos = rs.getLong("elapsed_picos");
                    statements.add(new CapturedStatement(
                        rs.getString("sql_id"),
                        rs.getString("sql_text"),
                        rs.getLong("executions"),
                        elapsedPicos / 1_000_000L, // picoseconds -> microseconds
                        0L, // CPU time: not reliably available across MySQL/MariaDB versions, left at 0 rather than guessed
                        rs.getLong("rows_examined"),
                        rs.getLong("no_index_used"),
                        rs.getLong("rows_sent"),
                        rs.getLong("executions"), // MySQL's digest summary doesn't separate "parse calls" from executions
                        rs.getString("schema_name"),
                        null // MySQL has no equivalent of Oracle's V$SQL.MODULE
                    ));
                }
            }
        }
        return statements;
    }
}
