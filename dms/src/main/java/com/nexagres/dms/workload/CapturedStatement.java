package com.nexagres.dms.workload;

/**
 * One SQL statement observed actually running against the source database -- the "what's really
 * called, how often" signal that catalog-only analysis can't give you (a package existing in the
 * catalog doesn't tell you if it's dead code or hit every second). Field names deliberately use
 * Oracle's own {@code V$SQL} vocabulary (elapsed time, CPU time, buffer gets, disk reads, rows
 * processed, parse calls) rather than inventing generic synonyms -- an Oracle DBA reading this
 * output should recognize it immediately. A MySQL/MariaDB capture would map
 * {@code performance_schema.events_statements_summary_by_digest} into this same shape, with a
 * comment at that call site translating MySQL's vocabulary into these field names.
 */
public record CapturedStatement(
    String sqlId,
    String sqlText,
    long executions,
    long elapsedTimeMicros,
    long cpuTimeMicros,
    long bufferGets,
    long diskReads,
    long rowsProcessed,
    long parseCalls,
    String parsingSchema,
    String module
) {
    /** Elapsed time per execution -- 0 executions means the statement is parsed but never run (rare but real; avoids a divide-by-zero rather than reporting a misleading number). */
    public double avgElapsedMicrosPerExec() {
        return executions == 0 ? 0 : (double) elapsedTimeMicros / executions;
    }
}
