package com.polygres.advisor.workload;

/**
 * One SQL statement observed actually running against the source database -- the "what's really
 * called, how often" signal that catalog-only analysis can't give you (a package existing in the
 * catalog doesn't tell you if it's dead code or hit every second). Oracle rows come from
 * {@code V$SQL}; a MySQL/MariaDB capture would map {@code performance_schema.events_statements_summary_by_digest}
 * into the same shape.
 */
public record CapturedStatement(
    String sqlId,
    String sqlText,
    long executions,
    long elapsedTimeMicros,
    String parsingSchema,
    String module
) {}
