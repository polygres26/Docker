package com.polygres.wire.stats;

import java.io.Serializable;
import java.util.Map;

/**
 * What {@link StatisticsStore} keeps per table, and what {@link
 * com.polygres.wire.core.FederationStage}'s Calcite planner integration turns into real planner
 * metadata — see that class's javadoc for exactly how. Two pieces, wired into Calcite through two
 * genuinely different mechanisms:
 * <ul>
 *   <li>{@link #rowCount} — a clean, documented extension point ({@code
 *   org.apache.calcite.schema.Statistic#getRowCount()}), consumed via {@link
 *   StatisticsAwareTable}.</li>
 *   <li>{@link #columnDistinctCounts} — no equivalent hook exists on {@code Statistic} at all;
 *   Calcite's default equality-predicate selectivity is a flat guessed constant regardless of a
 *   column's real cardinality. Consumed via a custom {@code
 *   org.apache.calcite.rel.metadata.MetadataHandler} — see {@code
 *   ColumnStatisticsMetadataHandler}'s javadoc for the real mechanism (there is no simpler one).
 *   {@code null}/absent for a column means "no distinct-count known," not "one distinct value" —
 *   callers must fall back to Calcite's own default guess for that column, same as an entirely
 *   uncollected table falls back to {@code Statistics.UNKNOWN}.
 * </ul>
 */
public record TableStatistics(
        String qualifiedTableName,
        long rowCount,
        Map<String, Long> columnDistinctCounts,
        long collectedAtMillis) implements Serializable {

    /** Row count only, no column-level distinct counts — the shape every existing caller (row-count-only v1) already constructs. */
    public TableStatistics(String qualifiedTableName, long rowCount, long collectedAtMillis) {
        this(qualifiedTableName, rowCount, Map.of(), collectedAtMillis);
    }
}
