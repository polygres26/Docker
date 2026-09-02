package com.sayonora.wire.oswire;

import java.util.List;

/**
 * V2 of the internal Search IR: what an aggregation request compiles into, same "one shape every
 * adapter compiles into, one planner executes" principle as {@link SearchFilter}. Deliberately
 * covers the two aggregation shapes that cover the large majority of real dashboards/reports:
 * grouping (terms) and per-group or whole-result-set numeric summaries (metrics) nested under a
 * terms bucket. Real OpenSearch has many more bucket types (date_histogram, range, histogram,
 * nested, ...); those are a natural next increment on this same sealed hierarchy, not a
 * rearchitecture -- exactly the same "add a case, the planner already switches exhaustively"
 * story {@link SearchFilter}'s javadoc describes for query clauses.
 */
public sealed interface Aggregation {

    String name();

    /** OpenSearch's {@code terms} bucket aggregation -- one bucket per distinct value of
     * {@code field}, ordered by document count descending, capped at {@code size} buckets.
     * {@code subAggs} run once per bucket (real OpenSearch's nested-aggregation semantics), not
     * once over the whole result set. */
    record Terms(String name, String field, int size, List<Aggregation> subAggs) implements Aggregation {
    }

    /** A single numeric summary -- OpenSearch's {@code avg}/{@code sum}/{@code min}/{@code max}/
     * {@code value_count} metric aggregations. Runs over whichever document set it's nested under
     * (the whole search's matched documents at the top level, or one {@link Terms} bucket's
     * documents when nested). */
    record Metric(String name, MetricType type, String field) implements Aggregation {
    }

    enum MetricType { AVG, SUM, MIN, MAX, COUNT }
}
