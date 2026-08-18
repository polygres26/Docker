package com.polygres.wire.rollup;

import java.util.List;

/**
 * One operator-defined pre-aggregation — see {@code ARCHITECTURE.md}'s rollup-acceleration entry
 * for the full design (built after comparing PolyWire to Cube/AtScale/dbt Semantic Layer and finding
 * this the one genuine gap: those three all accelerate aggregate queries against huge fact tables
 * via pre-aggregation; PolyWire's only cache ({@code CacheStage}) is a plain cache-aside result
 * cache, useless for "any slice of this fact table, fast").
 *
 * <p><b>Scope, deliberate</b>: single-backend only (the rollup table is an ordinary real table
 * living in the same backend database as {@code sourceTable}, via a plain {@code CREATE TABLE AS} —
 * no new storage engine); full-refresh only, not incremental (see {@link
 * com.polygres.wire.rollup.RollupRefreshJob}); {@code maxStalenessMinutes} bounds how out-of-date a
 * served answer can be, checked by {@code RollupStage} before ever using this rollup.
 *
 * @param name a stable identifier, used to derive {@link #rollupTableName()} — validated by {@link
 *     RollupConfig} to be a safe SQL identifier (admin-only config, but still not treated as a raw
 *     SQL injection vector, same rigor {@code AccessPolicyYamlConfig} applies to its own fields)
 * @param backendName the {@code BackendRegistry} entry both {@code sourceTable} and the materialized
 *     rollup table live in
 * @param sourceTable {@code schema.table} of the real fact table this rollup summarizes
 * @param groupByColumns the rollup's grouping columns, in order — also exactly the columns a query's
 *     {@code WHERE}/{@code GROUP BY} must be expressible in terms of for Calcite's substitution to
 *     find a match; this is what naturally (not by any extra guard PolyWire has to hand-write)
 *     prevents a row-filter-rewritten predicate on a column absent from the rollup from ever being
 *     silently served — see {@code RollupStage}'s javadoc for the full access-control note
 * @param aggregations each a validated {@code "AGG(expr) AS alias"} expression (see {@link
 *     RollupConfig}'s validation)
 * @param refreshIntervalMinutes how often {@link com.polygres.wire.rollup.RollupRefreshJob} re-runs the
 *     defining query and replaces the rollup table's contents
 * @param maxStalenessMinutes the most out-of-date this rollup is ever allowed to be served as —
 *     independent of {@code refreshIntervalMinutes} so an operator can require fresher-than-schedule
 *     answers (a missed/slow refresh cycle correctly falls through to the real table rather than
 *     silently serving old data)
 */
public record RollupDefinition(
        String name,
        String backendName,
        String sourceTable,
        List<String> groupByColumns,
        List<String> aggregations,
        int refreshIntervalMinutes,
        int maxStalenessMinutes) {

    private static final String TABLE_PREFIX = "polywire_rollup_";

    public String rollupTableName() {
        return TABLE_PREFIX + name;
    }

    /** The query that defines this rollup's contents — used both to materialize it ({@link #createTableSql()}) and, identically, as the {@code queryRel} side of the {@code RelOptMaterialization} {@code RollupStage} registers for substitution. Both uses must see the exact same SQL text, which is why this is the single source of truth for it rather than two near-duplicate builders. */
    public String definingSql() {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", groupByColumns));
        if (!aggregations.isEmpty()) {
            sql.append(", ").append(String.join(", ", aggregations));
        }
        sql.append(" FROM ").append(sourceTable);
        sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        return sql.toString();
    }

    public String createTableSql() {
        return "CREATE TABLE " + rollupTableName() + " AS " + definingSql();
    }

    public String dropTableSql() {
        return "DROP TABLE IF EXISTS " + rollupTableName();
    }
}
