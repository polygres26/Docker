package com.sayonora.warp.influxwire;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Storage operations {@link InfluxEngine} needs; implemented over Postgres by {@link PgTimeSeriesStore}. */
interface InfluxBackend {

    /** One stored point: exact epoch-nanosecond time, tags (string) and typed fields (Long/Double/String/Boolean). */
    final class Pt {
        long time;
        Map<String, String> tags;
        Map<String, Object> fields;

        Pt(long time, Map<String, String> tags, Map<String, Object> fields) {
            this.time = time;
            this.tags = tags;
            this.fields = fields;
        }
    }

    /** Field types (float/integer/string/boolean) and tag keys of one measurement. */
    final class Schema {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        final Set<String> tagKeys = new java.util.TreeSet<>();
    }

    record RetentionPolicy(String name, long durationNanos, long shardDurationNanos, int replication, boolean isDefault) {
    }

    /** Pushdown filter: a superset-safe restriction of rows the SQL side may apply (null = none). */
    final class Filter {
        long lo = Long.MIN_VALUE;
        long hi = Long.MAX_VALUE;
        /** DNF-free simple tree: tag comparisons combined with AND/OR (each leaf is exact). */
        Node tree;
        /** When non-null, only rows whose tag set equals one of these (exact deletes of matched series). */
        List<Map<String, String>> exact;

        abstract static class Node {
        }

        static final class Leaf extends Node {
            final String key;
            final String op; // = or !=
            final String value;

            Leaf(String key, String op, String value) {
                this.key = key;
                this.op = op;
                this.value = value;
            }
        }

        static final class Group extends Node {
            final boolean and;
            final List<Node> kids;

            Group(boolean and, List<Node> kids) {
                this.and = and;
                this.kids = kids;
            }
        }
    }

    // --- databases / retention policies
    List<String> databases() throws SQLException;

    boolean databaseExists(String db) throws SQLException;

    void createDatabase(String db, RetentionPolicy defaultRp) throws SQLException;

    void dropDatabase(String db) throws SQLException;

    List<RetentionPolicy> retentionPolicies(String db) throws SQLException;

    /** Name of the database's default retention policy (cached by the store). */
    String defaultRetentionPolicy(String db) throws SQLException;

    void createRetentionPolicy(String db, RetentionPolicy rp) throws SQLException;

    void alterRetentionPolicy(String db, RetentionPolicy rp) throws SQLException;

    void dropRetentionPolicy(String db, String rp) throws SQLException;

    // --- data
    List<String> measurements(String db, String rp) throws SQLException;

    Schema schema(String db, String rp, String measurement) throws SQLException;

    List<Pt> fetch(String db, String rp, String measurement, Schema schema, Filter filter) throws SQLException;

    /** Distinct tag sets (one entry per series) of a measurement. */
    List<Map<String, String>> series(String db, String rp, String measurement, Filter filter) throws SQLException;

    /** Deletes matching points; removes the measurement's catalog entry when nothing remains. */
    void delete(String db, String rp, String measurement, Filter filter) throws SQLException;

    void dropMeasurement(String db, String rp, String measurement) throws SQLException;

    /** Writes (measurement points already validated); returns the number of points dropped (type conflicts). */
    WriteOutcome write(String db, String rp, List<InfluxPoint> points, boolean autoCreateDb) throws SQLException;

    record WriteOutcome(int dropped, String conflictMessage) {
    }
}
