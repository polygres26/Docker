package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates, idempotently, the schema a Postgres backend needs to HOST a protocol store. Reuses the
 * frontends' own DDL templates ({@code ddl/postgres/*.sql}) so there is exactly one definition of
 * each store's tables. Stores whose tables are per-collection (MongoDB collections, Influx
 * measurements, OpenSearch indexes, DynamoDB item tables, SQS queue tables) get those tables
 * lazily on first use, on every host; enabling them creates the fixed catalog/graph tables (where
 * the protocol has any) and a {@code warp_enabled_stores} marker row.
 *
 * <p>Disabling a store NEVER drops anything -- there is deliberately no inverse of this class.
 */
public final class StoreBootstrap {

    private static final Logger log = LoggerFactory.getLogger(StoreBootstrap.class);
    public static final String MARKER_TABLE = "warp_enabled_stores";

    /** (backend jdbc url + store) already bootstrapped by this process. */
    private static final Map<String, Boolean> DONE = new ConcurrentHashMap<>();

    private StoreBootstrap() {
    }

    public static void ensure(BackendTarget target, StoreType store) throws SQLException {
        String key = target.jdbcUrl() + "#" + store.id();
        if (DONE.containsKey(key)) {
            return;
        }
        try {
            run(target, store);
        } catch (SQLException first) {
            // two Warp instances bootstrapping the same backend at once can race on CREATE TABLE
            // IF NOT EXISTS (duplicate pg_type); one retry settles it
            run(target, store);
        }
        DONE.put(key, Boolean.TRUE);
        log.info("stores: {} schema ensured on backend '{}'", store.id(), target.name());
    }

    /** Forgets the per-process cache (tests / a backend whose data was wiped). */
    public static void forgetAll() {
        DONE.clear();
    }

    private static void run(BackendTarget target, StoreType store) throws SQLException {
        try (Connection c = target.open(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS " + MARKER_TABLE + " ("
                    + "store text PRIMARY KEY, enabled_at timestamptz NOT NULL DEFAULT now())");
            switch (store) {
                case DYNAMODB -> exec(st, "dynamowire_catalog");
                case SQS -> exec(st, "sqswire_catalog");
                case NEO4J -> exec(st, "boltwire_graph_schema");
                case S3 -> exec(st, "s3wire_store");
                default -> {
                    // per-collection tables are created on first use
                }
            }
            try (var ps = c.prepareStatement("INSERT INTO " + MARKER_TABLE + " (store) VALUES (?) "
                    + "ON CONFLICT (store) DO NOTHING")) {
                ps.setString(1, store.id());
                ps.executeUpdate();
            }
        }
    }

    private static void exec(Statement st, String template) throws SQLException {
        List<String> statements = DdlTemplates.loadStatements("postgres", template, Map.of());
        for (String s : statements) {
            st.execute(s);
        }
    }

    /** Ensures every store currently enabled on a Postgres backend of {@code registry}. Errors are
     * logged per (backend, store), never thrown: an unreachable host must not break a config reload. */
    public static void ensureAll(BackendRegistry registry) {
        for (Map.Entry<String, List<StoreType>> e : registry.allEnabledStores().entrySet()) {
            BackendTarget t = registry.get(e.getKey());
            for (StoreType s : e.getValue()) {
                try {
                    ensure(t, s);
                } catch (SQLException | RuntimeException ex) {
                    log.warn("stores: could not ensure {} schema on backend '{}': {}", s.id(), e.getKey(), ex.toString());
                }
            }
        }
    }
}
