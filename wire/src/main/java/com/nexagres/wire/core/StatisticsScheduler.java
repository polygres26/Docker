package com.nexagres.wire.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically warms {@link StatisticsStore} for every (schema, backend) pair {@link
 * ShardJoinExecutor}/{@link SchemaFederationStage} could ever federate across -- the collection
 * side of what their Calcite planner integration consumes. Ported from the sibling Omnigate
 * project's own {@code StatisticsScheduler} (real, tested, production code there), adapted for a
 * real difference in this codebase: Omnigate's own convention is "a backend's registry name doubles
 * as its real database schema name," which doesn't hold here -- Warp's own {@link
 * RouterStage.ShardRule}/{@link RouterStage.SchemaRule} name the schema and the backend
 * independently (e.g. schema {@code "orders_db"} routes to backend {@code "orders"}), so this class
 * takes the configured rule lists directly instead of guessing a schema name from a backend name.
 *
 * <p>Same "runs once at startup, then on a fixed interval" shape, same "unset means the feature
 * doesn't exist" default ({@code WARP_STATS_REFRESH_INTERVAL_MINUTES}, 0/unset -- no scheduler
 * constructed, no background thread).
 */
public final class StatisticsScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StatisticsScheduler.class);

    private final BackendRegistry backendRegistry;
    private final List<RouterStage.ShardRule> shardRules;
    private final List<RouterStage.SchemaRule> schemaRules;
    private final StatisticsStore store;
    private final ScheduledExecutorService executor;

    private StatisticsScheduler(BackendRegistry backendRegistry, List<RouterStage.ShardRule> shardRules,
            List<RouterStage.SchemaRule> schemaRules, StatisticsStore store) {
        this.backendRegistry = backendRegistry;
        this.shardRules = shardRules;
        this.schemaRules = schemaRules;
        this.store = store;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-stats-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /** {@code null} (no scheduler constructed, no background thread) unless {@code
     * WARP_STATS_REFRESH_INTERVAL_MINUTES} is set to a positive integer, or there's nothing
     * configured for it to ever collect (fewer than 2 shard/schema rules total -- same "the
     * feature this feeds is itself a no-op" reasoning {@link ShardJoinExecutor}/{@link
     * SchemaFederationStage}'s own {@code fromConfigOrNull}-style gates use). An initial collection
     * pass runs immediately (not after waiting a full interval) so a freshly started server doesn't
     * plan its first federated queries against a completely cold {@link StatisticsStore}. */
    public static StatisticsScheduler startIfConfigured(BackendRegistry backendRegistry,
            List<RouterStage.ShardRule> shardRules, List<RouterStage.SchemaRule> schemaRules, StatisticsStore store) {
        int intervalMinutes = intEnv("WARP_STATS_REFRESH_INTERVAL_MINUTES", 0);
        if (intervalMinutes <= 0 || (shardRules.isEmpty() && schemaRules.size() < 2)) {
            return null;
        }
        StatisticsScheduler scheduler = new StatisticsScheduler(backendRegistry, shardRules, schemaRules, store);
        scheduler.executor.scheduleWithFixedDelay(scheduler::runCycleSafely, 0, intervalMinutes, TimeUnit.MINUTES);
        log.info("statistics: table row-count collection scheduled every {} minute(s)", intervalMinutes);
        return scheduler;
    }

    private void runCycleSafely() {
        try {
            runCycle();
        } catch (RuntimeException e) {
            log.warn("statistics: collection cycle failed, will retry next interval ({})", e.toString());
        }
    }

    /** Package-visible so tests can drive one cycle synchronously without waiting on a real
     * interval. Every (schema, backend) pair from every {@code ShardRule} (expanded across the
     * whole shard group -- the same schema lives on every shard) and every {@code SchemaRule}
     * (one backend each). */
    synchronized void runCycle() {
        Set<SchemaBackend> pairs = new LinkedHashSet<>();
        for (RouterStage.ShardRule rule : shardRules) {
            for (String shardName : backendRegistry.shardGroup()) {
                pairs.add(new SchemaBackend(rule.schemaName(), shardName));
            }
        }
        for (RouterStage.SchemaRule rule : schemaRules) {
            pairs.add(new SchemaBackend(rule.schemaName(), rule.backendName()));
        }
        int collected = 0;
        for (SchemaBackend pair : pairs) {
            collected += collectForSchemaBackend(pair.schemaName(), pair.backendName());
        }
        log.info("statistics: collection cycle done -- {} table row-count(s) collected/refreshed across "
                + "{} (schema, backend) pair(s)", collected, pairs.size());
    }

    private int collectForSchemaBackend(String schemaName, String backendName) {
        BackendTarget target = backendRegistry.resolveForRouting(backendName);
        if (target == null) {
            return 0;
        }
        int count = 0;
        try (Connection connection = target.open()) {
            for (String table : listTables(connection, schemaName)) {
                Long rowCount = store.rowCount(connection, backendName + "." + schemaName + "." + table, schemaName, table);
                if (rowCount != null) {
                    store.put(backendName + "." + schemaName + "." + table, rowCount);
                    count++;
                }
            }
        } catch (SQLException e) {
            log.warn("statistics: could not collect for schema \"{}\" on backend \"{}\" -- skipping this cycle ({})",
                    schemaName, backendName, e.getMessage());
        }
        return count;
    }

    private static List<String> listTables(Connection connection, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    private static int intEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private record SchemaBackend(String schemaName, String backendName) {
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
