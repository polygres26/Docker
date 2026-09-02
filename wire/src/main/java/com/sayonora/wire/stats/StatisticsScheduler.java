package com.sayonora.wire.stats;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StatisticsScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StatisticsScheduler.class);

    private final BackendRegistry backendRegistry;
    private final StatisticsStore store;
    private final NativeStatisticsCollector collector = new NativeStatisticsCollector();
    private final ScheduledExecutorService executor;

    private StatisticsScheduler(BackendRegistry backendRegistry, StatisticsStore store) {
        this.backendRegistry = backendRegistry;
        this.store = store;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-stats-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public static StatisticsScheduler forOnDemandCollection(BackendRegistry backendRegistry, StatisticsStore store) {
        return new StatisticsScheduler(backendRegistry, store);
    }

    public static StatisticsScheduler startIfConfigured(BackendRegistry backendRegistry, StatisticsStore store) {
        int intervalMinutes = intEnv("WARP_STATS_REFRESH_INTERVAL_MINUTES", 0);
        if (intervalMinutes <= 0) {
            return null;
        }
        StatisticsScheduler scheduler = new StatisticsScheduler(backendRegistry, store);
        scheduler.executor.scheduleWithFixedDelay(scheduler::runCycleSafely, 0, intervalMinutes, TimeUnit.MINUTES);
        log.info("stats: table row-count collection scheduled every {} minute(s)", intervalMinutes);
        return scheduler;
    }

    private void runCycleSafely() {
        try {
            runCycle();
        } catch (RuntimeException e) {
            log.warn("stats: collection cycle failed, will retry next interval ({})", e.toString());
        }
    }

    synchronized void runCycle() {
        int collected = 0;
        for (BackendTarget target : backendRegistry.all()) {
            SourceDialect dialect = target.dialect();
            if (dialect == null || dialect == SourceDialect.GENERIC_REST) {
                continue;
            }
            collected += collectForBackend(target, dialect);
        }
        log.info("stats: collection cycle done — {} table row-count(s) collected/refreshed", collected);
    }

    public int collectForBackend(BackendTarget target, SourceDialect dialect) {
        String schema = foldedSchemaName(target, dialect);
        int count = 0;
        try (Connection connection = target.open()) {
            for (String table : listTables(connection, schema)) {
                Long rowCount = collector.rowCount(connection, dialect, schema, table);
                if (rowCount == null) {
                    continue;
                }
                java.util.Map<String, Long> columnDistinctCounts =
                        collector.columnDistinctCounts(connection, dialect, schema, table, rowCount);
                store.put(new TableStatistics(target.name() + "." + table, rowCount, columnDistinctCounts,
                        System.currentTimeMillis()));
                count++;
            }
        } catch (SQLException e) {
            log.warn("stats: could not collect for backend \"{}\" ({}) — skipping this cycle ({})",
                    target.name(), dialect, e.getMessage());
        }
        return count;
    }

    private static String foldedSchemaName(BackendTarget target, SourceDialect dialect) {
        return dialect == SourceDialect.ORACLE
                ? target.name().toUpperCase(Locale.ROOT)
                : target.name().toLowerCase(Locale.ROOT);
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

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
