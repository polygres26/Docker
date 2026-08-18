package com.polygres.wire.stats;

import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import com.polygres.wire.core.SourceDialect;
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

/**
 * Periodically populates {@link StatisticsStore} from every real-JDBC-dialect {@link BackendTarget}
 * in a {@link BackendRegistry} — the collection side of what {@code FederationStage}'s Calcite
 * planner integration consumes. Scoped to <b>named backends only</b> ({@link BackendRegistry#all()},
 * not the anonymous default connection): those are exactly the ones {@code FederationStage} can ever
 * mount into a federated Calcite connection (it only federates schema-rule-matched named backends —
 * see that class's javadoc on the backend-registry-name-doubles-as-real-schema-name convention),
 * so there's nothing to gain profiling a backend federation can never reach.
 *
 * <p>Same "runs once at startup, then on a fixed interval" shape as {@code OntologyScheduler}, same
 * "unset means the feature doesn't exist" default. {@code GENERIC_REST} backends (REST/S3/SharePoint)
 * are silently skipped — see {@link NativeStatisticsCollector}'s javadoc for why (no native optimizer
 * to read from at all; a sampling-based collector for those is separate, real future work).
 */
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
            Thread t = new Thread(r, "polywire-stats-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * A usable {@link #collectForBackend} caller that never schedules anything on its own — for
     * {@code BackendOnboardingPipeline}'s one-shot "collect stats for just this new backend right
     * now" call (ARCHITECTURE.md §51), independent of whether periodic collection is even
     * configured (unlike {@link #startIfConfigured}, this never returns {@code null}: on-demand
     * collection for a freshly registered backend shouldn't depend on {@code
     * POLYWIRE_STATS_REFRESH_INTERVAL_MINUTES} being set at all). The instance's own {@link
     * #executor} is never used — {@link #close} is a no-op-safe shutdown of an executor that never
     * ran anything, kept only so this type doesn't need two different shapes.
     */
    public static StatisticsScheduler forOnDemandCollection(BackendRegistry backendRegistry, StatisticsStore store) {
        return new StatisticsScheduler(backendRegistry, store);
    }

    /**
     * {@code null} (no scheduler constructed, no background thread) unless {@code
     * POLYWIRE_STATS_REFRESH_INTERVAL_MINUTES} is set to a positive integer. An initial collection
     * pass runs immediately (not after waiting a full interval) so a freshly started server doesn't
     * plan its first federated queries against a completely empty {@link StatisticsStore}.
     */
    public static StatisticsScheduler startIfConfigured(BackendRegistry backendRegistry, StatisticsStore store) {
        int intervalMinutes = intEnv("POLYWIRE_STATS_REFRESH_INTERVAL_MINUTES", 0);
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

    /** Package-visible so tests can drive one cycle synchronously without waiting on a real interval. */
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

    /**
     * Public (not just the scheduled cycle's own internal use) so {@code
     * com.polygres.wire.server.onboarding.BackendOnboardingPipeline} can collect statistics for a single,
     * just-registered backend immediately rather than waiting for the next scheduled interval —
     * ARCHITECTURE.md §51. Same collection logic either way, just invoked for one {@link
     * BackendTarget} instead of every one in a {@link BackendRegistry}.
     */
    public int collectForBackend(BackendTarget target, SourceDialect dialect) {
        String schema = foldedSchemaName(target, dialect);
        int count = 0;
        try (Connection connection = target.open()) {
            for (String table : listTables(connection, schema)) {
                Long rowCount = collector.rowCount(connection, dialect, schema, table);
                if (rowCount == null) {
                    continue; // no native stats gathered for this table yet -- nothing to store, not zero rows
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

    /** Same convention {@code FederationStage.foldedSchemaName} already establishes: the backend registry name doubles as the real database schema name, folded to whatever case that dialect stores unquoted identifiers in. */
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
