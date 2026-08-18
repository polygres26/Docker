package com.polygres.wire.rollup;

import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.BackendTarget;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Materializes each configured rollup as a real table (full-refresh, not incremental — see {@link
 * RollupDefinition}'s javadoc for the explicit scope) and keeps it refreshed on its own schedule.
 * Same "runs once immediately, then on a fixed per-item interval" shape as {@code
 * com.polygres.wire.stats.StatisticsScheduler}, one {@link ScheduledFuture} per rollup rather than one
 * shared interval, since different rollups can legitimately want very different refresh cadences.
 *
 * <p><b>Runs with the backend's own connection-pool credentials, not any caller's {@code
 * AccessContext}</b> — same trust level as {@code StatisticsScheduler}'s own row-count collection:
 * this is an operator-configured background job materializing a full, unfiltered aggregate of the
 * source table, not a query made on any particular user's behalf. See {@code RollupStage}'s javadoc
 * for why this is safe for the app-level row-filter-rewrite enforcement path (Calcite's substitution
 * naturally can't apply a rollup whose grouping doesn't cover the filtered column) and the explicit,
 * documented limitation for native RLS/VPD pass-through (this job — and the rollup path generally —
 * bypasses it; rollups on natively-row-secured tables are not supported in this phase).
 *
 * <p><b>A failed refresh leaves the previous rollup table and its freshness timestamp untouched</b>
 * — degrades to "stale, {@code RollupStage} will fall through to the real table once past {@code
 * maxStalenessMinutes}", never to "a half-written or broken rollup table served as fresh."
 */
public final class RollupRefreshJob implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RollupRefreshJob.class);

    private final BackendRegistry backendRegistry;
    private final RollupStore store;
    private final ScheduledExecutorService executor;
    private final Map<String, ScheduledFuture<?>> scheduledByRollup = new ConcurrentHashMap<>();

    public RollupRefreshJob(BackendRegistry backendRegistry, RollupStore store) {
        this.backendRegistry = backendRegistry;
        this.store = store;
        this.executor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "polywire-rollup-refresh");
            t.setDaemon(true);
            return t;
        });
    }

    /** Schedules every currently-configured rollup (called once at startup, and again after a live reload — see {@link #rescheduleAll}). */
    public void scheduleAll() {
        rescheduleAll(store.definitions());
    }

    /** Cancels every previously-scheduled refresh and reschedules from {@code definitions} — called by the admin reload path so a rollup removed from config stops refreshing, and one whose interval changed picks up the new cadence, without a server restart. */
    public void rescheduleAll(List<RollupDefinition> definitions) {
        scheduledByRollup.values().forEach(f -> f.cancel(false));
        scheduledByRollup.clear();
        for (RollupDefinition def : definitions) {
            ScheduledFuture<?> future = executor.scheduleWithFixedDelay(
                    () -> refreshSafely(def), 0, def.refreshIntervalMinutes(), TimeUnit.MINUTES);
            scheduledByRollup.put(def.name(), future);
        }
    }

    /** On-demand refresh (the admin {@code POST /api/rollups/{name}/refresh} route) — runs synchronously so the caller gets a real success/failure result, not a fire-and-forget. */
    public void refreshNow(RollupDefinition def) throws SQLException {
        refresh(def);
    }

    private void refreshSafely(RollupDefinition def) {
        try {
            refresh(def);
        } catch (SQLException | RuntimeException e) {
            log.warn("rollup: refresh failed for \"{}\", leaving the previous table (if any) in place ({})",
                    def.name(), e.toString());
        }
    }

    private void refresh(RollupDefinition def) throws SQLException {
        BackendTarget target = backendRegistry.get(def.backendName());
        if (target == null) {
            throw new SQLException("rollup \"" + def.name() + "\" references backend \"" + def.backendName()
                    + "\", which isn't a configured POLYWIRE_BACKENDS entry");
        }
        try (Connection connection = target.open(); Statement st = connection.createStatement()) {
            st.executeUpdate(def.dropTableSql());
            st.executeUpdate(def.createTableSql());
        }
        store.markRefreshed(def.name());
        log.info("rollup: \"{}\" refreshed (table {})", def.name(), def.rollupTableName());
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
