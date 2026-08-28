package com.nexagres.wire.rollup;

import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.core.BackendTarget;
import com.nexagres.wire.core.ErrorCatalog;
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

    public void scheduleAll() {
        rescheduleAll(store.definitions());
    }

    public void rescheduleAll(List<RollupDefinition> definitions) {
        scheduledByRollup.values().forEach(f -> f.cancel(false));
        scheduledByRollup.clear();
        for (RollupDefinition def : definitions) {
            ScheduledFuture<?> future = executor.scheduleWithFixedDelay(
                    () -> refreshSafely(def), 0, def.refreshIntervalMinutes(), TimeUnit.MINUTES);
            scheduledByRollup.put(def.name(), future);
        }
    }

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
            throw ErrorCatalog.sqlException("ERR_ROLLUP_UNKNOWN_BACKEND", def.name(), def.backendName());
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
