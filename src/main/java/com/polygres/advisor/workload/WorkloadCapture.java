package com.polygres.advisor.workload;

import com.polygres.advisor.core.BackendTarget;
import java.sql.SQLException;
import java.util.List;

/**
 * One implementation per source dialect, same split as {@link com.polygres.advisor.catalog.CatalogProfiler}.
 * Oracle ships first (V$SQL-based, point-in-time snapshot of the shared pool -- not a continuous
 * trace); MariaDB/MySQL (performance_schema-based) is next.
 */
public interface WorkloadCapture {
    /** Snapshot of currently-cached SQL, ranked by elapsed time descending, capped at {@code limit} rows. */
    List<CapturedStatement> capture(BackendTarget target, int limit) throws SQLException;
}
