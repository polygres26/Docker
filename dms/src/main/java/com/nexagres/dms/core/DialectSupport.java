package com.nexagres.dms.core;

import com.nexagres.dms.catalog.CatalogProfiler;
import com.nexagres.dms.catalog.MySqlCatalogProfiler;
import com.nexagres.dms.catalog.MySqlObjectExplorer;
import com.nexagres.dms.catalog.MySqlParameterReader;
import com.nexagres.dms.catalog.ObjectExplorer;
import com.nexagres.dms.catalog.OracleCatalogProfiler;
import com.nexagres.dms.catalog.OracleObjectExplorer;
import com.nexagres.dms.catalog.OracleParameterReader;
import com.nexagres.dms.catalog.ParameterReader;
import com.nexagres.dms.catalog.SqlServerCatalogProfiler;
import com.nexagres.dms.catalog.SqlServerObjectExplorer;
import com.nexagres.dms.catalog.SqlServerParameterReader;
import com.nexagres.dms.workload.MySqlWorkloadCapture;
import com.nexagres.dms.workload.OracleWorkloadCapture;
import com.nexagres.dms.workload.SqlServerWorkloadCapture;
import com.nexagres.dms.workload.WorkloadCapture;

/**
 * Single dispatch point from {@link SourceDialect} to the four per-dialect implementations
 * (catalog profiler, object explorer, parameter reader, workload capture) -- what lets
 * {@code ConnectionsRoute}/{@code ScanRoute}/{@code WorkloadRoute} stay dialect-agnostic instead
 * of each hardcoding "if Oracle do X" branches. Adding a new dialect means adding one case to each
 * method here, not touching every route.
 *
 * <p>Every method throws {@link UnsupportedOperationException} for a dialect that isn't wired up
 * yet (today: only {@link SourceDialect#POSTGRES}, which is a migration *target*, never a source
 * Advisor profiles) -- callers turn that into a 501 with an explicit message, never a silent
 * fallback to Oracle's implementation.
 */
public final class DialectSupport {

    private DialectSupport() {}

    public static CatalogProfiler profilerFor(SourceDialect dialect) {
        return switch (dialect) {
            case ORACLE -> new OracleCatalogProfiler();
            case MYSQL, MARIADB -> new MySqlCatalogProfiler();
            case SQL_SERVER -> new SqlServerCatalogProfiler();
            default -> throw unsupported(dialect);
        };
    }

    public static ObjectExplorer explorerFor(SourceDialect dialect) {
        return switch (dialect) {
            case ORACLE -> new OracleObjectExplorer();
            case MYSQL, MARIADB -> new MySqlObjectExplorer();
            case SQL_SERVER -> new SqlServerObjectExplorer();
            default -> throw unsupported(dialect);
        };
    }

    public static ParameterReader parameterReaderFor(SourceDialect dialect) {
        return switch (dialect) {
            case ORACLE -> new OracleParameterReader();
            case MYSQL, MARIADB -> new MySqlParameterReader();
            case SQL_SERVER -> new SqlServerParameterReader();
            default -> throw unsupported(dialect);
        };
    }

    public static WorkloadCapture workloadCaptureFor(SourceDialect dialect) {
        return switch (dialect) {
            case ORACLE -> new OracleWorkloadCapture();
            case MYSQL, MARIADB -> new MySqlWorkloadCapture();
            case SQL_SERVER -> new SqlServerWorkloadCapture();
            default -> throw unsupported(dialect);
        };
    }

    private static UnsupportedOperationException unsupported(SourceDialect dialect) {
        return new UnsupportedOperationException("No support wired up for dialect: " + dialect);
    }
}
