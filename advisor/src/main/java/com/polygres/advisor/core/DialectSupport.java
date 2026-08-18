package com.polygres.advisor.core;

import com.polygres.advisor.catalog.CatalogProfiler;
import com.polygres.advisor.catalog.MySqlCatalogProfiler;
import com.polygres.advisor.catalog.MySqlObjectExplorer;
import com.polygres.advisor.catalog.MySqlParameterReader;
import com.polygres.advisor.catalog.ObjectExplorer;
import com.polygres.advisor.catalog.OracleCatalogProfiler;
import com.polygres.advisor.catalog.OracleObjectExplorer;
import com.polygres.advisor.catalog.OracleParameterReader;
import com.polygres.advisor.catalog.ParameterReader;
import com.polygres.advisor.catalog.SqlServerCatalogProfiler;
import com.polygres.advisor.catalog.SqlServerObjectExplorer;
import com.polygres.advisor.catalog.SqlServerParameterReader;
import com.polygres.advisor.workload.MySqlWorkloadCapture;
import com.polygres.advisor.workload.OracleWorkloadCapture;
import com.polygres.advisor.workload.SqlServerWorkloadCapture;
import com.polygres.advisor.workload.WorkloadCapture;

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
