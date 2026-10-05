package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.BackendFailoverPersister;
import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.NodeRegistry;
import com.sayonora.warp.config.PgFailoverCoordination;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Warp itself promotes a SQL Server availability-group secondary (promote mode) after the primary dies. Needs the
 * read-scale AG from {@code Warp/tests/sqlserver-ag/ag.sh up} (fresh: this kills sql1, so run it on its own AG) and
 * a Postgres config database (WARP_HOST/WARP_PORT/WARP_USER/WARP_PASSWORD) for the lease and observations.
 * Opt-in: WARP_TEST_MSSQL_PROMOTE=1; {@code docker} must reach the AG containers (set DOCKER_CONTEXT if needed).
 */
class SqlServerPromoteLiveTest {

    private static final String PW = "Warp_Test_1234!";
    private static final String OPTS = ";databaseName=w;encrypt=true;trustServerCertificate=true";
    private static final String P_URL = "jdbc:sqlserver://127.0.0.1:14331" + OPTS;
    private static final String S_URL = "jdbc:sqlserver://127.0.0.1:14332" + OPTS;

    private static Connection conn(String url) throws Exception {
        return DriverManager.getConnection(url, "sa", PW);
    }

    private static void waitUntil(String what, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (cond.call()) {
                    return;
                }
            } catch (Exception ignored) {
                // not yet
            }
            Thread.sleep(500);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    @Test
    void warpPromotesTheSecondaryOfAReadScaleAvailabilityGroupWhenThePrimaryDies() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("WARP_TEST_MSSQL_PROMOTE")), "set WARP_TEST_MSSQL_PROMOTE=1");
        EngineHa ha = EngineHa.forDialect(SourceDialect.SQL_SERVER);
        BackendTarget primary = new BackendTarget("p", P_URL, "sa", PW);
        BackendTarget secondary = new BackendTarget("s", S_URL, "sa", PW);
        waitUntil("the secondary to be measurable", () -> ha.lag(secondary, 0).ok());
        assertTrue(ha.walPosition(secondary).isPresent(), "last_hardened_lsn ranks the replica");
        assertEquals(FailoverMonitor.NodeRole.READ_ONLY, ha.role(secondary));

        String spec = "mssql=" + P_URL.replace(";", "%3B") + "|sa|" + PW + "||" + S_URL.replace(";", "%3B") + "~30|promote";
        ServerOptions options = ServerOptions.parse(new String[0]);
        ConfigStore store = new ConfigStore(options);
        store.ensureSchema();
        NodeRegistry.ensureSchema(options);
        store.write(WarpConfig.fromEnvDefaults().withBackendModel(spec, null, null, null, null, null));
        PgFailoverCoordination coordination = new PgFailoverCoordination(options);
        coordination.ensureSchema();
        BackendRegistry registry = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(registry, FailoverMonitor::probeRole,
                new BackendFailoverPersister(store, registry), null, System::currentTimeMillis, 3, 60, 5)
                .withPromoteHooks(FailoverMonitor.defaultHooks(coordination));

        registry.replicaRouter().probeAll();
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertEquals(P_URL, registry.get("mssql").jdbcUrl(), "healthy: nothing happens");

        new ProcessBuilder("docker", "kill", "sql1").inheritIO().start().waitFor();
        waitUntil("sql1 to be unreachable", () -> ha.role(primary) == FailoverMonitor.NodeRole.UNREACHABLE);
        assertEquals(FailoverMonitor.NodeRole.READ_ONLY, ha.role(secondary), "nobody else promoted");

        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertEquals(FailoverMonitor.NodeRole.READ_ONLY, ha.role(secondary), "not before the confirmation window");
        monitor.evaluateOnce(false);

        assertEquals(S_URL, registry.get("mssql").jdbcUrl(), "Warp promoted the secondary and switched to it");
        assertEquals(FailoverMonitor.NodeRole.WRITABLE, ha.role(secondary));
        try (Connection c = registry.get("mssql").open(); var st = c.createStatement()) {
            st.execute("INSERT INTO dbo.t VALUES (888, 'after-warp-promotion')");
        }
        WarpConfig latest = store.readLatest().orElseThrow().payload();
        assertTrue(latest.backends().startsWith("mssql=" + S_URL.replace(";", "%3B")), latest.backends());
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("promoted")), monitor.recentEvents().toString());
    }
}
