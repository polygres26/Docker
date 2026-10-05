package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live SQL Server Availability Group check. Opt-in: set {@code WARP_TEST_MSSQL_AG=1} after
 * {@code Warp/tests/sqlserver-ag/ag.sh up} (two Docker containers, sql1 primary on 14331 and sql2
 * readable secondary on 14332, database {@code w}, table {@code dbo.t}). Warp only FOLLOWS a failover
 * for SQL Server, so the failover is done here the way a DBA would on a read-scale AG: the primary
 * container is killed and {@code FORCE_FAILOVER_ALLOW_DATA_LOSS} is run on the secondary.
 */
class SqlServerAgLiveTest {

    private static final String PW = "Warp_Test_1234!";
    private static final String OPTS = ";databaseName=w;encrypt=true;trustServerCertificate=true";
    private static final String P_URL = "jdbc:sqlserver://127.0.0.1:14331" + OPTS;
    private static final String S_URL = "jdbc:sqlserver://127.0.0.1:14332" + OPTS;
    // Availability-group DDL is only allowed from master.
    private static final String S_MASTER_URL = "jdbc:sqlserver://127.0.0.1:14332;databaseName=master;encrypt=true;trustServerCertificate=true";

    private static Connection conn(String url) throws Exception {
        return DriverManager.getConnection(url, "sa", PW);
    }

    private static String serverName(RoutingBackendExecutor ex, String sql) throws Exception {
        return String.valueOf(ex.execute(Statement.of(SourceDialect.SQL_SERVER, sql, List.of())).rows().get(0).get(0));
    }

    private static void exec(String url, String sql) throws Exception {
        try (Connection c = conn(url); var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static void waitUntil(String what, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.call()) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    @Test
    void roleLagRoutingSuspendAndFollowAgainstARealAvailabilityGroup() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("WARP_TEST_MSSQL_AG")), "set WARP_TEST_MSSQL_AG=1 after ag.sh up");
        EngineHa ha = EngineHa.forDialect(SourceDialect.SQL_SERVER);
        BackendTarget primary = new BackendTarget("p", P_URL, "sa", PW);
        BackendTarget secondary = new BackendTarget("s", S_URL, "sa", PW);

        // ---- engine probes ---------------------------------------------------------------
        assertEquals(FailoverMonitor.NodeRole.WRITABLE, ha.role(primary));
        assertEquals(FailoverMonitor.NodeRole.READ_ONLY, ha.role(secondary), "a readable secondary is read-only");
        waitUntil("the secondary to be measurable", () -> ha.lag(secondary, 0).ok());
        var lag = ha.lag(secondary, 0);
        assertTrue(lag.isReplica(), String.valueOf(lag.message()));
        assertTrue(lag.lagSeconds() < 30, "freshly seeded secondary, lag " + lag.lagSeconds());
        var primaryLag = ha.lag(primary, 0);
        assertTrue(primaryLag.ok() && !primaryLag.isReplica(), "the primary is not a replica");

        // ---- read routing ------------------------------------------------------------------
        String spec = "mssql=" + P_URL.replace(";", "%3B") + "|sa|" + PW + "||" + S_URL.replace(";", "%3B") + "~30|follow";
        BackendRegistry registry = BackendRegistry.fromConfig(spec, null);
        registry.replicaRouter().probeAll();
        Thread.sleep(2200);
        try (Connection pc = conn(P_URL)) {
            RoutingBackendExecutor ex = new RoutingBackendExecutor(registry, new JdbcBackendExecutor(pc))
                    .withDefaultExecutorBackendName("mssql").withSessionStatePredicate(() -> false);
            assertEquals("sql2", serverName(ex, "select @@servername"), "a plain read is served by the secondary");
            assertEquals("sql1", serverName(ex, "select @@servername from dbo.t with (updlock) where id = 1"),
                    "a locking read stays on the primary");
        }

        // ---- a suspended secondary is skipped, and returns after resume ---------------------------
        exec(S_MASTER_URL, "ALTER DATABASE [w] SET HADR SUSPEND");
        registry.replicaRouter().probeAll();
        var replica = registry.replicaRouter().replicasOf("mssql").get(0);
        waitUntil("the suspended secondary to be ineligible", () -> {
            registry.replicaRouter().probeAll();
            return !registry.replicaRouter().eligible(replica);
        });
        assertFalse(ha.lag(secondary, 0).ok(), "suspended data movement is unmeasurable, never 0");
        exec(S_MASTER_URL, "ALTER DATABASE [w] SET HADR RESUME");
        waitUntil("the resumed secondary to be eligible", () -> {
            registry.replicaRouter().probeAll();
            return registry.replicaRouter().eligible(replica);
        });

        // ---- failover: Warp follows the new primary, never performs the failover ---------------
        BackendRegistry live = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(live, FailoverMonitor::probeRole,
                (b, o, n, r) -> live.applyFailoverLocally(b, o, n, r), null, System::currentTimeMillis, 3, 60, 5);
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertEquals(P_URL, live.get("mssql").jdbcUrl(), "healthy AG: nothing to follow");

        new ProcessBuilder("docker", "kill", "sql1").inheritIO().start().waitFor();
        waitUntil("sql1 to be unreachable", () -> ha.role(primary) == FailoverMonitor.NodeRole.UNREACHABLE);
        exec(S_MASTER_URL, "ALTER AVAILABILITY GROUP [ag1] FORCE_FAILOVER_ALLOW_DATA_LOSS");
        waitUntil("sql2 to accept writes", () -> ha.role(secondary) == FailoverMonitor.NodeRole.WRITABLE);

        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertEquals(P_URL, live.get("mssql").jdbcUrl(), "not before the confirmation window");
        monitor.evaluateOnce(false);
        assertEquals(S_URL, live.get("mssql").jdbcUrl(), "followed the new primary");
        assertEquals(P_URL, live.replicaSpecsOf("mssql").get(0).url(), "old primary is now listed as a replica");
        try (Connection c = live.get("mssql").open(); var st = c.createStatement()) {
            st.execute("INSERT INTO dbo.t VALUES (777, 'after-failover')");
        }
        assertEquals(1L, ((Number) scalar(S_URL, "select count(*) from dbo.t where id = 777")).longValue());
    }

    private static Object scalar(String url, String sql) throws Exception {
        try (Connection c = conn(url); var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getObject(1);
        }
    }
}
