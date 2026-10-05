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
 * Survivor resume, and reporting of the returned old primary, after Warp promotes a SQL Server secondary. Needs a FRESH three-node
 * read-scale AG ({@code AG_NODES=3 Warp/tests/sqlserver-ag/ag.sh up}; this kills sql1) and a Postgres config database
 * (WARP_HOST/WARP_PORT/WARP_USER/WARP_PASSWORD). Opt-in: WARP_TEST_MSSQL_REJOIN=1; {@code docker} must reach the
 * containers (set DOCKER_CONTEXT if needed).
 */
class SqlServerRejoinLiveTest {

    private static final String PW = "Warp_Test_1234!";
    private static final String OPTS = ";databaseName=w;encrypt=true;trustServerCertificate=true";

    private static String url(int port) {
        return "jdbc:sqlserver://127.0.0.1:" + port + OPTS;
    }

    private static Connection conn(String url) throws Exception {
        return DriverManager.getConnection(url, "sa", PW);
    }

    private static long count(String url, String where) throws Exception {
        try (Connection c = conn(url); var st = c.createStatement(); var rs = st.executeQuery("select count(*) from dbo.t where " + where)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void waitUntil(String what, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
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

    private static void docker(String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "docker";
        System.arraycopy(args, 0, cmd, 1, args.length);
        new ProcessBuilder(cmd).inheritIO().start().waitFor();
    }

    @Test
    void survivorsResumeAndTheReturnedOldPrimaryIsReportedNotTouched() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("WARP_TEST_MSSQL_REJOIN")), "set WARP_TEST_MSSQL_REJOIN=1");
        EngineHa ha = EngineHa.forDialect(SourceDialect.SQL_SERVER);
        String u1 = url(14331);
        String u2 = url(14332);
        String u3 = url(14333);
        BackendTarget t1 = new BackendTarget("n1", u1, "sa", PW);
        waitUntil("both secondaries to be measurable", () -> ha.lag(new BackendTarget("x", u2, "sa", PW), 0).ok()
                && ha.lag(new BackendTarget("y", u3, "sa", PW), 0).ok());

        String spec = "mssql=" + u1.replace(";", "%3B") + "|sa|" + PW + "||" + u2.replace(";", "%3B") + "~30^"
                + u3.replace(";", "%3B") + "~30|promote";
        ServerOptions options = ServerOptions.parse(new String[0]);
        ConfigStore store = new ConfigStore(options);
        store.ensureSchema();
        NodeRegistry.ensureSchema(options);
        store.write(WarpConfig.fromEnvDefaults().withBackendModel(spec, null, null, null, null, null));
        PgFailoverCoordination coordination = new PgFailoverCoordination(options);
        coordination.ensureSchema();
        BackendRegistry registry = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(registry, FailoverMonitor::probeRole,
                new BackendFailoverPersister(store, registry), null, System::currentTimeMillis, 3, 1, 5)
                .withPromoteHooks(FailoverMonitor.defaultHooks(coordination));

        registry.replicaRouter().probeAll();
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);

        // ---- the primary dies; Warp promotes one secondary and resumes the other ----------------------
        docker("kill", "sql1");
        waitUntil("sql1 to be unreachable", () -> ha.role(t1) == FailoverMonitor.NodeRole.UNREACHABLE);
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        String newPrimary = registry.get("mssql").jdbcUrl();
        assertTrue(newPrimary.equals(u2) || newPrimary.equals(u3), "promoted: " + newPrimary);
        String survivor = newPrimary.equals(u2) ? u3 : u2;
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("repointed")),
                "survivor was resumed: " + monitor.recentEvents());
        try (Connection c = conn(newPrimary); var st = c.createStatement()) {
            st.execute("INSERT INTO dbo.t VALUES (20, 'after-promotion')");
        }
        waitUntil("the survivor to receive the new primary's write", () -> count(survivor, "id = 20") == 1);

        // ---- the old primary returns as a second primary and takes a write nobody else has -------------
        docker("start", "sql1");
        waitUntil("sql1 to accept connections", () -> ha.role(t1) != FailoverMonitor.NodeRole.UNREACHABLE);
        assertEquals(FailoverMonitor.NodeRole.WRITABLE, ha.role(t1), "a stale second primary");
        try (Connection c = conn(u1); var st = c.createStatement()) {
            st.execute("INSERT INTO dbo.t VALUES (30, 'written-on-the-stale-primary')");
        }

        // ---- Warp reports the second primary and leaves it exactly as found -------------------------------
        Thread.sleep(1500); // past the 1s rejoin cooldown used here
        monitor.evaluateOnce(false);
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("rejoin-needed")),
                monitor.recentEvents().toString());
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("split-brain-suspected")),
                "a writable second primary keeps raising the alarm");
        assertEquals(FailoverMonitor.NodeRole.WRITABLE, ha.role(t1), "Warp did not touch it");
        assertEquals(1, count(u1, "id = 30"), "its data is untouched");
        assertEquals(0, count(newPrimary, "id = 30"), "and it is not on the new primary: the split brain is real");
    }
}
