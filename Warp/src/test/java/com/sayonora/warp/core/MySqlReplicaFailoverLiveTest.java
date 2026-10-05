package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.BackendFailoverPersister;
import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.NodeRegistry;
import com.sayonora.warp.config.PgFailoverCoordination;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live MySQL check: real MySQL 9.x primary + two GTID replicas (ports from WARP_TEST_MYSQL_PRIMARY_PORT
 * / _REPLICA1_PORT / _REPLICA2_PORT, root without password, database {@code w}, table {@code w.t}),
 * plus a Postgres server holding warp_config and the lease tables (WARP_HOST/WARP_PORT/... env, as in
 * the Postgres live tests). Opt-in; skipped otherwise.
 */
class MySqlReplicaFailoverLiveTest {

    private static final String OPTS = "?allowPublicKeyRetrieval=true&useSSL=false";

    private static String url(String port) {
        return "jdbc:mysql://127.0.0.1:" + port + "/w" + OPTS;
    }

    private static Connection conn(String port) throws Exception {
        return DriverManager.getConnection(url(port), "root", "");
    }

    private static long scalar(String port, String sql) throws Exception {
        try (Connection c = conn(port); var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void exec(String port, String sql) throws Exception {
        try (Connection c = conn(port); var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static void waitUntil(String what, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.call()) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static int portOf(RoutingBackendExecutor ex, String sql) throws Exception {
        return ((Number) ex.execute(Statement.of(SourceDialect.MYSQL, sql, List.of())).rows().get(0).get(0)).intValue();
    }

    @Test
    void mysqlProbesRoutingLagGatingAndPromotionWorkAgainstRealServers() throws Exception {
        String pPort = System.getenv("WARP_TEST_MYSQL_PRIMARY_PORT");
        String r1Port = System.getenv("WARP_TEST_MYSQL_REPLICA1_PORT");
        String r2Port = System.getenv("WARP_TEST_MYSQL_REPLICA2_PORT");
        Assumptions.assumeTrue(pPort != null && r1Port != null && r2Port != null);
        String pUrl = url(pPort);
        String r1Url = url(r1Port);
        String r2Url = url(r2Port);
        EngineHa my = EngineHa.forDialect(SourceDialect.MYSQL);

        // ---- engine probes -----------------------------------------------------------------
        BackendTarget primaryT = new BackendTarget("p", pUrl, "root", "");
        BackendTarget r1T = new BackendTarget("r1", r1Url, "root", "");
        BackendTarget r2T = new BackendTarget("r2", r2Url, "root", "");
        assertEquals(FailoverMonitor.NodeRole.WRITABLE, my.role(primaryT));
        assertEquals(FailoverMonitor.NodeRole.READ_ONLY, my.role(r1T));
        var lag = my.lag(r1T, 0);
        assertTrue(lag.ok() && lag.isReplica(), String.valueOf(lag.message()));
        assertTrue(lag.lagSeconds() < 2, "caught-up replica: " + lag.lagSeconds());
        var primaryLag = my.lag(primaryT, 0);
        assertTrue(primaryLag.ok() && !primaryLag.isReplica(), "a primary is not a replica");
        assertEquals(my.walPosition(r1T).getAsLong(), my.walPosition(r2T).getAsLong(), "equally caught up");
        assertTrue(my.walPosition(r1T).getAsLong() > 0);

        // ---- routing: reads to a lag-eligible replica, never the unsafe ones -----------------
        String spec = "my=" + pUrl + "|root|||" + r1Url + "~4^" + r2Url + "~4|promote";
        BackendRegistry registry = BackendRegistry.fromConfig(spec, null);
        registry.replicaRouter().probeAll();
        Thread.sleep(2200);
        try (Connection pc = conn(pPort)) {
            RoutingBackendExecutor ex = new RoutingBackendExecutor(registry, new JdbcBackendExecutor(pc))
                    .withDefaultExecutorBackendName("my").withSessionStatePredicate(() -> false);
            int served = portOf(ex, "select @@port");
            assertTrue(served == Integer.parseInt(r1Port) || served == Integer.parseInt(r2Port),
                    "a plain read goes to a replica, got " + served);
            assertEquals(Integer.parseInt(pPort), portOf(ex, "select @@port from w.t for update"));
            assertEquals(Integer.parseInt(pPort), portOf(ex, "select get_lock('x', 0) * 0 + @@port"),
                    "GET_LOCK keeps the read on the primary");
        }

        // ---- lag gating on real replication state ---------------------------------------------
        exec(r1Port, "stop replica sql_thread");
        exec(pPort, "insert into w.t values (10, 'while-r1-sql-stopped')");
        Thread.sleep(1500);
        registry.replicaRouter().probeAll();
        var r1Replica = registry.replicaRouter().replicasOf("my").get(0);
        assertFalse(registry.replicaRouter().eligible(r1Replica), "replica with a stopped SQL thread must not serve reads");
        exec(r1Port, "start replica sql_thread");
        waitUntil("r1 to catch up", () -> scalar(r1Port, "select count(*) from w.t") == scalar(pPort, "select count(*) from w.t"));
        registry.replicaRouter().probeAll();
        assertTrue(registry.replicaRouter().eligible(r1Replica), "eligible again once both threads run");

        // a real, measured delay beyond the allowance
        exec(r2Port, "stop replica");
        exec(r2Port, "change replication source to source_delay = 12");
        exec(r2Port, "start replica");
        exec(pPort, "insert into w.t values (11, 'delayed')");
        Thread.sleep(7000);
        var delayed = my.lag(r2T, 0);
        assertTrue(delayed.ok() && delayed.lagSeconds() > 4, "measured lag " + delayed.lagSeconds());
        registry.replicaRouter().probeAll();
        assertFalse(registry.replicaRouter().eligible(registry.replicaRouter().replicasOf("my").get(1)));
        exec(r2Port, "stop replica");
        exec(r2Port, "change replication source to source_delay = 0");
        exec(r2Port, "start replica");
        waitUntil("r2 to catch up", () -> scalar(r2Port, "select count(*) from w.t") == scalar(pPort, "select count(*) from w.t"));

        // ---- failover: Warp promotes the replica that received the most -----------------------
        ServerOptions options = ServerOptions.parse(new String[0]);
        ConfigStore store = new ConfigStore(options);
        store.ensureSchema();
        NodeRegistry.ensureSchema(options);
        store.write(WarpConfig.fromEnvDefaults().withBackendModel(spec, null, null, null, null, null));
        PgFailoverCoordination coordination = new PgFailoverCoordination(options);
        coordination.ensureSchema();
        BackendRegistry live = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(live, FailoverMonitor::probeRole,
                new BackendFailoverPersister(store, live), null, System::currentTimeMillis, 3, 60, 5)
                .withPromoteHooks(FailoverMonitor.defaultHooks(coordination));
        live.replicaRouter().probeAll();
        monitor.evaluateOnce(false); // healthy pass: records lag measured while the primary is up
        monitor.evaluateOnce(false);
        assertEquals(pUrl, live.get("my").jdbcUrl());

        // r1 stops fetching; r2 keeps up -> r2 holds strictly more of the primary's log
        exec(r1Port, "stop replica io_thread");
        exec(pPort, "insert into w.t values (20, 'only-r2-gets-this'), (21, 'nor-this')");
        waitUntil("r2 to receive the new rows", () -> scalar(r2Port, "select count(*) from w.t where id >= 20") == 2);
        assertTrue(my.walPosition(r2T).getAsLong() > my.walPosition(r1T).getAsLong());
        long rowsOnR1 = scalar(r1Port, "select count(*) from w.t");

        // the primary dies (kill -9, no clean shutdown)
        new ProcessBuilder("sh", "-c", "pkill -9 -f -- --port=" + pPort).start().waitFor();
        waitUntil("primary to be gone", () -> my.role(primaryT) == FailoverMonitor.NodeRole.UNREACHABLE);

        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertEquals(pUrl, live.get("my").jdbcUrl(), "not before the confirmation window");
        monitor.evaluateOnce(false);

        assertEquals(r2Url, live.get("my").jdbcUrl(), "r2 had received the most -> promoted");
        assertEquals(0, scalar(r2Port, "select @@global.read_only"), "promoted node is writable");
        assertEquals(FailoverMonitor.NodeRole.WRITABLE, my.role(r2T));
        try (Connection c = conn(r2Port); var st = c.createStatement(); var rs = st.executeQuery("show replica status")) {
            assertFalse(rs.next(), "replication configuration was reset on the promoted node");
        }
        assertEquals(1, scalar(r2Port, "select count(*) from w.t where id = 21"), "no received transaction was lost");
        assertEquals(1, scalar(r1Port, "select @@global.read_only"), "the other replica stays read-only");
        assertTrue(rowsOnR1 < scalar(r2Port, "select count(*) from w.t"), "r1 really was behind");

        // writes through the registry's new primary, and warp_config names it
        try (Connection c = live.get("my").open(); var st = c.createStatement()) {
            st.execute("insert into w.t values (30, 'after-promotion')");
        }
        WarpConfig latest = store.readLatest().orElseThrow().payload();
        assertTrue(latest.backends().startsWith("my=" + r2Url), latest.backends());
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("promoted")));

        // the surviving replica was repointed at r2: it fetches what it had missed and the new write
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("repointed")), "repointed event");
        waitUntil("r1 to follow the new primary", () -> scalar(r1Port, "select count(*) from w.t where id in (21, 30)") == 2);
        try (Connection c = conn(r1Port); var st = c.createStatement(); var rs = st.executeQuery("show replica status")) {
            assertTrue(rs.next());
            assertEquals("127.0.0.1", rs.getString("Source_Host"));
            assertEquals(Integer.parseInt(r2Port), rs.getInt("Source_Port"));
            assertEquals("Yes", rs.getString("Replica_IO_Running"));
            assertEquals("Yes", rs.getString("Replica_SQL_Running"));
        }
        assertEquals(1, scalar(r1Port, "select @@global.read_only"), "and it is still read-only");
    }
}
