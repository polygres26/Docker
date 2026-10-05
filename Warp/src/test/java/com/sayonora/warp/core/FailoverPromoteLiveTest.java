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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live promote-mode check on REAL Postgres: one primary, two streaming replicas, and a fourth server
 * holding warp_config plus the lease/observation tables. Opt-in via WARP_TEST_PROMOTE_* env, with
 * WARP_HOST/WARP_PORT/WARP_USER/WARP_PASSWORD pointing at the config server. Here Warp itself runs
 * pg_promote -- nothing else promotes.
 */
class FailoverPromoteLiveTest {

    @Test
    void warpPromotesTheBestReplicaOnlyWithTheLeaseAndAMajorityAndRecordsItInWarpConfig() throws Exception {
        String pPort = System.getenv("WARP_TEST_PROMOTE_PRIMARY_PORT");
        String r1Port = System.getenv("WARP_TEST_PROMOTE_REPLICA1_PORT");
        String r2Port = System.getenv("WARP_TEST_PROMOTE_REPLICA2_PORT");
        String cfgPort = System.getenv("WARP_TEST_PROMOTE_CONFIG_PORT");
        String bin = System.getenv("WARP_TEST_PROMOTE_PG_BIN");
        String pDir = System.getenv("WARP_TEST_PROMOTE_PRIMARY_DIR");
        Assumptions.assumeTrue(pPort != null && r1Port != null && r2Port != null && cfgPort != null && bin != null
                && pDir != null);

        String pUrl = url(pPort);
        String r1Url = url(r1Port);
        String r2Url = url(r2Port);
        String spec = "pg=" + pUrl + "|warp|||" + r1Url + "~4^" + r2Url + "~4|promote";

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

        // healthy: lag is measured and recorded while the primary is up; nothing is promoted
        registry.replicaRouter().probeAll();
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertEquals(pUrl, registry.get("pg").jdbcUrl());
        assertTrue(inRecovery(r1Port) && inRecovery(r2Port));

        // the primary dies; nobody else promotes anything
        run(bin + "/pg_ctl", "-D", pDir, "-m", "immediate", "-w", "stop");

        // another instance holds the lease: Warp must not promote, however long the primary is down
        try (Connection c = config(cfgPort); var st = c.createStatement()) {
            st.execute("insert into warp_failover_lease (backend, holder, term, expires_at) values "
                    + "('pg', 'some-other-instance', 7, now() + interval '5 minutes') "
                    + "on conflict (backend) do update set holder = 'some-other-instance', "
                    + "expires_at = now() + interval '5 minutes'");
        }
        for (int i = 0; i < 4; i++) {
            monitor.evaluateOnce(false);
        }
        assertTrue(inRecovery(r1Port) && inRecovery(r2Port), "lease held elsewhere -> no promotion");
        assertEquals(pUrl, registry.get("pg").jdbcUrl());
        assertTrue(monitor.recentEvents().stream()
                .anyMatch(e -> e.kind().equals("promote-blocked") && e.detail().contains("lease")));

        // the other holder's lease expires; now this instance promotes
        try (Connection c = config(cfgPort); var st = c.createStatement()) {
            st.execute("update warp_failover_lease set expires_at = now() - interval '1 second' where backend = 'pg'");
        }
        monitor.evaluateOnce(false);

        // exactly one replica became writable (both were equally caught up -> the first), the other did not
        boolean r1Writable = !inRecovery(r1Port);
        boolean r2Writable = !inRecovery(r2Port);
        assertTrue(r1Writable ^ r2Writable, "exactly one promotion, never two writers");
        String newPrimaryUrl = r1Writable ? r1Url : r2Url;
        assertEquals(newPrimaryUrl, registry.get("pg").jdbcUrl());
        assertEquals(r1Url, newPrimaryUrl, "equal WAL -> the earlier replica wins");

        // the promoted node really takes writes through the registry's new primary
        try (Connection c = registry.get("pg").open(); var st = c.createStatement()) {
            st.execute("insert into t values (424242, 'promoted-by-warp')");
        }

        // recorded in warp_config, and the lease was released for the next failover
        WarpConfig latest = store.readLatest().orElseThrow().payload();
        assertTrue(latest.backends().startsWith("pg=" + newPrimaryUrl), latest.backends());
        try (Connection c = config(cfgPort); var st = c.createStatement();
                var rs = st.executeQuery("select expires_at < now() from warp_failover_lease where backend = 'pg'")) {
            assertTrue(rs.next() && rs.getBoolean(1), "lease released");
        }
        // a second pass changes nothing (the new primary is writable)
        monitor.evaluateOnce(false);
        assertEquals(newPrimaryUrl, registry.get("pg").jdbcUrl());
        assertFalse(monitor.recentEvents().isEmpty());
    }

    private static String url(String port) {
        return "jdbc:postgresql://127.0.0.1:" + port + "/postgres";
    }

    private static Connection config(String port) throws Exception {
        return DriverManager.getConnection(url(port), "warp", "");
    }

    private static boolean inRecovery(String port) throws Exception {
        try (Connection c = config(port); var st = c.createStatement(); var rs = st.executeQuery("select pg_is_in_recovery()")) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    private static void run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).inheritIO().start();
        assertEquals(0, p.waitFor(), String.join(" ", cmd));
    }
}
