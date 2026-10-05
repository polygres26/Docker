package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.BackendFailoverPersister;
import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live failover-follow check on REAL Postgres: a primary, a streaming replica, and a third server
 * that holds warp_config. Opt-in -- needs WARP_TEST_FAILOVER_* env (see the commit that added it)
 * AND WARP_HOST/WARP_PORT/WARP_USER/WARP_PASSWORD pointing at the config server so the real
 * ConfigStore connects there. The "HA tooling" is {@code pg_ctl promote}; Warp only follows it.
 */
class FailoverFollowLiveTest {

    @Test
    void followsAPromotionAndRecordsItInWarpConfigSoAnotherInstanceAgrees() throws Exception {
        String pPort = System.getenv("WARP_TEST_FAILOVER_PRIMARY_PORT");
        String rPort = System.getenv("WARP_TEST_FAILOVER_REPLICA_PORT");
        String bin = System.getenv("WARP_TEST_FAILOVER_PG_BIN");
        String pDir = System.getenv("WARP_TEST_FAILOVER_PRIMARY_DIR");
        String rDir = System.getenv("WARP_TEST_FAILOVER_REPLICA_DIR");
        Assumptions.assumeTrue(pPort != null && rPort != null && bin != null && pDir != null && rDir != null);

        String pUrl = "jdbc:postgresql://127.0.0.1:" + pPort + "/postgres";
        String rUrl = "jdbc:postgresql://127.0.0.1:" + rPort + "/postgres";
        String spec = "pg=" + pUrl + "|warp|||" + rUrl + "~4";

        ConfigStore store = new ConfigStore(ServerOptions.parse(new String[0]));
        store.ensureSchema();
        store.write(WarpConfig.fromEnvDefaults().withBackendModel(spec, null, null, null, null, null));

        BackendRegistry registry = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(registry, FailoverMonitor::probeRole,
                new BackendFailoverPersister(store, registry), null, System::currentTimeMillis, 3, 60, 5);

        // 1. healthy cluster: nothing happens, however many times we look
        for (int i = 0; i < 4; i++) {
            monitor.evaluateOnce(false);
        }
        assertEquals(pUrl, registry.get("pg").jdbcUrl());

        // 2. the primary dies and the DB's own HA tooling promotes the replica
        run(bin + "/pg_ctl", "-D", pDir, "-m", "immediate", "-w", "stop");
        run(bin + "/pg_ctl", "-D", rDir, "-w", "promote");

        // 3. Warp follows -- but only after the confirmation window, not on the first probe
        monitor.evaluateOnce(false);
        assertEquals(pUrl, registry.get("pg").jdbcUrl(), "one probe must not move traffic");
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertEquals(rUrl, registry.get("pg").jdbcUrl(), "confirmed: writes now follow the promoted node");

        // 4. writes really land on the promoted node
        try (Connection c = registry.get("pg").open()) {
            try (var st = c.createStatement()) {
                st.execute("insert into t values (987654, 'after-failover')");
                var rs = st.executeQuery("select inet_server_port(), pg_is_in_recovery()");
                rs.next();
                assertEquals(Integer.parseInt(rPort), rs.getInt(1));
                assertFalse(rs.getBoolean(2), "the new primary accepts writes");
            }
        }

        // 5. the old primary is now listed as a replica (dead -> never eligible for reads)
        assertEquals(List.of(pUrl), registry.replicaSpecsOf("pg").stream().map(ReplicaSpec::url).toList());

        // 6. warp_config was updated, so ANOTHER instance (fresh registry from the latest config) agrees
        WarpConfig latest = store.readLatest().orElseThrow().payload();
        BackendRegistry other = BackendRegistry.fromConfig(latest.backends(), null);
        assertEquals(rUrl, other.get("pg").jdbcUrl());
        assertEquals(pUrl, other.replicaSpecsOf("pg").get(0).url());
        assertEquals(4.0, other.replicaSpecsOf("pg").get(0).maxLagSeconds());
        assertTrue(latest.backends().contains(rUrl));

        // 7. a second evaluation after the switch is a no-op (new primary is writable)
        monitor.evaluateOnce(false);
        assertEquals(rUrl, registry.get("pg").jdbcUrl());
    }

    private static void run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).inheritIO().start();
        assertEquals(0, p.waitFor(), String.join(" ", cmd));
    }
}
