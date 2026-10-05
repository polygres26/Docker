package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live Oracle Data Guard check -- opt-in, and NEVER RUN by this project's authors (it needs a real
 * Data Guard pair on Enterprise Edition, which cannot be stood up in this project's test setup), so
 * it is unproven: expect to adjust it on first contact with a real configuration.
 *
 * <p>Needs a physical standby opened READ ONLY WITH APPLY (Active Data Guard). Environment:
 * {@code WARP_TEST_ORACLE_DG_PRIMARY_URL}, {@code WARP_TEST_ORACLE_DG_STANDBY_URL},
 * {@code WARP_TEST_ORACLE_DG_USER}, {@code WARP_TEST_ORACLE_DG_PASSWORD} (the user needs SELECT on
 * V_$DATABASE and V_$DATAGUARD_STATS on both). Optional {@code WARP_TEST_ORACLE_DG_FAILOVER_CMD}: a shell
 * command that fails the pair over to the standby (for example a {@code dgmgrl} script) and returns once
 * the standby is open READ WRITE; without it the failover-follow part is skipped. Warp itself never
 * performs the failover.
 */
class OracleDataGuardLiveTest {

    private static String env(String n) {
        return System.getenv(n);
    }

    @Test
    void roleLagRoutingAndFollowAgainstARealDataGuardPair() throws Exception {
        String pUrl = env("WARP_TEST_ORACLE_DG_PRIMARY_URL");
        String sUrl = env("WARP_TEST_ORACLE_DG_STANDBY_URL");
        String user = env("WARP_TEST_ORACLE_DG_USER");
        String pw = env("WARP_TEST_ORACLE_DG_PASSWORD");
        Assumptions.assumeTrue(pUrl != null && sUrl != null && user != null && pw != null,
                "needs WARP_TEST_ORACLE_DG_* (see this class's javadoc)");
        EngineHa ha = EngineHa.forDialect(SourceDialect.ORACLE);
        BackendTarget primary = new BackendTarget("p", pUrl, user, pw);
        BackendTarget standby = new BackendTarget("s", sUrl, user, pw);

        assertEquals(FailoverMonitor.NodeRole.WRITABLE, ha.role(primary));
        assertEquals(FailoverMonitor.NodeRole.READ_ONLY, ha.role(standby));
        var lag = ha.lag(standby, 0);
        assertTrue(lag.ok() && lag.isReplica(), "standby must be open read-only with apply: " + lag.message());
        var primaryLag = ha.lag(primary, 0);
        assertTrue(primaryLag.ok() && !primaryLag.isReplica());

        String spec = "ora=" + pUrl.replace(";", "%3B") + "|" + user + "|" + pw + "||" + sUrl.replace(";", "%3B") + "~60|follow";
        BackendRegistry registry = BackendRegistry.fromConfig(spec, null);
        registry.replicaRouter().probeAll();
        Thread.sleep(2200);
        try (Connection pc = DriverManager.getConnection(pUrl, user, pw)) {
            RoutingBackendExecutor ex = new RoutingBackendExecutor(registry, new JdbcBackendExecutor(pc))
                    .withDefaultExecutorBackendName("ora").withSessionStatePredicate(() -> false);
            // the standby reports DATABASE_ROLE = PHYSICAL STANDBY; the primary PRIMARY
            ExecutionResult r = ex.execute(Statement.of(SourceDialect.ORACLE,
                    "select database_role from v$database", List.of()));
            assertEquals("PHYSICAL STANDBY", String.valueOf(r.rows().get(0).get(0)), "a plain read is served by the standby");
        }

        String failoverCmd = env("WARP_TEST_ORACLE_DG_FAILOVER_CMD");
        Assumptions.assumeTrue(failoverCmd != null && !failoverCmd.isBlank(),
                "WARP_TEST_ORACLE_DG_FAILOVER_CMD not set: skipping the failover-follow part");
        BackendRegistry live = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(live, FailoverMonitor::probeRole,
                (b, o, n, rs) -> live.applyFailoverLocally(b, o, n, rs), null, System::currentTimeMillis, 3, 60, 5);
        monitor.evaluateOnce(false);
        assertEquals(pUrl, live.get("ora").jdbcUrl());
        assertEquals(0, new ProcessBuilder("sh", "-c", failoverCmd).inheritIO().start().waitFor(), "failover command failed");
        for (int i = 0; i < 6 && !sUrl.equals(live.get("ora").jdbcUrl()); i++) {
            monitor.evaluateOnce(false);
        }
        assertEquals(sUrl, live.get("ora").jdbcUrl(), "Warp followed the Data Guard failover");
        assertEquals(FailoverMonitor.NodeRole.WRITABLE, ha.role(standby));
    }
}
