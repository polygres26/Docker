package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.core.BackendSetModel.ModelException;
import com.sayonora.warp.core.FailoverMonitor.NodeRole;
import com.sayonora.warp.core.ReplicaRouter.LagSample;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Oracle Data Guard support is NOT verified against a live standby (see {@link OracleHa}); these tests
 * pin down its decision logic and the "follow, never promote" contract.
 */
class OracleHaTest {

    @Test
    void intervalsParseToSeconds() {
        assertEquals(0.0, OracleHa.parseIntervalSeconds("+00 00:00:00"));
        assertEquals(2.0, OracleHa.parseIntervalSeconds("+00 00:00:02"));
        assertEquals(3723.0, OracleHa.parseIntervalSeconds("+00 01:02:03"));
        assertEquals(86400.0 + 5.5, OracleHa.parseIntervalSeconds("+01 00:00:05.5"));
        assertEquals(-5.0, OracleHa.parseIntervalSeconds("-00 00:00:05"));
        assertNull(OracleHa.parseIntervalSeconds(""));
        assertNull(OracleHa.parseIntervalSeconds(null));
        assertNull(OracleHa.parseIntervalSeconds("2 seconds"));
    }

    @Test
    void onlyAReadWritePrimaryIsWritable() {
        assertEquals(NodeRole.WRITABLE, OracleHa.roleFrom("PRIMARY", "READ WRITE"));
        assertEquals(NodeRole.WRITABLE, OracleHa.roleFrom(" primary ", " read write "));
        assertEquals(NodeRole.READ_ONLY, OracleHa.roleFrom("PHYSICAL STANDBY", "READ ONLY WITH APPLY"));
        assertEquals(NodeRole.READ_ONLY, OracleHa.roleFrom("PHYSICAL STANDBY", "MOUNTED"));
        assertEquals(NodeRole.READ_ONLY, OracleHa.roleFrom("PRIMARY", "READ ONLY"), "e.g. mid-switchover");
        assertEquals(NodeRole.READ_ONLY, OracleHa.roleFrom(null, null));
    }

    @Test
    void lagIsOnlyTrustedFromAnOpenStandbyWithFreshApplyStatistics() {
        LagSample ok = OracleHa.lagFrom("PHYSICAL STANDBY", "READ ONLY WITH APPLY", "+00 00:00:03", "+00 00:00:01", 10.0, 1);
        assertTrue(ok.ok() && ok.isReplica());
        assertEquals(3.0, ok.lagSeconds(), "the larger of apply and transport lag");
        assertEquals(7.0, OracleHa.lagFrom("PHYSICAL STANDBY", "READ ONLY WITH APPLY", "+00 00:00:02", "+00 00:00:07", 1.0, 1)
                .lagSeconds());
        assertEquals(2.0, OracleHa.lagFrom("PHYSICAL STANDBY", "READ ONLY WITH APPLY", "+00 00:00:02", null, 1.0, 1)
                .lagSeconds(), "no transport figure -> apply lag alone");

        LagSample primary = OracleHa.lagFrom("PRIMARY", "READ WRITE", null, null, null, 1);
        assertTrue(primary.ok() && !primary.isReplica());

        assertFalse(OracleHa.lagFrom("PHYSICAL STANDBY", "MOUNTED", "+00 00:00:00", "+00 00:00:00", 1.0, 1).ok(),
                "a mounted standby cannot serve reads");
        assertFalse(OracleHa.lagFrom("PHYSICAL STANDBY", "READ ONLY", null, null, 1.0, 1).ok(), "no apply lag value");
        assertFalse(OracleHa.lagFrom("PHYSICAL STANDBY", "READ ONLY WITH APPLY", "+00 00:00:00", null, 600.0, 1).ok(),
                "stale statistics are not a 0");
        assertFalse(OracleHa.lagFrom("PHYSICAL STANDBY", "READ ONLY WITH APPLY", "+00 00:00:00", null, null, 1).ok(),
                "unknown statistics age");
        assertFalse(OracleHa.lagFrom("SNAPSHOT STANDBY", "READ WRITE", "+00 00:00:00", null, 1.0, 1).ok());
        assertFalse(OracleHa.lagFrom(null, null, null, null, null, 1).ok());
    }

    @Test
    void oracleIsASupportedEngineButNeverPromotedByWarp() {
        EngineHa ha = EngineHa.forDialect(SourceDialect.ORACLE);
        assertTrue(ha != null);
        assertFalse(ha.supportsPromote());
        assertThrows(UnsupportedOperationException.class,
                () -> ha.promote(new BackendTarget("o", "jdbc:oracle:thin:@//h:1521/s", "u", "p")));
        assertTrue(EngineHa.forDialect(SourceDialect.POSTGRES).supportsPromote());
        assertTrue(EngineHa.forDialect(SourceDialect.MYSQL).supportsPromote());
    }

    @Test
    void theApiRefusesPromoteModeForOracleButAcceptsFollow() {
        WarpConfig c = WarpConfig.fromEnvDefaults().withBackendModel(
                "o=jdbc:oracle:thin:@//p:1521/svc|u|pw||jdbc:oracle:thin:@//r:1521/svc", null, null, null, null, null);
        BackendSetModel m = BackendSetModel.from(c, null);
        ModelException e = assertThrows(ModelException.class,
                () -> m.patchBackend("o", null, null, null, false, null, null, null, "promote"));
        assertEquals(400, e.status());
        m.patchBackend("o", null, null, null, false, null, null, null, "follow");
        assertEquals("follow", m.backend("o").effectiveFailoverMode());
        assertThrows(ModelException.class, () -> m.addBackend("default", "o2", "jdbc:oracle:thin:@//x:1521/s", "u", "p",
                null, null, List.of(), "jdbc:oracle:thin:@//y:1521/s", "promote"));
        // Postgres still accepts it
        BackendSetModel pg = BackendSetModel.from(WarpConfig.fromEnvDefaults().withBackendModel(
                "p=jdbc:postgresql://a/db||", null, null, null, null, null), null);
        pg.patchBackend("p", null, null, null, false, null, null, "jdbc:postgresql://b/db", "promote");
        assertEquals("promote", pg.backend("p").effectiveFailoverMode());
    }

    // ---- the monitor with Oracle ----------------------------------------------------------------

    private static final String OP = "jdbc:oracle:thin:@//p:1521/svc";
    private static final String OR = "jdbc:oracle:thin:@//r:1521/svc";

    private final Map<String, NodeRole> cluster = new HashMap<>();
    private final AtomicLong clock = new AtomicLong(1_000_000);

    private FailoverMonitor monitor(BackendRegistry reg) {
        return new FailoverMonitor(reg, n -> cluster.getOrDefault(n.jdbcUrl(), NodeRole.UNREACHABLE),
                (b, o, nu, r) -> reg.applyFailoverLocally(b, o, nu, r), null, clock::get, 3, 60, 5);
    }

    @Test
    void followModeRepointsAnOracleBackendAtTheStandbyOnceDataGuardOpensItReadWrite() {
        BackendRegistry reg = BackendRegistry.fromConfig("o=" + OP + "|u|pw||" + OR + "~5", null);
        FailoverMonitor m = monitor(reg);
        cluster.put(OP, NodeRole.UNREACHABLE);
        cluster.put(OR, NodeRole.UNREACHABLE); // a mounted standby cannot be probed by an ordinary login
        for (int i = 0; i < 4; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(OP, reg.get("o").jdbcUrl(), "nothing writable yet -> nothing to follow");
        cluster.put(OR, NodeRole.WRITABLE); // the broker failed over and opened it READ WRITE
        for (int i = 0; i < 3; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(OR, reg.get("o").jdbcUrl());
        assertEquals(OP, reg.replicaSpecsOf("o").get(0).url());
    }

    @Test
    void promoteModeOnOracleIsBlockedWithAClearReasonAndNeverPromotes() {
        BackendRegistry reg = BackendRegistry.fromConfig("o=" + OP + "|u|pw||" + OR + "~5|promote", null);
        FailoverMonitor m = monitor(reg).withPromoteHooks(new FailoverMonitor.PromoteHooks(
                new FailoverCoordination() {
                    public void publishObservation(String b, String u, boolean d) {
                    }

                    public java.util.Optional<Long> tryAcquireLease(String b, long t) {
                        return java.util.Optional.of(1L);
                    }

                    public void releaseLease(String b, long t) {
                    }

                    public int votesPrimaryDown(String b, String u, long f) {
                        return 1;
                    }

                    public int liveInstances() {
                        return 1;
                    }
                }, r -> java.util.OptionalLong.of(1), r -> {
                    throw new AssertionError("must never be asked to promote Oracle");
                }, null, 30, 120, 30));
        cluster.put(OP, NodeRole.UNREACHABLE);
        cluster.put(OR, NodeRole.READ_ONLY);
        for (int i = 0; i < 5; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(OP, reg.get("o").jdbcUrl());
        assertTrue(m.recentEvents().stream().anyMatch(e -> e.kind().equals("promote-blocked")
                && e.detail().contains("does not promote ORACLE")), m.recentEvents().toString());
    }
}
