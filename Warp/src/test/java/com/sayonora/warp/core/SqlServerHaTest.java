package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.core.BackendSetModel.ModelException;
import com.sayonora.warp.core.FailoverMonitor.NodeRole;
import com.sayonora.warp.core.ReplicaRouter.LagSample;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * SQL Server Availability Group support is NOT verified against a live AG (see {@link SqlServerHa});
 * these pin down its decision logic and the "follow, never fail over" contract.
 */
class SqlServerHaTest {

    private static LagSample lag(String upd, Integer primary, String state, String health, Boolean susp, Double secLag,
            Long redoQ, Long redoRate) {
        return SqlServerHa.lagFrom(upd, primary, state, health, susp, secLag, redoQ, redoRate, 1);
    }

    @Test
    void writableOnlyForAReadWriteDatabaseThatIsThePrimaryIfInAnAvailabilityGroup() {
        assertEquals(NodeRole.WRITABLE, SqlServerHa.roleFrom("READ_WRITE", 1));
        assertEquals(NodeRole.WRITABLE, SqlServerHa.roleFrom("read_write", null), "standalone database, not in an AG");
        assertEquals(NodeRole.READ_ONLY, SqlServerHa.roleFrom("READ_ONLY", 0), "readable secondary");
        assertEquals(NodeRole.READ_ONLY, SqlServerHa.roleFrom("READ_WRITE", 0), "never writable on a secondary");
        assertEquals(NodeRole.READ_ONLY, SqlServerHa.roleFrom(null, 1));
    }

    @Test
    void aPrimaryOrANonAgDatabaseIsNotAReplica() {
        LagSample p = lag("READ_WRITE", 1, null, null, null, null, null, null);
        assertTrue(p.ok() && !p.isReplica());
        LagSample standalone = lag("READ_WRITE", null, null, null, null, null, null, null);
        assertTrue(standalone.ok() && !standalone.isReplica());
    }

    @Test
    void lagTrustOrderForAReadableSecondary() {
        // fully synchronized -> 0
        LagSample sync = lag("READ_ONLY", 0, "SYNCHRONIZED", "HEALTHY", false, null, 0L, 0L);
        assertTrue(sync.ok() && sync.isReplica());
        assertEquals(0.0, sync.lagSeconds());
        // the server's own lag figure wins while synchronizing
        assertEquals(6.5, lag("READ_ONLY", 0, "SYNCHRONIZING", "HEALTHY", false, 6.5, 999L, 10L).lagSeconds());
        // else backlog / rate
        assertEquals(20.0, lag("READ_ONLY", 0, "SYNCHRONIZING", "HEALTHY", false, null, 2000L, 100L).lagSeconds());
        // empty backlog -> 0
        assertEquals(0.0, lag("READ_ONLY", 0, "SYNCHRONIZING", "HEALTHY", false, null, 0L, null).lagSeconds());
    }

    @Test
    void anythingUntrustworthyIsUnmeasurableNeverZero() {
        assertFalse(lag("READ_ONLY", 0, "SYNCHRONIZING", "HEALTHY", true, 1.0, 0L, 1L).ok(), "suspended");
        assertFalse(lag("READ_ONLY", 0, "SYNCHRONIZING", "NOT_HEALTHY", false, 1.0, 0L, 1L).ok());
        assertFalse(lag("READ_ONLY", 0, "SYNCHRONIZING", "PARTIALLY_HEALTHY", false, 1.0, 0L, 1L).ok());
        assertFalse(lag("READ_ONLY", 0, "NOT SYNCHRONIZING", "HEALTHY", false, null, 0L, 0L).ok());
        assertFalse(lag("READ_ONLY", 0, "REVERTING", "HEALTHY", false, null, 0L, 0L).ok());
        assertFalse(lag("READ_ONLY", 0, "SYNCHRONIZING", "HEALTHY", false, null, 5000L, null).ok(),
                "backlog but no redo rate");
        assertFalse(lag("READ_ONLY", 0, "SYNCHRONIZING", "HEALTHY", false, null, 5000L, 0L).ok());
        assertFalse(lag("RESTRICTED", 0, "SYNCHRONIZED", "HEALTHY", false, null, 0L, 0L).ok(), "not readable");
        assertFalse(lag(null, 0, "SYNCHRONIZED", "HEALTHY", false, null, 0L, 0L).ok());
    }

    @Test
    void sqlServerIsSupportedAndCanBePromotedByWarp() {
        EngineHa ha = EngineHa.forDialect(SourceDialect.SQL_SERVER);
        assertTrue(ha != null);
        assertTrue(ha.supportsPromote());
        assertEquals(null, EngineHa.forDialect(SourceDialect.SYNAPSE), "Synapse has no availability groups");
    }

    @Test
    void semicolonsInSqlServerUrlsSurviveTheSpecRoundTripAndPromoteModeIsAccepted() {
        String spec = "s=jdbc:sqlserver://p:1433%3BdatabaseName=d|u|pw||jdbc:sqlserver://r:1433%3BdatabaseName=d%3BapplicationIntent=ReadOnly~3";
        BackendRegistry reg = BackendRegistry.fromConfig(spec, null);
        assertEquals("jdbc:sqlserver://p:1433;databaseName=d", reg.get("s").jdbcUrl());
        assertEquals("jdbc:sqlserver://r:1433;databaseName=d;applicationIntent=ReadOnly",
                reg.replicaSpecsOf("s").get(0).url());
        BackendSetModel m = BackendSetModel.from(WarpConfig.fromEnvDefaults()
                .withBackendModel(spec, null, null, null, null, null), null);
        assertEquals(spec, m.applyTo(WarpConfig.fromEnvDefaults()).backends());
        // promote mode is accepted for SQL Server now (it is refused only for engines Warp cannot promote)
        m.patchBackend("s", null, null, null, false, null, null, null, "promote");
    }

    private final Map<String, NodeRole> cluster = new HashMap<>();
    private final AtomicLong clock = new AtomicLong(1_000_000);

    @Test
    void followRepointsASqlServerBackendOnceTheAgHasANewWritablePrimary() {
        String p = "jdbc:sqlserver://p:1433;databaseName=d";
        String r = "jdbc:sqlserver://r:1433;databaseName=d";
        BackendRegistry reg = BackendRegistry.fromConfig("s=" + p.replace(";", "%3B") + "|u|pw||" + r.replace(";", "%3B") + "~5", null);
        FailoverMonitor m = new FailoverMonitor(reg, n -> cluster.getOrDefault(n.jdbcUrl(), NodeRole.UNREACHABLE),
                (b, o, nu, rs) -> reg.applyFailoverLocally(b, o, nu, rs), null, clock::get, 3, 60, 5);
        cluster.put(p, NodeRole.UNREACHABLE);
        cluster.put(r, NodeRole.UNREACHABLE); // a non-readable secondary rejects logins until it is promoted
        for (int i = 0; i < 4; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(p, reg.get("s").jdbcUrl());
        cluster.put(r, NodeRole.WRITABLE); // the AG failed over; r is now primary and accepts writes
        for (int i = 0; i < 3; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(r, reg.get("s").jdbcUrl());
        assertEquals(p, reg.replicaSpecsOf("s").get(0).url());
    }
}
