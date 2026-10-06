package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Oracle Data Guard control (planned switchover, failover) against a scripted fake database that enforces the preconditions of each statement
 * as Oracle documents them. <b>This checks Warp's sequence, preflight and error handling; it does not show that the statements work on a real
 * Data Guard configuration</b>, which was not available.
 */
class OracleHaControlTest {

    /** One database: its state and the statements run on it. */
    private static final class Db {
        String role;
        String status;
        String open;
        String unique;
        String broker = "FALSE";
        boolean mrpRunning;
        String receivedScn = "4815162342";
        Db partner;

        Db(String role, String status, String open, String unique) {
            this.role = role;
            this.status = status;
            this.open = open;
            this.unique = unique;
        }
    }

    private final Map<String, Db> dbs = new HashMap<>();
    private final List<String> log = new ArrayList<>();
    private final List<String> nonSysdba = new ArrayList<>();

    private static String urlOf(String name) {
        return "jdbc:oracle:thin:@//" + name + ":1521/svc";
    }

    private BackendTarget node(String name, Db db) {
        dbs.put(urlOf(name), db);
        return new BackendTarget(name, urlOf(name), "sys as sysdba", "pw");
    }

    private String nameOf(BackendTarget n) {
        String u = n.jdbcUrl();
        return u.substring(u.indexOf("//") + 2, u.indexOf(":1521"));
    }

    @BeforeEach
    void install() {
        OracleHa.controlForTesting(true);
        OracleHa.useSqlForTesting(new OracleHa.Sql() {
            @Override
            public String one(BackendTarget n, boolean sysdba, String q) throws SQLException {
                if (!sysdba) {
                    nonSysdba.add(q);
                }
                Db d = dbs.get(n.jdbcUrl());
                if (d == null) {
                    throw new SQLException("ORA-12541: no listener");
                }
                String lq = q.toLowerCase();
                if (lq.contains("database_role")) {
                    return d.role;
                }
                if (lq.contains("switchover_status")) {
                    return d.status;
                }
                if (lq.contains("open_mode")) {
                    return d.open;
                }
                if (lq.contains("db_unique_name")) {
                    return d.unique;
                }
                if (lq.contains("dg_broker_start")) {
                    return d.broker;
                }
                if (lq.contains("next_change#")) {
                    return d.receivedScn;
                }
                throw new SQLException("unscripted query: " + q);
            }

            @Override
            public void exec(BackendTarget n, boolean sysdba, String st) throws SQLException {
                if (!sysdba) {
                    nonSysdba.add(st);
                }
                Db d = dbs.get(n.jdbcUrl());
                if (d == null) {
                    throw new SQLException("ORA-12541: no listener");
                }
                log.add(nameOf(n) + ": " + st);
                if (st.startsWith("ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY")) {
                    if (!d.role.equals("PRIMARY") || !d.status.equals("TO STANDBY")) {
                        throw new SQLException("ORA-16416: switchover target is not synchronized");
                    }
                    d.role = "PHYSICAL STANDBY";
                    d.open = "MOUNTED";
                    d.status = "RECOVERY NEEDED"; // a database that was a primary a moment ago is not applying redo yet
                    if (d.partner != null) {
                        d.partner.status = "TO PRIMARY"; // the end-of-redo marker arrives and is applied
                    }
                } else if (st.startsWith("ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY")) {
                    if (!d.role.equals("PHYSICAL STANDBY") || !d.status.equals("TO PRIMARY")) {
                        throw new SQLException("ORA-16416: not ready to become the primary");
                    }
                    d.role = "PRIMARY";
                    d.open = "MOUNTED";
                    d.status = "TO STANDBY";
                } else if (st.startsWith("ALTER DATABASE FAILOVER TO")) {
                    if (!d.role.equals("PHYSICAL STANDBY")) {
                        throw new SQLException("ORA-16139: media recovery required");
                    }
                    d.role = "PRIMARY";
                    d.open = "MOUNTED";
                    d.status = "TO STANDBY";
                } else if (st.equals("ALTER DATABASE OPEN")) {
                    if (!d.open.equals("MOUNTED")) {
                        throw new SQLException("ORA-01531: a database already open by the instance");
                    }
                    d.open = d.role.equals("PRIMARY") ? "READ WRITE" : "READ ONLY WITH APPLY";
                } else if (st.startsWith("ALTER DATABASE RECOVER MANAGED STANDBY DATABASE")) {
                    if (d.mrpRunning) {
                        throw new SQLException("ORA-01153: an incompatible media recovery is active");
                    }
                    d.mrpRunning = true;
                    if (d.status.equals("RECOVERY NEEDED")) {
                        d.status = "TO PRIMARY";
                    }
                } else {
                    throw new SQLException("unscripted statement: " + st);
                }
            }
        });
    }

    @AfterEach
    void restore() {
        OracleHa.controlForTesting(null);
        OracleHa.useSqlForTesting(null);
        OracleHa.unfreezeWaitMillis = 60_000;
    }

    private static final OracleHa HA = OracleHa.INSTANCE;

    @Test
    void offUnlessTheOperatorSwitchesItOn() {
        OracleHa.controlForTesting(false);
        assertFalse(HA.supportsPromote());
        assertFalse(HA.supportsSwitchover());
        BackendTarget p = node("p", new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM"));
        BackendTarget s = node("s", new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "STBY"));
        assertThrows(UnsupportedOperationException.class, () -> HA.prepareSwitchover(p, s));
        assertThrows(UnsupportedOperationException.class, () -> HA.promote(s));
        assertTrue(log.isEmpty(), "nothing was run");
        OracleHa.controlForTesting(true);
        assertTrue(HA.supportsPromote() && HA.supportsSwitchover());
    }

    @Test
    void aPlannedSwitchoverRunsTheStatementsInOrderAndLeavesTheOldPrimaryAStandby() throws Exception {
        Db prim = new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM");
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "STBY");
        prim.partner = stby;
        BackendTarget p = node("p", prim);
        BackendTarget s = node("s", stby);

        HA.prepareSwitchover(p, s);
        assertTrue(log.isEmpty(), "preparing changes nothing");
        HA.freezeWrites(p);
        HA.awaitCaughtUp(p, s, 5);
        HA.promote(s);
        assertTrue(HA.demoteToReplica(p, s));

        assertEquals(List.of(
                "p: ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH SESSION SHUTDOWN",
                "s: ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN",
                "s: ALTER DATABASE OPEN",
                "p: ALTER DATABASE OPEN",
                "p: ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT FROM SESSION"), log);
        assertEquals("PRIMARY", stby.role);
        assertEquals("READ WRITE", stby.open);
        assertEquals("PHYSICAL STANDBY", prim.role);
        assertTrue(prim.mrpRunning, "the old primary applies redo again");
        assertTrue(nonSysdba.isEmpty(), "every administrative statement ran as SYSDBA: " + nonSysdba);
    }

    @Test
    void aSwitchoverThatCannotWorkIsRefusedBeforeAnythingIsTouched() {
        Db prim = new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM");
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "STBY");
        BackendTarget p = node("p", prim);
        BackendTarget s = node("s", stby);

        prim.status = "NOT ALLOWED"; // redo transport is broken
        var e1 = assertThrows(SQLException.class, () -> HA.prepareSwitchover(p, s));
        assertTrue(e1.getMessage().contains("SWITCHOVER_STATUS is NOT ALLOWED"), e1.getMessage());
        prim.status = "SESSIONS ACTIVE"; // fine: the command ends the sessions
        stby.broker = "TRUE";
        var e2 = assertThrows(SQLException.class, () -> HA.prepareSwitchover(p, s));
        assertTrue(e2.getMessage().contains("Data Guard Broker") && e2.getMessage().contains("dgmgrl"), e2.getMessage());
        stby.broker = "FALSE";
        stby.role = "PRIMARY";
        var e3 = assertThrows(SQLException.class, () -> HA.prepareSwitchover(p, s));
        assertTrue(e3.getMessage().contains("not a physical standby"), e3.getMessage());
        assertTrue(log.isEmpty(), "no statement was run by a refused switchover");
        stby.role = "PHYSICAL STANDBY";
        assertDoesNotThrowPrepare(p, s); // and once everything is right it is accepted
    }

    private void assertDoesNotThrowPrepare(BackendTarget p, BackendTarget s) {
        try {
            HA.prepareSwitchover(p, s);
            HA.unfreezeWrites(p); // nothing was demoted: nothing to undo, and the prepared state is cleared
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void abortingAfterTheDemotionStartsRedoApplySwitchesTheOldPrimaryBackAndOpensIt() throws Exception {
        Db prim = new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM");
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "STBY");
        prim.partner = stby;
        BackendTarget p = node("p", prim);
        BackendTarget s = node("s", stby);
        HA.prepareSwitchover(p, s);
        HA.freezeWrites(p);
        assertEquals("PHYSICAL STANDBY", prim.role);
        HA.unfreezeWrites(p); // the standby never took over: abort
        assertEquals(List.of(
                "p: ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH SESSION SHUTDOWN",
                "p: ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT FROM SESSION",
                "p: ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN",
                "p: ALTER DATABASE OPEN"), log);
        assertEquals("PRIMARY", prim.role);
        assertEquals("READ WRITE", prim.open);
    }

    @Test
    void anAbortThatCannotSwitchBackSaysSoInsteadOfClaimingWritesAreBack() throws Exception {
        Db prim = new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM");
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "STBY");
        prim.partner = stby;
        BackendTarget p = node("p", prim);
        BackendTarget s = node("s", stby);
        HA.prepareSwitchover(p, s);
        HA.freezeWrites(p);
        OracleHa.unfreezeWaitMillis = 1500;
        prim.mrpRunning = true; // redo apply is running yet the database still does not reach TO PRIMARY
        prim.status = "SWITCHOVER LATENT";
        var e = assertThrows(SQLException.class, () -> HA.unfreezeWrites(p));
        assertTrue(e.getMessage().contains("could not be switched back") && e.getMessage().contains("finish it by hand"), e.getMessage());
        assertEquals("PHYSICAL STANDBY", prim.role, "it is left as it is, not guessed at");
    }

    @Test
    void aStandbyThatNeedsRedoApplyGetsItStartedWhileWaiting() throws Exception {
        Db prim = new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM");
        Db stby = new Db("PHYSICAL STANDBY", "RECOVERY NEEDED", "MOUNTED", "STBY");
        BackendTarget p = node("p", prim);
        BackendTarget s = node("s", stby);
        HA.awaitCaughtUp(p, s, 5);
        assertEquals(List.of("s: ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT FROM SESSION"), log);
        stby.status = "SWITCHOVER LATENT";
        var e = assertThrows(SQLException.class, () -> HA.awaitCaughtUp(p, s, 1));
        assertTrue(e.getMessage().contains("SWITCHOVER LATENT, not TO PRIMARY"), e.getMessage());
    }

    @Test
    void aFailoverWhenThePrimaryIsGoneUsesFailoverToAndOpensTheDatabase() throws Exception {
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "MOUNTED", "STBY1");
        BackendTarget s = node("s", stby);
        HA.promote(s);
        assertEquals(List.of("s: ALTER DATABASE FAILOVER TO STBY1", "s: ALTER DATABASE OPEN"), log);
        assertEquals("PRIMARY", stby.role);
        assertEquals("READ WRITE", stby.open);
        assertTrue(nonSysdba.isEmpty());
    }

    @Test
    void promotingRefusesWhatItCannotDoSafely() {
        Db prim = new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM");
        BackendTarget p = node("p", prim);
        var e1 = assertThrows(SQLException.class, () -> HA.promote(p));
        assertTrue(e1.getMessage().contains("not a physical standby"), e1.getMessage());
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "MOUNTED", "STBY; DROP USER x");
        BackendTarget s = node("s", stby);
        var e2 = assertThrows(SQLException.class, () -> HA.promote(s));
        assertTrue(e2.getMessage().contains("usable DB_UNIQUE_NAME"), e2.getMessage());
        stby.unique = "STBY";
        stby.broker = "TRUE";
        var e3 = assertThrows(SQLException.class, () -> HA.promote(s));
        assertTrue(e3.getMessage().contains("Data Guard Broker"), e3.getMessage());
        assertTrue(log.isEmpty(), "none of those ran a statement");
    }

    @Test
    void theMonitorsSwitchoverRunsOracleThroughPrepareFreezeCatchUpPromoteAndDemote() {
        Db prim = new Db("PRIMARY", "TO STANDBY", "READ WRITE", "PRIM");
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "STBY");
        prim.partner = stby;
        BackendTarget p = node("p", prim);
        BackendTarget s = node("s", stby);
        BackendRegistry reg = BackendRegistry.fromConfig("ora=" + p.jdbcUrl() + "|sys as sysdba|pw||" + s.jdbcUrl() + "~30|follow", null);
        FailoverMonitor monitor = new FailoverMonitor(reg, n -> {
            Db d = dbs.get(n.jdbcUrl());
            return d.role.equals("PRIMARY") && d.open.equals("READ WRITE") ? FailoverMonitor.NodeRole.WRITABLE
                    : d.open.startsWith("READ ONLY") ? FailoverMonitor.NodeRole.READ_ONLY : FailoverMonitor.NodeRole.UNREACHABLE;
        }, (b, o, nu, r) -> reg.applyFailoverLocally(b, o, nu, r), null, () -> 1_000_000L, 3, 60, 5);
        var result = monitor.switchover("ora", s.jdbcUrl());
        assertTrue(result.ok(), result.message());
        assertEquals(s.jdbcUrl(), reg.get("ora").jdbcUrl(), "Warp now points at the new primary");
        assertEquals(List.of(
                "p: ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH SESSION SHUTDOWN",
                "s: ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN",
                "s: ALTER DATABASE OPEN",
                "p: ALTER DATABASE OPEN",
                "p: ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT FROM SESSION"), log);
        assertTrue(result.message().contains("replicates from the new one"), result.message());
    }

    @Test
    void aSwitchoverThePreflightRefusesChangesNothingThroughTheMonitorToo() {
        Db prim = new Db("PRIMARY", "NOT ALLOWED", "READ WRITE", "PRIM");
        Db stby = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "STBY");
        BackendTarget p = node("p", prim);
        BackendTarget s = node("s", stby);
        BackendRegistry reg = BackendRegistry.fromConfig("ora=" + p.jdbcUrl() + "|sys as sysdba|pw||" + s.jdbcUrl() + "~30|follow", null);
        FailoverMonitor monitor = new FailoverMonitor(reg, n -> dbs.get(n.jdbcUrl()).role.equals("PRIMARY")
                ? FailoverMonitor.NodeRole.WRITABLE : FailoverMonitor.NodeRole.READ_ONLY,
                (b, o, nu, r) -> reg.applyFailoverLocally(b, o, nu, r), null, () -> 1_000_000L, 3, 60, 5);
        var result = monitor.switchover("ora", s.jdbcUrl());
        assertFalse(result.ok());
        assertTrue(result.message().contains("could not prepare") && result.message().contains("SWITCHOVER_STATUS is NOT ALLOWED"), result.message());
        assertEquals(p.jdbcUrl(), reg.get("ora").jdbcUrl());
        assertTrue(log.isEmpty());
    }

    @Test
    void standbysAreRankedByTheLastChangeNumberOfTheRedoTheyReceived() throws Exception {
        Db a = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "A");
        Db b = new Db("PHYSICAL STANDBY", "NOT ALLOWED", "READ ONLY WITH APPLY", "B");
        a.receivedScn = "5000000000";
        b.receivedScn = "5000000123";
        assertEquals(5000000000L, HA.walPosition(node("a", a)).getAsLong());
        assertTrue(HA.walPosition(node("b", b)).getAsLong() > HA.walPosition(node("a", a)).getAsLong());
        a.receivedScn = null; // nothing received yet: no position, so it cannot be ranked
        assertTrue(HA.walPosition(node("a", a)).isEmpty());
    }
}
