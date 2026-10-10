package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Warp's sequence for fencing a stale Oracle primary and for rejoining it, against a scripted fake. The fence is also checked against a real
 * Oracle in {@code OracleFenceLiveTest}; the rejoin is not: it needs a Data Guard configuration, which was not available.
 */
class OracleFenceRejoinTest {

    private final List<String> executed = new ArrayList<>();
    private List<String> sessions = List.of("12,345", "13,346", "evil'; drop user x; --");
    private String role = "PRIMARY";
    private String mrp = "0";
    private String becamePrimaryScn = "5000";
    private String ownScn = "5200";
    private boolean commandMakesStandby;
    private final BackendTarget node = new BackendTarget("old", "jdbc:oracle:thin:@//old:1521/svc", "app", "pw");
    private final BackendTarget current = new BackendTarget("new", "jdbc:oracle:thin:@//new:1521/svc", "app", "pw");

    @BeforeEach
    void install() {
        OracleHa.useSqlForTesting(new OracleHa.Sql() {
            @Override
            public String one(BackendTarget n, boolean sysdba, String q) {
                String l = q.toLowerCase();
                if (l.contains("standby_became_primary_scn")) {
                    return becamePrimaryScn;
                }
                if (l.contains("current_scn")) {
                    return ownScn;
                }
                if (l.contains("database_role")) {
                    return role;
                }
                if (l.contains("v$managed_standby")) {
                    return mrp;
                }
                return null;
            }

            @Override
            public void exec(BackendTarget n, boolean sysdba, String s) {
                executed.add((sysdba ? "[sysdba] " : "") + s);
            }

            @Override
            public List<String> list(BackendTarget n, boolean sysdba, String q) {
                return sessions;
            }
        });
        OracleHa.rejoinWaitMillis = 1500;
    }

    @AfterEach
    void restore() {
        OracleHa.useSqlForTesting(null);
        OracleHa.fenceAdminForTesting(null);
        OracleHa.rejoinWaitMillis = 180_000;
    }

    @Test
    void theFenceRestrictsSessionsBeforeItKillsAnyAndNeverPutsUnexpectedTextInAStatement() {
        // no login probe is possible against a fake, so the final check reports the backend user as not stopped
        SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> OracleHa.INSTANCE.fenceStaleWriter(node));
        assertTrue(e.getMessage().contains("RESTRICTED SESSION"), e.getMessage());
        assertEquals("[sysdba] ALTER SYSTEM ENABLE RESTRICTED SESSION", executed.get(0), "restricted first, so nobody new gets in while sessions are ended");
        assertTrue(executed.contains("[sysdba] ALTER SYSTEM KILL SESSION '12,345' IMMEDIATE"));
        assertTrue(executed.contains("[sysdba] ALTER SYSTEM KILL SESSION '13,346' IMMEDIATE"));
        assertTrue(executed.stream().noneMatch(s -> s.contains("drop user")), "a value that is not sid,serial# never reaches a statement");
        assertEquals("[sysdba] ALTER SYSTEM DISABLE RESTRICTED SESSION", executed.get(executed.size() - 1), "and the restriction is lifted again when it did not work");
    }

    @Test
    void aSeparateFenceAccountIsUsedWhenGiven() {
        List<String> users = new ArrayList<>();
        OracleHa.useSqlForTesting(new OracleHa.Sql() {
            @Override
            public String one(BackendTarget n, boolean sysdba, String q) {
                return null;
            }

            @Override
            public void exec(BackendTarget n, boolean sysdba, String s) {
                users.add(n.user());
            }

            @Override
            public List<String> list(BackendTarget n, boolean sysdba, String q) {
                return List.of();
            }
        });
        OracleHa.fenceAdminForTesting(new BackendTarget("old", node.jdbcUrl(), "sys", "adminpw"));
        org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> OracleHa.INSTANCE.fenceStaleWriter(node));
        assertTrue(users.stream().allMatch("sys"::equals), "fencing runs as the fence account, not the application user: " + users);
    }

    @Test
    void aStandbyThatIsApplyingRedoNeedsNothing() throws Exception {
        role = "PHYSICAL STANDBY";
        mrp = "1";
        assertEquals(EngineHa.RejoinOutcome.NOT_NEEDED, OracleHa.INSTANCE.rejoin(node, current).outcome());
        mrp = "0";
        var stopped = OracleHa.INSTANCE.rejoin(node, current);
        assertEquals(EngineHa.RejoinOutcome.NEEDS_REBUILD, stopped.outcome());
        assertTrue(stopped.detail().contains("RECOVER MANAGED STANDBY"), stopped.detail());
    }

    @Test
    void withoutARejoinCommandTheAnswerIsHowToReinstateItAndWhichScnToFlashBackTo() throws Exception {
        var res = OracleHa.INSTANCE.rejoin(node, current);
        assertEquals(EngineHa.RejoinOutcome.NEEDS_REBUILD, res.outcome());
        assertTrue(res.detail().contains("5000") && res.detail().contains("REINSTATE DATABASE"), res.detail());
    }
}
