package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * The Oracle stale-writer fence against a real Oracle Free 23ai (no Data Guard involved): an application account has an open session; after the
 * fence its session is gone, a new login is refused with ORA-01035 (which is how the node is recognised as frozen), writes cannot happen, and
 * releasing the fence lets it log in again. Opt-in: WARP_TEST_SHARD_ENGINE=oracle with the container of the setup notes (port 15211, user appusr / apppw
 * in FREEPDB1, SYS password Warp_Test_1234).
 */
class OracleFenceLiveTest {

    private static final String URL = "jdbc:oracle:thin:@//127.0.0.1:15211/FREEPDB1";
    private final BackendTarget app = new BackendTarget("o", URL, "appusr", "apppw");
    private final BackendTarget admin = new BackendTarget("o", URL, "sys", "Warp_Test_1234");

    private static Connection sysdba() throws SQLException {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("user", "sys");
        p.setProperty("password", "Warp_Test_1234");
        p.setProperty("internal_logon", "sysdba");
        return DriverManager.getConnection(URL, p);
    }

    @AfterEach
    void restore() throws Exception {
        if ("oracle".equals(System.getenv("WARP_TEST_SHARD_ENGINE"))) {
            OracleHa.INSTANCE.releaseFence(app);
        }
        OracleHa.fenceAdminForTesting(null);
    }

    @Test
    void aFencedNodeKicksOutOpenSessionsRefusesNewOnesAndComesBackOnRelease() throws Exception {
        Assumptions.assumeTrue("oracle".equals(System.getenv("WARP_TEST_SHARD_ENGINE")), "set WARP_TEST_SHARD_ENGINE=oracle with the Oracle container up");
        OracleHa.fenceAdminForTesting(admin);
        EngineHa ha = EngineHa.forDialect(SourceDialect.ORACLE);
        try (Connection c = DriverManager.getConnection(URL, "appusr", "apppw"); Statement st = c.createStatement()) {
            try {
                st.execute("drop table fence_t");
            } catch (SQLException absent) {
                // first run
            }
            st.execute("create table fence_t (id int primary key)");
            st.execute("insert into fence_t values (1)");
            c.commit();
            assertFalse(ha.writesFrozen(app), "a normal node is not frozen");

            ha.fenceStaleWriter(app);

            assertTrue(ha.writesFrozen(app), "a new login is refused with ORA-01035");
            SQLException refused = assertThrows(SQLException.class, () -> DriverManager.getConnection(URL, "appusr", "apppw").close());
            assertEquals(1035, refused.getErrorCode(), refused.getMessage());
            SQLException killed = assertThrows(SQLException.class, () -> {
                st.execute("insert into fence_t values (2)");
                c.commit();
            }, "the session that was open when the node was fenced no longer works");
            System.out.println("ORACLE-FENCE-NOTE open session after the fence: " + killed.getMessage().split("\n")[0]);
        }
        OracleHa.INSTANCE.releaseFence(app);
        assertFalse(ha.writesFrozen(app));
        try (Connection c = DriverManager.getConnection(URL, "appusr", "apppw"); Statement st = c.createStatement()) {
            st.execute("insert into fence_t values (3)");
            c.commit();
            var rs = st.executeQuery("select count(*) from fence_t");
            rs.next();
            assertEquals(2, rs.getInt(1), "only the writes before the fence and after the release exist");
            st.execute("drop table fence_t");
        }
    }

    @Test
    void aBackendUserThatHoldsRestrictedSessionIsReportedAsNotStopped() throws Exception {
        Assumptions.assumeTrue("oracle".equals(System.getenv("WARP_TEST_SHARD_ENGINE")), "set WARP_TEST_SHARD_ENGINE=oracle with the Oracle container up");
        OracleHa.fenceAdminForTesting(admin);
        try (Connection c = sysdba(); Statement st = c.createStatement()) {
            try {
                st.execute("drop user dbauser cascade");
            } catch (SQLException absent) {
                // first run
            }
            st.execute("create user dbauser identified by dbapw quota unlimited on users");
            st.execute("grant create session, restricted session to dbauser"); // like any DBA
        }
        BackendTarget dba = new BackendTarget("o", URL, "dbauser", "dbapw");
        SQLException e = assertThrows(SQLException.class, () -> EngineHa.forDialect(SourceDialect.ORACLE).fenceStaleWriter(dba));
        assertTrue(e.getMessage().contains("RESTRICTED SESSION"), e.getMessage());
        assertFalse(EngineHa.forDialect(SourceDialect.ORACLE).writesFrozen(dba), "and the restriction was lifted again");
        try (Connection c = sysdba(); Statement st = c.createStatement()) {
            st.execute("drop user dbauser cascade");
        }
    }
}
