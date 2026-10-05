package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.OptionalDouble;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * The two split-brain guards against a real SQL Server Availability Group. Opt-in: WARP_TEST_MSSQL_AG=1 after
 * {@code Warp/tests/sqlserver-ag/ag.sh up} (sql1 primary on 14331, sql2 secondary on 14332; needs {@code docker} on the PATH pointing at
 * the engine that runs them). Destructive: it kills sql1, forces a failover to sql2 and restarts sql1 as a stale second primary, so run
 * {@code ag.sh down} afterwards.
 */
class SqlServerSplitBrainGuardLiveTest {

    private static final String PW = "Warp_Test_1234!";
    private static final String OPTS = ";databaseName=w;encrypt=true;trustServerCertificate=true";
    private static final String P_URL = "jdbc:sqlserver://127.0.0.1:14331" + OPTS;
    private static final String S_URL = "jdbc:sqlserver://127.0.0.1:14332" + OPTS;
    private static final String S_MASTER = "jdbc:sqlserver://127.0.0.1:14332;databaseName=master;encrypt=true;trustServerCertificate=true";

    private static Connection conn(String url) throws Exception {
        return DriverManager.getConnection(url, "sa", PW);
    }

    private static void docker(String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "docker";
        System.arraycopy(args, 0, cmd, 1, args.length);
        Process p = new ProcessBuilder(cmd).inheritIO().start();
        assertEquals(0, p.waitFor());
    }

    private static void waitUntil(String what, long seconds, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + seconds * 1000;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (cond.call()) {
                    return;
                }
            } catch (Exception ignored) {
                // not yet
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static void insert(String url, int id) throws Exception {
        try (Connection c = conn(url); var st = c.createStatement()) {
            st.execute("INSERT INTO dbo.t VALUES (" + id + ", 'split-brain test')");
        }
    }

    @Test
    void aSecondaryReportsItsPrimaryAndAStaleReturnedPrimaryIsTakenOutOfService() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("WARP_TEST_MSSQL_AG")), "set WARP_TEST_MSSQL_AG=1 after ag.sh up");
        EngineHa ha = EngineHa.forDialect(SourceDialect.SQL_SERVER);
        BackendTarget primary = new BackendTarget("p", P_URL, "sa", PW);
        BackendTarget secondary = new BackendTarget("s", S_URL, "sa", PW);

        // A: the secondary sees its primary; once the primary is killed it stops seeing it
        waitUntil("the secondary to be connected to the primary", 90, () -> ha.heardFromPrimarySecondsAgo(secondary, primary).isPresent());
        assertEquals(OptionalDouble.of(0), ha.heardFromPrimarySecondsAgo(secondary, primary));
        assertTrue(ha.heardFromPrimarySecondsAgo(primary, secondary).isEmpty(), "a primary is not connected to a primary");
        docker("kill", "sql1");
        waitUntil("the secondary to notice the primary is gone", 90, () -> ha.heardFromPrimarySecondsAgo(secondary, primary).isEmpty());

        // B: fail over to sql2, bring the old primary back: it is a second primary that takes writes
        try (Connection c = conn(S_MASTER); var st = c.createStatement()) {
            st.execute("ALTER AVAILABILITY GROUP [ag1] FORCE_FAILOVER_ALLOW_DATA_LOSS");
        }
        waitUntil("sql2 to accept writes", 60, () -> ha.role(secondary) == FailoverMonitor.NodeRole.WRITABLE);
        docker("start", "sql1");
        waitUntil("the old primary to come back writable", 150, () -> ha.role(primary) == FailoverMonitor.NodeRole.WRITABLE);
        insert(P_URL, 9001); // split brain: both take writes

        ha.fenceStaleWriter(primary);

        Exception refused = assertThrows(Exception.class, () -> insert(P_URL, 9002));
        assertTrue(refused.getMessage().contains("983") || refused.getMessage().toLowerCase().contains("not in the primary or secondary role"),
                refused.getMessage());
        assertNotEquals(FailoverMonitor.NodeRole.WRITABLE, ha.role(primary), "Warp no longer sees it as a writer");
        insert(S_URL, 9003); // the new primary is unaffected
        // fencing the new primary is refused on the grounds that it is not a stale writer? it IS the PRIMARY of a NONE group, so only
        // the caller's decision (a writable replica while the configured primary is writable) protects it -- not tested here.
    }
}
