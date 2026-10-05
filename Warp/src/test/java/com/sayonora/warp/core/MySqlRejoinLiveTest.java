package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.EngineHa.RejoinOutcome;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live MySQL rejoin check. WARP_TEST_REJOIN_MY_PORTS=p,r1: two GTID servers (root, empty password, table
 * {@code w.t(id int primary key, v varchar)}), r1 replicating from p and caught up.
 */
class MySqlRejoinLiveTest {

    private static final String OPTS = "?allowPublicKeyRetrieval=true&useSSL=false";

    private static String url(String port) {
        return "jdbc:mysql://127.0.0.1:" + port + "/w" + OPTS;
    }

    private static Connection conn(String port) throws Exception {
        return DriverManager.getConnection(url(port), "root", "");
    }

    private static void exec(String port, String... sql) throws Exception {
        try (Connection c = conn(port); var st = c.createStatement()) {
            for (String s : sql) {
                st.execute(s);
            }
        }
    }

    private static long scalar(String port, String sql) throws Exception {
        try (Connection c = conn(port); var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void aReturnedOldPrimaryRejoinsOnlyWhenItHoldsNothingTheNewPrimaryLacks() throws Exception {
        String ports = System.getenv("WARP_TEST_REJOIN_MY_PORTS");
        Assumptions.assumeTrue(ports != null);
        String[] p = ports.split(",");
        BackendTarget oldPrimary = new BackendTarget("old", url(p[0]), "root", "");
        BackendTarget newPrimary = new BackendTarget("new", url(p[1]), "root", "");
        EngineHa my = EngineHa.forDialect(SourceDialect.MYSQL);

        exec(p[0], "insert into w.t values (1, 'before')");
        long deadline = System.currentTimeMillis() + 20_000;
        while (scalar(p[1], "select count(*) from w.t") < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        // r1 is promoted by someone else; the old primary is still up and writable but holds nothing extra
        exec(p[1], "stop replica", "reset replica all", "set global super_read_only=off", "set global read_only=off");
        exec(p[1], "insert into w.t values (2, 'on-new-primary')");

        var res = my.rejoin(oldPrimary, newPrimary);
        assertEquals(RejoinOutcome.REJOINED, res.outcome(), res.detail());
        assertEquals(1, scalar(p[0], "select @@global.read_only"), "the rejoined node is read-only");
        deadline = System.currentTimeMillis() + 20_000;
        while (scalar(p[0], "select count(*) from w.t where id = 2") < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        assertEquals(1, scalar(p[0], "select count(*) from w.t where id = 2"), "it catches up from the new primary");
        assertEquals(RejoinOutcome.NOT_NEEDED, my.rejoin(oldPrimary, newPrimary).outcome(), "idempotent");

        // now the old primary is cut loose and takes a write the new primary never saw: errant transaction
        exec(p[0], "stop replica", "reset replica all", "set global super_read_only=off", "set global read_only=off");
        exec(p[0], "insert into w.t values (99, 'errant')");
        var refused = my.rejoin(oldPrimary, newPrimary);
        assertEquals(RejoinOutcome.NEEDS_REBUILD, refused.outcome(), refused.detail());
        assertEquals(0, scalar(p[0], "select @@global.read_only"), "a refused node is left exactly as it was");
        assertEquals(1, scalar(p[0], "select count(*) from w.t where id = 99"), "and its data is untouched");
        assertTrue(refused.detail().contains("transactions"));
    }
}
