package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Live planned-switchover checks on real servers, no config database needed. Postgres: primary + two
 * streaming replicas (WARP_TEST_SWITCH_PG_PORTS=p,r1,r2; superuser "warp", trust auth, table {@code t(id int, v text)}).
 * MySQL: primary + two GTID replicas (WARP_TEST_SWITCH_MY_PORTS=p,r1,r2; root, empty password, table {@code w.t}).
 */
class SwitchoverLiveTest {

    private static Connection pg(String port) throws Exception {
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/postgres", "warp", "");
    }

    private static Connection my(String port) throws Exception {
        return DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + port + "/w?allowPublicKeyRetrieval=true&useSSL=false",
                "root", "");
    }

    private static long scalar(Connection c, String sql) throws Exception {
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void waitFor(String what, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.call()) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    @Test
    void postgresSwitchoverLosesNothingAndRepointsTheOtherReplica() throws Exception {
        String ports = System.getenv("WARP_TEST_SWITCH_PG_PORTS");
        Assumptions.assumeTrue(ports != null);
        String[] p = ports.split(",");
        String pUrl = "jdbc:postgresql://127.0.0.1:" + p[0] + "/postgres";
        String r1Url = "jdbc:postgresql://127.0.0.1:" + p[1] + "/postgres";
        String r2Url = "jdbc:postgresql://127.0.0.1:" + p[2] + "/postgres";
        String spec = "pg=" + pUrl + "|warp|||" + r1Url + "^" + r2Url + "|follow";
        BackendRegistry reg = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(reg, FailoverMonitor::probeRole,
                (b, o, n, r) -> reg.applyFailoverLocally(b, o, n, r), null, System::currentTimeMillis, 3, 60, 5);

        try (Connection c = pg(p[0]); var st = c.createStatement()) {
            for (int i = 0; i < 200; i++) {
                st.execute("insert into t values (" + (1000 + i) + ", 'before-switch')");
            }
        }
        var res = monitor.switchover("pg", r1Url);
        assertTrue(res.ok(), res.message());
        assertEquals(r1Url, reg.get("pg").jdbcUrl());

        try (Connection c = pg(p[1])) {
            assertEquals(200, scalar(c, "select count(*) from t where v = 'before-switch'"), "no committed row lost");
            assertEquals(0, scalar(c, "select pg_is_in_recovery()::int"), "new primary is writable");
            c.createStatement().execute("insert into t values (9999, 'after-switch')");
        }
        // the old primary refuses writes (frozen, not a replica)
        try (Connection c = pg(p[0]); var st = c.createStatement()) {
            boolean refused = false;
            try {
                st.execute("insert into t values (8888, 'must-fail')");
            } catch (java.sql.SQLException e) {
                refused = true;
            }
            assertTrue(refused, "the frozen old primary must not take writes");
        }
        // the other replica now follows the new primary
        waitFor("r2 to replicate from the new primary", () -> {
            try (Connection c = pg(p[2])) {
                return scalar(c, "select count(*) from t where id = 9999") == 1;
            }
        });
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("switchover")));
    }

    @Test
    void mysqlSwitchoverLosesNothingAndTheOldPrimaryBecomesAReplica() throws Exception {
        String ports = System.getenv("WARP_TEST_SWITCH_MY_PORTS");
        Assumptions.assumeTrue(ports != null);
        String[] p = ports.split(",");
        String o = "?allowPublicKeyRetrieval=true&useSSL=false";
        String pUrl = "jdbc:mysql://127.0.0.1:" + p[0] + "/w" + o;
        String r1Url = "jdbc:mysql://127.0.0.1:" + p[1] + "/w" + o;
        String r2Url = "jdbc:mysql://127.0.0.1:" + p[2] + "/w" + o;
        String spec = "my=" + pUrl + "|root|||" + r1Url + "^" + r2Url + "|follow";
        BackendRegistry reg = BackendRegistry.fromConfig(spec, null);
        FailoverMonitor monitor = new FailoverMonitor(reg, FailoverMonitor::probeRole,
                (b, old, n, r) -> reg.applyFailoverLocally(b, old, n, r), null, System::currentTimeMillis, 3, 60, 5);

        try (Connection c = my(p[0]); var st = c.createStatement()) {
            for (int i = 0; i < 200; i++) {
                st.execute("insert into w.t values (" + (1000 + i) + ", 'before-switch')");
            }
        }
        var res = monitor.switchover("my", r1Url);
        assertTrue(res.ok(), res.message());
        assertEquals(r1Url, reg.get("my").jdbcUrl());

        try (Connection c = my(p[1])) {
            assertEquals(200, scalar(c, "select count(*) from w.t where v = 'before-switch'"), "no committed row lost");
            assertEquals(0, scalar(c, "select @@global.read_only"));
            c.createStatement().execute("insert into w.t values (9999, 'after-switch')");
        }
        for (String port : new String[] {p[0], p[2]}) {
            waitFor(port + " to replicate from the new primary", () -> {
                try (Connection c = my(port)) {
                    return scalar(c, "select count(*) from w.t where id = 9999") == 1;
                }
            });
            try (Connection c = my(port)) {
                assertEquals(1, scalar(c, "select @@global.read_only"), "still read-only");
            }
        }
        assertTrue(res.message().contains("replicates from the new one"), res.message());
    }
}
