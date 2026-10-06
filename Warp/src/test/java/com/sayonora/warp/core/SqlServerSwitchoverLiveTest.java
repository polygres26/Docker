package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Planned switchover, abort and crash-failover rejoin on a real two-node read-scale availability group, through running Warp processes.
 * Opt-in: WARP_TEST_MSSQL_AG=1 and WARP_TEST_BROWNOUT_PG_BIN (the control-plane Postgres) after {@code Warp/tests/sqlserver-ag/ag.sh up}
 * (sql1 on 14331, sql2 on 14332, database {@code w}, table {@code dbo.t}); {@code docker} must reach the containers. Either node may be the
 * primary at the start.
 */
class SqlServerSwitchoverLiveTest {

    private static final String PW = "Warp_Test_1234!";
    private static final String OPTS = ";databaseName=w;encrypt=true;trustServerCertificate=true";
    private static final int[] PORTS = {14331, 14332};

    private static String url(int port) {
        return "jdbc:sqlserver://127.0.0.1:" + port + OPTS;
    }

    private static String masterUrl(int port) {
        return "jdbc:sqlserver://127.0.0.1:" + port + ";databaseName=master;encrypt=true;trustServerCertificate=true";
    }

    private static Connection conn(String url) throws Exception {
        return DriverManager.getConnection(url, "sa", PW);
    }

    private static boolean writable(int port) {
        return EngineHa.forDialect(SourceDialect.SQL_SERVER).role(new BackendTarget("n", url(port), "sa", PW)) == FailoverMonitor.NodeRole.WRITABLE;
    }

    private static int primaryPort() {
        for (int p : PORTS) {
            if (writable(p)) {
                return p;
            }
        }
        throw new AssertionError("no writable node");
    }

    private static int other(int port) {
        return port == PORTS[0] ? PORTS[1] : PORTS[0];
    }

    private static String query(int port, String sql) throws Exception {
        try (Connection c = conn(masterUrl(port)); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static void waitFor(String what, long seconds, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + seconds * 1000;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (cond.call()) {
                    return;
                }
            } catch (Exception ignored) {
                // not yet
            }
            Thread.sleep(500);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static void docker(String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "docker";
        System.arraycopy(args, 0, cmd, 1, args.length);
        assertEquals(0, new ProcessBuilder(cmd).inheritIO().start().waitFor());
    }

    /** The replica of port {@code p} is a synchronizing secondary of the group, and the group is back to its normal modes. */
    private static void assertHealthyAfter(int newPrimary, int oldPrimary) throws Exception {
        waitFor("the old primary to synchronize as a secondary", 90, () -> {
            String role = query(oldPrimary, "select cast(role_desc as varchar(20)) from sys.dm_hadr_availability_replica_states where is_local = 1");
            String state = query(oldPrimary, "select cast(synchronization_state_desc as varchar(30)) + cast(is_suspended as varchar) "
                    + "from sys.dm_hadr_database_replica_states where is_local = 1 and database_id = db_id('w')");
            return "SECONDARY".equals(role) && state != null && state.startsWith("SYNCHRON") && state.endsWith("0");
        });
        assertEquals("PRIMARY", query(newPrimary, "select cast(role_desc as varchar(20)) from sys.dm_hadr_availability_replica_states where is_local = 1"));
        assertEquals("0", query(newPrimary, "select cast(required_synchronized_secondaries_to_commit as varchar) from sys.availability_groups"));
        assertEquals("0", query(newPrimary, "select cast(count(*) as varchar) from sys.availability_replicas where availability_mode_desc <> 'ASYNCHRONOUS_COMMIT'"),
                "both replicas are back in asynchronous commit");
    }

    private static WarpProcess warp(LocalPostgres cfg, int primary, String... extraEnv) throws Exception {
        String spec = "mssql=" + url(primary).replace(";", "%3B") + "|sa|" + PW + "||" + url(other(primary)).replace(";", "%3B") + "~30|promote";
        WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                .env("WARP_BACKENDS", spec)
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled");
        for (int i = 0; i + 1 < extraEnv.length; i += 2) {
            b.env(extraEnv[i], extraEnv[i + 1]);
        }
        return b.start();
    }

    private static JsonObject switchover(WarpProcess w, int target) throws Exception {
        return JsonParser.parseString(BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort() + "/api/failover/mssql/switchover",
                "{\"target\":\"" + url(target) + "\"}")).getAsJsonObject();
    }

    @Test
    void plannedSwitchoverThereAndBackLosesNothingAndAnAbortChangesNothing() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("WARP_TEST_MSSQL_AG")), "set WARP_TEST_MSSQL_AG=1 after ag.sh up");
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        LocalPostgres cfg = LocalPostgres.primary(bin, Files.createTempDirectory("mssqlsw"), "cfg", LocalPostgres.freePort());
        try {
            int first = primaryPort();
            int second = other(first);
            try (WarpProcess w = warp(cfg, first, "WARP_SWITCHOVER_SYNC_SECONDS", "6")) {
                Thread.sleep(4000);
                Set<Long> acked = java.util.concurrent.ConcurrentHashMap.newKeySet();
                AtomicBoolean stop = new AtomicBoolean();
                long[] failures = {0};
                Thread writer = new Thread(() -> {
                    long id = 7_000_000L + (System.nanoTime() % 1_000_000L) * 10;
                    while (!stop.get()) {
                        id++;
                        boolean ok = false;
                        for (int port : PORTS) {
                            try (Connection c = conn(url(port)); Statement st = c.createStatement()) {
                                st.setQueryTimeout(5);
                                st.executeUpdate("INSERT INTO dbo.t VALUES (" + id + ", 'switchover-load')");
                                acked.add(id);
                                ok = true;
                                break;
                            } catch (Exception e) {
                                // that node is not the primary right now (or is moving): try the other
                            }
                        }
                        if (!ok) {
                            failures[0]++;
                        }
                        try {
                            Thread.sleep(40);
                        } catch (InterruptedException ie) {
                            return;
                        }
                    }
                }, "mssql-load");
                writer.start();
                Thread.sleep(3000);

                // 1. there
                long t0 = System.nanoTime();
                JsonObject there = switchover(w, second);
                long thereMs = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(there.get("ok").getAsBoolean(), there.toString());
                waitFor("the target to be writable", 60, () -> writable(second));
                assertHealthyAfter(second, first);
                Thread.sleep(3000);

                // 2. back
                t0 = System.nanoTime();
                JsonObject back = switchover(w, first);
                long backMs = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(back.get("ok").getAsBoolean(), back.toString());
                waitFor("the original primary to be writable again", 60, () -> writable(first));
                assertHealthyAfter(first, second);
                Thread.sleep(3000);

                // 3. an aborted switchover: the target's data movement is suspended, so it can never reach SYNCHRONIZED
                try (Connection c = conn(masterUrl(second)); Statement st = c.createStatement()) {
                    st.execute("ALTER DATABASE [w] SET HADR SUSPEND");
                }
                JsonObject aborted = switchover(w, second);
                assertFalse(aborted.get("ok").getAsBoolean(), aborted.toString());
                assertTrue(aborted.get("message").getAsString().contains("could not prepare"), aborted.toString());
                assertTrue(writable(first), "the primary is untouched by an aborted switchover");
                assertEquals("0", query(first, "select cast(required_synchronized_secondaries_to_commit as varchar) from sys.availability_groups"));
                assertEquals("0", query(first, "select cast(count(*) as varchar) from sys.availability_replicas where availability_mode_desc <> 'ASYNCHRONOUS_COMMIT'"));
                try (Connection c = conn(masterUrl(second)); Statement st = c.createStatement()) {
                    st.execute("ALTER DATABASE [w] SET HADR RESUME");
                }
                waitFor("the target to resume", 60, () -> "0".equals(query(second, "select cast(is_suspended as varchar) from sys.dm_hadr_database_replica_states "
                        + "where is_local = 1 and database_id = db_id('w')")));

                Thread.sleep(3000);
                stop.set(true);
                writer.join(20_000);

                // nothing acknowledged is missing on the current primary, and the secondary catches up to the same rows
                Set<Long> onPrimary = new HashSet<>();
                try (Connection c = conn(url(first)); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select id from dbo.t where v = 'switchover-load'")) {
                    while (rs.next()) {
                        onPrimary.add(rs.getLong(1));
                    }
                }
                Set<Long> lost = new HashSet<>(acked);
                lost.removeAll(onPrimary);
                System.out.println("MSSQL-SWITCHOVER-RESULT acked " + acked.size() + " | on primary " + onPrimary.size() + " | lost " + lost.size()
                        + " | writes that failed on both nodes " + failures[0] + " | switchover there " + thereMs + " ms, back " + backMs + " ms");
                assertEquals(Set.of(), lost);
                waitFor("the secondary to hold the same rows", 60, () -> {
                    try (Connection c = conn(url(second).replace("databaseName=w", "databaseName=w;applicationIntent=ReadOnly"));
                            Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from dbo.t where v = 'switchover-load'")) {
                        rs.next();
                        return rs.getLong(1) == onPrimary.size();
                    }
                });
            }
        } finally {
            cfg.stop("immediate");
        }
    }

    @Test
    void aCrashedPrimaryIsPromotedAndTheReturnedOldPrimaryIsRejoinedWithTheOperatorsRestartCommand() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("WARP_TEST_MSSQL_AG")), "set WARP_TEST_MSSQL_AG=1 after ag.sh up");
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        LocalPostgres cfg = LocalPostgres.primary(bin, Files.createTempDirectory("mssqlrj"), "cfg", LocalPostgres.freePort());
        try {
            int oldPrimary = primaryPort();
            int survivor = other(oldPrimary);
            String oldContainer = oldPrimary == PORTS[0] ? "sql1" : "sql2";
            int base = 1_000_000_000 + (int) (System.nanoTime() % 900_000_000L); // the id column is an int, and earlier runs left rows behind
            try (WarpProcess w = warp(cfg, oldPrimary, "WARP_FAILOVER_REJOIN_COMMAND", "docker restart " + oldContainer)) {
                Thread.sleep(5000);
                try (Connection c = conn(url(oldPrimary)); Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO dbo.t VALUES (" + (base + 1) + ", 'before-crash')");
                }
                Thread.sleep(3000);
                docker("kill", oldContainer);
                waitFor("Warp to promote the survivor", 120, () -> writable(survivor));
                try (Connection c = conn(url(survivor)); Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO dbo.t VALUES (" + (base + 2) + ", 'after-failover')");
                }
                docker("start", oldContainer);
                // the old primary comes back as a second primary: Warp takes it offline, runs the operator's restart command, waits for it to return as
                // a secondary and resumes it
                waitFor("the returned old primary to rejoin as a synchronizing secondary", 180, () -> {
                    String role = query(oldPrimary, "select cast(role_desc as varchar(20)) from sys.dm_hadr_availability_replica_states where is_local = 1");
                    String state = query(oldPrimary, "select cast(synchronization_state_desc as varchar(30)) + cast(is_suspended as varchar) "
                            + "from sys.dm_hadr_database_replica_states where is_local = 1 and database_id = db_id('w')");
                    return "SECONDARY".equals(role) && state != null && state.startsWith("SYNCHRON") && state.endsWith("0");
                });
                waitFor("the rejoined node to hold the new primary's rows", 60, () -> {
                    try (Connection c = conn(url(oldPrimary).replace("databaseName=w", "databaseName=w;applicationIntent=ReadOnly"));
                            Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from dbo.t where id = " + (base + 2))) {
                        rs.next();
                        return rs.getLong(1) == 1;
                    }
                });
                assertTrue(writable(survivor));
                assertFalse(writable(oldPrimary));
                System.out.println("MSSQL-REJOIN-RESULT ok | " + oldContainer + " killed, " + (oldPrimary == PORTS[0] ? "sql2" : "sql1")
                        + " promoted by Warp, old primary restarted and rejoined as a synchronizing secondary");
            }
        } finally {
            cfg.stop("immediate");
        }
    }
}
