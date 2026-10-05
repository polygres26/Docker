package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Live check against a REAL Postgres primary and a REAL streaming replica -- opt-in, skipped unless
 * {@code WARP_TEST_REPLICA_PRIMARY_PORT}/{@code WARP_TEST_REPLICA_REPLICA_PORT} are set (trust-auth
 * user {@code warp}, database {@code postgres}, table {@code t(id int primary key, v text)} present).
 * Which server answered a read is observed with {@code inet_server_port()}, which the classifier
 * treats as a plain read.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicaReadRoutingLiveTest {

    private static int primaryPort;
    private static int replicaPort;
    private static BackendRegistry registry;
    private static String primaryUrl;
    private static String replicaUrl;

    @BeforeAll
    static void setUp() {
        String p = System.getenv("WARP_TEST_REPLICA_PRIMARY_PORT");
        String r = System.getenv("WARP_TEST_REPLICA_REPLICA_PORT");
        Assumptions.assumeTrue(p != null && r != null, "live replica test needs WARP_TEST_REPLICA_*_PORT");
        primaryPort = Integer.parseInt(p);
        replicaPort = Integer.parseInt(r);
        primaryUrl = "jdbc:postgresql://127.0.0.1:" + primaryPort + "/postgres";
        replicaUrl = "jdbc:postgresql://127.0.0.1:" + replicaPort + "/postgres";
        registry = BackendRegistry.fromConfig("pg=" + primaryUrl + "|warp|||" + replicaUrl + "~2", null);
    }

    private RoutingBackendExecutor executor(Connection primaryConn) {
        return new RoutingBackendExecutor(registry, new JdbcBackendExecutor(primaryConn))
                .withDefaultExecutorBackendName("pg")
                .withSessionStatePredicate(() -> false);
    }

    private static Connection primaryConn() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + primaryPort + "/postgres", "warp", "");
        c.setAutoCommit(true);
        return c;
    }

    private static int serverPort(RoutingBackendExecutor ex, String sql) throws Exception {
        ExecutionResult r = ex.execute(Statement.of(SourceDialect.POSTGRES, sql, List.of()));
        return ((Number) r.rows().get(0).get(0)).intValue();
    }

    private static void sleep(long ms) throws Exception {
        Thread.sleep(ms);
    }

    private static void direct(int port, String sql) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/postgres", "warp", "");
                java.sql.Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    @Test
    @Order(1)
    void idleCaughtUpReplicaReportsNearZeroLagNotTheIdleTimestampGap() throws Exception {
        sleep(3000); // idle primary: the old now()-replay_timestamp probe would report ~3s+ here
        ReplicationLag.Result r = ReplicationLag.check(new BackendTarget("r", replicaUrl, "warp", ""));
        assertTrue(r.ok() && r.isReplica(), String.valueOf(r.message()));
        assertTrue(r.lagSeconds() < 1.0, "idle caught-up replica must report ~0 lag, got " + r.lagSeconds());
    }

    @Test
    @Order(2)
    void aPlainReadGoesToTheReplicaAndLockingOrWritingReadsStayOnThePrimary() throws Exception {
        registry.replicaRouter().probeAll();
        sleep(2200); // clear any read-your-writes window left by earlier statements
        try (Connection c = primaryConn()) {
            RoutingBackendExecutor ex = executor(c);
            assertEquals(replicaPort, serverPort(ex, "select inet_server_port()"));
            assertEquals(primaryPort, serverPort(ex, "select inet_server_port() from t for update limit 1"));
            assertEquals(primaryPort, serverPort(ex, "select nextval('pg_catalog.pg_class_oid_index'::regclass)::int * 0 + inet_server_port()")
                    , "a side-effect function must keep the read on the primary");
        } catch (org.postgresql.util.PSQLException expectedForBogusNextval) {
            // nextval on a non-sequence errors on the primary -- reaching the primary is the point.
        }
    }

    @Test
    @Order(3)
    void readAfterWriteStaysOnThePrimaryUntilTheWindowPasses() throws Exception {
        sleep(2200);
        try (Connection c = primaryConn()) {
            RoutingBackendExecutor ex = executor(c);
            assertEquals(replicaPort, serverPort(ex, "select inet_server_port()"));
            ex.execute(Statement.of(SourceDialect.POSTGRES, "insert into t values (" + (int) (Math.random() * 1_000_000 + 10) + ", 'ryw')", List.of()));
            assertEquals(primaryPort, serverPort(ex, "select inet_server_port()"), "just wrote -> read-your-writes on primary");
            sleep(2200);
            assertEquals(replicaPort, serverPort(ex, "select inet_server_port()"), "window passed -> replica again");
        }
    }

    @Test
    @Order(4)
    void aSessionWithStateNeverUsesTheReplicaOnTheSuppliedConnection() throws Exception {
        sleep(2200);
        try (Connection c = primaryConn()) {
            RoutingBackendExecutor ex = new RoutingBackendExecutor(registry, new JdbcBackendExecutor(c))
                    .withDefaultExecutorBackendName("pg").withSessionStatePredicate(() -> true);
            assertEquals(primaryPort, serverPort(ex, "select inet_server_port()"));
        }
    }

    @Test
    @Order(5)
    void aLaggingReplicaIsSkippedAndComesBackWhenItCatchesUp() throws Exception {
        try (Connection c = primaryConn()) {
            RoutingBackendExecutor ex = executor(c);
            direct(replicaPort, "select pg_wal_replay_pause()");
            try {
                direct(primaryPort, "insert into t values (" + (int) (Math.random() * 1_000_000 + 2_000_000) + ", 'lag')");
                sleep(4000);
                registry.replicaRouter().probeAll();
                var sample = registry.replicaRouter().samplesSnapshot().get(replicaUrl);
                assertTrue(sample.lagSeconds() > 2.0, "paused replay must show real lag, got " + sample.lagSeconds());
                sleep(2200);
                assertEquals(primaryPort, serverPort(ex, "select inet_server_port()"),
                        "replica beyond maxLag (2s) must not serve reads");
            } finally {
                direct(replicaPort, "select pg_wal_replay_resume()");
            }
            long deadline = System.currentTimeMillis() + 15_000;
            boolean back = false;
            while (System.currentTimeMillis() < deadline) {
                registry.replicaRouter().probeAll();
                if (registry.replicaRouter().eligible(registry.replicaRouter().replicasOf("pg").get(0))) {
                    back = true;
                    break;
                }
                sleep(500);
            }
            assertTrue(back, "replica should become eligible again after replay resumes");
            assertEquals(replicaPort, serverPort(ex, "select inet_server_port()"));
        }
    }

    @Test
    @Order(6)
    void aDeadReplicaFallsBackToThePrimaryTransparentlyAndIsQuarantined() throws Exception {
        String bin = System.getenv("WARP_TEST_REPLICA_PG_BIN");
        String replicaDir = System.getenv("WARP_TEST_REPLICA_DATA_DIR");
        Assumptions.assumeTrue(bin != null && replicaDir != null, "needs WARP_TEST_REPLICA_PG_BIN/DATA_DIR to kill the replica");
        sleep(2200);
        registry.replicaRouter().probeAll();
        try (Connection c = primaryConn()) {
            RoutingBackendExecutor ex = executor(c);
            assertEquals(replicaPort, serverPort(ex, "select inet_server_port()"));
            new ProcessBuilder(bin + "/pg_ctl", "-D", replicaDir, "-m", "immediate", "-w", "stop").inheritIO().start().waitFor();
            // The replica's last lag sample is still "fresh and fine"; the read must not fail.
            assertEquals(primaryPort, serverPort(ex, "select inet_server_port()"),
                    "dead replica must be retried on the primary without surfacing an error");
            var replica = registry.replicaRouter().replicasOf("pg").get(0);
            assertTrue(registry.replicaRouter().isQuarantined(replica));
            assertTrue(registry.replicaRouter().reasonCounts("pg")
                    .getOrDefault(ReplicaRouter.Reason.REPLICA_RETRIED_ON_PRIMARY, 0L) >= 1);
        }
    }
}
