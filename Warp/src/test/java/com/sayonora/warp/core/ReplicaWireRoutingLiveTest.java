package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * End to end through a real pgwire client and a real Warp process: replica reads happen for a client that
 * selects a backend by database name. WARP_TEST_WIRE_PG_PORTS=primary,replica: a Postgres primary and its
 * streaming replica (superuser "warp", trust auth, table {@code t(id int, v text)}).
 */
class ReplicaWireRoutingLiveTest {

    /** Warp keeps its configuration in warp_config on the primary and only seeds it from WARP_BACKENDS /
     * WARP_REPLICAS when it does not exist yet, so a config left by an earlier Warp would silently win. */
    private static void resetWarpConfig(String primaryPort) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + primaryPort + "/postgres",
                "warp", "secret"); var st = c.createStatement()) {
            st.execute("drop table if exists warp_config cascade");
        }
    }

    private static int freePort() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static int serverPort(Connection c) throws Exception {
        try (var st = c.createStatement(); var rs = st.executeQuery("select inet_server_port()")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void readsFromAPgwireClientGoToTheReplicaAndWritesAndFreshWritesStayOnThePrimary() throws Exception {
        String ports = System.getenv("WARP_TEST_WIRE_PG_PORTS");
        Assumptions.assumeTrue(ports != null);
        String[] p = ports.split(",");
        int primaryPort = Integer.parseInt(p[0]);
        int replicaPort = Integer.parseInt(p[1]);
        resetWarpConfig(p[0]);
        String spec = "pg=jdbc:postgresql://127.0.0.1:" + p[0] + "/postgres|warp|secret||jdbc:postgresql://127.0.0.1:"
                + p[1] + "/postgres~10|follow";
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", primaryPort, "postgres", "warp", "secret")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", spec)
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_READ_AFTER_WRITE_WINDOW_MS", "1500")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            // database name "pg" selects the backend of that name (and so its replicas)
            String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/pg";
            try (Connection c = DriverManager.getConnection(url, "warp", "secret")) {
                c.setAutoCommit(true);
                Thread.sleep(3500); // let the lag sampler measure the replica
                assertEquals(replicaPort, serverPort(c), "a plain read goes to the replica");

                c.createStatement().execute("insert into t values (777001, 'via-warp')");
                assertEquals(primaryPort, serverPort(c), "right after a write the session reads from the primary");
                try (var st = c.createStatement(); var rs = st.executeQuery("select count(*) from t where id = 777001")) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), "read-your-writes");
                }
                Thread.sleep(2200); // past the read-after-write window; the replica has caught up
                assertEquals(replicaPort, serverPort(c), "reads return to the replica after the window");

                c.setAutoCommit(false);
                assertEquals(primaryPort, serverPort(c), "inside a transaction nothing goes to the replica");
                c.rollback();
                c.setAutoCommit(true);
                try (var st = c.createStatement(); var rs = st.executeQuery("select id from t where id = 777001 for update")) {
                    assertTrue(rs.next());
                }
            }
            // the admin API agrees about what was routed
            var http = java.net.http.HttpClient.newHttpClient();
            var body = http.send(java.net.http.HttpRequest.newBuilder(
                    java.net.URI.create("http://localhost:" + warp.metricsPort() + "/metrics")).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString()).body();
            assertTrue(body.contains("warp_replica_reads_routed_total{backend=\"pg\""), body);
        }
    }

    /** The implicit WARP_* backend (no WARP_BACKENDS at all) with WARP_REPLICAS. */
    @Test
    void theImplicitDefaultBackendGetsReplicaReadsWhenWarpReplicasIsSet() throws Exception {
        String ports = System.getenv("WARP_TEST_WIRE_PG_PORTS");
        Assumptions.assumeTrue(ports != null);
        String[] p = ports.split(",");
        int primaryPort = Integer.parseInt(p[0]);
        int replicaPort = Integer.parseInt(p[1]);
        resetWarpConfig(p[0]);
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", primaryPort, "postgres", "warp", "secret")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_REPLICAS", "jdbc:postgresql://127.0.0.1:" + p[1] + "/postgres~10")
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_READ_AFTER_WRITE_WINDOW_MS", "1500")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
            try (Connection c = DriverManager.getConnection(url, "warp", "secret")) {
                c.setAutoCommit(true);
                Thread.sleep(3500);
                assertEquals(replicaPort, serverPort(c), "a plain read on the default connection goes to the replica");

                c.createStatement().execute("insert into t values (777002, 'default-conn')");
                assertEquals(primaryPort, serverPort(c), "right after a write: primary");
                Thread.sleep(2200);
                assertEquals(replicaPort, serverPort(c), "back to the replica after the window");

                c.createStatement().execute("set application_name = 'pinned'"); // connection-bound state
                Thread.sleep(2200);
                assertEquals(primaryPort, serverPort(c), "a session holding SET state never reads from a replica");
            }
            try (Connection c = DriverManager.getConnection(url, "warp", "secret")) {
                c.setAutoCommit(false);
                assertEquals(primaryPort, serverPort(c), "inside a transaction: primary");
                c.rollback();
            }
        }
    }

    /**
     * Same, through a real MySQL client (Connector/J) and mywire, with a MySQL backend and replica.
     * WARP_TEST_WIRE_MY_PORTS=primary,replica (GTID or plain async replication, root with no password,
     * table {@code w.t(id int primary key, v varchar(50))}); the config database is the Postgres from
     * WARP_TEST_WIRE_PG_PORTS (primary port only).
     */
    @Test
    void readsFromAMySqlClientThroughMywireGoToTheMySqlReplica() throws Exception {
        String my = System.getenv("WARP_TEST_WIRE_MY_PORTS");
        String pg = System.getenv("WARP_TEST_WIRE_PG_PORTS");
        Assumptions.assumeTrue(my != null && pg != null);
        String[] m = my.split(",");
        String pgPort = pg.split(",")[0];
        int primaryPort = Integer.parseInt(m[0]);
        int replicaPort = Integer.parseInt(m[1]);
        resetWarpConfig(pgPort);
        String opts = "?allowPublicKeyRetrieval=true&useSSL=false";
        String spec = "my=jdbc:mysql://127.0.0.1:" + m[0] + "/w" + opts + "|root|||jdbc:mysql://127.0.0.1:"
                + m[1] + "/w" + opts + "~10|follow";
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", Integer.parseInt(pgPort), "postgres", "warp", "secret")
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_BACKENDS", spec)
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_READ_AFTER_WRITE_WINDOW_MS", "1500")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/my?allowPublicKeyRetrieval=true&useSSL=false";
            try (Connection c = DriverManager.getConnection(url, "warp", "secret")) {
                c.setAutoCommit(true);
                Thread.sleep(3500);
                assertEquals(replicaPort, mysqlPort(c), "a plain read goes to the MySQL replica");
                c.createStatement().execute("insert into w.t values (777101, 'via-mywire')");
                assertEquals(primaryPort, mysqlPort(c), "right after a write: primary");
                Thread.sleep(2200);
                assertEquals(replicaPort, mysqlPort(c), "back to the replica after the window");
                try (var st = c.createStatement(); var rs = st.executeQuery("select v from w.t where id = 777101 for update")) {
                    assertTrue(rs.next());
                }
            }
        }
    }

    private static int mysqlPort(Connection c) throws Exception {
        // a FROM clause, so mywire forwards it instead of answering the system variable itself (select @@port is
        // answered by Warp's own emulation and says nothing about which backend served the read)
        try (var st = c.createStatement(); var rs = st.executeQuery(
                "select variable_value from performance_schema.global_variables where variable_name = 'port'")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /**
     * Same, through a real SQL Server client (mssql-jdbc) and mssqlwire, against the read-scale availability group
     * from {@code Warp/tests/sqlserver-ag/ag.sh up} (sql1 primary on 14331, sql2 readable secondary on 14332).
     * Opt-in with WARP_TEST_WIRE_MSSQL_AG=1; the config database is the Postgres from WARP_TEST_WIRE_PG_PORTS.
     */
    @Test
    void readsFromASqlServerClientThroughMssqlwireGoToTheAvailabilityGroupSecondary() throws Exception {
        String pg = System.getenv("WARP_TEST_WIRE_PG_PORTS");
        Assumptions.assumeTrue("1".equals(System.getenv("WARP_TEST_WIRE_MSSQL_AG")) && pg != null);
        String pgPort = pg.split(",")[0];
        resetWarpConfig(pgPort);
        String opts = ";databaseName=w;encrypt=true;trustServerCertificate=true";
        String spec = "ms=jdbc:sqlserver://127.0.0.1:14331" + opts + "|sa|Warp_Test_1234!||jdbc:sqlserver://127.0.0.1:14332"
                + opts + "~10|follow";
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", Integer.parseInt(pgPort), "postgres", "warp", "secret")
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .env("WARP_BACKENDS", spec.replace(";", "%3B"))
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_READ_AFTER_WRITE_WINDOW_MS", "1500")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                    + ";databaseName=ms;encrypt=false;trustServerCertificate=true";
            try (Connection c = DriverManager.getConnection(url, "warp", "secret")) {
                c.setAutoCommit(true);
                Thread.sleep(3500);
                assertEquals("sql2", mssqlServer(c), "a plain read goes to the secondary");
                c.createStatement().execute("insert into dbo.t values (777201, 'via-mssqlwire')");
                assertEquals("sql1", mssqlServer(c), "right after a write: primary");
                Thread.sleep(2200);
                assertEquals("sql2", mssqlServer(c), "back to the secondary after the window");
            }
        }
    }

    private static String mssqlServer(Connection c) throws Exception {
        // a FROM clause so the read is forwarded rather than answered by the wire emulation
        try (var st = c.createStatement(); var rs = st.executeQuery("select name from sys.servers where server_id = 0")) {
            rs.next();
            return rs.getString(1);
        }
    }
}
