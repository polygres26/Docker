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
}
