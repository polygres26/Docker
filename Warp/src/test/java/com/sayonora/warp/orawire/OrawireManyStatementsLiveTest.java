package com.sayonora.warp.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * One long-lived Oracle connection running well over a thousand distinct statements through orawire (Adapt). ojdbc
 * closes cursors by id in a piggyback ahead of the next call; when the id happened to look like another call
 * (773 = bytes 03 05) the server misparsed the following request and failed with ORA-00600. Needs a Postgres
 * ({@code WARP_TEST_ORAWIRE_PG_PORT}, superuser "warp", trust auth, table {@code t(id bigint primary key, proto text, n int)}).
 */
class OrawireManyStatementsLiveTest {

    @Test
    void aLongLivedConnectionSurvivesTheCursorIdsThatLookLikeCalls() throws Exception {
        String pgPort = System.getenv("WARP_TEST_ORAWIRE_PG_PORT");
        Assumptions.assumeTrue(pgPort != null);
        // Warp seeds its stored config (QoS limits included) from the environment only when warp_config does not exist
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + pgPort + "/postgres", "warp", "secret");
                var st = c.createStatement()) {
            st.execute("drop table if exists warp_config cascade");
        }
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", Integer.parseInt(pgPort), "postgres", "warp", "secret")
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                // the harness default (1000/s) would throttle this loop and surface as ORA-01013
                .env("WARP_QOS_RATE_PER_SEC", "1000000")
                .env("WARP_QOS_BURST", "1000000")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            Properties p = new Properties();
            p.setProperty("user", "warp");
            p.setProperty("password", "secret");
            try (Connection c = DriverManager.getConnection("jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything", p)) {
                for (int i = 1; i <= 1500; i++) {
                    // a new Statement per call, as an application using one per operation does: each opens a new
                    // cursor, the driver closes the old ones by id in a piggyback, and the ids keep climbing
                    try (var st = c.createStatement()) {
                        st.executeUpdate("insert into t values (" + i + ", 'many', " + i + ")");
                    }
                }
                try (var st = c.createStatement(); var rs = st.executeQuery("select count(*) from t where proto = 'many'")) {
                    rs.next();
                    assertEquals(1500, rs.getInt(1));
                }
            }
        }
    }

    private static int freePort() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
