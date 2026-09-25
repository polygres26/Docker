package com.sayonora.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * orawire's own dual-port smoke test: {@code WARP_ORAWIRE_NATIVE_PORT} makes ONE running Warp
 * process open a SECOND orawire listener, alongside the primary one, without disturbing the
 * primary listener's own (translated-into-Postgres) behavior. Unlike mywire's/mssqlwire's
 * dual-port tests, this doesn't exercise a real Oracle backend behind the native port -- this
 * codebase has no lightweight real-Oracle testcontainer to point one at (see
 * {@code OraclePlsqlCallIntegrationTest} and friends, which all use dual-exec against a real
 * Oracle only when one happens to be configured, not started fresh here) -- so this proves what's
 * actually new and verifiable without one: the second port opens and accepts a real TCP
 * connection, and the primary port's translated behavior is completely unaffected by the second
 * listener's own startup.
 */
class OracleDualPortListenerIntegrationTest {

    private RealPostgres postgres;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    @Test
    void secondNativePortOpensAlongsideThePrimaryTranslatedPort() throws Exception {
        postgres = RealPostgres.start();

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .frontend("orawire-native", "WARP_ORAWIRE_NATIVE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        // The second (native) port accepts a real TCP connection -- proves Main actually started
        // a second listener bound to it, not just parsed the env var.
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", warp.port("orawire-native")), 2000);
            assertTrue(socket.isConnected());
        }

        // The primary port's own translated-mode behavior is unaffected: an ordinary client query
        // still works exactly as every other orawire test expects.
        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
        try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 21 * 2 FROM DUAL")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }
}
