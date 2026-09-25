package com.sayonora.wire.acl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Proves the Developer-edition connection cap ({@code License.DEVELOPER_MAX_CONNECTIONS}, 25) is
 * genuinely enforced at the TCP-accept layer, not just documented -- a real subprocess with no
 * {@code WARP_LICENSE_KEY} set (so it's running Developer tier, exactly like a real customer
 * who hasn't bought Enterprise), real pgwire JDBC connections held open simultaneously, no mocks.
 *
 * <p>Also proves {@code ConnectionGate.release()} actually gets called on session end (wired via
 * {@code Main.submitSession}'s try/finally, not inside any individual session handler) -- closing
 * one held connection and opening a new one must succeed, not stay stuck at the cap forever.
 */
class DeveloperConnectionCapIntegrationTest {

    @Test
    void the26thConcurrentConnectionIsRejectedAndClosingOneFreesASlot() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(),
                                postgres.username(), postgres.password())
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        // No WARP_LICENSE_KEY -- this subprocess is genuinely running Developer
                        // tier, the same as any real install that hasn't bought Enterprise.
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
            List<Connection> held = new ArrayList<>();
            try {
                for (int i = 0; i < 25; i++) {
                    Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password());
                    assertTrue(c.isValid(2), "connection " + (i + 1) + " of 25 must succeed -- still at/under the cap");
                    held.add(c);
                }

                SQLException rejected = assertThrows(SQLException.class,
                        () -> DriverManager.getConnection(url, postgres.username(), postgres.password()),
                        "the 26th simultaneous connection must be rejected -- Developer edition is capped at 25");
                assertTrue(rejected.getMessage() != null);

                // Free exactly one slot -- release() must have actually run on close, via
                // Main.submitSession's try/finally, not left the count stuck at 25 forever.
                held.remove(0).close();
                Thread.sleep(300); // the session's own close is async from this side; give it a beat
                try (Connection c = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = c.createStatement();
                        ResultSet rs = st.executeQuery("SELECT 1")) {
                    assertTrue(rs.next(), "a new connection must succeed once a held one is closed and its slot released");
                    assertEquals(1, rs.getInt(1));
                }
            } finally {
                for (Connection c : held) {
                    try {
                        c.close();
                    } catch (SQLException ignored) {
                        // best-effort cleanup
                    }
                }
            }
        }
    }
}
