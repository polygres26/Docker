package com.sayonora.warp.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Logging in many times through mywire must never fail with correct credentials. The handshake's random challenge is
 * sent with a NUL after its last 12 bytes, so a challenge containing a zero byte there reaches the client truncated and
 * its password response no longer matches: a few percent of logins were refused with "Access denied". Needs a Postgres
 * ({@code WARP_TEST_MYWIRE_PG_PORT}, superuser "warp", trust auth).
 */
class MywireManyLoginsLiveTest {

    @Test
    void thousandsOfLoginsWithTheRightPasswordAllSucceed() throws Exception {
        String pgPort = System.getenv("WARP_TEST_MYWIRE_PG_PORT");
        Assumptions.assumeTrue(pgPort != null);
        try (WarpProcess warp = WarpProcess.builder()
                .pgBackend("127.0.0.1", Integer.parseInt(pgPort), "postgres", "warp", "secret")
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_QOS_RATE_PER_SEC", "1000000")
                .env("WARP_QOS_BURST", "1000000")
                .env("WARP_GRPC_PORT", String.valueOf(freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start()) {
            String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres?useSSL=false&allowPublicKeyRetrieval=true";
            int logins = 3000;
            int failures = 0;
            Map<String, Integer> reasons = new TreeMap<>();
            for (int i = 0; i < logins; i++) {
                try (Connection c = DriverManager.getConnection(url, "warp", "secret")) {
                    c.isValid(1);
                } catch (Exception e) {
                    failures++;
                    reasons.merge(String.valueOf(e.getMessage()).split("\n")[0], 1, Integer::sum);
                }
            }
            System.out.println("LOGIN-RESULT failures=" + failures + " of " + logins + " " + reasons);
            assertEquals(0, failures, "refused logins: " + reasons);
        }
    }

    private static int freePort() throws java.io.IOException {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
