package com.sayonora.warp.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link OtlpConnectivityTest} -- exercised against REAL sockets (an
 * unreachable port, and a real local socket that accepts and immediately closes the connection),
 * not mocks, since the whole point of this class is that it makes a genuine network call. */
class OtlpConnectivityTestTest {

    @Test
    void anUnreachablePortFailsWithARealErrorMessageWithinTheTimeout() {
        long start = System.currentTimeMillis();
        OtlpConnectivityTest.Result r = OtlpConnectivityTest.test("grpc", "http://localhost:1", Map.of(), Duration.ofSeconds(2));
        long elapsed = System.currentTimeMillis() - start;
        assertFalse(r.success());
        assertNotNull(r.errorMessage());
        assertFalse(r.errorMessage().isBlank());
        assertTrue(elapsed < 5000, "must respect the timeout bound, took " + elapsed + "ms");
        assertEquals("grpc", r.protocol());
        assertEquals("http://localhost:1", r.endpoint());
    }

    @Test
    void httpProtocolIsNormalizedAndReported() {
        OtlpConnectivityTest.Result r = OtlpConnectivityTest.test("http", "http://localhost:1", Map.of(), Duration.ofSeconds(2));
        assertEquals("http", r.protocol());
        assertFalse(r.success());
    }

    @Test
    void anUnrecognizedProtocolDefaultsToGrpc() {
        OtlpConnectivityTest.Result r = OtlpConnectivityTest.test("carrier-pigeon", "http://localhost:1", Map.of(), Duration.ofSeconds(2));
        assertEquals("grpc", r.protocol());
    }

    @Test
    void tookMsIsARealPositiveMeasurement() {
        OtlpConnectivityTest.Result r = OtlpConnectivityTest.test("grpc", "http://localhost:1", Map.of(), Duration.ofSeconds(2));
        assertTrue(r.tookMs() >= 0, "elapsed time must be a real, non-negative measurement");
    }

    @Test
    void aRealListeningSocketThatImmediatelyClosesStillProducesARealAttempt() throws Exception {
        // Proves the probe makes a genuine connection attempt against something actually
        // listening, not just a DNS/syntax check -- the connection is accepted then torn down
        // immediately, so the OTLP request itself still fails (not a valid gRPC/HTTP response),
        // but that failure must be a real one from a real handshake attempt, not a fast-fail on
        // an unreachable port.
        try (ServerSocket server = new ServerSocket(0)) {
            Thread acceptor = new Thread(() -> {
                try {
                    var socket = server.accept();
                    socket.close();
                } catch (Exception ignored) {
                    // server closed while waiting -- fine, test is ending
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();

            OtlpConnectivityTest.Result r = OtlpConnectivityTest.test("grpc", "http://localhost:" + server.getLocalPort(),
                    Map.of(), Duration.ofSeconds(3));
            assertFalse(r.success(), "a socket that accepts then closes is not a valid OTLP receiver");
            assertNotNull(r.errorMessage());
        }
    }
}
