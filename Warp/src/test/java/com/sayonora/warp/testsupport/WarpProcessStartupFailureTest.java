package com.sayonora.warp.testsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** A Warp that cannot start must fail the test quickly, say why, and not leave a process behind. */
class WarpProcessStartupFailureTest {

    @Test
    void aWarpThatCannotStartFailsFastExplainsWhyAndLeavesNoProcessBehind() throws Exception {
        long before = warpProcesses();
        try (ServerSocket unusedPort = new ServerSocket(0)) {
            int closedPort = unusedPort.getLocalPort(); // bound only to reserve a number, closed below
            unusedPort.close();
            Instant start = Instant.now();
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> WarpProcess.builder()
                    .pgBackend("127.0.0.1", closedPort, "postgres", "warp", "secret") // nothing listens there
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start());
            assertTrue(Duration.between(start, Instant.now()).toSeconds() < 25, "must not wait out the full startup window");
            assertTrue(e.getMessage().contains("last Warp output"), e.getMessage());
        }
        // give the OS a moment to reap the destroyed process
        long deadline = System.currentTimeMillis() + 5000;
        while (warpProcesses() > before && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(before, warpProcesses(), "the failed Warp must not stay running");
    }

    private static long warpProcesses() {
        return ProcessHandle.current().children()
                .filter(h -> h.info().arguments().map(a -> java.util.Arrays.asList(a).contains("com.sayonora.warp.server.Main")).orElse(false))
                .count();
    }
}
