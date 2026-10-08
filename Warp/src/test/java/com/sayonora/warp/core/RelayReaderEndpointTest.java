package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.ReplicaRouter.LagSample;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The relay reader endpoint sends each connection to a lag-eligible replica, skips unreachable ones, and falls back or refuses when none is left. */
class RelayReaderEndpointTest {

    private final java.util.List<ServerSocket> servers = new java.util.ArrayList<>();
    private RelayReaderEndpoint endpoint;

    @AfterEach
    void close() throws IOException {
        if (endpoint != null) {
            endpoint.close();
        }
        for (ServerSocket s : servers) {
            s.close();
        }
    }

    /** A fake database: writes its tag as a line to whoever connects, then closes. */
    private int fake(String tag) throws IOException {
        ServerSocket ss = new ServerSocket(0);
        servers.add(ss);
        Thread t = new Thread(() -> {
            while (!ss.isClosed()) {
                try (Socket c = ss.accept()) {
                    c.getOutputStream().write((tag + "\n").getBytes());
                    c.getOutputStream().flush();
                    Thread.sleep(50);
                } catch (Exception ignored) {
                    // done
                }
            }
        });
        t.setDaemon(true);
        t.start();
        return ss.getLocalPort();
    }

    private static int deadPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static String banner(int port) throws IOException {
        try (Socket s = new Socket("127.0.0.1", port)) {
            s.setSoTimeout(5000);
            return new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
        }
    }

    private RelayReaderEndpoint start(int primary, int r1, int r2, boolean fallback, Set<Integer> eligible) throws IOException {
        String spec = "db=jdbc:mysql://127.0.0.1:" + primary + "/x|u|p||jdbc:mysql://127.0.0.1:" + r1 + "/x~10^jdbc:mysql://127.0.0.1:" + r2 + "/x~10";
        BackendRegistry registry = BackendRegistry.fromConfig(spec, null);
        Map<String, LagSample> answers = new HashMap<>();
        for (int p : eligible) {
            answers.put("jdbc:mysql://127.0.0.1:" + p + "/x", new LagSample(true, true, 1, null, 0));
        }
        ReplicaRouter router = new ReplicaRouter(registry, (target, now) -> {
            LagSample a = answers.getOrDefault(target.jdbcUrl(), new LagSample(false, false, 0, "down", now));
            return new LagSample(a.ok(), a.isReplica(), a.lagSeconds(), a.message(), now);
        }, System::currentTimeMillis, 5, 30);
        router.probeAll();
        endpoint = new RelayReaderEndpoint("test", 0, "db", registry, router, 3306, fallback, 500);
        endpoint.start();
        return endpoint;
    }

    @Test
    void connectionsGoToTheEligibleReplicasInTurnAndNeverToThePrimary() throws Exception {
        int p = fake("PRIMARY");
        int r1 = fake("R1");
        int r2 = fake("R2");
        start(p, r1, r2, true, Set.of(r1, r2));
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            seen.add(banner(endpoint.port()));
        }
        assertEquals(Set.of("R1", "R2"), seen);
    }

    @Test
    void anIneligibleReplicaIsSkipped() throws Exception {
        int p = fake("PRIMARY");
        int r1 = fake("R1");
        int r2 = fake("R2");
        start(p, r1, r2, true, Set.of(r2));
        for (int i = 0; i < 4; i++) {
            assertEquals("R2", banner(endpoint.port()));
        }
    }

    @Test
    void anEligibleButUnreachableReplicaIsQuarantinedAndTheOtherServesTheConnection() throws Exception {
        int p = fake("PRIMARY");
        int dead = deadPort();
        int r2 = fake("R2");
        start(p, dead, r2, true, Set.of(dead, r2));
        for (int i = 0; i < 4; i++) {
            assertEquals("R2", banner(endpoint.port()));
        }
    }

    @Test
    void withNoReplicaTheConnectionGoesToThePrimaryOrIsRefusedAsConfigured() throws Exception {
        int p = fake("PRIMARY");
        int r1 = fake("R1");
        int r2 = fake("R2");
        start(p, r1, r2, true, Set.of());
        assertEquals("PRIMARY", banner(endpoint.port()));
        endpoint.close();
        start(p, r1, r2, false, Set.of());
        assertTrue(banner(endpoint.port()) == null, "refused: the connection closes without a banner");
    }
}
