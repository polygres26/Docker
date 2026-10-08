package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class FailoverFencersTest {

    private static final String URL = "jdbc:postgresql://db1.internal:6432/app?password=hunter2";

    @Test
    void nothingConfiguredMeansNoFencer() {
        assertNull(FailoverFencers.fromEnv(Map.of()));
        assertNull(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_SSH_TARGET", "root@h")), "an SSH target without a command is not a fencer");
    }

    @Test
    void shellCommandSeesTheFailedPrimaryUrl() throws Exception {
        assertTrue(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_COMMAND", "test \"$FAILED_PRIMARY_URL\" = '" + URL + "'")).fence(URL));
        assertFalse(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_COMMAND", "exit 3")).fence(URL));
    }

    @Test
    void execRunsWithoutAShellAndFillsInThePlaceholders() throws Exception {
        assertTrue(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_EXEC", "test {host}:{port} = db1.internal:6432")).fence(URL));
        assertFalse(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_EXEC", "test {host} = other")).fence(URL));
        // {url} is the masked URL, never the password
        assertTrue(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_EXEC", "test {url} = jdbc:postgresql://db1.internal:6432/app?password=****")).fence(URL));
        // no shell: a metacharacter in a value is just text
        assertFalse(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_EXEC", "test x = x;true;false")).fence(URL));
    }

    @Test
    void sshRunsTheCommandOnTheFailedPrimarysHost() throws Exception {
        Path dir = Files.createTempDirectory("fakessh");
        Path log = dir.resolve("args");
        Path bin = dir.resolve("ssh");
        Files.writeString(bin, "#!/bin/sh\necho \"$@\" > " + log + "\n");
        bin.toFile().setExecutable(true);
        assertTrue(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_SSH_BIN", bin.toString(), "WARP_FAILOVER_FENCE_SSH_TARGET", "root@{host}",
                "WARP_FAILOVER_FENCE_SSH_COMMAND", "systemctl stop postgresql", "WARP_FAILOVER_FENCE_SSH_OPTS", "-i /k")).fence(URL));
        assertEquals("-o BatchMode=yes -o ConnectTimeout=10 -i /k root@db1.internal systemctl stop postgresql", Files.readString(log).trim());
    }

    @Test
    void webhookPostsTheFailureAndAny2xxMeansFenced() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ok", ex -> {
            seen.add(ex.getRequestHeaders().getFirst("Authorization") + " " + new String(ex.getRequestBody().readAllBytes()));
            ex.sendResponseHeaders(204, -1);
            ex.close();
        });
        server.createContext("/no", ex -> {
            ex.sendResponseHeaders(500, -1);
            ex.close();
        });
        server.start();
        try {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            assertTrue(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_WEBHOOK", base + "/ok", "WARP_FAILOVER_FENCE_WEBHOOK_TOKEN", "t0k")).fence(URL));
            assertEquals(1, seen.size());
            assertTrue(seen.get(0).startsWith("Bearer t0k "), seen.get(0));
            assertTrue(seen.get(0).contains("\"host\":\"db1.internal\"") && seen.get(0).contains("\"port\":6432"), seen.get(0));
            assertFalse(seen.get(0).contains("hunter2"), "the password never leaves Warp");
            assertFalse(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_WEBHOOK", base + "/no")).fence(URL));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void severalFencersAllMustSucceedInOrder() throws Exception {
        var f = FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_COMMAND", "true", "WARP_FAILOVER_FENCE_EXEC", "false"));
        assertFalse(f.fence(URL));
        assertTrue(FailoverFencers.fromEnv(Map.of("WARP_FAILOVER_FENCE_COMMAND", "true", "WARP_FAILOVER_FENCE_EXEC", "true")).fence(URL));
    }
}
