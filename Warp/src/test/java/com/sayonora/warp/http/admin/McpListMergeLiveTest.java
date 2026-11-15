package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.mcp.upstream.McpUpstream;
import com.sayonora.warp.server.ServerOptions;
import com.sayonora.warp.testsupport.LocalPostgres;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** An MCP list edit built from a stale read is replayed onto the latest list, so a concurrent add/change/delete survives. */
class McpListMergeLiveTest {

    private record Item(String id, String name, int v) {
    }

    private static List<Item> merge(List<Item> before, List<Item> mine, List<Item> fresh) {
        return HttpAdminAccess.merge(before, mine, fresh);
    }

    /** Test shim: mergeById is package-private in MetricsServer. */
    private static final class HttpAdminAccess {
        static List<Item> merge(List<Item> before, List<Item> mine, List<Item> fresh) {
            Function<Item, String> id = Item::id;
            return MetricsServer.mergeById(before, mine, new ArrayList<>(fresh), id, i -> "name:" + i.name(), "item");
        }
    }

    @Test
    void aConcurrentAddSurvivesAnEditMadeFromAStaleRead() {
        Item a = new Item("a", "A", 1);
        Item b = new Item("b", "B", 1);
        List<Item> before = List.of(a);
        List<Item> mine = List.of(new Item("a", "A", 2));            // I changed a
        List<Item> fresh = List.of(a, b);                            // someone added b meanwhile
        assertEquals(List.of(new Item("a", "A", 2), b), merge(before, mine, fresh));
    }

    @Test
    void anAddAndADeleteAreReplayed() {
        Item a = new Item("a", "A", 1);
        Item b = new Item("b", "B", 1);
        Item c = new Item("c", "C", 1);
        assertEquals(List.of(b, c), merge(List.of(a, b), List.of(b, c), List.of(a, b)));
    }

    @Test
    void changingAnEntryDeletedMeanwhileOrAddingADuplicateNameIsRefused() {
        Item a = new Item("a", "A", 1);
        assertThrows(IllegalStateException.class, () -> merge(List.of(a), List.of(new Item("a", "A", 2)), List.of()));
        Item theirs = new Item("x", "same", 1);
        assertThrows(IllegalStateException.class, () -> merge(List.of(), List.of(new Item("y", "same", 1)), List.of(theirs)));
    }

    @Test
    void concurrentUpstreamWritesFromStaleReadsAllSurvive() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        LocalPostgres pg = LocalPostgres.primary(bin, Files.createTempDirectory("mcpmerge"), "cfg", LocalPostgres.freePort());
        try {
            ConfigStore store = new ConfigStore(ServerOptions.forTesting("127.0.0.1", pg.port(), "postgres", "warp", "secret"));
            store.ensureSchema();
            store.write(WarpConfig.fromEnvDefaults());
            List<McpUpstream> stale = List.of(); // every writer read the empty list before anyone wrote
            int n = 12;
            List<Thread> threads = new ArrayList<>();
            List<Throwable> failures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                final int k = i;
                Thread t = new Thread(() -> {
                    try {
                        McpUpstream u = new McpUpstream("id" + k, "n" + k, "p" + k, "http://h" + k, McpUpstream.Transport.STREAMABLE_HTTP,
                                McpUpstream.AuthMode.NONE, true, false, null, null, null, null, null, null, null, null, null,
                                "unknown", null, 0, Instant.EPOCH, Instant.EPOCH);
                        MetricsServer.writeMcpUpstreams(store, stale, List.of(u));
                    } catch (Throwable e) {
                        synchronized (failures) {
                            failures.add(e);
                        }
                    }
                });
                t.start();
                threads.add(t);
            }
            for (Thread t : threads) {
                t.join(60_000);
            }
            assertTrue(failures.isEmpty(), failures.toString());
            assertEquals(n, McpUpstream.parse(store.readLatest().get().payload().mcpUpstreams()).size(), "no concurrent add lost");
        } finally {
            pg.stop("immediate");
        }
    }
}
