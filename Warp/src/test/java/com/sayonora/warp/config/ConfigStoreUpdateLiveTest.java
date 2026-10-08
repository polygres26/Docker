package com.sayonora.warp.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.server.ServerOptions;
import com.sayonora.warp.testsupport.LocalPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Concurrent edits to different parts of the stored config must all survive. {@code update} re-reads and retries when another version
 * appeared; the plain read-then-write used before drops edits, which the control run counts. Opt-in: set WARP_TEST_BROWNOUT_PG_BIN.
 */
class ConfigStoreUpdateLiveTest {

    private static final int THREADS = 8;
    private static final int EACH = 12;

    private static int run(ConfigStore store, boolean safe) throws Exception {
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            final int id = t;
            Thread th = new Thread(() -> {
                try {
                    for (int i = 0; i < EACH; i++) {
                        String marker = "m" + id + "_" + i + ";";
                        if (safe) {
                            store.update(c -> c.withRouterTableShards((c.routerTableShards() == null ? "" : c.routerTableShards()) + marker));
                        } else {
                            var latest = store.readLatest().get().payload();
                            store.write(latest.withRouterTableShards((latest.routerTableShards() == null ? "" : latest.routerTableShards()) + marker));
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            th.start();
            threads.add(th);
        }
        for (Thread th : threads) {
            th.join(60_000);
        }
        String all = store.readLatest().get().payload().routerTableShards();
        int present = 0;
        for (int t = 0; t < THREADS; t++) {
            for (int i = 0; i < EACH; i++) {
                if (all.contains("m" + t + "_" + i + ";")) {
                    present++;
                }
            }
        }
        return present;
    }

    @Test
    void updateKeepsEveryConcurrentEditWhereAPlainReadThenWriteDropsSome() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("cfgupdate");
        LocalPostgres pg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        try {
            ConfigStore store = new ConfigStore(ServerOptions.forTesting("127.0.0.1", pg.port(), "postgres", "warp", "secret"));
            store.ensureSchema();
            store.write(com.sayonora.warp.config.WarpConfig.fromEnvDefaults().withRouterTableShards(""));
            int plain = run(store, false);
            store.write(com.sayonora.warp.config.WarpConfig.fromEnvDefaults().withRouterTableShards(""));
            int safe = run(store, true);
            System.out.println("CFG-NOTE plain read-then-write kept " + plain + " of " + THREADS * EACH + " concurrent edits; update kept " + safe);
            assertEquals(THREADS * EACH, safe, "update loses nothing");
            assertTrue(plain < THREADS * EACH, "control: the plain read-then-write does lose edits under contention (kept " + plain + ")");
        } finally {
            pg.stop("immediate");
        }
    }
}
