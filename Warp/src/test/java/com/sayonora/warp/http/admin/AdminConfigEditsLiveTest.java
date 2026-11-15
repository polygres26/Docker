package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.server.ServerOptions;
import com.sayonora.warp.testsupport.LocalPostgres;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Admin endpoints that edit the stored config (backend sets, observability) must not drop a change another writer made meanwhile
 * (for example a recorded failover): they go through {@code ConfigStore.update}. Opt-in: set WARP_TEST_BROWNOUT_PG_BIN.
 */
class AdminConfigEditsLiveTest {

    private static final int EACH = 15;

    private static HttpServletRequest request(String method, String body) {
        return (HttpServletRequest) Proxy.newProxyInstance(getClass0(), new Class<?>[] {HttpServletRequest.class}, (p, m, a) -> switch (m.getName()) {
            case "getMethod" -> method;
            case "getReader" -> new BufferedReader(new StringReader(body));
            default -> null;
        });
    }

    private static ClassLoader getClass0() {
        return AdminConfigEditsLiveTest.class.getClassLoader();
    }

    private static HttpServletResponse response(StringWriter out, int[] statusOut) {
        PrintWriter w = new PrintWriter(out);
        return (HttpServletResponse) Proxy.newProxyInstance(getClass0(), new Class<?>[] {HttpServletResponse.class}, (p, m, a) -> {
            if (m.getName().equals("getWriter")) {
                return w;
            }
            if (m.getName().equals("setStatus")) {
                statusOut[0] = (Integer) a[0];
            }
            return null;
        });
    }

    @Test
    void endpointsKeepConcurrentConfigChanges() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        LocalPostgres pg = LocalPostgres.primary(bin, Files.createTempDirectory("admincfg"), "cfg", LocalPostgres.freePort());
        try {
            ServerOptions options = ServerOptions.forTesting("127.0.0.1", pg.port(), "postgres", "warp", "secret");
            ConfigStore store = new ConfigStore(options);
            store.ensureSchema();
            store.write(WarpConfig.fromEnvDefaults().withRouterTableShards(""));

            List<Throwable> failures = new ArrayList<>();
            Runnable markers = () -> run(failures, () -> {
                for (int i = 0; i < EACH; i++) {
                    String marker = "m" + i + ";";
                    store.update(c -> c.withRouterTableShards((c.routerTableShards() == null ? "" : c.routerTableShards()) + marker));
                }
            });
            Runnable sets = () -> run(failures, () -> {
                for (int i = 0; i < EACH; i++) {
                    int[] st = new int[1];
                    StringWriter out = new StringWriter();
                    BackendSetsApi.handle("/api/backend-sets", request("POST", "{\"name\":\"set" + i + "\"}"), response(out, st), store, null, options);
                    assertEquals(201, st[0], out.toString());
                }
            });
            Runnable observability = () -> run(failures, () -> {
                for (int i = 0; i < EACH; i++) {
                    int[] st = new int[1];
                    StringWriter out = new StringWriter();
                    ObservabilityApi.handlePatch(request("PATCH", "{\"otlpEnabled\":" + (i % 2 == 0) + "}"), response(out, st), store,
                            new com.sayonora.warp.core.StatsCollectorStage(),
                            new com.sayonora.warp.core.QosControlStage(new com.sayonora.warp.core.QosControlStage.ClassLimit(1000, 1000, 0), java.util.Map.of(), 1, null),
                            null);
                    assertEquals(200, st[0], out.toString());
                }
            });
            List<Thread> threads = List.of(new Thread(markers), new Thread(sets), new Thread(observability));
            threads.forEach(Thread::start);
            for (Thread t : threads) {
                t.join(120_000);
            }
            assertTrue(failures.isEmpty(), "endpoint calls failed: " + failures);

            WarpConfig last = store.readLatest().get().payload();
            for (int i = 0; i < EACH; i++) {
                assertTrue(last.routerTableShards().contains("m" + i + ";"), "marker " + i + " was lost");
                assertTrue(last.backendSetNames() != null && last.backendSetNames().contains("set" + i), "set" + i + " was lost");
            }
            assertEquals("true", last.otlpExportOverride(), "last observability PATCH (i=" + (EACH - 1) + ") kept");
        } finally {
            pg.stop("immediate");
        }
    }

    private interface Body {
        void run() throws Exception;
    }

    private static void run(List<Throwable> failures, Body body) {
        try {
            body.run();
        } catch (Throwable t) {
            synchronized (failures) {
                failures.add(t);
            }
        }
    }
}
