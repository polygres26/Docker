package com.sayonora.warp.cosmoswire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * cosmoswire: the Azure Cosmos DB for NoSQL (SQL/Core) REST API over plain HTTP (default port 18081; the official SDKs and the
 * emulator's well-known master key work against it), with documents in the {@code cosmos} store of the backend set's Postgres hosts.
 * See {@link CosmosHttp} (protocol), {@link CosmosService} (operations), {@link CosmosSql}/{@link CosmosQuery} (the SQL language),
 * {@link CosmosStore} (storage and sharding).
 */
public final class CosmosWireServer {

    private static final Logger log = LoggerFactory.getLogger(CosmosWireServer.class);

    private final Server server;
    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final CosmosStore store;
    private final CosmosHttp http;
    private final CosmosConfig cfg;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "cosmoswire-sweeper");
        t.setDaemon(true);
        return t;
    });

    public CosmosWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(port, registry, gate, metrics, CosmosConfig.fromEnv());
    }

    CosmosWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics, CosmosConfig cfg) {
        this.registry = registry;
        this.metrics = metrics;
        this.cfg = cfg;
        this.store = new CosmosStore(registry);
        this.http = new CosmosHttp(new CosmosService(store, cfg), cfg);
        this.server = new Server(new QueuedThreadPool(threads()));
        ServerConnector c = new ServerConnector(server);
        c.setPort(port);
        server.addConnector(c);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response) throws IOException {
                base.setHandled(true);
                serve(request, response, gate);
            }
        });
    }

    private static int threads() {
        String v = System.getenv("WARP_COSMOSWIRE_MAX_THREADS");
        try {
            return v == null || v.isBlank() ? 400 : Math.max(16, Integer.parseInt(v.trim()));
        } catch (NumberFormatException e) {
            return 400;
        }
    }

    private void serve(HttpServletRequest request, HttpServletResponse response, ConnectionGate gate) throws IOException {
        long t0 = System.nanoTime();
        CosmosHttp.Outcome out = new CosmosHttp.Outcome();
        try {
            if (gate != null && !gate.acceptHttp(request)) {
                response.setStatus(403);
                response.setContentType("application/json");
                response.getOutputStream().write("{\"code\":\"Forbidden\",\"message\":\"This request is not authorized to perform this operation.\"}"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                return;
            }
            http.handle(request, response, out);
        } finally {
            record(out, System.nanoTime() - t0);
        }
    }

    private void record(CosmosHttp.Outcome out, long nanos) {
        if (metrics == null || "Unknown".equals(out.op)) {
            return;
        }
        String backend = "default";
        List<String> h = registry.storeHosts(StoreType.COSMOS);
        if (!h.isEmpty()) {
            backend = h.size() == 1 ? h.get(0) : String.join(",", h);
        }
        metrics.recordOperation("cosmoswire", backend, out.write ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ,
                out.op, nanos, nanos);
    }

    public void start() throws Exception {
        server.start();
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                if (store.available()) {
                    store.sweepExpired();
                }
            } catch (RuntimeException e) {
                log.debug("cosmoswire TTL sweep failed: {}", e.getMessage());
            }
        }, cfg.sweepSeconds, cfg.sweepSeconds, TimeUnit.SECONDS);
        log.info("warp cosmoswire listening on port {}", port());
    }

    public int port() {
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    public void stop() throws Exception {
        sweeper.shutdownNow();
        server.stop();
    }
}
