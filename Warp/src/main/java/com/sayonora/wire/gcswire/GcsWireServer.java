package com.sayonora.wire.gcswire;

import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
 * gcswire: Google Cloud Storage frontend (JSON API + XML API on one port, default 4443 like fake-gcs-server) whose data lives
 * in the {@code gcs} store of the backend set's Postgres hosts. See {@link GcsService} (JSON), {@link GcsXml} (XML),
 * {@link GcsStore} (storage), {@link GcsAuth}. Every request borrows pooled connections only for short statements; nothing is
 * pinned while a client uploads or downloads slowly.
 */
public final class GcsWireServer {

    private static final Logger log = LoggerFactory.getLogger(GcsWireServer.class);

    private final Server server;
    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;
    private final GcsStore store;
    private final GcsService json;
    private final GcsXml xml;
    private final GcsAuth auth;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "gcswire-sweeper");
        t.setDaemon(true);
        return t;
    });

    public GcsWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(port, registry, gate, metrics, GcsConfig.fromEnv());
    }

    GcsWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics, GcsConfig cfg) {
        this.registry = registry;
        this.metrics = metrics;
        GcsShards shards = new GcsShards(registry, StoreType.GCS);
        this.store = new GcsStore(shards, cfg);
        this.json = new GcsService(store, cfg);
        this.xml = new GcsXml(store, cfg, json.ops, json.resumable);
        this.auth = new GcsAuth(cfg, store);
        if (cfg.tokens.isEmpty() && !cfg.allowAnonymous) {
            log.warn("gcswire: no WARP_GCSWIRE_TOKENS and WARP_GCSWIRE_ALLOW_ANONYMOUS is not true -- only HMAC-signed XML requests and "
                    + "signed URLs can succeed");
        }
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
        String v = System.getenv("WARP_GCSWIRE_MAX_THREADS");
        try {
            return v == null || v.isBlank() ? 400 : Math.max(16, Integer.parseInt(v.trim()));
        } catch (NumberFormatException e) {
            return 400;
        }
    }

    private void serve(HttpServletRequest request, HttpServletResponse response, ConnectionGate gate) throws IOException {
        GcsReq r = GcsReq.of(request);
        GcsResp w = new GcsResp(response);
        List<String> segs = r.segments();
        boolean isJson = GcsService.isJsonPath(segs);
        try {
            if (!gate.acceptHttp(request)) {
                throw new GcsException(403, "forbidden", "AccessDenied", "This request is not authorized.");
            }
            if (r.method.equals("OPTIONS")) {
                r.op = "preflight";
                if (isJson) {
                    w.header("Access-Control-Allow-Origin", "*");
                    w.header("Access-Control-Allow-Methods", "GET, HEAD, PUT, POST, DELETE, PATCH, OPTIONS");
                    w.header("Access-Control-Allow-Headers", r.header("access-control-request-headers"));
                    w.header("Access-Control-Max-Age", "3600");
                    w.empty(200);
                } else {
                    xml.preflight(r, w);
                }
                return;
            }
            r.principal = auth.authenticate(r);
            if (isJson) {
                json.handle(r, w, segs, false);
            } else {
                xml.handle(r, w);
            }
        } catch (GcsException e) {
            render(e, r, w, response, isJson);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("gcswire request failed: {} {}", r.method, r.rawPath, e);
            if (!response.isCommitted()) {
                render(GcsException.internal("Internal Error (" + e + ")"), r, w, response, isJson);
            }
        } finally {
            record(r);
        }
    }

    private void render(GcsException e, GcsReq r, GcsResp w, HttpServletResponse response, boolean isJson) throws IOException {
        if (response.isCommitted()) {
            return;
        }
        response.reset();
        GcsResp out = new GcsResp(response);
        e.headers.forEach(out::header);
        out.headOnly = "HEAD".equals(r.method);
        if (e.status == 499 || e.status == 304) {
            out.empty(e.status);
            return;
        }
        if (isJson) {
            out.bytes(e.status, "application/json; charset=UTF-8", GcsJson.error(e).toString().getBytes(StandardCharsets.UTF_8));
        } else {
            out.bytes(e.status, "application/xml; charset=UTF-8", GcsXml.error(e).getBytes(StandardCharsets.UTF_8));
        }
    }

    private void record(GcsReq r) {
        if (metrics == null || "Unknown".equals(r.op)) {
            return;
        }
        long nanos = System.nanoTime() - r.startNanos;
        String backend = "default";
        List<String> h = registry.storeHosts(StoreType.GCS);
        if (!h.isEmpty()) {
            backend = h.size() == 1 ? h.get(0) : String.join(",", h);
        }
        metrics.recordOperation("gcswire", backend, r.write ? SqlMetricsCollector.StatementKind.WRITE
                : SqlMetricsCollector.StatementKind.READ, r.op, nanos, nanos);
    }

    public void start() throws Exception {
        server.start();
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                if (registry != null && !registry.storeHosts(StoreType.GCS).isEmpty()) {
                    store.sweep();
                }
            } catch (RuntimeException e) {
                log.debug("gcswire sweep failed: {}", e.getMessage());
            }
        }, 60, 60, TimeUnit.SECONDS);
        log.info("warp gcswire listening on port {}", ((ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        sweeper.shutdownNow();
        server.stop();
    }
}
