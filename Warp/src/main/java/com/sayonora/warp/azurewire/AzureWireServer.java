package com.sayonora.warp.azurewire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.ConnectionRoute;
import com.sayonora.warp.core.ConnectionRouter;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
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
 * azurewire: the Azure Storage REST frontends (Blob, Queue, Table), each its own listener, all storing in Postgres
 * through the {@code azblob}/{@code azqueue}/{@code aztable} stores of the backend set. One store type per service:
 * they are independent products that shard on different keys, and an operator may want blobs on one pair of hosts and
 * tables on another. See {@link BlobService}, {@link QueueService}, {@link TableService}, {@link AzureAuth}.
 */
public final class AzureWireServer {

    private static final Logger log = LoggerFactory.getLogger(AzureWireServer.class);

    @FunctionalInterface
    interface Handler {
        void handle(AzReq r, HttpServletResponse resp) throws IOException;
    }

    private final Server server;
    private final String service;
    private final StoreType type;
    private final BackendRegistry registry;
    private final AzureConfig cfg;
    private final SqlMetricsCollector metrics;
    private final Handler handler;
    private final Runnable sweep;
    private final long sweepSeconds;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "azurewire-sweeper");
        t.setDaemon(true);
        return t;
    });

    private AzureWireServer(int port, String service, StoreType type, BackendRegistry registry, AzureConfig cfg,
            ConnectionGate gate, SqlMetricsCollector metrics, Handler handler, Runnable sweep, long sweepSeconds) {
        this.service = service;
        this.type = type;
        this.registry = registry;
        this.cfg = cfg;
        this.metrics = metrics;
        this.handler = handler;
        this.sweep = sweep;
        this.sweepSeconds = sweepSeconds;
        this.server = new Server(new QueuedThreadPool(threads()));
        ServerConnector c = new ServerConnector(server);
        c.setPort(port);
        server.addConnector(c);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response)
                    throws IOException {
                base.setHandled(true);
                serve(request, response, gate);
            }
        });
    }

    private static int threads() {
        String v = System.getenv("WARP_AZUREWIRE_MAX_THREADS");
        try {
            return v == null || v.isBlank() ? 400 : Math.max(16, Integer.parseInt(v.trim()));
        } catch (NumberFormatException e) {
            return 400;
        }
    }

    public static AzureWireServer blob(int port, BackendRegistry reg, AzureConfig cfg, ConnectionGate gate, SqlMetricsCollector m) {
        AzShards sh = new AzShards(reg, StoreType.AZBLOB);
        BlobStore st = new BlobStore(sh);
        BlobService svc = new BlobService(st, cfg, new AzureAuth(cfg));
        return new AzureWireServer(port, AzReq.BLOB, StoreType.AZBLOB, reg, cfg, gate, m, svc::handle, () -> {
            if (sh.available()) {
                st.sweepTemporary(3600);
            }
        }, 300);
    }

    public static AzureWireServer queue(int port, BackendRegistry reg, AzureConfig cfg, ConnectionGate gate, SqlMetricsCollector m) {
        AzShards sh = new AzShards(reg, StoreType.AZQUEUE);
        QueueStore st = new QueueStore(sh);
        QueueService svc = new QueueService(st, cfg, new AzureAuth(cfg));
        return new AzureWireServer(port, AzReq.QUEUE, StoreType.AZQUEUE, reg, cfg, gate, m, svc::handle, () -> {
            if (sh.available()) {
                st.sweepExpired();
            }
        }, sweepSeconds());
    }

    public static AzureWireServer table(int port, BackendRegistry reg, AzureConfig cfg, ConnectionGate gate, SqlMetricsCollector m) {
        AzShards sh = new AzShards(reg, StoreType.AZTABLE);
        TableStore st = new TableStore(sh);
        TableService svc = new TableService(st, cfg, new AzureAuth(cfg));
        return new AzureWireServer(port, AzReq.TABLE, StoreType.AZTABLE, reg, cfg, gate, m, svc::handle, () -> { }, 3600);
    }

    private static long sweepSeconds() {
        String v = System.getenv("WARP_AZQUEUEWIRE_SWEEP_SECONDS");
        try {
            return v == null || v.isBlank() ? 10 : Math.max(1, Long.parseLong(v.trim()));
        } catch (NumberFormatException e) {
            return 10;
        }
    }

    private String serverName() {
        return switch (service) {
            case AzReq.BLOB -> "Windows-Azure-Blob/1.0 Microsoft-HTTPAPI/2.0";
            case AzReq.QUEUE -> "Windows-Azure-Queue/1.0 Microsoft-HTTPAPI/2.0";
            default -> "Windows-Azure-Table/1.0 Microsoft-HTTPAPI/2.0";
        };
    }

    private void serve(HttpServletRequest request, HttpServletResponse response, ConnectionGate gate) throws IOException {
        String requestId = UUID.randomUUID().toString();
        AzReq r = null;
        try {
            r = new AzReq(request, service, cfg, requestId);
            AzHttp.setCommon(response, r, serverName());
            if (!gate.acceptHttp(request)) {
                throw new AzureException(403, "AuthorizationFailure", "This request is not authorized to perform this operation.");
            }
            if (r.account.isEmpty() || cfg.account(r.account) == null) {
                throw new AzureException(404, "AccountNotFound", "The specified storage account \"" + r.account
                        + "\" is not configured on this Warp (WARP_AZURE_ACCOUNTS).");
            }
            ConnectionRoute route = registry.connectionRouter().resolve(ConnectionRouter.PROTO_HTTP, r.account, null);
            if (route.isRejected()) {
                throw new AzureException(403, "AccountIsDisabled", "The specified account is disabled: " + route.description());
            }
            List<String> routed = registry.connectionRouter().storeBackends(route);
            if (routed != null && routed.isEmpty()) {
                throw new AzureException(403, "AccountIsDisabled", "The route for this account names a backend that cannot host "
                        + type.id());
            }
            AzShards.route(routed);
            handler.handle(r, response);
        } catch (AzureException e) {
            render(e, r, request, response);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("azurewire {} request failed", service, e);
            if (!response.isCommitted()) {
                render(new AzureException(500, "InternalError", "The server encountered an internal error. Please retry the "
                        + "request. (" + e + ")"), r, request, response);
            }
        } finally {
            AzShards.route(null);
            record(r);
        }
    }

    private void render(AzureException e, AzReq r, HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (r == null) {
            response.setStatus(e.status);
            return;
        }
        if (response.isCommitted()) {
            return;
        }
        response.reset();
        AzHttp.setCommon(response, r, serverName());
        e.headers.forEach(response::setHeader);
        response.setHeader("x-ms-error-code", e.code);
        if ("HEAD".equals(r.method)) {
            response.setStatus(e.status);
            return;
        }
        if (AzReq.TABLE.equals(service)) {
            byte[] b = AzHttp.jsonError(e, r, request.getHeader("Accept")).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            response.setStatus(e.status);
            response.setContentType(TableService.contentType(errLevel(request.getHeader("Accept"))));
            response.setContentLength(b.length);
            response.getOutputStream().write(b);
        } else {
            AzHttp.xml(response, e.status, AzHttp.xmlError(e, r));
        }
    }

    private static String errLevel(String accept) {
        String l = accept == null ? "" : accept.toLowerCase(java.util.Locale.ROOT);
        return l.contains("odata=nometadata") ? "nometadata" : l.contains("odata=fullmetadata") ? "fullmetadata" : "minimalmetadata";
    }

    private void record(AzReq r) {
        if (metrics == null || r == null || "Unknown".equals(r.op)) {
            return;
        }
        long nanos = System.nanoTime() - r.startNanos;
        String backend = "default";
        List<String> h = registry.storeHosts(type);
        if (!h.isEmpty()) {
            backend = h.size() == 1 ? h.get(0) : String.join(",", h);
        }
        metrics.recordOperation("az" + service + "wire", backend, r.write ? SqlMetricsCollector.StatementKind.WRITE
                : SqlMetricsCollector.StatementKind.READ, r.op, nanos, nanos);
    }

    public void start() throws Exception {
        server.start();
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                sweep.run();
            } catch (RuntimeException e) {
                log.debug("azurewire {} sweep failed: {}", service, e.getMessage());
            }
        }, sweepSeconds, sweepSeconds, TimeUnit.SECONDS);
        log.info("warp azurewire ({}) listening on port {}", service, ((ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        sweeper.shutdownNow();
        server.stop();
    }
}
