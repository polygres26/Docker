package com.polygres.wire.http.admin;

import com.polygres.wire.config.ConfigStore;
import com.polygres.wire.core.QosControlStage;
import com.polygres.wire.core.StatsCollectorStage;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.function.Supplier;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal embedded Jetty server exposing {@code GET /metrics} as Prometheus text (see
 * {@link MetricsRenderer}). No auth — internal metrics-scrape endpoint, not customer-facing,
 * matching Omnigate's own posture for this endpoint. {@code qosStage} is nullable (QoS metrics
 * are simply omitted from the output when QoS is disabled).
 *
 * <p><b>{@code GET /config}</b>: read-only introspection of this node's currently-applied {@code
 * polywire_config} version — {@code currentVersionSupplier} is a {@code Supplier} (not a
 * snapshotted value) so every request reflects whatever this node has most recently applied via
 * {@code ConfigStore}'s LISTEN/NOTIFY callback, including a change picked up seconds ago with no
 * restart. Deliberately still no auth, same posture as {@code /metrics} — this is version/payload
 * introspection, not a write surface; see {@code ConfigStore}'s javadoc for why a real write
 * endpoint is out of scope for this pass. Nullable {@code currentVersionSupplier} (config store
 * not wired up) renders a small JSON body saying so rather than 404ing.
 */
public final class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final Server server;

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage) {
        this(port, statsStage, qosStage, null);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier) {
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws java.io.IOException {
                if ("/metrics".equals(target)) {
                    String body = MetricsRenderer.render(statsStage, qosStage);
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("text/plain; version=0.0.4; charset=utf-8");
                    response.getWriter().write(body);
                    baseRequest.setHandled(true);
                    return;
                }
                if ("/config".equals(target)) {
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("application/json; charset=utf-8");
                    response.getWriter().write(renderConfig(currentVersionSupplier));
                    baseRequest.setHandled(true);
                    return;
                }
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                baseRequest.setHandled(true);
            }
        });
    }

    private static String renderConfig(Supplier<ConfigStore.Version> currentVersionSupplier) {
        if (currentVersionSupplier == null) {
            return "{\"configStoreEnabled\":false}";
        }
        ConfigStore.Version version = currentVersionSupplier.get();
        if (version == null) {
            return "{\"configStoreEnabled\":true,\"version\":null}";
        }
        return "{\"configStoreEnabled\":true,\"version\":" + version.version()
                + ",\"createdAt\":\"" + version.createdAt() + "\""
                + ",\"payload\":" + version.payload().toJson() + "}";
    }

    public void start() throws Exception {
        server.start();
        log.info("polywire /metrics endpoint listening on port {}", ((org.eclipse.jetty.server.ServerConnector)
                server.getConnectors()[0]).getPort());
    }

}
