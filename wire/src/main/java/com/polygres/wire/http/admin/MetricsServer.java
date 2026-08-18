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

public final class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final Server server;

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage) {
        this(port, statsStage, qosStage, null, com.polygres.wire.acl.ConnectionGate.DISABLED);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier) {
        this(port, statsStage, qosStage, currentVersionSupplier, com.polygres.wire.acl.ConnectionGate.DISABLED);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate,
                com.polygres.wire.http.auth.AccessContextResolver.DISABLED);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth) {
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws java.io.IOException {
                if (!connectionGate.acceptHttp(request)) {
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    response.getWriter().write("forbidden");
                    baseRequest.setHandled(true);
                    return;
                }
                if (oauth.enforce(request, response) == null) {
                    baseRequest.setHandled(true);
                    return;
                }
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
