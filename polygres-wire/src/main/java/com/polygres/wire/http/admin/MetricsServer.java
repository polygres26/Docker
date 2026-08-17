package com.polygres.wire.http.admin;

import com.polygres.wire.core.QosControlStage;
import com.polygres.wire.core.StatsCollectorStage;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
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
 */
public final class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final Server server;

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage) {
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws java.io.IOException {
                if (!"/metrics".equals(target)) {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    baseRequest.setHandled(true);
                    return;
                }
                String body = MetricsRenderer.render(statsStage, qosStage);
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("text/plain; version=0.0.4; charset=utf-8");
                response.getWriter().write(body);
                baseRequest.setHandled(true);
            }
        });
    }

    public void start() throws Exception {
        server.start();
        log.info("polywire /metrics endpoint listening on port {}", ((org.eclipse.jetty.server.ServerConnector)
                server.getConnectors()[0]).getPort());
    }

}
