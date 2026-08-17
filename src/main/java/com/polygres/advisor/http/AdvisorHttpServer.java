package com.polygres.advisor.http;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded HTTP server for the Advisor API. Same raw-Handler-API-plus-route-table shape as
 * Omnigate's {@code com.omnigate.http.OmniGateHttpServer} (~/Projects/Omnigate) -- one process
 * wide handler dispatching by exact path, which is all a handful of routes needs; expand to a
 * real router/servlet setup only if the route count grows enough to justify it.
 */
public class AdvisorHttpServer extends AbstractHandler {

    private static final Logger log = LoggerFactory.getLogger(AdvisorHttpServer.class);

    private final Map<String, RouteHandler> routes = new LinkedHashMap<>();

    public AdvisorHttpServer() {
        routes.put("/api/scan", new ScanRoute());
        routes.put("/api/health", (req, res) -> {
            res.setContentType("application/json");
            res.getWriter().write("{\"status\":\"ok\"}");
        });
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException {
        RouteHandler handler = routes.get(target);
        if (handler == null) {
            response.setStatus(404);
            baseRequest.setHandled(true);
            return;
        }
        try {
            handler.handle(request, response);
        } catch (Exception e) {
            log.error("Route {} failed", target, e);
            response.setStatus(500);
        }
        baseRequest.setHandled(true);
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("POLYGRES_ADVISOR_PORT", "8090"));
        Server server = new Server(port);
        server.setHandler(new AdvisorHttpServer());
        server.start();
        log.info("Polygres Advisor listening on http://localhost:{}", port);
        server.join();
    }
}
