package com.nexagres.advisor.http;

import com.nexagres.advisor.http.auth.AdminAuth;
import com.nexagres.advisor.http.auth.AuthGuard;
import com.nexagres.advisor.http.auth.LoginRoute;
import com.nexagres.advisor.http.auth.LogoutRoute;
import com.nexagres.advisor.http.auth.SessionRoute;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded HTTP server for the Advisor API. Same raw-Handler-API-plus-route-table shape as
 * Omnigate's {@code com.omnigate.http.OmniGateHttpServer} -- one process-wide handler dispatching
 * by path, which is all this route count needs.
 *
 * <p>Routing is exact-match-first, then longest-registered-prefix -- {@code /api/connections} is
 * registered once and owns everything under it ({@link ConnectionsRoute} does its own path
 * parsing for the id/sub-resource shape, see its javadoc), rather than every id/sub-path
 * combination needing its own table entry.
 *
 * <p>Everything except {@code /api/health}, {@code /api/login}, and {@code /api/session} requires
 * a valid admin session (see {@link AuthGuard}) -- this is an admin tool, not a public API.
 *
 * <p><b>SPA hosting</b>: this class only ever claims {@code /api/*} -- see {@link #handle}. When
 * {@code NEXAGRES_ADVISOR_WEB_DIR} is set (see {@link #main}), it's wired into a Jetty {@code
 * HandlerList} ahead of {@link SpaResourceHandler}, which serves the built {@code advisor/web}
 * SPA for everything this class doesn't claim -- no separate nginx/static-file server required,
 * Jetty (already this module's embedded HTTP server) does both jobs in one process.
 */
public class AdvisorHttpServer extends AbstractHandler {

    private static final Logger log = LoggerFactory.getLogger(AdvisorHttpServer.class);

    private final Map<String, RouteHandler> routes = new LinkedHashMap<>();

    public AdvisorHttpServer() {
        AdminAuth auth = new AdminAuth();

        routes.put("/api/health", (req, res) -> {
            res.setContentType("application/json");
            res.getWriter().write("{\"status\":\"ok\"}");
        });
        routes.put("/api/login", new LoginRoute(auth));
        routes.put("/api/session", new SessionRoute(auth));
        routes.put("/api/logout", AuthGuard.require(auth, new LogoutRoute(auth)));

        routes.put("/api/scan", AuthGuard.require(auth, new ScanRoute()));
        routes.put("/api/workload", AuthGuard.require(auth, new WorkloadRoute()));
        routes.put("/api/summarize", AuthGuard.require(auth, new SummarizeRoute()));
        routes.put("/api/connections", AuthGuard.require(auth, new ConnectionsRoute()));
        routes.put("/api/llm-settings", AuthGuard.require(auth, new LlmSettingsRoute()));
        routes.put("/api/reports", AuthGuard.require(auth, new ReportsRoute()));
        routes.put("/api/migration/status", AuthGuard.require(auth, new MigrationStatusRoute()));
        routes.put("/api/migration/jobs", AuthGuard.require(auth, new MigrationJobsRoute()));
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException {
        if (!target.startsWith("/api/")) {
            // Not ours -- leave unhandled (don't 404 here) so a HandlerList's next handler, the
            // SPA static-file server, gets a chance. See this class's own SPA-hosting javadoc.
            return;
        }
        RouteHandler handler = resolve(target);
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

    /** Exact match first; otherwise the longest registered path that {@code target} starts with as a path segment (not just a string prefix). */
    private RouteHandler resolve(String target) {
        RouteHandler exact = routes.get(target);
        if (exact != null) return exact;
        return routes.entrySet().stream()
            .filter(e -> target.startsWith(e.getKey() + "/"))
            .max(Comparator.comparingInt(e -> e.getKey().length()))
            .map(Map.Entry::getValue)
            .orElse(null);
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("NEXAGRES_ADVISOR_PORT", "8090"));
        Server server = new Server(port);

        AdvisorHttpServer api = new AdvisorHttpServer();
        // NEXAGRES_ADVISOR_WEB_DIR: path to the built advisor/web SPA (its `dist/`). Opt-in --
        // unset means API-only, identical to this class's behavior before SpaResourceHandler
        // existed (run advisor/web separately via `npm run dev`, or front this with nginx, as
        // before). Set it and Jetty serves the SPA itself, no nginx/second container needed.
        String webDir = System.getenv("NEXAGRES_ADVISOR_WEB_DIR");
        if (webDir != null && !webDir.isBlank() && java.nio.file.Files.isDirectory(java.nio.file.Path.of(webDir))) {
            org.eclipse.jetty.server.handler.HandlerList handlers = new org.eclipse.jetty.server.handler.HandlerList();
            handlers.setHandlers(new org.eclipse.jetty.server.Handler[] {api, new SpaResourceHandler(webDir)});
            server.setHandler(handlers);
        } else {
            if (webDir != null && !webDir.isBlank()) {
                log.warn("NEXAGRES_ADVISOR_WEB_DIR={} is not a directory -- serving API only", webDir);
            }
            server.setHandler(api);
        }

        server.start();
        log.info("Nexagres Advisor listening on http://localhost:{}", port);
        server.join();
    }
}
