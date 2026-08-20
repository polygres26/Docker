package com.polygres.advisor.http;

import com.google.gson.Gson;
import com.polygres.advisor.wire.WireConnectionStore;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * {@code /api/wire/firewall-rules[/{id}]} -- proxies straight through to PolyWire's own
 * {@code /api/firewall-rules} admin API (see {@code com.polygres.wire.http.admin.MetricsServer}),
 * server-to-server, using the URL/token from {@link WireConnectionStore}. The browser only ever
 * talks to this Advisor route (already behind {@link com.polygres.advisor.http.auth.AuthGuard}'s
 * session check) -- it never sees PolyWire's admin token or calls PolyWire directly, so there's no
 * CORS to configure and no second credential to hand the browser.
 */
public class WireFirewallRulesRoute implements RouteHandler {

    private static final Gson GSON = new Gson();
    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final WireConnectionStore store = new WireConnectionStore();

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        WireConnectionStore.WireConnection conn = store.get();
        if (!conn.configured()) {
            writeError(response, 409, "PolyWire connection not configured yet -- set it on the Wire Settings page first.");
            return;
        }

        // /api/wire/firewall-rules[/{id}] -> forward everything after "/wire" to PolyWire's own
        // /api/... path, preserving method, query string, and body verbatim.
        String path = request.getRequestURI();
        int wireIdx = path.indexOf("/wire/");
        String upstreamPath = "/api/" + path.substring(wireIdx + "/wire/".length());
        String query = request.getQueryString();
        String upstreamUrl = conn.adminUrl().replaceAll("/$", "") + upstreamPath + (query != null ? "?" + query : "");

        try {
            HttpRequest.Builder upstreamRequest = HttpRequest.newBuilder(URI.create(upstreamUrl))
                    .timeout(Duration.ofSeconds(10))
                    .header("Authorization", "Bearer " + conn.adminToken());

            String method = request.getMethod().toUpperCase();
            if ("GET".equals(method) || "DELETE".equals(method)) {
                upstreamRequest.method(method, HttpRequest.BodyPublishers.noBody());
            } else {
                byte[] body = request.getInputStream().readAllBytes();
                upstreamRequest.header("Content-Type", "application/json")
                        .method(method, HttpRequest.BodyPublishers.ofByteArray(body));
            }

            HttpResponse<String> upstreamResponse = CLIENT.send(upstreamRequest.build(), HttpResponse.BodyHandlers.ofString());
            response.setStatus(upstreamResponse.statusCode());
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().write(upstreamResponse.body());
        } catch (java.net.ConnectException | java.net.http.HttpTimeoutException e) {
            writeError(response, 502, "Could not reach PolyWire's admin API at " + conn.adminUrl() + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writeError(response, 502, "Interrupted while calling PolyWire's admin API.");
        }
    }

    private void writeError(HttpServletResponse response, int status, String message) throws IOException {
        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(GSON.toJson(Map.of("error", message)));
    }
}
