package com.polygres.wire.http.admin;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.polygres.wire.config.ConfigStore;
import com.polygres.wire.config.FirewallRuleStore;
import com.polygres.wire.config.PolyWireConfig;
import com.polygres.wire.core.QosControlStage;
import com.polygres.wire.core.StatsCollectorStage;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The admin HTTP surface: {@code /metrics} (Prometheus text), {@code /config} (read-only current
 * config snapshot), and -- when a {@link FirewallRuleStore} is supplied -- a real CRUD API for SQL
 * Firewall rules under {@code /api/firewall-rules}, guarded by a bearer token
 * ({@code POLYWIRE_ADMIN_TOKEN}). When a {@link ConfigStore} is also supplied, {@code /api/acl-rules}
 * exposes the ACL (IP/CIDR allow/reject) rules living inside {@code polywire_config.aclRules} as
 * their own GET/PUT resource -- a write there appends a new {@code polywire_config} version, the
 * same LISTEN/NOTIFY path every other config field already reloads through. Meant to be called
 * server-to-server (e.g. by PolyAdvisor's own backend, proxying on behalf of an already-authenticated
 * admin session), not directly from a browser -- there's no CORS handling and no session/cookie
 * machinery here on purpose.
 */
public final class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);
    private static final Pattern FIREWALL_RULE_ID_PATH = Pattern.compile("^/api/firewall-rules/(\\d+)$");

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
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, null);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore, null);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore) {
        String adminToken = System.getenv("POLYWIRE_ADMIN_TOKEN");
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
                if (firewallRuleStore != null && target.startsWith("/api/firewall-rules")) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleFirewallRules(target, request, response, firewallRuleStore);
                    baseRequest.setHandled(true);
                    return;
                }
                if (configStore != null && "/api/acl-rules".equals(target)) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleAclRules(request, response, configStore);
                    baseRequest.setHandled(true);
                    return;
                }
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                baseRequest.setHandled(true);
            }
        });
    }

    private static boolean bearerTokenValid(HttpServletRequest request, String adminToken) {
        if (adminToken == null || adminToken.isBlank()) {
            // Opt-in like every other feature: unset means this whole API surface is disabled by
            // the caller never being able to authenticate -- not silently open.
            return false;
        }
        String header = request.getHeader("Authorization");
        if (header == null || !header.startsWith("Bearer ")) {
            return false;
        }
        return constantTimeEquals(adminToken, header.substring("Bearer ".length()));
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a.length() != b.length()) {
            return false;
        }
        int diff = 0;
        for (int i = 0; i < a.length(); i++) {
            diff |= a.charAt(i) ^ b.charAt(i);
        }
        return diff == 0;
    }

    private static void handleFirewallRules(String target, HttpServletRequest request, HttpServletResponse response,
            FirewallRuleStore store) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            Matcher idMatch = FIREWALL_RULE_ID_PATH.matcher(target);
            if ("/api/firewall-rules".equals(target) && "GET".equals(request.getMethod())) {
                writeRulesList(response, store);
            } else if ("/api/firewall-rules".equals(target) && "POST".equals(request.getMethod())) {
                JsonObject body = readJsonBody(request);
                long id = store.insert(
                        body.has("priority") ? body.get("priority").getAsInt() : 100,
                        requireAction(body),
                        optionalString(body, "statementType"),
                        optionalString(body, "tablePattern"),
                        optionalString(body, "sqlPattern"),
                        !body.has("enabled") || body.get("enabled").getAsBoolean(),
                        optionalString(body, "description"));
                response.setStatus(HttpServletResponse.SC_CREATED);
                response.getWriter().write("{\"id\":" + id + "}");
            } else if (idMatch.matches() && "PUT".equals(request.getMethod())) {
                long id = Long.parseLong(idMatch.group(1));
                JsonObject body = readJsonBody(request);
                boolean found = store.update(id,
                        body.has("priority") ? body.get("priority").getAsInt() : 100,
                        requireAction(body),
                        optionalString(body, "statementType"),
                        optionalString(body, "tablePattern"),
                        optionalString(body, "sqlPattern"),
                        !body.has("enabled") || body.get("enabled").getAsBoolean(),
                        optionalString(body, "description"));
                response.setStatus(found ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write(found ? "{\"ok\":true}" : "{\"error\":\"not found\"}");
            } else if (idMatch.matches() && "DELETE".equals(request.getMethod())) {
                long id = Long.parseLong(idMatch.group(1));
                boolean found = store.delete(id);
                response.setStatus(found ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write(found ? "{\"ok\":true}" : "{\"error\":\"not found\"}");
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\":\"no such route\"}");
            }
        } catch (java.sql.SQLException e) {
            log.warn("firewall-rules admin API: database error", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    private static void handleAclRules(HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            if ("GET".equals(request.getMethod())) {
                PolyWireConfig current = configStore.readLatest()
                        .map(ConfigStore.Version::payload)
                        .orElseGet(PolyWireConfig::fromEnvDefaults);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"aclRules\":" + jsonString(current.aclRules())
                        + ",\"aclPpv2Enabled\":" + jsonString(current.aclPpv2Enabled())
                        + ",\"aclTrustedProxies\":" + jsonString(current.aclTrustedProxies()) + "}");
            } else if ("PUT".equals(request.getMethod())) {
                JsonObject body = readJsonBody(request);
                PolyWireConfig current = configStore.readLatest()
                        .map(ConfigStore.Version::payload)
                        .orElseGet(PolyWireConfig::fromEnvDefaults);
                PolyWireConfig updated = new PolyWireConfig(
                        current.qosRatePerSec(), current.qosBurst(), current.qosMaxWaitMs(),
                        current.qosClassLimits(), current.qosPoolWaitThreshold(),
                        current.cacheTables(), current.cacheTtlMs(),
                        current.backends(), current.shardBackends(),
                        current.routerSchemaRules(), current.routerPredicateRules(),
                        current.routerValueShardRules(), current.routerShardTables(),
                        current.rollupDefinitionsYaml(),
                        body.has("aclRules") ? optionalString(body, "aclRules") : current.aclRules(),
                        body.has("aclPpv2Enabled") ? optionalString(body, "aclPpv2Enabled") : current.aclPpv2Enabled(),
                        body.has("aclTrustedProxies") ? optionalString(body, "aclTrustedProxies") : current.aclTrustedProxies(),
                        current.oauthIssuer(), current.oauthAudience(), current.oauthUserIdClaim(),
                        current.oauthRolesClaim(), current.awsIamCredentials());
                com.polygres.wire.acl.ClientAcl.parse(updated.aclRules());
                long version = configStore.write(updated);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"ok\":true,\"version\":" + version + "}");
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\":\"no such route\"}");
            }
        } catch (java.sql.SQLException e) {
            log.warn("acl-rules admin API: database error", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    private static void writeRulesList(HttpServletResponse response, FirewallRuleStore store)
            throws java.sql.SQLException, java.io.IOException {
        StringBuilder json = new StringBuilder("[");
        boolean first = true;
        for (FirewallRuleStore.AdminRow row : store.listAll()) {
            if (!first) {
                json.append(",");
            }
            first = false;
            json.append("{\"id\":").append(row.id())
                    .append(",\"priority\":").append(row.priority())
                    .append(",\"action\":").append(jsonString(row.action()))
                    .append(",\"statementType\":").append(jsonString(row.statementType()))
                    .append(",\"tablePattern\":").append(jsonString(row.tablePattern()))
                    .append(",\"sqlPattern\":").append(jsonString(row.sqlPattern()))
                    .append(",\"enabled\":").append(row.enabled())
                    .append(",\"description\":").append(jsonString(row.description()))
                    .append(",\"createdAt\":").append(jsonString(row.createdAt()))
                    .append("}");
        }
        json.append("]");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(json.toString());
    }

    private static String jsonString(String s) {
        return s == null ? "null" : "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String requireAction(JsonObject body) {
        if (!body.has("action")) {
            throw new IllegalArgumentException("action is required (allow or deny)");
        }
        String action = body.get("action").getAsString().toLowerCase(java.util.Locale.ROOT);
        if (!action.equals("allow") && !action.equals("deny")) {
            throw new IllegalArgumentException("action must be 'allow' or 'deny'");
        }
        return action;
    }

    private static String optionalString(JsonObject body, String key) {
        return body.has(key) && !body.get(key).isJsonNull() ? body.get(key).getAsString() : null;
    }

    private static JsonObject readJsonBody(HttpServletRequest request) throws java.io.IOException {
        try (var reader = request.getReader()) {
            return JsonParser.parseReader(reader).getAsJsonObject();
        }
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
