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
 * ({@code POLYWIRE_ADMIN_TOKEN}). When a {@link ConfigStore} is also supplied, {@code /api/config}
 * exposes every field of {@link PolyWireConfig} (backends, router rules, QoS limits, ACL rules,
 * OAuth settings, ...) as one GET/PUT(-partial) resource -- a PUT merges the given fields onto the
 * latest version and appends a new {@code polywire_config} row, the same LISTEN/NOTIFY path every
 * config field already reloads through. Callers only send the fields they're changing; everything
 * else carries forward from the current version untouched. When a {@link com.polygres.wire.core.BackendRegistry}
 * is also supplied, {@code /api/backends} lists every configured backend and {@code /api/backends/{name}/tables},
 * {@code /api/backends/{name}/tables/{schema}/{table}/columns}, and {@code /api/backends/{name}/query}
 * expose {@link com.polygres.wire.core.DataExplorer}'s object browser and ad-hoc query console --
 * see that class's javadoc for why this deliberately bypasses the wire pipeline (Firewall/ACL
 * don't apply to it) and why that's fine given it's gated the same way as everything else here.
 * {@code POST /api/backends/test} probes a candidate jdbcUrl/user/password (never persisted --
 * pure connectivity check) via {@link com.polygres.wire.core.BackendConnectivityTest}; {@code
 * POST /api/backends/{name}/test} runs the same probe against an already-configured backend's
 * live credentials, for a "is this still reachable" re-check. When {@code backendRegistry} is
 * supplied, {@code GET /api/queues} also lists every sqswire queue (depth, FIFO/DLQ attributes,
 * resolved shard backend) and {@code DELETE /api/queues/{name}} removes one -- see
 * {@link #handleQueues}.
 * Meant to be called server-to-server (e.g. by PolyAdvisor's own backend, proxying on behalf of
 * an already-authenticated admin session), not directly from a browser -- there's no CORS
 * handling and no session/cookie machinery here on purpose.
 */
public final class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);
    private static final Pattern FIREWALL_RULE_ID_PATH = Pattern.compile("^/api/firewall-rules/(\\d+)$");
    private static final Pattern BACKEND_TABLES_PATH = Pattern.compile("^/api/backends/([^/]+)/tables$");
    private static final Pattern BACKEND_COLUMNS_PATH = Pattern.compile("^/api/backends/([^/]+)/tables/([^/]+)/([^/]+)/columns$");
    private static final Pattern BACKEND_QUERY_PATH = Pattern.compile("^/api/backends/([^/]+)/query$");
    private static final Pattern BACKEND_TEST_NAMED_PATH = Pattern.compile("^/api/backends/([^/]+)/test$");

    private final Server server;
    private final com.polygres.wire.sqswire.PgQueueStore queueStore;
    private final com.polygres.wire.audit.AuditLog auditLog;

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
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, null);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.polygres.wire.core.BackendRegistry backendRegistry) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, null, null);
    }

    /**
     * Full constructor -- adds the {@link DialectTranslationStage} reference {@code /api/llm-config}
     * needs to hot-apply a PUT without waiting for the next {@code polywire_config} LISTEN/NOTIFY
     * round-trip, and {@code adminWebDir} (see {@code POLYWIRE_ADMIN_WEB_DIR} in {@code Main}), the
     * built {@code wire/web} SPA's {@code dist/} directory. When set, static assets are served by
     * {@link SpaResourceHandler} for anything this handler doesn't claim -- same "embedded Jetty
     * does both jobs" approach as advisor's {@code AdvisorHttpServer}/{@code SpaResourceHandler}.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, null);
    }

    /**
     * As the full constructor above, plus {@code options} -- when non-null, enables
     * {@code GET /api/nodes} (deployment-topology visibility, see {@link com.polygres.wire.config.NodeRegistry}).
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.polygres.wire.server.ServerOptions options) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, null);
    }

    /**
     * As the full constructor above, plus {@code mcpMetrics} -- the shared
     * {@link com.polygres.wire.mcp.McpMetricsCollector} instance {@code Main} also passes to
     * {@code PolyWireMcpServer}, so both read/write the same per-tool call counts. {@code null}
     * (every other constructor's default) means MCP metrics are omitted from both
     * {@code /api/metrics/summary} and {@code /metrics} -- harmless, not an error, for any caller
     * that genuinely has no MCP server running.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.polygres.wire.server.ServerOptions options, com.polygres.wire.mcp.McpMetricsCollector mcpMetrics) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, mcpMetrics, null);
    }

    /**
     * As the full constructor above, plus {@code captureBuffer} -- when non-null, enables
     * {@code GET /api/capture} (this instance's in-memory {@link com.polygres.wire.capture.WorkloadCaptureBuffer},
     * see that class and {@code WorkloadCaptureStage}). {@code null} (every other constructor's
     * default) means the route is absent, same "omitted, not an error" convention as every other
     * optional dependency here.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.polygres.wire.server.ServerOptions options, com.polygres.wire.mcp.McpMetricsCollector mcpMetrics,
            com.polygres.wire.capture.WorkloadCaptureBuffer captureBuffer) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, mcpMetrics,
                captureBuffer, null);
    }

    /**
     * As the full constructor above, plus {@code auditLog} -- when non-null, enables
     * {@code GET /api/audit} (this process's recent {@link com.polygres.wire.audit.AuditEvent}s,
     * most-recent-first; the durable, hash-chained store when {@code POLYWIRE_AUDIT_LOG_DB} is
     * configured, the in-memory ring buffer otherwise). {@code null} (every other constructor's
     * default) means the route is absent, same "omitted, not an error" convention as every other
     * optional dependency here.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.polygres.wire.core.BackendRegistry backendRegistry,
            com.polygres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.polygres.wire.server.ServerOptions options, com.polygres.wire.mcp.McpMetricsCollector mcpMetrics,
            com.polygres.wire.capture.WorkloadCaptureBuffer captureBuffer,
            com.polygres.wire.audit.AuditLog auditLog) {
        String adminToken = System.getenv("POLYWIRE_ADMIN_TOKEN");
        this.auditLog = auditLog;
        // Reuses the same live backendRegistry sqswire itself routes through -- a separate
        // PgQueueStore instance (its own small ensured-table cache, nothing else stateful) rather
        // than threading sqswire's own store across process wiring just for this read-only page.
        this.queueStore = backendRegistry == null ? null : new com.polygres.wire.sqswire.PgQueueStore(backendRegistry);
        this.server = new Server(port);
        boolean servesSpa = adminWebDir != null && !adminWebDir.isBlank()
                && java.nio.file.Files.isDirectory(java.nio.file.Path.of(adminWebDir));
        if (adminWebDir != null && !adminWebDir.isBlank() && !servesSpa) {
            log.warn("POLYWIRE_ADMIN_WEB_DIR={} is not a directory -- serving API only", adminWebDir);
        }
        AbstractHandler api = new AbstractHandler() {
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
                    String body = MetricsRenderer.render(statsStage, qosStage, mcpMetrics);
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("text/plain; version=0.0.4; charset=utf-8");
                    response.getWriter().write(body);
                    baseRequest.setHandled(true);
                    return;
                }
                if ("/config".equals(target)) {
                    // Unlike /metrics (Prometheus counters, no secrets), this returns the full
                    // config -- including the backends field's embedded password and
                    // awsIamCredentials -- decrypted. Gated like every other config-bearing
                    // route, not left open; nothing internal ever relied on this being public.
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("application/json; charset=utf-8");
                    response.getWriter().write(renderConfig(currentVersionSupplier));
                    baseRequest.setHandled(true);
                    return;
                }
                if ("/api/metrics/summary".equals(target)) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("application/json; charset=utf-8");
                    response.getWriter().write(renderMetricsSummary(statsStage, mcpMetrics));
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
                if (configStore != null && "/api/config".equals(target)) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleConfig(request, response, configStore);
                    baseRequest.setHandled(true);
                    return;
                }
                if (backendRegistry != null && target.startsWith("/api/backends")) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleBackends(target, request, response, backendRegistry);
                    baseRequest.setHandled(true);
                    return;
                }
                if (queueStore != null && target.startsWith("/api/queues")) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleQueues(target, request, response, queueStore);
                    baseRequest.setHandled(true);
                    return;
                }
                if (options != null && "/api/nodes".equals(target)) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleNodes(request, response, options);
                    baseRequest.setHandled(true);
                    return;
                }
                if (captureBuffer != null && "/api/capture".equals(target)) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleCapture(request, response, captureBuffer);
                    baseRequest.setHandled(true);
                    return;
                }
                if (auditLog != null && "/api/audit".equals(target)) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleAudit(request, response, auditLog);
                    baseRequest.setHandled(true);
                    return;
                }
                if (configStore != null && "/api/llm-config".equals(target)) {
                    if (!bearerTokenValid(request, adminToken)) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write("{\"error\":\"missing or invalid admin token\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleLlmConfig(request, response, configStore, dialectTranslationStage);
                    baseRequest.setHandled(true);
                    return;
                }
                if (servesSpa) {
                    // Not one of ours -- leave unhandled so the HandlerList's next handler (the
                    // SPA static-file server) gets a chance, instead of 404ing it ourselves. See
                    // SpaResourceHandler's javadoc.
                    return;
                }
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                baseRequest.setHandled(true);
            }
        };
        if (servesSpa) {
            org.eclipse.jetty.server.handler.HandlerList handlers = new org.eclipse.jetty.server.handler.HandlerList();
            handlers.setHandlers(new org.eclipse.jetty.server.Handler[] {api, new SpaResourceHandler(adminWebDir)});
            server.setHandler(handlers);
        } else {
            server.setHandler(api);
        }
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

    private static void handleConfig(HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            if ("GET".equals(request.getMethod())) {
                PolyWireConfig current = configStore.readLatest()
                        .map(ConfigStore.Version::payload)
                        .orElseGet(PolyWireConfig::fromEnvDefaults);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(current.toJson());
            } else if ("PUT".equals(request.getMethod())) {
                JsonObject body = readJsonBody(request);
                PolyWireConfig current = configStore.readLatest()
                        .map(ConfigStore.Version::payload)
                        .orElseGet(PolyWireConfig::fromEnvDefaults);
                PolyWireConfig updated = new PolyWireConfig(
                        field(body, "qosRatePerSec", current.qosRatePerSec()),
                        field(body, "qosBurst", current.qosBurst()),
                        field(body, "qosMaxWaitMs", current.qosMaxWaitMs()),
                        field(body, "qosClassLimits", current.qosClassLimits()),
                        field(body, "qosPoolWaitThreshold", current.qosPoolWaitThreshold()),
                        field(body, "cacheTables", current.cacheTables()),
                        field(body, "cacheTtlMs", current.cacheTtlMs()),
                        field(body, "backends", current.backends()),
                        field(body, "shardBackends", current.shardBackends()),
                        field(body, "routerSchemaRules", current.routerSchemaRules()),
                        field(body, "routerPredicateRules", current.routerPredicateRules()),
                        field(body, "routerValueShardRules", current.routerValueShardRules()),
                        field(body, "routerShardTables", current.routerShardTables()),
                        field(body, "rollupDefinitionsYaml", current.rollupDefinitionsYaml()),
                        field(body, "aclRules", current.aclRules()),
                        field(body, "aclPpv2Enabled", current.aclPpv2Enabled()),
                        field(body, "aclTrustedProxies", current.aclTrustedProxies()),
                        field(body, "oauthIssuer", current.oauthIssuer()),
                        field(body, "oauthAudience", current.oauthAudience()),
                        field(body, "oauthUserIdClaim", current.oauthUserIdClaim()),
                        field(body, "oauthRolesClaim", current.oauthRolesClaim()),
                        field(body, "awsIamCredentials", current.awsIamCredentials()),
                        field(body, "llmProvider", current.llmProvider()),
                        field(body, "llmApiKey", current.llmApiKey()),
                        field(body, "llmBaseUrl", current.llmBaseUrl()),
                        field(body, "llmModel", current.llmModel()));
                // Validate the pieces that have a real parser before committing a new version --
                // fail loud on the request instead of publishing a version every listener chokes on.
                com.polygres.wire.acl.ClientAcl.parse(updated.aclRules());
                long version = configStore.write(updated);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"ok\":true,\"version\":" + version + "}");
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\":\"no such route\"}");
            }
        } catch (java.sql.SQLException e) {
            log.warn("config admin API: database error", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    /**
     * {@code GET}/{@code PUT /api/llm-config} -- the dialect-translation LLM fallback's runtime
     * settings (provider/apiKey/baseUrl/model), stored as four more fields on the same
     * {@code polywire_config} row {@code /api/config} manages (see {@link PolyWireConfig}). Kept
     * as its own route rather than folded into {@code /api/config} because the response shape is
     * deliberately different: {@code GET} never echoes the decrypted {@code apiKey} back (only
     * {@code apiKeySet: boolean}), mirroring PolyAdvisor's {@code LlmSettingsStore}/{@code
     * WireConnectionStore} "the browser never has the real secret to send back" convention. A
     * PUT with a blank/omitted {@code apiKey} keeps whatever key is already stored -- same
     * convention as {@code WireConnectionStore#save}.
     *
     * <p>On a successful PUT, also calls {@code dialectTranslationStage.reconfigureLlm(...)}
     * directly so the change is live immediately for this process, rather than only after the
     * next {@code polywire_config} LISTEN/NOTIFY round-trip lands (every other process listening
     * on the same config still picks it up that way).
     */
    private static void handleLlmConfig(HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore, com.polygres.wire.core.DialectTranslationStage dialectTranslationStage)
            throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            if ("GET".equals(request.getMethod())) {
                PolyWireConfig current = configStore.readLatest()
                        .map(ConfigStore.Version::payload)
                        .orElseGet(PolyWireConfig::fromEnvDefaults);
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"provider\":" + jsonString(current.llmProvider())
                        + ",\"baseUrl\":" + jsonString(current.llmBaseUrl())
                        + ",\"model\":" + jsonString(current.llmModel())
                        + ",\"apiKeySet\":" + (current.llmApiKey() != null && !current.llmApiKey().isBlank())
                        + "}");
            } else if ("PUT".equals(request.getMethod())) {
                JsonObject body = readJsonBody(request);
                PolyWireConfig current = configStore.readLatest()
                        .map(ConfigStore.Version::payload)
                        .orElseGet(PolyWireConfig::fromEnvDefaults);
                String newProvider = field(body, "provider", current.llmProvider());
                if (newProvider != null && !newProvider.isBlank()
                        && !newProvider.equalsIgnoreCase("openai")
                        && !newProvider.equalsIgnoreCase("custom")
                        && !newProvider.equalsIgnoreCase("none")) {
                    throw new IllegalArgumentException("provider must be 'openai', 'custom', or 'none'");
                }
                // Blank/omitted apiKey keeps the currently-stored key -- the browser never has the
                // real decrypted key to send back in the first place, so "unchanged" must be the
                // default, not "blank it out". Only a non-blank apiKey in the body overwrites it.
                String newApiKey = body.has("apiKey") && !optionalString(body, "apiKey").isBlank()
                        ? optionalString(body, "apiKey")
                        : current.llmApiKey();
                String newBaseUrl = field(body, "baseUrl", current.llmBaseUrl());
                String newModel = field(body, "model", current.llmModel());
                PolyWireConfig updated = new PolyWireConfig(
                        current.qosRatePerSec(), current.qosBurst(), current.qosMaxWaitMs(),
                        current.qosClassLimits(), current.qosPoolWaitThreshold(),
                        current.cacheTables(), current.cacheTtlMs(),
                        current.backends(), current.shardBackends(),
                        current.routerSchemaRules(), current.routerPredicateRules(),
                        current.routerValueShardRules(), current.routerShardTables(),
                        current.rollupDefinitionsYaml(),
                        current.aclRules(), current.aclPpv2Enabled(), current.aclTrustedProxies(),
                        current.oauthIssuer(), current.oauthAudience(), current.oauthUserIdClaim(),
                        current.oauthRolesClaim(), current.awsIamCredentials(),
                        newProvider, newApiKey, newBaseUrl, newModel);
                long version = configStore.write(updated);
                if (dialectTranslationStage != null) {
                    dialectTranslationStage.reconfigureLlm(newProvider, newApiKey, newBaseUrl, newModel);
                }
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"ok\":true,\"version\":" + version + "}");
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\":\"no such route\"}");
            }
        } catch (java.sql.SQLException e) {
            log.warn("llm-config admin API: database error", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    private static String field(JsonObject body, String key, String fallback) {
        return body.has(key) ? optionalString(body, key) : fallback;
    }

    private static void handleBackends(String target, HttpServletRequest request, HttpServletResponse response,
            com.polygres.wire.core.BackendRegistry backendRegistry) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            Matcher tablesMatch = BACKEND_TABLES_PATH.matcher(target);
            Matcher columnsMatch = BACKEND_COLUMNS_PATH.matcher(target);
            Matcher queryMatch = BACKEND_QUERY_PATH.matcher(target);
            Matcher testNamedMatch = BACKEND_TEST_NAMED_PATH.matcher(target);

            if ("/api/backends/test".equals(target) && "POST".equals(request.getMethod())) {
                // Test-before-add: params the caller is considering, not anything already in
                // polywire_config -- this never touches BackendRegistry or writes anything.
                JsonObject body = readJsonBody(request);
                if (!body.has("jdbcUrl") || body.get("jdbcUrl").getAsString().isBlank()) {
                    throw new IllegalArgumentException("jdbcUrl is required");
                }
                var result = com.polygres.wire.core.BackendConnectivityTest.test(
                        body.get("jdbcUrl").getAsString(),
                        optionalString(body, "user"),
                        optionalString(body, "password"));
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"ok\":" + result.ok()
                        + ",\"message\":" + jsonString(result.message())
                        + ",\"tookMs\":" + result.tookMs()
                        + ",\"serverVersion\":" + jsonString(result.serverVersion()) + "}");
            } else if (testNamedMatch.matches() && "POST".equals(request.getMethod())) {
                // Re-check an already-configured backend -- same probe, but reading the
                // jdbcUrl/user/password straight out of the live registry instead of the request
                // body, so the Backends page can offer a "test" action per already-saved entry.
                com.polygres.wire.core.BackendTarget t = requireBackend(backendRegistry, testNamedMatch.group(1), response);
                if (t == null) return;
                var result = com.polygres.wire.core.BackendConnectivityTest.test(t.jdbcUrl(), t.user(), t.password());
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"ok\":" + result.ok()
                        + ",\"message\":" + jsonString(result.message())
                        + ",\"tookMs\":" + result.tookMs()
                        + ",\"serverVersion\":" + jsonString(result.serverVersion()) + "}");
            } else if ("/api/backends".equals(target) && "GET".equals(request.getMethod())) {
                StringBuilder json = new StringBuilder("[");
                boolean first = true;
                for (com.polygres.wire.core.BackendTarget t : backendRegistry.all()) {
                    if (!first) json.append(',');
                    first = false;
                    json.append("{\"name\":").append(jsonString(t.name()))
                            .append(",\"jdbcUrl\":").append(jsonString(t.jdbcUrl()))
                            .append(",\"dialect\":").append(jsonString(t.dialect() == null ? null : t.dialect().name()))
                            .append('}');
                }
                json.append(']');
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(json.toString());
            } else if (tablesMatch.matches() && "GET".equals(request.getMethod())) {
                com.polygres.wire.core.BackendTarget t = requireBackend(backendRegistry, tablesMatch.group(1), response);
                if (t == null) return;
                StringBuilder json = new StringBuilder("[");
                boolean first = true;
                for (var table : com.polygres.wire.core.DataExplorer.listTables(t)) {
                    if (!first) json.append(',');
                    first = false;
                    json.append("{\"schema\":").append(jsonString(table.schema()))
                            .append(",\"name\":").append(jsonString(table.name()))
                            .append(",\"type\":").append(jsonString(table.type()))
                            .append('}');
                }
                json.append(']');
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(json.toString());
            } else if (columnsMatch.matches() && "GET".equals(request.getMethod())) {
                com.polygres.wire.core.BackendTarget t = requireBackend(backendRegistry, columnsMatch.group(1), response);
                if (t == null) return;
                StringBuilder json = new StringBuilder("[");
                boolean first = true;
                for (var col : com.polygres.wire.core.DataExplorer.listColumns(t, columnsMatch.group(2), columnsMatch.group(3))) {
                    if (!first) json.append(',');
                    first = false;
                    json.append("{\"name\":").append(jsonString(col.name()))
                            .append(",\"type\":").append(jsonString(col.type()))
                            .append(",\"nullable\":").append(col.nullable())
                            .append('}');
                }
                json.append(']');
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(json.toString());
            } else if (queryMatch.matches() && "POST".equals(request.getMethod())) {
                com.polygres.wire.core.BackendTarget t = requireBackend(backendRegistry, queryMatch.group(1), response);
                if (t == null) return;
                JsonObject body = readJsonBody(request);
                if (!body.has("sql") || body.get("sql").getAsString().isBlank()) {
                    throw new IllegalArgumentException("sql is required");
                }
                var result = com.polygres.wire.core.DataExplorer.runQuery(t, body.get("sql").getAsString());
                StringBuilder json = new StringBuilder("{\"columns\":[");
                boolean first = true;
                for (String c : result.columns()) {
                    if (!first) json.append(',');
                    first = false;
                    json.append(jsonString(c));
                }
                json.append("],\"rows\":[");
                first = true;
                for (var row : result.rows()) {
                    if (!first) json.append(',');
                    first = false;
                    json.append('[');
                    boolean firstCell = true;
                    for (Object cell : row) {
                        if (!firstCell) json.append(',');
                        firstCell = false;
                        json.append(cell == null ? "null" : jsonString(String.valueOf(cell)));
                    }
                    json.append(']');
                }
                json.append("],\"rowCount\":").append(result.rowCount())
                        .append(",\"truncated\":").append(result.truncated())
                        .append(",\"tookMs\":").append(result.tookMs())
                        .append('}');
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(json.toString());
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\":\"no such route\"}");
            }
        } catch (java.sql.SQLException e) {
            log.warn("backends admin API: database error", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    private static com.polygres.wire.core.BackendTarget requireBackend(com.polygres.wire.core.BackendRegistry backendRegistry,
            String name, HttpServletResponse response) throws java.io.IOException {
        com.polygres.wire.core.BackendTarget t = backendRegistry.get(name);
        if (t == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\":\"no such backend: " + name.replace("\"", "'") + "\"}");
            return null;
        }
        return t;
    }

    private static final Pattern QUEUE_NAME_PATH = Pattern.compile("^/api/queues/([^/]+)$");

    /**
     * Read-only admin view of sqswire's queues -- {@code GET /api/queues} lists every queue with
     * its live depth (visible/in-flight message counts from {@link com.polygres.wire.sqswire.PgQueueStore#countMessages}),
     * FIFO/DLQ/redrive attributes, and which shard backend it currently resolves to (so the page
     * can show sharding is actually splitting queues across backends, same idea as the Backends
     * page's per-backend view). {@code DELETE /api/queues/{name}} drops a queue entirely -- the
     * one mutating action this route offers, useful for clearing out a demo/test queue from the
     * UI without a psql session.
     */
    private static void handleQueues(String target, HttpServletRequest request, HttpServletResponse response,
            com.polygres.wire.sqswire.PgQueueStore queueStore) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            Matcher nameMatch = QUEUE_NAME_PATH.matcher(target);
            if ("/api/queues".equals(target) && "GET".equals(request.getMethod())) {
                StringBuilder json = new StringBuilder("[");
                boolean first = true;
                for (String name : queueStore.listQueues()) {
                    if (!first) json.append(',');
                    first = false;
                    var counts = queueStore.countMessages(name);
                    var attrs = queueStore.queueAttributes(name);
                    json.append("{\"name\":").append(jsonString(name))
                            .append(",\"visible\":").append(counts.visible())
                            .append(",\"inFlight\":").append(counts.inFlight())
                            .append(",\"fifo\":").append(attrs.fifo())
                            .append(",\"visibilityTimeout\":").append(attrs.visibilityTimeout())
                            .append(",\"dlqQueueName\":").append(jsonString(attrs.dlqQueueName()))
                            .append(",\"maxReceiveCount\":").append(attrs.maxReceiveCount() == null ? "null" : attrs.maxReceiveCount())
                            .append(",\"backend\":").append(jsonString(queueStore.resolveBackendFor(name)))
                            .append('}');
                }
                json.append(']');
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(json.toString());
            } else if (nameMatch.matches() && "DELETE".equals(request.getMethod())) {
                queueStore.deleteQueue(nameMatch.group(1));
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"ok\":true}");
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\":\"no such route\"}");
            }
        } catch (java.sql.SQLException e) {
            log.warn("queues admin API: database error", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
        }
    }

    /**
     * {@code GET /api/capture?since=<localSeq>&limit=<n>} -- a page of this instance's own
     * {@link com.polygres.wire.capture.WorkloadCaptureBuffer}, oldest-first, {@code localSeq}
     * strictly greater than {@code since} (default 0), capped at {@code limit} (default 1000).
     * {@code WorkloadReplayer} calls this on every live node (discovered via {@code /api/nodes})
     * and merges the results by {@code wallClock} to reconstruct one global arrival order across
     * the fleet -- see {@link com.polygres.wire.capture.WorkloadCaptureBuffer}'s javadoc.
     */
    private static void handleCapture(HttpServletRequest request, HttpServletResponse response,
            com.polygres.wire.capture.WorkloadCaptureBuffer captureBuffer) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        if (!"GET".equals(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\":\"no such route\"}");
            return;
        }
        long since = parseLongParam(request.getParameter("since"), 0);
        int limit = (int) parseLongParam(request.getParameter("limit"), 1000);
        StringBuilder json = new StringBuilder("[");
        boolean first = true;
        for (com.polygres.wire.capture.WorkloadCaptureBuffer.Entry e : captureBuffer.since(since, limit)) {
            if (!first) json.append(',');
            first = false;
            json.append("{\"localSeq\":").append(e.localSeq())
                    .append(",\"wallClock\":").append(jsonString(e.wallClock().toString()))
                    .append(",\"nodeId\":").append(jsonString(captureBuffer.nodeId()))
                    .append(",\"protocol\":").append(jsonString(e.protocol()))
                    .append(",\"tenantId\":").append(jsonString(e.tenantId()))
                    .append(",\"sqlText\":").append(jsonString(e.sqlText()))
                    .append(",\"targetBackend\":").append(jsonString(e.targetBackend()))
                    .append(",\"bindParams\":[");
            java.util.List<Object> params = e.bindParams();
            for (int i = 0; i < params.size(); i++) {
                if (i > 0) json.append(',');
                Object p = params.get(i);
                json.append(p == null ? "null" : jsonString(String.valueOf(p)));
            }
            json.append("]}");
        }
        json.append(']');
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(json.toString());
    }

    /** {@code GET /api/audit?limit=N} (default 100) -- most-recent-first, from the durable
     * hash-chained DB sink when {@code POLYWIRE_AUDIT_LOG_DB} is configured, the in-memory ring
     * buffer otherwise (see {@link com.polygres.wire.audit.AuditLog#recent}). */
    private static void handleAudit(HttpServletRequest request, HttpServletResponse response,
            com.polygres.wire.audit.AuditLog auditLog) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        if (!"GET".equals(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\":\"no such route\"}");
            return;
        }
        int limit = (int) parseLongParam(request.getParameter("limit"), 100);
        StringBuilder json = new StringBuilder("[");
        boolean first = true;
        for (com.polygres.wire.audit.AuditEvent event : auditLog.recent(limit)) {
            if (!first) json.append(',');
            first = false;
            json.append(com.polygres.wire.audit.AuditLog.toJson(event));
        }
        json.append(']');
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(json.toString());
    }

    private static long parseLongParam(String raw, long defaultValue) {
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * {@code GET /api/nodes} -- one row per live-or-recently-live polywire instance, from
     * {@code polywire_nodes} (see {@link com.polygres.wire.config.NodeRegistry}). Sorted by zone
     * then host for a stable, readable order; {@code status} is {@code "up"} if the row's
     * heartbeat is within the last 30s, else {@code "stale"}.
     */
    private static void handleNodes(HttpServletRequest request, HttpServletResponse response,
            com.polygres.wire.server.ServerOptions options) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        if (!"GET".equals(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\":\"no such route\"}");
            return;
        }
        try {
            StringBuilder json = new StringBuilder("[");
            boolean first = true;
            for (com.polygres.wire.config.NodeRegistry.NodeRow row : com.polygres.wire.config.NodeRegistry.listAll(options)) {
                if (!first) json.append(',');
                first = false;
                json.append("{\"nodeId\":").append(jsonString(row.nodeId().toString()))
                        .append(",\"host\":").append(jsonString(row.host()))
                        .append(",\"adminPort\":").append(row.adminPort())
                        .append(",\"zone\":").append(jsonString(row.zone()))
                        .append(",\"version\":").append(jsonString(row.version()))
                        .append(",\"startedAt\":").append(jsonString(row.startedAt().toString()))
                        .append(",\"lastHeartbeat\":").append(jsonString(row.lastHeartbeat().toString()))
                        .append(",\"status\":").append(jsonString(row.status()))
                        .append('}');
            }
            json.append(']');
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(json.toString());
        } catch (java.sql.SQLException e) {
            log.warn("nodes admin API: database error", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
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

    private static String renderMetricsSummary(StatsCollectorStage statsStage,
            com.polygres.wire.mcp.McpMetricsCollector mcpMetrics) {
        com.polygres.wire.core.SqlMetricsCollector.Snapshot snap = statsStage.sqlMetricsSnapshot();
        StringBuilder json = new StringBuilder("{");
        json.append("\"protocolCounts\":{");
        boolean first = true;
        for (var entry : snap.protocolCounts().entrySet()) {
            if (!first) json.append(',');
            first = false;
            json.append(jsonString(entry.getKey())).append(':').append(entry.getValue());
        }
        json.append('}');
        json.append(",\"totalReads\":").append(snap.totalReads());
        json.append(",\"totalWrites\":").append(snap.totalWrites());
        json.append(",\"totalOther\":").append(snap.totalOther());
        json.append(",\"readsPerSec\":").append(String.format(java.util.Locale.ROOT, "%.2f", snap.readsPerSec()));
        json.append(",\"writesPerSec\":").append(String.format(java.util.Locale.ROOT, "%.2f", snap.writesPerSec()));
        json.append(",\"avgRttMs\":").append(snap.avgRttMs() == null ? "null" : snap.avgRttMs());
        json.append(",\"rttSamples\":").append(snap.rttSamples());
        json.append(",\"topSql\":[");
        first = true;
        for (var s : snap.topSql()) {
            if (!first) json.append(',');
            first = false;
            json.append("{\"sql\":").append(jsonString(s.normalizedSql()))
                    .append(",\"calls\":").append(s.calls())
                    .append(",\"totalMs\":").append(s.totalMillis())
                    .append(",\"avgMs\":").append(s.avgMillis())
                    .append(",\"avgRttMs\":").append(s.avgRttMillis() == null ? "null" : s.avgRttMillis())
                    .append('}');
        }
        json.append("],\"byBackend\":[");
        first = true;
        for (var b : snap.byBackend()) {
            if (!first) json.append(',');
            first = false;
            json.append("{\"backend\":").append(jsonString(b.backend()))
                    .append(",\"calls\":").append(b.calls())
                    .append(",\"reads\":").append(b.reads())
                    .append(",\"writes\":").append(b.writes())
                    .append(",\"totalMs\":").append(b.totalMillis())
                    .append(",\"avgMs\":").append(b.avgMillis())
                    .append('}');
        }
        json.append("],\"mcpTools\":[");
        if (mcpMetrics != null) {
            boolean firstTool = true;
            for (var t : mcpMetrics.snapshot()) {
                if (!firstTool) json.append(',');
                firstTool = false;
                json.append("{\"tool\":").append(jsonString(t.toolName()))
                        .append(",\"calls\":").append(t.calls())
                        .append(",\"errors\":").append(t.errors())
                        .append(",\"totalMs\":").append(t.totalMillis())
                        .append(",\"avgMs\":").append(t.avgMillis())
                        .append('}');
            }
        }
        json.append("],\"rttByOutcome\":[");
        first = true;
        for (var r : statsStage.sqlMetrics().rttOutcomeSnapshot()) {
            if (!first) json.append(',');
            first = false;
            json.append("{\"protocol\":").append(jsonString(r.protocol()))
                    .append(",\"outcome\":").append(jsonString(r.outcome()))
                    .append(",\"calls\":").append(r.calls())
                    .append(",\"totalMs\":").append(r.totalMillis())
                    .append(",\"avgMs\":").append(r.avgMillis())
                    .append('}');
        }
        json.append("]}");
        return json.toString();
    }

    public void start() throws Exception {
        server.start();
        log.info("polywire /metrics endpoint listening on port {}", ((org.eclipse.jetty.server.ServerConnector)
                server.getConnectors()[0]).getPort());
    }

}
