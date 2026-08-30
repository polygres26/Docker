package com.nexagres.wire.http.admin;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.config.ConfigStore;
import com.nexagres.wire.config.FirewallRuleStore;
import com.nexagres.wire.config.PolyWireConfig;
import com.nexagres.wire.core.AccessContext;
import com.nexagres.wire.core.QosControlStage;
import com.nexagres.wire.core.StatsCollectorStage;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The admin HTTP surface: {@code /metrics} (Prometheus text), {@code /config} (read-only current
 * config snapshot), and -- when a {@link FirewallRuleStore} is supplied -- a real CRUD API for SQL
 * Firewall rules under {@code /api/firewall-rules}.
 *
 * <p><b>Auth:</b> every route is gated one of two ways, and both work simultaneously -- neither
 * disables the other. The simple path, unchanged from before role support existed: a shared
 * bearer token ({@code POLYWIRE_ADMIN_TOKEN}) grants full read+write access. This stays fully
 * supported for developer testing, CI, and any customer who just wants one token -- no OIDC setup
 * required. The SSO path, opt-in via {@code POLYWIRE_OAUTH_ISSUER} (see
 * {@link com.nexagres.wire.http.auth.AccessContextResolver}, works against Okta, Entra ID, or any
 * OIDC-compliant IdP): a caller presents a real JWT instead, and its roles claim (name configured
 * via {@code POLYWIRE_OAUTH_ROLES_CLAIM}, e.g. an Okta group or an Entra ID app role) is checked
 * against {@code POLYWIRE_OAUTH_ADMIN_ROLES}/{@code POLYWIRE_OAUTH_VIEWER_ROLES} (comma-separated
 * role names, defaulting to {@code admin}/{@code viewer}) to grant read-only access (GET routes
 * only) or full read+write, respectively -- see {@link #resolveAdminRole}. When a customer wants
 * "read vs. change" access split by real identity instead of one shared secret, this is how.
 *
 * <p>When a {@link ConfigStore} is also supplied, {@code /api/config}
 * exposes every field of {@link PolyWireConfig} (backends, router rules, QoS limits, ACL rules,
 * OAuth settings, ...) as one GET/PUT(-partial) resource -- a PUT merges the given fields onto the
 * latest version and appends a new {@code polywire_config} row, the same LISTEN/NOTIFY path every
 * config field already reloads through. Callers only send the fields they're changing; everything
 * else carries forward from the current version untouched. When a {@link com.nexagres.wire.core.BackendRegistry}
 * is also supplied, {@code /api/backends} lists every configured backend and {@code /api/backends/{name}/tables},
 * {@code /api/backends/{name}/tables/{schema}/{table}/columns}, and {@code /api/backends/{name}/query}
 * expose {@link com.nexagres.wire.core.DataExplorer}'s object browser and ad-hoc query console --
 * see that class's javadoc for why this deliberately bypasses the wire pipeline (Firewall/ACL
 * don't apply to it) and why that's fine given it's gated the same way as everything else here.
 * {@code POST /api/backends/test} probes a candidate jdbcUrl/user/password (never persisted --
 * pure connectivity check) via {@link com.nexagres.wire.core.BackendConnectivityTest}; {@code
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
    private static final Pattern BACKEND_DRAIN_PATH = Pattern.compile("^/api/backends/([^/]+)/drain$");
    private static final Pattern BACKEND_UNDRAIN_PATH = Pattern.compile("^/api/backends/([^/]+)/undrain$");

    private final Server server;
    private final com.nexagres.wire.sqswire.PgQueueStore queueStore;
    private final com.nexagres.wire.audit.AuditLog auditLog;
    // Set after the delegating constructor call below (not a constructor param on every overload,
    // same "orthogonal, opt-in" reasoning RouterStage's own federation-support fields use) --
    // read at real request-handling time by the anonymous AbstractHandler below (an outer-instance
    // field access, not a captured local), so setting it after that handler object is built is
    // still safe: no request is ever handled before this constructor call returns.
    private com.nexagres.wire.core.SqlPlanStore federationPlanStore;
    // Same "set after construction, orthogonal and opt-in" reasoning as federationPlanStore above
    // -- QueryRepairStage doesn't exist yet at MetricsServer construction time in Main#main
    // either. Null means "query repair isn't wired into this pipeline" (a plain -1/-1 pair in the
    // summary, not an error) rather than every other constructor overload above needing yet
    // another parameter.
    private com.nexagres.wire.core.QueryRepairStage queryRepairStage;

    public void setQueryRepairStage(com.nexagres.wire.core.QueryRepairStage queryRepairStage) {
        this.queryRepairStage = queryRepairStage;
    }

    // Same "set after construction, orthogonal and opt-in" reasoning as queryRepairStage above.
    private com.nexagres.wire.core.AnomalyDetectionScheduler anomalyScheduler;

    public void setAnomalyScheduler(com.nexagres.wire.core.AnomalyDetectionScheduler anomalyScheduler) {
        this.anomalyScheduler = anomalyScheduler;
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage) {
        this(port, statsStage, qosStage, null, com.nexagres.wire.acl.ConnectionGate.DISABLED);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier) {
        this(port, statsStage, qosStage, currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate.DISABLED);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate,
                com.nexagres.wire.http.auth.AccessContextResolver.DISABLED);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, null);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore, null);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, null);
    }

    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry) {
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
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, null);
    }

    /**
     * As the full constructor above, plus {@code options} -- when non-null, enables
     * {@code GET /api/nodes} (deployment-topology visibility, see {@link com.nexagres.wire.config.NodeRegistry}).
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.nexagres.wire.server.ServerOptions options) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, null);
    }

    /**
     * As the full constructor above, plus {@code mcpMetrics} -- the shared
     * {@link com.nexagres.wire.mcp.McpMetricsCollector} instance {@code Main} also passes to
     * {@code PolyWireMcpServer}, so both read/write the same per-tool call counts. {@code null}
     * (every other constructor's default) means MCP metrics are omitted from both
     * {@code /api/metrics/summary} and {@code /metrics} -- harmless, not an error, for any caller
     * that genuinely has no MCP server running.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.nexagres.wire.server.ServerOptions options, com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, mcpMetrics, null);
    }

    /**
     * As the full constructor above, plus {@code captureBuffer} -- when non-null, enables
     * {@code GET /api/capture} (this instance's in-memory {@link com.nexagres.wire.capture.WorkloadCaptureBuffer},
     * see that class and {@code WorkloadCaptureStage}). {@code null} (every other constructor's
     * default) means the route is absent, same "omitted, not an error" convention as every other
     * optional dependency here.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.nexagres.wire.server.ServerOptions options, com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics,
            com.nexagres.wire.capture.WorkloadCaptureBuffer captureBuffer) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, mcpMetrics,
                captureBuffer, null);
    }

    /**
     * As the full constructor above, plus {@code auditLog} -- when non-null, enables
     * {@code GET /api/audit} (this process's recent {@link com.nexagres.wire.audit.AuditEvent}s,
     * most-recent-first; the durable, hash-chained store when {@code POLYWIRE_AUDIT_LOG_DB} is
     * configured, the in-memory ring buffer otherwise). {@code null} (every other constructor's
     * default) means the route is absent, same "omitted, not an error" convention as every other
     * optional dependency here.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.nexagres.wire.server.ServerOptions options, com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics,
            com.nexagres.wire.capture.WorkloadCaptureBuffer captureBuffer,
            com.nexagres.wire.audit.AuditLog auditLog) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, mcpMetrics,
                captureBuffer, auditLog, null);
    }

    /** As the full constructor below, plus {@code federationPlanStore} -- when non-null, exposes
     * {@code GET /api/federation/plans}: every {@link com.nexagres.wire.core.ShardJoinExecutor}/
     * {@link com.nexagres.wire.core.SchemaFederationStage} federated query's real captured
     * {@code EXPLAIN PLAN FOR} plan text, timing, row count, and success/failure -- the same
     * {@code V$SQL_PLAN}-style history the sibling Omnigate project's own admin API already
     * exposes. {@code null} (every other constructor's default): the route doesn't exist at all,
     * matching {@code POLYWIRE_FEDERATION_PLAN_HISTORY} being unset. */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.nexagres.wire.server.ServerOptions options, com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics,
            com.nexagres.wire.capture.WorkloadCaptureBuffer captureBuffer,
            com.nexagres.wire.audit.AuditLog auditLog, com.nexagres.wire.xa.XaRecoveryLog xaRecoveryLog,
            com.nexagres.wire.core.SqlPlanStore federationPlanStore) {
        this(port, statsStage, qosStage, currentVersionSupplier, connectionGate, oauth, firewallRuleStore,
                configStore, backendRegistry, dialectTranslationStage, adminWebDir, options, mcpMetrics,
                captureBuffer, auditLog, xaRecoveryLog);
        this.federationPlanStore = federationPlanStore;
    }

    /**
     * As the full constructor above, plus {@code xaRecoveryLog} -- when non-null, {@code
     * POST /api/backends/{name}/drain} refuses (409) to drain a backend with any unresolved
     * in-doubt XA branch against it (see {@link com.nexagres.wire.xa.XaRecoveryLog#hasUnresolvedFor}'s
     * javadoc for why), and the drain/undrain routes fan out to every other live node in {@code
     * polywire_nodes} (see {@link com.nexagres.wire.config.NodeRegistry}) instead of only mutating
     * this process's own in-memory {@code BackendRegistry} state -- drain/undrain state is
     * deliberately NOT propagated via {@code ConfigStore}/LISTEN-NOTIFY like routing config is
     * (it's an operational, not a config, fact), so without this fan-out a drain call would only
     * ever take effect on whichever single node happened to receive the HTTP request. {@code null}
     * (every other constructor's default) means neither behavior is active: drain never gates on
     * XA state and only ever mutates the local node, same as before this was added.
     */
    public MetricsServer(int port, StatsCollectorStage statsStage, QosControlStage qosStage,
            Supplier<ConfigStore.Version> currentVersionSupplier, com.nexagres.wire.acl.ConnectionGate connectionGate,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, FirewallRuleStore firewallRuleStore,
            ConfigStore configStore, com.nexagres.wire.core.BackendRegistry backendRegistry,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage, String adminWebDir,
            com.nexagres.wire.server.ServerOptions options, com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics,
            com.nexagres.wire.capture.WorkloadCaptureBuffer captureBuffer,
            com.nexagres.wire.audit.AuditLog auditLog, com.nexagres.wire.xa.XaRecoveryLog xaRecoveryLog) {
        String adminToken = System.getenv("POLYWIRE_ADMIN_TOKEN");
        // Real per-user SSO on top of the token above, not instead of it -- POLYWIRE_ADMIN_TOKEN
        // stays fully supported for developer testing, CI, and any customer who just wants a
        // single shared secret; it always resolves to full ADMIN access, unchanged. When
        // POLYWIRE_OAUTH_ISSUER is also configured (Okta, Entra ID, or any OIDC-compliant IdP),
        // a caller can instead present a real JWT whose configured roles claim determines VIEWER
        // (read-only) vs ADMIN (read+write) access -- see resolveAdminRole/authorized below.
        Set<String> adminRoleNames = splitRoles(System.getenv("POLYWIRE_OAUTH_ADMIN_ROLES"), "admin");
        Set<String> viewerRoleNames = splitRoles(System.getenv("POLYWIRE_OAUTH_VIEWER_ROLES"), "viewer");
        this.auditLog = auditLog;
        // Reuses the same live backendRegistry sqswire itself routes through -- a separate
        // PgQueueStore instance (its own small ensured-table cache, nothing else stateful) rather
        // than threading sqswire's own store across process wiring just for this read-only page.
        this.queueStore = backendRegistry == null ? null : new com.nexagres.wire.sqswire.PgQueueStore(backendRegistry);
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
                // The shared POLYWIRE_ADMIN_TOKEN path is checked and, on a match, taken FIRST --
                // before oauth.enforce() ever runs -- so it keeps working unchanged even when SSO
                // is also configured. oauth.enforce() rejects any Authorization header that isn't
                // a parseable JWT once an issuer is set; without this ordering, presenting the
                // plain admin token with SSO enabled would 401 before ever reaching the token
                // check below. This is the "simple setup stays available" guarantee, not
                // incidental behavior.
                String authHeader = request.getHeader("Authorization");
                AccessContext accessContext;
                AdminRole role;
                if (bearerTokenValid(authHeader, adminToken)) {
                    role = AdminRole.ADMIN;
                    accessContext = AccessContext.ANONYMOUS;
                } else {
                    accessContext = oauth.enforce(request, response);
                    if (accessContext == null) {
                        baseRequest.setHandled(true);
                        return;
                    }
                    role = resolveAdminRole(authHeader, accessContext, adminToken, adminRoleNames, viewerRoleNames);
                }
                // Attributes mutating admin calls to a real person when the caller authenticated
                // via SSO (accessContext.userId(), e.g. an Okta/Entra email or sub claim) instead
                // of collapsing every action to "someone with the shared token." A caller using
                // the shared POLYWIRE_ADMIN_TOKEN has no per-user identity to attribute to -- that
                // limitation is recorded plainly as "shared-admin-token" rather than hidden. Reads
                // (GET/HEAD) aren't recorded here -- audit is for changes, not every poll.
                if (auditLog != null && role == AdminRole.ADMIN && target.startsWith("/api/")
                        && !"GET".equalsIgnoreCase(request.getMethod()) && !"HEAD".equalsIgnoreCase(request.getMethod())) {
                    recordAdminAction(auditLog, accessContext, request.getMethod(), target);
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
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
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
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("application/json; charset=utf-8");
                    response.getWriter().write(renderMetricsSummary(statsStage, mcpMetrics, queryRepairStage));
                    baseRequest.setHandled(true);
                    return;
                }
                if ("/api/anomalies".equals(target) && "GET".equals(request.getMethod())) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("application/json; charset=utf-8");
                    response.getWriter().write(renderAnomalies(anomalyScheduler));
                    baseRequest.setHandled(true);
                    return;
                }
                if (firewallRuleStore != null && target.startsWith("/api/firewall-rules")) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleFirewallRules(target, request, response, firewallRuleStore, dialectTranslationStage);
                    baseRequest.setHandled(true);
                    return;
                }
                if (configStore != null && "/api/config".equals(target)) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleConfig(request, response, configStore);
                    baseRequest.setHandled(true);
                    return;
                }
                if (configStore != null && "/api/qos-suggestions/draft".equals(target) && "POST".equals(request.getMethod())) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleQosSuggestionDraft(response, configStore, statsStage, dialectTranslationStage);
                    baseRequest.setHandled(true);
                    return;
                }
                if (configStore != null && "/api/rollup-suggestions/draft".equals(target) && "POST".equals(request.getMethod())) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleRollupSuggestionDraft(response, configStore, statsStage, dialectTranslationStage);
                    baseRequest.setHandled(true);
                    return;
                }
                if (configStore != null && backendRegistry != null && "/api/router-suggestions/draft".equals(target)
                        && "POST".equals(request.getMethod())) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleRouterSuggestionDraft(response, configStore, statsStage, dialectTranslationStage, backendRegistry);
                    baseRequest.setHandled(true);
                    return;
                }
                if (federationPlanStore != null && "/api/federation/plans".equals(target)) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    StringBuilder json = new StringBuilder("[");
                    boolean first = true;
                    for (com.nexagres.wire.core.SqlPlanStore.PlanEntry entry : federationPlanStore.snapshot()) {
                        if (!first) json.append(',');
                        first = false;
                        json.append("{\"planId\":").append(entry.planId())
                                .append(",\"capturedAt\":").append(jsonString(entry.capturedAt().toString()))
                                .append(",\"backends\":").append(jsonString(entry.backends()))
                                .append(",\"sqlText\":").append(jsonString(entry.sqlText()))
                                .append(",\"planText\":").append(jsonString(entry.planText()))
                                .append(",\"elapsedMillis\":").append(entry.elapsedMillis())
                                .append(",\"rowCount\":").append(entry.rowCount())
                                .append(",\"success\":").append(entry.success())
                                .append(",\"errorMessage\":").append(jsonString(entry.errorMessage()))
                                .append(",\"leafScans\":[");
                        boolean firstLeaf = true;
                        for (com.nexagres.wire.core.SqlPlanStore.LeafScanMetric leaf : entry.leafScans()) {
                            if (!firstLeaf) json.append(',');
                            firstLeaf = false;
                            json.append("{\"backend\":").append(jsonString(leaf.backend()))
                                    .append(",\"sqlText\":").append(jsonString(leaf.sqlText()))
                                    .append(",\"elapsedMillis\":").append(leaf.elapsedMillis())
                                    .append(",\"rowCount\":").append(leaf.rowCount())
                                    .append(",\"errorMessage\":").append(jsonString(leaf.errorMessage()))
                                    .append('}');
                        }
                        json.append("]}");
                    }
                    json.append(']');
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("application/json; charset=utf-8");
                    response.getWriter().write(json.toString());
                    baseRequest.setHandled(true);
                    return;
                }
                if (backendRegistry != null && target.startsWith("/api/backends")) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleBackends(target, request, response, backendRegistry, xaRecoveryLog, options, port,
                            request.getHeader("Authorization"));
                    baseRequest.setHandled(true);
                    return;
                }
                if (queueStore != null && target.startsWith("/api/queues")) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleQueues(target, request, response, queueStore);
                    baseRequest.setHandled(true);
                    return;
                }
                if (options != null && "/api/nodes".equals(target)) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleNodes(request, response, options);
                    baseRequest.setHandled(true);
                    return;
                }
                if (captureBuffer != null && "/api/capture".equals(target)) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleCapture(request, response, captureBuffer);
                    baseRequest.setHandled(true);
                    return;
                }
                if (auditLog != null && "/api/audit".equals(target)) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleAudit(request, response, auditLog);
                    baseRequest.setHandled(true);
                    return;
                }
                if (auditLog != null && "/api/mcp-audit/summarize".equals(target) && "POST".equals(request.getMethod())) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
                        baseRequest.setHandled(true);
                        return;
                    }
                    handleMcpAuditSummarize(request, response, auditLog, dialectTranslationStage);
                    baseRequest.setHandled(true);
                    return;
                }
                if (configStore != null && "/api/llm-config".equals(target)) {
                    if (!authorized(request.getMethod(), role)) {
                        response.setStatus(role == AdminRole.NONE ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN);
                        response.setContentType("application/json; charset=utf-8");
                        response.getWriter().write(role == AdminRole.NONE
                                ? "{\"error\":\"missing or invalid admin credentials\"}"
                                : "{\"error\":\"read-only access -- this operation requires the admin role\"}");
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

    /**
     * A caller's resolved admin-console access level for this request. {@code NONE} means neither
     * the shared {@code POLYWIRE_ADMIN_TOKEN} nor a real SSO identity with a recognized role was
     * presented. {@code VIEWER} is read-only (GET routes only); {@code ADMIN} is full read+write,
     * exactly like every admin request before role support existed.
     */
    enum AdminRole {
        NONE, VIEWER, ADMIN
    }

    /**
     * Resolves the caller's {@link AdminRole} for this request. The static {@code
     * POLYWIRE_ADMIN_TOKEN} bearer token (see {@link #bearerTokenValid}) always resolves to {@code
     * ADMIN} when present and valid -- this is deliberate, not a fallback to remove: developer
     * testing, CI, single-node/dev deployments, and any customer who simply wants one shared
     * secret keep working exactly as before, with no OIDC setup required. When no admin token is
     * presented (or none is configured), a real SSO identity from {@code accessContext} -- resolved
     * by {@link com.nexagres.wire.http.auth.AccessContextResolver} against Okta, Entra ID, or any
     * OIDC-compliant IdP -- is checked against the configured admin/viewer role-name sets (drawn
     * from the JWT's {@code POLYWIRE_OAUTH_ROLES_CLAIM} claim, e.g. an Okta group or an Entra ID
     * app role) to grant {@code ADMIN} or {@code VIEWER} instead. A caller authenticated via SSO
     * but carrying neither role name gets {@code NONE} -- default-deny, not silently treated as a
     * viewer.
     */
    static AdminRole resolveAdminRole(String authorizationHeader, AccessContext accessContext,
            String adminToken, Set<String> adminRoleNames, Set<String> viewerRoleNames) {
        if (bearerTokenValid(authorizationHeader, adminToken)) {
            return AdminRole.ADMIN;
        }
        if (accessContext != null && !accessContext.isAnonymous()) {
            if (accessContext.hasAnyRole(adminRoleNames)) {
                return AdminRole.ADMIN;
            }
            if (accessContext.hasAnyRole(viewerRoleNames)) {
                return AdminRole.VIEWER;
            }
        }
        return AdminRole.NONE;
    }

    /** GET/HEAD (read) requests need at least {@code VIEWER}; every other HTTP method (the
     * mutating routes -- config PUT, firewall rule CRUD, backend drain/undrain/query, queue
     * delete, LLM config PUT) needs full {@code ADMIN}. */
    /** Records one mutating admin call to {@code auditLog}, attributed to the real SSO identity
     * when one resolved this request, or to the literal {@code "shared-admin-token"} placeholder
     * when the caller used {@code POLYWIRE_ADMIN_TOKEN} instead (which carries no per-user
     * identity to attribute to). */
    static void recordAdminAction(com.nexagres.wire.audit.AuditLog auditLog, AccessContext accessContext,
            String httpMethod, String target) {
        String userId = accessContext == null || accessContext.isAnonymous() ? "shared-admin-token" : accessContext.userId();
        auditLog.record(com.nexagres.wire.audit.AuditEvent.of(
                com.nexagres.wire.audit.AuditEvent.Type.ADMIN_ACTION, userId, httpMethod + " " + target));
    }

    static boolean authorized(String httpMethod, AdminRole role) {
        if ("GET".equalsIgnoreCase(httpMethod) || "HEAD".equalsIgnoreCase(httpMethod)) {
            return role != AdminRole.NONE;
        }
        return role == AdminRole.ADMIN;
    }

    /** Splits a comma-separated env var into a role-name set, falling back to a single default
     * role name (e.g. {@code "admin"}/{@code "viewer"}) when unset -- so a customer who configures
     * OIDC but doesn't bother naming custom roles still gets sensible behavior by naming their IdP
     * group/app-role claim value exactly {@code admin} or {@code viewer}. */
    static Set<String> splitRoles(String envValue, String defaultRole) {
        if (envValue == null || envValue.isBlank()) {
            return Set.of(defaultRole);
        }
        return Arrays.stream(envValue.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    static boolean bearerTokenValid(String authorizationHeader, String adminToken) {
        if (adminToken == null || adminToken.isBlank()) {
            // Opt-in like every other feature: unset means this whole API surface is disabled by
            // the caller never being able to authenticate -- not silently open.
            return false;
        }
        if (authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
            return false;
        }
        return constantTimeEquals(adminToken, authorizationHeader.substring("Bearer ".length()));
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
            FirewallRuleStore store, com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            Matcher idMatch = FIREWALL_RULE_ID_PATH.matcher(target);
            if ("/api/firewall-rules/draft".equals(target) && "POST".equals(request.getMethod())) {
                handleFirewallRuleDraft(request, response, dialectTranslationStage);
            } else if ("/api/firewall-rules".equals(target) && "GET".equals(request.getMethod())) {
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

    /**
     * Turns a plain-English policy into a firewall-rule DRAFT via the LLM -- never inserts
     * anything. The response is a proposed {@code /api/firewall-rules} POST body an admin reviews
     * (and can edit) before submitting it through that existing, already-authorized endpoint; the
     * LLM only ever gets to propose structured data here, never touch the request path, and never
     * write to {@code polywire_firewall_rules} directly. Shares dialect translation's own LLM
     * provider config ({@link DialectTranslationStage#llmClient()}) rather than a second one.
     */
    private static void handleFirewallRuleDraft(HttpServletRequest request, HttpServletResponse response,
            com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage) throws java.io.IOException {
        com.nexagres.wire.core.TranslationLlmClient llmClient =
                dialectTranslationStage == null ? null : dialectTranslationStage.llmClient();
        if (llmClient == null) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write("{\"error\":\"no LLM provider configured -- set it via PUT /api/llm-config "
                    + "or the POLYWIRE_LLM_* env vars before drafting rules from plain English\"}");
            return;
        }
        JsonObject body;
        try {
            body = readJsonBody(request);
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"invalid JSON request body\"}");
            return;
        }
        String prompt = optionalString(body, "prompt");
        if (prompt == null || prompt.isBlank()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"missing required field: prompt\"}");
            return;
        }

        String rawLlmReply;
        try {
            rawLlmReply = llmClient.draftFirewallRule(prompt);
        } catch (Exception e) {
            log.warn("firewall-rule draft: LLM call failed: {}", e.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM request failed: " + jsonString(e.getMessage()) + "}");
            return;
        }

        JsonObject draft;
        try {
            draft = JsonParser.parseString(rawLlmReply).getAsJsonObject();
        } catch (RuntimeException e) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM did not return a valid JSON object\",\"raw\":"
                    + jsonString(rawLlmReply) + "}");
            return;
        }

        // Validate against the SAME semantics FirewallRuleStore/FirewallStage actually give these
        // fields at runtime (see readRules()/globToPattern()) -- action must be one the DB CHECK
        // constraint accepts, and sqlPattern is a real Java regex a hallucinating LLM can get
        // syntactically wrong (tablePattern is a glob, translated by escaping every non-'*'
        // character, so it can never fail to compile and needs no such check).
        String action = optionalString(draft, "action");
        if (action == null || !(action.equalsIgnoreCase("allow") || action.equalsIgnoreCase("deny"))) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM returned an invalid or missing action (must be "
                    + "\\\"allow\\\" or \\\"deny\\\")\",\"raw\":" + jsonString(rawLlmReply) + "}");
            return;
        }
        String sqlPattern = optionalString(draft, "sqlPattern");
        if (sqlPattern != null) {
            try {
                Pattern.compile(sqlPattern);
            } catch (java.util.regex.PatternSyntaxException e) {
                response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
                response.getWriter().write("{\"error\":\"LLM returned an sqlPattern that is not a valid regular "
                        + "expression: " + jsonString(e.getMessage()) + "\",\"raw\":" + jsonString(rawLlmReply) + "}");
                return;
            }
        }
        int priority = draft.has("priority") && draft.get("priority").isJsonPrimitive() && draft.get("priority").getAsJsonPrimitive().isNumber()
                ? draft.get("priority").getAsInt() : 100;
        boolean enabled = !draft.has("enabled") || (draft.get("enabled").isJsonPrimitive() && draft.get("enabled").getAsJsonPrimitive().isBoolean()
                ? draft.get("enabled").getAsBoolean() : true);
        String description = optionalString(draft, "description");

        JsonObject normalized = new JsonObject();
        normalized.addProperty("action", action.toLowerCase(java.util.Locale.ROOT));
        normalized.addProperty("priority", priority);
        normalized.addProperty("statementType", optionalString(draft, "statementType"));
        normalized.addProperty("tablePattern", optionalString(draft, "tablePattern"));
        normalized.addProperty("sqlPattern", sqlPattern);
        normalized.addProperty("enabled", enabled);
        normalized.addProperty("description", description == null ? prompt : description);

        JsonObject responseBody = new JsonObject();
        responseBody.add("draft", normalized);
        responseBody.addProperty("applied", false);
        responseBody.addProperty("note", "This rule has NOT been created. Review it, edit any field if "
                + "needed, then POST it to /api/firewall-rules to actually add it.");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(responseBody.toString());
    }

    /**
     * Proposes ONE targeted QoS rate-limit change via the LLM -- never writes to {@code
     * polywire_config}. The response's {@code qosClassLimitsIfApplied}/{@code
     * qosRatePerSecIfApplied} etc. fields are exactly what an admin would paste into a
     * {@code PUT /api/config} body to actually apply it -- that existing, already-authorized
     * endpoint is the only write path, same "LLM proposes structured data, a human applies it
     * through the endpoint that already existed" shape {@link #handleFirewallRuleDraft} uses.
     *
     * <p>Context given to the LLM is per-BACKEND load ({@code StatsCollectorStage}'s own
     * snapshot) -- there is no per-workload-CLASS throughput tracked anywhere in this codebase
     * today (QoS buckets key on workload class, but {@code SqlMetricsCollector} never has), so a
     * suggestion here is necessarily backend-level evidence applied to a class-level knob, not a
     * precise class-level measurement. Disclosed in the prompt itself, not hidden.
     */
    private static void handleQosSuggestionDraft(HttpServletResponse response, ConfigStore configStore,
            StatsCollectorStage statsStage, com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage)
            throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        com.nexagres.wire.core.TranslationLlmClient llmClient =
                dialectTranslationStage == null ? null : dialectTranslationStage.llmClient();
        if (llmClient == null) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write("{\"error\":\"no LLM provider configured -- set it via PUT /api/llm-config "
                    + "or the POLYWIRE_LLM_* env vars before requesting QoS tuning suggestions\"}");
            return;
        }

        PolyWireConfig current;
        try {
            current = configStore.readLatest().map(ConfigStore.Version::payload).orElseGet(PolyWireConfig::fromEnvDefaults);
        } catch (java.sql.SQLException e) {
            log.warn("qos-suggestion draft: could not read current config", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
            return;
        }

        StringBuilder context = new StringBuilder();
        context.append("Current default limit: ratePerSecond=").append(current.qosRatePerSec())
                .append(", burstCapacity=").append(current.qosBurst())
                .append(", maxWaitMillis=").append(current.qosMaxWaitMs()).append('\n');
        context.append("Current per-class limits (class:rate:burst:maxWait): ")
                .append(current.qosClassLimits() == null || current.qosClassLimits().isBlank() ? "(none)" : current.qosClassLimits())
                .append('\n');
        context.append("Pool wait threshold (threads awaiting a connection before treating a backend as "
                + "saturated): ").append(current.qosPoolWaitThreshold() == null ? "(disabled)" : current.qosPoolWaitThreshold()).append('\n');
        context.append("Recent load by backend (calls, reads, writes, avg execution ms):\n");
        for (var b : statsStage.sqlMetricsSnapshot().byBackend()) {
            context.append("- ").append(b.backend()).append(": ").append(b.calls()).append(" calls, ")
                    .append(b.reads()).append(" reads, ").append(b.writes()).append(" writes, avg ")
                    .append(b.avgMillis()).append("ms\n");
        }

        String rawLlmReply;
        try {
            rawLlmReply = llmClient.draftQosTuning(context.toString());
        } catch (Exception e) {
            log.warn("qos-suggestion draft: LLM call failed: {}", e.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM request failed: " + jsonString(e.getMessage()) + "\"}");
            return;
        }

        JsonObject draft;
        try {
            draft = JsonParser.parseString(rawLlmReply).getAsJsonObject();
        } catch (RuntimeException e) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM did not return a valid JSON object\",\"raw\":"
                    + jsonString(rawLlmReply) + "}");
            return;
        }

        String target = optionalString(draft, "target");
        Double ratePerSecond = draft.has("ratePerSecond") && draft.get("ratePerSecond").isJsonPrimitive()
                && draft.get("ratePerSecond").getAsJsonPrimitive().isNumber() ? draft.get("ratePerSecond").getAsDouble() : null;
        Double burstCapacity = draft.has("burstCapacity") && draft.get("burstCapacity").isJsonPrimitive()
                && draft.get("burstCapacity").getAsJsonPrimitive().isNumber() ? draft.get("burstCapacity").getAsDouble() : null;
        Long maxWaitMillis = draft.has("maxWaitMillis") && draft.get("maxWaitMillis").isJsonPrimitive()
                && draft.get("maxWaitMillis").getAsJsonPrimitive().isNumber() ? draft.get("maxWaitMillis").getAsLong() : null;
        if (target == null || target.isBlank() || ratePerSecond == null || ratePerSecond <= 0
                || burstCapacity == null || burstCapacity <= 0 || maxWaitMillis == null || maxWaitMillis < 0) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM returned an incomplete or invalid tuning proposal "
                    + "(target/ratePerSecond/burstCapacity/maxWaitMillis)\",\"raw\":" + jsonString(rawLlmReply) + "}");
            return;
        }
        String rationale = optionalString(draft, "rationale");

        JsonObject responseBody = new JsonObject();
        JsonObject normalized = new JsonObject();
        normalized.addProperty("target", target);
        normalized.addProperty("ratePerSecond", ratePerSecond);
        normalized.addProperty("burstCapacity", burstCapacity);
        normalized.addProperty("maxWaitMillis", maxWaitMillis);
        normalized.addProperty("rationale", rationale);
        responseBody.add("draft", normalized);

        if ("default".equalsIgnoreCase(target)) {
            responseBody.addProperty("qosRatePerSecIfApplied", String.valueOf(ratePerSecond));
            responseBody.addProperty("qosBurstIfApplied", String.valueOf(burstCapacity));
            responseBody.addProperty("qosMaxWaitMsIfApplied", String.valueOf(maxWaitMillis));
        } else {
            var classLimits = new java.util.LinkedHashMap<>(com.nexagres.wire.core.QosControlStage.parseClassLimitsSpec(
                    current.qosClassLimits(), maxWaitMillis));
            classLimits.put(target, new com.nexagres.wire.core.QosControlStage.ClassLimit(ratePerSecond, burstCapacity, maxWaitMillis));
            responseBody.addProperty("qosClassLimitsIfApplied",
                    com.nexagres.wire.core.QosControlStage.formatClassLimitsSpec(classLimits));
        }
        responseBody.addProperty("applied", false);
        responseBody.addProperty("note", "This has NOT been applied. Review it, then PUT the "
                + "*IfApplied field(s) above into the matching field(s) of /api/config to actually apply it.");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(responseBody.toString());
    }

    /**
     * Proposes ONE new {@code RollupStage} pre-aggregation definition via the LLM -- never writes
     * to {@code polywire_config}. Validated by literally running the candidate through {@code
     * RollupConfig.parse} (the real parser {@code Main}'s own config-reload path uses, not a
     * second copy of its grammar), merged into the rest of the current definitions unchanged via
     * {@code RollupConfig.toYaml}, and returned as {@code rollupDefinitionsYamlIfApplied} -- exactly
     * what an admin pastes into {@code PUT /api/config}'s {@code rollupDefinitionsYaml} field to
     * actually apply it. Same "LLM proposes, a human applies through the endpoint that already
     * existed" shape {@link #handleFirewallRuleDraft}/{@link #handleQosSuggestionDraft} use.
     */
    private static void handleRollupSuggestionDraft(HttpServletResponse response, ConfigStore configStore,
            StatsCollectorStage statsStage, com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage)
            throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        com.nexagres.wire.core.TranslationLlmClient llmClient =
                dialectTranslationStage == null ? null : dialectTranslationStage.llmClient();
        if (llmClient == null) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write("{\"error\":\"no LLM provider configured -- set it via PUT /api/llm-config "
                    + "or the POLYWIRE_LLM_* env vars before requesting rollup suggestions\"}");
            return;
        }

        PolyWireConfig current;
        java.util.List<com.nexagres.wire.rollup.RollupDefinition> existingRollups;
        try {
            current = configStore.readLatest().map(ConfigStore.Version::payload).orElseGet(PolyWireConfig::fromEnvDefaults);
            existingRollups = com.nexagres.wire.rollup.RollupConfig.parse(current.rollupDefinitionsYaml());
        } catch (java.sql.SQLException e) {
            log.warn("rollup-suggestion draft: could not read current config", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
            return;
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":\"current rollupDefinitionsYaml does not parse: "
                    + e.getMessage().replace("\"", "'") + "\"}");
            return;
        }

        StringBuilder context = new StringBuilder();
        context.append("Existing rollup definitions:\n");
        if (existingRollups.isEmpty()) {
            context.append("(none)\n");
        } else {
            for (var def : existingRollups) {
                context.append("- ").append(def.name()).append(": ").append(def.definingSql()).append('\n');
            }
        }
        context.append("Recent expensive/frequent SQL (normalized, calls, total ms):\n");
        for (var s : statsStage.sqlMetricsSnapshot().topSql()) {
            context.append("- ").append(s.normalizedSql()).append(" (").append(s.calls()).append(" calls, ")
                    .append(s.totalMillis()).append("ms total)\n");
        }

        String rawLlmReply;
        try {
            rawLlmReply = llmClient.draftRollupSuggestion(context.toString());
        } catch (Exception e) {
            log.warn("rollup-suggestion draft: LLM call failed: {}", e.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM request failed: " + jsonString(e.getMessage()) + "\"}");
            return;
        }

        JsonObject draft;
        try {
            draft = JsonParser.parseString(rawLlmReply).getAsJsonObject();
        } catch (RuntimeException e) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM did not return a valid JSON object\",\"raw\":"
                    + jsonString(rawLlmReply) + "}");
            return;
        }

        if (!draft.has("name") || draft.get("name").isJsonNull()) {
            response.setStatus(HttpServletResponse.SC_OK);
            JsonObject nothing = new JsonObject();
            nothing.addProperty("draft", (String) null);
            nothing.addProperty("note", "the LLM found nothing in recent SQL worth pre-aggregating");
            response.getWriter().write(nothing.toString());
            return;
        }

        String candidateYaml;
        try {
            String name = draft.get("name").getAsString();
            String backend = optionalString(draft, "backend");
            if (backend == null) {
                backend = "default";
            }
            String sourceTable = draft.has("sourceTable") ? draft.get("sourceTable").getAsString() : null;
            var groupBy = new java.util.ArrayList<String>();
            if (draft.has("groupBy") && draft.get("groupBy").isJsonArray()) {
                draft.get("groupBy").getAsJsonArray().forEach(e -> groupBy.add(e.getAsString()));
            }
            var aggregations = new java.util.ArrayList<String>();
            if (draft.has("aggregations") && draft.get("aggregations").isJsonArray()) {
                draft.get("aggregations").getAsJsonArray().forEach(e -> aggregations.add(e.getAsString()));
            }
            int refreshMinutes = draft.has("refreshIntervalMinutes") ? draft.get("refreshIntervalMinutes").getAsInt() : 0;
            int stalenessMinutes = draft.has("maxStalenessMinutes") ? draft.get("maxStalenessMinutes").getAsInt() : 0;

            String candidateSingleYaml = com.nexagres.wire.rollup.RollupConfig.toYaml(java.util.List.of(
                    new com.nexagres.wire.rollup.RollupDefinition(name, backend, sourceTable,
                            groupBy, aggregations, refreshMinutes, stalenessMinutes)));
            java.util.List<com.nexagres.wire.rollup.RollupDefinition> parsedNew =
                    com.nexagres.wire.rollup.RollupConfig.parse(candidateSingleYaml);
            var candidateList = new java.util.ArrayList<>(existingRollups);
            candidateList.addAll(parsedNew);
            candidateYaml = com.nexagres.wire.rollup.RollupConfig.toYaml(candidateList);
        } catch (RuntimeException e) {
            // Covers both a malformed draft (missing/wrong-typed field -> Gson exception) and a
            // draft that's real JSON but fails RollupConfig's own grammar (bad name, bad
            // aggregation expression shape, non-positive interval) -- IllegalArgumentException is
            // exactly what RollupConfig.parse throws for the latter.
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM returned an invalid rollup definition: "
                    + e.getMessage().replace("\"", "'") + "\",\"raw\":" + jsonString(rawLlmReply) + "}");
            return;
        }

        JsonObject responseBody = new JsonObject();
        responseBody.add("draft", draft);
        responseBody.addProperty("rollupDefinitionsYamlIfApplied", candidateYaml);
        responseBody.addProperty("applied", false);
        responseBody.addProperty("note", "This has NOT been applied. Review it, then PUT "
                + "rollupDefinitionsYamlIfApplied's value into /api/config's rollupDefinitionsYaml field "
                + "to actually apply it.");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(responseBody.toString());
    }

    /**
     * Proposes ONE new per-table hash-sharding rule via the LLM -- never writes to {@code
     * polywire_config}. Never proposes a backend that isn't real: the LLM is given exactly the
     * currently-configured backend names as context, and the draft is rejected outright if it
     * names anything else -- an LLM can hallucinate a plausible-sounding backend name even when
     * told the real list, so this is checked in code, not trusted from the prompt alone. The
     * response's {@code routerTableShardsIfApplied} field is exactly what an admin pastes into
     * the existing {@code PUT /api/config}'s {@code routerTableShards} field to actually apply it,
     * same shape every other drafting feature in this series uses.
     */
    private static void handleRouterSuggestionDraft(HttpServletResponse response, ConfigStore configStore,
            StatsCollectorStage statsStage, com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage,
            com.nexagres.wire.core.BackendRegistry backendRegistry) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        com.nexagres.wire.core.TranslationLlmClient llmClient =
                dialectTranslationStage == null ? null : dialectTranslationStage.llmClient();
        if (llmClient == null) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write("{\"error\":\"no LLM provider configured -- set it via PUT /api/llm-config "
                    + "or the POLYWIRE_LLM_* env vars before requesting router/sharding suggestions\"}");
            return;
        }

        PolyWireConfig current;
        try {
            current = configStore.readLatest().map(ConfigStore.Version::payload).orElseGet(PolyWireConfig::fromEnvDefaults);
        } catch (java.sql.SQLException e) {
            log.warn("router-suggestion draft: could not read current config", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\":" + jsonString(e.getMessage()) + "}");
            return;
        }

        java.util.Set<String> configuredBackends = new java.util.LinkedHashSet<>();
        for (var target : backendRegistry.all()) {
            configuredBackends.add(target.name());
        }

        StringBuilder context = new StringBuilder();
        context.append("Configured backends: ").append(String.join(", ", configuredBackends)).append('\n');
        context.append("Current routerTableShards rules (table:strategy:column:backends): ")
                .append(current.routerTableShards() == null || current.routerTableShards().isBlank()
                        ? "(none)" : current.routerTableShards())
                .append('\n');
        context.append("Recent load by backend (calls, reads, writes, avg execution ms):\n");
        for (var b : statsStage.sqlMetricsSnapshot().byBackend()) {
            context.append("- ").append(b.backend()).append(": ").append(b.calls()).append(" calls, ")
                    .append(b.reads()).append(" reads, ").append(b.writes()).append(" writes, avg ")
                    .append(b.avgMillis()).append("ms\n");
        }

        String rawLlmReply;
        try {
            rawLlmReply = llmClient.draftTableShardSuggestion(context.toString());
        } catch (Exception e) {
            log.warn("router-suggestion draft: LLM call failed: {}", e.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM request failed: " + jsonString(e.getMessage()) + "\"}");
            return;
        }

        JsonObject draft;
        try {
            draft = JsonParser.parseString(rawLlmReply).getAsJsonObject();
        } catch (RuntimeException e) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM did not return a valid JSON object\",\"raw\":"
                    + jsonString(rawLlmReply) + "}");
            return;
        }

        if (!draft.has("table") || draft.get("table").isJsonNull()) {
            response.setStatus(HttpServletResponse.SC_OK);
            JsonObject nothing = new JsonObject();
            nothing.addProperty("draft", (String) null);
            nothing.addProperty("note", "the LLM found nothing in recent load worth sharding");
            response.getWriter().write(nothing.toString());
            return;
        }

        String table = optionalString(draft, "table");
        String shardColumn = optionalString(draft, "shardColumn");
        java.util.List<String> backends = new java.util.ArrayList<>();
        if (draft.has("backends") && draft.get("backends").isJsonArray()) {
            draft.get("backends").getAsJsonArray().forEach(e -> backends.add(e.getAsString()));
        }
        // Grammar delimiters this entry will be embedded in -- table:strategy:column:backend,backend
        // pipe-separated from any other entry -- so none of these three fields can contain them
        // without corrupting every OTHER rule in the same spec, not just this new one.
        String grammarChars = "|:;";
        boolean tableOk = table != null && !table.isBlank() && table.chars().noneMatch(c -> grammarChars.indexOf(c) >= 0);
        boolean columnOk = shardColumn != null && !shardColumn.isBlank()
                && shardColumn.chars().noneMatch(c -> grammarChars.indexOf(c) >= 0);
        boolean backendsOk = backends.size() >= 2 && backends.stream()
                .allMatch(b -> b != null && !b.isBlank() && configuredBackends.contains(b) && b.chars().noneMatch(c -> grammarChars.indexOf(c) >= 0));
        if (!tableOk || !columnOk || !backendsOk) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM returned an invalid sharding proposal -- table/"
                    + "shardColumn must be non-blank and free of ':'/'|'/';' , and backends must be 2 or "
                    + "more names from the real configured list (" + String.join(", ", configuredBackends)
                    + ")\",\"raw\":" + jsonString(rawLlmReply) + "}");
            return;
        }
        String existingSpec = current.routerTableShards();
        if (existingSpec != null && !existingSpec.isBlank()) {
            for (String entry : existingSpec.split("\\|")) {
                String existingTable = entry.split(":", 2)[0].trim();
                if (existingTable.equalsIgnoreCase(table)) {
                    response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
                    response.getWriter().write("{\"error\":\"table \\\"" + table.replace("\"", "'")
                            + "\\\" already has a routerTableShards rule -- refusing to propose a duplicate\"}");
                    return;
                }
            }
        }

        String newEntry = table + ":hash:" + shardColumn + ":" + String.join(",", backends);
        String candidateSpec = existingSpec == null || existingSpec.isBlank() ? newEntry : existingSpec + "|" + newEntry;
        // Real end-to-end confirmation using the actual runtime parser -- RouterStage's own
        // tableShardSpec parsing silently DROPS a malformed entry rather than throwing (unlike
        // RollupConfig.parse), so the only reliable check is confirming the parsed rule count
        // actually grew by exactly one, not assuming our own deterministic checks above were
        // the only way this could go wrong.
        int expectedCount = (existingSpec == null || existingSpec.isBlank() ? 0 : existingSpec.split("\\|").length) + 1;
        int actualCount = com.nexagres.wire.core.RouterStage.fromConfig(null, null, null, null, candidateSpec, backendRegistry)
                .tableShardRules().size();
        if (actualCount != expectedCount) {
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"the candidate routerTableShards entry failed to parse "
                    + "correctly -- refusing to propose it\",\"raw\":" + jsonString(rawLlmReply) + "}");
            return;
        }

        JsonObject responseBody = new JsonObject();
        responseBody.add("draft", draft);
        responseBody.addProperty("routerTableShardsIfApplied", candidateSpec);
        responseBody.addProperty("applied", false);
        responseBody.addProperty("note", "This has NOT been applied. Review it, then PUT "
                + "routerTableShardsIfApplied's value into /api/config's routerTableShards field to "
                + "actually apply it.");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(responseBody.toString());
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
                        field(body, "routerTableShards", current.routerTableShards()),
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
                com.nexagres.wire.acl.ClientAcl.parse(updated.aclRules());
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
            ConfigStore configStore, com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage)
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
                        current.routerValueShardRules(), current.routerShardTables(), current.routerTableShards(),
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
            com.nexagres.wire.core.BackendRegistry backendRegistry, com.nexagres.wire.xa.XaRecoveryLog xaRecoveryLog,
            com.nexagres.wire.server.ServerOptions options, int selfAdminPort, String forwardedAuthHeader)
            throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            Matcher tablesMatch = BACKEND_TABLES_PATH.matcher(target);
            Matcher columnsMatch = BACKEND_COLUMNS_PATH.matcher(target);
            Matcher queryMatch = BACKEND_QUERY_PATH.matcher(target);
            Matcher testNamedMatch = BACKEND_TEST_NAMED_PATH.matcher(target);
            Matcher drainMatch = BACKEND_DRAIN_PATH.matcher(target);
            Matcher undrainMatch = BACKEND_UNDRAIN_PATH.matcher(target);

            if ("/api/backends/test".equals(target) && "POST".equals(request.getMethod())) {
                // Test-before-add: params the caller is considering, not anything already in
                // polywire_config -- this never touches BackendRegistry or writes anything.
                JsonObject body = readJsonBody(request);
                if (!body.has("jdbcUrl") || body.get("jdbcUrl").getAsString().isBlank()) {
                    throw new IllegalArgumentException("jdbcUrl is required");
                }
                var result = com.nexagres.wire.core.BackendConnectivityTest.test(
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
                com.nexagres.wire.core.BackendTarget t = requireBackend(backendRegistry, testNamedMatch.group(1), response);
                if (t == null) return;
                var result = com.nexagres.wire.core.BackendConnectivityTest.test(t.jdbcUrl(), t.user(), t.password());
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"ok\":" + result.ok()
                        + ",\"message\":" + jsonString(result.message())
                        + ",\"tookMs\":" + result.tookMs()
                        + ",\"serverVersion\":" + jsonString(result.serverVersion()) + "}");
            } else if ("/api/backends".equals(target) && "GET".equals(request.getMethod())) {
                StringBuilder json = new StringBuilder("[");
                boolean first = true;
                for (com.nexagres.wire.core.BackendTarget t : backendRegistry.all()) {
                    if (!first) json.append(',');
                    first = false;
                    json.append("{\"name\":").append(jsonString(t.name()))
                            .append(",\"jdbcUrl\":").append(jsonString(t.jdbcUrl()))
                            .append(",\"dialect\":").append(jsonString(t.dialect() == null ? null : t.dialect().name()))
                            .append(",\"state\":").append(jsonString(backendRegistry.stateOf(t.name()).name()))
                            .append(",\"fallback\":").append(jsonString(t.fallbackName()))
                            .append('}');
                }
                json.append(']');
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(json.toString());
            } else if (drainMatch.matches() && "POST".equals(request.getMethod())) {
                // Planned-maintenance switchover, step 1: stop routing NEW statements to this
                // backend's name (resolveForRouting starts preferring its configured fallback, if
                // any, the instant setState below returns) and drain its connection pool -- wait
                // (bounded by graceMs) for whatever's already checked out to finish and be
                // returned, then close it. Existing sessions still mid-transaction on this backend
                // are unaffected by either step; they keep running on the connection they already
                // hold until they naturally finish. drainedCleanly=false in the response means the
                // grace period expired with connections still active -- the pool was closed anyway
                // (see BackendConnectionPools.drain's javadoc for exactly what that means), so the
                // caller knows to check for exceptions in whatever was still running against it.
                com.nexagres.wire.core.BackendTarget t = requireBackend(backendRegistry, drainMatch.group(1), response);
                if (t == null) return;
                if (xaRecoveryLog != null && xaRecoveryLog.hasUnresolvedFor(t.name())) {
                    response.setStatus(409);
                    response.getWriter().write("{\"error\":\"backend '" + t.name() + "' has an unresolved "
                            + "in-doubt XA branch -- draining now would make it unrecoverable by name. Wait "
                            + "for it to resolve (see GET /api/backends -- XA state isn't exposed there yet, "
                            + "check polywire_xa_log) and retry.\"}");
                    return;
                }
                long graceMs = parseLongParam(request.getParameter("graceMs"), 30_000);
                boolean local = "true".equals(request.getParameter("local"));
                backendRegistry.setState(t.name(), com.nexagres.wire.core.BackendRegistry.BackendState.DRAINING);
                var result = com.nexagres.wire.core.BackendConnectionPools.drain(t.poolKey(), graceMs);
                java.util.List<String> peerResults = local ? java.util.List.of()
                        : fanOutToPeers(options, selfAdminPort, forwardedAuthHeader,
                                "/api/backends/" + t.name() + "/drain?local=true&graceMs=" + graceMs);
                // "Wait for zero data loss" -- checked ONCE here (never on a local=true forwarded
                // call: replication lag is a property of the Postgres primary/replica pair, not of
                // any one PolyWire node, so N nodes each checking it would be redundant) and only
                // after every node has already stopped routing new statements here and drained its
                // in-flight ones -- by this point nothing is writing to `t` through PolyWire
                // anymore, so waiting for its fallback's lag to reach zero here is waiting for a
                // real, achievable full catch-up, not chasing a moving target. Unlike Phase
                // 3's failover lag check (advisory, never blocks -- see BackendHealthChecker's
                // javadoc), a PLANNED switchover can and should actually wait: there's no outage
                // forcing an immediate cutover. zeroDataLossConfirmed=false means the grace period
                // ran out before lag reached zero -- the caller should NOT proceed with physical
                // maintenance on `t` yet (some committed writes may still be missing on the
                // fallback), and can retry the drain (already-DRAINING, so idempotent) once the
                // replica has had more time.
                String lagStatus = "not applicable";
                if (!local && t.fallbackName() != null) {
                    com.nexagres.wire.core.BackendTarget fallback = backendRegistry.get(t.fallbackName());
                    if (fallback != null) {
                        var lag = com.nexagres.wire.core.ReplicationLag.awaitLagBelow(fallback, 0.0, graceMs);
                        lagStatus = !lag.ok() ? "fallback unreachable: " + lag.message()
                                : !lag.isReplica() ? "fallback is not a streaming replica -- no lag to wait for"
                                : lag.lagSeconds() <= 0.0 ? "confirmed zero lag"
                                : "TIMED OUT waiting for zero lag -- still " + String.format("%.1f", lag.lagSeconds()) + "s behind";
                    }
                }
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"name\":" + jsonString(t.name())
                        + ",\"state\":\"DRAINING\""
                        + ",\"fallback\":" + jsonString(t.fallbackName())
                        + ",\"poolExisted\":" + result.poolExisted()
                        + ",\"drainedCleanly\":" + result.drainedCleanly()
                        + ",\"activeConnectionsAtClose\":" + result.activeConnectionsAtClose()
                        + ",\"zeroDataLoss\":" + jsonString(lagStatus)
                        + ",\"peers\":" + jsonStringArray(peerResults) + "}");
            } else if (undrainMatch.matches() && "POST".equals(request.getMethod())) {
                // Step 2, once maintenance is done: resolveForRouting immediately starts sending
                // new statements back to this backend's own name again. No pool action needed here
                // -- drain() already removed the old pool entirely, so the very next borrow()
                // against this backend transparently creates a fresh one (see BackendConnectionPools
                // .drain's javadoc); there's nothing to "resume".
                com.nexagres.wire.core.BackendTarget t = requireBackend(backendRegistry, undrainMatch.group(1), response);
                if (t == null) return;
                boolean local = "true".equals(request.getParameter("local"));
                backendRegistry.setState(t.name(), com.nexagres.wire.core.BackendRegistry.BackendState.ACTIVE);
                java.util.List<String> peerResults = local ? java.util.List.of()
                        : fanOutToPeers(options, selfAdminPort, forwardedAuthHeader,
                                "/api/backends/" + t.name() + "/undrain?local=true");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("{\"name\":" + jsonString(t.name()) + ",\"state\":\"ACTIVE\""
                        + ",\"peers\":" + jsonStringArray(peerResults) + "}");
            } else if (tablesMatch.matches() && "GET".equals(request.getMethod())) {
                com.nexagres.wire.core.BackendTarget t = requireBackend(backendRegistry, tablesMatch.group(1), response);
                if (t == null) return;
                StringBuilder json = new StringBuilder("[");
                boolean first = true;
                for (var table : com.nexagres.wire.core.DataExplorer.listTables(t)) {
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
                com.nexagres.wire.core.BackendTarget t = requireBackend(backendRegistry, columnsMatch.group(1), response);
                if (t == null) return;
                StringBuilder json = new StringBuilder("[");
                boolean first = true;
                for (var col : com.nexagres.wire.core.DataExplorer.listColumns(t, columnsMatch.group(2), columnsMatch.group(3))) {
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
                com.nexagres.wire.core.BackendTarget t = requireBackend(backendRegistry, queryMatch.group(1), response);
                if (t == null) return;
                JsonObject body = readJsonBody(request);
                if (!body.has("sql") || body.get("sql").getAsString().isBlank()) {
                    throw new IllegalArgumentException("sql is required");
                }
                var result = com.nexagres.wire.core.DataExplorer.runQuery(t, body.get("sql").getAsString());
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

    private static com.nexagres.wire.core.BackendTarget requireBackend(com.nexagres.wire.core.BackendRegistry backendRegistry,
            String name, HttpServletResponse response) throws java.io.IOException {
        com.nexagres.wire.core.BackendTarget t = backendRegistry.get(name);
        if (t == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\":\"no such backend: " + name.replace("\"", "'") + "\"}");
            return null;
        }
        return t;
    }

    /**
     * The cluster-wide half of switchover drain/undrain (Phase 5): {@code BackendRegistry}'s
     * drain/down state lives only in each process's own memory (deliberately not pushed through
     * {@code ConfigStore}/LISTEN-NOTIFY -- see the constructor javadoc above), so an operator's
     * drain call landing on one node behind a load balancer must be re-issued against every OTHER
     * live node too, or routing on those nodes would keep sending traffic to a backend that's
     * supposedly under maintenance. Reads live node addresses from {@code polywire_nodes} via
     * {@link com.nexagres.wire.config.NodeRegistry#listAll} (the same table the {@code /api/nodes}
     * admin page already reads), skips the row matching this process's own {@code
     * NodeRegistry.resolveHost()}/{@code selfAdminPort} (that node already applied the call
     * directly, not through HTTP), and forwards the caller's own {@code Authorization} header
     * on to each peer -- every node in a cluster shares the same {@code POLYWIRE_ADMIN_TOKEN}, so
     * this doesn't need its own separate credential.
     *
     * <p>Best-effort, not a two-phase commit: a peer that's unreachable or errors is reported in
     * the returned list as a {@code "host:port -> ERROR: ..."} line rather than failing the whole
     * request -- an operator running a real switchover is expected to read this list and confirm
     * every peer succeeded before proceeding with maintenance, the same judgment call a real load
     * balancer's "drain and confirm" workflow already requires.
     */
    private static java.util.List<String> fanOutToPeers(com.nexagres.wire.server.ServerOptions options,
            int selfAdminPort, String authHeader, String path) {
        java.util.List<String> results = new java.util.ArrayList<>();
        if (options == null) {
            return results;
        }
        java.util.List<com.nexagres.wire.config.NodeRegistry.NodeRow> nodes;
        try {
            nodes = com.nexagres.wire.config.NodeRegistry.listAll(options);
        } catch (java.sql.SQLException e) {
            results.add("ERROR: could not list cluster nodes to fan out to: " + e.getMessage());
            return results;
        }
        String selfHost = com.nexagres.wire.config.NodeRegistry.resolveHost();
        var client = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(5)).build();
        for (var node : nodes) {
            if (selfHost.equals(node.host()) && selfAdminPort == node.adminPort()) {
                continue;
            }
            String peer = node.host() + ":" + node.adminPort();
            try {
                var requestBuilder = java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create("http://" + node.host() + ":" + node.adminPort() + path))
                        .timeout(java.time.Duration.ofSeconds(15))
                        .POST(java.net.http.HttpRequest.BodyPublishers.noBody());
                if (authHeader != null) {
                    requestBuilder.header("Authorization", authHeader);
                }
                var httpResponse = client.send(requestBuilder.build(), java.net.http.HttpResponse.BodyHandlers.ofString());
                results.add(peer + " -> " + httpResponse.statusCode());
            } catch (Exception e) {
                results.add(peer + " -> ERROR: " + e.getMessage());
            }
        }
        return results;
    }

    private static String jsonStringArray(java.util.List<String> values) {
        StringBuilder json = new StringBuilder("[");
        boolean first = true;
        for (String v : values) {
            if (!first) json.append(',');
            first = false;
            json.append(jsonString(v));
        }
        return json.append(']').toString();
    }

    private static final Pattern QUEUE_NAME_PATH = Pattern.compile("^/api/queues/([^/]+)$");

    /**
     * Read-only admin view of sqswire's queues -- {@code GET /api/queues} lists every queue with
     * its live depth (visible/in-flight message counts from {@link com.nexagres.wire.sqswire.PgQueueStore#countMessages}),
     * FIFO/DLQ/redrive attributes, and which shard backend it currently resolves to (so the page
     * can show sharding is actually splitting queues across backends, same idea as the Backends
     * page's per-backend view). {@code DELETE /api/queues/{name}} drops a queue entirely -- the
     * one mutating action this route offers, useful for clearing out a demo/test queue from the
     * UI without a psql session.
     */
    private static void handleQueues(String target, HttpServletRequest request, HttpServletResponse response,
            com.nexagres.wire.sqswire.PgQueueStore queueStore) throws java.io.IOException {
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
     * {@link com.nexagres.wire.capture.WorkloadCaptureBuffer}, oldest-first, {@code localSeq}
     * strictly greater than {@code since} (default 0), capped at {@code limit} (default 1000).
     * {@code WorkloadReplayer} calls this on every live node (discovered via {@code /api/nodes})
     * and merges the results by {@code wallClock} to reconstruct one global arrival order across
     * the fleet -- see {@link com.nexagres.wire.capture.WorkloadCaptureBuffer}'s javadoc.
     */
    private static void handleCapture(HttpServletRequest request, HttpServletResponse response,
            com.nexagres.wire.capture.WorkloadCaptureBuffer captureBuffer) throws java.io.IOException {
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
        for (com.nexagres.wire.capture.WorkloadCaptureBuffer.Entry e : captureBuffer.since(since, limit)) {
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
     * buffer otherwise (see {@link com.nexagres.wire.audit.AuditLog#recent}). */
    private static void handleAudit(HttpServletRequest request, HttpServletResponse response,
            com.nexagres.wire.audit.AuditLog auditLog) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        if (!"GET".equals(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\":\"no such route\"}");
            return;
        }
        int limit = (int) parseLongParam(request.getParameter("limit"), 100);
        StringBuilder json = new StringBuilder("[");
        boolean first = true;
        for (com.nexagres.wire.audit.AuditEvent event : auditLog.recent(limit)) {
            if (!first) json.append(',');
            first = false;
            json.append(com.nexagres.wire.audit.AuditLog.toJson(event));
        }
        json.append(']');
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(json.toString());
    }

    /**
     * Turns real {@code MCP_TOOL_CALLED} audit events into a short plain-English narrative -- an
     * optional {@code userId} in the request body filters to just that caller's activity (MCP has
     * no first-class "session" concept at this dispatch point, so {@code accessContext.userId()}
     * is the natural grouping key -- {@code "anonymous"} if OAuth isn't enforced). Reads from
     * {@link com.nexagres.wire.audit.AuditLog#recent}, which returns EVERY event type most-recent-
     * first, so this filters client-side for {@code MCP_TOOL_CALLED} rather than assuming the
     * store has a type-filtered query -- it doesn't, confirmed by reading {@code AuditLog}/{@code
     * AuditLogStore} rather than assuming.
     */
    private static void handleMcpAuditSummarize(HttpServletRequest request, HttpServletResponse response,
            com.nexagres.wire.audit.AuditLog auditLog, com.nexagres.wire.core.DialectTranslationStage dialectTranslationStage)
            throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        com.nexagres.wire.core.TranslationLlmClient llmClient =
                dialectTranslationStage == null ? null : dialectTranslationStage.llmClient();
        if (llmClient == null) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().write("{\"error\":\"no LLM provider configured -- set it via PUT /api/llm-config "
                    + "or the POLYWIRE_LLM_* env vars before summarizing MCP activity\"}");
            return;
        }
        JsonObject body;
        try {
            body = request.getContentLength() > 0 ? readJsonBody(request) : new JsonObject();
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\":\"invalid JSON request body\"}");
            return;
        }
        String userIdFilter = optionalString(body, "userId");
        int limit = body.has("limit") && body.get("limit").isJsonPrimitive() && body.get("limit").getAsJsonPrimitive().isNumber()
                ? body.get("limit").getAsInt() : 25;

        // recent() returns every event type -- scan a generously larger window (10x the requested
        // limit, capped) so a busy audit log full of unrelated login/firewall events doesn't
        // starve out real MCP_TOOL_CALLED rows before this filter ever sees enough of them.
        java.util.List<com.nexagres.wire.audit.AuditEvent> matched = new java.util.ArrayList<>();
        for (com.nexagres.wire.audit.AuditEvent event : auditLog.recent(Math.min(limit * 10, 2000))) {
            if (event.type() != com.nexagres.wire.audit.AuditEvent.Type.MCP_TOOL_CALLED) {
                continue;
            }
            if (userIdFilter != null && !userIdFilter.isBlank() && !userIdFilter.equals(event.userId())) {
                continue;
            }
            matched.add(event);
            if (matched.size() >= limit) {
                break;
            }
        }

        if (matched.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_OK);
            JsonObject empty = new JsonObject();
            empty.addProperty("summary", (String) null);
            empty.addProperty("eventCount", 0);
            empty.addProperty("note", "no matching MCP_TOOL_CALLED audit events found -- nothing to summarize");
            response.getWriter().write(empty.toString());
            return;
        }

        StringBuilder context = new StringBuilder();
        for (com.nexagres.wire.audit.AuditEvent event : matched) {
            context.append(event.timestamp()).append(" user=").append(event.userId()).append(": ")
                    .append(event.summary());
            String args = event.details().get("arguments");
            if (args != null) {
                context.append(" args=").append(args);
            }
            String error = event.details().get("error");
            if (error != null) {
                context.append(" error=").append(error);
            }
            context.append('\n');
        }

        String summary;
        try {
            summary = llmClient.summarizeMcpActivity(context.toString());
        } catch (Exception e) {
            log.warn("mcp-audit summarize: LLM call failed: {}", e.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
            response.getWriter().write("{\"error\":\"LLM request failed: " + jsonString(e.getMessage()) + "\"}");
            return;
        }

        JsonObject responseBody = new JsonObject();
        responseBody.addProperty("summary", summary);
        responseBody.addProperty("eventCount", matched.size());
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(responseBody.toString());
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
     * {@code polywire_nodes} (see {@link com.nexagres.wire.config.NodeRegistry}). Sorted by zone
     * then host for a stable, readable order; {@code status} is {@code "up"} if the row's
     * heartbeat is within the last 30s, else {@code "stale"}.
     */
    private static void handleNodes(HttpServletRequest request, HttpServletResponse response,
            com.nexagres.wire.server.ServerOptions options) throws java.io.IOException {
        response.setContentType("application/json; charset=utf-8");
        if (!"GET".equals(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\":\"no such route\"}");
            return;
        }
        try {
            StringBuilder json = new StringBuilder("[");
            boolean first = true;
            for (com.nexagres.wire.config.NodeRegistry.NodeRow row : com.nexagres.wire.config.NodeRegistry.listAll(options)) {
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

    /** Real bug, found live building {@code /api/federation/plans}: this helper only ever escaped
     * backslashes and double-quotes -- every earlier caller's own strings happened to be single-
     * line (names, messages, SQL text without embedded newlines), so a raw, unescaped {@code '\n'}
     * inside a JSON string value (a real multi-line {@code EXPLAIN PLAN FOR} plan) never surfaced
     * this gap before. Strict JSON requires every control character be escaped, not just the two
     * this was handling. */
    private static String jsonString(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder(s.length() + 16).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '"' -> out.append("\\\"");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
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

    private static String renderAnomalies(com.nexagres.wire.core.AnomalyDetectionScheduler anomalyScheduler) {
        if (anomalyScheduler == null) {
            return "{\"enabled\":false,\"notes\":[]}";
        }
        StringBuilder json = new StringBuilder("{\"enabled\":true,\"notes\":[");
        boolean first = true;
        for (var note : anomalyScheduler.recentNotes(50)) {
            if (!first) json.append(',');
            first = false;
            json.append("{\"timestamp\":").append(jsonString(note.timestamp().toString()))
                    .append(",\"protocol\":").append(jsonString(note.protocol()))
                    .append(",\"baselinePerSec\":").append(String.format(java.util.Locale.ROOT, "%.3f", note.baselinePerSec()))
                    .append(",\"currentPerSec\":").append(String.format(java.util.Locale.ROOT, "%.3f", note.currentPerSec()))
                    .append(",\"ratio\":").append(String.format(java.util.Locale.ROOT, "%.2f", note.ratio()))
                    .append(",\"narrative\":").append(note.narrative() == null ? "null" : jsonString(note.narrative()))
                    .append('}');
        }
        json.append("]}");
        return json.toString();
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
            com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics,
            com.nexagres.wire.core.QueryRepairStage queryRepairStage) {
        com.nexagres.wire.core.SqlMetricsCollector.Snapshot snap = statsStage.sqlMetricsSnapshot();
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
        json.append("],\"queryRepair\":{\"attempted\":")
                .append(queryRepairStage == null ? -1 : queryRepairStage.attemptCount())
                .append(",\"repaired\":")
                .append(queryRepairStage == null ? -1 : queryRepairStage.repairedCount())
                .append('}');
        json.append('}');
        return json.toString();
    }

    public void start() throws Exception {
        server.start();
        log.info("polywire /metrics endpoint listening on port {}", ((org.eclipse.jetty.server.ServerConnector)
                server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
    }

}
