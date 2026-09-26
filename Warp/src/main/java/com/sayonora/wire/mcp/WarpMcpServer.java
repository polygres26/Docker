package com.sayonora.wire.mcp;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.core.AdHocQueryRunner;
import com.sayonora.wire.core.BackendCatalogDiscovery;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendScope;
import com.sayonora.wire.core.ExecutionResult;
import com.sayonora.wire.core.JdbcBackendExecutor;
import com.sayonora.wire.core.PipelineStage;
import com.sayonora.wire.core.RouterStage;
import com.sayonora.wire.core.SchemaFederationStage;
import com.sayonora.wire.core.SourceDialect;
import com.sayonora.wire.core.Statement;
import com.sayonora.wire.mssqlwire.MssqlBackendConnections;
import com.sayonora.wire.mywire.MySqlBackendConnections;
import com.sayonora.wire.pgwire.PgConnections;
import com.sayonora.wire.server.ServerOptions;
import com.sayonora.wire.server.ServerOptions.McpBackendMode;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WarpMcpServer {

    private static final Logger log = LoggerFactory.getLogger(WarpMcpServer.class);
    private static final String PROTOCOL_VERSION = "2024-11-05";
    private static final Gson GSON = new Gson();
    private static final Gson GSON_NULLS = new com.google.gson.GsonBuilder().serializeNulls().create();

    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final BackendRegistry backendRegistry;
    private final ConnectionGate connectionGate;
    private final com.sayonora.wire.http.auth.AccessContextResolver oauth;
    private final Server server;
    private final List<RegisteredFunctionTool> functionTools;
    private final McpMetricsCollector metrics;
    private final com.sayonora.wire.audit.AuditLog auditLog;
    private final java.util.function.Supplier<com.sayonora.wire.core.TranslationLlmClient> llmClientSupplier;
    private final McpScope scope;
    private final java.util.Map<String, McpScope> roleScopes;
    // Backend-kind vocabulary (see BackendKind / BackendToolProvider): which kinds this endpoint
    // advertises (WARP_MCP_KIND, default relational = unchanged behavior), the per-kind providers,
    // and WARP_MCP_READ_ONLY (hides/refuses the providers' write tools).
    private final java.util.Set<BackendKind> enabledKinds;
    private final boolean legacyKinds;
    private final EmulatedStores emulatedStores;
    private final McpBackendCatalog catalog;
    private final McpEndpoints endpoints = new McpEndpoints();
    private final ExternalClients externalClients = new ExternalClients();
    private final boolean requireEndpoint;
    private final Map<BackendKind, BackendToolProvider> providers = new java.util.EnumMap<>(BackendKind.class);
    private final boolean providerReadOnly;
    private static final ThreadLocal<java.util.Set<BackendKind>> CURRENT_KINDS = new ThreadLocal<>();
    private static final ThreadLocal<McpEndpoints.Endpoint> CURRENT_ENDPOINT = new ThreadLocal<>();

    // Ambient, request-scoped effective McpScope -- set once per HTTP request (see #handleRequest
    // and #callNaturalLanguageQuery, its own non-HTTP entry point) and read by every enforcement
    // primitive (openBackendConnection, runSql, scopedDiscoveredTables/Columns,
    // buildToolsListResult) instead of the field default directly. A deliberate, narrow use of a
    // ThreadLocal rather than threading an extra parameter through ~20 existing method signatures
    // -- safe here specifically because this Jetty AbstractHandler processes one request
    // synchronously start-to-finish on one thread, with no async/continuation hop mid-request (the
    // same reasoning that makes frameworks like Spring's RequestContextHolder safe for the same
    // "ambient per-request context without a signature rewrite" problem). Always cleared in a
    // finally block -- never left set across requests on a pooled thread.
    private static final ThreadLocal<McpScope> CURRENT_SCOPE = new ThreadLocal<>();

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec,
                com.sayonora.wire.http.auth.AccessContextResolver.DISABLED);
    }

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.sayonora.wire.http.auth.AccessContextResolver oauth) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, new McpMetricsCollector());
    }

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.sayonora.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, metrics, null);
    }

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.sayonora.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics,
            com.sayonora.wire.audit.AuditLog auditLog) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, metrics, auditLog, () -> null);
    }

    /**
     * Full constructor -- adds {@code metrics}, the shared {@link McpMetricsCollector} instance
     * {@code MetricsServer} reads from to render {@code /api/metrics/summary}'s {@code mcpTools}
     * field and {@code /metrics}' {@code warp_mcp_tool_*} series. Passed in (not constructed
     * internally and exposed via a getter) so both this class and {@code MetricsServer} share the
     * exact same instance regardless of which one {@code Main} happens to construct first.
     *
     * <p>{@code auditLog}, if non-null, gets one {@code MCP_TOOL_CALLED} event per real tool
     * invocation (see {@link #handleToolsCall}) -- previously MCP traffic was counted in {@code
     * metrics} but never actually reached the audit trail every other protocol's login/query
     * events do, which meant {@code /api/mcp-audit/summarize}'s "what did this agent actually do"
     * question had no real per-call data to answer from, only aggregate counters.
     *
     * <p>{@code llmClientSupplier}, read fresh on every {@code query_natural_language} call (same
     * "current hot-reloadable client" pattern {@link com.sayonora.wire.core.QueryRepairStage}
     * uses), powers that one tool's natural-language-to-SQL drafting and judging -- see {@link
     * #runNaturalLanguageQuery}. {@code () -> null} on every other constructor overload disables
     * just that tool (it errors clearly, "no LLM provider configured"), not the whole server.
     */
    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.sayonora.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics,
            com.sayonora.wire.audit.AuditLog auditLog,
            java.util.function.Supplier<com.sayonora.wire.core.TranslationLlmClient> llmClientSupplier) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, metrics, auditLog,
                llmClientSupplier, McpScope.fromEnv());
    }

    /** As the 10-arg constructor, plus {@code scope} -- see {@link McpScope}'s own javadoc for the
     * real access-boundary this enforces (not just a cosmetic listing filter). Every other
     * overload defaults to {@link McpScope#fromEnv()} ({@code WARP_MCP_SCOPE}, itself defaulting
     * to unscoped when unset) so existing callers/tests need no changes to keep today's behavior. */
    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.sayonora.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics,
            com.sayonora.wire.audit.AuditLog auditLog,
            java.util.function.Supplier<com.sayonora.wire.core.TranslationLlmClient> llmClientSupplier,
            McpScope scope) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.connectionGate = connectionGate;
        this.oauth = oauth;
        this.metrics = metrics;
        this.auditLog = auditLog;
        this.llmClientSupplier = llmClientSupplier;
        this.scope = scope;
        this.roleScopes = McpScope.roleScopesFromEnv();
        this.legacyKinds = BackendKind.explicitFromEnv();
        this.enabledKinds = BackendKind.fromEnv();
        this.requireEndpoint = "true".equalsIgnoreCase(System.getenv("WARP_MCP_REQUIRE_ENDPOINT"));
        this.emulatedStores = new EmulatedStores(backendRegistry);
        this.catalog = new McpBackendCatalog(backendRegistry, emulatedStores, emulatedKindsFromEnv(legacyKinds, enabledKinds),
                options.mcpBackendMode());
        this.providers.put(BackendKind.DYNAMODB, new DynamoDbToolProvider(emulatedStores, externalClients));
        this.providers.put(BackendKind.INFLUX, new InfluxToolProvider(emulatedStores));
        this.providers.put(BackendKind.MONGODB, new MongoToolProvider(emulatedStores, externalClients));
        this.providers.put(BackendKind.S3, new S3ToolProvider(externalClients));
        for (BackendKind describeOnly : new BackendKind[] {BackendKind.KAFKA, BackendKind.CASSANDRA, BackendKind.SPLUNK}) {
            this.providers.put(describeOnly, new ConnectorDescribeProvider(describeOnly));
        }
        this.providers.put(BackendKind.SQS, new StoreDescribeProvider(BackendKind.SQS,
                com.sayonora.wire.core.StoreType.SQS, backendRegistry));
        this.providers.put(BackendKind.OPENSEARCH, new StoreDescribeProvider(BackendKind.OPENSEARCH,
                com.sayonora.wire.core.StoreType.OPENSEARCH, backendRegistry));
        this.providers.put(BackendKind.NEO4J, new StoreDescribeProvider(BackendKind.NEO4J,
                com.sayonora.wire.core.StoreType.NEO4J, backendRegistry));
        this.providers.put(BackendKind.S3STORE, new StoreDescribeProvider(BackendKind.S3STORE,
                com.sayonora.wire.core.StoreType.S3, backendRegistry));
        this.providers.put(BackendKind.REDIS, new StoreDescribeProvider(BackendKind.REDIS,
                com.sayonora.wire.core.StoreType.REDIS, backendRegistry));
        this.providers.put(BackendKind.AZBLOB, new StoreDescribeProvider(BackendKind.AZBLOB,
                com.sayonora.wire.core.StoreType.AZBLOB, backendRegistry));
        this.providers.put(BackendKind.AZQUEUE, new StoreDescribeProvider(BackendKind.AZQUEUE,
                com.sayonora.wire.core.StoreType.AZQUEUE, backendRegistry));
        this.providers.put(BackendKind.AZTABLE, new StoreDescribeProvider(BackendKind.AZTABLE,
                com.sayonora.wire.core.StoreType.AZTABLE, backendRegistry));
        this.providers.put(BackendKind.SNS, new StoreDescribeProvider(BackendKind.SNS,
                com.sayonora.wire.core.StoreType.SNS, backendRegistry));
        this.providers.put(BackendKind.KINESIS, new StoreDescribeProvider(BackendKind.KINESIS,
                com.sayonora.wire.core.StoreType.KINESIS, backendRegistry));
        this.providers.put(BackendKind.AWSPARAMS, new StoreDescribeProvider(BackendKind.AWSPARAMS,
                com.sayonora.wire.core.StoreType.AWSPARAMS, backendRegistry));
        this.providers.put(BackendKind.GCS, new StoreDescribeProvider(BackendKind.GCS,
                com.sayonora.wire.core.StoreType.GCS, backendRegistry));
        this.providers.put(BackendKind.FIRESTORE, new StoreDescribeProvider(BackendKind.FIRESTORE,
                com.sayonora.wire.core.StoreType.FIRESTORE, backendRegistry));
        this.providers.put(BackendKind.DATASTORE, new StoreDescribeProvider(BackendKind.DATASTORE,
                com.sayonora.wire.core.StoreType.DATASTORE, backendRegistry));
        this.providers.put(BackendKind.PUBSUB, new StoreDescribeProvider(BackendKind.PUBSUB,
                com.sayonora.wire.core.StoreType.PUBSUB, backendRegistry));
        this.providerReadOnly = "true".equalsIgnoreCase(System.getenv("WARP_MCP_READ_ONLY"));
        this.functionTools = introspectRegisteredTools(options, toolsSpec);
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                if (!connectionGate.acceptHttp(request)) {
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    return;
                }
                String reqPath = request.getRequestURI();
                com.sayonora.wire.core.AccessContext accessContext;
                if (reqPath != null && reqPath.startsWith(McpEndpoints.PATH_PREFIX)) {
                    // A user-created endpoint: its own bearer token authenticates it (SSO is not
                    // consulted); expiry is evaluated against server time on EVERY request.
                    McpEndpoints.Auth auth = endpoints.authenticate(reqPath, request.getHeader("Authorization"));
                    if (!auth.ok()) {
                        log.info("MCP endpoint request refused: {}", auth.reason());
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        response.setHeader("WWW-Authenticate", "Bearer");
                        writeError(response, null, -32001, McpEndpoints.CLIENT_MESSAGE);
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        return;
                    }
                    CURRENT_ENDPOINT.set(auth.endpoint());
                    accessContext = com.sayonora.wire.core.AccessContext.ANONYMOUS;
                } else if (requireEndpoint) {
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    response.setHeader("WWW-Authenticate", "Bearer");
                    writeError(response, null, -32001, "this MCP listener only serves user-created endpoints "
                            + "(WARP_MCP_REQUIRE_ENDPOINT=true): use /e/<id> with its bearer token");
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    return;
                } else {
                    accessContext = oauth.enforce(request, response);
                    if (accessContext == null) {
                        return;
                    }
                }
                try {
                    handleRequest(request, response, accessContext);
                } finally {
                    CURRENT_ENDPOINT.remove();
                }
            }
        });
    }

    /** Handles to the Warp-emulated stores (dynamowire/mongowire) that the kind-specific tools
     * read and write; {@code Main} injects the running instances. */
    public EmulatedStores emulatedStores() {
        return emulatedStores;
    }

    /** The live user-created endpoint set; {@code Main} loads it from {@code warp_config} at startup
     * and on every config reload. */
    public McpEndpoints endpoints() {
        return endpoints;
    }

    /** {@code WARP_MCP_EMULATED_STORES} = comma list of dynamodb/mongodb/influx, {@code all}, or
     * empty (default: none). A non-relational kind named by the legacy {@code WARP_MCP_KIND} also
     * opts its emulated store in. */
    private static java.util.Set<BackendKind> emulatedKindsFromEnv(boolean legacy, java.util.Set<BackendKind> kinds) {
        java.util.Set<BackendKind> out = new java.util.LinkedHashSet<>();
        String spec = System.getenv("WARP_MCP_EMULATED_STORES");
        if (spec != null && !spec.isBlank()) {
            if (spec.trim().equalsIgnoreCase("all")) {
                out.addAll(java.util.List.of(BackendKind.DYNAMODB, BackendKind.MONGODB, BackendKind.INFLUX));
            } else if (!spec.trim().equalsIgnoreCase("none")) {
                out.addAll(BackendKind.parseList(spec));
            }
        }
        if (legacy) {
            out.addAll(kinds);
        }
        out.remove(BackendKind.RELATIONAL);
        return out;
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        externalClients.close();
        server.stop();
    }

    /**
     * Public entry point for OTHER Warp-hosted AI-agent protocol frontends (currently: A2A --
     * see {@code com.sayonora.wire.a2a.A2AServer}) that want the SAME governed
     * natural-language-to-SQL capability MCP's own {@code query_natural_language} tool exposes,
     * without duplicating its draft/judge/read-only-enforcement logic or its Postgres-only-mode
     * restriction (see {@link #handleToolsCall}'s own identical gate) -- A2A gets exactly the same
     * "one deterministic check, everything else through the real firewall/QoS/audit pipeline"
     * guarantee MCP already has, not a separate, less-governed path to the same backend.
     *
     * @return the same {@code AdHocQueryRunner.Result} shape every tool call already returns
     *      (never {@code null}); a non-Postgres {@code WARP_MCP_BACKEND} mode or a missing LLM
     *      provider both come back as an ordinary {@code Result.ofError}-shaped failure, not an
     *      exception -- the caller doesn't need special-case handling beyond checking
     *      {@code success()}, same as every other AdHocQueryRunner.Result consumer.
     */
    public AdHocQueryRunner.Result callNaturalLanguageQuery(String question, com.sayonora.wire.core.AccessContext accessContext)
            throws SQLException {
        if (options.mcpBackendMode() != McpBackendMode.POSTGRES) {
            return notSupportedInNativeMode("query_natural_language", options.mcpBackendMode());
        }
        // A2A doesn't go through #handleRequest -- CURRENT_SCOPE has to be set here directly,
        // same resolution priority as every real MCP request gets.
        CURRENT_SCOPE.set(McpScope.resolveForCaller(accessContext, roleScopes, scope));
        try (Connection backend = openBackendConnection()) {
            return runNaturalLanguageQuery(backend, question, accessContext).result();
        } finally {
            CURRENT_SCOPE.remove();
        }
    }

    private static List<RegisteredFunctionTool> introspectRegisteredTools(ServerOptions options, String toolsSpec) {
        List<RegisteredFunctionTool> tools = new ArrayList<>();
        if (toolsSpec == null || toolsSpec.isBlank()) {
            return tools;
        }
        // WARP_MCP_TOOLS registers real Postgres functions/procedures as MCP tools by introspecting
        // pg_proc (see PgFunctionIntrospector) -- there's no Oracle/MySQL/SQL Server equivalent
        // built yet, so in native mode this whole feature is cleanly unavailable rather than
        // silently querying pg_proc against a connection that isn't Postgres at all.
        if (options.mcpBackendMode() != McpBackendMode.POSTGRES) {
            log.warn("MCP: WARP_MCP_TOOLS is configured but WARP_MCP_BACKEND={} -- registered "
                    + "function tools introspect real Postgres functions (pg_proc) and aren't "
                    + "supported in native-backend mode yet; skipping, every other tool still works",
                    options.mcpBackendMode());
            return tools;
        }
        try (Connection conn = PgConnections.open(options)) {
            for (String entry : toolsSpec.split(";")) {
                String trimmed = entry.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try {
                    tools.add(RegisteredFunctionTool.introspect(conn, trimmed));
                } catch (Exception e) {
                    log.warn("MCP: skipping registered tool '{}' -- introspection failed: {}", trimmed, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("MCP: could not open a connection to introspect WARP_MCP_TOOLS -- no registered "
                    + "function tools will be available this run: {}", e.getMessage());
        }
        log.info("MCP: {} registered function tool(s) available", tools.size());
        return tools;
    }

    private void handleRequest(HttpServletRequest request, HttpServletResponse response,
            com.sayonora.wire.core.AccessContext accessContext) throws IOException {
        String body = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        JsonObject req;
        try {
            req = JsonParser.parseString(body).getAsJsonObject();
        } catch (RuntimeException e) {
            writeError(response, null, -32700, "Parse error: " + e.getMessage());
            return;
        }
        JsonElement idElement = req.get("id");
        String method = req.has("method") ? req.get("method").getAsString() : null;
        JsonObject params = req.has("params") && req.get("params").isJsonObject() ? req.getAsJsonObject("params") : new JsonObject();

        if (method == null) {
            writeError(response, idElement, -32600, "Invalid Request: missing method");
            return;
        }
        if (idElement == null) {
            
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
            return;
        }

        response.setHeader("Mcp-Session-Id", request.getHeader("Mcp-Session-Id") != null
                ? request.getHeader("Mcp-Session-Id") : UUID.randomUUID().toString());

        // Resolved ONCE per request, from this caller's own authenticated identity -- see
        // McpScope#resolveForCaller's own javadoc for the real priority order (a token's own
        // "warp_scope" claim, then role-mapped scope, then this endpoint's configured default).
        McpScope requestScope = McpScope.resolveForCaller(accessContext, roleScopes, scope);
        McpEndpoints.Endpoint endpoint = CURRENT_ENDPOINT.get();
        if (endpoint != null) {
            // An endpoint can only NARROW what the listener/caller already allows, never widen it.
            McpScope narrowed;
            try {
                narrowed = McpEndpoints.narrow(requestScope, endpoint.mcpScope(), backendRegistry::membersOfGroup);
            } catch (IllegalArgumentException e) {
                narrowed = null;
            }
            if (narrowed == null) {
                writeError(response, idElement, -32001, "endpoint scope \"" + endpoint.scope()
                        + "\" is outside what this MCP listener allows");
                return;
            }
            requestScope = narrowed;
        }
        CURRENT_SCOPE.set(requestScope);
        // "/kinds/<kind>" narrows this request to one enabled kind's own vocabulary.
        String path = request.getRequestURI();
        if (path != null && path.startsWith("/kinds/")) {
            try {
                BackendKind narrowed = BackendKind.parse(path.substring("/kinds/".length()).replaceAll("/+$", ""));
                if (!enabledKinds.contains(narrowed)) {
                    CURRENT_SCOPE.remove();
                    writeError(response, idElement, -32602, "kind \"" + narrowed.id()
                            + "\" is not enabled on this endpoint (WARP_MCP_KIND)");
                    return;
                }
                CURRENT_KINDS.set(java.util.Set.of(narrowed));
            } catch (IllegalArgumentException e) {
                CURRENT_SCOPE.remove();
                writeError(response, idElement, -32602, e.getMessage());
                return;
            }
        }
        try {
            switch (method) {
                case "initialize" -> writeResult(response, idElement, buildInitializeResult());
                case "tools/list" -> writeResult(response, idElement, buildToolsListResult());
                case "tools/call" -> handleToolsCall(response, idElement, params, accessContext);
                default -> writeError(response, idElement, -32601, "Method not found: " + method);
            }
        } finally {
            CURRENT_SCOPE.remove();
            CURRENT_KINDS.remove();
        }
    }

    private static final String LIST_BACKENDS = "list_backends";
    private static final String DESCRIBE_BACKEND = "describe_backend";
    /** SQL tools that accept an optional {@code backend} argument to pin the call to one relational
     * backend in scope. Without it they behave exactly as before (the gateway's own routing). The
     * gateway-wide tools (query_federated, inspect_schema, document_schema, query_natural_language,
     * explain_query) span backends by design and take no {@code backend}. */
    private static final java.util.Set<String> BACKEND_ARG_TOOLS = java.util.Set.of("execute_sql", "run_sql",
            "list_tables", "describe_table", "column_stats", "compare_groups", "correlation", "sample_rows",
            "find_outliers", "find_join_path", "explain_sql");

    private java.util.Set<BackendKind> effectiveKinds() {
        java.util.Set<BackendKind> k = CURRENT_KINDS.get();
        return k != null ? k : enabledKinds;
    }

    /** Legacy {@code WARP_MCP_KIND=a,b} mode advertises non-relational tools "<kind>_"-prefixed. */
    private boolean legacyPrefix() {
        return CURRENT_KINDS.get() == null && legacyKinds && enabledKinds.size() > 1;
    }

    private List<McpBackend> backendsInScope() {
        return catalog.inScope(currentScope(), effectiveKinds());
    }

    private record ProviderTool(BackendToolProvider provider, BackendToolProvider.Tool tool, String advertisedName,
            BackendKind kind) {
    }

    /** Non-relational tools for the tool families present among {@code backends}. */
    private List<ProviderTool> providerTools(List<McpBackend> backends) {
        List<ProviderTool> out = new ArrayList<>();
        boolean prefix = legacyPrefix();
        java.util.Set<BackendKind> present = new java.util.LinkedHashSet<>();
        for (McpBackend b : backends) {
            if (b.kind() != BackendKind.RELATIONAL) {
                present.add(b.kind());
            }
        }
        for (BackendKind k : present) {
            BackendToolProvider p = providers.get(k);
            if (p == null) {
                continue;
            }
            for (BackendToolProvider.Tool t : p.tools()) {
                if (t.write() && providerReadOnly) {
                    continue;
                }
                out.add(new ProviderTool(p, t, prefix ? k.prefix() + t.name() : t.name(), k));
            }
        }
        return out;
    }

    /** One advertised tool name, possibly served by several backend types. */
    private static final class Offer {
        String name;
        String description;
        JsonObject properties = new JsonObject();
        java.util.Set<String> required;
        final List<McpBackend> candidates = new ArrayList<>();
        final java.util.Set<BackendKind> kinds = new java.util.LinkedHashSet<>();
        final java.util.Map<BackendKind, String> kindDescriptions = new LinkedHashMap<>();

        void mergeSchema(JsonObject schema) {
            if (schema.has("properties")) {
                schema.getAsJsonObject("properties").entrySet()
                        .forEach(e -> { if (!properties.has(e.getKey())) properties.add(e.getKey(), e.getValue()); });
            }
            java.util.Set<String> req = new java.util.LinkedHashSet<>();
            if (schema.has("required")) {
                schema.getAsJsonArray("required").forEach(e -> req.add(e.getAsString()));
            }
            if (required == null) {
                required = req;
            } else {
                required.retainAll(req);
            }
        }
    }

    private JsonObject buildToolsListResult() {
        List<McpBackend> backends = backendsInScope();
        List<McpBackend> relational = backends.stream().filter(b -> b.kind() == BackendKind.RELATIONAL).toList();
        Map<String, Offer> offers = new LinkedHashMap<>();
        java.util.Set<String> relationalNames = new java.util.LinkedHashSet<>();
        List<JsonObject> passthrough = new ArrayList<>();
        if (!relational.isEmpty()) {
            for (JsonElement el : buildRelationalToolsListResult().getAsJsonArray("tools")) {
                JsonObject def = el.getAsJsonObject();
                String name = def.get("name").getAsString();
                relationalNames.add(name);
                Offer o = new Offer();
                o.name = name;
                o.description = def.get("description").getAsString();
                o.mergeSchema(def.getAsJsonObject("inputSchema"));
                o.kinds.add(BackendKind.RELATIONAL);
                if (BACKEND_ARG_TOOLS.contains(name)) {
                    o.candidates.addAll(relational);
                }
                offers.put(name, o);
            }
        }
        for (ProviderTool pt : providerTools(backends)) {
            List<McpBackend> cands = backends.stream().filter(b -> b.kind() == pt.kind()).toList();
            Offer o = offers.computeIfAbsent(pt.advertisedName(), n -> {
                Offer fresh = new Offer();
                fresh.name = n;
                fresh.description = pt.tool().description();
                return fresh;
            });
            if (o.kinds.contains(BackendKind.RELATIONAL) || !o.kinds.isEmpty()) {
                o.kindDescriptions.put(pt.kind(), pt.tool().description());
            }
            o.kinds.add(pt.kind());
            o.mergeSchema(pt.tool().inputSchema());
            o.candidates.addAll(cands);
        }
        JsonArray tools = new JsonArray();
        for (Offer o : offers.values()) {
            String description = o.description;
            if (!o.kindDescriptions.isEmpty()) {
                StringBuilder sb = new StringBuilder(description);
                for (var e : o.kindDescriptions.entrySet()) {
                    sb.append(" For ").append(e.getKey().id()).append(" backends: ").append(e.getValue());
                }
                description = sb.toString();
            }
            JsonObject props = o.properties.deepCopy();
            java.util.Set<String> required = new java.util.LinkedHashSet<>(o.required == null ? java.util.Set.of() : o.required);
            if (o.candidates.size() > 1) {
                StringBuilder names = new StringBuilder();
                for (McpBackend b : o.candidates) {
                    names.append(names.length() == 0 ? "" : ", ").append(b.name()).append(" (").append(b.type()).append(")");
                }
                boolean relationalDefault = o.kinds.contains(BackendKind.RELATIONAL);
                props.add("backend", stringSchema("Backend to run this tool against -- one of: " + names
                        + (relationalDefault ? ". Optional for relational backends (omitted = the gateway's own routing)."
                                : ". Required.")));
                if (!relationalDefault) {
                    required.add("backend");
                }
                description = description + " Applies to backends: " + names + ".";
            }
            JsonObject schema = new JsonObject();
            schema.addProperty("type", "object");
            schema.add("properties", props);
            if (!required.isEmpty()) {
                JsonArray req = new JsonArray();
                required.forEach(req::add);
                schema.add("required", req);
            }
            tools.add(toolDef(o.name, description, schema));
        }
        if (CURRENT_KINDS.get() == null) {
            tools.add(toolDef(LIST_BACKENDS, "List every backend this endpoint can reach: name, type "
                    + "(postgres, mysql, mongodb, dynamodb, s3, ...), the operator's description, group/set "
                    + "memberships, status, and which tools apply to it. Call this first to learn what is here.",
                    objectSchema(Map.of(), List.of())));
            tools.add(toolDef(DESCRIBE_BACKEND, "Describe one backend: its type, the operator's description and "
                    + "its live contents (tables and columns, collections, measurements, buckets and prefixes, "
                    + "topics ... whatever that type can list). \"backend\" may be omitted when the endpoint has "
                    + "exactly one backend.",
                    objectSchema(Map.of("backend", stringSchema("Backend name from list_backends")), List.of())));
        }
        JsonObject result = new JsonObject();
        result.add("tools", tools);
        return result;
    }

    private static String describeBackends(List<McpBackend> backends) {
        StringBuilder names = new StringBuilder();
        for (McpBackend b : backends) {
            names.append(names.length() == 0 ? "" : ", ").append(b.name()).append(" (").append(b.type()).append(")");
        }
        return names.toString();
    }

    private boolean routingError(HttpServletResponse response, JsonElement id, String message, boolean[] isError,
            String[] errorMessage) throws IOException {
        isError[0] = true;
        errorMessage[0] = message;
        writeResult(response, id, providerResult(BackendToolProvider.Outcome.error(message)));
        return true;
    }

    /**
     * Routes a tool call to a backend by the tool name and the optional {@code backend} argument.
     * Returns {@code true} when the response has been written (provider/discovery tools, routing
     * errors); {@code false} to fall through to the relational handlers (possibly after narrowing
     * the request scope to the one backend the caller named).
     *
     * <p>Rule: a tool call may carry {@code backend}. Relational tools never need it (omitted = the
     * gateway's own routing, exactly as before); for every other type it is optional when exactly
     * one in-scope backend supports the tool, and required (with an error listing the valid names)
     * when several do. A single-backend endpoint never needs it.
     */
    private boolean handleProviderCall(HttpServletResponse response, JsonElement id, String toolName,
            JsonObject arguments, com.sayonora.wire.core.AccessContext accessContext, boolean[] isError,
            String[] errorMessage) throws IOException {
        if (LIST_BACKENDS.equals(toolName) || DESCRIBE_BACKEND.equals(toolName)) {
            BackendToolProvider.Outcome o;
            try {
                o = LIST_BACKENDS.equals(toolName) ? listBackends() : describeBackend(arguments, accessContext);
            } catch (RuntimeException e) {
                o = BackendToolProvider.Outcome.error(String.valueOf(e.getMessage()));
            }
            isError[0] = o.isError();
            errorMessage[0] = o.isError() ? o.texts().get(0) : null;
            writeResult(response, id, providerResult(o));
            return true;
        }
        List<McpBackend> backends = backendsInScope();
        List<McpBackend> relational = backends.stream().filter(b -> b.kind() == BackendKind.RELATIONAL).toList();
        boolean relationalTool = !relational.isEmpty() && relationalToolNames().contains(toolName);

        // provider-tool candidates: (backend kind, bare tool name)
        BackendToolProvider provider = null;
        String bare = null;
        List<McpBackend> providerCands = new ArrayList<>();
        java.util.Set<BackendKind> candKinds = new java.util.LinkedHashSet<>();
        for (ProviderTool pt : providerTools(backends)) {
            if (pt.advertisedName().equals(toolName)) {
                provider = pt.provider();
                bare = pt.tool().name();
                candKinds.add(pt.kind());
                backends.stream().filter(b -> b.kind() == pt.kind()).forEach(providerCands::add);
            }
        }
        // a name offered by several families is dispatched by the chosen backend's family below
        if (provider == null && !relationalTool) {
            // known-unsupported names of a present kind get a clear tool error, not "unknown"
            java.util.Set<BackendKind> present = new java.util.LinkedHashSet<>();
            backends.forEach(b -> { if (b.kind() != BackendKind.RELATIONAL) present.add(b.kind()); });
            for (BackendKind k : present) {
                BackendToolProvider p = providers.get(k);
                String b = legacyPrefix() ? (toolName.startsWith(k.prefix()) ? toolName.substring(k.prefix().length()) : null) : toolName;
                McpBackend first = backends.stream().filter(x -> x.kind() == k).findFirst().orElse(null);
                if (p == null || b == null || first == null || p.tools().isEmpty()
                        || p.tools().stream().anyMatch(t -> t.name().equals(b))) {
                    continue;
                }
                try {
                    BackendToolProvider.Outcome o = p.call(b, arguments, unavailableCtx(first));
                    if (o.isError() && o.texts().get(0).startsWith("UnsupportedOperation")) {
                        isError[0] = true;
                        errorMessage[0] = o.texts().get(0);
                        writeResult(response, id, providerResult(o));
                        return true;
                    }
                } catch (Exception ignored) {
                    // not an unsupported-operation probe hit
                }
            }
            if (relational.isEmpty() || !effectiveKinds().contains(BackendKind.RELATIONAL)
                    || !relationalToolNames().contains(toolName) && !isRegisteredFunctionTool(toolName)) {
                if (!relational.isEmpty() && isRegisteredFunctionTool(toolName)) {
                    return false;
                }
                writeError(response, id, -32602, "Unknown tool: " + toolName);
                isError[0] = true;
                errorMessage[0] = "Unknown tool: " + toolName;
                return true;
            }
            return false;
        }

        String requested = arguments.has("backend") && arguments.get("backend").isJsonPrimitive()
                ? arguments.get("backend").getAsString() : null;
        McpBackend chosen = null;
        if (requested != null) {
            chosen = backends.stream().filter(b -> b.name().equals(requested)).findFirst().orElse(null);
            if (chosen == null) {
                // same message for "unknown" and "outside this endpoint's scope": never reveal names
                // of backends the endpoint cannot reach
                return routingError(response, id, "ERROR [42501]: backend \"" + requested + "\" is not available on "
                        + "this endpoint (unknown, or outside its scope). Valid backends: "
                        + describeBackends(backends), isError, errorMessage);
            }
            boolean applies = chosen.kind() == BackendKind.RELATIONAL ? relationalTool : candKinds.contains(chosen.kind());
            if (!applies) {
                List<McpBackend> ok = new ArrayList<>(providerCands);
                if (relationalTool) {
                    ok.addAll(0, relational);
                }
                return routingError(response, id, "tool \"" + toolName + "\" does not apply to backend \""
                        + requested + "\" (type " + chosen.type() + "). Backends this tool applies to: "
                        + describeBackends(ok), isError, errorMessage);
            }
        } else if (!relationalTool) {
            if (providerCands.size() == 1) {
                chosen = providerCands.get(0);
            } else {
                return routingError(response, id, "argument \"backend\" is required: tool \"" + toolName
                        + "\" applies to " + providerCands.size() + " backends in this endpoint's scope: "
                        + describeBackends(providerCands) + ". Pass \"backend\": \"<name>\".", isError, errorMessage);
            }
        }
        if (chosen == null || chosen.kind() == BackendKind.RELATIONAL) {
            if (chosen != null && options.mcpBackendMode() == McpBackendMode.POSTGRES
                    && currentScope().type() != McpScope.Type.DATABASE) {
                // narrow this request to the one relational backend the caller named
                CURRENT_SCOPE.set(McpScope.database(chosen.name()));
            }
            return false;
        }
        // provider tool on `chosen`
        BackendToolProvider p = providers.get(chosen.kind());
        String bareName = legacyPrefix() ? toolName.substring(chosen.kind().prefix().length()) : toolName;
        BackendToolProvider.Tool tool = p.tools().stream().filter(t -> t.name().equals(bareName)).findFirst().orElse(null);
        if (tool == null || tool.write() && providerReadOnly) {
            writeError(response, id, -32602, "Unknown tool: " + toolName);
            return true;
        }
        McpBackend target = chosen;
        BackendToolProvider.Ctx ctx = new BackendToolProvider.Ctx() {
            @Override
            public McpBackend backend() {
                return target;
            }

            @Override
            public AdHocQueryRunner.Result sql(String sql) {
                return runProviderSql(target, sql, accessContext);
            }
        };
        BackendToolProvider.Outcome outcome;
        try {
            outcome = p.call(bareName, arguments, ctx);
        } catch (Exception e) {
            outcome = BackendToolProvider.Outcome.error(String.valueOf(e.getMessage()));
        }
        isError[0] = outcome.isError();
        if (outcome.isError()) {
            errorMessage[0] = outcome.texts().get(0);
        }
        writeResult(response, id, providerResult(outcome));
        return true;
    }

    private static BackendToolProvider.Ctx unavailableCtx(McpBackend b) {
        return new BackendToolProvider.Ctx() {
            @Override
            public McpBackend backend() {
                return b;
            }

            @Override
            public AdHocQueryRunner.Result sql(String sql) {
                return AdHocQueryRunner.Result.ofError(new SQLException("unavailable"));
            }
        };
    }

    private boolean isRegisteredFunctionTool(String name) {
        return functionTools.stream().anyMatch(t -> t.toolName().equals(name));
    }

    private java.util.Set<String> relationalToolNames() {
        java.util.Set<String> names = new java.util.LinkedHashSet<>(java.util.List.of("query_federated",
                "document_schema", "explain_query", "query_natural_language", "inspect_schema"));
        for (JsonElement el : buildRelationalToolsListResult().getAsJsonArray("tools")) {
            names.add(el.getAsJsonObject().get("name").getAsString());
        }
        return names;
    }

    // ---- list_backends / describe_backend ----

    private static final int DESCRIBE_MAX_TABLES = 200;

    private java.util.List<String> applicableTools(McpBackend b) {
        java.util.List<String> names = new ArrayList<>();
        if (b.kind() == BackendKind.RELATIONAL) {
            names.addAll(relationalToolNames());
        } else {
            BackendToolProvider p = providers.get(b.kind());
            if (p != null) {
                for (BackendToolProvider.Tool t : p.tools()) {
                    if (!(t.write() && providerReadOnly)) {
                        names.add(legacyPrefix() ? b.kind().prefix() + t.name() : t.name());
                    }
                }
            }
        }
        return names;
    }

    private JsonObject backendJson(McpBackend b) {
        JsonObject o = new JsonObject();
        o.addProperty("name", b.name());
        o.addProperty("type", b.type());
        o.addProperty("family", b.kind().id());
        o.addProperty("engine", b.emulated() ? "warp-emulated" : "real");
        String hostName = b.emulated() ? b.host() : b.name();
        if (b.emulated()) {
            o.addProperty("host", b.host());
            com.sayonora.wire.core.StoreType st = storeTypeOf(b.kind());
            if (st != null) {
                List<String> hosts = backendRegistry.storeHosts(st);
                JsonArray hostArr = new JsonArray();
                hosts.forEach(hostArr::add);
                o.add("hostedOn", hostArr);
                o.addProperty("sharded", hosts.size() > 1);
            }
        }
        o.addProperty("backendSet", backendRegistry.setOf(hostName));
        JsonArray enabled = new JsonArray();
        if (!b.emulated()) {
            backendRegistry.enabledStores(b.name()).forEach(t -> enabled.add(t.id()));
            o.add("enabledStores", enabled);
        }
        String desc = backendRegistry.descriptionOf(b.name());
        o.addProperty("description", desc);
        BackendRegistry.BackendGroupInfo gi = backendRegistry.groupInfoFor(hostName);
        JsonArray groups = new JsonArray();
        if (gi != null && !BackendRegistry.UNGROUPED_GROUP_NAME.equals(gi.name())) {
            groups.add(gi.name());
        }
        o.add("groups", groups);
        JsonArray sets = new JsonArray();
        backendRegistry.backendSetsContaining(hostName).forEach(sets::add);
        o.add("sets", sets);
        o.addProperty("status", backendRegistry.stateOf(hostName).name());
        JsonArray tools = new JsonArray();
        applicableTools(b).forEach(tools::add);
        o.add("tools", tools);
        return o;
    }

    private static com.sayonora.wire.core.StoreType storeTypeOf(BackendKind k) {
        return switch (k) {
            case INFLUX -> com.sayonora.wire.core.StoreType.INFLUXDB;
            case MONGODB -> com.sayonora.wire.core.StoreType.MONGODB;
            case SQS -> com.sayonora.wire.core.StoreType.SQS;
            case NEO4J -> com.sayonora.wire.core.StoreType.NEO4J;
            case OPENSEARCH -> com.sayonora.wire.core.StoreType.OPENSEARCH;
            case DYNAMODB -> com.sayonora.wire.core.StoreType.DYNAMODB;
            case S3STORE -> com.sayonora.wire.core.StoreType.S3;
            case REDIS -> com.sayonora.wire.core.StoreType.REDIS;
            case AZBLOB -> com.sayonora.wire.core.StoreType.AZBLOB;
            case AZQUEUE -> com.sayonora.wire.core.StoreType.AZQUEUE;
            case AZTABLE -> com.sayonora.wire.core.StoreType.AZTABLE;
            case SNS -> com.sayonora.wire.core.StoreType.SNS;
            case KINESIS -> com.sayonora.wire.core.StoreType.KINESIS;
            case AWSPARAMS -> com.sayonora.wire.core.StoreType.AWSPARAMS;
            case FIRESTORE -> com.sayonora.wire.core.StoreType.FIRESTORE;
            case DATASTORE -> com.sayonora.wire.core.StoreType.DATASTORE;
            case GCS -> com.sayonora.wire.core.StoreType.GCS;
            case PUBSUB -> com.sayonora.wire.core.StoreType.PUBSUB;
            default -> null;
        };
    }

    /** SQL for a provider tool: on a store hosted by a non-default backend it must run ON that
     * backend (pinned + scoped to it, like DATABASE scope), otherwise the gateway's default one. */
    private AdHocQueryRunner.Result runProviderSql(McpBackend b, String sql,
            com.sayonora.wire.core.AccessContext accessContext) {
        if (b.emulated() && b.host() != null && !BackendRegistry.DEFAULT_BACKEND_NAME.equals(b.host())
                && options.mcpBackendMode() == McpBackendMode.POSTGRES
                && currentScope().type() != McpScope.Type.DATABASE) {
            com.sayonora.wire.core.BackendTarget t = backendRegistry.get(b.host());
            if (t != null) {
                try (Connection c = t.open()) {
                    return AdHocQueryRunner.run(c, sharedStages, backendRegistry, "default", sql, List.of(),
                            accessContext, null, t.name(), SourceDialect.POSTGRES, BackendScope.single(t.name()));
                } catch (SQLException e) {
                    return AdHocQueryRunner.Result.ofError(e);
                }
            }
        }
        try (Connection c = openBackendConnection()) {
            return runSql(c, sql, accessContext);
        } catch (SQLException e) {
            return AdHocQueryRunner.Result.ofError(e);
        }
    }

    private BackendToolProvider.Outcome listBackends() {
        List<McpBackend> backends = backendsInScope();
        McpScope sc = currentScope();
        JsonObject out = new JsonObject();
        JsonObject scopeJson = new JsonObject();
        scopeJson.addProperty("type", sc.type() == McpScope.Type.DATABASE ? "db"
                : sc.type().name().toLowerCase(java.util.Locale.ROOT));
        scopeJson.addProperty("name", sc.name());
        if (sc.type() == McpScope.Type.GROUP) {
            scopeJson.addProperty("description", backendRegistry.groupDescriptionOf(sc.name()));
        }
        out.add("scope", scopeJson);
        McpEndpoints.Endpoint ep = CURRENT_ENDPOINT.get();
        if (ep != null) {
            JsonObject e = new JsonObject();
            e.addProperty("name", ep.name());
            e.addProperty("description", ep.description());
            e.addProperty("expiresAt", ep.expiresAt() == null ? null : ep.expiresAt().toString());
            out.add("endpoint", e);
        }
        JsonArray arr = new JsonArray();
        java.util.Set<String> setNames = new java.util.LinkedHashSet<>();
        for (McpBackend b : backends) {
            JsonObject bj = backendJson(b);
            arr.add(bj);
            bj.getAsJsonArray("groups").forEach(g -> setNames.add("g:" + g.getAsString()));
            bj.getAsJsonArray("sets").forEach(g -> setNames.add("s:" + g.getAsString()));
        }
        out.addProperty("backendCount", backends.size());
        out.add("backends", arr);
        JsonArray sets = new JsonArray();
        for (String tagged : setNames) {
            String name = tagged.substring(2);
            JsonObject s = new JsonObject();
            s.addProperty("name", name);
            s.addProperty("kind", tagged.startsWith("g:") ? "group" : "set");
            s.addProperty("description", backendRegistry.groupDescriptionOf(name));
            JsonArray members = new JsonArray();
            List<String> memberNames = tagged.startsWith("g:") ? backendRegistry.membersOfGroup(name)
                    : backendRegistry.backendSets().getOrDefault(name, List.of());
            for (String m : memberNames) {
                if (backends.stream().anyMatch(b -> b.name().equals(m) || m.equals(b.host()))) {
                    members.add(m);
                }
            }
            s.add("members", members);
            sets.add(s);
        }
        out.add("backendSets", sets);
        return BackendToolProvider.Outcome.ok(GSON_NULLS.toJson(out));
    }

    private BackendToolProvider.Outcome describeBackend(JsonObject arguments,
            com.sayonora.wire.core.AccessContext accessContext) {
        List<McpBackend> backends = backendsInScope();
        String requested = arguments.has("backend") && arguments.get("backend").isJsonPrimitive()
                ? arguments.get("backend").getAsString()
                : arguments.has("name") && arguments.get("name").isJsonPrimitive() ? arguments.get("name").getAsString() : null;
        McpBackend b;
        if (requested == null) {
            if (backends.size() != 1) {
                return BackendToolProvider.Outcome.error("argument \"backend\" is required: this endpoint has "
                        + backends.size() + " backends: " + describeBackends(backends));
            }
            b = backends.get(0);
        } else {
            b = backends.stream().filter(x -> x.name().equals(requested)).findFirst().orElse(null);
            if (b == null) {
                return BackendToolProvider.Outcome.error("ERROR [42501]: backend \"" + requested + "\" is not "
                        + "available on this endpoint (unknown, or outside its scope). Valid backends: "
                        + describeBackends(backends));
            }
        }
        JsonObject out = backendJson(b);
        try {
            JsonObject contents;
            if (b.kind() == BackendKind.RELATIONAL) {
                contents = describeRelational(b);
            } else {
                BackendToolProvider p = providers.get(b.kind());
                if (p == null) {
                    throw new UnsupportedOperationException("UnsupportedOperation: Warp cannot list the contents of "
                            + b.type() + " backends yet");
                }
                McpBackend target = b;
                contents = p.describe(new BackendToolProvider.Ctx() {
                    @Override
                    public McpBackend backend() {
                        return target;
                    }

                    @Override
                    public AdHocQueryRunner.Result sql(String sql) {
                        return runProviderSql(target, sql, accessContext);
                    }
                });
            }
            out.add("contents", contents);
        } catch (Exception e) {
            out.add("contents", com.google.gson.JsonNull.INSTANCE);
            Throwable c = e.getCause() != null && e.getMessage() == null ? e.getCause() : e;
            out.addProperty("contentsNote", "contents could not be listed (" + (e instanceof UnsupportedOperationException
                    ? "" : "backend unreachable or access failed: ") + c.getMessage() + ")");
        }
        return BackendToolProvider.Outcome.ok(GSON_NULLS.toJson(out));
    }

    private static final java.util.Set<String> SYSTEM_SCHEMAS = java.util.Set.of("pg_catalog", "information_schema",
            "sys", "pg_toast", "mysql", "performance_schema", "INFORMATION_SCHEMA", "SYS", "SYSTEM");

    /** Tables and columns of one relational backend from its own JDBC metadata (system schemas
     * excluded, capped at {@value #DESCRIBE_MAX_TABLES} tables). */
    private JsonObject describeRelational(McpBackend b) throws SQLException {
        Map<String, JsonArray> tables = new LinkedHashMap<>();
        try (Connection conn = b.target().open()) {
            java.sql.DatabaseMetaData md = conn.getMetaData();
            List<String[]> names = new ArrayList<>();
            try (java.sql.ResultSet rs = md.getTables(null, null, "%", new String[] {"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String schema = rs.getString("TABLE_SCHEM");
                    if (schema != null && SYSTEM_SCHEMAS.contains(schema)) {
                        continue;
                    }
                    names.add(new String[] {schema, rs.getString("TABLE_NAME")});
                }
            }
            boolean truncated = names.size() > DESCRIBE_MAX_TABLES;
            for (String[] n : names.subList(0, Math.min(names.size(), DESCRIBE_MAX_TABLES))) {
                JsonArray cols = new JsonArray();
                try (java.sql.ResultSet rs = md.getColumns(null, n[0], n[1], "%")) {
                    while (rs.next()) {
                        JsonObject c = new JsonObject();
                        c.addProperty("name", rs.getString("COLUMN_NAME"));
                        c.addProperty("type", rs.getString("TYPE_NAME"));
                        c.addProperty("nullable", "YES".equals(rs.getString("IS_NULLABLE")));
                        cols.add(c);
                    }
                }
                tables.put((n[0] == null || n[0].isEmpty() ? "" : n[0] + ".") + n[1], cols);
            }
            JsonObject out = new JsonObject();
            out.addProperty("tableCount", names.size());
            out.addProperty("truncated", truncated);
            JsonArray arr = new JsonArray();
            tables.forEach((name, cols) -> {
                JsonObject t = new JsonObject();
                t.addProperty("table", name);
                t.add("columns", cols);
                arr.add(t);
            });
            out.add("tables", arr);
            return out;
        }
    }

    private static JsonObject providerResult(BackendToolProvider.Outcome outcome) {
        JsonObject callResult = new JsonObject();
        JsonArray content = new JsonArray();
        for (String text : outcome.texts()) {
            JsonObject item = new JsonObject();
            item.addProperty("type", "text");
            item.addProperty("text", text);
            content.add(item);
        }
        callResult.add("content", content);
        callResult.addProperty("isError", outcome.isError());
        return callResult;
    }

    /** The effective scope for whatever request is CURRENTLY being handled on this thread -- see
     * {@link #CURRENT_SCOPE}'s own javadoc. Falls back to this endpoint's own configured default
     * if read outside a request (defensive; every real call site sets it first). */
    private McpScope currentScope() {
        McpScope current = CURRENT_SCOPE.get();
        return current != null ? current : scope;
    }

    private JsonObject buildInitializeResult() {
        JsonObject result = new JsonObject();
        result.addProperty("protocolVersion", PROTOCOL_VERSION);
        JsonObject capabilities = new JsonObject();
        capabilities.add("tools", new JsonObject());
        result.add("capabilities", capabilities);
        JsonObject serverInfo = new JsonObject();
        serverInfo.addProperty("name", "warp");
        serverInfo.addProperty("version", "1.0");
        result.add("serverInfo", serverInfo);
        result.addProperty("instructions", "Call list_backends to see which backends (and of which type) this "
                + "endpoint reaches and what each contains; describe_backend shows one backend's live contents. "
                + "Tools apply to backends by type; pass \"backend\" when several backends support a tool.");
        return result;
    }

    private JsonObject buildRelationalToolsListResult() {
        String backendName = options.mcpBackendMode().name().charAt(0)
                + options.mcpBackendMode().name().substring(1).toLowerCase(java.util.Locale.ROOT);
        if (options.mcpBackendMode() == McpBackendMode.SQLSERVER) {
            backendName = "SQL Server";
        }
        JsonArray tools = new JsonArray();
        tools.add(toolDef("execute_sql", "Execute a SQL statement against the " + backendName + " backend and return the results.",
                objectSchema(Map.of("sql", stringSchema("The SQL statement to execute")), List.of("sql"))));
        tools.add(toolDef("list_tables", "List tables in the " + backendName + " backend (excludes system schemas).",
                objectSchema(Map.of(), List.of())));
        tools.add(toolDef("describe_table", "Describe a table's columns (name, type, nullability).",
                objectSchema(Map.of(
                        "table", stringSchema("Table name, optionally schema-qualified as schema.table"),
                        "schema", stringSchema("Schema name (default: " + defaultSchemaFor(options) + ")")),
                        List.of("table"))));
        // The data-investigation toolset -- structured, JSON-shaped database operations meant for
        // an agent (an SLM being trained or evaluated, in particular) to build up evidence about a
        // database step by step, rather than writing every query itself from scratch every time.
        // Same name as execute_sql -- run_sql is registered separately since that's the name this
        // toolset uses. All nine work in every WARP_MCP_BACKEND mode, with real per-dialect SQL
        // where the dialects genuinely differ (see DataInvestigationTools's own javadoc).
        tools.add(toolDef("run_sql", "Execute a SQL statement against the " + backendName
                        + " backend and return the results. Identical to execute_sql.",
                objectSchema(Map.of("sql", stringSchema("The SQL statement to execute")), List.of("sql"))));
        McpScope effectiveScope = currentScope();
        tools.add(effectiveScope.type() != McpScope.Type.ALL
                ? toolDef("inspect_schema",
                        "List every table and column this endpoint can see -- " + (effectiveScope.type() == McpScope.Type.DATABASE
                                ? "this caller is scoped to a single backend (\"" + effectiveScope.name() + "\")"
                                : "this caller is scoped to backend group \"" + effectiveScope.name() + "\"")
                                + "; there is nothing to choose -- every call always returns exactly this fixed scope, "
                                + "the same set of backends every other tool on this endpoint (execute_sql, "
                                + "query_federated) is limited to.",
                        objectSchema(Map.of(), List.of()))
                : toolDef("inspect_schema",
                        "List every table and column, scoped by the optional \"scope\" argument: \"current\" "
                                + "(default) -- just the " + backendName + " backend this session is connected to, "
                                + "excluding system schemas, the original behavior. \"group\" -- every backend in one "
                                + "named WARP_BACKEND_GROUPS group (pass \"group\": \"<name>\"), each row tagged with "
                                + "which backend it came from -- use this to see a whole shard set or a whole plain "
                                + "group at once, e.g. before writing a query meant to touch several backends in it. "
                                + "\"all\" -- every registered backend, also tagged by backend -- the full multi-backend "
                                + "catalog schema auto-discovery (query_federated) itself resolves against, so a query "
                                + "referencing a table this call shows can be trusted to actually resolve. \"current\" is "
                                + "a real SQL query with full column detail (name/type/nullability); \"group\"/\"all\" use "
                                + "real JDBC metadata directly (works across mixed engines in one group) with the same "
                                + "column detail.",
                        objectSchema(Map.of(
                                "scope", stringSchema("\"current\" (default), \"group\", or \"all\""),
                                "group", stringSchema("Required when scope is \"group\": the WARP_BACKEND_GROUPS name")),
                                List.of())));
        tools.add(toolDef("column_stats",
                "Statistical summary of one column: row count, null count, mean, standard deviation, "
                        + "min, max, and distinct-value count.",
                objectSchema(Map.of(
                        "table", stringSchema("Table name, optionally schema-qualified"),
                        "column", stringSchema("Column name")),
                        List.of("table", "column"))));
        tools.add(toolDef("compare_groups",
                "Aggregate a metric column grouped by another column, sorted by the aggregate value -- "
                        + "e.g. average order value by region.",
                objectSchema(Map.of(
                        "table", stringSchema("Table name, optionally schema-qualified"),
                        "group_by", stringSchema("Column to group by"),
                        "metric", stringSchema("Column to aggregate"),
                        "agg", stringSchema("avg, sum, count, min, or max (default avg)"),
                        "limit", stringSchema("Max groups to return (default 50, capped at 1000)")),
                        List.of("table", "group_by", "metric"))));
        tools.add(toolDef("correlation",
                "Pearson correlation coefficient between two numeric columns, plus the row count used.",
                objectSchema(Map.of(
                        "table", stringSchema("Table name, optionally schema-qualified"),
                        "col1", stringSchema("First column"),
                        "col2", stringSchema("Second column")),
                        List.of("table", "col1", "col2"))));
        tools.add(toolDef("sample_rows",
                "A representative sample of rows from a table.",
                objectSchema(Map.of(
                        "table", stringSchema("Table name, optionally schema-qualified"),
                        "limit", stringSchema("Row count to return (default 20, capped at 1000)")),
                        List.of("table"))));
        tools.add(toolDef("find_outliers",
                "Rows where a column's value deviates from the column's own mean by more than "
                        + "threshold standard deviations (z-score outlier detection), most extreme first.",
                objectSchema(Map.of(
                        "table", stringSchema("Table name, optionally schema-qualified"),
                        "column", stringSchema("Column to check")),
                        List.of("table", "column"))));
        tools.add(toolDef("find_join_path",
                "Find the shortest real foreign-key JOIN path between two tables, as a list of hops "
                        + "plus the ready-to-use JOIN SQL -- for a table relationship an agent hasn't seen yet.",
                objectSchema(Map.of(
                        "from_table", stringSchema("Starting table name"),
                        "to_table", stringSchema("Target table name")),
                        List.of("from_table", "to_table"))));
        tools.add(toolDef("explain_sql",
                "A real EXPLAIN plan for a SQL statement, without an LLM narration -- available in "
                        + "every backend mode (the Postgres-only, LLM-narrated explain tool is separate).",
                objectSchema(Map.of("sql", stringSchema("The SQL statement to explain")), List.of("sql"))));
        if (options.mcpBackendMode() != McpBackendMode.POSTGRES) {
            JsonObject result = new JsonObject();
            result.add("tools", tools);
            return result;
        }
        tools.add(toolDef("query_federated",
                "Execute a SQL statement that may span more than one of Warp's own backends, using "
                        + "plain, unqualified table names -- e.g. \"SELECT * FROM orders JOIN customers ON "
                        + "...\" where orders and customers live on two different real backends. No "
                        + "WARP_ROUTER_SCHEMA_RULES configuration is needed: every registered backend is "
                        + "auto-discovered (its real tables introspected) at call time, so an agent never "
                        + "needs to know which backend holds which table or an operator's schema-alias "
                        + "naming scheme. A table name found on more than one backend is refused with a "
                        + "clear error rather than guessed at -- qualify it via a real schema-rule alias "
                        + "in that case. Also honors any WARP_ROUTER_SCHEMA_RULES-declared federation and "
                        + "WARP_TABLE_SHARDS/WARP_SHARD_BACKENDS scatter-gather sharding already configured "
                        + "on this gateway. Routed through the same Apache Calcite-based planner a real "
                        + "Postgres/MySQL/SQL Server/Oracle client gets for the same cross-backend query. "
                        + "Identical to execute_sql for a query that only touches one backend; use this "
                        + "name when the query might cross backends, so the response can say whether it "
                        + "actually did and how.",
                objectSchema(Map.of("sql", stringSchema("The SQL statement to execute")), List.of("sql"))));
        tools.add(toolDef("document_schema",
                "List every table/column in the database (excludes system schemas) and, when an LLM "
                        + "provider is configured, generate a short plain-English data dictionary describing "
                        + "what each table likely represents and how tables relate via foreign keys. The raw "
                        + "table/column listing is always returned either way.",
                objectSchema(Map.of(), List.of())));
        tools.add(toolDef("explain_query",
                "Get a real Postgres EXPLAIN plan for a read-only SQL SELECT, plus a short plain-English "
                        + "narration of what the plan does (sequential scans, missing indexes, expensive "
                        + "sorts, etc.) when an LLM provider is configured -- the raw plan is always "
                        + "returned either way. Set analyze=true to run EXPLAIN ANALYZE instead (executes "
                        + "the query for real, timing information included) -- still SELECT-only.",
                objectSchema(Map.of(
                        "sql", stringSchema("The read-only SELECT statement to explain"),
                        "analyze", stringSchema("\"true\" to run EXPLAIN ANALYZE (executes the query); default false")),
                        List.of("sql"))));
        tools.add(toolDef("query_natural_language",
                "Ask a question in plain English. Drafts a read-only SQL SELECT via an LLM, has a second "
                        + "LLM pass judge (and correct, if needed) it against the schema and the question, then "
                        + "executes the judged SQL through the same firewall/QoS/cache pipeline every other tool "
                        + "uses and returns the result along with the SQL that actually ran. Never executes "
                        + "writes -- use execute_sql directly if a write is really intended. Requires an LLM "
                        + "provider configured (PUT /api/llm-config or WARP_LLM_*).",
                objectSchema(Map.of("question", stringSchema("The question to answer, in plain English")),
                        List.of("question"))));
        for (RegisteredFunctionTool tool : functionTools) {
            tools.add(toolDef(tool.toolName(), tool.description(), tool.inputSchema()));
        }
        JsonObject result = new JsonObject();
        result.add("tools", tools);
        return result;
    }

    private static JsonObject toolDef(String name, String description, JsonObject inputSchema) {
        JsonObject tool = new JsonObject();
        tool.addProperty("name", name);
        tool.addProperty("description", description);
        tool.add("inputSchema", inputSchema);
        return tool;
    }

    private static JsonObject stringSchema(String description) {
        JsonObject schema = new JsonObject();
        schema.addProperty("type", "string");
        schema.addProperty("description", description);
        return schema;
    }

    private static JsonObject objectSchema(Map<String, JsonObject> properties, List<String> required) {
        JsonObject schema = new JsonObject();
        schema.addProperty("type", "object");
        JsonObject props = new JsonObject();
        properties.forEach(props::add);
        schema.add("properties", props);
        if (!required.isEmpty()) {
            JsonArray req = new JsonArray();
            required.forEach(req::add);
            schema.add("required", req);
        }
        return schema;
    }

    private void handleToolsCall(HttpServletResponse response, JsonElement id, JsonObject params,
            com.sayonora.wire.core.AccessContext accessContext) throws IOException {
        if (!params.has("name")) {
            writeError(response, id, -32602, "Invalid params: missing tool name");
            return;
        }
        String toolName = params.get("name").getAsString();
        JsonObject arguments = params.has("arguments") && params.get("arguments").isJsonObject()
                ? params.getAsJsonObject("arguments") : new JsonObject();

        long startNanos = System.nanoTime();
        boolean isError = true;
        String errorMessage = null;
        McpBackendMode backendMode = options.mcpBackendMode();
        boolean[] providerError = {true};
        String[] providerMessage = {null};
        try {
            if (handleProviderCall(response, id, toolName, arguments, accessContext, providerError, providerMessage)) {
                metrics.record(toolName, System.nanoTime() - startNanos, providerError[0]);
                if (auditLog != null) {
                    recordToolCallAudit(toolName, arguments, accessContext, providerError[0], providerMessage[0],
                            System.nanoTime() - startNanos);
                }
                return;
            }
        } catch (RuntimeException e) {
            metrics.record(toolName, System.nanoTime() - startNanos, true);
            writeError(response, id, -32602, e.getMessage());
            return;
        }
        try (Connection backend = openBackendConnection()) {
            if ("query_natural_language".equals(toolName) || "explain_query".equals(toolName)
                    || "document_schema".equals(toolName) || "query_federated".equals(toolName)) {
                // query_natural_language/document_schema/explain_query all hardcode Postgres-only
                // SQL (see their own long-standing comment below); query_federated is Postgres-only
                // for a different reason -- Warp's own shard/backend routing (RouterStage,
                // SchemaFederationStage) only exists in that mode. Native mode is bound to exactly
                // one real backend of one real engine, so "which shard did this touch" has no
                // meaning there -- execute_sql already IS the right (and only) tool in that mode.
                // Refusing cleanly here beats silently running SQL that's simply wrong for the
                // configured backend.
                ResultWithNote outcome = backendMode != McpBackendMode.POSTGRES
                        ? new ResultWithNote(notSupportedInNativeMode(toolName, backendMode), null)
                        : switch (toolName) {
                            case "query_natural_language" -> runNaturalLanguageQuery(backend, requireString(arguments, "question"), accessContext);
                            case "explain_query" -> runExplainQuery(backend, requireString(arguments, "sql"),
                                    arguments.has("analyze") && "true".equalsIgnoreCase(arguments.get("analyze").getAsString()), accessContext);
                            case "query_federated" -> runFederatedQuery(backend, requireString(arguments, "sql"), accessContext);
                            default -> runDocumentSchema(backend, accessContext);
                        };
                isError = !outcome.result().success();
                errorMessage = outcome.result().error();
                writeResult(response, id, toolCallResult(outcome.result(), outcome.note()));
            } else if ("find_join_path".equals(toolName)) {
                // Its own JSON shape (a hop list), not the tabular {columns, rows} every other
                // tool here returns -- see runFindJoinPath's own javadoc.
                JsonObject pathResult = runFindJoinPath(backend, requireString(arguments, "from_table"),
                        requireString(arguments, "to_table"));
                isError = false;
                writeResult(response, id, pathResult);
            } else {
                AdHocQueryRunner.Result result = switch (toolName) {
                    case "execute_sql", "run_sql" -> runSql(backend, requireString(arguments, "sql"), accessContext);
                    case "list_tables" -> runListTables(backend, accessContext);
                    case "describe_table" -> runDescribeTable(backend, arguments, accessContext);
                    case "inspect_schema" -> runInspectSchema(backend, arguments, accessContext);
                    case "column_stats" -> runSql(backend, DataInvestigationTools.columnStatsSql(backendMode,
                            requireString(arguments, "table"), requireString(arguments, "column")), accessContext);
                    case "compare_groups" -> runSql(backend, DataInvestigationTools.compareGroupsSql(backendMode,
                            requireString(arguments, "table"), requireString(arguments, "group_by"),
                            requireString(arguments, "metric"),
                            arguments.has("agg") ? arguments.get("agg").getAsString() : "avg",
                            positiveIntArg(arguments, "limit", 50, 1000)), accessContext);
                    case "correlation" -> runSql(backend, DataInvestigationTools.correlationSql(backendMode,
                            requireString(arguments, "table"), requireString(arguments, "col1"),
                            requireString(arguments, "col2")), accessContext);
                    case "sample_rows" -> runSql(backend, DataInvestigationTools.sampleRowsSql(backendMode,
                            requireString(arguments, "table"),
                            positiveIntArg(arguments, "limit", 20, 1000)), accessContext);
                    case "find_outliers" -> runSql(backend, DataInvestigationTools.findOutliersSql(backendMode,
                            requireString(arguments, "table"), requireString(arguments, "column"),
                            arguments.has("threshold") ? arguments.get("threshold").getAsDouble() : 3.0,
                            positiveIntArg(arguments, "limit", 50, 1000)), accessContext);
                    case "explain_sql" -> runExplainSql(backend, requireString(arguments, "sql"));
                    default -> runRegisteredTool(backend, toolName, arguments, accessContext);
                };
                isError = !result.success();
                errorMessage = result.error();
                writeResult(response, id, toolCallResult(result));
            }
        } catch (RuntimeException | java.sql.SQLException e) {
            errorMessage = e.getMessage();
            writeError(response, id, -32602, e.getMessage());
        } finally {
            long elapsedNanos = System.nanoTime() - startNanos;
            metrics.record(toolName, elapsedNanos, isError);
            if (auditLog != null) {
                recordToolCallAudit(toolName, arguments, accessContext, isError, errorMessage, elapsedNanos);
            }
        }
    }

    /** One real per-call audit event -- see the full constructor's own javadoc for why this
     * matters beyond just {@code metrics}' aggregate counters. {@code arguments} is truncated the
     * same way {@code SqlMetricsCollector.normalize} caps SQL text, for the same reason: an
     * {@code execute_sql} call's own {@code sql} argument can be arbitrarily long, and an audit
     * summary is a place to know WHAT ran, not to store the full statement text a second time. */
    private void recordToolCallAudit(String toolName, JsonObject arguments,
            com.sayonora.wire.core.AccessContext accessContext, boolean isError, String errorMessage, long elapsedNanos) {
        String userId = accessContext == null || accessContext.isAnonymous() ? "anonymous" : accessContext.userId();
        String argsText = arguments.toString();
        if (argsText.length() > 300) {
            argsText = argsText.substring(0, 300) + "…";
        }
        Map<String, String> details = new java.util.LinkedHashMap<>();
        details.put("tool", toolName);
        McpEndpoints.Endpoint auditEndpoint = CURRENT_ENDPOINT.get();
        if (auditEndpoint != null) {
            details.put("endpoint", auditEndpoint.name());
        }
        details.put("arguments", argsText);
        details.put("success", String.valueOf(!isError));
        details.put("elapsedMs", String.valueOf(elapsedNanos / 1_000_000));
        if (isError && errorMessage != null) {
            details.put("error", errorMessage);
        }
        String summary = "MCP tool \"" + toolName + "\" " + (isError ? "failed" : "succeeded");
        auditLog.record(com.sayonora.wire.audit.AuditEvent.of(
                com.sayonora.wire.audit.AuditEvent.Type.MCP_TOOL_CALLED, userId, summary, details));
    }

    /** Opens the connection every tool call in {@link #handleToolsCall} runs against -- Postgres
     * by default (unchanged), or a real Oracle/MySQL/SQL Server connection of the gateway's own
     * when {@code WARP_MCP_BACKEND} names one, mirroring orawire/mywire/mssqlwire's own
     * native-backend-mode connection choice. See {@link ServerOptions.McpBackendMode}'s own
     * javadoc for the full picture, including why MCP needs its own gateway-held Oracle credential
     * where orawire's native mode doesn't. */
    private Connection openBackendConnection() throws SQLException {
        // McpScope.DATABASE: open the NAMED backend directly, not the gateway's own default.
        // runSql then pins every statement to this same name (RoutingBackendExecutor runs it on
        // THIS connection) and scopes it to that one backend -- see McpScope's own javadoc.
        // currentScope(), not the field -- a per-token "warp_scope" claim or role mapping can make
        // THIS caller's effective scope different from the endpoint's own configured default.
        McpScope effectiveScope = currentScope();
        if (effectiveScope.type() == McpScope.Type.DATABASE) {
            com.sayonora.wire.core.BackendTarget target = backendRegistry.get(effectiveScope.name());
            if (target == null) {
                throw new SQLException("WARP_MCP_SCOPE names backend \"" + effectiveScope.name()
                        + "\", which is not currently registered", "08001");
            }
            return target.open();
        }
        return switch (options.mcpBackendMode()) {
            case ORACLE -> OracleJdbcConnections.open(options);
            case MYSQL -> MySqlBackendConnections.open(options);
            case SQLSERVER -> MssqlBackendConnections.open(options);
            case POSTGRES -> PgConnections.open(options);
        };
    }

    private static SourceDialect dialectFor(McpBackendMode mode) {
        return switch (mode) {
            case ORACLE -> SourceDialect.ORACLE;
            case MYSQL -> SourceDialect.MYSQL;
            case SQLSERVER -> SourceDialect.SQL_SERVER;
            case POSTGRES -> SourceDialect.MCP;
        };
    }

    private AdHocQueryRunner.Result runSql(Connection backend, String sql,
            com.sayonora.wire.core.AccessContext accessContext) {
        return runSql(backend, sql, List.of(), accessContext);
    }

    /**
     * Same execution path {@code execute_sql}/{@code run_sql} already use -- {@link #runSql}
     * calls {@link AdHocQueryRunner#run}, which builds a real {@link
     * com.sayonora.wire.core.RoutingBackendExecutor} wired with {@code .withFederationSupport(...)}
     * off the SAME {@code sharedStages}/{@code backendRegistry} every wire-protocol frontend's own
     * session handler uses -- a cross-shard JOIN sent through {@code execute_sql} already gets
     * planned by {@link com.sayonora.wire.core.SchemaFederationStage}'s Calcite planner today. This
     * tool exists for a real, separate reason: naming and provenance, not new query capability. An
     * agent calling generic {@code execute_sql} has no signal that federation is even a thing this
     * gateway does, and the plain result carries no indication of whether cross-shard planning
     * actually fired. {@code query_federated} makes the capability discoverable via {@code
     * tools/list}'s own description, and its note tells the caller whether this deployment has any
     * shard group configured at all -- so "why didn't federation happen" (no shards configured,
     * most commonly) is visible instead of silently indistinguishable from "it happened and there
     * was only one shard's worth of data anyway."
     */
    // Not cached, unlike SchemaAutoDiscoveryStage's own BackendCatalogCache -- an MCP tool call is
    // comparatively rare, so a fresh BackendCatalogDiscovery.discoverAll per call is tolerable here
    // in a way it isn't for wire-protocol traffic (see that stage's own javadoc for why it needs
    // the cache and this doesn't).
    private ResultWithNote runFederatedQuery(Connection backend, String sql,
            com.sayonora.wire.core.AccessContext accessContext) {
        // scopedDiscoveredTables(), not the raw discoverAll -- a DATABASE/GROUP-scoped endpoint's
        // query_federated must never be able to resolve a table onto a backend outside its own
        // McpScope, or the "endpoint-level boundary" this class exists for would be fake for
        // exactly the tool that most needs it enforced.
        List<BackendCatalogDiscovery.DiscoveredTable> discovered = scopedDiscoveredTables();
        com.sayonora.wire.core.SchemaAutoDiscovery.Resolution auto = com.sayonora.wire.core.SchemaAutoDiscovery.resolve(
                sql, backendRegistry, BackendCatalogDiscovery.byTableNameLowercase(discovered),
                RouterStage.tableShardBackendNames(RouterStage.tableShardRulesIn(sharedStages)));
        if (auto.ambiguous()) {
            return new ResultWithNote(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0,
                    "42P09", "table \"" + auto.ambiguousTable() + "\" is ambiguous: " + auto.ambiguousMessage()), null);
        }
        AdHocQueryRunner.Result result;
        String autoDiscoveryNote;
        if (auto.federated()) {
            try {
                Statement statement = new Statement("default", SourceDialect.MCP, auto.rewrittenSql(), List.of(),
                        "default", null, accessContext);
                ExecutionResult execResult = new SchemaFederationStage(List.of(), backendRegistry)
                        .executeWithMounts(auto.mounts(), statement);
                result = AdHocQueryRunner.Result.ofSuccess(execResult);
            } catch (SQLException e) {
                result = AdHocQueryRunner.Result.ofError(e);
            }
            autoDiscoveryNote = "Auto-discovered and federated across " + auto.mounts().size()
                    + " backend(s) with NO WARP_ROUTER_SCHEMA_RULES configuration needed: "
                    + String.join(", ", auto.mounts().keySet()) + ".";
        } else {
            result = runSql(backend, sql, accessContext);
            autoDiscoveryNote = null;
        }

        // A SEPARATE, config-declared mechanism can also make a query cross backends -- see
        // SchemaFederationStage's own javadoc distinguishing its heterogeneous "each backend holds
        // a different, complete table" (schema rules) case from ShardJoinExecutor's homogeneous
        // "same table, row-partitioned" (shard group) one. Reported here too so the note is honest
        // about every mechanism in play, not just auto-discovery.
        List<String> shardGroup = backendRegistry.shardGroup();
        List<RouterStage.SchemaRule> schemaRules = RouterStage.schemaRulesIn(sharedStages);
        List<String> configured = new ArrayList<>();
        if (!shardGroup.isEmpty()) {
            configured.add(shardGroup.size() + " shard(s) in the scatter/shard group: " + String.join(", ", shardGroup));
        }
        if (!schemaRules.isEmpty()) {
            List<String> schemaNames = schemaRules.stream().map(RouterStage.SchemaRule::schemaName).toList();
            configured.add(schemaRules.size() + " schema-federation rule(s): " + String.join(", ", schemaNames));
        }
        String configuredNote = configured.isEmpty()
                ? (autoDiscoveryNote == null
                        ? "No shard group or schema-federation rule is configured on this gateway "
                                + "(WARP_SHARD_BACKENDS/WARP_TABLE_SHARDS or WARP_ROUTER_SCHEMA_RULES), and "
                                + "auto-discovery found this query touches at most one backend -- it ran against "
                                + "the single default backend, the same as execute_sql would have."
                        : null)
                : "Also configured: " + String.join("; ", configured) + ".";
        String note = java.util.stream.Stream.of(autoDiscoveryNote, configuredNote)
                .filter(java.util.Objects::nonNull)
                .collect(java.util.stream.Collectors.joining(" "));
        return new ResultWithNote(result, note.isBlank() ? null : note);
    }

    /**
     * Every tool-call SQL path runs through the FULL shared pipeline (firewall, workload capture,
     * federation, router, QoS, translation, rollup, cache, stats, repair) -- including the two
     * paths that used to bypass it entirely via a bare {@code JdbcBackendExecutor}: DATABASE scope
     * and native (non-Postgres) backend mode. What changed is HOW those two are made safe/correct:
     * instead of skipping the pipeline, the Statement is (a) pinned to the registered name of the
     * backend {@code backend} actually is (so {@code RouterStage} honours it rather than applying
     * a rule), (b) tagged with that target's REAL dialect (so {@code DialectTranslationStage}
     * no-ops -- no wrong Postgres-ward translation of real Oracle/MySQL/SQL Server SQL), and
     * (c) carries a {@link BackendScope} enforced at every backend-resolution point (router,
     * terminal executor incl. cursor/scatter/shard-join targets, both federation stages). See
     * {@link #executionTargetFor} for the per-scope/per-mode mapping and {@link McpScope}'s own
     * javadoc for the enforcement picture. Postgres + ALL scope is byte-for-byte what it always
     * was (no pin, {@code SourceDialect.MCP}, no scope).
     *
     * <p>Tenant is always {@code "default"} -- QoS buckets are keyed {@code tenantId:workloadClass},
     * so MCP traffic (any scope/mode) shares them with pgwire's own; deliberately NOT a per-user
     * tenant (unbounded bucket cardinality). Noted on {@link McpScope} too.
     */
    private AdHocQueryRunner.Result runSql(Connection backend, String sql, List<Object> bindParams,
            com.sayonora.wire.core.AccessContext accessContext) {
        ExecutionTarget target;
        try {
            target = executionTargetFor(currentScope());
        } catch (SQLException e) {
            return AdHocQueryRunner.Result.ofError(e);
        }
        return AdHocQueryRunner.run(backend, sharedStages, backendRegistry, "default", sql, bindParams, accessContext,
                null, target.pinnedBackend(), target.dialect(), target.scope());
    }

    /** How one {@link McpScope} (plus the endpoint's own {@code WARP_MCP_BACKEND} mode) maps onto
     * the pipeline's routing pin, source dialect, and enforced {@link BackendScope} -- see
     * {@link #runSql}. {@code pinnedBackend}/{@code scope} are nullable ("no pin"/"unconstrained"). */
    private record ExecutionTarget(String pinnedBackend, SourceDialect dialect, BackendScope scope) {
    }

    private ExecutionTarget executionTargetFor(McpScope effectiveScope) throws SQLException {
        if (effectiveScope.type() == McpScope.Type.DATABASE) {
            // openBackendConnection opened THIS named backend directly; pin to it, speak its real
            // dialect, and permit nothing else.
            String name = effectiveScope.name();
            com.sayonora.wire.core.BackendTarget target = backendRegistry.get(name);
            SourceDialect dialect = target != null && target.dialect() != null
                    ? target.dialect() : dialectFor(options.mcpBackendMode());
            return new ExecutionTarget(name, dialect, BackendScope.single(name));
        }
        if (options.mcpBackendMode() != McpBackendMode.POSTGRES) {
            // Native mode (any non-DATABASE scope): the connection is the gateway's own Oracle/
            // MySQL/SQL Server one, registered by Main as MCP_NATIVE_DEFAULT_NAME (same URL, same
            // pool). Pinned there so no WARP_ROUTER_* rule can send real native SQL to Postgres.
            return new ExecutionTarget(BackendRegistry.MCP_NATIVE_DEFAULT_NAME, dialectFor(options.mcpBackendMode()),
                    BackendScope.single(BackendRegistry.MCP_NATIVE_DEFAULT_NAME));
        }
        if (effectiveScope.type() == McpScope.Type.GROUP) {
            // Not pinned -- routing rules still apply within the group -- but every resolved
            // target (incl. the no-rule "default" fallback) must be a member.
            List<String> members = backendRegistry.membersOfGroup(effectiveScope.name());
            if (members.isEmpty()) {
                throw new SQLException("WARP_MCP_SCOPE names group \"" + effectiveScope.name()
                        + "\", which has no currently-registered member backends", "08001");
            }
            return new ExecutionTarget(null, SourceDialect.MCP,
                    new BackendScope(new java.util.HashSet<>(members), "group:" + effectiveScope.name()));
        }
        // Postgres + ALL: exactly today's unpinned, unscoped Statement.
        return new ExecutionTarget(null, SourceDialect.MCP, null);
    }

    private static AdHocQueryRunner.Result notSupportedInNativeMode(String toolName, McpBackendMode mode) {
        return new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "0A000",
                toolName + " isn't supported with WARP_MCP_BACKEND=" + mode.name().toLowerCase(java.util.Locale.ROOT)
                        + " yet -- it hardcodes Postgres-specific SQL (EXPLAIN syntax, an LLM schema-drafting "
                        + "prompt, or both). execute_sql, list_tables, and describe_table all work natively; "
                        + "use execute_sql directly for anything this tool would have run.");
    }

    private record ResultWithNote(AdHocQueryRunner.Result result, String note) {
    }

    /**
     * "Draft, judge, execute" -- the {@code NL2SQL_QUERY_EXECUTED}/{@code NL2SQL_JUDGE_CORRECTED}
     * audit event types existed in {@link com.sayonora.wire.audit.AuditEvent.Type} unused before
     * this method, which is what this tool actually is: an LLM drafts a Postgres SELECT from
     * plain English, a SECOND, independent LLM call judges that draft against the schema and the
     * original question (and can correct it), then the judged SQL runs through the exact same
     * {@link AdHocQueryRunner#run} pipeline every other tool uses -- firewall, QoS, dialect
     * translation, cache, stats -- never a bypass.
     *
     * <p>The one thing that's deterministic here, not LLM-decided: {@link #isReadOnlySelect}
     * refuses anything that isn't a plain read before it's ever executed, regardless of what
     * either LLM call said -- this tool never runs a write, full stop. That's on top of, not
     * instead of, the real firewall every statement still passes through.
     */
    private ResultWithNote runNaturalLanguageQuery(Connection backend, String question,
            com.sayonora.wire.core.AccessContext accessContext) {
        com.sayonora.wire.core.TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
        if (llmClient == null) {
            return new ResultWithNote(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "58000",
                    "no LLM provider configured -- set it via PUT /api/llm-config or the WARP_LLM_* env vars "
                            + "before using query_natural_language"), null);
        }

        String schemaContext = introspectSchemaContext(backend);
        String draftedSql;
        try {
            draftedSql = llmClient.draftSqlFromNaturalLanguage("Schema:\n" + schemaContext + "\n\nQuestion: " + question);
        } catch (Exception e) {
            return new ResultWithNote(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "58000",
                    "LLM SQL drafting failed: " + e.getMessage()), null);
        }

        String finalSql = draftedSql;
        boolean corrected = false;
        String reasoning = null;
        try {
            String rawVerdict = llmClient.judgeSql("Schema:\n" + schemaContext + "\n\nQuestion: " + question
                    + "\n\nDrafted SQL:\n" + draftedSql);
            JsonObject verdict = JsonParser.parseString(rawVerdict).getAsJsonObject();
            if (verdict.has("sql") && verdict.get("sql").isJsonPrimitive()) {
                finalSql = verdict.get("sql").getAsString();
            }
            corrected = verdict.has("corrected") && verdict.get("corrected").isJsonPrimitive()
                    && verdict.get("corrected").getAsJsonPrimitive().isBoolean() && verdict.get("corrected").getAsBoolean();
            reasoning = verdict.has("reasoning") && verdict.get("reasoning").isJsonPrimitive()
                    ? verdict.get("reasoning").getAsString() : null;
        } catch (Exception e) {
            // The judge is a safety/quality improvement, not a hard dependency -- if it fails
            // (bad JSON, HTTP error) run the un-judged draft rather than refusing the whole
            // request, same "narrate/verify, don't block on it" tolerance QueryRepairStage's own
            // LLM-failure handling uses.
            log.warn("nl2sql: judge step failed ({}) -- running the un-judged draft as-is", e.getMessage());
        }

        if (!isReadOnlySelect(finalSql)) {
            return new ResultWithNote(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "42501",
                    "the drafted/judged SQL is not a read-only SELECT -- query_natural_language never "
                            + "executes writes; use execute_sql directly if a write is really intended: " + finalSql),
                    null);
        }

        AdHocQueryRunner.Result result = runSql(backend, finalSql, accessContext);
        if (auditLog != null) {
            recordNl2SqlAudit(question, draftedSql, finalSql, corrected, reasoning, result, accessContext);
        }
        String note = "Executed SQL: " + finalSql
                + (corrected ? "\nThe judge corrected the drafted SQL" + (reasoning == null ? "." : ": " + reasoning) : "");
        return new ResultWithNote(result, note);
    }

    /**
     * Lists every table/column (a real, executed, firewall/QoS/stats-covered SQL statement via
     * {@link #runSql}, not a bypass) and, when an LLM is configured, has it generate a short
     * plain-English data dictionary on top -- pure narration of the real schema, same "always
     * return the real fact, LLM commentary is purely additive" shape {@link #runExplainQuery}
     * uses. The narrative is built from a richer context than the returned {@code Result} alone
     * (adds foreign-key relationships via {@link #introspectForeignKeys}, gathered the same
     * non-pipeline "just context" way {@link #introspectSchemaContext} already does) since
     * describing how tables relate needs that, not just their own columns.
     */
    private ResultWithNote runDocumentSchema(Connection backend, com.sayonora.wire.core.AccessContext accessContext) {
        AdHocQueryRunner.Result result = runSql(backend,
                "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
                        + "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                        + "ORDER BY table_schema, table_name, ordinal_position", accessContext);
        if (!result.success()) {
            return new ResultWithNote(result, null);
        }
        com.sayonora.wire.core.TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
        if (llmClient == null) {
            return new ResultWithNote(result, null);
        }
        String schemaContext = introspectSchemaContext(backend) + "\n\n" + introspectForeignKeys(backend);
        try {
            String documentation = llmClient.documentSchema(schemaContext);
            return new ResultWithNote(result, documentation);
        } catch (Exception e) {
            log.warn("document_schema: LLM documentation failed ({}) -- returning the raw table/column listing only",
                    e.getMessage());
            return new ResultWithNote(result, null);
        }
    }

    /**
     * {@code EXPLAIN (FORMAT JSON[, ANALYZE, BUFFERS]) <sql>}, run through the same {@link
     * AdHocQueryRunner#run} pipeline every other tool uses (firewall, QoS, translation, stats --
     * not a bypass), then optionally narrated by the LLM. The safest tool in this series: pure
     * narration of a real fact Postgres itself computed, nothing for the LLM to decide and
     * nothing to validate afterward -- the raw plan is always returned, with or without an LLM
     * configured; the narrative is purely additive when one is.
     *
     * <p>{@code isReadOnlySelect} still gates this the same way {@link #runNaturalLanguageQuery}
     * is gated: {@code analyze=true} genuinely EXECUTES the statement (that's what {@code
     * ANALYZE} means), so refusing anything but a real read here matters just as much as it does
     * there, for the same reason.
     */
    private ResultWithNote runExplainQuery(Connection backend, String sql, boolean analyze,
            com.sayonora.wire.core.AccessContext accessContext) {
        if (!isReadOnlySelect(sql)) {
            return new ResultWithNote(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "42501",
                    "explain_query only accepts a read-only SELECT -- analyze=true genuinely executes the "
                            + "statement, so this is not the tool for anything else: " + sql), null);
        }
        String explainSql = "EXPLAIN (FORMAT JSON" + (analyze ? ", ANALYZE, BUFFERS" : "") + ") " + sql;
        AdHocQueryRunner.Result result = runSql(backend, explainSql, accessContext);
        if (!result.success() || result.rows().isEmpty() || result.rows().get(0).isEmpty()) {
            return new ResultWithNote(result, null);
        }
        String planJson = String.valueOf(result.rows().get(0).get(0));

        com.sayonora.wire.core.TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
        if (llmClient == null) {
            return new ResultWithNote(result, null);
        }
        try {
            String narrative = llmClient.narrateExplainPlan(sql, planJson);
            return new ResultWithNote(result, narrative);
        } catch (Exception e) {
            log.warn("explain_query: LLM narration failed ({}) -- returning the raw plan only", e.getMessage());
            return new ResultWithNote(result, null);
        }
    }

    /** Only what an LLM needs to draft/judge plausible SQL -- table and column names/types, not a
     * full pg_catalog dump. Capped at 400 columns total so a very wide database doesn't blow out
     * the prompt; a real database with more than that has bigger problems for this tool than a
     * truncated schema summary. Uses {@code backend} directly (a plain query, not routed through
     * {@code sharedStages}) since this is context-gathering, not a monitored/audited operation in
     * its own right -- the real, audited operation is the SQL this context leads to. */
    private static String introspectSchemaContext(Connection backend) {
        StringBuilder sb = new StringBuilder();
        String lastTable = null;
        try (var st = backend.createStatement();
                var rs = st.executeQuery(
                        "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
                                + "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                                + "ORDER BY table_schema, table_name, ordinal_position LIMIT 400")) {
            while (rs.next()) {
                String table = rs.getString(1) + "." + rs.getString(2);
                if (!table.equals(lastTable)) {
                    if (lastTable != null) {
                        sb.append('\n');
                    }
                    sb.append(table).append(": ");
                    lastTable = table;
                } else {
                    sb.append(", ");
                }
                sb.append(rs.getString(3)).append(' ').append(rs.getString(4));
            }
        } catch (java.sql.SQLException e) {
            log.warn("nl2sql: schema introspection failed ({}) -- drafting without schema context", e.getMessage());
            return "(schema introspection failed: " + e.getMessage() + ")";
        }
        return sb.length() == 0 ? "(no user tables found)" : sb.toString();
    }

    /** Real foreign-key relationships -- what {@link #introspectSchemaContext} can't show (each
     * table's own columns say nothing about how tables relate to EACH OTHER), and the specific
     * thing {@code document_schema}'s narrative most needs beyond a bare column list. Capped at
     * 200 for the same "a schema this wide has bigger problems than a truncated summary" reasoning
     * {@link #introspectSchemaContext}'s own 400-column cap uses. */
    private static String introspectForeignKeys(Connection backend) {
        StringBuilder sb = new StringBuilder();
        try (var st = backend.createStatement();
                var rs = st.executeQuery(
                        "SELECT tc.table_schema, tc.table_name, kcu.column_name, "
                                + "ccu.table_schema, ccu.table_name, ccu.column_name "
                                + "FROM information_schema.table_constraints tc "
                                + "JOIN information_schema.key_column_usage kcu "
                                + "  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
                                + "JOIN information_schema.constraint_column_usage ccu "
                                + "  ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema "
                                + "WHERE tc.constraint_type = 'FOREIGN KEY' "
                                + "  AND tc.table_schema NOT IN ('pg_catalog', 'information_schema') "
                                + "ORDER BY tc.table_schema, tc.table_name LIMIT 200")) {
            sb.append("Foreign keys:\n");
            boolean any = false;
            while (rs.next()) {
                any = true;
                sb.append(rs.getString(1)).append('.').append(rs.getString(2)).append('.').append(rs.getString(3))
                        .append(" -> ").append(rs.getString(4)).append('.').append(rs.getString(5)).append('.')
                        .append(rs.getString(6)).append('\n');
            }
            if (!any) {
                sb.append("(none)\n");
            }
        } catch (java.sql.SQLException e) {
            log.warn("document_schema: foreign-key introspection failed ({}) -- documenting without them", e.getMessage());
            return "Foreign keys: (introspection failed: " + e.getMessage() + ")";
        }
        return sb.toString();
    }

    /** Deterministic, not LLM-decided -- see {@link #runNaturalLanguageQuery}'s own javadoc for
     * why. Doesn't need to be airtight against every disguised write (a WITH ... INSERT CTE, say)
     * to be worth having: it's a first-line check on top of the real firewall every statement
     * still passes through via {@link AdHocQueryRunner#run}, not the only line of defense. */
    private static boolean isReadOnlySelect(String sql) {
        String upper = sql.strip().toUpperCase(java.util.Locale.ROOT);
        return upper.startsWith("SELECT") || upper.startsWith("WITH");
    }

    private void recordNl2SqlAudit(String question, String draftedSql, String finalSql, boolean corrected,
            String reasoning, AdHocQueryRunner.Result result, com.sayonora.wire.core.AccessContext accessContext) {
        String userId = accessContext == null || accessContext.isAnonymous() ? "anonymous" : accessContext.userId();
        Map<String, String> details = new LinkedHashMap<>();
        details.put("question", truncate(question, 300));
        details.put("draftedSql", truncate(draftedSql, 300));
        details.put("finalSql", truncate(finalSql, 300));
        details.put("success", String.valueOf(result.success()));
        if (!result.success()) {
            details.put("error", result.error());
        }
        auditLog.record(com.sayonora.wire.audit.AuditEvent.of(
                com.sayonora.wire.audit.AuditEvent.Type.NL2SQL_QUERY_EXECUTED, userId,
                "NL2SQL query " + (result.success() ? "executed" : "failed") + ": " + truncate(question, 120), details));
        if (corrected) {
            Map<String, String> judgeDetails = new LinkedHashMap<>(details);
            judgeDetails.put("reasoning", reasoning == null ? "" : reasoning);
            auditLog.record(com.sayonora.wire.audit.AuditEvent.of(
                    com.sayonora.wire.audit.AuditEvent.Type.NL2SQL_JUDGE_CORRECTED, userId,
                    "NL2SQL judge corrected the drafted SQL", judgeDetails));
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() > max ? s.substring(0, max) + "…" : s;
    }

    /** Real Postgres/MySQL/SQL Server all implement the same ANSI {@code information_schema}
     * views, so {@code list_tables}/{@code describe_table}'s SQL is genuinely shared across three
     * of the four backends -- only the "what counts as a system schema to exclude" and "what's the
     * caller's default schema when they don't name one" answers differ per dialect. Oracle has no
     * real {@code information_schema} at all; {@code all_tables}/{@code all_tab_columns} (filtered
     * to the connected user's own schema, the same "don't need DBA privileges" scoping {@code
     * user_tables}/{@code user_tab_columns} give directly) are its real equivalent. */
    private static String defaultSchemaFor(ServerOptions options) {
        return switch (options.mcpBackendMode()) {
            case MYSQL -> options.mysqlDatabase();
            case SQLSERVER -> "dbo";
            // Oracle has no "public" schema concept -- an unqualified table always means the
            // connected user's own schema, exactly what user_tables/all_tab_columns WHERE owner =
            // (this) already assume. Real bug caught live by this class's own test: without this,
            // describe_table's fallback silently queried owner = 'PUBLIC' (a reserved Oracle role
            // name, never a real schema) and always came back empty, even for a table that
            // genuinely existed.
            case ORACLE -> options.oracleUser();
            case POSTGRES -> "public";
        };
    }

    /**
     * {@code scope} ARGUMENT-aware, per the real gap raised directly: {@code inspect_schema}
     * previously only ever showed the ONE backend a session happens to be connected to, even
     * though schema auto-discovery (query_federated) resolves queries against every registered
     * backend -- an agent exploring the database before writing a query had no way to see the
     * tables auto-discovery would actually find.
     *
     * <p>When this ENDPOINT itself is pinned to a scope (see {@link McpScope}'s own javadoc --
     * {@code WARP_MCP_SCOPE}, a real, enforced boundary, not just a listing preference), the
     * argument is ignored entirely: a DATABASE- or GROUP-scoped endpoint always shows exactly its
     * own fixed scope, deterministically, with nothing for the caller to choose. Only an unscoped
     * ({@code all}) endpoint honors the {@code scope}/{@code group} arguments below.
     */
    private AdHocQueryRunner.Result runInspectSchema(Connection backend, JsonObject arguments,
            com.sayonora.wire.core.AccessContext accessContext) {
        if (currentScope().type() == McpScope.Type.DATABASE || currentScope().type() == McpScope.Type.GROUP) {
            return multiBackendInspectResult(scopedDiscoveredColumns());
        }
        String argScope = arguments.has("scope") ? arguments.get("scope").getAsString() : "current";
        return switch (argScope) {
            case "current" -> runSql(backend, DataInvestigationTools.inspectSchemaSql(options.mcpBackendMode()), accessContext);
            case "all" -> multiBackendInspectResult(BackendCatalogDiscovery.discoverAllColumns(backendRegistry));
            case "group" -> {
                String groupName = requireString(arguments, "group");
                List<BackendCatalogDiscovery.DiscoveredColumn> matched = BackendCatalogDiscovery.discoverAllColumns(backendRegistry)
                        .stream()
                        .filter(c -> {
                            BackendRegistry.BackendGroupInfo info = backendRegistry.groupInfoFor(c.backendName());
                            return info != null && info.name().equals(groupName);
                        })
                        .toList();
                yield multiBackendInspectResult(matched);
            }
            default -> new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "22023",
                    "unknown scope \"" + argScope + "\" -- expected \"current\", \"group\", or \"all\"");
        };
    }

    /** Every {@link BackendCatalogDiscovery.DiscoveredColumn} visible to THIS endpoint's own
     * {@link McpScope} -- the real filter behind both {@link #runInspectSchema}'s pinned-endpoint
     * branch and {@link #runFederatedQuery}'s auto-discovery (via {@link #scopedDiscoveredTables}
     * for the table-name-only shape that needs). {@code ALL} scope returns everything unfiltered. */
    private List<BackendCatalogDiscovery.DiscoveredColumn> scopedDiscoveredColumns() {
        McpScope effectiveScope = currentScope();
        List<BackendCatalogDiscovery.DiscoveredColumn> all = BackendCatalogDiscovery.discoverAllColumns(backendRegistry);
        return switch (effectiveScope.type()) {
            case DATABASE -> all.stream().filter(c -> c.backendName().equals(effectiveScope.name())).toList();
            case GROUP -> all.stream().filter(c -> {
                BackendRegistry.BackendGroupInfo info = backendRegistry.groupInfoFor(c.backendName());
                return info != null && info.name().equals(effectiveScope.name());
            }).toList();
            case ALL -> all;
        };
    }

    /** As {@link #scopedDiscoveredColumns}, for {@link BackendCatalogDiscovery.DiscoveredTable}'s
     * table-name-only shape -- what {@link #runFederatedQuery}'s auto-discovery resolution needs. */
    private List<BackendCatalogDiscovery.DiscoveredTable> scopedDiscoveredTables() {
        McpScope effectiveScope = currentScope();
        List<BackendCatalogDiscovery.DiscoveredTable> all = BackendCatalogDiscovery.discoverAll(backendRegistry);
        return switch (effectiveScope.type()) {
            case DATABASE -> all.stream().filter(t -> t.backendName().equals(effectiveScope.name())).toList();
            case GROUP -> all.stream().filter(t -> {
                BackendRegistry.BackendGroupInfo info = backendRegistry.groupInfoFor(t.backendName());
                return info != null && info.name().equals(effectiveScope.name());
            }).toList();
            case ALL -> all;
        };
    }

    private AdHocQueryRunner.Result multiBackendInspectResult(List<BackendCatalogDiscovery.DiscoveredColumn> discovered) {
        List<String> columns = List.of("backend", "backend_type", "backend_description", "schema", "table", "column",
                "data_type", "is_nullable");
        List<List<Object>> rows = new ArrayList<>();
        for (BackendCatalogDiscovery.DiscoveredColumn c : discovered) {
            com.sayonora.wire.core.BackendTarget t = backendRegistry.get(c.backendName());
            String desc = backendRegistry.descriptionOf(c.backendName());
            rows.add(List.of(c.backendName(), t == null ? "" : BackendTypes.typeOf(t), desc == null ? "" : desc,
                    c.realSchemaName() == null ? "" : c.realSchemaName(),
                    c.tableName(), c.columnName(), c.dataTypeName(), c.nullable() ? "YES" : "NO"));
        }
        return new AdHocQueryRunner.Result(true, true, columns, rows, 0, null, null);
    }

    private AdHocQueryRunner.Result runListTables(Connection backend, com.sayonora.wire.core.AccessContext accessContext) {
        String sql = switch (options.mcpBackendMode()) {
            case POSTGRES -> "SELECT schemaname, tablename FROM pg_catalog.pg_tables "
                    + "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
                    + "ORDER BY schemaname, tablename";
            case MYSQL -> "SELECT table_schema, table_name FROM information_schema.tables "
                    + "WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') "
                    + "ORDER BY table_schema, table_name";
            case SQLSERVER -> "SELECT table_schema, table_name FROM information_schema.tables "
                    + "WHERE table_type = 'BASE TABLE' ORDER BY table_schema, table_name";
            case ORACLE -> "SELECT USER AS owner, table_name FROM user_tables ORDER BY table_name";
        };
        return runSql(backend, sql, accessContext);
    }

    private AdHocQueryRunner.Result runDescribeTable(Connection backend, JsonObject arguments,
            com.sayonora.wire.core.AccessContext accessContext) {
        String table = requireString(arguments, "table");
        String schema = arguments.has("schema") ? arguments.get("schema").getAsString() : defaultSchemaFor(options);
        if (table.contains(".")) {
            String[] parts = table.split("\\.", 2);
            schema = parts[0];
            table = parts[1];
        }
        // Oracle has no information_schema -- all_tab_columns is its real equivalent, and its
        // column order is already the connected user's own creation order (no ordinal_position
        // column to sort by the way information_schema.columns has one for the other three).
        if (options.mcpBackendMode() == McpBackendMode.ORACLE) {
            String sql = "SELECT column_name, data_type, nullable FROM all_tab_columns "
                    + "WHERE owner = ? AND table_name = ? ORDER BY column_id";
            return runSql(backend, sql, List.of(schema.toUpperCase(java.util.Locale.ROOT),
                    table.toUpperCase(java.util.Locale.ROOT)), accessContext);
        }
        String sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        return runSql(backend, sql, List.of(schema, table), accessContext);
    }

    private AdHocQueryRunner.Result runRegisteredTool(Connection backend, String toolName, JsonObject arguments,
            com.sayonora.wire.core.AccessContext accessContext) {
        RegisteredFunctionTool tool = functionTools.stream()
                .filter(t -> t.toolName().equals(toolName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no such tool: " + toolName));
        List<Object> binds = new ArrayList<>();
        StringBuilder placeholders = new StringBuilder();
        for (PgFunctionIntrospector.ParamDef param : tool.signature().params()) {
            
            if ("OUT".equalsIgnoreCase(param.mode())) {
                continue;
            }
            if (!placeholders.isEmpty()) {
                placeholders.append(", ");
            }
            placeholders.append('?');
            binds.add(jsonToBindValue(arguments.get(param.name()), param.pgType()));
        }
        String qualified = tool.signature().schema() + "." + tool.signature().name();
        String sql = tool.signature().isProcedure()
                ? "CALL " + qualified + "(" + placeholders + ")"
                : "SELECT * FROM " + qualified + "(" + placeholders + ")";
        // Through runSql (not AdHocQueryRunner directly) so a registered tool is pinned/scoped
        // exactly like execute_sql -- under DATABASE scope it used to skip the pin entirely.
        return runSql(backend, sql, binds, accessContext);
    }

    private static Object jsonToBindValue(JsonElement element, String pgType) {
        if (element == null || element.isJsonNull()) {
            return null;
        }
        String type = pgType == null ? "" : pgType.toLowerCase(java.util.Locale.ROOT);
        if (element.isJsonPrimitive()) {
            var prim = element.getAsJsonPrimitive();
            if (prim.isBoolean()) {
                return prim.getAsBoolean();
            }
            if (prim.isNumber()) {
                return switch (type) {
                    case "smallint", "integer" -> prim.getAsInt();
                    case "bigint" -> prim.getAsLong();
                    case "real", "double precision" -> prim.getAsDouble();
                    default -> prim.getAsBigDecimal();
                };
            }
            return prim.getAsString();
        }
        return element.toString();
    }

    private static String requireString(JsonObject obj, String key) {
        if (!obj.has(key) || obj.get(key).isJsonNull()) {
            throw new IllegalArgumentException("missing required argument: " + key);
        }
        return obj.get(key).getAsString();
    }

    /** Caps a caller-supplied row limit at {@code max} (every data-investigation tool that takes
     * one dumps rows into a small model's context -- an unbounded value would defeat the point of
     * a "sample", not just waste a query). */
    private static int positiveIntArg(JsonObject obj, String key, int defaultValue, int max) {
        if (!obj.has(key) || obj.get(key).isJsonNull()) {
            return defaultValue;
        }
        int value = obj.get(key).getAsInt();
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive -- got: " + value);
        }
        return Math.min(value, max);
    }

    /** find_join_path's own JSON shape -- a real BFS-shortest hop list (see {@link
     * JoinPathFinder}), not the tabular {@code {columns, rows}} shape every other tool here
     * returns, since the point of this tool is the JOIN chain itself, not a row dump. Reads the
     * whole schema's real foreign-key edges fresh on every call (see {@link
     * DataInvestigationTools#foreignKeyEdgesSql}) rather than caching them -- a schema that's
     * actively being explored by an agent (creating tables, adding constraints) shouldn't answer
     * from a stale graph. */
    private JsonObject runFindJoinPath(Connection backend, String fromTable, String toTable) throws SQLException {
        DataInvestigationTools.requireValidIdentifier(fromTable, "from_table");
        DataInvestigationTools.requireValidIdentifier(toTable, "to_table");
        List<JoinPathFinder.Edge> edges = new ArrayList<>();
        try (var st = backend.createStatement();
                var rs = st.executeQuery(DataInvestigationTools.foreignKeyEdgesSql(options.mcpBackendMode()))) {
            while (rs.next()) {
                edges.add(new JoinPathFinder.Edge(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
        }
        List<JoinPathFinder.Hop> hops = JoinPathFinder.findPath(edges, fromTable, toTable);
        JsonObject payload = new JsonObject();
        if (hops == null) {
            payload.addProperty("found", false);
            payload.addProperty("message", "no foreign-key path connects " + fromTable + " and " + toTable);
        } else {
            payload.addProperty("found", true);
            JsonArray hopsJson = new JsonArray();
            StringBuilder joinSql = new StringBuilder();
            for (JoinPathFinder.Hop hop : hops) {
                JsonObject hopJson = new JsonObject();
                hopJson.addProperty("from_table", hop.fromTable());
                hopJson.addProperty("from_column", hop.fromColumn());
                hopJson.addProperty("to_table", hop.toTable());
                hopJson.addProperty("to_column", hop.toColumn());
                hopsJson.add(hopJson);
                if (!joinSql.isEmpty()) {
                    joinSql.append(" ");
                }
                joinSql.append("JOIN ").append(hop.toTable()).append(" ON ")
                        .append(hop.fromTable()).append(".").append(hop.fromColumn())
                        .append(" = ").append(hop.toTable()).append(".").append(hop.toColumn());
            }
            payload.add("hops", hopsJson);
            payload.addProperty("join_sql", "FROM " + fromTable + " " + joinSql);
        }
        JsonObject content = new JsonObject();
        JsonArray contentArray = new JsonArray();
        JsonObject textContent = new JsonObject();
        textContent.addProperty("type", "text");
        textContent.addProperty("text", GSON.toJson(payload));
        contentArray.add(textContent);
        content.add("content", contentArray);
        content.addProperty("isError", false);
        return content;
    }

    /** {@code explain_sql} -- a real, no-LLM, dialect-aware EXPLAIN available in every backend
     * mode (unlike {@code explain_query}, which stays Postgres-only and adds an LLM narration on
     * top -- see this class's own native-mode dispatch comment). Postgres and MySQL both accept a
     * single {@code EXPLAIN ... FORMAT JSON <sql>} statement and never execute the query for real.
     * Oracle and SQL Server can't do that in one statement, so both get their own real,
     * standard-for-that-database two-statement flow, executed directly against {@code backend}
     * (not through {@link JdbcBackendExecutor}, which caches one {@link java.sql.PreparedStatement}
     * per SQL text and isn't built for a "run this control statement, then read this different
     * query" pair sharing session state).
     */
    private AdHocQueryRunner.Result runExplainSql(Connection backend, String sql) throws SQLException {
        return switch (options.mcpBackendMode()) {
            case POSTGRES -> runSql(backend, "EXPLAIN (FORMAT JSON) " + sql, com.sayonora.wire.core.AccessContext.ANONYMOUS);
            case MYSQL -> runSql(backend, "EXPLAIN FORMAT=JSON " + sql, com.sayonora.wire.core.AccessContext.ANONYMOUS);
            case ORACLE -> runOracleExplain(backend, sql);
            case SQLSERVER -> runSqlServerExplain(backend, sql);
        };
    }

    /** {@code EXPLAIN PLAN FOR <sql>} populates Oracle's own {@code PLAN_TABLE} (a real,
     * automatically-available global temporary table, session-scoped, needing no setup) without
     * executing the statement; {@code DBMS_XPLAN.DISPLAY()} with no arguments then reads back the
     * most recently explained plan in THIS session -- which is exactly why both statements have to
     * run on the same, still-open {@code backend} connection. */
    private AdHocQueryRunner.Result runOracleExplain(Connection backend, String sql) throws SQLException {
        try (var st = backend.createStatement()) {
            st.execute("EXPLAIN PLAN FOR " + sql);
        }
        try (var st = backend.createStatement();
                var rs = st.executeQuery("SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY())")) {
            return AdHocQueryRunner.Result.ofSuccess(readResultSet(rs));
        }
    }

    /** SQL Server has no single-statement JSON-plan syntax; {@code SET SHOWPLAN_ALL ON} instead
     * puts the WHOLE session into "next statement returns its plan instead of running it" mode
     * (the real mechanism behind SSMS's own "Display Estimated Execution Plan") -- genuinely
     * different in kind from Postgres/MySQL's inline {@code EXPLAIN}, not just spelled differently.
     * Must be turned back {@code OFF} before this connection returns to its pool (see
     * {@code BackendConnectionPools}) or every later borrower would silently get plan rows instead
     * of real query results -- the {@code finally} block is load-bearing, not defensive style. */
    // TODO: this runs the caller's SQL directly on the raw connection -- un-governed by the
    // shared pipeline (no firewall/QoS/scope check) -- in SQL Server native mode only; the other
    // three modes go through runSql. Left as-is because SHOWPLAN_ALL needs session-state pairing
    // JdbcBackendExecutor can't provide; a FirewallStage pre-check on the text would close most of it.
    private AdHocQueryRunner.Result runSqlServerExplain(Connection backend, String sql) throws SQLException {
        try (var setOn = backend.createStatement()) {
            setOn.execute("SET SHOWPLAN_ALL ON");
        }
        try (var st = backend.createStatement();
                var rs = st.executeQuery(sql)) {
            return AdHocQueryRunner.Result.ofSuccess(readResultSet(rs));
        } finally {
            try (var setOff = backend.createStatement()) {
                setOff.execute("SET SHOWPLAN_ALL OFF");
            }
        }
    }

    /** As {@code JdbcBackendExecutor}'s own package-private {@code readResultSet} (not reusable
     * across packages) -- builds an {@link ExecutionResult} from a plain, already-executed {@link
     * java.sql.ResultSet}, for the raw-JDBC explain/join-path paths above that read a result set
     * directly rather than going through a {@link Statement}. */
    private static ExecutionResult readResultSet(java.sql.ResultSet rs) throws SQLException {
        java.sql.ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        List<com.sayonora.wire.core.ColumnInfo> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(new com.sayonora.wire.core.ColumnInfo(md.getColumnLabel(i), md.getColumnType(i),
                    md.getPrecision(i), md.getScale(i), md.getColumnDisplaySize(i),
                    md.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls));
        }
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                row.add(rs.wasNull() ? null : value);
            }
            rows.add(row);
        }
        return ExecutionResult.ofQuery(columns, rows);
    }

    private static JsonObject toolCallResult(AdHocQueryRunner.Result result) {
        JsonObject callResult = new JsonObject();
        JsonArray content = new JsonArray();
        JsonObject textContent = new JsonObject();
        textContent.addProperty("type", "text");
        if (!result.success()) {
            textContent.addProperty("text", "ERROR [" + result.sqlState() + "]: " + result.error());
            callResult.addProperty("isError", true);
        } else if (result.isQuery()) {
            textContent.addProperty("text", GSON.toJson(rowsAsMaps(result)));
            callResult.addProperty("isError", false);
        } else {
            textContent.addProperty("text", "OK, " + result.updateCount() + " row(s) affected");
            callResult.addProperty("isError", false);
        }
        content.add(textContent);
        callResult.add("content", content);
        return callResult;
    }

    /** As {@link #toolCallResult(AdHocQueryRunner.Result)}, with one extra text content item
     * prepended -- {@code query_natural_language}'s way of showing the SQL it actually ran (and
     * the judge's correction, if any) alongside the result, and {@code explain_query}'s way of
     * showing the LLM's narration alongside the raw plan, since in both cases the point is the
     * LLM's contribution being visible, not silent. */
    private static JsonObject toolCallResult(AdHocQueryRunner.Result result, String note) {
        JsonObject callResult = toolCallResult(result);
        if (note == null) {
            return callResult;
        }
        JsonObject noteContent = new JsonObject();
        noteContent.addProperty("type", "text");
        noteContent.addProperty("text", note);
        JsonArray combined = new JsonArray();
        combined.add(noteContent);
        callResult.getAsJsonArray("content").forEach(combined::add);
        callResult.add("content", combined);
        return callResult;
    }

    private static List<Map<String, Object>> rowsAsMaps(AdHocQueryRunner.Result result) {
        List<Map<String, Object>> mapped = new ArrayList<>(result.rows().size());
        for (List<Object> row : result.rows()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < result.columns().size(); i++) {
                map.put(result.columns().get(i), row.get(i));
            }
            mapped.add(map);
        }
        return mapped;
    }

    private static void writeResult(HttpServletResponse response, JsonElement id, JsonObject result) throws IOException {
        JsonObject envelope = new JsonObject();
        envelope.addProperty("jsonrpc", "2.0");
        envelope.add("id", id);
        envelope.add("result", result);
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(GSON.toJson(envelope));
    }

    private static void writeError(HttpServletResponse response, JsonElement id, int code, String message) throws IOException {
        JsonObject envelope = new JsonObject();
        envelope.addProperty("jsonrpc", "2.0");
        envelope.add("id", id);
        JsonObject error = new JsonObject();
        error.addProperty("code", code);
        error.addProperty("message", message);
        envelope.add("error", error);
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(GSON.toJson(envelope));
    }
}
