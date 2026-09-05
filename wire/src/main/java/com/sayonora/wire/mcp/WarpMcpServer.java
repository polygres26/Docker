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
                com.sayonora.wire.core.AccessContext accessContext = oauth.enforce(request, response);
                if (accessContext == null) {
                    return;
                }
                handleRequest(request, response, accessContext);
            }
        });
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
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
        try (Connection backend = openBackendConnection()) {
            return runNaturalLanguageQuery(backend, question, accessContext).result();
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

        switch (method) {
            case "initialize" -> writeResult(response, idElement, buildInitializeResult());
            case "tools/list" -> writeResult(response, idElement, buildToolsListResult());
            case "tools/call" -> handleToolsCall(response, idElement, params, accessContext);
            default -> writeError(response, idElement, -32601, "Method not found: " + method);
        }
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
        return result;
    }

    private JsonObject buildToolsListResult() {
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
        tools.add(scope.type() != McpScope.Type.ALL
                ? toolDef("inspect_schema",
                        "List every table and column this endpoint can see -- " + (scope.type() == McpScope.Type.DATABASE
                                ? "this endpoint is pinned (WARP_MCP_SCOPE) to a single backend (\"" + scope.name() + "\")"
                                : "this endpoint is pinned (WARP_MCP_SCOPE) to backend group \"" + scope.name() + "\"")
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
        // McpScope.DATABASE: open the NAMED backend directly, not the gateway's own default --
        // see McpScope's own javadoc for why this is the one scope enforced with no gaps at all
        // (runSql below also forces every statement onto THIS SAME connection with no RouterStage
        // rerouting, so there is no path to any other backend regardless of what the SQL says).
        if (scope.type() == McpScope.Type.DATABASE) {
            com.sayonora.wire.core.BackendTarget target = backendRegistry.get(scope.name());
            if (target == null) {
                throw new SQLException("WARP_MCP_SCOPE names backend \"" + scope.name()
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

    private AdHocQueryRunner.Result runSql(Connection backend, String sql, List<Object> bindParams,
            com.sayonora.wire.core.AccessContext accessContext) {
        // McpScope.DATABASE: run directly on THIS connection with no pipeline/routing involved at
        // all -- the real enforcement point. Unlike native mode below (which bypasses the pipeline
        // for a DIFFERENT reason -- avoiding a wrong Postgres-dialect translation), this is a
        // deliberate SECURITY boundary: even a query that would otherwise route elsewhere via an
        // operator's own WARP_ROUTER_* rule has no path off this one named backend, because
        // RouterStage never runs at all. See McpScope's own javadoc for why this is the one scope
        // enforced with no gaps, unlike GROUP scope's narrower (discovery-only) enforcement.
        if (scope.type() == McpScope.Type.DATABASE) {
            try {
                backend.setAutoCommit(true);
                com.sayonora.wire.core.BackendTarget target = backendRegistry.get(scope.name());
                com.sayonora.wire.core.SourceDialect dialect = target != null && target.dialect() != null
                        ? target.dialect() : dialectFor(options.mcpBackendMode());
                Statement statement = Statement.of(dialect, sql, bindParams, accessContext);
                ExecutionResult result = new JdbcBackendExecutor(backend).execute(statement);
                return AdHocQueryRunner.Result.ofSuccess(result);
            } catch (SQLException e) {
                return AdHocQueryRunner.Result.ofError(e);
            }
        }
        if (options.mcpBackendMode() == McpBackendMode.POSTGRES) {
            return AdHocQueryRunner.run(backend, sharedStages, backendRegistry, "default", sql, bindParams, accessContext);
        }
        // Native mode bypasses the whole shared pipeline (RouterStage/DialectTranslationStage/
        // FirewallStage/QosControlStage/etc.), not just picks a different connection -- the exact
        // same reasoning orawire/mywire/mssqlwire's own native-mode dispatch already documents:
        // the pipeline's "default" backend target is always Postgres regardless of this frontend's
        // own mode, so running the client's real Oracle/MySQL/SQL Server SQL through it would still
        // translate toward Postgres and send the (wrong) translated SQL to the real backend.
        try {
            backend.setAutoCommit(true);
            Statement statement = Statement.of(dialectFor(options.mcpBackendMode()), sql, bindParams, accessContext);
            ExecutionResult result = new JdbcBackendExecutor(backend).execute(statement);
            return AdHocQueryRunner.Result.ofSuccess(result);
        } catch (SQLException e) {
            return AdHocQueryRunner.Result.ofError(e);
        }
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
        if (scope.type() == McpScope.Type.DATABASE || scope.type() == McpScope.Type.GROUP) {
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
        List<BackendCatalogDiscovery.DiscoveredColumn> all = BackendCatalogDiscovery.discoverAllColumns(backendRegistry);
        return switch (scope.type()) {
            case DATABASE -> all.stream().filter(c -> c.backendName().equals(scope.name())).toList();
            case GROUP -> all.stream().filter(c -> {
                BackendRegistry.BackendGroupInfo info = backendRegistry.groupInfoFor(c.backendName());
                return info != null && info.name().equals(scope.name());
            }).toList();
            case ALL -> all;
        };
    }

    /** As {@link #scopedDiscoveredColumns}, for {@link BackendCatalogDiscovery.DiscoveredTable}'s
     * table-name-only shape -- what {@link #runFederatedQuery}'s auto-discovery resolution needs. */
    private List<BackendCatalogDiscovery.DiscoveredTable> scopedDiscoveredTables() {
        List<BackendCatalogDiscovery.DiscoveredTable> all = BackendCatalogDiscovery.discoverAll(backendRegistry);
        return switch (scope.type()) {
            case DATABASE -> all.stream().filter(t -> t.backendName().equals(scope.name())).toList();
            case GROUP -> all.stream().filter(t -> {
                BackendRegistry.BackendGroupInfo info = backendRegistry.groupInfoFor(t.backendName());
                return info != null && info.name().equals(scope.name());
            }).toList();
            case ALL -> all;
        };
    }

    private static AdHocQueryRunner.Result multiBackendInspectResult(List<BackendCatalogDiscovery.DiscoveredColumn> discovered) {
        List<String> columns = List.of("backend", "schema", "table", "column", "data_type", "is_nullable");
        List<List<Object>> rows = new ArrayList<>();
        for (BackendCatalogDiscovery.DiscoveredColumn c : discovered) {
            rows.add(List.of(c.backendName(), c.realSchemaName() == null ? "" : c.realSchemaName(),
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
        return AdHocQueryRunner.run(backend, sharedStages, backendRegistry, "default", sql, binds, accessContext);
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
