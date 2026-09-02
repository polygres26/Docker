package com.nexagres.wire.mcp;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.acl.ConnectionGate;
import com.nexagres.wire.core.AdHocQueryRunner;
import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.core.ExecutionResult;
import com.nexagres.wire.core.JdbcBackendExecutor;
import com.nexagres.wire.core.PipelineStage;
import com.nexagres.wire.core.SourceDialect;
import com.nexagres.wire.core.Statement;
import com.nexagres.wire.mssqlwire.MssqlBackendConnections;
import com.nexagres.wire.mywire.MySqlBackendConnections;
import com.nexagres.wire.pgwire.PgConnections;
import com.nexagres.wire.server.ServerOptions;
import com.nexagres.wire.server.ServerOptions.McpBackendMode;
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
    private final com.nexagres.wire.http.auth.AccessContextResolver oauth;
    private final Server server;
    private final List<RegisteredFunctionTool> functionTools;
    private final McpMetricsCollector metrics;
    private final com.nexagres.wire.audit.AuditLog auditLog;
    private final java.util.function.Supplier<com.nexagres.wire.core.TranslationLlmClient> llmClientSupplier;

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec,
                com.nexagres.wire.http.auth.AccessContextResolver.DISABLED);
    }

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, new McpMetricsCollector());
    }

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, metrics, null);
    }

    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics,
            com.nexagres.wire.audit.AuditLog auditLog) {
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
     * "current hot-reloadable client" pattern {@link com.nexagres.wire.core.QueryRepairStage}
     * uses), powers that one tool's natural-language-to-SQL drafting and judging -- see {@link
     * #runNaturalLanguageQuery}. {@code () -> null} on every other constructor overload disables
     * just that tool (it errors clearly, "no LLM provider configured"), not the whole server.
     */
    public WarpMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics,
            com.nexagres.wire.audit.AuditLog auditLog,
            java.util.function.Supplier<com.nexagres.wire.core.TranslationLlmClient> llmClientSupplier) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.connectionGate = connectionGate;
        this.oauth = oauth;
        this.metrics = metrics;
        this.auditLog = auditLog;
        this.llmClientSupplier = llmClientSupplier;
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
                com.nexagres.wire.core.AccessContext accessContext = oauth.enforce(request, response);
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
            com.nexagres.wire.core.AccessContext accessContext) throws IOException {
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
        if (options.mcpBackendMode() != McpBackendMode.POSTGRES) {
            JsonObject result = new JsonObject();
            result.add("tools", tools);
            return result;
        }
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
            com.nexagres.wire.core.AccessContext accessContext) throws IOException {
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
                    || "document_schema".equals(toolName)) {
                // All three hardcode Postgres-only SQL -- query_natural_language/document_schema
                // draft against information_schema.columns text they then hand an LLM as if it
                // were the ONLY schema shape that exists, and explain_query emits a literal
                // EXPLAIN (FORMAT JSON ...) Postgres never shares syntax for with any of the other
                // three dialects. Refusing cleanly here beats silently running SQL that's simply
                // wrong for the configured backend.
                ResultWithNote outcome = backendMode != McpBackendMode.POSTGRES
                        ? new ResultWithNote(notSupportedInNativeMode(toolName, backendMode), null)
                        : switch (toolName) {
                            case "query_natural_language" -> runNaturalLanguageQuery(backend, requireString(arguments, "question"), accessContext);
                            case "explain_query" -> runExplainQuery(backend, requireString(arguments, "sql"),
                                    arguments.has("analyze") && "true".equalsIgnoreCase(arguments.get("analyze").getAsString()), accessContext);
                            default -> runDocumentSchema(backend, accessContext);
                        };
                isError = !outcome.result().success();
                errorMessage = outcome.result().error();
                writeResult(response, id, toolCallResult(outcome.result(), outcome.note()));
            } else {
                AdHocQueryRunner.Result result = switch (toolName) {
                    case "execute_sql" -> runSql(backend, requireString(arguments, "sql"), accessContext);
                    case "list_tables" -> runListTables(backend, accessContext);
                    case "describe_table" -> runDescribeTable(backend, arguments, accessContext);
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
            com.nexagres.wire.core.AccessContext accessContext, boolean isError, String errorMessage, long elapsedNanos) {
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
        auditLog.record(com.nexagres.wire.audit.AuditEvent.of(
                com.nexagres.wire.audit.AuditEvent.Type.MCP_TOOL_CALLED, userId, summary, details));
    }

    /** Opens the connection every tool call in {@link #handleToolsCall} runs against -- Postgres
     * by default (unchanged), or a real Oracle/MySQL/SQL Server connection of the gateway's own
     * when {@code WARP_MCP_BACKEND} names one, mirroring orawire/mywire/mssqlwire's own
     * native-backend-mode connection choice. See {@link ServerOptions.McpBackendMode}'s own
     * javadoc for the full picture, including why MCP needs its own gateway-held Oracle credential
     * where orawire's native mode doesn't. */
    private Connection openBackendConnection() throws SQLException {
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
            com.nexagres.wire.core.AccessContext accessContext) {
        return runSql(backend, sql, List.of(), accessContext);
    }

    private AdHocQueryRunner.Result runSql(Connection backend, String sql, List<Object> bindParams,
            com.nexagres.wire.core.AccessContext accessContext) {
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
     * audit event types existed in {@link com.nexagres.wire.audit.AuditEvent.Type} unused before
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
            com.nexagres.wire.core.AccessContext accessContext) {
        com.nexagres.wire.core.TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
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
    private ResultWithNote runDocumentSchema(Connection backend, com.nexagres.wire.core.AccessContext accessContext) {
        AdHocQueryRunner.Result result = runSql(backend,
                "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
                        + "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                        + "ORDER BY table_schema, table_name, ordinal_position", accessContext);
        if (!result.success()) {
            return new ResultWithNote(result, null);
        }
        com.nexagres.wire.core.TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
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
            com.nexagres.wire.core.AccessContext accessContext) {
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

        com.nexagres.wire.core.TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
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
            String reasoning, AdHocQueryRunner.Result result, com.nexagres.wire.core.AccessContext accessContext) {
        String userId = accessContext == null || accessContext.isAnonymous() ? "anonymous" : accessContext.userId();
        Map<String, String> details = new LinkedHashMap<>();
        details.put("question", truncate(question, 300));
        details.put("draftedSql", truncate(draftedSql, 300));
        details.put("finalSql", truncate(finalSql, 300));
        details.put("success", String.valueOf(result.success()));
        if (!result.success()) {
            details.put("error", result.error());
        }
        auditLog.record(com.nexagres.wire.audit.AuditEvent.of(
                com.nexagres.wire.audit.AuditEvent.Type.NL2SQL_QUERY_EXECUTED, userId,
                "NL2SQL query " + (result.success() ? "executed" : "failed") + ": " + truncate(question, 120), details));
        if (corrected) {
            Map<String, String> judgeDetails = new LinkedHashMap<>(details);
            judgeDetails.put("reasoning", reasoning == null ? "" : reasoning);
            auditLog.record(com.nexagres.wire.audit.AuditEvent.of(
                    com.nexagres.wire.audit.AuditEvent.Type.NL2SQL_JUDGE_CORRECTED, userId,
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

    private AdHocQueryRunner.Result runListTables(Connection backend, com.nexagres.wire.core.AccessContext accessContext) {
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
            com.nexagres.wire.core.AccessContext accessContext) {
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
            com.nexagres.wire.core.AccessContext accessContext) {
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
