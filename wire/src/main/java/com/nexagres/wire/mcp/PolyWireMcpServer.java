package com.nexagres.wire.mcp;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.acl.ConnectionGate;
import com.nexagres.wire.core.AdHocQueryRunner;
import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.core.PipelineStage;
import com.nexagres.wire.pgwire.PgConnections;
import com.nexagres.wire.server.ServerOptions;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
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

public final class PolyWireMcpServer {

    private static final Logger log = LoggerFactory.getLogger(PolyWireMcpServer.class);
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

    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec,
                com.nexagres.wire.http.auth.AccessContextResolver.DISABLED);
    }

    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, new McpMetricsCollector());
    }

    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, metrics, null);
    }

    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics,
            com.nexagres.wire.audit.AuditLog auditLog) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec, oauth, metrics, auditLog, () -> null);
    }

    /**
     * Full constructor -- adds {@code metrics}, the shared {@link McpMetricsCollector} instance
     * {@code MetricsServer} reads from to render {@code /api/metrics/summary}'s {@code mcpTools}
     * field and {@code /metrics}' {@code polywire_mcp_tool_*} series. Passed in (not constructed
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
    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
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
            log.warn("MCP: could not open a connection to introspect POLYWIRE_MCP_TOOLS -- no registered "
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
        serverInfo.addProperty("name", "polywire");
        serverInfo.addProperty("version", "1.0");
        result.add("serverInfo", serverInfo);
        return result;
    }

    private JsonObject buildToolsListResult() {
        JsonArray tools = new JsonArray();
        tools.add(toolDef("execute_sql", "Execute a SQL statement against the Postgres backend and return the results.",
                objectSchema(Map.of("sql", stringSchema("The SQL statement to execute")), List.of("sql"))));
        tools.add(toolDef("list_tables", "List tables in the Postgres backend (excludes system schemas).",
                objectSchema(Map.of(), List.of())));
        tools.add(toolDef("describe_table", "Describe a table's columns (name, type, nullability).",
                objectSchema(Map.of(
                        "table", stringSchema("Table name, optionally schema-qualified as schema.table"),
                        "schema", stringSchema("Schema name (default: public)")),
                        List.of("table"))));
        tools.add(toolDef("query_natural_language",
                "Ask a question in plain English. Drafts a read-only SQL SELECT via an LLM, has a second "
                        + "LLM pass judge (and correct, if needed) it against the schema and the question, then "
                        + "executes the judged SQL through the same firewall/QoS/cache pipeline every other tool "
                        + "uses and returns the result along with the SQL that actually ran. Never executes "
                        + "writes -- use execute_sql directly if a write is really intended. Requires an LLM "
                        + "provider configured (PUT /api/llm-config or POLYWIRE_LLM_*).",
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
        try (Connection backend = PgConnections.open(options)) {
            if ("query_natural_language".equals(toolName)) {
                Nl2SqlOutcome outcome = runNaturalLanguageQuery(backend, requireString(arguments, "question"), accessContext);
                isError = !outcome.result().success();
                errorMessage = outcome.result().error();
                writeResult(response, id, toolCallResult(outcome.result(), outcome.note()));
            } else {
                AdHocQueryRunner.Result result = switch (toolName) {
                    case "execute_sql" -> runSql(backend, requireString(arguments, "sql"), accessContext);
                    case "list_tables" -> runSql(backend,
                            "SELECT schemaname, tablename FROM pg_catalog.pg_tables "
                                    + "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
                                    + "ORDER BY schemaname, tablename", accessContext);
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

    private AdHocQueryRunner.Result runSql(Connection backend, String sql,
            com.nexagres.wire.core.AccessContext accessContext) {
        return AdHocQueryRunner.run(backend, sharedStages, backendRegistry, "default", sql, accessContext);
    }

    private record Nl2SqlOutcome(AdHocQueryRunner.Result result, String note) {
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
    private Nl2SqlOutcome runNaturalLanguageQuery(Connection backend, String question,
            com.nexagres.wire.core.AccessContext accessContext) {
        com.nexagres.wire.core.TranslationLlmClient llmClient = llmClientSupplier == null ? null : llmClientSupplier.get();
        if (llmClient == null) {
            return new Nl2SqlOutcome(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "58000",
                    "no LLM provider configured -- set it via PUT /api/llm-config or the POLYWIRE_LLM_* env vars "
                            + "before using query_natural_language"), null);
        }

        String schemaContext = introspectSchemaContext(backend);
        String draftedSql;
        try {
            draftedSql = llmClient.draftSqlFromNaturalLanguage("Schema:\n" + schemaContext + "\n\nQuestion: " + question);
        } catch (Exception e) {
            return new Nl2SqlOutcome(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "58000",
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
            return new Nl2SqlOutcome(new AdHocQueryRunner.Result(false, false, List.of(), List.of(), 0, "42501",
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
        return new Nl2SqlOutcome(result, note);
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

    private AdHocQueryRunner.Result runDescribeTable(Connection backend, JsonObject arguments,
            com.nexagres.wire.core.AccessContext accessContext) {
        String table = requireString(arguments, "table");
        String schema = arguments.has("schema") ? arguments.get("schema").getAsString() : "public";
        if (table.contains(".")) {
            String[] parts = table.split("\\.", 2);
            schema = parts[0];
            table = parts[1];
        }
        String sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        return AdHocQueryRunner.run(backend, sharedStages, backendRegistry, "default", sql,
                List.of(schema, table), accessContext);
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
     * prepended -- {@code query_natural_language}'s own way of showing the SQL it actually ran
     * (and the judge's correction, if any) alongside the result, since that's the whole point of
     * "judge corrects" being visible rather than silent. */
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
