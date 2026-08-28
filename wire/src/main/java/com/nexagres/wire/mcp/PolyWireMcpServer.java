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

    /**
     * Full constructor -- adds {@code metrics}, the shared {@link McpMetricsCollector} instance
     * {@code MetricsServer} reads from to render {@code /api/metrics/summary}'s {@code mcpTools}
     * field and {@code /metrics}' {@code polywire_mcp_tool_*} series. Passed in (not constructed
     * internally and exposed via a getter) so both this class and {@code MetricsServer} share the
     * exact same instance regardless of which one {@code Main} happens to construct first.
     */
    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.nexagres.wire.http.auth.AccessContextResolver oauth, McpMetricsCollector metrics) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.connectionGate = connectionGate;
        this.oauth = oauth;
        this.metrics = metrics;
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
        try (Connection backend = PgConnections.open(options)) {
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
            writeResult(response, id, toolCallResult(result));
        } catch (RuntimeException | java.sql.SQLException e) {
            writeError(response, id, -32602, e.getMessage());
        } finally {
            metrics.record(toolName, System.nanoTime() - startNanos, isError);
        }
    }

    private AdHocQueryRunner.Result runSql(Connection backend, String sql,
            com.nexagres.wire.core.AccessContext accessContext) {
        return AdHocQueryRunner.run(backend, sharedStages, backendRegistry, "default", sql, accessContext);
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
