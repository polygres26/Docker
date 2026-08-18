package com.polygres.wire.mcp;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.polygres.wire.acl.ConnectionGate;
import com.polygres.wire.core.AdHocQueryRunner;
import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
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

/**
 * Embedded Jetty MCP (Model Context Protocol) server -- the frontend {@link
 * com.polygres.wire.core.AdHocQueryRunner}'s own class javadoc already named and was pulled out
 * for, before this pass actually built it. Speaks MCP's Streamable HTTP transport (JSON-RPC 2.0
 * over a single POST endpoint) -- same raw-{@code Handler} pattern as {@code DynamoWireServer}/
 * {@code MetricsServer}, and the same "every frontend converges on {@code StatementPipeline}"
 * architecture as every other protocol here.
 *
 * <h2>Tools exposed</h2>
 * <ul>
 *   <li>{@code execute_sql} -- runs arbitrary SQL through the real pipeline (so {@code
 *   FirewallStage}/{@code AccessControlStage}/{@code CacheStage}, if configured, all apply exactly
 *   as they would to any other frontend -- this class adds no separate SQL-safety layer of its
 *   own, matching how every wire-protocol frontend already relies on the pipeline itself for
 *   that).</li>
 *   <li>{@code list_tables} / {@code describe_table} -- catalog introspection, same
 *   {@code pg_catalog}/{@code information_schema} shape {@code DialectTranslations}' own
 *   {@code SHOW TABLES} rewrite already uses for mywire.</li>
 *   <li>Zero or more <b>registered-function tools</b> ({@code POLYWIRE_MCP_TOOLS}) -- the
 *   DBMS_CLOUD_AI_AGENT-style piece: an explicit, operator-controlled allow-list naming real
 *   Postgres functions/procedures to expose as individually-named, individually-described tools.
 *   Each one's input JSON Schema is generated from the function's actual signature via {@link
 *   PgFunctionIntrospector} -- the same "introspect the catalog, auto-derive one caller-facing
 *   surface per callable object" mechanic PostgREST already established for REST endpoints, just
 *   targeting MCP tools instead of HTTP routes. Deliberately <b>not</b> auto-discovery of every
 *   function in {@code pg_proc} -- that would be a real privilege-escalation surface ({@code
 *   SECURITY DEFINER} functions, internal helpers never meant to be externally callable); an
 *   explicit list is the same "loud, never silently expand scope" convention {@link
 *   BackendRegistry}/{@code ConfigStore} already use throughout this project.</li>
 * </ul>
 *
 * <h2>Session handling</h2>
 * A real {@code Mcp-Session-Id} is minted on {@code initialize} and echoed back on every
 * subsequent request -- narrow-slice: not actually used to isolate per-session state (this server
 * is stateless per call, same as every other frontend's ad-hoc-query path), just issued and
 * validated for protocol compliance with clients that check for it.
 *
 * <h2>Not implemented in this pass</h2>
 * SSE/streaming responses (every response here is a single JSON object -- spec-compliant for
 * synchronous tool calls, which is the only shape this server's tools ever produce), MCP
 * resources/prompts (tools only), and batched JSON-RPC requests (deprecated in the current MCP
 * spec revision anyway).
 */
public final class PolyWireMcpServer {

    private static final Logger log = LoggerFactory.getLogger(PolyWireMcpServer.class);
    private static final String PROTOCOL_VERSION = "2024-11-05";
    private static final Gson GSON = new Gson();

    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final BackendRegistry backendRegistry;
    private final ConnectionGate connectionGate;
    private final com.polygres.wire.http.auth.AccessContextResolver oauth;
    private final Server server;
    private final List<RegisteredFunctionTool> functionTools;

    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec) {
        this(port, options, sharedStages, backendRegistry, connectionGate, toolsSpec,
                com.polygres.wire.http.auth.AccessContextResolver.DISABLED);
    }

    /**
     * {@code oauth}: once enabled ({@code POLYWIRE_OAUTH_ISSUER}), the resolved caller identity
     * rides every tool call's {@link Statement} as its real {@link
     * com.polygres.wire.core.AccessContext} (instead of {@code ANONYMOUS}) -- so {@code
     * AccessControlStage}'s row/column policy, if an operator has one configured, enforces against
     * the real authenticated MCP caller, not a blank identity.
     */
    public PolyWireMcpServer(int port, ServerOptions options, List<PipelineStage> sharedStages,
            BackendRegistry backendRegistry, ConnectionGate connectionGate, String toolsSpec,
            com.polygres.wire.http.auth.AccessContextResolver oauth) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.connectionGate = connectionGate;
        this.oauth = oauth;
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
                com.polygres.wire.core.AccessContext accessContext = oauth.enforce(request, response);
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

    // ---- registered-function-tool introspection (startup only) ------------------------------

    /**
     * {@code toolsSpec}: {@code ;}-separated, each entry {@code toolName=schema.function|description}
     * (schema defaults to {@code public} if omitted) -- same {@code ;}-separated-entries shape
     * {@code POLYWIRE_BACKENDS} already uses. A function that fails to introspect (doesn't exist,
     * introspection query itself fails) is logged and skipped, not fatal to server startup -- one
     * bad entry in a long list shouldn't take down every other registered tool.
     */
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

    // ---- JSON-RPC dispatch --------------------------------------------------------------------

    private void handleRequest(HttpServletRequest request, HttpServletResponse response,
            com.polygres.wire.core.AccessContext accessContext) throws IOException {
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
            // A notification (e.g. notifications/initialized) -- no response body expected, just ack.
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

    // ---- tools/call -----------------------------------------------------------------------

    private void handleToolsCall(HttpServletResponse response, JsonElement id, JsonObject params,
            com.polygres.wire.core.AccessContext accessContext) throws IOException {
        if (!params.has("name")) {
            writeError(response, id, -32602, "Invalid params: missing tool name");
            return;
        }
        String toolName = params.get("name").getAsString();
        JsonObject arguments = params.has("arguments") && params.get("arguments").isJsonObject()
                ? params.getAsJsonObject("arguments") : new JsonObject();

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
            writeResult(response, id, toolCallResult(result));
        } catch (RuntimeException | java.sql.SQLException e) {
            writeError(response, id, -32602, e.getMessage());
        }
    }

    private AdHocQueryRunner.Result runSql(Connection backend, String sql,
            com.polygres.wire.core.AccessContext accessContext) {
        return AdHocQueryRunner.run(backend, sharedStages, backendRegistry, "default", sql, accessContext);
    }

    private AdHocQueryRunner.Result runDescribeTable(Connection backend, JsonObject arguments,
            com.polygres.wire.core.AccessContext accessContext) {
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
            com.polygres.wire.core.AccessContext accessContext) {
        RegisteredFunctionTool tool = functionTools.stream()
                .filter(t -> t.toolName().equals(toolName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no such tool: " + toolName));
        List<Object> binds = new ArrayList<>();
        StringBuilder placeholders = new StringBuilder();
        for (PgFunctionIntrospector.ParamDef param : tool.signature().params()) {
            // OUT parameters aren't supplied by the caller in the call syntax at all -- same
            // "skip them" rule RegisteredFunctionTool.buildInputSchema already applies when
            // deciding what belongs in the tool's *input* schema; kept consistent here so the
            // placeholder count always matches what the schema actually asked the caller for.
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

    /**
     * {@code pgType}-aware -- found live: binding every JSON number as a plain {@code BigDecimal}
     * regardless of the target parameter's real type made Postgres's own function-overload
     * resolution reject an otherwise-correct call ({@code function public.f(numeric) does not
     * exist} for a real {@code f(integer)}), since JDBC's {@code setObject(BigDecimal)} sends the
     * bind as {@code numeric} and Postgres won't implicitly widen/narrow that to match a
     * differently-typed parameter during overload resolution. Narrowed to the introspected
     * parameter's actual type instead, same category of type-directed coercion {@code
     * JdbcBackendExecutor#coerce} already does, just informed by real catalog metadata here
     * instead of a string-parse guess.
     */
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
                    default -> prim.getAsBigDecimal(); // numeric/decimal and anything unrecognized
                };
            }
            return prim.getAsString();
        }
        return element.toString(); // json/jsonb-shaped argument -- passed through as its JSON text
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

    // ---- JSON-RPC response writing ---------------------------------------------------------

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
        response.setStatus(HttpServletResponse.SC_OK); // JSON-RPC errors are still HTTP 200, per spec
        response.getWriter().write(GSON.toJson(envelope));
    }
}
