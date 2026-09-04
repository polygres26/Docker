package com.sayonora.wire.a2a;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.core.AccessContext;
import com.sayonora.wire.core.AdHocQueryRunner;
import com.sayonora.wire.mcp.WarpMcpServer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.UUID;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A real, working A2A (Agent2Agent) frontend -- the gap this closes, found auditing Warp's own
 * architecture diagram against what was actually implemented: MCP was real, A2A (listed right
 * alongside it, "MCP + REST/A2A (optional)") was zero lines of code.
 *
 * <p>Scope, deliberately narrow, matching this codebase's own discipline of shipping a real,
 * verified slice rather than guessing at a whole spec: serves a real Agent Card (the A2A
 * discovery document every client fetches first) and implements exactly ONE JSON-RPC method,
 * {@code message/send} -- a synchronous, non-streaming request/response, immediately completed
 * (no persisted task store, no {@code tasks/get}/{@code tasks/cancel}, no {@code message/stream}
 * SSE, no push notifications). The Agent Card's own {@code capabilities} block advertises exactly
 * that (all three off), so a spec-compliant client knows not to attempt them rather than silently
 * hanging or guessing.
 *
 * <p>{@code message/send} itself is NOT a separate, less-governed path to the backend: it
 * delegates straight to {@link WarpMcpServer#callNaturalLanguageQuery}, the SAME
 * draft-then-judge-then-read-only-enforced-execute pipeline MCP's own {@code
 * query_natural_language} tool already uses -- same firewall/QoS/audit trail, same deterministic
 * read-only check regardless of what either LLM call said. A2A and MCP end up as two different
 * front doors onto one governed capability, not two.
 */
public final class A2AServer {

    private static final Logger log = LoggerFactory.getLogger(A2AServer.class);
    private static final String AGENT_CARD_PATH = "/.well-known/agent-card.json";

    private final Server server;
    private final WarpMcpServer mcpServer;
    private final ConnectionGate connectionGate;
    private final com.sayonora.wire.http.auth.AccessContextResolver oauth;
    private final String publicUrl;

    public A2AServer(int port, String publicUrl, WarpMcpServer mcpServer, ConnectionGate connectionGate,
            com.sayonora.wire.http.auth.AccessContextResolver oauth) {
        this.mcpServer = mcpServer;
        this.connectionGate = connectionGate;
        this.oauth = oauth;
        this.publicUrl = publicUrl;
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
                if (AGENT_CARD_PATH.equals(target) && "GET".equals(request.getMethod())) {
                    response.setContentType("application/json; charset=utf-8");
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.getWriter().write(buildAgentCard());
                    return;
                }
                if (!"POST".equals(request.getMethod())) {
                    response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
                    return;
                }
                AccessContext accessContext = A2AServer.this.oauth.enforce(request, response);
                if (accessContext == null) {
                    return;
                }
                handleJsonRpc(request, response, accessContext);
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
     * The one skill this agent actually offers -- a real, honest description of what {@code
     * query_natural_language} does and doesn't guarantee (read-only, judged, real firewall still
     * applies), not generic marketing copy a client would have to discover the hard way.
     */
    private String buildAgentCard() {
        JsonObject card = new JsonObject();
        card.addProperty("protocolVersion", "0.2.6");
        card.addProperty("name", "Warp");
        card.addProperty("description", "Ask a plain-English question about the data behind this "
                + "Warp gateway; get back a real, read-only SQL query result. The query is drafted "
                + "and judged by an LLM but always executed read-only, through the same SQL "
                + "firewall/QoS/audit pipeline every other client of this gateway goes through.");
        card.addProperty("url", publicUrl);
        card.addProperty("preferredTransport", "JSONRPC");
        card.addProperty("version", "1.0.0");

        JsonObject capabilities = new JsonObject();
        capabilities.addProperty("streaming", false);
        capabilities.addProperty("pushNotifications", false);
        capabilities.addProperty("stateTransitionHistory", false);
        card.add("capabilities", capabilities);

        JsonArray defaultInputModes = new JsonArray();
        defaultInputModes.add("text/plain");
        card.add("defaultInputModes", defaultInputModes);
        JsonArray defaultOutputModes = new JsonArray();
        defaultOutputModes.add("text/plain");
        card.add("defaultOutputModes", defaultOutputModes);

        JsonArray skills = new JsonArray();
        JsonObject skill = new JsonObject();
        skill.addProperty("id", "query_database");
        skill.addProperty("name", "Query the database");
        skill.addProperty("description", "Answers a plain-English question about the data by "
                + "drafting a read-only SQL query, having a second LLM call judge/correct it "
                + "against the real schema, then executing it through Warp's own governed "
                + "pipeline. Never performs a write.");
        JsonArray tags = new JsonArray();
        tags.add("sql");
        tags.add("database");
        tags.add("read-only");
        skill.add("tags", tags);
        JsonArray examples = new JsonArray();
        examples.add("How many orders were placed last week?");
        examples.add("List the 5 most recent customers.");
        skill.add("examples", examples);
        skills.add(skill);
        card.add("skills", skills);

        return card.toString();
    }

    private void handleJsonRpc(HttpServletRequest request, HttpServletResponse response,
            AccessContext accessContext) throws IOException {
        String body = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        JsonObject req;
        try {
            req = JsonParser.parseString(body).getAsJsonObject();
        } catch (RuntimeException e) {
            writeError(response, null, -32700, "Parse error: " + e.getMessage());
            return;
        }
        JsonElement id = req.get("id");
        String method = req.has("method") ? req.get("method").getAsString() : null;
        JsonObject params = req.has("params") && req.get("params").isJsonObject() ? req.getAsJsonObject("params") : new JsonObject();

        if (method == null) {
            writeError(response, id, -32600, "Invalid Request: missing method");
            return;
        }
        switch (method) {
            case "message/send" -> handleMessageSend(response, id, params, accessContext);
            // Deliberately refused, not silently accepted-and-ignored -- see this class's own
            // javadoc for why these three aren't implemented in this first version.
            case "message/stream" -> writeError(response, id, -32004, "message/stream (SSE) is not "
                    + "supported by this agent -- see its Agent Card's capabilities.streaming=false");
            case "tasks/get", "tasks/cancel" -> writeError(response, id, -32001,
                    "task lookup/cancellation is not supported -- message/send always completes "
                            + "synchronously and returns its final result immediately, there is no "
                            + "persisted task to look up afterward");
            default -> writeError(response, id, -32601, "Method not found: " + method);
        }
    }

    private void handleMessageSend(HttpServletResponse response, JsonElement id, JsonObject params,
            AccessContext accessContext) throws IOException {
        String question = extractText(params);
        if (question == null || question.isBlank()) {
            writeError(response, id, -32602, "Invalid params: message has no text part to answer");
            return;
        }
        AdHocQueryRunner.Result result;
        try {
            result = mcpServer.callNaturalLanguageQuery(question, accessContext);
        } catch (SQLException e) {
            writeError(response, id, -32000, "backend error: " + e.getMessage());
            return;
        }
        writeResult(response, id, buildTaskResult(result));
    }

    /** The A2A message shape is {@code params.message.parts[]}, each a {@code {kind, text}} (or
     * {@code {type, text}} on an older/looser client) part -- this takes the first text part
     * found, matching this agent's single-skill, single-turn scope (no multi-part/file/data
     * parts, no multi-turn context -- see this class's own javadoc on why). */
    private static String extractText(JsonObject params) {
        if (!params.has("message") || !params.get("message").isJsonObject()) {
            return null;
        }
        JsonObject message = params.getAsJsonObject("message");
        if (!message.has("parts") || !message.get("parts").isJsonArray()) {
            return null;
        }
        for (JsonElement partEl : message.getAsJsonArray("parts")) {
            if (!partEl.isJsonObject()) {
                continue;
            }
            JsonObject part = partEl.getAsJsonObject();
            String kind = part.has("kind") ? part.get("kind").getAsString()
                    : part.has("type") ? part.get("type").getAsString() : null;
            if (("text".equals(kind) || kind == null) && part.has("text")) {
                return part.get("text").getAsString();
            }
        }
        return null;
    }

    /** A completed {@code Task} -- the whole point of scoping this to synchronous {@code
     * message/send} is that every request is done by the time this method runs, so {@code
     * status.state} is always exactly {@code "completed"} or {@code "failed"}, never
     * {@code "working"}/{@code "input-required"} (those exist for the streaming/multi-turn cases
     * this agent doesn't implement). */
    private static JsonObject buildTaskResult(AdHocQueryRunner.Result result) {
        JsonObject task = new JsonObject();
        task.addProperty("id", UUID.randomUUID().toString());
        task.addProperty("contextId", UUID.randomUUID().toString());
        task.addProperty("kind", "task");

        JsonObject status = new JsonObject();
        status.addProperty("state", result.success() ? "completed" : "failed");
        JsonObject statusMessage = new JsonObject();
        statusMessage.addProperty("role", "agent");
        statusMessage.addProperty("kind", "message");
        statusMessage.addProperty("messageId", UUID.randomUUID().toString());
        JsonArray statusParts = new JsonArray();
        JsonObject statusPart = new JsonObject();
        statusPart.addProperty("kind", "text");
        statusPart.addProperty("text", result.success()
                ? "Query executed successfully."
                : "ERROR [" + result.sqlState() + "]: " + result.error());
        statusParts.add(statusPart);
        statusMessage.add("parts", statusParts);
        status.add("message", statusMessage);
        task.add("status", status);

        if (result.success()) {
            JsonArray artifacts = new JsonArray();
            JsonObject artifact = new JsonObject();
            artifact.addProperty("artifactId", UUID.randomUUID().toString());
            artifact.addProperty("name", "query_result");
            JsonArray artifactParts = new JsonArray();
            JsonObject artifactPart = new JsonObject();
            artifactPart.addProperty("kind", "text");
            artifactPart.addProperty("text", renderResultAsText(result));
            artifactParts.add(artifactPart);
            artifact.add("parts", artifactParts);
            artifacts.add(artifact);
            task.add("artifacts", artifacts);
        }
        return task;
    }

    private static String renderResultAsText(AdHocQueryRunner.Result result) {
        if (!result.isQuery()) {
            return "OK, " + result.updateCount() + " row(s) affected";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(" | ", result.columns())).append('\n');
        for (var row : result.rows()) {
            sb.append(row.stream().map(String::valueOf).reduce((a, b) -> a + " | " + b).orElse("")).append('\n');
        }
        return sb.toString();
    }

    private static void writeResult(HttpServletResponse response, JsonElement id, JsonObject result) throws IOException {
        JsonObject envelope = new JsonObject();
        envelope.addProperty("jsonrpc", "2.0");
        envelope.add("id", id);
        envelope.add("result", result);
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(envelope.toString());
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
        response.getWriter().write(envelope.toString());
    }
}
