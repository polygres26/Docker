package com.sayonora.wire.mcp;

import com.google.gson.JsonObject;
import com.sayonora.wire.core.AdHocQueryRunner;
import java.util.List;

/**
 * One implementation per non-relational {@link BackendKind}: the tool vocabulary an agent expects
 * from that system's own MCP server, mapped onto the backend the call is routed to -- either a
 * Warp-emulated store (data in Postgres tables, run through the wire-protocol handlers) or a real
 * external backend registered in {@code WARP_BACKENDS} (reached with the connector client
 * construction in {@code core/connector}). {@link WarpMcpServer} keeps the JSON-RPC plumbing,
 * scope enforcement, backend routing, metrics and audit; a provider only declares tools, executes
 * them against {@link Ctx#backend()} and describes that backend's contents.
 */
public interface BackendToolProvider {

    BackendKind kind();

    /** Tools this kind advertises. {@code write=true} tools are omitted and refused under
     * {@code WARP_MCP_READ_ONLY=true}. */
    List<Tool> tools();

    /** Runs one tool against {@code ctx.backend()}. {@code tool} is the un-prefixed (official)
     * name. Must return an error Outcome (never a silent wrong result) for anything it cannot do
     * faithfully. */
    Outcome call(String tool, JsonObject args, Ctx ctx) throws Exception;

    /** Live contents of {@code ctx.backend()} for {@code describe_backend}: whatever this type can
     * list cheaply (tables, collections, measurements, buckets/prefixes, ...). Throws when the
     * backend is unreachable or the type cannot list anything (the server turns that into a note). */
    default JsonObject describe(Ctx ctx) throws Exception {
        throw new UnsupportedOperationException("UnsupportedOperation: contents of " + ctx.backend().type()
                + " backends cannot be listed by Warp yet");
    }

    record Tool(String name, String description, JsonObject inputSchema, boolean write) {
    }

    /** {@code texts} become MCP text content items in order. */
    record Outcome(boolean isError, List<String> texts) {
        public static Outcome ok(String... texts) {
            return new Outcome(false, List.of(texts));
        }

        public static Outcome error(String message) {
            return new Outcome(true, List.of(message));
        }
    }

    /** What the server hands a provider: the routed backend, and governed SQL execution
     * (firewall, QoS, stats, RLS, scope pin -- the same path {@code execute_sql} takes) for
     * emulated stores whose data lives in Postgres. */
    interface Ctx {
        McpBackend backend();

        AdHocQueryRunner.Result sql(String sql);
    }
}
