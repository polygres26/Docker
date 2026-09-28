package com.sayonora.warp.mcp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.core.SqlMetricsCollector;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Base of the data-tool providers of Warp-hosted stores (Redis, Azure Blob/Queue/Table, GCS, SNS, Kinesis, Secrets/SSM/KMS/STS,
 * Pub/Sub, Firestore, Datastore, Bigtable). A store plugs in with one subclass and one registration line in
 * {@code WarpMcpServer}: it declares its {@link #tools()} (write tools marked {@code write=true}, which the server hides and
 * refuses under {@code WARP_MCP_READ_ONLY}), and runs them in {@link #run} against the store's in-process engine obtained from
 * {@link EmulatedStores#engine} -- the frontend's own service classes over the same Postgres tables the wire protocol uses.
 * {@code describe_backend} keeps coming from the {@link StoreDescribeProvider}. Every call is timed into the gateway's
 * operation metrics as protocol {@code mcp-<kind>}.
 */
abstract class StoreToolProvider implements BackendToolProvider {

    static final Gson GSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
    /** Longest text value returned in one tool result (characters); longer values are cut and flagged. */
    static final int MAX_TEXT = 262_144;

    private final BackendKind kind;
    private final StoreDescribeProvider describer;
    protected final EmulatedStores stores;
    private volatile List<Tool> toolList;

    StoreToolProvider(BackendKind kind, StoreDescribeProvider describer, EmulatedStores stores) {
        this.kind = kind;
        this.describer = describer;
        this.stores = stores;
    }

    @Override
    public final BackendKind kind() {
        return kind;
    }

    @Override
    public final JsonObject describe(Ctx ctx) throws Exception {
        return describer.describe(ctx);
    }

    @Override
    public final List<Tool> tools() {
        List<Tool> t = toolList;
        if (t == null) {
            t = List.copyOf(defineTools());
            toolList = t;
        }
        return t;
    }

    protected abstract List<Tool> defineTools();

    /** Runs one declared tool; throw {@link IllegalArgumentException} for bad arguments. */
    protected abstract Outcome run(String tool, JsonObject args, Ctx ctx) throws Exception;

    @Override
    public final Outcome call(String tool, JsonObject args, Ctx ctx) {
        Tool def = tools().stream().filter(t -> t.name().equals(tool)).findFirst().orElse(null);
        if (def == null) {
            return Outcome.error("unknown " + kind.id() + " tool: " + tool);
        }
        if (!ctx.backend().emulated()) {
            return Outcome.error("UnsupportedOperation: the " + kind.id() + " tools are only available on Warp's hosted "
                    + kind.id() + " store");
        }
        long t0 = System.nanoTime();
        try {
            return run(tool, args, ctx);
        } catch (IllegalArgumentException e) {
            return Outcome.error("invalid arguments: " + e.getMessage());
        } catch (Exception e) {
            String m = e.getMessage();
            return Outcome.error(m == null || m.isBlank() ? e.getClass().getSimpleName() : m);
        } finally {
            SqlMetricsCollector m = stores.sqlMetrics();
            if (m != null) {
                long nanos = System.nanoTime() - t0;
                m.recordOperation("mcp-" + kind.id(), ctx.backend().host() == null ? "default" : ctx.backend().host(),
                        def.write() ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ,
                        tool, nanos, nanos);
            }
        }
    }

    // ------------------------------------------------------------------ shared helpers

    static Outcome json(JsonElement e) {
        return Outcome.ok(GSON.toJson(e));
    }

    /** UTF-8 text when {@code bytes} decode strictly, else null. */
    static String strictUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    /** Adds {@code encoding}/{@code body}/{@code size}/{@code truncated} for a (possibly partial) payload: text if UTF-8, else base64. */
    static void putBody(JsonObject o, byte[] bytes, long totalSize) {
        boolean partial = totalSize > bytes.length;
        String text = strictUtf8(bytes);
        for (int cut = 1; partial && text == null && cut <= 3 && bytes.length - cut > 0; cut++) {
            text = strictUtf8(java.util.Arrays.copyOf(bytes, bytes.length - cut)); // cut inside a multi-byte character
        }
        if (text != null) {
            o.addProperty("encoding", "utf-8");
            o.addProperty("body", text);
        } else {
            o.addProperty("encoding", "base64");
            o.addProperty("body", Base64.getEncoder().encodeToString(bytes));
        }
        o.addProperty("size", totalSize);
        o.addProperty("truncated", partial);
    }

    /** Request body bytes from {@code content} (UTF-8 text) or {@code contentBase64}. */
    static byte[] bodyArg(JsonObject a, String textKey, String base64Key) {
        String b64 = ToolSchemas.optString(a, base64Key);
        if (b64 != null) {
            return Base64.getDecoder().decode(b64);
        }
        String t = ToolSchemas.optString(a, textKey);
        if (t == null) {
            throw new IllegalArgumentException("missing required argument: " + textKey + " (or " + base64Key + ")");
        }
        return t.getBytes(StandardCharsets.UTF_8);
    }

    /** The Google Cloud project a tool addresses: the {@code project} argument, else WARP_MCP_GCP_PROJECT, else "warp-project". */
    static String gcpProject(JsonObject a) {
        String p = ToolSchemas.optString(a, "project");
        if (p != null) {
            return p;
        }
        String env = System.getenv("WARP_MCP_GCP_PROJECT");
        return env == null || env.isBlank() ? "warp-project" : env.trim();
    }

    static String enc(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /** Path with each segment percent-encoded, keeping '/' separators. */
    static String encPath(String s) {
        return java.util.Arrays.stream(s.split("/", -1)).map(StoreToolProvider::enc).collect(java.util.stream.Collectors.joining("/"));
    }
}
