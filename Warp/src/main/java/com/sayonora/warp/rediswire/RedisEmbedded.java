package com.sayonora.warp.rediswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * In-process access to the rediswire command engine for the MCP tools: one command runs through the SAME registry
 * ({@code Cmd.Registry}), shard routing and Postgres handlers a RESP client reaches, on a headless {@link Session}
 * (no socket). The reply is encoded as RESP2 and decoded to JSON, so every reply type is covered without a
 * per-command mapping. Blocking commands are not offered by the tools; callers must not pass them.
 */
public final class RedisEmbedded implements AutoCloseable {

    /** A Redis error reply ({@code -ERR ...}). */
    public static final class ReplyError extends RuntimeException {
        public ReplyError(String message) {
            super(message);
        }
    }

    private final RedisStore store;
    private final Cmd.Registry registry = Cmd.buildRegistry();

    public RedisEmbedded(BackendRegistry registry, SqlMetricsCollector metrics) {
        this.store = new RedisStore(new RegistryBackends(registry, metrics), RedisOptions.fromEnv(0));
    }

    public int databases() {
        return (int) Math.max(1, RedisOptions.fromEnv(0).databases());
    }

    /** Runs one command on logical database {@code db}; throws {@link ReplyError} for an error reply. */
    public JsonElement command(int db, List<String> args) {
        byte[][] a = new byte[args.size()][];
        for (int i = 0; i < a.length; i++) {
            a[i] = args.get(i).getBytes(StandardCharsets.UTF_8);
        }
        Session s = new Session(store, registry, null, Runnable::run, () -> { });
        s.authed = true;
        s.db = db;
        s.reroute();
        Object reply = s.handle(a);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Resp.Writer w = new Resp.Writer(out);
            w.write(reply, false);
            w.flush();
            byte[] bytes = out.toByteArray();
            int[] pos = {0};
            return parse(bytes, pos);
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String line(byte[] b, int[] pos) {
        int start = pos[0];
        while (b[pos[0]] != '\r') {
            pos[0]++;
        }
        String s = new String(b, start, pos[0] - start, StandardCharsets.UTF_8);
        pos[0] += 2;
        return s;
    }

    private static JsonElement parse(byte[] b, int[] pos) {
        char t = (char) b[pos[0]++];
        switch (t) {
            case '+':
                return new JsonPrimitive(line(b, pos));
            case '-':
                throw new ReplyError(line(b, pos));
            case ':':
                return new JsonPrimitive(Long.parseLong(line(b, pos)));
            case '$': {
                int n = Integer.parseInt(line(b, pos));
                if (n < 0) {
                    return JsonNull.INSTANCE;
                }
                byte[] v = java.util.Arrays.copyOfRange(b, pos[0], pos[0] + n);
                pos[0] += n + 2;
                return text(v);
            }
            case '*': {
                int n = Integer.parseInt(line(b, pos));
                if (n < 0) {
                    return JsonNull.INSTANCE;
                }
                JsonArray arr = new JsonArray();
                for (int i = 0; i < n; i++) {
                    try {
                        arr.add(parse(b, pos));
                    } catch (ReplyError e) {
                        JsonObject o = new JsonObject();
                        o.addProperty("error", e.getMessage());
                        arr.add(o);
                    }
                }
                return arr;
            }
            default:
                throw new IllegalStateException("unexpected reply type " + t);
        }
    }

    /** UTF-8 text when valid, else {"base64": "..."}. */
    private static JsonElement text(byte[] v) {
        try {
            return new JsonPrimitive(StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(v)).toString());
        } catch (CharacterCodingException e) {
            JsonObject o = new JsonObject();
            o.addProperty("base64", Base64.getEncoder().encodeToString(v));
            return o;
        }
    }

    @Override
    public void close() {
        store.close();
    }
}
