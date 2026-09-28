package com.sayonora.warp.gremlinwire;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/** Message serializers negotiated by mimetype: GraphSON 3.0 (typed / untyped), GraphSON 2.0 and GraphBinary 1.0 (+ string results). */
final class Serializers {

    private Serializers() {
    }

    /** A request as decoded from the wire. */
    static final class Req {
        Object requestId;
        String op;
        String processor;
        Map<String, Object> args = new LinkedHashMap<>();
    }

    /** A response message before encoding. */
    static final class Resp {
        Object requestId;
        int code;
        String message = "";
        Map<String, Object> attributes = new LinkedHashMap<>();
        Map<String, Object> meta = new LinkedHashMap<>();
        Object data;
        boolean hasData;
    }

    interface Ser {
        String mime();

        boolean binary();

        Req decode(byte[] payload, int offset);

        byte[] encode(Resp r);
    }

    static final String GRAPHBINARY = "application/vnd.graphbinary-v1.0";
    static final String GRAPHBINARY_STRINGD = "application/vnd.graphbinary-v1.0-stringd";

    /** The serializer for {@code mime} (parameters after ';' other than types=false are ignored), or null. */
    static Ser forMime(String mime) {
        if (mime == null) {
            return null;
        }
        String m = mime.trim().toLowerCase(Locale.ROOT);
        boolean untyped = m.contains("types=false");
        String base = m.contains(";") ? m.substring(0, m.indexOf(';')).trim() : m;
        switch (base) {
            case "application/json", "application/vnd.gremlin-v3.0+json":
                return new Json(untyped ? GraphSon.UNTYPED : GraphSon.V3, untyped ? "application/vnd.gremlin-v3.0+json;types=false" : base);
            case "application/vnd.gremlin-v2.0+json":
                return new Json(untyped ? GraphSon.UNTYPED : GraphSon.V2, untyped ? "application/vnd.gremlin-v2.0+json;types=false" : base);
            case GRAPHBINARY:
                return new Bin(false);
            case GRAPHBINARY_STRINGD:
                return new Bin(true);
            default:
                return null;
        }
    }

    // ------------------------------------------------------------------ GraphSON

    static final class Json implements Ser {
        final GraphSon w;
        final String mime;

        Json(GraphSon w, String mime) {
            this.w = w;
            this.mime = mime;
        }

        @Override
        public String mime() {
            return mime;
        }

        @Override
        public boolean binary() {
            return false;
        }

        @Override
        public Req decode(byte[] payload, int offset) {
            JsonObject o;
            try {
                JsonElement e = JsonParser.parseString(new String(payload, offset, payload.length - offset, StandardCharsets.UTF_8));
                if (!e.isJsonObject()) {
                    throw new JsonParseException("not an object");
                }
                o = e.getAsJsonObject();
            } catch (JsonParseException | IllegalStateException e) {
                throw new G.GremlinError(498, "Message could not be parsed. Check the format of the request. [" + e.getMessage() + "]");
            }
            Req r = new Req();
            try {
                Object id = o.has("requestId") ? GraphSon.V3.read(o.get("requestId")) : null;
                r.requestId = id;
                r.op = o.has("op") && !o.get("op").isJsonNull() ? o.get("op").getAsString() : null;
                r.processor = o.has("processor") && !o.get("processor").isJsonNull() ? o.get("processor").getAsString() : "";
                if (o.has("args") && o.get("args").isJsonObject()) {
                    for (Map.Entry<String, JsonElement> e : o.getAsJsonObject("args").entrySet()) {
                        r.args.put(e.getKey(), GraphSon.V3.read(e.getValue()));
                    }
                }
            } catch (G.GremlinError e) {
                throw e;
            } catch (RuntimeException e) {
                throw new G.GremlinError(498, "Message could not be parsed. Check the format of the request. [" + e.getMessage() + "]");
            }
            if (r.op == null) {
                throw new G.GremlinError(498, "Message could not be parsed. Check the format of the request. [no op]");
            }
            return r;
        }

        @Override
        public byte[] encode(Resp r) {
            JsonObject o = new JsonObject();
            o.add("requestId", r.requestId == null ? JsonNull.INSTANCE : new JsonPrimitive(String.valueOf(r.requestId)));
            JsonObject st = new JsonObject();
            st.addProperty("message", r.message == null ? "" : r.message);
            st.addProperty("code", r.code);
            st.add("attributes", w.write(r.attributes));
            o.add("status", st);
            JsonObject res = new JsonObject();
            res.add("data", r.hasData ? w.write(r.data) : JsonNull.INSTANCE);
            res.add("meta", w.write(r.meta));
            o.add("result", res);
            return o.toString().getBytes(StandardCharsets.UTF_8);
        }

        /** Body of an HTTP JSON request ({gremlin, bindings, language, aliases}). */
        static JsonObject parseHttpBody(String body) {
            try {
                JsonElement e = JsonParser.parseString(body);
                return e.isJsonObject() ? e.getAsJsonObject() : null;
            } catch (JsonParseException ex) {
                return null;
            }
        }
    }

    // ------------------------------------------------------------------ GraphBinary

    static final class Bin implements Ser {
        final boolean strings;

        Bin(boolean strings) {
            this.strings = strings;
        }

        @Override
        public String mime() {
            return strings ? GRAPHBINARY_STRINGD : GRAPHBINARY;
        }

        @Override
        public boolean binary() {
            return true;
        }

        @Override
        public Req decode(byte[] payload, int offset) {
            GraphBinary.Request q;
            try {
                q = GraphBinary.readRequest(ByteBuffer.wrap(payload, offset, payload.length - offset));
            } catch (G.GremlinError e) {
                throw e;
            } catch (RuntimeException e) {
                throw new G.GremlinError(498, "Message could not be parsed. Check the format of the request. [" + e + "]");
            }
            Req r = new Req();
            r.requestId = q.requestId;
            r.op = q.op;
            r.processor = q.processor;
            r.args = q.args;
            return r;
        }

        @Override
        public byte[] encode(Resp r) {
            UUID id = null;
            if (r.requestId instanceof UUID u) {
                id = u;
            } else if (r.requestId != null) {
                try {
                    id = UUID.fromString(String.valueOf(r.requestId));
                } catch (IllegalArgumentException ignored) {
                    id = null;
                }
            }
            return GraphBinary.response(id, r.code, r.message, r.attributes, r.meta, r.data, r.hasData, strings);
        }
    }
}
