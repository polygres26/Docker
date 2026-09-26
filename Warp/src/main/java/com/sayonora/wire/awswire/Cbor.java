package com.sayonora.wire.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

/**
 * A minimal CBOR (RFC 8949) codec for the {@code application/x-amz-cbor-1.1} protocol AWS SDK v2 uses for Kinesis. Byte
 * strings decode to base64 text (the JSON-protocol blob shape) and tag 1 timestamps to epoch-second numbers, so services
 * see the same request object as for JSON; on encode the service names its blob and timestamp fields.
 */
public final class Cbor {

    private Cbor() {
    }

    // ------------------------------------------------------------------------------------------ decode

    public static JsonElement decode(byte[] data) {
        if (data.length == 0) {
            return new JsonObject();
        }
        ByteBuffer b = ByteBuffer.wrap(data);
        return read(b);
    }

    private static long argument(ByteBuffer b, int info) {
        return switch (info) {
            case 24 -> b.get() & 0xffL;
            case 25 -> b.getShort() & 0xffffL;
            case 26 -> b.getInt() & 0xffffffffL;
            case 27 -> b.getLong();
            default -> info;
        };
    }

    private static JsonElement read(ByteBuffer b) {
        int head = b.get() & 0xff;
        int major = head >> 5;
        int info = head & 0x1f;
        switch (major) {
            case 0:
                return new JsonPrimitive(unsigned(argument(b, info)));
            case 1: {
                long v = argument(b, info);
                return new JsonPrimitive(v >= 0 ? BigDecimal.valueOf(-1).subtract(BigDecimal.valueOf(v))
                        : BigDecimal.valueOf(-1).subtract(new BigDecimal(Long.toUnsignedString(v))));
            }
            case 2: {
                byte[] bytes = info == 31 ? chunks(b, 2) : take(b, argument(b, info));
                return new JsonPrimitive(Base64.getEncoder().encodeToString(bytes));
            }
            case 3: {
                byte[] bytes = info == 31 ? chunks(b, 3) : take(b, argument(b, info));
                return new JsonPrimitive(new String(bytes, StandardCharsets.UTF_8));
            }
            case 4: {
                JsonArray a = new JsonArray();
                if (info == 31) {
                    while ((b.get(b.position()) & 0xff) != 0xff) {
                        a.add(read(b));
                    }
                    b.get();
                } else {
                    long n = argument(b, info);
                    for (long i = 0; i < n; i++) {
                        a.add(read(b));
                    }
                }
                return a;
            }
            case 5: {
                JsonObject o = new JsonObject();
                if (info == 31) {
                    while ((b.get(b.position()) & 0xff) != 0xff) {
                        JsonElement k = read(b);
                        o.add(k.getAsString(), read(b));
                    }
                    b.get();
                } else {
                    long n = argument(b, info);
                    for (long i = 0; i < n; i++) {
                        JsonElement k = read(b);
                        o.add(k.getAsString(), read(b));
                    }
                }
                return o;
            }
            case 6: {
                argument(b, info);
                return read(b); // tag 1 (epoch seconds) and any other tag: keep the content
            }
            default:
                switch (info) {
                    case 20:
                        return new JsonPrimitive(false);
                    case 21:
                        return new JsonPrimitive(true);
                    case 22:
                    case 23:
                        return JsonNull.INSTANCE;
                    case 25:
                        return new JsonPrimitive(half(b.getShort() & 0xffff));
                    case 26:
                        return new JsonPrimitive(b.getFloat());
                    case 27:
                        return new JsonPrimitive(b.getDouble());
                    default:
                        return JsonNull.INSTANCE;
                }
        }
    }

    private static BigDecimal unsigned(long v) {
        return v >= 0 ? BigDecimal.valueOf(v) : new BigDecimal(Long.toUnsignedString(v));
    }

    private static double half(int h) {
        int exp = (h >> 10) & 0x1f;
        int mant = h & 0x3ff;
        double v = exp == 0 ? Math.scalb((double) mant, -24)
                : exp != 31 ? Math.scalb((double) (mant + 1024), exp - 25) : mant == 0 ? Double.POSITIVE_INFINITY : Double.NaN;
        return (h & 0x8000) == 0 ? v : -v;
    }

    private static byte[] take(ByteBuffer b, long n) {
        byte[] out = new byte[(int) n];
        b.get(out);
        return out;
    }

    private static byte[] chunks(ByteBuffer b, int major) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ((b.get(b.position()) & 0xff) != 0xff) {
            int head = b.get() & 0xff;
            byte[] part = take(b, argument(b, head & 0x1f));
            out.write(part, 0, part.length);
        }
        b.get();
        return out.toByteArray();
    }

    // ------------------------------------------------------------------------------------------ encode

    public static byte[] encode(JsonElement e, Set<String> blobs, Set<String> timestamps) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, e, null, blobs, timestamps);
        return out.toByteArray();
    }

    private static void head(ByteArrayOutputStream out, int major, long v) {
        int m = major << 5;
        if (v < 24) {
            out.write(m | (int) v);
        } else if (v < 0x100) {
            out.write(m | 24);
            out.write((int) v);
        } else if (v < 0x10000) {
            out.write(m | 25);
            out.write((int) (v >> 8));
            out.write((int) v);
        } else if (v < 0x100000000L) {
            out.write(m | 26);
            for (int s = 24; s >= 0; s -= 8) {
                out.write((int) (v >> s));
            }
        } else {
            out.write(m | 27);
            for (int s = 56; s >= 0; s -= 8) {
                out.write((int) (v >> s));
            }
        }
    }

    private static void write(ByteArrayOutputStream out, JsonElement e, String field, Set<String> blobs, Set<String> ts) {
        if (e == null || e.isJsonNull()) {
            out.write(0xf6);
        } else if (e.isJsonObject()) {
            JsonObject o = e.getAsJsonObject();
            head(out, 5, o.size());
            for (Map.Entry<String, JsonElement> en : o.entrySet()) {
                byte[] k = en.getKey().getBytes(StandardCharsets.UTF_8);
                head(out, 3, k.length);
                out.write(k, 0, k.length);
                write(out, en.getValue(), en.getKey(), blobs, ts);
            }
        } else if (e.isJsonArray()) {
            JsonArray a = e.getAsJsonArray();
            head(out, 4, a.size());
            for (JsonElement x : a) {
                write(out, x, field, blobs, ts);
            }
        } else {
            JsonPrimitive p = e.getAsJsonPrimitive();
            if (p.isBoolean()) {
                out.write(p.getAsBoolean() ? 0xf5 : 0xf4);
            } else if (p.isNumber()) {
                BigDecimal d = p.getAsBigDecimal();
                if (field != null && ts.contains(field)) {
                    out.write(0xc1);
                    out.write(0xfb);
                    long bits = Double.doubleToLongBits(d.doubleValue());
                    for (int s = 56; s >= 0; s -= 8) {
                        out.write((int) (bits >> s));
                    }
                } else if (d.scale() <= 0 && d.abs().compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) < 0) {
                    long v = d.longValue();
                    if (v >= 0) {
                        head(out, 0, v);
                    } else {
                        head(out, 1, -1 - v);
                    }
                } else {
                    out.write(0xfb);
                    long bits = Double.doubleToLongBits(d.doubleValue());
                    for (int s = 56; s >= 0; s -= 8) {
                        out.write((int) (bits >> s));
                    }
                }
            } else if (field != null && blobs.contains(field)) {
                byte[] raw = Base64.getDecoder().decode(p.getAsString());
                head(out, 2, raw.length);
                out.write(raw, 0, raw.length);
            } else {
                byte[] s = p.getAsString().getBytes(StandardCharsets.UTF_8);
                head(out, 3, s.length);
                out.write(s, 0, s.length);
            }
        }
    }
}
