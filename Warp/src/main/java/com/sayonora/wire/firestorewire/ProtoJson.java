package com.sayonora.wire.firestorewire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * A small proto3-JSON codec driven by message descriptors (the REST surfaces of Firestore and Datastore are the gRPC messages in
 * proto3 JSON: camelCase names, int64 as strings, bytes as base64, Timestamp as RFC 3339, enums by name). protobuf-java-util is not
 * a Warp dependency, so this covers exactly what those APIs use, including google.protobuf wrappers, Timestamp, Duration, Empty.
 */
public final class ProtoJson {

    private ProtoJson() {
    }

    /** Thrown for JSON that does not fit the message; carries the location like Google's "Invalid JSON payload" errors. */
    public static final class JsonError extends IllegalArgumentException {
        public JsonError(String m) {
            super(m);
        }
    }

    // ------------------------------------------------------------------ message -> JSON

    public static JsonElement toJson(Message m) {
        String full = m.getDescriptorForType().getFullName();
        switch (full) {
            case "google.protobuf.Timestamp":
                return new JsonPrimitive(formatTimestamp((Timestamp) m));
            case "google.protobuf.Duration": {
                var d = (com.google.protobuf.Duration) m;
                String frac = d.getNanos() == 0 ? "" : "." + String.format("%09d", Math.abs(d.getNanos())).replaceAll("0+$", "");
                return new JsonPrimitive(d.getSeconds() + frac + "s");
            }
            case "google.protobuf.Empty":
                return new JsonObject();
            case "google.protobuf.Int32Value", "google.protobuf.Int64Value", "google.protobuf.DoubleValue", "google.protobuf.BoolValue",
                    "google.protobuf.StringValue", "google.protobuf.FloatValue", "google.protobuf.UInt32Value", "google.protobuf.UInt64Value":
                return scalar(m.getDescriptorForType().findFieldByName("value"), m.getField(m.getDescriptorForType().findFieldByName("value")));
            default:
                break;
        }
        JsonObject o = new JsonObject();
        for (Map.Entry<FieldDescriptor, Object> e : m.getAllFields().entrySet()) {
            FieldDescriptor fd = e.getKey();
            if (fd.isMapField()) {
                JsonObject mo = new JsonObject();
                FieldDescriptor vd = fd.getMessageType().findFieldByName("value");
                for (Object me : (List<?>) e.getValue()) {
                    Message entry = (Message) me;
                    Object k = entry.getField(fd.getMessageType().findFieldByName("key"));
                    mo.add(String.valueOf(k), single(vd, entry.getField(vd)));
                }
                o.add(fd.getJsonName(), mo);
            } else if (fd.isRepeated()) {
                JsonArray a = new JsonArray();
                for (Object x : (List<?>) e.getValue()) {
                    a.add(single(fd, x));
                }
                o.add(fd.getJsonName(), a);
            } else {
                o.add(fd.getJsonName(), single(fd, e.getValue()));
            }
        }
        return o;
    }

    private static JsonElement single(FieldDescriptor fd, Object v) {
        if (fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return toJson((Message) v);
        }
        return scalar(fd, v);
    }

    private static JsonElement scalar(FieldDescriptor fd, Object v) {
        switch (fd.getJavaType()) {
            case INT:
                return new JsonPrimitive(((Number) v).intValue());
            case LONG:
                return new JsonPrimitive(String.valueOf(v));
            case FLOAT:
            case DOUBLE: {
                double d = ((Number) v).doubleValue();
                if (Double.isNaN(d)) {
                    return new JsonPrimitive("NaN");
                }
                if (Double.isInfinite(d)) {
                    return new JsonPrimitive(d > 0 ? "Infinity" : "-Infinity");
                }
                return new JsonPrimitive(fd.getJavaType() == FieldDescriptor.JavaType.FLOAT ? (Number) (float) d : (Number) d);
            }
            case BOOLEAN:
                return new JsonPrimitive((Boolean) v);
            case STRING:
                return new JsonPrimitive((String) v);
            case BYTE_STRING:
                return new JsonPrimitive(Base64.getEncoder().encodeToString(((ByteString) v).toByteArray()));
            case ENUM:
                return new JsonPrimitive(((EnumValueDescriptor) v).getName());
            default:
                return JsonNull.INSTANCE;
        }
    }

    static String formatTimestamp(Timestamp t) {
        Instant i = Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
        String base = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC).format(i.truncatedTo(ChronoUnit.SECONDS));
        int n = t.getNanos();
        String frac = n == 0 ? "" : n % 1_000_000 == 0 ? String.format(".%03d", n / 1_000_000) : n % 1000 == 0 ? String.format(".%06d", n / 1000)
                : String.format(".%09d", n);
        return base + frac + "Z";
    }

    // ------------------------------------------------------------------ JSON -> message

    public static <B extends Message.Builder> B fromJson(JsonElement j, B b) {
        merge(j, b, "");
        return b;
    }

    private static void merge(JsonElement j, Message.Builder b, String path) {
        Descriptor d = b.getDescriptorForType();
        String full = d.getFullName();
        switch (full) {
            case "google.protobuf.Timestamp":
                ((Timestamp.Builder) b).mergeFrom(parseTimestamp(j, path));
                return;
            case "google.protobuf.Duration": {
                String s = str(j, path);
                if (!s.endsWith("s")) {
                    throw new JsonError("Invalid value at '" + path + "': Duration must end with 's'");
                }
                BigDecimal bd = new BigDecimal(s.substring(0, s.length() - 1));
                long sec = bd.longValue();
                ((com.google.protobuf.Duration.Builder) b).setSeconds(sec).setNanos(bd.subtract(BigDecimal.valueOf(sec)).movePointRight(9).intValue());
                return;
            }
            case "google.protobuf.Empty":
                return;
            case "google.protobuf.Int32Value", "google.protobuf.Int64Value", "google.protobuf.DoubleValue", "google.protobuf.BoolValue",
                    "google.protobuf.StringValue", "google.protobuf.FloatValue", "google.protobuf.UInt32Value", "google.protobuf.UInt64Value": {
                FieldDescriptor vf = d.findFieldByName("value");
                b.setField(vf, scalarFrom(vf, j, path));
                return;
            }
            default:
                break;
        }
        if (!j.isJsonObject()) {
            throw new JsonError("Invalid value at '" + path + "' (type " + d.getName() + "), expected an object");
        }
        for (Map.Entry<String, JsonElement> e : j.getAsJsonObject().entrySet()) {
            FieldDescriptor fd = d.findFieldByName(e.getKey());
            if (fd == null) {
                for (FieldDescriptor f : d.getFields()) {
                    if (f.getJsonName().equals(e.getKey())) {
                        fd = f;
                        break;
                    }
                }
            }
            String p = path.isEmpty() ? e.getKey() : path + "." + e.getKey();
            if (fd == null) {
                throw new JsonError("Invalid JSON payload received. Unknown name \"" + e.getKey() + "\"" + (path.isEmpty() ? "" : " at '" + path + "'")
                        + ": Cannot find field.");
            }
            JsonElement v = e.getValue();
            if (v.isJsonNull()) {
                continue;
            }
            if (fd.isMapField()) {
                FieldDescriptor kd = fd.getMessageType().findFieldByName("key");
                FieldDescriptor vd = fd.getMessageType().findFieldByName("value");
                if (!v.isJsonObject()) {
                    throw new JsonError("Invalid value at '" + p + "', expected an object");
                }
                for (Map.Entry<String, JsonElement> me : v.getAsJsonObject().entrySet()) {
                    Message.Builder eb = b.newBuilderForField(fd);
                    eb.setField(kd, scalarFrom(kd, new JsonPrimitive(me.getKey()), p));
                    setValue(eb, vd, me.getValue(), p + "[" + me.getKey() + "]");
                    b.addRepeatedField(fd, eb.build());
                }
            } else if (fd.isRepeated()) {
                if (!v.isJsonArray()) {
                    throw new JsonError("Invalid value at '" + p + "', expected an array");
                }
                int i = 0;
                for (JsonElement x : v.getAsJsonArray()) {
                    if (fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                        Message.Builder eb = b.newBuilderForField(fd);
                        merge(x, eb, p + "[" + i + "]");
                        b.addRepeatedField(fd, eb.build());
                    } else {
                        b.addRepeatedField(fd, scalarFrom(fd, x, p));
                    }
                    i++;
                }
            } else {
                setValue(b, fd, v, p);
            }
        }
    }

    private static void setValue(Message.Builder b, FieldDescriptor fd, JsonElement v, String p) {
        if (fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            Message.Builder sub = b.newBuilderForField(fd);
            merge(v, sub, p);
            b.setField(fd, sub.build());
        } else {
            b.setField(fd, scalarFrom(fd, v, p));
        }
    }

    private static String str(JsonElement j, String p) {
        if (!j.isJsonPrimitive()) {
            throw new JsonError("Invalid value at '" + p + "', expected a string");
        }
        return j.getAsString();
    }

    private static Object scalarFrom(FieldDescriptor fd, JsonElement j, String p) {
        if (!j.isJsonPrimitive()) {
            throw new JsonError("Invalid value at '" + p + "' (" + fd.getJavaType().name().toLowerCase() + "), " + j);
        }
        JsonPrimitive pr = j.getAsJsonPrimitive();
        try {
            switch (fd.getJavaType()) {
                case INT:
                    return new BigDecimal(pr.getAsString()).intValueExact();
                case LONG:
                    return new BigDecimal(pr.getAsString()).longValueExact();
                case FLOAT:
                case DOUBLE: {
                    String s = pr.getAsString();
                    double d = s.equals("NaN") ? Double.NaN : s.equals("Infinity") ? Double.POSITIVE_INFINITY
                            : s.equals("-Infinity") ? Double.NEGATIVE_INFINITY : Double.parseDouble(s);
                    return fd.getJavaType() == FieldDescriptor.JavaType.FLOAT ? (Object) (float) d : (Object) d;
                }
                case BOOLEAN:
                    if (pr.isBoolean()) {
                        return pr.getAsBoolean();
                    }
                    if (pr.getAsString().equals("true") || pr.getAsString().equals("false")) {
                        return Boolean.parseBoolean(pr.getAsString());
                    }
                    throw new JsonError("Invalid value at '" + p + "' (TYPE_BOOL), \"" + pr.getAsString() + "\"");
                case STRING:
                    if (pr.isString()) {
                        return pr.getAsString();
                    }
                    throw new JsonError("Invalid value at '" + p + "' (TYPE_STRING), " + pr.getAsString());
                case BYTE_STRING:
                    return ByteString.copyFrom(Base64.getDecoder().decode(pr.getAsString().replace('-', '+').replace('_', '/')));
                case ENUM: {
                    EnumValueDescriptor ev = pr.isNumber() ? fd.getEnumType().findValueByNumber(pr.getAsInt())
                            : fd.getEnumType().findValueByName(pr.getAsString());
                    if (ev == null) {
                        throw new JsonError("Invalid value at '" + p + "' (type " + fd.getEnumType().getName() + "), \"" + pr.getAsString() + "\"");
                    }
                    return ev;
                }
                default:
                    throw new JsonError("Unsupported field type at '" + p + "'");
            }
        } catch (ArithmeticException | IllegalArgumentException e) {
            if (e instanceof JsonError) {
                throw (JsonError) e;
            }
            throw new JsonError("Invalid value at '" + p + "' (" + fd.getJavaType().name().toLowerCase() + "), " + pr.getAsString());
        }
    }

    private static Timestamp parseTimestamp(JsonElement j, String p) {
        String s = str(j, p);
        try {
            Instant i = java.time.OffsetDateTime.parse(s).toInstant();
            return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
        } catch (RuntimeException e) {
            throw new JsonError("Invalid value at '" + p + "' (type.googleapis.com/google.protobuf.Timestamp), Field '" + p
                    + "', Invalid data.  Invalid time format: " + s);
        }
    }

    /** All string keys of an object, for callers that pre-validate. */
    static List<String> keys(JsonObject o) {
        return new ArrayList<>(o.keySet());
    }
}
