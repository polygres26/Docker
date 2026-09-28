package com.sayonora.warp.sqswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Message attribute handling shared by both wire protocols: validation of what a sender supplies,
 * the exact {@code MD5OfMessageAttributes} algorithm AWS SDKs verify client side, and the
 * receive-side name filters. Message attributes are stored as the JSON object the JSON protocol
 * carries ({@code {"name": {"DataType": "...", "StringValue": "..."}}}, binary values base64).
 *
 * <p>MD5 algorithm (AWS docs "Calculating the MD5 message digest for message attributes"): attributes sorted by
 * name (byte order); for each, 4-byte big-endian length + UTF-8 name, 4-byte length + UTF-8 data
 * type (the full type including any custom {@code .suffix}), one transport byte (1 = string value
 * for String/Number, 2 = binary), then 4-byte length + value bytes (UTF-8 for strings, raw bytes for
 * binary); the digest is the lowercase hex MD5 of the concatenation.
 */
public final class SqsMessageAttributes {

    public static final int MAX_ATTRIBUTES = 10;
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.\\-]{1,256}");
    private static final Pattern NUMBER = Pattern.compile("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?");

    private SqsMessageAttributes() {
    }

    public static String md5Hex(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(data);
            StringBuilder sb = new StringBuilder(32);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String md5Hex(String s) {
        return md5Hex(s.getBytes(StandardCharsets.UTF_8));
    }

    public static String sha256Hex(String s) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** MD5 over the given attributes, or {@code null} when there are none. */
    public static String md5OfAttributes(JsonObject attributes) {
        if (attributes == null || attributes.size() == 0) {
            return null;
        }
        // AWS sorts by name in UTF-8 byte order; for the ASCII-only names SQS allows that equals String order.
        TreeMap<String, JsonObject> sorted = new TreeMap<>();
        for (Map.Entry<String, JsonElement> e : attributes.entrySet()) {
            sorted.put(e.getKey(), e.getValue().getAsJsonObject());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Map.Entry<String, JsonObject> e : sorted.entrySet()) {
            JsonObject v = e.getValue();
            String type = v.get("DataType").getAsString();
            writeLengthPrefixed(out, e.getKey().getBytes(StandardCharsets.UTF_8));
            writeLengthPrefixed(out, type.getBytes(StandardCharsets.UTF_8));
            if (isBinary(v)) {
                out.write(2);
                writeLengthPrefixed(out, Base64.getDecoder().decode(v.get("BinaryValue").getAsString()));
            } else {
                out.write(1);
                writeLengthPrefixed(out, v.get("StringValue").getAsString().getBytes(StandardCharsets.UTF_8));
            }
        }
        return md5Hex(out.toByteArray());
    }

    private static boolean isBinary(JsonObject v) {
        return v.has("BinaryValue") && !v.get("BinaryValue").isJsonNull();
    }

    private static void writeLengthPrefixed(ByteArrayOutputStream out, byte[] bytes) {
        out.writeBytes(ByteBuffer.allocate(4).putInt(bytes.length).array());
        out.writeBytes(bytes);
    }

    /**
     * Validates a sender-supplied attribute map (JSON protocol shape) and returns a normalised
     * copy (only DataType + StringValue/BinaryValue kept). Throws the SQS error real SQS would.
     */
    public static JsonObject validateAndNormalise(JsonObject attributes) {
        JsonObject out = new JsonObject();
        if (attributes == null) {
            return out;
        }
        if (attributes.size() > MAX_ATTRIBUTES) {
            throw SqsException.invalidParam("Number of message attributes [" + attributes.size()
                    + "] exceeds the allowed maximum [" + MAX_ATTRIBUTES + "].");
        }
        for (Map.Entry<String, JsonElement> e : attributes.entrySet()) {
            String name = e.getKey();
            validateName(name);
            if (!e.getValue().isJsonObject()) {
                throw SqsException.invalidParam("The message attribute '" + name + "' must be an object.");
            }
            JsonObject v = e.getValue().getAsJsonObject();
            String type = v.has("DataType") && !v.get("DataType").isJsonNull() ? v.get("DataType").getAsString() : null;
            if (type == null || type.isEmpty()) {
                throw SqsException.invalidParam("The message attribute '" + name + "' must contain a non-empty message attribute type.");
            }
            String base = type.contains(".") ? type.substring(0, type.indexOf('.')) : type;
            if (type.length() > 256 || !(base.equals("String") || base.equals("Number") || base.equals("Binary"))
                    || (type.contains(".") && type.endsWith("."))) {
                throw SqsException.invalidParam("The type of message attribute '" + name + "' is invalid. You must use only the following supported type prefixes: Binary, Number, String.");
            }
            JsonObject n = new JsonObject();
            n.addProperty("DataType", type);
            boolean hasString = v.has("StringValue") && !v.get("StringValue").isJsonNull();
            boolean hasBinary = isBinary(v);
            if (base.equals("Binary")) {
                if (!hasBinary || v.get("BinaryValue").getAsString().isEmpty()) {
                    throw SqsException.invalidParam("The message attribute '" + name
                            + "' must contain non-empty message attribute value for message attribute type 'Binary'.");
                }
                try {
                    Base64.getDecoder().decode(v.get("BinaryValue").getAsString());
                } catch (IllegalArgumentException ex) {
                    throw SqsException.invalidParam("The message attribute '" + name + "' has an invalid binary value.");
                }
                n.addProperty("BinaryValue", v.get("BinaryValue").getAsString());
            } else {
                if (!hasString || v.get("StringValue").getAsString().isEmpty()) {
                    throw SqsException.invalidParam("The message attribute '" + name
                            + "' must contain non-empty message attribute value for message attribute type '" + base + "'.");
                }
                String sv = v.get("StringValue").getAsString();
                if (base.equals("Number") && !NUMBER.matcher(sv).matches()) {
                    throw SqsException.invalidParam("Could not cast message attribute '" + name + "' value to number.");
                }
                n.addProperty("StringValue", sv);
            }
            out.add(name, n);
        }
        return out;
    }

    private static void validateName(String name) {
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        if (!NAME.matcher(name).matches() || name.startsWith(".") || name.endsWith(".") || name.contains("..")
                || lower.startsWith("aws.") || lower.startsWith("amazon.")) {
            throw SqsException.invalidParam("Invalid message attribute name '" + name
                    + "'. Message attribute names can contain alphanumeric characters, hyphens, underscores and periods; "
                    + "must not start or end with a period, contain consecutive periods, or start with AWS. or Amazon.");
        }
    }

    /** Bytes counted against MaximumMessageSize: body + per-attribute name, type and value. */
    public static int payloadSize(String body, JsonObject attributes) {
        int total = body == null ? 0 : body.getBytes(StandardCharsets.UTF_8).length;
        if (attributes != null) {
            for (Map.Entry<String, JsonElement> e : attributes.entrySet()) {
                JsonObject v = e.getValue().getAsJsonObject();
                total += e.getKey().getBytes(StandardCharsets.UTF_8).length;
                total += v.get("DataType").getAsString().getBytes(StandardCharsets.UTF_8).length;
                if (isBinary(v)) {
                    total += Base64.getDecoder().decode(v.get("BinaryValue").getAsString()).length;
                } else if (v.has("StringValue")) {
                    total += v.get("StringValue").getAsString().getBytes(StandardCharsets.UTF_8).length;
                }
            }
        }
        return total;
    }

    /** Receive-side filter: {@code All}, {@code .*}, an exact name, or a {@code prefix.*} pattern. */
    public static JsonObject filter(JsonObject attributes, List<String> requestedNames) {
        JsonObject out = new JsonObject();
        if (attributes == null || attributes.size() == 0 || requestedNames == null || requestedNames.isEmpty()) {
            return out;
        }
        for (Map.Entry<String, JsonElement> e : attributes.entrySet()) {
            for (String p : requestedNames) {
                if (matches(p, e.getKey())) {
                    out.add(e.getKey(), e.getValue());
                    break;
                }
            }
        }
        return out;
    }

    static boolean matches(String pattern, String name) {
        if (pattern.equals("All") || pattern.equals(".*")) {
            return true;
        }
        if (pattern.endsWith(".*")) {
            return name.startsWith(pattern.substring(0, pattern.length() - 1));
        }
        return pattern.equals(name);
    }

    /** Normalises {@code MessageSystemAttributes}: only {@code AWSTraceHeader} (a String) is settable. */
    public static String traceHeader(JsonObject systemAttributes) {
        if (systemAttributes == null || systemAttributes.size() == 0) {
            return null;
        }
        String trace = null;
        for (Map.Entry<String, JsonElement> e : systemAttributes.entrySet()) {
            if (!"AWSTraceHeader".equals(e.getKey())) {
                throw SqsException.invalidParam("Value " + e.getKey() + " for parameter MessageSystemAttributeName is invalid. "
                        + "Reason: Only AWSTraceHeader is supported.");
            }
            JsonObject v = e.getValue().getAsJsonObject();
            if (!v.has("StringValue") || !"String".equals(v.has("DataType") ? v.get("DataType").getAsString() : null)) {
                throw SqsException.invalidParam("Message system attribute AWSTraceHeader must have DataType String and a StringValue.");
            }
            trace = v.get("StringValue").getAsString();
            if (trace.isEmpty() || trace.length() > 256) {
                throw SqsException.invalidParam("The message system attribute AWSTraceHeader is not a valid X-Ray trace header.");
            }
        }
        return trace;
    }

    /** MD5OfMessageSystemAttributes for a stored trace header. */
    public static String md5OfTraceHeader(String trace) {
        if (trace == null) {
            return null;
        }
        JsonObject o = new JsonObject();
        JsonObject v = new JsonObject();
        v.addProperty("DataType", "String");
        v.addProperty("StringValue", trace);
        o.add("AWSTraceHeader", v);
        return md5OfAttributes(o);
    }

    static List<String> names(JsonObject attributes) {
        return attributes == null ? List.of() : new ArrayList<>(attributes.keySet());
    }
}
