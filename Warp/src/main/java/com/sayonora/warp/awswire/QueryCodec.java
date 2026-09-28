package com.sayonora.warp.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The AWS Query protocol, generically: form parameters ({@code Foo.member.1.Bar=x}, {@code Attributes.entry.1.key=k})
 * become the JSON request shape (lists from {@code member}/numbered segments, maps from {@code entry} pairs, every scalar a
 * string), and a JSON-shaped result becomes the XML response ({@code <member>} lists, {@code <entry>} maps for the
 * service's {@link AwsService#mapFields()}).
 */
public final class QueryCodec {

    private QueryCodec() {
    }

    private static final class Node {
        String value;
        final TreeMap<String, Node> kids = new TreeMap<>((a, b) -> {
            boolean an = isInt(a), bn = isInt(b);
            if (an && bn) {
                return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
            }
            return a.compareTo(b);
        });
    }

    private static boolean isInt(String s) {
        if (s.isEmpty() || s.length() > 9) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static JsonObject toJson(Map<String, String> params) {
        Node root = new Node();
        for (var e : params.entrySet()) {
            String k = e.getKey();
            if (k.equals("Action") || k.equals("Version") || k.startsWith("X-Amz-") || k.equals("AWSAccessKeyId")
                    || k.equals("Signature") || k.equals("SignatureMethod") || k.equals("SignatureVersion")
                    || k.equals("Timestamp") || k.equals("Expires")) {
                continue;
            }
            Node n = root;
            for (String seg : k.split("\\.")) {
                n = n.kids.computeIfAbsent(seg, s -> new Node());
            }
            n.value = e.getValue();
        }
        JsonElement e = convert(root, true);
        return e.isJsonObject() ? e.getAsJsonObject() : new JsonObject();
    }

    private static JsonElement convert(Node n, boolean top) {
        if (n.kids.isEmpty()) {
            return n.value == null ? JsonNull.INSTANCE : new JsonPrimitive(n.value);
        }
        if (!top && n.kids.size() == 1 && n.kids.containsKey("member")) {
            return list(n.kids.get("member"));
        }
        if (!top && n.kids.size() == 1 && n.kids.containsKey("entry")) {
            return entries(n.kids.get("entry"));
        }
        if (!top && allInts(n)) {
            return list(n);
        }
        JsonObject o = new JsonObject();
        for (var e : n.kids.entrySet()) {
            o.add(e.getKey(), convert(e.getValue(), false));
        }
        return o;
    }

    private static boolean allInts(Node n) {
        for (String k : n.kids.keySet()) {
            if (!isInt(k)) {
                return false;
            }
        }
        return true;
    }

    private static JsonElement list(Node holder) {
        JsonArray a = new JsonArray();
        for (Node item : holder.kids.values()) {
            a.add(convert(item, false));
        }
        return a;
    }

    private static JsonElement entries(Node holder) {
        JsonObject o = new JsonObject();
        for (Node item : holder.kids.values()) {
            Node key = item.kids.containsKey("key") ? item.kids.get("key") : item.kids.get("Name");
            Node val = item.kids.containsKey("value") ? item.kids.get("value") : item.kids.get("Value");
            if (key != null && key.value != null) {
                o.add(key.value, val == null ? new JsonPrimitive("") : convert(val, false));
            }
        }
        return o;
    }

    // ---------------------------------------------------------------------------------------------- response

    public static String renderResponse(AwsService svc, String action, JsonObject result, String requestId) {
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\"?>\n");
        String ns = svc.xmlNamespace();
        sb.append('<').append(action).append("Response");
        if (ns != null) {
            sb.append(" xmlns=\"").append(ns).append('"');
        }
        sb.append('>');
        // always present, even empty: botocore looks for <ActionResult> when the operation's output shape has a result wrapper
        // (TagResource, UntagResource, ...), and SDKs ignore it for operations with no output
        sb.append('<').append(action).append("Result>");
        if (result != null) {
            for (var e : result.entrySet()) {
                render(sb, svc.mapFields(), e.getKey(), e.getValue());
            }
        }
        sb.append("</").append(action).append("Result>");
        sb.append("<ResponseMetadata><RequestId>").append(requestId).append("</RequestId></ResponseMetadata></")
                .append(action).append("Response>");
        return sb.toString();
    }

    private static void render(StringBuilder sb, Set<String> maps, String name, JsonElement v) {
        if (v == null || v.isJsonNull()) {
            return;
        }
        if (v.isJsonPrimitive()) {
            sb.append('<').append(name).append('>').append(esc(v.getAsString())).append("</").append(name).append('>');
        } else if (v.isJsonArray()) {
            sb.append('<').append(name).append('>');
            for (JsonElement item : v.getAsJsonArray()) {
                if (item.isJsonPrimitive()) {
                    sb.append("<member>").append(esc(item.getAsString())).append("</member>");
                } else {
                    sb.append("<member>");
                    renderFields(sb, maps, item.getAsJsonObject());
                    sb.append("</member>");
                }
            }
            sb.append("</").append(name).append('>');
        } else if (maps.contains(name)) {
            sb.append('<').append(name).append('>');
            for (var e : v.getAsJsonObject().entrySet()) {
                sb.append("<entry><key>").append(esc(e.getKey())).append("</key><value>");
                if (e.getValue().isJsonPrimitive()) {
                    sb.append(esc(e.getValue().getAsString()));
                } else if (e.getValue().isJsonObject()) {
                    renderFields(sb, maps, e.getValue().getAsJsonObject());
                }
                sb.append("</value></entry>");
            }
            sb.append("</").append(name).append('>');
        } else {
            sb.append('<').append(name).append('>');
            renderFields(sb, maps, v.getAsJsonObject());
            sb.append("</").append(name).append('>');
        }
    }

    private static void renderFields(StringBuilder sb, Set<String> maps, JsonObject o) {
        for (var e : o.entrySet()) {
            render(sb, maps, e.getKey(), e.getValue());
        }
    }

    public static String renderError(AwsService svc, String code, String message, String requestId, boolean sender) {
        String ns = svc == null ? null : svc.xmlNamespace();
        return "<?xml version=\"1.0\"?>\n<ErrorResponse" + (ns == null ? "" : " xmlns=\"" + ns + "\"")
                + "><Error><Type>" + (sender ? "Sender" : "Receiver") + "</Type><Code>" + esc(code) + "</Code><Message>"
                + esc(message) + "</Message></Error><RequestId>" + requestId + "</RequestId></ErrorResponse>";
    }

    public static String esc(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&apos;");
                default -> {
                    if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
                        sb.append("&#").append((int) c).append(';');
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    public static Map<String, String> parseForm(String... sources) {
        Map<String, String> into = new LinkedHashMap<>();
        for (String source : sources) {
            if (source == null || source.isBlank()) {
                continue;
            }
            for (String pair : source.split("&")) {
                if (pair.isEmpty()) {
                    continue;
                }
                int eq = pair.indexOf('=');
                String key = eq < 0 ? pair : pair.substring(0, eq);
                String value = eq < 0 ? "" : pair.substring(eq + 1);
                into.put(java.net.URLDecoder.decode(key, java.nio.charset.StandardCharsets.UTF_8),
                        java.net.URLDecoder.decode(value, java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        return into;
    }
}
