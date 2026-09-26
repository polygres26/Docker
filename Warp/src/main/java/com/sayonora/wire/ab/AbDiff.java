package com.sayonora.wire.ab;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/** Normalised comparison of two responses: status, a few headers, and the body (JSON / XML flattened to path->value). */
public final class AbDiff {

    /** One side's response as captured for comparison (body capped, full-body digest and length always kept). */
    public static final class Capture {
        public int status;
        public final Map<String, String> headers = new HashMap<>();
        public byte[] head = new byte[0];
        public long total;
        public String sha256 = "";
        public boolean truncated;
        public long nanos;
        public String error;
    }

    static final int MAX_DIFFS = 20;

    private AbDiff() {
    }

    /** @param structured false for opaque payloads (S3 object bytes): compared by digest only */
    public static List<String> diff(Capture local, Capture cloud, Set<String> ignore, boolean recordValues, boolean structured) {
        List<String> out = new ArrayList<>();
        if (local.error != null || cloud.error != null) {
            if (local.error != null) {
                out.add("local side failed: " + local.error);
            }
            if (cloud.error != null) {
                out.add("cloud side failed: " + cloud.error);
            }
            return out;
        }
        if (local.status != cloud.status) {
            out.add("status: local=" + local.status + " cloud=" + cloud.status);
        }
        for (String h : List.of("content-type", "etag", "x-amz-version-id")) {
            String a = norm(h, local.headers.get(h));
            String b = norm(h, cloud.headers.get(h));
            if (a != null && b != null && !a.equals(b)) {
                out.add("header " + h + " differs" + (recordValues ? ": local=" + cut(a) + " cloud=" + cut(b) : ""));
            } else if ((a == null) != (b == null) && h.equals("content-type")) {
                out.add("header " + h + " present only on " + (a == null ? "cloud" : "local"));
            }
        }
        if (local.sha256.equals(cloud.sha256) && local.total == cloud.total) {
            return out;
        }
        if (structured && !local.truncated && !cloud.truncated) {
            Map<String, String> a = flatten(local, ignore);
            Map<String, String> b = flatten(cloud, ignore);
            if (a != null && b != null) {
                for (var e : a.entrySet()) {
                    if (out.size() >= MAX_DIFFS) {
                        break;
                    }
                    String other = b.get(e.getKey());
                    if (other == null) {
                        out.add("body " + e.getKey() + ": missing on cloud");
                    } else if (!other.equals(e.getValue())) {
                        out.add("body " + e.getKey() + ": value differs" + (recordValues
                                ? " (local=" + cut(e.getValue()) + " cloud=" + cut(other) + ")" : ""));
                    }
                }
                for (String k : b.keySet()) {
                    if (out.size() >= MAX_DIFFS) {
                        break;
                    }
                    if (!a.containsKey(k)) {
                        out.add("body " + k + ": missing on local");
                    }
                }
                return out;
            }
        }
        out.add("body differs (local " + local.total + " bytes, cloud " + cloud.total + " bytes"
                + (local.sha256.equals(cloud.sha256) ? "" : ", sha256 differs") + ")");
        return out;
    }

    private static String norm(String header, String v) {
        if (v == null) {
            return null;
        }
        if (header.equals("content-type")) {
            int i = v.indexOf(';');
            return (i < 0 ? v : v.substring(0, i)).trim().toLowerCase(Locale.ROOT);
        }
        return v.replace("\"", "").trim();
    }

    private static String cut(String s) {
        return s.length() > 60 ? s.substring(0, 60) + "..." : s;
    }

    /** null = not parseable as JSON/XML (caller falls back to the raw comparison). */
    static Map<String, String> flatten(Capture c, Set<String> ignore) {
        String body = new String(c.head, StandardCharsets.UTF_8).trim();
        if (body.isEmpty()) {
            return new TreeMap<>();
        }
        try {
            Map<String, String> out = new TreeMap<>();
            if (body.startsWith("{") || body.startsWith("[")) {
                flatJson("", JsonParser.parseString(body), ignore, out);
                return out;
            }
            if (body.startsWith("<")) {
                DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
                f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                Element root = f.newDocumentBuilder().parse(new ByteArrayInputStream(c.head)).getDocumentElement();
                flatXml(root.getNodeName(), root, ignore, out);
                return out;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    private static void flatJson(String path, JsonElement e, Set<String> ignore, Map<String, String> out) {
        if (e.isJsonObject()) {
            for (var en : e.getAsJsonObject().entrySet()) {
                if (!ignore.contains(en.getKey())) {
                    flatJson(path + "/" + en.getKey(), en.getValue(), ignore, out);
                }
            }
        } else if (e.isJsonArray()) {
            JsonArray a = e.getAsJsonArray();
            for (int i = 0; i < a.size(); i++) {
                flatJson(path + "[" + i + "]", a.get(i), ignore, out);
            }
            if (a.size() == 0) {
                out.put(path, "[]");
            }
        } else if (e.isJsonNull()) {
            out.put(path, "null");
        } else if (e.getAsJsonPrimitive().isNumber()) {
            out.put(path, new BigDecimal(e.getAsString()).stripTrailingZeros().toPlainString());
        } else {
            out.put(path, e.getAsString());
        }
    }

    private static void flatXml(String path, Element el, Set<String> ignore, Map<String, String> out) {
        Map<String, Integer> seen = new HashMap<>();
        boolean hasChild = false;
        StringBuilder text = new StringBuilder();
        for (Node n = el.getFirstChild(); n != null; n = n.getNextSibling()) {
            if (n instanceof Element ce) {
                hasChild = true;
                String name = ce.getNodeName().replaceAll("^.*:", "");
                if (ignore.contains(name)) {
                    continue;
                }
                int idx = seen.merge(name, 1, Integer::sum) - 1;
                flatXml(path + "/" + name + "[" + idx + "]", ce, ignore, out);
            } else if (n.getNodeType() == Node.TEXT_NODE || n.getNodeType() == Node.CDATA_SECTION_NODE) {
                text.append(n.getTextContent());
            }
        }
        if (!hasChild) {
            out.put(path, text.toString().trim());
        }
    }
}
