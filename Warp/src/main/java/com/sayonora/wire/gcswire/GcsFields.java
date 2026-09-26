package com.sayonora.wire.gcswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.LinkedHashMap;
import java.util.Map;

/** The {@code fields=} partial-response mask of Google APIs: {@code a,b/c,items(name,size),*}. */
final class GcsFields {

    /** A parsed mask node; {@code all} = keep the whole subtree. */
    private static final class Node {
        boolean all;
        final Map<String, Node> kids = new LinkedHashMap<>();
    }

    private final Node root;

    private GcsFields(Node root) {
        this.root = root;
    }

    static GcsFields parse(String spec) {
        if (spec == null || spec.isBlank()) {
            return null;
        }
        Node root = new Node();
        int[] pos = {0};
        parseList(spec.trim(), pos, root);
        return new GcsFields(root);
    }

    private static void parseList(String s, int[] pos, Node into) {
        while (pos[0] < s.length()) {
            StringBuilder path = new StringBuilder();
            while (pos[0] < s.length() && ",()".indexOf(s.charAt(pos[0])) < 0) {
                path.append(s.charAt(pos[0]++));
            }
            Node cur = into;
            String p = path.toString().trim();
            String[] segs = p.isEmpty() ? new String[0] : p.split("/");
            for (String seg : segs) {
                cur = cur.kids.computeIfAbsent(seg.trim(), k -> new Node());
            }
            if (pos[0] < s.length() && s.charAt(pos[0]) == '(') {
                pos[0]++;
                parseList(s, pos, cur);
                if (pos[0] < s.length() && s.charAt(pos[0]) == ')') {
                    pos[0]++;
                }
            } else {
                cur.all = true;
            }
            if (pos[0] < s.length() && s.charAt(pos[0]) == ',') {
                pos[0]++;
            } else {
                return;
            }
        }
    }

    JsonElement apply(JsonElement e) {
        return filter(e, root);
    }

    private static JsonElement filter(JsonElement e, Node n) {
        if (n.all || e == null || e.isJsonNull() || e.isJsonPrimitive()) {
            return e;
        }
        if (e.isJsonArray()) {
            JsonArray out = new JsonArray();
            for (JsonElement x : e.getAsJsonArray()) {
                out.add(filter(x, n));
            }
            return out;
        }
        JsonObject in = e.getAsJsonObject();
        JsonObject out = new JsonObject();
        Node star = n.kids.get("*");
        for (Map.Entry<String, JsonElement> en : in.entrySet()) {
            Node k = n.kids.get(en.getKey());
            if (k != null) {
                out.add(en.getKey(), filter(en.getValue(), k));
            } else if (star != null) {
                out.add(en.getKey(), en.getValue());
            }
        }
        return out;
    }
}
