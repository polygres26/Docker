package com.sayonora.wire.gremlinwire;

import java.util.HashMap;
import java.util.Map;

/**
 * Argument-shape checks of the traversal steps, so a call the Groovy/Java API would reject ({@code hasLabel()}, {@code values(1)},
 * {@code out(1)}, {@code valueMap(true, true)}) fails with the same status instead of being silently accepted. Patterns are alternatives
 * of space-separated tokens: S string, N number, Z boolean, P predicate, B traversal, T token, C scope, O any value (including null);
 * {@code |} joins alternatives for one position and the suffixes {@code * + ?} repeat the token.
 */
final class Sigs {

    private Sigs() {
    }

    private static final Map<String, String[]> SIGS = new HashMap<>();

    static {
        for (String n : new String[] {"out", "in", "both", "outE", "inE", "bothE", "values", "properties", "elementMap", "propertyMap"}) {
            SIGS.put(n, new String[] {"S*"});
        }
        SIGS.put("valueMap", new String[] {"S*", "Z S*"});
        SIGS.put("hasLabel", new String[] {"S+", "P"});
        SIGS.put("hasId", new String[] {"O+"});
        SIGS.put("hasKey", new String[] {"S+", "P"});
        SIGS.put("hasValue", new String[] {"O+", "P"});
        SIGS.put("has", new String[] {"K", "K O", "S K O"});
        SIGS.put("hasNot", new String[] {"S"});
        SIGS.put("is", new String[] {"O|P"});
        SIGS.put("and", new String[] {"B*"});
        SIGS.put("or", new String[] {"B+"});
        SIGS.put("not", new String[] {"B"});
        SIGS.put("where", new String[] {"B", "P", "S P"});
        SIGS.put("filter", new String[] {"B", "P", "O"});
        SIGS.put("limit", new String[] {"N", "C N"});
        SIGS.put("skip", new String[] {"N", "C N"});
        SIGS.put("range", new String[] {"N N", "C N N"});
        SIGS.put("tail", new String[] {"", "N", "C", "C N"});
        SIGS.put("sample", new String[] {"N", "C N"});
        SIGS.put("coin", new String[] {"N"});
        SIGS.put("as", new String[] {"S S*"});
        SIGS.put("project", new String[] {"S S*"});
        SIGS.put("order", new String[] {"", "C"});
        SIGS.put("dedup", new String[] {"S*", "C S*"});
        for (String n : new String[] {"count", "sum", "max", "min", "mean"}) {
            SIGS.put(n, new String[] {"", "C"});
        }
        SIGS.put("group", new String[] {"", "S"});
        SIGS.put("groupCount", new String[] {"", "S"});
        SIGS.put("aggregate", new String[] {"S", "C S"});
        SIGS.put("cap", new String[] {"S S*"});
        SIGS.put("choose", new String[] {"B", "P B", "B B", "P B B", "B B B"});
        SIGS.put("optional", new String[] {"B"});
        SIGS.put("local", new String[] {"B"});
        SIGS.put("map", new String[] {"B", "O"});
        SIGS.put("flatMap", new String[] {"B", "O"});
        SIGS.put("union", new String[] {"B*"});
        SIGS.put("coalesce", new String[] {"B*"});
        SIGS.put("repeat", new String[] {"B", "S B"});
        SIGS.put("constant", new String[] {"O"});
        SIGS.put("tree", new String[] {"", "S"});
        for (String n : new String[] {"id", "label", "key", "value", "outV", "inV", "bothV", "otherV", "identity", "unfold", "simplePath", "cyclicPath", "drop",
                "index"}) {
            SIGS.put(n, new String[] {""});
        }
        SIGS.put("loops", new String[] {"", "S"});
        SIGS.put("math", new String[] {"S"});
        SIGS.put("addV", new String[] {"", "S", "B"});
        SIGS.put("addE", new String[] {"S", "B"});
        SIGS.put("sideEffect", new String[] {"B", "O"});
    }

    static void check(Engine.Step s) {
        String[] alts = SIGS.get(s.name);
        if (alts == null) {
            return;
        }
        for (String alt : alts) {
            if (matches(alt.isEmpty() ? new String[0] : alt.split(" "), 0, s.args, 0)) {
                break;
            }
            if (alt == alts[alts.length - 1]) {
                throw G.GremlinError.translation("No signature of method: DefaultGraphTraversal." + s.name + "() is applicable for argument types: ("
                        + types(s.args) + ")");
            }
        }
        int by = s.by.size();
        if (s.name.equals("group") && by > 2) {
            throw G.GremlinError.script("The key and value traversals for group()-step have already been set");
        }
        if (s.name.equals("groupCount") && by > 1) {
            throw G.GremlinError.script("The by() modulator of groupCount() has already been set");
        }
    }

    private static String types(Object[] a) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < a.length; i++) {
            sb.append(i > 0 ? ", " : "").append(a[i] == null ? "null" : a[i].getClass().getSimpleName());
        }
        return sb.toString();
    }

    private static boolean matches(String[] pat, int pi, Object[] args, int ai) {
        if (pi == pat.length) {
            return ai == args.length;
        }
        String tok = pat[pi];
        char q = tok.charAt(tok.length() - 1);
        boolean star = q == '*';
        boolean plus = q == '+';
        boolean opt = q == '?';
        String types = star || plus || opt ? tok.substring(0, tok.length() - 1) : tok;
        if (star || plus) {
            int min = plus ? 1 : 0;
            int n = 0;
            while (ai + n < args.length && ok(types, args[ai + n])) {
                n++;
            }
            for (int k = n; k >= min; k--) {
                if (matches(pat, pi + 1, args, ai + k)) {
                    return true;
                }
            }
            return false;
        }
        if (opt) {
            if (ai < args.length && ok(types, args[ai]) && matches(pat, pi + 1, args, ai + 1)) {
                return true;
            }
            return matches(pat, pi + 1, args, ai);
        }
        return ai < args.length && ok(types, args[ai]) && matches(pat, pi + 1, args, ai + 1);
    }

    private static boolean ok(String types, Object o) {
        for (String t : types.split("\\|")) {
            boolean r = switch (t) {
                case "S" -> o instanceof String;
                case "N" -> o instanceof Number;
                case "Z" -> o instanceof Boolean;
                case "P" -> o instanceof G.P;
                case "B" -> o instanceof G.Bytecode;
                case "T" -> o instanceof G.T;
                case "K" -> o == null || o instanceof String || o instanceof G.T;
                case "C" -> o instanceof G.Scope;
                case "O" -> true;
                default -> false;
            };
            if (r) {
                return true;
            }
        }
        return false;
    }
}
