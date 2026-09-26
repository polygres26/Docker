package com.sayonora.wire.gremlinwire;

import com.sayonora.wire.gremlinwire.Engine.Ctx;
import com.sayonora.wire.gremlinwire.Engine.Step;
import com.sayonora.wire.gremlinwire.Engine.Tr;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** String steps (asString, toLower, toUpper, trim, length, split, replace, substring, concat), global and local scope. */
final class Strings {

    private Strings() {
    }

    static Iterator<Tr> apply(Step s, Iterator<Tr> in, Ctx ctx) {
        Object[] a = s.args;
        boolean local = a.length > 0 && a[0] == G.Scope.local;
        Object[] rest = local ? java.util.Arrays.copyOfRange(a, 1, a.length) : a;
        return Engine.flat(in, t -> {
            Object v = t.v;
            if (v == null) {
                return Engine.once(t.child(null));
            }
            if (local && v instanceof Collection<?> c) {
                List<Object> out = new ArrayList<>();
                for (Object x : c) {
                    out.add(one(s.name, x, rest, ctx, t));
                }
                return Engine.once(t.child(out));
            }
            Object r = one(s.name, v, rest, ctx, t);
            return Engine.once(t.child(r));
        });
    }

    private static Object one(String name, Object v, Object[] a, Ctx ctx, Tr t) {
        if (name.equals("asString")) {
            return v instanceof String ? v : String.valueOf(v);
        }
        if (name.equals("length") && v instanceof Collection<?> c) {
            return c.size();
        }
        if (!(v instanceof String s)) {
            throw G.GremlinError.script("The " + name + "() step can only take string as argument, encountered " + v.getClass().getName());
        }
        return switch (name) {
            case "toLower" -> s.toLowerCase();
            case "toUpper" -> s.toUpperCase();
            case "trim" -> s.strip();
            case "lTrim" -> s.stripLeading();
            case "rTrim" -> s.stripTrailing();
            case "length" -> s.length();
            case "reverse" -> new StringBuilder(s).reverse().toString();
            case "split" -> {
                String d = (String) a[0];
                List<Object> parts = new ArrayList<>();
                if (d.isEmpty()) {
                    for (char ch : s.toCharArray()) {
                        parts.add(String.valueOf(ch));
                    }
                } else {
                    for (String p : s.split(java.util.regex.Pattern.quote(d), -1)) {
                        parts.add(p);
                    }
                }
                yield parts;
            }
            case "replace" -> s.replace((String) a[0], (String) a[1]);
            case "substring" -> {
                int b = ((Number) a[0]).intValue();
                int e = a.length > 1 ? ((Number) a[1]).intValue() : s.length();
                b = Math.max(0, Math.min(b, s.length()));
                e = e < 0 ? s.length() : Math.min(e, s.length());
                yield b >= e ? "" : s.substring(b, e);
            }
            case "concat" -> {
                StringBuilder sb = new StringBuilder(s);
                for (Object x : a) {
                    sb.append(x);
                }
                yield sb.toString();
            }
            default -> throw G.GremlinError.script("unsupported string step " + name);
        };
    }
}
