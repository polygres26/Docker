package com.sayonora.wire.amqpwire;

import java.util.List;
import java.util.Map;

/** Exchange routing rules: direct, fanout, topic (* and #) and headers (x-match all / any / all-with-x / any-with-x). */
final class AmqpRouting {

    private AmqpRouting() {
    }

    static boolean matches(String type, AmqpStore.BindingDef b, String rk, Map<String, Object> headers) {
        return switch (type) {
            case "fanout" -> true;
            case "direct" -> b.rkey().equals(rk);
            case "topic" -> topic(b.rkey(), rk);
            case "headers" -> headers(b.args(), headers);
            default -> b.rkey().equals(rk);
        };
    }

    static String[] words(String s) {
        return s.isEmpty() ? new String[0] : s.split("\\.", -1);
    }

    /** RabbitMQ topic matching: '*' is exactly one word, '#' zero or more words; the empty key has no words. */
    static boolean topic(String pattern, String key) {
        return match(words(pattern), 0, words(key), 0);
    }

    private static boolean match(String[] p, int pi, String[] k, int ki) {
        while (pi < p.length) {
            String w = p[pi];
            if (w.equals("#")) {
                if (pi == p.length - 1) {
                    return true;
                }
                for (int j = ki; j <= k.length; j++) {
                    if (match(p, pi + 1, k, j)) {
                        return true;
                    }
                }
                return false;
            }
            if (ki >= k.length) {
                return false;
            }
            if (!w.equals("*") && !w.equals(k[ki])) {
                return false;
            }
            pi++;
            ki++;
        }
        return ki == k.length;
    }

    static boolean headers(Map<String, Object> bindArgs, Map<String, Object> msgHeaders) {
        String mode = "all";
        Object xm = bindArgs.get("x-match");
        if (xm instanceof String s) {
            mode = s;
        }
        boolean any = mode.startsWith("any");
        boolean withX = mode.endsWith("-with-x");
        boolean sawAny = false;
        for (Map.Entry<String, Object> e : bindArgs.entrySet()) {
            String k = e.getKey();
            if (k.equals("x-match") || (!withX && k.startsWith("x-"))) {
                continue;
            }
            boolean present = msgHeaders != null && msgHeaders.containsKey(k);
            boolean eq = present && (e.getValue() == null || AmqpCodec.canonical(e.getValue()).equals(AmqpCodec.canonical(msgHeaders.get(k))));
            if (any) {
                if (eq) {
                    sawAny = true;
                }
            } else if (!eq) {
                return false;
            }
        }
        if (any) {
            return sawAny;
        }
        return true;
    }

    static boolean validType(String t) {
        return List.of("direct", "fanout", "topic", "headers").contains(t);
    }
}
