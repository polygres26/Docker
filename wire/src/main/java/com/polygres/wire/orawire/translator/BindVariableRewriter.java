package com.polygres.wire.orawire.translator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class BindVariableRewriter {

    private static final Pattern BIND_TOKEN = Pattern.compile(":([A-Za-z_][A-Za-z0-9_]*|[0-9]+)");

    public record Result(String sql, int[] placeholderToBindIndex) {
    }

    public static Result rewrite(String oracleSql) {
        
        Map<String, Integer> firstSeenIndex = new LinkedHashMap<>();
        List<Integer> placeholderOrder = new ArrayList<>();
        StringBuilder rewritten = new StringBuilder();
        int i = 0;
        int n = oracleSql.length();
        while (i < n) {
            char c = oracleSql.charAt(i);
            if (c == '\'') {
                int start = i;
                i++;
                while (i < n) {
                    if (oracleSql.charAt(i) == '\'') {
                        if (i + 1 < n && oracleSql.charAt(i + 1) == '\'') {
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    i++;
                }
                rewritten.append(oracleSql, start, i);
                continue;
            }
            if (c == ':') {
                Matcher m = BIND_TOKEN.matcher(oracleSql).region(i, n);
                if (m.lookingAt()) {
                    rewritten.append('?');
                    String name = m.group(1);
                    int index = firstSeenIndex.computeIfAbsent(name, k -> firstSeenIndex.size());
                    placeholderOrder.add(index);
                    i = m.end();
                    continue;
                }
            }
            rewritten.append(c);
            i++;
        }

        int[] mapping = new int[placeholderOrder.size()];
        for (int idx = 0; idx < mapping.length; idx++) {
            mapping[idx] = placeholderOrder.get(idx);
        }
        return new Result(rewritten.toString(), mapping);
    }

    private BindVariableRewriter() {
    }
}
