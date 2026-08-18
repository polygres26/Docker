package com.polygres.wire.orawire.translator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites Oracle bind placeholders (":1", ":name") into JDBC "?" markers.
 *
 * The wire protocol carries bind VALUES purely positionally, in the order
 * the client's own SQL parse first encountered each distinct bind token
 * (see ExecuteRequestReader's javadoc) — no bind name is ever transmitted.
 * This rewriter reproduces that same "first occurrence" ordering locally by
 * scanning the SQL text, so wire bind index N always lines up with the
 * value that should be substituted for the Nth distinct token encountered.
 * A token repeated later in the SQL (e.g. ":x" used twice) reuses that same
 * value rather than consuming a new one, matching Oracle bind semantics
 * (JDBC has no equivalent "named" placeholder, so each repeated occurrence
 * becomes its own "?" bound to the same value).
 */
public final class BindVariableRewriter {

    private static final Pattern BIND_TOKEN = Pattern.compile(":([A-Za-z_][A-Za-z0-9_]*|[0-9]+)");

    public record Result(String sql, int[] placeholderToBindIndex) {
    }

    public static Result rewrite(String oracleSql) {
        // RESOLVED: a colon inside a single-quoted string literal (a time/timestamp literal
        // like '10:30:00', a PL/SQL label, any application string containing a colon) used to
        // match BIND_TOKEN just as readily as a real bind placeholder, inflating the reported
        // distinct-bind count beyond what the client actually sent values for — surfaced live
        // as "SQL references more distinct bind variables than the client sent values for"
        // (RequestLoop.orderedBindValues) the first time a real client's own SQL (not this
        // project's own narrower hand-written test queries) happened to contain one. Fixed by
        // only matching ":" tokens outside single-quoted regions — scanning for the next
        // unescaped/unclosed quote boundary rather than running BIND_TOKEN over the whole
        // string blindly. Oracle strings escape an embedded quote by doubling it ('') rather
        // than backslash-escaping, so quote-toggling naively at every `'` is wrong; this walks
        // the string once, treating a `''` pair inside a literal as a single escaped quote
        // rather than a close+reopen.
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
                            i += 2; // escaped quote within the literal
                            continue;
                        }
                        i++; // closing quote
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
