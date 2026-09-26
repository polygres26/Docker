package com.sayonora.wire.azurewire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Blob index tags: validation and the tag filter grammar of Find Blobs by Tags / {@code x-ms-if-tags}: a conjunction
 * ({@code AND}) of {@code "tagname" <op> 'value'} terms with {@code = > >= < <=}, plus {@code @container = 'name'}.
 * Comparisons are ordinal string comparisons, as in Azure.
 */
final class BlobTags {

    private BlobTags() {
    }

    private static final Pattern TAG_CHARS = Pattern.compile("^[a-zA-Z0-9 +\\-./:=_]*$");

    static void validate(Map<String, String> tags) {
        if (tags.size() > 10) {
            throw new AzureException(400, "TagsTooLarge", "The tags specified are invalid. Number of tags is too large.");
        }
        for (Map.Entry<String, String> e : tags.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            if (k.isEmpty() || k.length() > 128 || v.length() > 256 || !TAG_CHARS.matcher(k).matches()
                    || !TAG_CHARS.matcher(v).matches()) {
                throw new AzureException(400, "InvalidTag", "The tags specified are invalid. It contains characters that are "
                        + "not permitted.");
            }
        }
    }

    private record Term(String name, String op, String value) {
    }

    /** @throws IllegalArgumentException for a malformed expression */
    static boolean matches(String expr, Map<String, String> tags) {
        for (Term t : parse(expr)) {
            String actual = tags.get(t.name);
            if (actual == null) {
                return false;
            }
            int c = actual.compareTo(t.value);
            boolean ok = switch (t.op) {
                case "=" -> c == 0;
                case ">" -> c > 0;
                case ">=" -> c >= 0;
                case "<" -> c < 0;
                case "<=" -> c <= 0;
                default -> false;
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    static List<Term> parse(String expr) {
        List<Term> out = new ArrayList<>();
        int i = 0;
        int n = expr.length();
        while (true) {
            i = skip(expr, i);
            String name;
            if (i < n && expr.charAt(i) == '"') {
                int end = expr.indexOf('"', i + 1);
                while (end >= 0 && end + 1 < n && expr.charAt(end + 1) == '"') {
                    end = expr.indexOf('"', end + 2);
                }
                if (end < 0) {
                    throw new IllegalArgumentException("unterminated tag name");
                }
                name = expr.substring(i + 1, end).replace("\"\"", "\"");
                i = end + 1;
            } else if (i < n && expr.charAt(i) == '@') {
                int j = i + 1;
                while (j < n && Character.isLetter(expr.charAt(j))) {
                    j++;
                }
                name = expr.substring(i, j);
                if (!name.equals("@container")) {
                    throw new IllegalArgumentException("unknown system property " + name);
                }
                i = j;
            } else {
                throw new IllegalArgumentException("expected a quoted tag name at position " + i);
            }
            i = skip(expr, i);
            String op;
            if (expr.startsWith(">=", i) || expr.startsWith("<=", i)) {
                op = expr.substring(i, i + 2);
                i += 2;
            } else if (i < n && "=<>".indexOf(expr.charAt(i)) >= 0) {
                op = expr.substring(i, i + 1);
                i++;
            } else {
                throw new IllegalArgumentException("expected a comparison operator at position " + i);
            }
            i = skip(expr, i);
            if (i >= n || expr.charAt(i) != '\'') {
                throw new IllegalArgumentException("expected a quoted value at position " + i);
            }
            StringBuilder v = new StringBuilder();
            i++;
            while (true) {
                if (i >= n) {
                    throw new IllegalArgumentException("unterminated value");
                }
                char c = expr.charAt(i);
                if (c == '\'') {
                    if (i + 1 < n && expr.charAt(i + 1) == '\'') {
                        v.append('\'');
                        i += 2;
                        continue;
                    }
                    i++;
                    break;
                }
                v.append(c);
                i++;
            }
            out.add(new Term(name, op, v.toString()));
            i = skip(expr, i);
            if (i >= n) {
                return out;
            }
            if (expr.regionMatches(true, i, "AND", 0, 3)) {
                i += 3;
            } else {
                throw new IllegalArgumentException("expected AND at position " + i);
            }
        }
    }

    private static int skip(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }
}
