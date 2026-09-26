package com.sayonora.wire.gcswire;

import java.util.regex.Pattern;

/** Object-name matching helpers: {@code matchGlob} (GCS glob syntax) and prefix arithmetic for listings. */
final class GcsMatch {

    private GcsMatch() {
    }

    /**
     * Compiles a GCS glob: {@code *} = any run without '/', {@code **} = any run including '/', {@code ?} = one non-'/'
     * character, {@code [abc]} / {@code [a-z]} / {@code [!a]} character classes, {@code {a,b}} alternation, {@code \x} escape.
     */
    static Pattern glob(String g) {
        StringBuilder re = new StringBuilder();
        int braces = 0;
        for (int i = 0; i < g.length(); i++) {
            char c = g.charAt(i);
            switch (c) {
                case '*' -> {
                    if (i + 1 < g.length() && g.charAt(i + 1) == '*') {
                        re.append(".*");
                        i++;
                        if (i + 1 < g.length() && g.charAt(i + 1) == '/') {
                            // "**/" also matches zero directories
                            re.setLength(re.length() - 2);
                            re.append("(?:.*/)?");
                            i++;
                        }
                    } else {
                        re.append("[^/]*");
                    }
                }
                case '?' -> re.append("[^/]");
                case '[' -> {
                    int j = g.indexOf(']', i + 2);
                    if (j < 0) {
                        re.append("\\[");
                    } else {
                        String body = g.substring(i + 1, j);
                        boolean neg = body.startsWith("!") || body.startsWith("^");
                        if (neg) {
                            body = body.substring(1);
                        }
                        re.append('[').append(neg ? "^" : "").append(body.replace("\\", "\\\\").replace("[", "\\[")).append(']');
                        i = j;
                    }
                }
                case '{' -> {
                    re.append("(?:");
                    braces++;
                }
                case '}' -> {
                    if (braces > 0) {
                        re.append(')');
                        braces--;
                    } else {
                        re.append("\\}");
                    }
                }
                case ',' -> re.append(braces > 0 ? "|" : ",");
                case '\\' -> {
                    if (i + 1 < g.length()) {
                        re.append(Pattern.quote(String.valueOf(g.charAt(++i))));
                    }
                }
                default -> re.append(Pattern.quote(String.valueOf(c)));
            }
        }
        while (braces-- > 0) {
            re.append(')');
        }
        return Pattern.compile(re.toString(), Pattern.DOTALL);
    }

    /** Smallest string greater than every string that starts with {@code prefix} (upper bound of a prefix range), or null. */
    static String prefixEnd(String prefix) {
        if (prefix.isEmpty()) {
            return null;
        }
        return prefix.substring(0, prefix.length() - 1) + (char) (prefix.charAt(prefix.length() - 1) + 1);
    }

    /**
     * The delimiter roll-up of one name: the common prefix it collapses into (prefix + up to and including the first
     * delimiter after it), or null when it stays an object.
     */
    static String rollup(String name, String prefix, String delimiter) {
        if (delimiter == null || delimiter.isEmpty()) {
            return null;
        }
        int i = name.indexOf(delimiter, prefix.length());
        return i < 0 ? null : name.substring(0, i + delimiter.length());
    }
}
