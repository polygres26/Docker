package com.sayonora.warp.core;

import java.util.ArrayList;
import java.util.List;

/** Minimal libpq connection-string handling: enough to repoint {@code primary_conninfo} at a new host and
 * port while keeping everything else (user, password, sslmode, application_name...) exactly as it was. */
final class PgConninfo {

    private PgConninfo() {
    }

    /** {@code key=value} pairs in order; values may be single-quoted with {@code \\'} and {@code \\\\} escapes. */
    static List<String[]> parse(String conninfo) {
        List<String[]> out = new ArrayList<>();
        int n = conninfo.length();
        int i = 0;
        while (i < n) {
            while (i < n && Character.isWhitespace(conninfo.charAt(i))) {
                i++;
            }
            if (i >= n) {
                break;
            }
            int eq = conninfo.indexOf('=', i);
            if (eq < 0) {
                throw new IllegalArgumentException("malformed conninfo (no '=' after \"" + conninfo.substring(i) + "\")");
            }
            String key = conninfo.substring(i, eq).trim();
            i = eq + 1;
            while (i < n && Character.isWhitespace(conninfo.charAt(i))) {
                i++;
            }
            StringBuilder value = new StringBuilder();
            if (i < n && conninfo.charAt(i) == '\'') {
                i++;
                boolean closed = false;
                while (i < n) {
                    char c = conninfo.charAt(i);
                    if (c == '\\' && i + 1 < n) {
                        value.append(conninfo.charAt(i + 1));
                        i += 2;
                    } else if (c == '\'') {
                        closed = true;
                        i++;
                        break;
                    } else {
                        value.append(c);
                        i++;
                    }
                }
                if (!closed) {
                    throw new IllegalArgumentException("malformed conninfo (unterminated quote)");
                }
            } else {
                while (i < n && !Character.isWhitespace(conninfo.charAt(i))) {
                    char c = conninfo.charAt(i);
                    if (c == '\\' && i + 1 < n) {
                        value.append(conninfo.charAt(i + 1));
                        i += 2;
                    } else {
                        value.append(c);
                        i++;
                    }
                }
            }
            out.add(new String[] {key, value.toString()});
        }
        return out;
    }

    static String quote(String value) {
        if (!value.isEmpty() && value.chars().noneMatch(c -> Character.isWhitespace(c) || c == '\'' || c == '\\')) {
            return value;
        }
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    /** {@code conninfo} with host and port replaced; {@code hostaddr} (which would override host) is dropped. */
    static String withHostPort(String conninfo, String host, int port) {
        List<String[]> kv = parse(conninfo);
        StringBuilder sb = new StringBuilder();
        boolean hasHost = false;
        boolean hasPort = false;
        for (String[] e : kv) {
            String key = e[0];
            String value = e[1];
            if (key.equals("hostaddr")) {
                continue;
            }
            if (key.equals("host")) {
                value = host;
                hasHost = true;
            } else if (key.equals("port")) {
                value = String.valueOf(port);
                hasPort = true;
            }
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(key).append('=').append(quote(value));
        }
        if (!hasHost) {
            sb.append(sb.length() > 0 ? " " : "").append("host=").append(quote(host));
        }
        if (!hasPort) {
            sb.append(' ').append("port=").append(port);
        }
        return sb.toString();
    }
}
