package com.sayonora.wire.azurewire;

import jakarta.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One parsed Azure Storage request: service, account (path-style {@code /account/...} or host-style
 * {@code account.blob.domain}), the resource path below the account, query parameters (decoded) and headers.
 */
public final class AzReq {

    public static final String BLOB = "blob";
    public static final String QUEUE = "queue";
    public static final String TABLE = "table";

    public final HttpServletRequest raw;
    public final String service;
    public final String method;
    public final String account;
    public final boolean hostStyle;
    /** raw (still percent-encoded) URL path as sent, e.g. {@code /devstoreaccount1/c/b%20x} */
    public final String rawPath;
    /** decoded path below the account, always starting with '/', or "" for the account root */
    public final String path;
    public final Map<String, List<String>> query = new LinkedHashMap<>();
    /** query parameters in wire order (name, value) as decoded */
    public final List<String[]> queryList = new ArrayList<>();
    public final String requestId;
    public final long startNanos = System.nanoTime();
    /** operation label for metrics; set by the service */
    public String op = "Unknown";
    public boolean write;

    public AzReq(HttpServletRequest raw, String service, AzureConfig cfg, String requestId) {
        this.raw = raw;
        this.service = service;
        this.method = raw.getMethod();
        this.requestId = requestId;
        this.rawPath = raw.getRequestURI() == null ? "/" : raw.getRequestURI();
        String host = raw.getHeader("Host");
        String hostAccount = hostAccount(host, service, cfg.domain());
        String below;
        if (hostAccount != null) {
            this.hostStyle = true;
            this.account = hostAccount;
            below = rawPath;
        } else {
            this.hostStyle = false;
            String p = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
            int slash = p.indexOf('/');
            this.account = decode(slash < 0 ? p : p.substring(0, slash), false);
            below = slash < 0 ? "" : p.substring(slash);
        }
        this.path = "/".equals(below) ? "" : decode(below, false);
        String qs = raw.getQueryString();
        if (qs != null && !qs.isEmpty()) {
            for (String pair : qs.split("&")) {
                if (pair.isEmpty()) {
                    continue;
                }
                int eq = pair.indexOf('=');
                String k = decode(eq < 0 ? pair : pair.substring(0, eq), true);
                String v = eq < 0 ? "" : decode(pair.substring(eq + 1), true);
                query.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
                queryList.add(new String[] {k, v});
            }
        }
    }

    private static String hostAccount(String host, String service, String domain) {
        if (host == null) {
            return null;
        }
        String h = host.toLowerCase(Locale.ROOT);
        int colon = h.lastIndexOf(':');
        if (colon > 0 && h.indexOf(']') < colon) {
            h = h.substring(0, colon);
        }
        String suffix = "." + service + "." + domain;
        if (h.endsWith(suffix) && h.length() > suffix.length()) {
            String acct = h.substring(0, h.length() - suffix.length());
            if (acct.indexOf('.') < 0) {
                return acct;
            }
        }
        return null;
    }

    public String header(String name) {
        return raw.getHeader(name);
    }

    public String q(String name) {
        List<String> v = query.get(name);
        return v == null || v.isEmpty() ? null : v.get(0);
    }

    /** case-insensitive query lookup */
    public String qi(String name) {
        for (Map.Entry<String, List<String>> e : query.entrySet()) {
            if (e.getKey().equalsIgnoreCase(name)) {
                return e.getValue().isEmpty() ? null : e.getValue().get(0);
            }
        }
        return null;
    }

    public boolean has(String name) {
        return query.containsKey(name);
    }

    /** first path segment below the account (container / queue / table), or null */
    public String first() {
        if (path.length() <= 1) {
            return null;
        }
        int s = path.indexOf('/', 1);
        return s < 0 ? path.substring(1) : path.substring(1, s);
    }

    /** the remainder after the first segment (blob name, may contain '/'), or null when there is none */
    public String rest() {
        if (path.length() <= 1) {
            return null;
        }
        int s = path.indexOf('/', 1);
        return s < 0 ? null : path.substring(s + 1);
    }

    public long contentLength() {
        String v = raw.getHeader("Content-Length");
        if (v == null) {
            return raw.getContentLengthLong();
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    public static String decode(String s, boolean plusIsSpace) {
        if (s.indexOf('%') < 0 && (!plusIsSpace || s.indexOf('+') < 0)) {
            return s;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length() + 0 && isHex(s.charAt(i + 1)) && isHex(s.charAt(i + 2))) {
                out.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 2;
            } else if (c == '+' && plusIsSpace) {
                out.write(' ');
            } else {
                byte[] b = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                if (Character.isHighSurrogate(c) && i + 1 < s.length()) {
                    b = s.substring(i, i + 2).getBytes(StandardCharsets.UTF_8);
                    i++;
                }
                out.writeBytes(b);
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private static boolean isHex(char c) {
        return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F';
    }
}
