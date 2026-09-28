package com.sayonora.warp.gcswire;

import jakarta.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** One parsed GCS request (from a servlet request, or one part of a batch). */
final class GcsReq {

    final String method;
    /** the URL path exactly as sent (percent-encoded) */
    final String rawPath;
    final String rawQuery;
    final Map<String, List<String>> query = new LinkedHashMap<>();
    /** header name (lower-case) -> values */
    final Map<String, List<String>> headers = new LinkedHashMap<>();
    final String host;
    final String scheme;
    final InputStream body;
    final long startNanos = System.nanoTime();
    /** metrics label, set by the service */
    String op = "Unknown";
    boolean write;
    /** the authenticated principal label ("anonymous", "bearer", "hmac:<accessId>", "signed-url") */
    String principal = "anonymous";

    GcsReq(String method, String rawPath, String rawQuery, Map<String, List<String>> headers, String host, String scheme, InputStream body) {
        this.method = method;
        this.rawPath = rawPath == null || rawPath.isEmpty() ? "/" : rawPath;
        this.rawQuery = rawQuery;
        this.host = host;
        this.scheme = scheme;
        this.body = body;
        headers.forEach((k, v) -> this.headers.put(k.toLowerCase(Locale.ROOT), v));
        if (rawQuery != null && !rawQuery.isEmpty()) {
            for (String pair : rawQuery.split("&")) {
                if (pair.isEmpty()) {
                    continue;
                }
                int eq = pair.indexOf('=');
                query.computeIfAbsent(decode(eq < 0 ? pair : pair.substring(0, eq), true), k -> new ArrayList<>())
                        .add(eq < 0 ? "" : decode(pair.substring(eq + 1), true));
            }
        }
    }

    static GcsReq of(HttpServletRequest r) throws IOException {
        Map<String, List<String>> h = new LinkedHashMap<>();
        for (String n : Collections.list(r.getHeaderNames())) {
            h.put(n, Collections.list(r.getHeaders(n)));
        }
        return new GcsReq(r.getMethod(), r.getRequestURI(), r.getQueryString(), h, r.getHeader("Host"), r.getScheme(), r.getInputStream());
    }

    static GcsReq sub(String method, String target, Map<String, List<String>> headers, byte[] body, GcsReq parent) {
        int q = target.indexOf('?');
        String path = q < 0 ? target : target.substring(0, q);
        String qs = q < 0 ? null : target.substring(q + 1);
        return new GcsReq(method, path, qs, headers, parent.host, parent.scheme, new ByteArrayInputStream(body));
    }

    String header(String name) {
        List<String> v = headers.get(name.toLowerCase(Locale.ROOT));
        return v == null || v.isEmpty() ? null : String.join(", ", v);
    }

    String q(String name) {
        List<String> v = query.get(name);
        return v == null || v.isEmpty() ? null : v.get(0);
    }

    boolean has(String name) {
        return query.containsKey(name);
    }

    boolean qBool(String name) {
        return "true".equalsIgnoreCase(q(name));
    }

    /** {@code scheme://host}, the base of every link and Location header this request generates. */
    String baseUrl() {
        return (scheme == null ? "http" : scheme) + "://" + (host == null ? "localhost" : host);
    }

    long contentLength() {
        String v = header("content-length");
        if (v == null) {
            return -1;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** the raw path split on '/' with each segment percent-decoded (empty leading segment dropped) */
    List<String> segments() {
        List<String> out = new ArrayList<>();
        String p = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
        if (p.isEmpty()) {
            return out;
        }
        for (String s : p.split("/", -1)) {
            out.add(decode(s, false));
        }
        return out;
    }

    byte[] readBody(long max) throws IOException {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        byte[] b = new byte[8192];
        int n;
        while ((n = body.read(b)) >= 0) {
            o.write(b, 0, n);
            if (o.size() > max) {
                throw new GcsException(413, "uploadTooLarge", "EntityTooLarge", "The request body is too large.");
            }
        }
        return o.toByteArray();
    }

    static String decode(String s, boolean plusIsSpace) {
        if (s.indexOf('%') < 0 && (!plusIsSpace || s.indexOf('+') < 0)) {
            return s;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length() + 0 && hex(s.charAt(i + 1)) && hex(s.charAt(i + 2))) {
                out.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 2;
            } else if (c == '+' && plusIsSpace) {
                out.write(' ');
            } else if (Character.isHighSurrogate(c) && i + 1 < s.length()) {
                out.writeBytes(s.substring(i, i + 2).getBytes(StandardCharsets.UTF_8));
                i++;
            } else {
                out.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private static boolean hex(char c) {
        return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F';
    }

    /** RFC 3986 percent-encoding of one path segment (object names in links). */
    static String enc(String s) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if (c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~') {
                sb.append((char) c);
            } else {
                sb.append('%').append(String.format("%02X", c));
            }
        }
        return sb.toString();
    }
}
