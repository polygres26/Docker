package com.sayonora.wire.azurewire;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Builds synthetic {@link HttpServletRequest}s for blob batch sub-requests and copy-source SAS checks. */
final class SubRequest {

    private SubRequest() {
    }

    static HttpServletRequest create(HttpServletRequest outer, String method, String uri, String queryString,
            Map<String, String> headers, byte[] body, String scheme, String remoteAddr) {
        TreeMap<String, String> h = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        h.putAll(headers);
        return (HttpServletRequest) Proxy.newProxyInstance(HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class}, (proxy, m, args) -> {
                    switch (m.getName()) {
                        case "getMethod":
                            return method;
                        case "getRequestURI":
                            return uri;
                        case "getQueryString":
                            return queryString;
                        case "getHeader":
                            return h.get((String) args[0]);
                        case "getHeaders": {
                            String v = h.get((String) args[0]);
                            return v == null ? Collections.emptyEnumeration() : Collections.enumeration(java.util.List.of(v));
                        }
                        case "getHeaderNames":
                            return Collections.enumeration(h.keySet());
                        case "getScheme":
                            return outer != null ? outer.getScheme() : scheme;
                        case "getRemoteAddr":
                            return outer != null ? outer.getRemoteAddr() : remoteAddr;
                        case "getContentLengthLong":
                            return (long) body.length;
                        case "getContentLength":
                            return body.length;
                        case "getInputStream": {
                            ByteArrayInputStream in = new ByteArrayInputStream(body);
                            return new ServletInputStream() {
                                @Override
                                public boolean isFinished() {
                                    return in.available() == 0;
                                }

                                @Override
                                public boolean isReady() {
                                    return true;
                                }

                                @Override
                                public void setReadListener(ReadListener l) {
                                }

                                @Override
                                public int read() {
                                    return in.read();
                                }

                                @Override
                                public int read(byte[] b, int off, int len) {
                                    return in.read(b, off, len);
                                }
                            };
                        }
                        default:
                            Class<?> rt = m.getReturnType();
                            if (rt == boolean.class) {
                                return false;
                            }
                            if (rt == int.class) {
                                return 0;
                            }
                            if (rt == long.class) {
                                return 0L;
                            }
                            return null;
                    }
                });
    }

    /** A GET on {@code /account/path?query} (path-style) used to run the SAS authenticator over a copy source. */
    static AzReq forUrl(String account, String path, Map<String, String> query, AzureConfig cfg) {
        StringBuilder qs = new StringBuilder();
        for (Map.Entry<String, String> e : query.entrySet()) {
            if (qs.length() > 0) {
                qs.append('&');
            }
            qs.append(java.net.URLEncoder.encode(e.getKey(), java.nio.charset.StandardCharsets.UTF_8)).append('=')
                    .append(java.net.URLEncoder.encode(e.getValue(), java.nio.charset.StandardCharsets.UTF_8));
        }
        HttpServletRequest req = create(null, "GET", "/" + account + path, qs.toString(), new LinkedHashMap<>(), new byte[0],
                "http", "127.0.0.1");
        return new AzReq(req, AzReq.BLOB, cfg, "copy-source");
    }
}
