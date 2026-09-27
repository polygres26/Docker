package com.sayonora.warp.mcp;

import com.google.gson.JsonObject;
import com.sayonora.warp.tls.TlsListeners;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * How a user should reach the MCP listener: the configured public URL ({@code WARP_MCP_PUBLIC_URL}, e.g. the
 * https address of a tunnel or reverse proxy), else the native HTTPS listener, else plaintext. Used by the admin API for
 * {@code GET /api/mcp-config} and for the URLs it returns when an endpoint is created.
 *
 * <p>Behind a TLS-terminating proxy the admin request may carry {@code X-Forwarded-Proto/Host}; those are honoured
 * here, for building display URLs only (they never influence authentication or the connection gate).
 */
public final class McpConnectionInfo {

    private McpConnectionInfo() {
    }

    /** {@code WARP_MCP_PUBLIC_URL} without a trailing slash, or null. */
    public static String configuredPublicUrl(Map<String, String> env) {
        String v = env.get("WARP_MCP_PUBLIC_URL");
        if (v == null || v.isBlank()) {
            return null;
        }
        String t = v.trim();
        while (t.endsWith("/")) {
            t = t.substring(0, t.length() - 1);
        }
        return t;
    }

    /** Base URL (scheme://host[:port], no path) a client should use, best option first. */
    public static String bestBase(HttpServletRequest req, Map<String, String> env, TlsListeners.Info info, int httpPort) {
        String pub = configuredPublicUrl(env);
        if (pub != null) {
            return pub;
        }
        String fwdHost = firstToken(req == null ? null : req.getHeader("X-Forwarded-Host"));
        String fwdProto = firstToken(req == null ? null : req.getHeader("X-Forwarded-Proto"));
        if (fwdHost != null) {
            return (fwdProto == null ? "https" : fwdProto) + "://" + fwdHost;
        }
        String host = hostOnly(req);
        if (info != null && info.httpsPort() > 0) {
            return "https://" + host + ":" + info.httpsPort();
        }
        int hp = info != null && info.httpPort() > 0 ? info.httpPort() : httpPort;
        return "http://" + host + ":" + hp;
    }

    public static JsonObject toJson(HttpServletRequest req, Map<String, String> env, int defaultHttpPort) {
        TlsListeners.Info info = TlsListeners.info("MCP");
        String pub = configuredPublicUrl(env);
        JsonObject o = new JsonObject();
        o.addProperty("publicUrl", pub);
        boolean tls = info != null && info.tlsEnabled();
        o.addProperty("tlsEnabled", tls);
        o.addProperty("selfSigned", info != null && info.selfSigned());
        o.addProperty("httpsPort", tls ? info.httpsPort() : null);
        Integer http = info == null ? Integer.valueOf(defaultHttpPort) : (info.httpPort() > 0 ? info.httpPort() : null);
        o.addProperty("httpPort", http);
        o.addProperty("tlsError", info == null ? null : info.tlsError());
        o.addProperty("certSubject", info == null ? null : info.certSubject());
        o.addProperty("certNotAfter", info == null ? null : info.certNotAfter());
        String base = bestBase(req, env, info, defaultHttpPort);
        o.addProperty("baseUrl", base);
        // Claude connectors need https:// AND a certificate that a public CA signed; self-signed never qualifies.
        boolean httpsBase = base.startsWith("https://");
        o.addProperty("https", httpsBase);
        o.addProperty("claudeConnectorReady", httpsBase && !(pub == null && info != null && info.selfSigned()));
        return o;
    }

    private static String hostOnly(HttpServletRequest req) {
        String h = req == null ? null : req.getHeader("Host");
        if (h == null || h.isBlank()) {
            return "localhost";
        }
        if (h.startsWith("[")) {
            int end = h.indexOf(']');
            return end > 0 ? h.substring(0, end + 1) : h;
        }
        int colon = h.lastIndexOf(':');
        return colon > 0 ? h.substring(0, colon) : h;
    }

    private static String firstToken(String v) {
        if (v == null || v.isBlank()) {
            return null;
        }
        String t = v.split(",")[0].trim();
        return t.isEmpty() ? null : t;
    }
}
