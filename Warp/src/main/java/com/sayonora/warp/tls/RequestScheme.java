package com.sayonora.warp.tls;

import jakarta.servlet.http.HttpServletRequest;

/**
 * The scheme to put in absolute URLs (S3 POST-object Location, SQS queue URLs, Cosmos/Azure self-links) built for a
 * request: {@code https} when the request arrived on an HTTPS connector or a reverse proxy in front says so through
 * {@code X-Forwarded-Proto}. For display URLs only, never for authentication or authorization decisions.
 */
public final class RequestScheme {

    private RequestScheme() {
    }

    public static String of(HttpServletRequest req) {
        return pick(req.getScheme(), req.getHeader("X-Forwarded-Proto"));
    }

    static String pick(String scheme, String forwardedProto) {
        if (forwardedProto != null) {
            String p = forwardedProto.split(",")[0].trim().toLowerCase(java.util.Locale.ROOT);
            if (p.equals("https") || p.equals("http")) {
                return p;
            }
        }
        return scheme == null || scheme.isBlank() ? "http" : scheme.toLowerCase(java.util.Locale.ROOT);
    }
}
