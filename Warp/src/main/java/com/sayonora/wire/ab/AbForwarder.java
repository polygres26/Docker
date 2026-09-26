package com.sayonora.wire.ab;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The HTTP-layer request forwarder: takes the client's request (already authenticated by Warp), re-signs it for the
 * cloud endpoint with the target's credentials and streams the cloud's answer back untouched (status, headers, body,
 * error XML/JSON). Request bodies are streamed, never buffered, unless the caller supplies a byte array (JSON
 * protocols, compare and dual-write). aws-chunked uploads are decoded (their chunk signatures belong to the client's
 * key, not ours) and sent with {@code UNSIGNED-PAYLOAD}.
 */
public final class AbForwarder {

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
            .version(HttpClient.Version.HTTP_1_1).followRedirects(HttpClient.Redirect.NEVER).build();
    private static final Pattern HEX64 = Pattern.compile("^[0-9a-fA-F]{64}$");
    private static final Set<String> DROP_REQ = Set.of("host", "connection", "content-length", "expect", "upgrade",
            "transfer-encoding", "authorization", "x-amz-date", "x-amz-security-token", "x-amz-content-sha256",
            "x-amz-decoded-content-length", "x-amz-trailer", "x-amz-sdk-checksum-algorithm", "accept-encoding", "keep-alive",
            "te", "proxy-authorization", "proxy-connection", "cookie", "origin");
    private static final Set<String> AUTH_QUERY = Set.of("X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires",
            "X-Amz-SignedHeaders", "X-Amz-Signature", "X-Amz-Security-Token");
    static final Set<String> DROP_RESP = Set.of("transfer-encoding", "connection", "keep-alive", "content-length", "date",
            "server", "upgrade", "trailer");

    private AbForwarder() {
    }

    /** A snapshot of the client request that stays valid after the servlet request has been recycled. */
    public static final class OutReq {
        public String method;
        public String rawPath;
        public String rawQuery;
        public Map<String, String> headers = new LinkedHashMap<>();
        /** Buffered body, or null when {@link #stream} is used / there is none. */
        public byte[] body;
        public InputStream stream;
        /** -1 unknown. For {@link #stream}: the length of what the stream yields (after decoding). */
        public long streamLength = -1;
        /** True for S3: header {@code x-amz-content-sha256} is sent, path is single-encoded. */
        public boolean s3;
        /** Original x-amz-content-sha256 the client sent (valid for the raw bytes only). */
        public String clientPayloadHash;
        public boolean bodyDecoded;
    }

    public static final class CloudResponse {
        public int status;
        public Map<String, java.util.List<String>> headers;
        public InputStream body;
        public long contentLength = -1;
    }

    /** Sends {@code req} to the cloud; the caller MUST close {@code body}. */
    public static CloudResponse send(AbTarget target, String service, OutReq req, String roleOverride)
            throws IOException, AbAuthProvider.AuthException, InterruptedException {
        AbAuthProvider.CloudCredential cc = target.provider().resolve(roleOverride);
        if (!(cc instanceof AbAuthProvider.AwsCreds creds)) {
            throw new AbAuthProvider.AuthException("auth provider '" + target.provider().type() + "' does not produce AWS credentials");
        }
        String base = target.endpointFor(service);
        URI baseUri = URI.create(base.endsWith("/") ? base.substring(0, base.length() - 1) : base);
        String path = AbSigV4.enc(AbSigV4.decode(req.rawPath == null || req.rawPath.isEmpty() ? "/" : req.rawPath, false), true);
        if (path.isEmpty()) {
            path = "/";
        }
        String query = AbSigV4.canonicalQuery(req.rawQuery, AUTH_QUERY);
        String host = baseUri.getPort() > 0 ? baseUri.getHost() + ":" + baseUri.getPort() : baseUri.getHost();

        Map<String, String> sign = new LinkedHashMap<>();
        sign.put("host", host);
        for (var e : req.headers.entrySet()) {
            String k = e.getKey().toLowerCase(Locale.ROOT);
            if (DROP_REQ.contains(k)) {
                continue;
            }
            String v = e.getValue();
            if (k.equals("content-encoding")) {
                v = stripAwsChunked(v);
                if (v == null) {
                    continue;
                }
            }
            sign.put(k, v);
        }
        String payloadHash;
        HttpRequest.BodyPublisher pub;
        if (req.body != null) {
            payloadHash = AbSigV4.hex(AbSigV4.sha256(req.body));
            pub = req.body.length == 0 ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(req.body);
        } else if (req.stream != null) {
            String ch = req.clientPayloadHash;
            payloadHash = !req.bodyDecoded && ch != null && HEX64.matcher(ch).matches() ? ch.toLowerCase(Locale.ROOT) : "UNSIGNED-PAYLOAD";
            final InputStream in = req.stream;
            pub = req.streamLength == 0 ? HttpRequest.BodyPublishers.noBody()
                    : req.streamLength > 0
                            ? HttpRequest.BodyPublishers.fromPublisher(HttpRequest.BodyPublishers.ofInputStream(() -> in), req.streamLength)
                            : HttpRequest.BodyPublishers.ofInputStream(() -> in);
        } else {
            payloadHash = AbSigV4.hex(AbSigV4.sha256(new byte[0]));
            pub = HttpRequest.BodyPublishers.noBody();
        }
        Map<String, String> signed = AbSigV4.sign(req.method, path, query, sign, payloadHash, creds, target.region, service,
                java.time.Instant.now(), req.s3);
        String url = baseUri.getScheme() + "://" + host + path + (query.isEmpty() ? "" : "?" + query);
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(target.timeoutSeconds))
                .method(req.method, pub);
        for (var e : signed.entrySet()) {
            if (!e.getKey().equals("host")) {
                rb.header(e.getKey(), e.getValue());
            }
        }
        HttpResponse<InputStream> r = HTTP.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());
        CloudResponse out = new CloudResponse();
        out.status = r.statusCode();
        out.headers = r.headers().map();
        out.body = r.body();
        out.contentLength = r.headers().firstValueAsLong("content-length").orElse(-1);
        return out;
    }

    private static String stripAwsChunked(String v) {
        StringBuilder sb = new StringBuilder();
        for (String p : v.split(",")) {
            if (!p.trim().equalsIgnoreCase("aws-chunked") && !p.isBlank()) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(p.trim());
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
