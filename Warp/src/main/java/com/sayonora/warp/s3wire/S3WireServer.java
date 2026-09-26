package com.sayonora.warp.s3wire;

import com.sayonora.warp.acl.ConnectionGate;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.SqlMetricsCollector;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * s3wire -- Amazon S3 REST API frontend. A stock S3 client (boto3, AWS SDKs, aws-cli) points its
 * endpoint at Warp, signs with SigV4 against the credentials in {@link S3WireConfig}, and Warp
 * stores/serves the objects through an {@link ObjectStore}:
 * <ul>
 *   <li><b>Postgres mode</b> ({@link PostgresObjectStore}): the {@code s3} store is enabled on one or more
 *       Postgres backends of the frontend's backend set (see {@code StoreType.S3}); objects are chunked into
 *       those databases and sharded by key across them. Wins over proxy mode when both are configured.</li>
 *   <li><b>Proxy mode</b> ({@link ProxyObjectStore}, {@code WARP_S3WIRE_BACKEND_BUCKET}): ONE real
 *       S3-compatible backend bucket (MinIO in tests) through the AWS SDK v2 using Warp's own credentials.</li>
 * </ul>
 * The mode is chosen per request from the live backend registry, so enabling/disabling the store hot-reloads.
 *
 * <p><b>Proxy-mode bucket model:</b> client-visible buckets are key prefixes inside the single backend bucket:
 * bucket {@code b}, key {@code k} lives at backend key {@code b/k}. CreateBucket writes a reserved
 * zero-byte marker {@code b/.s3wire-bucket} (hidden from listings and rejected as a client key).
 * ListBuckets returns the distinct first path segments in the backend bucket. Objects written into
 * the backend bucket directly without a {@code bucket/} prefix are not visible through s3wire.
 *
 * <p><b>Postgres mode</b> serves the whole surface documented in {@link S3Api}: versioning, tagging, ACL / public access /
 * policy / CORS / lifecycle / ... configuration documents, checksums, multipart incl. ListParts and UploadPartCopy,
 * presigned and POST-policy uploads, SelectObjectContent, annotations, virtual-hosted-style addressing.
 * <b>Proxy mode</b> keeps the original surface: ListBuckets, CreateBucket, HeadBucket, DeleteBucket, GetBucketLocation,
 * PutObject, GetObject (Range, If-Match, If-None-Match), HeadObject, DeleteObject(s), CopyObject (single request),
 * ListObjects v1/v2, multipart (Create/UploadPart/Complete/Abort), presigned GET/PUT. Everything else answers
 * 501 NotImplemented there (bucket subresources have no equivalent in a shared backend bucket).
 *
 * <p>Every operation is recorded in {@link SqlMetricsCollector} with protocol {@code s3wire}
 * (exec time and full request-to-response-written RTT are the same span), and the connection ACL
 * ({@link ConnectionGate#acceptHttp}) applies. Bodies are streamed, never buffered whole (only the
 * small DeleteObjects / CompleteMultipartUpload XML bodies are read into memory).
 */
public final class S3WireServer {

    private static final Logger log = LoggerFactory.getLogger(S3WireServer.class);
    static final String MARKER = ".s3wire-bucket";
    private static final String OWNER = "warp-s3wire";
    static final java.util.regex.Pattern BUCKET_NAME =
            java.util.regex.Pattern.compile("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$");
    private static final java.util.regex.Pattern HEX64 = java.util.regex.Pattern.compile("^[0-9a-fA-F]{64}$");
    private static final Set<String> UNSUPPORTED_SUBRESOURCES = Set.of("acl", "tagging", "versioning", "policy",
            "cors", "lifecycle", "encryption", "retention", "legal-hold", "versions", "object-lock", "website",
            "notification", "torrent", "restore", "select", "attributes", "accelerate", "logging", "replication",
            "ownershipControls", "publicAccessBlock", "analytics", "metrics", "inventory", "intelligent-tiering",
            "requestPayment", "policyStatus", "uploads");
    private static final int MAX_XML_BODY = 4 * 1024 * 1024;

    private final Server server;
    private final S3WireConfig config;
    private final SqlMetricsCollector sqlMetrics;
    private final S3SigV4Verifier verifier;
    private final ObjectStore proxyStore;
    private final PostgresObjectStore postgresStore;
    private final S3Api api;
    private final List<String> vhostDomains = S3Addressing.parseDomains(System.getenv("WARP_S3WIRE_VHOST_DOMAIN"));
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

    private record Route(String op, boolean write, String bucket, String key, Map<String, String> query) {
    }

    /** Proxy-only server (the original constructor): no Postgres store. */
    public S3WireServer(int port, S3WireConfig config, ConnectionGate connectionGate, SqlMetricsCollector sqlMetrics) {
        this(port, config, connectionGate, sqlMetrics, null, null);
    }

    /**
     * @param registry backend registry whose {@code s3} store hosts back Postgres mode; null = proxy only
     * @param options  Postgres-mode tunables (null = defaults)
     */
    public S3WireServer(int port, S3WireConfig config, ConnectionGate connectionGate, SqlMetricsCollector sqlMetrics,
            BackendRegistry registry, S3StoreOptions options) {
        this.config = config;
        this.sqlMetrics = sqlMetrics;
        this.verifier = new S3SigV4Verifier(config.clientCredentials());
        this.proxyStore = config.backendBucket() == null ? null : new ProxyObjectStore(config);
        this.postgresStore = registry == null ? null
                : new PostgresObjectStore(registry, options == null ? S3StoreOptions.defaults() : options);
        this.api = postgresStore == null ? null : new S3Api(postgresStore, config);
        this.server = new Server(port);
        allowAmbiguousKeyPaths(server);
        // A/B routing (com.sayonora.warp.ab): authenticate exactly like serve() does, then forward to the cloud when the policy says so
        com.sayonora.warp.ab.AbRouting.Authenticator abAuth = (request, response, bodyBytes) -> {
            if (!connectionGate.acceptHttp(request)) {
                return com.sayonora.warp.ab.AbRouting.AuthResult.denied(403, "AccessDenied", "Access denied by Warp connection ACL");
            }
            S3SigV4Verifier.Result auth = verifier.verify(request.getMethod(), request.getRequestURI(), request.getQueryString(),
                    headers(request), Instant.now());
            if (!auth.valid()) {
                return com.sayonora.warp.ab.AbRouting.AuthResult.denied(auth.status(), auth.code(), auth.message());
            }
            Addr addr = address(request);
            String path = !addr.vhost() ? null : "/" + com.sayonora.warp.ab.AbSigV4.enc(addr.bucket(), false)
                    + (addr.key().isEmpty() ? "/" : "/" + com.sayonora.warp.ab.AbSigV4.enc(addr.key(), true));
            S3SigV4Verifier.ChunkAuth chunk = auth.chunk();
            return com.sayonora.warp.ab.AbRouting.AuthResult.ok(auth.accessKeyId(), in -> new AwsChunkedInputStream(in, chunk), path);
        };
        server.setHandler(com.sayonora.warp.ab.AbRouting.wrap("s3", new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                serve(request, response, connectionGate);
            }
        }, abAuth));
    }

    /**
     * S3 keys may legally contain empty path segments ({@code a//b}), {@code .} / {@code ..} segments and
     * encoded slashes; Jetty rejects such request targets as "ambiguous" by default. s3wire never maps
     * the path onto the filesystem, so it accepts them and reads the raw URI.
     */
    private static void allowAmbiguousKeyPaths(Server server) {
        org.eclipse.jetty.http.UriCompliance lax = org.eclipse.jetty.http.UriCompliance.from(java.util.EnumSet.of(
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_SEGMENT,
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT,
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR,
                org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING));
        for (org.eclipse.jetty.server.Connector c : server.getConnectors()) {
            org.eclipse.jetty.server.HttpConnectionFactory f =
                    c.getConnectionFactory(org.eclipse.jetty.server.HttpConnectionFactory.class);
            if (f != null) {
                f.getHttpConfiguration().setUriCompliance(lax);
                // S3 caps the header section at 8 KiB itself; Warp validates (e.g. an oversized x-amz-tagging) and answers S3 errors
                f.getHttpConfiguration().setRequestHeaderSize(32 * 1024);
            }
        }
    }

    /** The request handler this server's own listener uses, so the unified AWS endpoint can dispatch to it in process. */
    public org.eclipse.jetty.server.Handler handler() {
        return server.getHandler();
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
        if (proxyStore != null) {
            proxyStore.close();
        }
        if (postgresStore != null) {
            postgresStore.close();
        }
    }

    /** Postgres mode wins whenever the s3 store is enabled; else proxy mode; else null. */
    private PostgresObjectStore selectPostgres() {
        return postgresStore != null && postgresStore.available() ? postgresStore : null;
    }

    // ---------------------------------------------------------------------------------------------

    /** Presigned URLs: every {@code x-amz-*} header the client sent must be part of X-Amz-SignedHeaders. */
    private static void requireHeadersSigned(HttpServletRequest request) {
        String spec = S3SigV4Verifier.parseQuery(request.getQueryString()).getOrDefault("X-Amz-SignedHeaders", "");
        Set<String> signed = new java.util.HashSet<>();
        for (String h : spec.split(";")) {
            signed.add(h.trim().toLowerCase(Locale.ROOT));
        }
        List<String> unsigned = new ArrayList<>();
        for (String name : Collections.list(request.getHeaderNames())) {
            String n = name.toLowerCase(Locale.ROOT);
            if (n.startsWith("x-amz-") && !signed.contains(n)) {
                unsigned.add(n);
            }
        }
        if (!unsigned.isEmpty()) {
            throw new S3WireException(403, "AccessDenied", "There were headers present in the request which were not signed",
                    null, Map.of("HeadersNotSigned", String.join(",", unsigned)));
        }
    }

    /** Where the request points: bucket (null/empty = service level) and decoded key ("" = none). */
    private record Addr(String bucket, String key, boolean vhost) {
    }

    private Addr address(HttpServletRequest request) {
        String rawPath = request.getRequestURI();
        String rest = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
        String vb = S3Addressing.bucketFromHost(request.getHeader("Host"), vhostDomains);
        if (vb != null) {
            return new Addr(vb, S3SigV4Verifier.percentDecode(rest, false), true);
        }
        int slash = rest.indexOf('/');
        String bucket = S3SigV4Verifier.percentDecode(slash < 0 ? rest : rest.substring(0, slash), false);
        String key = slash < 0 ? "" : S3SigV4Verifier.percentDecode(rest.substring(slash + 1), false);
        return new Addr(bucket, key, false);
    }

    private void serve(HttpServletRequest request, HttpServletResponse response, ConnectionGate gate) {
        long start = System.nanoTime();
        byte[] idBytes = new byte[12];
        RANDOM.nextBytes(idBytes);
        String requestId = UUID.randomUUID().toString().replace("-", "").substring(0, 16).toUpperCase(Locale.ROOT);
        response.setHeader("x-amz-request-id", requestId);
        response.setHeader("x-amz-id-2", java.util.Base64.getEncoder().encodeToString(idBytes) + "="
                + java.util.Base64.getEncoder().encodeToString(idBytes));
        response.setHeader("Server", "Warp-s3wire");
        Route route = null;
        ObjectStore store = null;
        PostgresObjectStore pg = null;
        try {
            if (!gate.acceptHttp(request)) {
                throw new S3WireException(403, "AccessDenied", "Access denied by Warp connection ACL");
            }
            pg = selectPostgres();
            Addr addr = address(request);
            if (pg != null && addr.bucket() != null && !addr.bucket().isEmpty()
                    && (request.getHeader("Origin") != null || "OPTIONS".equals(request.getMethod()))) {
                if (cors(pg, addr, request, response)) {
                    return;
                }
            }
            S3SigV4Verifier.Result auth = verifier.verify(request.getMethod(), request.getRequestURI(),
                    request.getQueryString(), headers(request), Instant.now());
            if (!auth.valid()) {
                if (pg != null && S3Api.isAnonymousPostForm(request, addr.key())) {
                    // browser form upload: authenticated by the signed policy inside the form, not by headers
                    route = new Route("PostObject", true, addr.bucket(), null, Map.of());
                    api.postObject(addr.bucket(), request, response);
                    return;
                }
                log.warn("s3wire: rejecting request -- {} ({})", auth.message(), auth.code());
                throw new S3WireException(auth.status(), auth.code(), auth.message());
            }
            request.setAttribute("s3wire.chunkAuth", auth.chunk());
            if (request.getQueryString() != null && request.getQueryString().contains("X-Amz-Signature")) {
                requireHeadersSigned(request);
            }
            if (pg == null) {
                store = proxyStore;
            }
            if (pg == null && store == null) {
                throw new S3WireException(503, "ServiceUnavailable",
                        "No object store is available: enable the s3 store on a Postgres backend of this set "
                                + "or configure WARP_S3WIRE_BACKEND_BUCKET");
            }
            if (pg != null) {
                S3Api.Route ar = api.route(request, addr.bucket(), addr.key(), S3SigV4Verifier.parseQuery(request.getQueryString()));
                route = new Route(ar.op(), ar.write(), ar.bucket(), ar.key(), ar.query());
                api.execute(ar, request, response);
            } else {
                route = route(request, store, addr);
                execute(store, route, request, response);
            }
        } catch (S3WireException e) {
            writeError(request, response, requestId, e.status, e.code, e.getMessage(), route, e);
        } catch (S3Exception e) {
            handleBackendError(store, request, response, requestId, e, route);
        } catch (SdkException e) {
            log.error("s3wire: backend unreachable/failed for {}", route == null ? "?" : route.op(), e);
            writeError(request, response, requestId, 503, "ServiceUnavailable",
                    "The backend object store is unavailable: " + e.getMessage(), route, null);
        } catch (IOException | RuntimeException e) {
            log.error("s3wire: {} failed", route == null ? "?" : route.op(), e);
            writeError(request, response, requestId, 500, "InternalError", "We encountered an internal error.", route, null);
        } finally {
            if (sqlMetrics != null && route != null && (store != null || pg != null)) {
                long elapsed = System.nanoTime() - start;
                sqlMetrics.recordOperation("s3wire", pg != null ? pg.label() : store.label(),
                        route.write() ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ,
                        route.op(), elapsed, elapsed);
            }
        }
    }

    /**
     * CORS: answers a preflight ({@code OPTIONS}) completely and decorates actual responses. Returns true when the
     * request was fully handled (preflight).
     */
    private boolean cors(PostgresObjectStore pg, Addr addr, HttpServletRequest request, HttpServletResponse response) {
        String origin = request.getHeader("Origin");
        boolean preflight = "OPTIONS".equals(request.getMethod());
        if (!preflight && origin == null) {
            return false;
        }
        if (preflight && (origin == null || request.getHeader("Access-Control-Request-Method") == null)) {
            throw new S3WireException(400, "BadRequest", "Insufficient information. Origin request header needed.");
        }
        if (preflight && !pg.bucketExists(addr.bucket())) {
            throw new S3WireException(404, "NoSuchBucket", "The specified bucket does not exist", null,
                    Map.of("BucketName", addr.bucket()));
        }
        String xml = pg.corsConfig(addr.bucket());
        List<S3Cfg.CorsRule> rules = null;
        if (xml != null) {
            try {
                rules = S3Cfg.parseCors(S3Xml.utf8(xml));
            } catch (RuntimeException e) {
                rules = null;
            }
        }
        String method = preflight ? request.getHeader("Access-Control-Request-Method") : request.getMethod();
        S3Cfg.CorsRule rule = rules == null ? null : S3Cfg.matchCors(rules, origin, method,
                preflight ? request.getHeader("Access-Control-Request-Headers") : null);
        if (rule == null) {
            if (preflight) {
                throw new S3WireException(403, "AccessForbidden", "CORSResponse: This CORS request is not allowed. This is "
                        + "usually because the evalution of Origin, request method / Access-Control-Request-Method or "
                        + "Access-Control-Request-Headers are not whitelisted by the resource's CORS spec.", null,
                        Map.of("Method", method, "ResourceType", "BUCKET"));
            }
            return false;
        }
        response.setHeader("Access-Control-Allow-Origin", rule.origins().contains("*") ? "*" : origin);
        response.setHeader("Vary", preflight ? "Origin, Access-Control-Request-Headers, Access-Control-Request-Method" : "Origin");
        response.setHeader("Access-Control-Allow-Methods", String.join(", ", rule.methods()));
        if (preflight && request.getHeader("Access-Control-Request-Headers") != null) {
            response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"));
        }
        if (!rule.expose().isEmpty()) {
            response.setHeader("Access-Control-Expose-Headers", String.join(", ", rule.expose()));
        }
        if (rule.maxAge() != null) {
            response.setHeader("Access-Control-Max-Age", String.valueOf(rule.maxAge()));
        }
        if (preflight) {
            response.setStatus(200);
            response.setContentLength(0);
            return true;
        }
        return false;
    }

    private static Map<String, List<String>> headers(HttpServletRequest request) {
        Map<String, List<String>> out = new HashMap<>();
        for (String name : Collections.list(request.getHeaderNames())) {
            out.put(name.toLowerCase(Locale.ROOT), Collections.list(request.getHeaders(name)));
        }
        return out;
    }

    private void handleBackendError(ObjectStore store, HttpServletRequest request, HttpServletResponse response,
            String requestId, S3Exception e, Route route) {
        int status = e.statusCode();
        String code = e.awsErrorDetails() != null && e.awsErrorDetails().errorCode() != null
                ? e.awsErrorDetails().errorCode() : "InternalError";
        String message = e.awsErrorDetails() != null && e.awsErrorDetails().errorMessage() != null
                ? e.awsErrorDetails().errorMessage() : e.getMessage();
        if (status == 304) {
            response.setStatus(304);
            return;
        }
        if (status == 404 && route != null && route.bucket() != null && !route.op().equals("HeadBucket")
                && !route.op().equals("CreateBucket") && !safeBucketExists(store, route.bucket())) {
            code = "NoSuchBucket";
            message = "The specified bucket does not exist";
        } else if (status == 404 && "NotFound".equals(code) && route != null && route.key() != null) {
            code = "NoSuchKey";
            message = "The specified key does not exist.";
        }
        writeError(request, response, requestId, status, code, message, route, null);
    }

    private static boolean safeBucketExists(ObjectStore store, String bucket) {
        try {
            return store.bucketExists(bucket);
        } catch (RuntimeException e) {
            return true;
        }
    }

    private void writeError(HttpServletRequest request, HttpServletResponse response, String requestId, int status,
            String code, String message, Route route, S3WireException ex) {
        if (response.isCommitted()) {
            log.warn("s3wire: error {} after response committed for {}", code, request.getRequestURI());
            return;
        }
        try {
            response.setStatus(status);
            response.setHeader("x-amz-error-code", code);
            if (ex != null && ex.headers != null) {
                ex.headers.forEach(response::setHeader);
            }
            if (status == 304 || "HEAD".equals(request.getMethod())) {
                return;
            }
            if (ex != null && (ex.details != null || ex.headers != null || status != 501)) {
                Map<String, String> details = new LinkedHashMap<>();
                if (ex.details != null) {
                    details.putAll(ex.details);
                } else if (route != null && "NoSuchKey".equals(code) && route.key() != null) {
                    details.put("Key", route.key());
                } else if (route != null && "NoSuchBucket".equals(code)) {
                    details.put("BucketName", route.bucket());
                }
                String resource = route != null && route.bucket() != null ? "/" + route.bucket()
                        + (route.key() == null || route.key().isEmpty() ? "" : "/" + route.key()) : request.getRequestURI();
                writeXml(response, status, S3Xml.errorRich(code, message, resource, requestId,
                        response.getHeader("x-amz-id-2"), details));
                return;
            }
            String extraName = null;
            String extraValue = null;
            if (route != null && "NoSuchKey".equals(code)) {
                extraName = "Key";
                extraValue = route.key();
            } else if (route != null && "NoSuchBucket".equals(code)) {
                extraName = "BucketName";
                extraValue = route.bucket();
            }
            writeXml(response, status, S3Xml.error(code, message, request.getRequestURI(), requestId, extraName, extraValue));
        } catch (IOException e) {
            log.debug("s3wire: could not write error response", e);
        }
    }

    private static void writeXml(HttpServletResponse response, int status, String xml) throws IOException {
        byte[] bytes = S3Xml.utf8(xml);
        response.setStatus(status);
        response.setContentType("application/xml");
        response.setContentLength(bytes.length);
        response.getOutputStream().write(bytes);
    }

    // ---- routing -------------------------------------------------------------------------------

    private Route route(HttpServletRequest request, ObjectStore store, Addr addr) {
        String method = request.getMethod();
        String bucket = addr.bucket();
        String key = addr.key();
        Map<String, String> q = S3SigV4Verifier.parseQuery(request.getQueryString());
        boolean unsupportedSub = false;
        for (String k : q.keySet()) {
            if (UNSUPPORTED_SUBRESOURCES.contains(k)) {
                unsupportedSub = true;
            }
        }
        if (bucket.isEmpty()) {
            if ("GET".equals(method)) {
                return new Route("ListBuckets", false, null, null, q);
            }
            throw new S3WireException(405, "MethodNotAllowed", "The specified method is not allowed against this resource.");
        }
        if (key.isEmpty()) {
            switch (method) {
                case "PUT" -> {
                    if (!unsupportedSub) {
                        return new Route("CreateBucket", true, bucket, null, q);
                    }
                }
                case "HEAD" -> {
                    return new Route("HeadBucket", false, bucket, null, q);
                }
                case "DELETE" -> {
                    if (!unsupportedSub) {
                        return new Route("DeleteBucket", true, bucket, null, q);
                    }
                }
                case "GET" -> {
                    if (q.containsKey("location")) {
                        return new Route("GetBucketLocation", false, bucket, null, q);
                    }
                    if (!unsupportedSub) {
                        return new Route(q.containsKey("list-type") ? "ListObjectsV2" : "ListObjects", false, bucket, null, q);
                    }
                }
                case "POST" -> {
                    if (q.containsKey("delete")) {
                        return new Route("DeleteObjects", true, bucket, null, q);
                    }
                }
                default -> {
                }
            }
            throw notImplemented(method, q);
        }
        if (key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) {
            throw new S3WireException(400, "KeyTooLongError", "Your key is too long");
        }
        if (store.reservesMarkerKey() && key.equals(MARKER)) {
            throw new S3WireException(400, "InvalidArgument", "Key '" + MARKER + "' is reserved by s3wire");
        }
        switch (method) {
            case "GET" -> {
                if (!unsupportedSub && !q.containsKey("uploadId")) {
                    return new Route("GetObject", false, bucket, key, q);
                }
            }
            case "HEAD" -> {
                return new Route("HeadObject", false, bucket, key, q);
            }
            case "PUT" -> {
                if (q.containsKey("uploadId") && q.containsKey("partNumber")) {
                    if (request.getHeader("x-amz-copy-source") != null) {
                        throw notImplemented("UploadPartCopy", q);
                    }
                    return new Route("UploadPart", true, bucket, key, q);
                }
                if (!unsupportedSub && !q.containsKey("uploadId")) {
                    return new Route(request.getHeader("x-amz-copy-source") != null ? "CopyObject" : "PutObject",
                            true, bucket, key, q);
                }
            }
            case "DELETE" -> {
                if (q.containsKey("uploadId")) {
                    return new Route("AbortMultipartUpload", true, bucket, key, q);
                }
                if (!unsupportedSub) {
                    return new Route("DeleteObject", true, bucket, key, q);
                }
            }
            case "POST" -> {
                if (q.containsKey("uploads")) {
                    return new Route("CreateMultipartUpload", true, bucket, key, q);
                }
                if (q.containsKey("uploadId")) {
                    return new Route("CompleteMultipartUpload", true, bucket, key, q);
                }
            }
            default -> {
            }
        }
        throw notImplemented(method, q);
    }

    private static S3WireException notImplemented(String what, Map<String, String> q) {
        return new S3WireException(501, "NotImplemented",
                "s3wire does not implement this operation (" + what + " " + q.keySet() + ")");
    }

    // ---- execution ---------------------------------------------------------------------------------

    private void requireBucket(ObjectStore store, String bucket) {
        if (!store.bucketExists(bucket)) {
            throw new S3WireException(404, "NoSuchBucket", "The specified bucket does not exist");
        }
    }

    private void execute(ObjectStore store, Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        switch (r.op()) {
            case "ListBuckets" -> listBuckets(store, resp);
            case "CreateBucket" -> {
                store.createBucket(r.bucket());
                resp.setHeader("Location", "/" + r.bucket());
                resp.setStatus(200);
            }
            case "HeadBucket" -> {
                requireBucket(store, r.bucket());
                resp.setStatus(200);
            }
            case "DeleteBucket" -> {
                store.deleteBucket(r.bucket());
                resp.setStatus(204);
            }
            case "GetBucketLocation" -> {
                requireBucket(store, r.bucket());
                writeXml(resp, 200, S3Xml.location(config.region()));
            }
            case "ListObjects", "ListObjectsV2" -> listObjects(store, r, resp);
            case "PutObject" -> putObject(store, r, req, resp);
            case "GetObject" -> getObject(store, r, req, resp);
            case "HeadObject" -> headObject(store, r, req, resp);
            case "DeleteObject" -> {
                requireBucket(store, r.bucket());
                store.delete(r.bucket(), r.key());
                resp.setStatus(204);
            }
            case "DeleteObjects" -> deleteObjects(store, r, req, resp);
            case "CopyObject" -> copyObject(store, r, req, resp);
            case "CreateMultipartUpload" -> {
                requireBucket(store, r.bucket());
                String id = store.createMultipart(r.bucket(), r.key(), Attrs.fromRequest(req));
                writeXml(resp, 200, S3Xml.initiateMultipart(r.bucket(), r.key(), id));
            }
            case "UploadPart" -> uploadPart(store, r, req, resp);
            case "CompleteMultipartUpload" -> {
                String eTag = store.completeMultipart(r.bucket(), r.key(), r.query().get("uploadId"),
                        S3Xml.parseCompleteMultipart(readSmallBody(req)));
                writeXml(resp, 200, S3Xml.completeMultipart(req.getRequestURL().toString(), r.bucket(), r.key(), eTag));
            }
            case "AbortMultipartUpload" -> {
                requireBucket(store, r.bucket());
                store.abortMultipart(r.bucket(), r.key(), r.query().get("uploadId"));
                resp.setStatus(204);
            }
            default -> throw notImplemented(r.op(), r.query());
        }
    }

    private void listBuckets(ObjectStore store, HttpServletResponse resp) throws IOException {
        List<String[]> buckets = new ArrayList<>();
        for (ObjectStore.BucketEntry b : store.listBuckets()) {
            buckets.add(new String[] {b.name(), S3Xml.iso(b.created())});
        }
        writeXml(resp, 200, S3Xml.listBuckets(buckets, OWNER));
    }

    private void listObjects(ObjectStore store, Route r, HttpServletResponse resp) throws IOException {
        Map<String, String> q = r.query();
        boolean v2 = "ListObjectsV2".equals(r.op());
        String prefix = q.getOrDefault("prefix", "");
        String delimiter = q.get("delimiter");
        boolean urlEncode = "url".equals(q.get("encoding-type"));
        int max = 1000;
        if (q.containsKey("max-keys")) {
            try {
                max = Integer.parseInt(q.get("max-keys"));
            } catch (NumberFormatException e) {
                throw new S3WireException(400, "InvalidArgument", "Provided max-keys not an integer or within integer range");
            }
            if (max < 0) {
                throw new S3WireException(400, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647");
            }
            max = Math.min(max, 1000);
        }
        String startAfter = v2 ? q.get("start-after") : q.get("marker");
        String token = v2 ? q.get("continuation-token") : null;
        if (max == 0) {
            requireBucket(store, r.bucket());
            writeXml(resp, 200, S3Xml.listObjects(new S3Xml.ListParams(r.bucket(), prefix, delimiter, 0, false, v2,
                    token, null, startAfter, startAfter, null, urlEncode), List.of(), List.of()));
            return;
        }
        ObjectStore.ListResult res = store.list(new ObjectStore.ListRequest(r.bucket(), prefix, delimiter, max, token,
                startAfter, v2));
        writeXml(resp, 200, S3Xml.listObjects(new S3Xml.ListParams(r.bucket(), prefix, delimiter, max, res.truncated(),
                v2, token, res.truncated() ? res.nextToken() : null, v2 ? startAfter : null, startAfter,
                res.truncated() ? res.lastKey() : null, urlEncode), res.objects(), res.commonPrefixes()));
    }

    // ---- objects -----------------------------------------------------------------------------------

    private void putObject(ObjectStore store, Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        requireBucket(store, r.bucket());
        Body body = openBody(req);
        ObjectStore.ObjectInfo info = store.put(new ObjectStore.PutRequest(r.bucket(), r.key(), Attrs.fromRequest(req),
                body.stream(), body.length(), req.getHeader("Content-MD5"), body::verifyDigest));
        if (info.eTag() != null) {
            resp.setHeader("ETag", info.eTag());
        }
        resp.setStatus(200);
    }

    private static ObjectStore.Conditions conditions(HttpServletRequest req, boolean all) {
        return new ObjectStore.Conditions(req.getHeader("If-Match"), req.getHeader("If-None-Match"),
                all ? req.getHeader("If-Modified-Since") : null, all ? req.getHeader("If-Unmodified-Since") : null);
    }

    private void getObject(ObjectStore store, Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // proxy mode forwards only If-Match / If-None-Match to the backend (unchanged); Postgres mode evaluates all four
        try (ObjectStore.ObjectRead g = store.get(r.bucket(), r.key(), conditions(req, !store.reservesMarkerKey()),
                req.getHeader("Range"))) {
            ObjectStore.ObjectInfo i = g.info();
            resp.setStatus(g.contentRange() != null ? 206 : 200);
            writeObjectHeaders(resp, g.contentLength() < 0 ? null : g.contentLength(), i, g.contentRange());
            g.transferTo(resp.getOutputStream());
        }
    }

    private void headObject(ObjectStore store, Route r, HttpServletRequest req, HttpServletResponse resp) {
        ObjectStore.ObjectInfo h = store.head(r.bucket(), r.key(), conditions(req, !store.reservesMarkerKey()));
        resp.setStatus(200);
        writeObjectHeaders(resp, h.size(), h, null);
    }

    private static void writeObjectHeaders(HttpServletResponse resp, Long length, ObjectStore.ObjectInfo info,
            String range) {
        ObjectStore.Attrs a = info.attrs();
        if (length != null) {
            resp.setContentLengthLong(length);
        }
        if (a.contentType() != null) {
            resp.setContentType(a.contentType());
        }
        if (info.eTag() != null) {
            resp.setHeader("ETag", info.eTag());
        }
        if (info.lastModified() != null) {
            resp.setHeader("Last-Modified", S3Xml.httpDate(info.lastModified()));
        }
        if (range != null) {
            resp.setHeader("Content-Range", range);
        }
        resp.setHeader("Accept-Ranges", "bytes");
        if (a.cacheControl() != null) {
            resp.setHeader("Cache-Control", a.cacheControl());
        }
        if (a.contentDisposition() != null) {
            resp.setHeader("Content-Disposition", a.contentDisposition());
        }
        if (a.contentEncoding() != null) {
            resp.setHeader("Content-Encoding", a.contentEncoding());
        }
        if (a.contentLanguage() != null) {
            resp.setHeader("Content-Language", a.contentLanguage());
        }
        if (a.expires() != null) {
            resp.setHeader("Expires", a.expires());
        }
        if (a.metadata() != null) {
            a.metadata().forEach((k, v) -> resp.setHeader("x-amz-meta-" + k, v));
        }
    }

    private void deleteObjects(ObjectStore store, Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        requireBucket(store, r.bucket());
        byte[] xml = readSmallBody(req);
        S3Xml.Delete d = S3Xml.parseDelete(xml);
        if (d.keys().size() > 1000 || d.keys().isEmpty()) {
            throw new S3WireException(400, "MalformedXML", "DeleteObjects requires between 1 and 1000 keys.");
        }
        ObjectStore.DeleteManyResult res = store.deleteMany(r.bucket(), d.keys());
        writeXml(resp, 200, S3Xml.deleteResult(res.deleted(), res.errors(), d.quiet()));
    }

    private void copyObject(ObjectStore store, Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        requireBucket(store, r.bucket());
        String src = req.getHeader("x-amz-copy-source");
        int qm = src.indexOf('?');
        if (qm >= 0) {
            src = src.substring(0, qm);
        }
        src = S3SigV4Verifier.percentDecode(src.startsWith("/") ? src.substring(1) : src, false);
        int slash = src.indexOf('/');
        if (slash <= 0 || slash == src.length() - 1) {
            throw new S3WireException(400, "InvalidArgument", "Copy Source must mention the source bucket and key: sourcebucket/sourcekey");
        }
        String srcBucket = src.substring(0, slash);
        String srcKey = src.substring(slash + 1);
        if (store.reservesMarkerKey() && srcKey.equals(MARKER)) {
            throw new S3WireException(400, "InvalidArgument", "Key '" + MARKER + "' is reserved by s3wire");
        }
        ObjectStore.Attrs replace = "REPLACE".equalsIgnoreCase(req.getHeader("x-amz-metadata-directive"))
                ? Attrs.fromRequest(req) : null;
        ObjectStore.CopyResult res = store.copy(new ObjectStore.CopyRequest(srcBucket, srcKey, r.bucket(), r.key(), replace));
        writeXml(resp, 200, S3Xml.copyResult(res.eTag(), res.lastModified()));
    }

    // ---- multipart ---------------------------------------------------------------------------------

    private void uploadPart(ObjectStore store, Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        int partNumber;
        try {
            partNumber = Integer.parseInt(r.query().get("partNumber"));
        } catch (NumberFormatException e) {
            throw new S3WireException(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive");
        }
        if (partNumber < 1 || partNumber > 10000) {
            throw new S3WireException(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive");
        }
        Body body = openBody(req);
        String eTag = store.uploadPart(new ObjectStore.UploadPartRequest(r.bucket(), r.key(), r.query().get("uploadId"),
                partNumber, body.stream(), body.length(), req.getHeader("Content-MD5"), body::verifyDigest));
        if (eTag != null) {
            resp.setHeader("ETag", eTag);
        }
        resp.setStatus(200);
    }

    // ---- request body / headers --------------------------------------------------------------------

    /** A streamed request body with its declared decoded length and optional payload-hash check. */
    private static final class Body {
        private final InputStream stream;
        private final long length;
        private final DigestInputStream digest;
        private final String expectedHex;

        Body(InputStream stream, long length, DigestInputStream digest, String expectedHex) {
            this.stream = stream;
            this.length = length;
            this.digest = digest;
            this.expectedHex = expectedHex;
        }

        InputStream stream() {
            return stream;
        }

        long length() {
            return length;
        }

        boolean verifyDigest() {
            return digest == null
                    || S3SigV4Verifier.hex(digest.getMessageDigest().digest()).equalsIgnoreCase(expectedHex);
        }
    }

    private static Body openBody(HttpServletRequest req) throws IOException {
        String sha = req.getHeader("x-amz-content-sha256");
        InputStream in = req.getInputStream();
        long length;
        if (sha != null && sha.startsWith("STREAMING-")) {
            String decoded = req.getHeader("x-amz-decoded-content-length");
            if (decoded == null) {
                throw new S3WireException(411, "MissingContentLength", "You must provide the Content-Length HTTP header.");
            }
            length = Long.parseLong(decoded.trim());
            in = new AwsChunkedInputStream(in);
        } else {
            length = req.getContentLengthLong();
            if (length < 0) {
                throw new S3WireException(411, "MissingContentLength", "You must provide the Content-Length HTTP header.");
            }
        }
        DigestInputStream digest = null;
        if (sha != null && HEX64.matcher(sha).matches()) {
            try {
                digest = new DigestInputStream(in, MessageDigest.getInstance("SHA-256"));
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
            in = digest;
        }
        return new Body(in, length, digest, sha);
    }

    private static byte[] readSmallBody(HttpServletRequest req) throws IOException {
        InputStream in = req.getInputStream();
        String sha = req.getHeader("x-amz-content-sha256");
        if (sha != null && sha.startsWith("STREAMING-")) {
            in = new AwsChunkedInputStream(in);
        }
        byte[] data = in.readNBytes(MAX_XML_BODY + 1);
        if (data.length > MAX_XML_BODY) {
            throw new S3WireException(400, "EntityTooLarge", "Request body too large");
        }
        if (sha != null && HEX64.matcher(sha).matches()) {
            try {
                String actual = S3SigV4Verifier.hex(MessageDigest.getInstance("SHA-256").digest(data));
                if (!actual.equalsIgnoreCase(sha)) {
                    throw new S3WireException(400, "XAmzContentSHA256Mismatch",
                            "The provided 'x-amz-content-sha256' header does not match what was computed.");
                }
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
        return data;
    }

    /** Builds {@link ObjectStore.Attrs} from request headers ({@code aws-chunked} is framing, not stored). */
    private static final class Attrs {
        static ObjectStore.Attrs fromRequest(HttpServletRequest req) {
            String enc = req.getHeader("Content-Encoding");
            String encoding = null;
            if (enc != null) {
                List<String> kept = new ArrayList<>();
                for (String e : enc.split(",")) {
                    if (!e.trim().equalsIgnoreCase("aws-chunked") && !e.isBlank()) {
                        kept.add(e.trim());
                    }
                }
                if (!kept.isEmpty()) {
                    encoding = String.join(",", kept);
                }
            }
            Map<String, String> meta = new LinkedHashMap<>();
            for (String name : Collections.list(req.getHeaderNames())) {
                if (name.toLowerCase(Locale.ROOT).startsWith("x-amz-meta-")) {
                    meta.put(name.substring("x-amz-meta-".length()).toLowerCase(Locale.ROOT), req.getHeader(name));
                }
            }
            return new ObjectStore.Attrs(req.getHeader("Content-Type"), req.getHeader("Cache-Control"),
                    req.getHeader("Content-Disposition"), encoding, req.getHeader("Content-Language"),
                    req.getHeader("Expires"), meta);
        }
    }

}
