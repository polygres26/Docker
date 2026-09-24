package com.sayonora.wire.s3wire;

import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.core.SqlMetricsCollector;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
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
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * s3wire -- Amazon S3 REST API frontend. A stock S3 client (boto3, AWS SDKs, aws-cli) points its
 * endpoint at Warp, signs with SigV4 against the credentials in {@link S3WireConfig}, and Warp
 * stores/serves the objects in ONE real backend S3-compatible bucket (MinIO in tests) using its own
 * backend credentials via the AWS SDK v2.
 *
 * <p><b>Bucket model:</b> client-visible buckets are key prefixes inside the single backend bucket:
 * bucket {@code b}, key {@code k} lives at backend key {@code b/k}. CreateBucket writes a reserved
 * zero-byte marker {@code b/.s3wire-bucket} (hidden from listings and rejected as a client key).
 * ListBuckets returns the distinct first path segments in the backend bucket. Objects written into
 * the backend bucket directly without a {@code bucket/} prefix are not visible through s3wire.
 *
 * <p><b>Supported:</b> ListBuckets, CreateBucket, HeadBucket, DeleteBucket, GetBucketLocation,
 * PutObject, GetObject (Range, If-Match, If-None-Match), HeadObject, DeleteObject, DeleteObjects,
 * CopyObject (server-side, single request), ListObjects (v1) and ListObjectsV2 (prefix, delimiter,
 * continuation-token, start-after, max-keys, encoding-type=url), multipart (Create/UploadPart/
 * Complete/Abort, proxied to backend multipart), presigned GET/PUT URLs. Path-style addressing only.
 *
 * <p><b>Not supported (501 NotImplemented):</b> ListParts, ListMultipartUploads, UploadPartCopy,
 * object versioning, ACLs, tagging, policies, lifecycle, CORS, encryption config, SelectObjectContent,
 * virtual-hosted-style addressing, CopyObject of objects over 5 GB.
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
    private static final java.util.regex.Pattern BUCKET_NAME =
            java.util.regex.Pattern.compile("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$");
    private static final java.util.regex.Pattern HEX64 = java.util.regex.Pattern.compile("^[0-9a-fA-F]{64}$");
    private static final Set<String> UNSUPPORTED_SUBRESOURCES = Set.of("acl", "tagging", "versioning", "policy",
            "cors", "lifecycle", "encryption", "retention", "legal-hold", "versions", "object-lock", "website",
            "notification", "torrent", "restore", "select", "attributes", "accelerate", "logging", "replication",
            "ownershipControls", "publicAccessBlock", "analytics", "metrics", "inventory", "intelligent-tiering",
            "requestPayment", "policyStatus", "uploads");
    private static final int MAX_XML_BODY = 4 * 1024 * 1024;

    private final Server server;
    private final S3Client s3;
    private final S3WireConfig config;
    private final SqlMetricsCollector sqlMetrics;
    private final S3SigV4Verifier verifier;
    private final Set<String> knownBuckets = ConcurrentHashMap.newKeySet();
    private final String backendLabel;

    private record Route(String op, boolean write, String bucket, String key, Map<String, String> query) {
    }

    public S3WireServer(int port, S3WireConfig config, ConnectionGate connectionGate, SqlMetricsCollector sqlMetrics) {
        this.config = config;
        this.sqlMetrics = sqlMetrics;
        this.verifier = new S3SigV4Verifier(config.clientCredentials());
        this.backendLabel = "s3:" + config.backendBucket();
        this.s3 = buildClient(config);
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                serve(request, response, connectionGate);
            }
        });
    }

    private static S3Client buildClient(S3WireConfig c) {
        AwsCredentialsProvider creds = c.accessKey() != null && c.secretKey() != null
                ? StaticCredentialsProvider.create(AwsBasicCredentials.create(c.accessKey(), c.secretKey()))
                : DefaultCredentialsProvider.create();
        S3ClientBuilder b = S3Client.builder().region(Region.of(c.region())).credentialsProvider(creds)
                // Only send checksums the API requires: keeps streamed uploads on plain
                // aws-chunked framing without an extra trailing-checksum pass.
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        if (c.endpoint() != null) {
            b.endpointOverride(URI.create(c.endpoint()));
        }
        if (c.pathStyle()) {
            b.forcePathStyle(true);
        }
        return b.build();
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
        s3.close();
    }

    // ---------------------------------------------------------------------------------------------

    private void serve(HttpServletRequest request, HttpServletResponse response, ConnectionGate gate) {
        long start = System.nanoTime();
        String requestId = UUID.randomUUID().toString().replace("-", "").substring(0, 16).toUpperCase(Locale.ROOT);
        response.setHeader("x-amz-request-id", requestId);
        response.setHeader("Server", "Warp-s3wire");
        Route route = null;
        try {
            if (!gate.acceptHttp(request)) {
                throw new S3WireException(403, "AccessDenied", "Access denied by Warp connection ACL");
            }
            S3SigV4Verifier.Result auth = verifier.verify(request.getMethod(), request.getRequestURI(),
                    request.getQueryString(), headers(request), Instant.now());
            if (!auth.valid()) {
                log.warn("s3wire: rejecting request -- {} ({})", auth.message(), auth.code());
                throw new S3WireException(auth.status(), auth.code(), auth.message());
            }
            route = route(request);
            execute(route, request, response);
        } catch (S3WireException e) {
            writeError(request, response, requestId, e.status, e.code, e.getMessage(), route);
        } catch (S3Exception e) {
            handleBackendError(request, response, requestId, e, route);
        } catch (SdkException e) {
            log.error("s3wire: backend unreachable/failed for {}", route == null ? "?" : route.op(), e);
            writeError(request, response, requestId, 503, "ServiceUnavailable",
                    "The backend object store is unavailable: " + e.getMessage(), route);
        } catch (IOException | RuntimeException e) {
            log.error("s3wire: {} failed", route == null ? "?" : route.op(), e);
            writeError(request, response, requestId, 500, "InternalError", "We encountered an internal error.", route);
        } finally {
            if (sqlMetrics != null && route != null) {
                long elapsed = System.nanoTime() - start;
                sqlMetrics.recordOperation("s3wire", backendLabel,
                        route.write() ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ,
                        route.op(), elapsed, elapsed);
            }
        }
    }

    private static Map<String, List<String>> headers(HttpServletRequest request) {
        Map<String, List<String>> out = new HashMap<>();
        for (String name : Collections.list(request.getHeaderNames())) {
            out.put(name.toLowerCase(Locale.ROOT), Collections.list(request.getHeaders(name)));
        }
        return out;
    }

    private void handleBackendError(HttpServletRequest request, HttpServletResponse response, String requestId,
            S3Exception e, Route route) {
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
                && !route.op().equals("CreateBucket") && !safeBucketExists(route.bucket())) {
            code = "NoSuchBucket";
            message = "The specified bucket does not exist";
        } else if (status == 404 && "NotFound".equals(code) && route != null && route.key() != null) {
            code = "NoSuchKey";
            message = "The specified key does not exist.";
        }
        writeError(request, response, requestId, status, code, message, route);
    }

    private boolean safeBucketExists(String bucket) {
        try {
            return bucketExists(bucket);
        } catch (RuntimeException e) {
            return true;
        }
    }

    private void writeError(HttpServletRequest request, HttpServletResponse response, String requestId, int status,
            String code, String message, Route route) {
        if (response.isCommitted()) {
            log.warn("s3wire: error {} after response committed for {}", code, request.getRequestURI());
            return;
        }
        try {
            response.setStatus(status);
            response.setHeader("x-amz-error-code", code);
            if ("HEAD".equals(request.getMethod())) {
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

    private Route route(HttpServletRequest request) {
        String method = request.getMethod();
        String rawPath = request.getRequestURI();
        String rest = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
        int slash = rest.indexOf('/');
        String bucket = S3SigV4Verifier.percentDecode(slash < 0 ? rest : rest.substring(0, slash), false);
        String key = slash < 0 ? "" : S3SigV4Verifier.percentDecode(rest.substring(slash + 1), false);
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
        if (key.equals(MARKER)) {
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

    private void execute(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        switch (r.op()) {
            case "ListBuckets" -> listBuckets(resp);
            case "CreateBucket" -> createBucket(r, resp);
            case "HeadBucket" -> {
                requireBucket(r.bucket());
                resp.setStatus(200);
            }
            case "DeleteBucket" -> deleteBucket(r, resp);
            case "GetBucketLocation" -> {
                requireBucket(r.bucket());
                writeXml(resp, 200, S3Xml.location(config.region()));
            }
            case "ListObjects", "ListObjectsV2" -> listObjects(r, resp);
            case "PutObject" -> putObject(r, req, resp);
            case "GetObject" -> getObject(r, req, resp);
            case "HeadObject" -> headObject(r, req, resp);
            case "DeleteObject" -> {
                requireBucket(r.bucket());
                s3.deleteObject(b -> b.bucket(config.backendBucket()).key(backendKey(r.bucket(), r.key())));
                resp.setStatus(204);
            }
            case "DeleteObjects" -> deleteObjects(r, req, resp);
            case "CopyObject" -> copyObject(r, req, resp);
            case "CreateMultipartUpload" -> createMultipart(r, req, resp);
            case "UploadPart" -> uploadPart(r, req, resp);
            case "CompleteMultipartUpload" -> completeMultipart(r, req, resp);
            case "AbortMultipartUpload" -> {
                requireBucket(r.bucket());
                s3.abortMultipartUpload(b -> b.bucket(config.backendBucket()).key(backendKey(r.bucket(), r.key()))
                        .uploadId(r.query().get("uploadId")));
                resp.setStatus(204);
            }
            default -> throw notImplemented(r.op(), r.query());
        }
    }

    // ---- buckets -------------------------------------------------------------------------------

    private static String backendKey(String bucket, String key) {
        return bucket + "/" + key;
    }

    private boolean bucketExists(String bucket) {
        if (knownBuckets.contains(bucket)) {
            return true;
        }
        ListObjectsV2Response res = s3.listObjectsV2(b -> b.bucket(config.backendBucket()).prefix(bucket + "/").maxKeys(1));
        boolean exists = res.keyCount() != null && res.keyCount() > 0;
        if (exists) {
            knownBuckets.add(bucket);
        }
        return exists;
    }

    private void requireBucket(String bucket) {
        if (!bucketExists(bucket)) {
            throw new S3WireException(404, "NoSuchBucket", "The specified bucket does not exist");
        }
    }

    private void createBucket(Route r, HttpServletResponse resp) {
        if (!BUCKET_NAME.matcher(r.bucket()).matches()) {
            throw new S3WireException(400, "InvalidBucketName", "The specified bucket is not valid.");
        }
        if (bucketExists(r.bucket())) {
            throw new S3WireException(409, "BucketAlreadyOwnedByYou",
                    "Your previous request to create the named bucket succeeded and you already own it.");
        }
        s3.putObject(b -> b.bucket(config.backendBucket()).key(backendKey(r.bucket(), MARKER)).contentLength(0L),
                RequestBody.empty());
        knownBuckets.add(r.bucket());
        resp.setHeader("Location", "/" + r.bucket());
        resp.setStatus(200);
    }

    private void deleteBucket(Route r, HttpServletResponse resp) {
        requireBucket(r.bucket());
        ListObjectsV2Response res = s3.listObjectsV2(b -> b.bucket(config.backendBucket()).prefix(r.bucket() + "/").maxKeys(2));
        for (S3Object o : res.contents()) {
            if (!o.key().equals(backendKey(r.bucket(), MARKER))) {
                throw new S3WireException(409, "BucketNotEmpty", "The bucket you tried to delete is not empty");
            }
        }
        s3.deleteObject(b -> b.bucket(config.backendBucket()).key(backendKey(r.bucket(), MARKER)));
        knownBuckets.remove(r.bucket());
        resp.setStatus(204);
    }

    private void listBuckets(HttpServletResponse resp) throws IOException {
        List<String[]> buckets = new ArrayList<>();
        String token = null;
        do {
            String t = token;
            ListObjectsV2Response res = s3.listObjectsV2(b -> {
                b.bucket(config.backendBucket()).delimiter("/");
                if (t != null) {
                    b.continuationToken(t);
                }
            });
            for (CommonPrefix cp : res.commonPrefixes()) {
                String name = cp.prefix().substring(0, cp.prefix().length() - 1);
                Instant created = Instant.EPOCH;
                try {
                    created = s3.headObject(b -> b.bucket(config.backendBucket()).key(backendKey(name, MARKER))).lastModified();
                } catch (S3Exception ignored) {
                    // prefix without a marker (objects written another way): still listed
                }
                buckets.add(new String[] {name, S3Xml.iso(created)});
            }
            token = Boolean.TRUE.equals(res.isTruncated()) ? res.nextContinuationToken() : null;
        } while (token != null);
        writeXml(resp, 200, S3Xml.listBuckets(buckets, OWNER));
    }

    // ---- listing -------------------------------------------------------------------------------

    private void listObjects(Route r, HttpServletResponse resp) throws IOException {
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
        String bucketPrefix = r.bucket() + "/";
        if (max == 0) {
            requireBucket(r.bucket());
            writeXml(resp, 200, S3Xml.listObjects(new S3Xml.ListParams(r.bucket(), prefix, delimiter, 0, false, v2,
                    token, null, startAfter, startAfter, null, urlEncode), List.of(), List.of()));
            return;
        }
        final int maxKeys = max;
        ListObjectsV2Response res = s3.listObjectsV2(b -> {
            b.bucket(config.backendBucket()).prefix(bucketPrefix + prefix).maxKeys(maxKeys);
            if (delimiter != null && !delimiter.isEmpty()) {
                b.delimiter(delimiter);
            }
            if (token != null) {
                b.continuationToken(token);
            }
            if (startAfter != null && !startAfter.isEmpty()) {
                b.startAfter(bucketPrefix + startAfter);
            }
        });
        if (res.contents().isEmpty() && res.commonPrefixes().isEmpty() && token == null && !bucketExists(r.bucket())) {
            throw new S3WireException(404, "NoSuchBucket", "The specified bucket does not exist");
        }
        List<S3Xml.ObjectEntry> objects = new ArrayList<>();
        String lastKey = null;
        for (S3Object o : res.contents()) {
            String k = o.key().substring(bucketPrefix.length());
            lastKey = k;
            if (!k.equals(MARKER)) {
                objects.add(new S3Xml.ObjectEntry(k, o.lastModified(), o.eTag(), o.size() == null ? 0 : o.size()));
            }
        }
        List<String> prefixes = new ArrayList<>();
        for (CommonPrefix cp : res.commonPrefixes()) {
            String p = cp.prefix().substring(bucketPrefix.length());
            prefixes.add(p);
            if (lastKey == null || p.compareTo(lastKey) > 0) {
                lastKey = p;
            }
        }
        boolean truncated = Boolean.TRUE.equals(res.isTruncated());
        writeXml(resp, 200, S3Xml.listObjects(new S3Xml.ListParams(r.bucket(), prefix, delimiter, maxKeys, truncated,
                v2, token, truncated ? res.nextContinuationToken() : null, v2 ? startAfter : null, startAfter,
                truncated ? lastKey : null, urlEncode), objects, prefixes));
    }

    // ---- objects -------------------------------------------------------------------------------

    private void putObject(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        requireBucket(r.bucket());
        Body body = openBody(req);
        PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key())).contentLength(body.length());
        applyObjectHeaders(req, b::contentType, b::cacheControl, b::contentDisposition, b::contentEncoding,
                b::contentLanguage, b::metadata);
        String md5 = req.getHeader("Content-MD5");
        if (md5 != null) {
            b.contentMD5(md5);
        }
        PutObjectResponse res = s3.putObject(b.build(), RequestBody.fromInputStream(body.stream(), body.length()));
        if (!body.verifyDigest()) {
            try {
                s3.deleteObject(d -> d.bucket(config.backendBucket()).key(backendKey(r.bucket(), r.key())));
            } catch (RuntimeException e) {
                log.warn("s3wire: could not roll back object after payload-hash mismatch", e);
            }
            throw new S3WireException(400, "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.");
        }
        if (res.eTag() != null) {
            resp.setHeader("ETag", res.eTag());
        }
        resp.setStatus(200);
    }

    private void getObject(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        GetObjectRequest.Builder b = GetObjectRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key()));
        if (req.getHeader("Range") != null) {
            b.range(req.getHeader("Range"));
        }
        if (req.getHeader("If-Match") != null) {
            b.ifMatch(req.getHeader("If-Match"));
        }
        if (req.getHeader("If-None-Match") != null) {
            b.ifNoneMatch(req.getHeader("If-None-Match"));
        }
        try (ResponseInputStream<GetObjectResponse> in = s3.getObject(b.build())) {
            GetObjectResponse g = in.response();
            resp.setStatus(g.contentRange() != null ? 206 : 200);
            writeObjectHeaders(resp, g.contentLength(), g.contentType(), g.eTag(), g.lastModified(), g.contentRange(),
                    g.cacheControl(), g.contentDisposition(), g.contentEncoding(), g.contentLanguage(), g.metadata());
            in.transferTo(resp.getOutputStream());
        }
    }

    private void headObject(Route r, HttpServletRequest req, HttpServletResponse resp) {
        HeadObjectRequest.Builder b = HeadObjectRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key()));
        if (req.getHeader("If-Match") != null) {
            b.ifMatch(req.getHeader("If-Match"));
        }
        if (req.getHeader("If-None-Match") != null) {
            b.ifNoneMatch(req.getHeader("If-None-Match"));
        }
        HeadObjectResponse h = s3.headObject(b.build());
        resp.setStatus(200);
        writeObjectHeaders(resp, h.contentLength(), h.contentType(), h.eTag(), h.lastModified(), null,
                h.cacheControl(), h.contentDisposition(), h.contentEncoding(), h.contentLanguage(), h.metadata());
    }

    private static void writeObjectHeaders(HttpServletResponse resp, Long length, String type, String eTag,
            Instant modified, String range, String cache, String disposition, String encoding, String language,
            Map<String, String> metadata) {
        if (length != null) {
            resp.setContentLengthLong(length);
        }
        if (type != null) {
            resp.setContentType(type);
        }
        if (eTag != null) {
            resp.setHeader("ETag", eTag);
        }
        if (modified != null) {
            resp.setHeader("Last-Modified", S3Xml.httpDate(modified));
        }
        if (range != null) {
            resp.setHeader("Content-Range", range);
        }
        resp.setHeader("Accept-Ranges", "bytes");
        if (cache != null) {
            resp.setHeader("Cache-Control", cache);
        }
        if (disposition != null) {
            resp.setHeader("Content-Disposition", disposition);
        }
        if (encoding != null) {
            resp.setHeader("Content-Encoding", encoding);
        }
        if (language != null) {
            resp.setHeader("Content-Language", language);
        }
        if (metadata != null) {
            metadata.forEach((k, v) -> resp.setHeader("x-amz-meta-" + k, v));
        }
    }

    private void deleteObjects(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        requireBucket(r.bucket());
        byte[] xml = readSmallBody(req);
        S3Xml.Delete d = S3Xml.parseDelete(xml);
        if (d.keys().size() > 1000 || d.keys().isEmpty()) {
            throw new S3WireException(400, "MalformedXML", "DeleteObjects requires between 1 and 1000 keys.");
        }
        List<ObjectIdentifier> ids = new ArrayList<>();
        List<S3Xml.DeleteOutcome> errors = new ArrayList<>();
        for (String k : d.keys()) {
            if (k.equals(MARKER)) {
                errors.add(new S3Xml.DeleteOutcome(k, "InvalidArgument", "Key is reserved by s3wire"));
            } else {
                ids.add(ObjectIdentifier.builder().key(backendKey(r.bucket(), k)).build());
            }
        }
        List<String> deleted = new ArrayList<>();
        if (!ids.isEmpty()) {
            DeleteObjectsResponse res = s3.deleteObjects(DeleteObjectsRequest.builder().bucket(config.backendBucket())
                    .delete(Delete.builder().objects(ids).quiet(false).build()).build());
            int strip = r.bucket().length() + 1;
            res.deleted().forEach(x -> deleted.add(x.key().substring(strip)));
            res.errors().forEach(x -> errors.add(new S3Xml.DeleteOutcome(x.key().substring(strip), x.code(), x.message())));
        }
        writeXml(resp, 200, S3Xml.deleteResult(deleted, errors, d.quiet()));
    }

    private void copyObject(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        requireBucket(r.bucket());
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
        if (srcKey.equals(MARKER)) {
            throw new S3WireException(400, "InvalidArgument", "Key '" + MARKER + "' is reserved by s3wire");
        }
        requireBucket(srcBucket);
        CopyObjectRequest.Builder b = CopyObjectRequest.builder().sourceBucket(config.backendBucket())
                .sourceKey(backendKey(srcBucket, srcKey)).destinationBucket(config.backendBucket())
                .destinationKey(backendKey(r.bucket(), r.key()));
        if ("REPLACE".equalsIgnoreCase(req.getHeader("x-amz-metadata-directive"))) {
            b.metadataDirective(MetadataDirective.REPLACE);
            applyObjectHeaders(req, b::contentType, b::cacheControl, b::contentDisposition, b::contentEncoding,
                    b::contentLanguage, b::metadata);
        }
        CopyObjectResponse res = s3.copyObject(b.build());
        writeXml(resp, 200, S3Xml.copyResult(res.copyObjectResult().eTag(), res.copyObjectResult().lastModified()));
    }

    // ---- multipart -----------------------------------------------------------------------------

    private void createMultipart(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        requireBucket(r.bucket());
        CreateMultipartUploadRequest.Builder b = CreateMultipartUploadRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key()));
        applyObjectHeaders(req, b::contentType, b::cacheControl, b::contentDisposition, b::contentEncoding,
                b::contentLanguage, b::metadata);
        String id = s3.createMultipartUpload(b.build()).uploadId();
        writeXml(resp, 200, S3Xml.initiateMultipart(r.bucket(), r.key(), id));
    }

    private void uploadPart(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
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
        UploadPartRequest.Builder b = UploadPartRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key())).uploadId(r.query().get("uploadId")).partNumber(partNumber)
                .contentLength(body.length());
        String md5 = req.getHeader("Content-MD5");
        if (md5 != null) {
            b.contentMD5(md5);
        }
        UploadPartResponse res = s3.uploadPart(b.build(), RequestBody.fromInputStream(body.stream(), body.length()));
        if (!body.verifyDigest()) {
            throw new S3WireException(400, "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.");
        }
        if (res.eTag() != null) {
            resp.setHeader("ETag", res.eTag());
        }
        resp.setStatus(200);
    }

    private void completeMultipart(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        List<S3Xml.Part> parts = S3Xml.parseCompleteMultipart(readSmallBody(req));
        List<CompletedPart> cps = new ArrayList<>();
        for (S3Xml.Part p : parts) {
            cps.add(CompletedPart.builder().partNumber(p.number()).eTag(p.eTag()).build());
        }
        var res = s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key())).uploadId(r.query().get("uploadId"))
                .multipartUpload(CompletedMultipartUpload.builder().parts(cps).build()).build());
        knownBuckets.add(r.bucket());
        writeXml(resp, 200, S3Xml.completeMultipart(req.getRequestURL().toString(), r.bucket(), r.key(), res.eTag()));
    }

    // ---- request body / headers ----------------------------------------------------------------

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

    private static void applyObjectHeaders(HttpServletRequest req, java.util.function.Consumer<String> contentType,
            java.util.function.Consumer<String> cache, java.util.function.Consumer<String> disposition,
            java.util.function.Consumer<String> encoding, java.util.function.Consumer<String> language,
            java.util.function.Consumer<Map<String, String>> metadata) {
        if (req.getHeader("Content-Type") != null) {
            contentType.accept(req.getHeader("Content-Type"));
        }
        if (req.getHeader("Cache-Control") != null) {
            cache.accept(req.getHeader("Cache-Control"));
        }
        if (req.getHeader("Content-Disposition") != null) {
            disposition.accept(req.getHeader("Content-Disposition"));
        }
        String enc = req.getHeader("Content-Encoding");
        if (enc != null) {
            // aws-chunked is transfer framing, not a property of the stored object
            List<String> kept = new ArrayList<>();
            for (String e : enc.split(",")) {
                if (!e.trim().equalsIgnoreCase("aws-chunked") && !e.isBlank()) {
                    kept.add(e.trim());
                }
            }
            if (!kept.isEmpty()) {
                encoding.accept(String.join(",", kept));
            }
        }
        if (req.getHeader("Content-Language") != null) {
            language.accept(req.getHeader("Content-Language"));
        }
        Map<String, String> meta = new LinkedHashMap<>();
        for (String name : Collections.list(req.getHeaderNames())) {
            if (name.toLowerCase(Locale.ROOT).startsWith("x-amz-meta-")) {
                meta.put(name.substring("x-amz-meta-".length()).toLowerCase(Locale.ROOT), req.getHeader(name));
            }
        }
        if (!meta.isEmpty()) {
            metadata.accept(meta);
        }
    }
}
