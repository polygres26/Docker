package com.sayonora.warp.s3wire;

import com.google.gson.JsonObject;
import com.sayonora.warp.s3wire.ObjectStore.Attrs;
import com.sayonora.warp.s3wire.S3Model.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The complete Amazon S3 REST surface of Postgres mode: routing of every operation s3wire implements onto
 * {@link PostgresObjectStore}, with S3's headers, XML documents and error semantics. Proxy mode keeps the
 * smaller legacy handler in {@link S3WireServer}.
 */
final class S3Api {

    record Route(String op, boolean write, String bucket, String key, Map<String, String> query) {
    }

    private static final Pattern REGION = Pattern.compile("Credential=[^/]+/\\d{8}/([^/]+)/s3/aws4_request");
    private static final Set<String> STORAGE_CLASSES = Set.of("STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA",
            "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "OUTPOSTS", "SNOW",
            "EXPRESS_ONEZONE");

    /** bucket subresource query key -> operation suffix. */
    private static final Map<String, String> BUCKET_SUB = Map.ofEntries(Map.entry("acl", "Acl"),
            Map.entry("cors", "Cors"), Map.entry("lifecycle", "Lifecycle"), Map.entry("policy", "Policy"),
            Map.entry("tagging", "Tagging"), Map.entry("encryption", "Encryption"), Map.entry("logging", "Logging"),
            Map.entry("website", "Website"), Map.entry("notification", "Notification"),
            Map.entry("replication", "Replication"), Map.entry("requestPayment", "RequestPayment"),
            Map.entry("accelerate", "Accelerate"), Map.entry("publicAccessBlock", "PublicAccessBlock"),
            Map.entry("ownershipControls", "OwnershipControls"), Map.entry("object-lock", "ObjectLockConfiguration"),
            Map.entry("versioning", "Versioning"), Map.entry("analytics", "Analytics"), Map.entry("metrics", "Metrics"),
            Map.entry("inventory", "Inventory"), Map.entry("intelligent-tiering", "IntelligentTiering"),
            Map.entry("policyStatus", "PolicyStatus"));

    private final PostgresObjectStore store;
    private final S3WireConfig config;

    S3Api(PostgresObjectStore store, S3WireConfig config) {
        this.store = store;
        this.config = config;
    }

    // ---- routing -----------------------------------------------------------------------------------

    private static S3WireException notImplemented(String what) {
        return new S3WireException(501, "NotImplemented", "A header you provided implies functionality that is not "
                + "implemented: " + what);
    }

    private static S3WireException methodNotAllowed() {
        return new S3WireException(405, "MethodNotAllowed", "The specified method is not allowed against this resource.");
    }

    Route route(HttpServletRequest req, String bucket, String key, Map<String, String> q) {
        String m = req.getMethod();
        if (bucket == null || bucket.isEmpty()) {
            if ("GET".equals(m)) {
                return new Route("ListBuckets", false, null, null, q);
            }
            throw methodNotAllowed();
        }
        if (key.isEmpty()) {
            return bucketRoute(req, m, bucket, q);
        }
        return objectRoute(req, m, bucket, key, q);
    }

    private static String subresource(Map<String, String> q) {
        for (String k : BUCKET_SUB.keySet()) {
            if (q.containsKey(k)) {
                return k;
            }
        }
        return null;
    }

    private Route bucketRoute(HttpServletRequest req, String m, String bucket, Map<String, String> q) {
        String sub = subresource(q);
        switch (m) {
            case "PUT" -> {
                if (sub != null && !"policyStatus".equals(sub)) {
                    return new Route("PutBucket" + BUCKET_SUB.get(sub), true, bucket, null, q);
                }
                if (sub == null) {
                    return new Route("CreateBucket", true, bucket, null, q);
                }
            }
            case "GET" -> {
                if (q.containsKey("location")) {
                    return new Route("GetBucketLocation", false, bucket, null, q);
                }
                if (q.containsKey("versions")) {
                    return new Route("ListObjectVersions", false, bucket, null, q);
                }
                if (q.containsKey("uploads")) {
                    return new Route("ListMultipartUploads", false, bucket, null, q);
                }
                if (sub != null) {
                    boolean list = List.of("analytics", "metrics", "inventory", "intelligent-tiering").contains(sub)
                            && !q.containsKey("id");
                    return new Route((list ? "ListBucket" : "GetBucket") + BUCKET_SUB.get(sub) + (list ? "Configurations" : ""),
                            false, bucket, null, q);
                }
                if (q.containsKey("torrent") || q.containsKey("events") || q.containsKey("mfa")) {
                    throw notImplemented(q.keySet().iterator().next());
                }
                return new Route(q.containsKey("list-type") ? "ListObjectsV2" : "ListObjects", false, bucket, null, q);
            }
            case "HEAD" -> {
                return new Route("HeadBucket", false, bucket, null, q);
            }
            case "DELETE" -> {
                if (sub != null && !"versioning".equals(sub) && !"acl".equals(sub) && !"policyStatus".equals(sub)
                        && !"logging".equals(sub) && !"notification".equals(sub)) {
                    return new Route("DeleteBucket" + BUCKET_SUB.get(sub), true, bucket, null, q);
                }
                if (sub == null) {
                    return new Route("DeleteBucket", true, bucket, null, q);
                }
            }
            case "POST" -> {
                if (q.containsKey("delete")) {
                    return new Route("DeleteObjects", true, bucket, null, q);
                }
                String ct = req.getContentType();
                if (ct != null && ct.toLowerCase(Locale.ROOT).startsWith("multipart/form-data")) {
                    return new Route("PostObject", true, bucket, null, q);
                }
            }
            default -> {
            }
        }
        throw methodNotAllowed();
    }

    private Route objectRoute(HttpServletRequest req, String m, String bucket, String key, Map<String, String> q) {
        if (key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) {
            throw new S3WireException(400, "KeyTooLongError", "Your key is too long", null,
                    Map.of("MaxSizeAllowed", "1024", "Size", String.valueOf(key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length)));
        }
        boolean upload = q.containsKey("uploadId");
        switch (m) {
            case "GET" -> {
                if (q.containsKey("annotation")) {
                    boolean named = q.get("annotationName") != null && !q.get("annotationName").isEmpty();
                    return new Route(named ? "GetObjectAnnotation" : "ListObjectAnnotations", false, bucket, key, q);
                }
                if (q.containsKey("acl")) {
                    return new Route("GetObjectAcl", false, bucket, key, q);
                }
                if (q.containsKey("tagging")) {
                    return new Route("GetObjectTagging", false, bucket, key, q);
                }
                if (q.containsKey("attributes")) {
                    return new Route("GetObjectAttributes", false, bucket, key, q);
                }
                if (q.containsKey("retention")) {
                    return new Route("GetObjectRetention", false, bucket, key, q);
                }
                if (q.containsKey("legal-hold")) {
                    return new Route("GetObjectLegalHold", false, bucket, key, q);
                }
                if (q.containsKey("torrent")) {
                    throw notImplemented("torrent");
                }
                if (upload) {
                    return new Route("ListParts", false, bucket, key, q);
                }
                return new Route("GetObject", false, bucket, key, q);
            }
            case "HEAD" -> {
                if (q.containsKey("annotation")) {
                    boolean named = q.get("annotationName") != null && !q.get("annotationName").isEmpty();
                    return new Route(named ? "GetObjectAnnotation" : "ListObjectAnnotations", false, bucket, key, q);
                }
                return new Route("HeadObject", false, bucket, key, q);
            }
            case "PUT" -> {
                if (q.containsKey("annotation")) {
                    return new Route("PutObjectAnnotation", true, bucket, key, q);
                }
                if (q.containsKey("acl")) {
                    return new Route("PutObjectAcl", true, bucket, key, q);
                }
                if (q.containsKey("tagging")) {
                    return new Route("PutObjectTagging", true, bucket, key, q);
                }
                if (q.containsKey("retention")) {
                    return new Route("PutObjectRetention", true, bucket, key, q);
                }
                if (q.containsKey("legal-hold")) {
                    return new Route("PutObjectLegalHold", true, bucket, key, q);
                }
                if (upload && q.containsKey("partNumber")) {
                    return new Route(req.getHeader("x-amz-copy-source") != null ? "UploadPartCopy" : "UploadPart", true,
                            bucket, key, q);
                }
                return new Route(req.getHeader("x-amz-copy-source") != null ? "CopyObject" : "PutObject", true, bucket, key, q);
            }
            case "DELETE" -> {
                if (q.containsKey("annotation")) {
                    return new Route("DeleteObjectAnnotation", true, bucket, key, q);
                }
                if (q.containsKey("tagging")) {
                    return new Route("DeleteObjectTagging", true, bucket, key, q);
                }
                if (upload) {
                    return new Route("AbortMultipartUpload", true, bucket, key, q);
                }
                return new Route("DeleteObject", true, bucket, key, q);
            }
            case "POST" -> {
                if (q.containsKey("uploads")) {
                    return new Route("CreateMultipartUpload", true, bucket, key, q);
                }
                if (upload) {
                    return new Route("CompleteMultipartUpload", true, bucket, key, q);
                }
                if (q.containsKey("restore")) {
                    return new Route("RestoreObject", true, bucket, key, q);
                }
                if (q.containsKey("select")) {
                    return new Route("SelectObjectContent", false, bucket, key, q);
                }
            }
            default -> {
            }
        }
        throw methodNotAllowed();
    }

    // ---- helpers -----------------------------------------------------------------------------------

    static void xml(HttpServletResponse resp, int status, String body) throws IOException {
        byte[] bytes = S3Xml.utf8(body);
        resp.setStatus(status);
        resp.setContentType("application/xml");
        resp.setContentLength(bytes.length);
        resp.getOutputStream().write(bytes);
    }

    /** The region the request was signed for (credential scope), or the configured default. */
    private String signingRegion(HttpServletRequest req) {
        String auth = req.getHeader("Authorization");
        String src = auth != null ? auth : req.getQueryString() == null ? "" : "Credential="
                + S3SigV4Verifier.percentDecode(S3SigV4Verifier.parseQuery(req.getQueryString())
                .getOrDefault("X-Amz-Credential", ""), true);
        Matcher m = REGION.matcher(src);
        return m.find() ? m.group(1) : "us-east-1";
    }

    private static Map<String, String> headerMap(HttpServletRequest req) {
        Map<String, String> h = new LinkedHashMap<>();
        for (String n : Collections.list(req.getHeaderNames())) {
            h.put(n.toLowerCase(Locale.ROOT), req.getHeader(n));
        }
        return h;
    }

    private static Attrs attrsOf(HttpServletRequest req) {
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
        return new Attrs(req.getHeader("Content-Type"), req.getHeader("Cache-Control"),
                req.getHeader("Content-Disposition"), encoding, req.getHeader("Content-Language"),
                req.getHeader("Expires"), meta);
    }

    /** PublicAccessBlock.BlockPublicAcls: a request that would set a public ACL is rejected. */
    private void enforceBlockPublicAcls(String bucket, String aclXml) {
        if (S3Cfg.aclIsPublic(aclXml) && S3Cfg.pabFlag(store.config(bucket, "publicAccessBlock"), "BlockPublicAcls")) {
            throw new S3WireException(403, "AccessDenied",
                    "Access Denied because public ACLs are blocked by the BlockPublicAcls block public access setting.");
        }
    }

    private JsonObject extraOf(HttpServletRequest req, BucketInfo b, boolean withTags) {
        JsonObject x = new JsonObject();
        if (withTags && req.getHeader("x-amz-tagging") != null) {
            Map<String, String> tags = S3Cfg.parseTagHeader(req.getHeader("x-amz-tagging"));
            if (!tags.isEmpty()) {
                JsonObject t = new JsonObject();
                tags.forEach(t::addProperty);
                x.add("tags", t);
            }
        }
        String acl = S3Cfg.aclFromRequest(req::getHeader, null);
        if (acl != null) {
            enforceBlockPublicAcls(b.name(), acl);
            if ("BucketOwnerEnforced".equals(b.ownership()) && !"bucket-owner-full-control".equals(req.getHeader("x-amz-acl"))) {
                throw new S3WireException(400, "AccessControlListNotSupported", "The bucket does not allow ACLs");
            }
            x.addProperty("acl", acl);
        }
        if (req.getHeader("x-amz-server-side-encryption-customer-algorithm") != null) {
            if (!req.isSecure()) {
                throw new S3WireException(400, "InvalidRequest", "Requests specifying Server Side Encryption with "
                        + "Customer provided keys must be made over a secure connection.");
            }
            throw notImplemented("SSE-C");
        }
        String sse = req.getHeader("x-amz-server-side-encryption");
        if (sse != null) {
            if (!List.of("AES256", "aws:kms", "aws:kms:dsse").contains(sse)) {
                throw new S3WireException(400, "InvalidArgument", "Server Side Encryption with AWS KMS managed key "
                        + "requires HTTP header x-amz-server-side-encryption : aws:kms", null,
                        Map.of("ArgumentName", "x-amz-server-side-encryption", "ArgumentValue", sse));
            }
            x.addProperty("sse", sse);
            String kms = req.getHeader("x-amz-server-side-encryption-aws-kms-key-id");
            if (kms != null) {
                x.addProperty("kms", kms);
            }
        }
        String sc = req.getHeader("x-amz-storage-class");
        if (sc != null) {
            if (!STORAGE_CLASSES.contains(sc)) {
                throw new S3WireException(400, "InvalidStorageClass", "The storage class you specified is not valid");
            }
            if (!sc.equals("STANDARD")) {
                x.addProperty("sc", sc);
            }
        }
        String wr = req.getHeader("x-amz-website-redirect-location");
        if (wr != null) {
            x.addProperty("wr", wr);
        }
        String mode = req.getHeader("x-amz-object-lock-mode");
        String until = req.getHeader("x-amz-object-lock-retain-until-date");
        String hold = req.getHeader("x-amz-object-lock-legal-hold");
        if (mode != null || until != null || hold != null) {
            if (!b.objectLock()) {
                throw new S3WireException(400, "InvalidRequest", "Bucket is missing Object Lock Configuration");
            }
            if (mode != null || until != null) {
                if (mode == null || until == null || !List.of("GOVERNANCE", "COMPLIANCE").contains(mode)) {
                    throw new S3WireException(400, "InvalidArgument",
                            "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied");
                }
                try {
                    x.addProperty("lu", Instant.parse(until).toString());
                } catch (RuntimeException e) {
                    throw new S3WireException(400, "InvalidArgument", "The retain until date must be provided in ISO 8601 format");
                }
                x.addProperty("lm", mode);
            }
            if (hold != null) {
                if (!List.of("ON", "OFF").contains(hold)) {
                    throw new S3WireException(400, "InvalidArgument", "Legal Hold must be ON or OFF");
                }
                x.addProperty("lh", hold);
            }
        } else if (b.objectLock()) {
            String cfg = store.config(b.name(), "object-lock");
            if (cfg != null) {
                String[] d = S3Cfg.parseObjectLockConfig(S3Xml.utf8(cfg));
                Instant until2 = S3Cfg.defaultRetentionUntil(d, Instant.now());
                if (until2 != null) {
                    x.addProperty("lm", d[0]);
                    x.addProperty("lu", until2.toString());
                }
            }
        }
        return x;
    }

    /** Headers describing an object version, shared by GET, HEAD and the write responses. */
    private void writeMetaHeaders(HttpServletResponse resp, Meta m, BucketInfo b, boolean checksumMode) {
        Attrs a = m.attrs();
        if (m.eTag() != null) {
            resp.setHeader("ETag", m.eTag());
        }
        if (m.lastModified() != null) {
            resp.setHeader("Last-Modified", S3Xml.httpDate(m.lastModified()));
        }
        if (b != null && b.versioned() && m.versionId() != null) {
            resp.setHeader("x-amz-version-id", m.versionId());
        }
        String tags = m.tagHeaderCount();
        if (tags != null) {
            resp.setHeader("x-amz-tagging-count", tags);
        }
        resp.setHeader("x-amz-server-side-encryption", m.str("sse") != null ? m.str("sse") : "AES256");
        if (m.str("kms") != null) {
            resp.setHeader("x-amz-server-side-encryption-aws-kms-key-id", m.str("kms"));
        }
        if (m.str("sc") != null) {
            resp.setHeader("x-amz-storage-class", m.str("sc"));
        }
        if (m.str("wr") != null) {
            resp.setHeader("x-amz-website-redirect-location", m.str("wr"));
        }
        if (m.str("lm") != null) {
            resp.setHeader("x-amz-object-lock-mode", m.str("lm"));
            resp.setHeader("x-amz-object-lock-retain-until-date", m.str("lu"));
        }
        if (m.str("lh") != null) {
            resp.setHeader("x-amz-object-lock-legal-hold", m.str("lh"));
        }
        if (a.metadata() != null) {
            a.metadata().forEach((k, v) -> resp.setHeader("x-amz-meta-" + k, v));
        }
        if (checksumMode) {
            checksumHeaders(resp, m);
        }
    }

    private static void checksumHeaders(HttpServletResponse resp, Meta m) {
        for (String algo : Checksums.ALL) {
            String v = m.checksums().get(algo);
            if (v != null) {
                resp.setHeader(Checksums.headerName(algo), v);
            }
        }
        if (!m.checksums().isEmpty() && m.checksumType() != null) {
            resp.setHeader("x-amz-checksum-type", m.checksumType());
        }
    }

    private void writeObjectHeaders(HttpServletResponse resp, Meta m, BucketInfo b, long length, String range,
            boolean checksumMode, Map<String, String> q) {
        Attrs a = m.attrs();
        resp.setContentLengthLong(length);
        String ct = q.get("response-content-type");
        resp.setContentType(ct != null ? ct : a.contentType() != null ? a.contentType() : "binary/octet-stream");
        writeMetaHeaders(resp, m, b, checksumMode && range == null);
        if (range != null) {
            resp.setHeader("Content-Range", range);
        }
        resp.setHeader("Accept-Ranges", "bytes");
        override(resp, "Cache-Control", q.get("response-cache-control"), a.cacheControl());
        override(resp, "Content-Disposition", q.get("response-content-disposition"), a.contentDisposition());
        override(resp, "Content-Encoding", q.get("response-content-encoding"), a.contentEncoding());
        override(resp, "Content-Language", q.get("response-content-language"), a.contentLanguage());
        override(resp, "Expires", q.get("response-expires"), a.expires());
        if (m.multipart()) {
            resp.setHeader("x-amz-mp-parts-count", String.valueOf(m.parts().size()));
        }
    }

    private static void override(HttpServletResponse resp, String header, String override, String stored) {
        String v = override != null ? override : stored;
        if (v != null) {
            resp.setHeader(header, v);
        }
    }

    private static boolean checksumMode(HttpServletRequest req) {
        return "ENABLED".equalsIgnoreCase(req.getHeader("x-amz-checksum-mode"));
    }

    private static ObjectStore.Conditions conditions(HttpServletRequest req) {
        return new ObjectStore.Conditions(req.getHeader("If-Match"), req.getHeader("If-None-Match"),
                req.getHeader("If-Modified-Since"), req.getHeader("If-Unmodified-Since"));
    }

    private static ObjectStore.Conditions copyConditions(HttpServletRequest req) {
        return new ObjectStore.Conditions(req.getHeader("x-amz-copy-source-if-match"),
                req.getHeader("x-amz-copy-source-if-none-match"), req.getHeader("x-amz-copy-source-if-modified-since"),
                req.getHeader("x-amz-copy-source-if-unmodified-since"));
    }

    /** Parsed {@code x-amz-copy-source}: bucket, key and optional versionId. */
    private record CopySource(String bucket, String key, String versionId) {
    }

    private static CopySource copySource(HttpServletRequest req) {
        String src = req.getHeader("x-amz-copy-source");
        String version = null;
        int qm = src.indexOf('?');
        if (qm >= 0) {
            String q = src.substring(qm + 1);
            src = src.substring(0, qm);
            version = S3SigV4Verifier.parseQuery(q).get("versionId");
        }
        src = S3SigV4Verifier.percentDecode(src.startsWith("/") ? src.substring(1) : src, false);
        int slash = src.indexOf('/');
        if (slash <= 0 || slash == src.length() - 1) {
            throw new S3WireException(400, "InvalidArgument",
                    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey");
        }
        PostgresObjectStore.checkVersionId(version);
        return new CopySource(src.substring(0, slash), src.substring(slash + 1), version);
    }

    // ---- execution ---------------------------------------------------------------------------------

    void execute(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Map<String, String> q = r.query();
        String bucket = r.bucket();
        switch (r.op()) {
            case "ListBuckets" -> listBuckets(q, resp);
            case "CreateBucket" -> createBucket(r, req, resp);
            case "HeadBucket" -> {
                BucketInfo b = store.requireBucket(bucket);
                resp.setHeader("x-amz-bucket-region", b.region() == null ? "us-east-1" : b.region());
                resp.setStatus(200);
            }
            case "DeleteBucket" -> {
                store.deleteBucket(bucket);
                resp.setStatus(204);
            }
            case "GetBucketLocation" -> {
                BucketInfo b = store.requireBucket(bucket);
                xml(resp, 200, S3Xml.locationRich(b.region()));
            }
            case "GetBucketVersioning" -> {
                BucketInfo b = store.requireBucket(bucket);
                xml(resp, 200, S3Cfg.versioningXml(b.versioning(), false));
            }
            case "PutBucketVersioning" -> {
                byte[] body = S3Body.readSmall(req);
                store.setVersioning(bucket, S3Cfg.parseVersioning(body));
                resp.setStatus(200);
            }
            case "ListObjects", "ListObjectsV2" -> listObjects(r, resp);
            case "ListObjectVersions" -> listVersions(r, resp);
            case "ListMultipartUploads" -> listUploads(r, resp);
            case "DeleteObjects" -> deleteObjects(r, req, resp);
            case "PostObject" -> postObject(r.bucket(), req, resp);
            case "PutObject" -> putObject(r, req, resp);
            case "GetObject" -> getObject(r, req, resp, false);
            case "HeadObject" -> getObject(r, req, resp, true);
            case "DeleteObject" -> deleteObject(r, req, resp);
            case "CopyObject" -> copyObject(r, req, resp);
            case "GetObjectAttributes" -> objectAttributes(r, req, resp);
            case "CreateMultipartUpload" -> createMultipart(r, req, resp);
            case "UploadPart" -> uploadPart(r, req, resp);
            case "UploadPartCopy" -> uploadPartCopy(r, req, resp);
            case "CompleteMultipartUpload" -> completeMultipart(r, req, resp);
            case "AbortMultipartUpload" -> {
                store.requireBucket(bucket);
                store.abortMultipart(bucket, r.key(), q.get("uploadId"));
                resp.setStatus(204);
            }
            case "ListParts" -> listParts(r, resp);
            case "GetObjectTagging", "PutObjectTagging", "DeleteObjectTagging" -> objectTagging(r, req, resp);
            case "GetObjectAcl", "PutObjectAcl" -> objectAcl(r, req, resp);
            case "GetObjectRetention", "PutObjectRetention", "GetObjectLegalHold", "PutObjectLegalHold" -> objectLock(r, req, resp);
            case "PutObjectAnnotation", "GetObjectAnnotation", "ListObjectAnnotations", "DeleteObjectAnnotation" ->
                    annotation(r, req, resp);
            case "RestoreObject" -> {
                Meta m = store.head(bucket, new GetIn(bucket, r.key(), q.get("versionId"), 0, ObjectStore.Conditions.NONE, null));
                throw new S3WireException(409, "InvalidObjectState", "Restore is not allowed for the object's current storage class",
                        null, Map.of("StorageClass", m.str("sc") == null ? "STANDARD" : m.str("sc")));
            }
            case "SelectObjectContent" -> selectObject(r, req, resp);
            default -> {
                if (r.op().startsWith("GetBucket") || r.op().startsWith("PutBucket") || r.op().startsWith("DeleteBucket")
                        || r.op().startsWith("ListBucket")) {
                    bucketConfig(r, req, resp);
                } else {
                    throw notImplemented(r.op());
                }
            }
        }
    }

    // ---- buckets -----------------------------------------------------------------------------------

    private void listBuckets(Map<String, String> q, HttpServletResponse resp) throws IOException {
        String prefix = q.get("prefix");
        String region = q.get("bucket-region");
        String token = q.get("continuation-token");
        int max = 10000;
        if (q.containsKey("max-buckets")) {
            try {
                max = Integer.parseInt(q.get("max-buckets"));
            } catch (NumberFormatException e) {
                throw new S3WireException(400, "InvalidArgument", "Provided max-buckets not an integer or within integer range");
            }
            if (max < 1 || max > 10000) {
                throw new S3WireException(400, "InvalidArgument", "Argument max-buckets must be an integer between 1 and 10000");
            }
        }
        List<BucketInfo> out = new ArrayList<>();
        boolean truncated = false;
        String next = null;
        for (BucketInfo b : store.listBuckets()) {
            if (prefix != null && !b.name().startsWith(prefix)) {
                continue;
            }
            if (region != null && !region.equals(b.region() == null ? "us-east-1" : b.region())) {
                continue;
            }
            if (token != null && b.name().compareTo(token) <= 0) {
                continue;
            }
            if (out.size() == max) {
                truncated = true;
                next = out.get(out.size() - 1).name();
                break;
            }
            out.add(b);
        }
        xml(resp, 200, S3Xml.listBucketsRich(out, prefix, token, truncated, next));
    }

    private static final Pattern REGION_NAME = Pattern.compile("^[a-z]{2}(-[a-z]+)+-\\d+$");

    private void createBucket(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String signing = signingRegion(req);
        byte[] body = S3Body.readSmall(req);
        String lc = S3Xml.parseLocationConstraint(body);
        String region;
        if (lc == null || lc.isEmpty()) {
            region = signing;
        } else {
            if (lc.equals("us-east-1")) {
                throw new S3WireException(400, "InvalidLocationConstraint", "The specified location-constraint is not valid",
                        null, Map.of("LocationConstraint", lc));
            }
            if (!REGION_NAME.matcher(lc).matches()) {
                throw new S3WireException(400, "InvalidLocationConstraint", "The specified location-constraint is not valid",
                        null, Map.of("LocationConstraint", lc));
            }
            if (!signing.equals("us-east-1") && !signing.equals(lc)) {
                throw new S3WireException(400, "IllegalLocationConstraintException", "The " + lc
                        + " location constraint is incompatible for the region specific endpoint this request was sent to.");
            }
            region = lc;
        }
        String ownership = req.getHeader("x-amz-object-ownership");
        if (ownership != null && !List.of("BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced").contains(ownership)) {
            throw new S3WireException(400, "InvalidArgument", "Invalid x-amz-object-ownership header");
        }
        boolean lock = "true".equalsIgnoreCase(req.getHeader("x-amz-bucket-object-lock-enabled"));
        String acl = S3Cfg.aclFromRequest(req::getHeader, null);
        if (acl != null && "BucketOwnerEnforced".equals(ownership)) {
            throw new S3WireException(400, "InvalidBucketAclWithObjectOwnership",
                    "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting");
        }
        store.createBucket(r.bucket(), region, lock, ownership, acl);
        resp.setHeader("Location", "/" + r.bucket());
        resp.setStatus(200);
    }

    // ---- bucket subresources -----------------------------------------------------------------------

    private static final Map<String, String[]> MISSING = Map.ofEntries(
            Map.entry("cors", new String[] {"NoSuchCORSConfiguration", "The CORS configuration does not exist"}),
            Map.entry("lifecycle", new String[] {"NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist"}),
            Map.entry("policy", new String[] {"NoSuchBucketPolicy", "The bucket policy does not exist"}),
            Map.entry("tagging", new String[] {"NoSuchTagSet", "The TagSet does not exist"}),
            Map.entry("encryption", new String[] {"ServerSideEncryptionConfigurationNotFoundError",
                    "The server side encryption configuration was not found"}),
            Map.entry("website", new String[] {"NoSuchWebsiteConfiguration",
                    "The specified bucket does not have a website configuration"}),
            Map.entry("replication", new String[] {"ReplicationConfigurationNotFoundError",
                    "The replication configuration was not found"}),
            Map.entry("publicAccessBlock", new String[] {"NoSuchPublicAccessBlockConfiguration",
                    "The public access block configuration was not found"}),
            Map.entry("ownershipControls", new String[] {"OwnershipControlsNotFoundError",
                    "The bucket ownership controls were not found"}),
            Map.entry("object-lock", new String[] {"ObjectLockConfigurationNotFoundError",
                    "Object Lock configuration does not exist for this bucket"}));

    private static final Map<String, String> ROOTS = Map.of("logging", "BucketLoggingStatus", "website",
            "WebsiteConfiguration", "notification", "NotificationConfiguration", "replication",
            "ReplicationConfiguration", "requestPayment", "RequestPaymentConfiguration", "accelerate",
            "AccelerateConfiguration");
    private static final Map<String, String> FAMILY_ROOT = Map.of("analytics", "AnalyticsConfiguration", "metrics",
            "MetricsConfiguration", "inventory", "InventoryConfiguration", "intelligent-tiering",
            "IntelligentTieringConfiguration");
    private static final Map<String, String> FAMILY_LIST = Map.of("analytics", "ListBucketAnalyticsConfigurationResult",
            "metrics", "ListBucketMetricsConfigurationResult", "inventory", "ListBucketInventoryConfigurationResult",
            "intelligent-tiering", "ListBucketIntelligentTieringConfigurationsOutput");
    private static final Map<String, String> EMPTY_DEFAULT = Map.of("logging",
            "<BucketLoggingStatus xmlns=\"" + S3Xml.NS + "\"/>", "notification",
            "<NotificationConfiguration xmlns=\"" + S3Xml.NS + "\"/>", "accelerate",
            "<AccelerateConfiguration xmlns=\"" + S3Xml.NS + "\"/>", "requestPayment",
            "<RequestPaymentConfiguration xmlns=\"" + S3Xml.NS + "\"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>");

    private static String subOf(String op) {
        String s = op;
        for (String pre : new String[] {"ListBucket", "GetBucket", "PutBucket", "DeleteBucket"}) {
            if (s.startsWith(pre)) {
                s = s.substring(pre.length());
                break;
            }
        }
        if (s.endsWith("Configurations")) {
            s = s.substring(0, s.length() - "Configurations".length());
        }
        for (Map.Entry<String, String> e : BUCKET_SUB.entrySet()) {
            if (e.getValue().equals(s)) {
                return e.getKey();
            }
        }
        return null;
    }

    private void bucketConfig(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String bucket = r.bucket();
        String sub = subOf(r.op());
        if (sub == null) {
            throw notImplemented(r.op());
        }
        Map<String, String> q = r.query();
        boolean family = FAMILY_ROOT.containsKey(sub);
        String kind = family ? sub + ":" + q.getOrDefault("id", "") : sub;
        if (r.op().startsWith("ListBucket")) {
            Map<String, String> all = store.configFamily(bucket, sub);
            StringBuilder sb = new StringBuilder(S3Xml.DECL + "<" + FAMILY_LIST.get(sub) + " xmlns=\"" + S3Xml.NS + "\">");
            S3Xml.tag(sb, "IsTruncated", "false");
            for (String body : all.values()) {
                sb.append(stripDecl(body));
            }
            xml(resp, 200, sb.append("</").append(FAMILY_LIST.get(sub)).append('>').toString());
            return;
        }
        if (family && (q.get("id") == null || q.get("id").isEmpty())) {
            throw new S3WireException(400, "InvalidArgument", "Argument id is required");
        }
        if (r.op().startsWith("Put")) {
            byte[] body = S3Body.readSmall(req);
            String stored = new String(body, java.nio.charset.StandardCharsets.UTF_8);
            switch (sub) {
                case "cors" -> S3Cfg.parseCors(body);
                case "lifecycle" -> S3Cfg.validateLifecycle(body);
                case "policy" -> validatePolicy(bucket, stored);
                case "tagging" -> stored = S3Cfg.taggingXml(S3Cfg.parseTaggingXml(body, 50));
                case "encryption" -> S3Cfg.validateEncryption(body);
                case "publicAccessBlock" -> stored = S3Cfg.validatePublicAccessBlock(body);
                case "ownershipControls" -> store.setOwnership(bucket, S3Cfg.validateOwnership(body));
                case "acl" -> {
                    String acl = S3Cfg.aclFromRequest(req::getHeader, body);
                    if (acl == null) {
                        throw new S3WireException(400, "MissingSecurityHeader", "Your request was missing a required header");
                    }
                    BucketInfo bi = store.requireBucket(bucket);
                    if ("BucketOwnerEnforced".equals(bi.ownership())) {
                        throw new S3WireException(400, "AccessControlListNotSupported", "The bucket does not allow ACLs");
                    }
                    enforceBlockPublicAcls(bucket, acl);
                    stored = acl;
                }
                case "object-lock" -> {
                    S3Cfg.parseObjectLockConfig(body);
                    BucketInfo bi = store.requireBucket(bucket);
                    if (!"Enabled".equals(bi.versioning())) {
                        throw new S3WireException(409, "InvalidBucketState",
                                "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration");
                    }
                    store.enableObjectLock(bucket, null);
                }
                case "replication" -> {
                    S3Cfg.validateRoot(body, ROOTS.get(sub));
                    if (!"Enabled".equals(store.requireBucket(bucket).versioning())) {
                        throw new S3WireException(400, "InvalidRequest",
                                "Versioning must be 'Enabled' on the bucket to apply a replication configuration");
                    }
                }
                default -> {
                    String root = ROOTS.get(sub);
                    if (root == null) {
                        root = FAMILY_ROOT.get(sub);
                    }
                    S3Cfg.validateRoot(body, root);
                    if (family) {
                        String id = S3Xml.text(S3Xml.parse(body), "Id");
                        if (id == null || !id.trim().equals(q.get("id"))) {
                            throw new S3WireException(400, "IllegalArgument", "ID in the request body must match the ID in the query string");
                        }
                    }
                }
            }
            store.putConfig(bucket, kind, stored);
            if ("lifecycle".equals(sub)) {
                String min = req.getHeader("x-amz-transition-default-minimum-object-size");
                if (min != null && !List.of("varies_by_storage_class", "all_storage_classes_128K").contains(min)) {
                    throw new S3WireException(400, "InvalidArgument", "Invalid transition default minimum object size");
                }
                store.putConfig(bucket, "lifecycle.minsize", min == null ? "all_storage_classes_128K" : min);
                resp.setHeader("x-amz-transition-default-minimum-object-size", min == null ? "all_storage_classes_128K" : min);
            }
            resp.setStatus("policy".equals(sub) || "tagging".equals(sub) ? 204 : 200);
            return;
        }
        if (r.op().startsWith("Delete")) {
            boolean had = store.deleteConfig(bucket, kind);
            if ("lifecycle".equals(sub)) {
                store.deleteConfig(bucket, "lifecycle.minsize");
            }
            if (!had && (MISSING.containsKey(sub) && !"tagging".equals(sub) && !"policy".equals(sub))) {
                // deleting a missing configuration is idempotent for most kinds
                resp.setStatus(204);
                return;
            }
            resp.setStatus(204);
            return;
        }
        // Get
        String body = store.config(bucket, kind);
        if (body == null) {
            switch (sub) {
                case "acl" -> {
                    xml(resp, 200, S3Cfg.defaultAclXml());
                    return;
                }
                case "policyStatus" -> {
                    xml(resp, 200, S3Xml.DECL + "<PolicyStatus xmlns=\"" + S3Xml.NS + "\"><IsPublic>false</IsPublic></PolicyStatus>");
                    return;
                }
                default -> {
                }
            }
            if ("policyStatus".equals(sub)) {
                return;
            }
            String def = EMPTY_DEFAULT.get(sub);
            if (def != null) {
                xml(resp, 200, S3Xml.DECL + def);
                return;
            }
            if ("object-lock".equals(sub) && store.requireBucket(bucket).objectLock()) {
                xml(resp, 200, S3Xml.DECL + "<ObjectLockConfiguration xmlns=\"" + S3Xml.NS
                        + "\"><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>");
                return;
            }
            String[] miss = MISSING.get(sub);
            if (miss == null) {
                miss = new String[] {"NoSuchConfiguration", "The specified configuration does not exist."};
            }
            throw new S3WireException(404, miss[0], miss[1], null, Map.of("BucketName", bucket));
        }
        if ("policy".equals(sub)) {
            resp.setStatus(200);
            resp.setContentType("application/json");
            byte[] b = S3Xml.utf8(body);
            resp.setContentLength(b.length);
            resp.getOutputStream().write(b);
            return;
        }
        if ("lifecycle".equals(sub)) {
            String min = store.config(bucket, "lifecycle.minsize");
            resp.setHeader("x-amz-transition-default-minimum-object-size", min == null ? "all_storage_classes_128K" : min);
        }
        xml(resp, 200, body.startsWith("<?xml") ? body : S3Xml.DECL + body);
    }

    private static String stripDecl(String xml) {
        return xml.startsWith("<?xml") ? xml.substring(xml.indexOf("?>") + 2).trim() : xml;
    }

    private void validatePolicy(String bucket, String json) {
        try {
            JsonObject o = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
            if (!o.has("Statement")) {
                throw new IllegalArgumentException("no Statement");
            }
            String pab = store.config(bucket, "publicAccessBlock");
            if (S3Cfg.pabFlag(pab, "BlockPublicPolicy") && policyIsPublic(json)) {
                throw new S3WireException(403, "AccessDenied", "User: is not authorized to perform: s3:PutBucketPolicy "
                        + "because public policies are blocked by the BlockPublicPolicy block public access setting.");
            }
        } catch (S3WireException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new S3WireException(400, "MalformedPolicy", "Policy has invalid resource");
        }
    }

    /** Coarse check: an Allow statement whose Principal is "*" (or {"AWS": "*"}). */
    static boolean policyIsPublic(String json) {
        try {
            com.google.gson.JsonElement st = com.google.gson.JsonParser.parseString(json).getAsJsonObject().get("Statement");
            List<com.google.gson.JsonElement> list = new ArrayList<>();
            if (st.isJsonArray()) {
                st.getAsJsonArray().forEach(list::add);
            } else {
                list.add(st);
            }
            for (com.google.gson.JsonElement e : list) {
                JsonObject s = e.getAsJsonObject();
                if (!"Allow".equals(s.has("Effect") ? s.get("Effect").getAsString() : "")) {
                    continue;
                }
                com.google.gson.JsonElement p = s.get("Principal");
                if (p == null) {
                    continue;
                }
                if (p.isJsonPrimitive() && "*".equals(p.getAsString())) {
                    return true;
                }
                if (p.isJsonObject() && p.getAsJsonObject().has("AWS")) {
                    com.google.gson.JsonElement a = p.getAsJsonObject().get("AWS");
                    if (a.isJsonPrimitive() && "*".equals(a.getAsString())) {
                        return true;
                    }
                }
            }
        } catch (RuntimeException e) {
            return false;
        }
        return false;
    }

    // ---- listing -----------------------------------------------------------------------------------

    private static int maxKeys(Map<String, String> q, String name) {
        int max = 1000;
        if (q.containsKey(name)) {
            try {
                max = Integer.parseInt(q.get(name));
            } catch (NumberFormatException e) {
                throw new S3WireException(400, "InvalidArgument", "Provided " + name + " not an integer or within integer range",
                        null, Map.of("ArgumentName", name, "ArgumentValue", q.get(name)));
            }
            if (max < 0) {
                throw new S3WireException(400, "InvalidArgument", "Argument " + name
                        + " must be an integer between 0 and 2147483647", null,
                        Map.of("ArgumentName", name, "ArgumentValue", q.get(name)));
            }
            max = Math.min(max, 1000);
        }
        return max;
    }

    private static boolean urlEncoding(Map<String, String> q) {
        String e = q.get("encoding-type");
        if (e != null && !e.equals("url")) {
            throw new S3WireException(400, "InvalidArgument", "Invalid Encoding Method specified in Request", null,
                    Map.of("ArgumentName", "encoding-type", "ArgumentValue", e));
        }
        return e != null;
    }

    private void listObjects(Route r, HttpServletResponse resp) throws IOException {
        Map<String, String> q = r.query();
        boolean v2 = "ListObjectsV2".equals(r.op());
        if (v2 && !"2".equals(q.get("list-type"))) {
            throw new S3WireException(400, "InvalidArgument", "Invalid list-type: " + q.get("list-type"));
        }
        String prefix = q.getOrDefault("prefix", "");
        String delimiter = q.get("delimiter");
        boolean urlEncode = urlEncoding(q);
        int max = maxKeys(q, "max-keys");
        String startAfter = v2 ? q.get("start-after") : q.get("marker");
        String token = v2 ? q.get("continuation-token") : null;
        if (v2 && q.containsKey("continuation-token") && token.isEmpty()) {
            throw new S3WireException(400, "InvalidArgument", "The continuation token provided is incorrect");
        }
        boolean owner = !v2 || "true".equalsIgnoreCase(q.get("fetch-owner"));
        if (max == 0) {
            store.requireBucket(r.bucket());
            xml(resp, 200, S3Xml.listObjectsRich(new S3Xml.ListParams(r.bucket(), prefix, delimiter, 0, false, v2, token,
                    null, startAfter, startAfter, null, urlEncode), List.of(), List.of(), owner));
            return;
        }
        ObjectStore.ListResult res = store.list(new ObjectStore.ListRequest(r.bucket(), prefix, delimiter, max, token,
                startAfter, v2));
        xml(resp, 200, S3Xml.listObjectsRich(new S3Xml.ListParams(r.bucket(), prefix, delimiter, max, res.truncated(),
                v2, token, res.truncated() ? res.nextToken() : null, v2 ? startAfter : null, startAfter,
                res.truncated() ? res.lastKey() : null, urlEncode), res.objects(), res.commonPrefixes(), owner));
    }

    private void listVersions(Route r, HttpServletResponse resp) throws IOException {
        Map<String, String> q = r.query();
        boolean urlEncode = urlEncoding(q);
        int max = maxKeys(q, "max-keys");
        String keyMarker = q.get("key-marker");
        String verMarker = q.get("version-id-marker");
        if (verMarker != null && !verMarker.isEmpty()) {
            PostgresObjectStore.checkVersionId(verMarker);
            if (keyMarker == null || keyMarker.isEmpty()) {
                throw new S3WireException(400, "InvalidArgument", "A version-id marker cannot be specified without a key marker.");
            }
        }
        VersionsListing l = max == 0 && store.bucketExists(r.bucket())
                ? new VersionsListing(List.of(), List.of(), false, null, null)
                : store.listVersions(r.bucket(), q.getOrDefault("prefix", ""), q.get("delimiter"), keyMarker, verMarker, max);
        xml(resp, 200, S3Xml.listVersions(r.bucket(), q.getOrDefault("prefix", ""), q.get("delimiter"), keyMarker,
                verMarker, max, urlEncode, l));
    }

    private void listUploads(Route r, HttpServletResponse resp) throws IOException {
        Map<String, String> q = r.query();
        boolean urlEncode = urlEncoding(q);
        int max = maxKeys(q, "max-uploads");
        String keyMarker = q.get("key-marker");
        String upMarker = q.get("upload-id-marker");
        UploadsListing l = store.listUploads(r.bucket(), q.getOrDefault("prefix", ""), q.get("delimiter"), keyMarker, upMarker,
                Math.max(max, 1));
        if (max == 0) {
            l = new UploadsListing(List.of(), List.of(), false, null, null);
        }
        xml(resp, 200, S3Xml.listUploads(r.bucket(), q.getOrDefault("prefix", ""), q.get("delimiter"), keyMarker, upMarker,
                max, urlEncode, l));
    }

    // ---- objects -----------------------------------------------------------------------------------

    private void putObject(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        BucketInfo b = store.requireBucket(r.bucket());
        JsonObject extra = extraOf(req, b, true);
        S3Body body = S3Body.open(req);
        PutIn in = new PutIn(r.bucket(), r.key(), attrsOf(req), extra, body.stream(), body.length(),
                req.getHeader("Content-MD5"), body::verifyDigest, S3Body.declaredAlgos(req), body::claimed,
                req.getHeader("If-None-Match"), req.getHeader("If-Match"), b);
        Meta m = store.put(in);
        writeMetaHeaders(resp, m, b, true);
        resp.setHeader("Last-Modified", null);
        resp.setStatus(200);
    }

    private void getObject(Route r, HttpServletRequest req, HttpServletResponse resp, boolean head) throws IOException {
        Map<String, String> q = r.query();
        int part = 0;
        if (q.containsKey("partNumber")) {
            try {
                part = Integer.parseInt(q.get("partNumber"));
            } catch (NumberFormatException e) {
                part = 0;
            }
            if (part < 1 || part > 10000) {
                throw new S3WireException(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive");
            }
        }
        GetIn in = new GetIn(r.bucket(), r.key(), q.get("versionId"), part, conditions(req), req.getHeader("Range"));
        BucketInfo b = store.requireBucket(r.bucket());
        boolean cm = checksumMode(req);
        if (head) {
            Meta m = store.head(r.bucket(), in);
            long len = m.size();
            String range = null;
            if (part > 0 && m.multipart()) {
                long start = 0;
                if (part > m.parts().size()) {
                    throw new S3WireException(416, "InvalidPartNumber", "The requested partnumber is not satisfiable");
                }
                for (int i = 0; i < part - 1; i++) {
                    start += m.parts().get(i).size();
                }
                len = m.parts().get(part - 1).size();
                range = "bytes " + start + "-" + (start + len - 1) + "/" + m.size();
                resp.setStatus(206);
            } else {
                S3Http.ByteRange br = S3Http.parseRange(req.getHeader("Range"), m.size());
                if (br != null) {
                    len = br.length();
                    range = "bytes " + br.start() + "-" + br.end() + "/" + m.size();
                    resp.setStatus(206);
                } else {
                    resp.setStatus(200);
                }
            }
            writeObjectHeaders(resp, m, b, len, range, cm, q);
            return;
        }
        try (Read g = store.get(in)) {
            resp.setStatus(g.contentRange() != null ? 206 : 200);
            writeObjectHeaders(resp, g.meta(), b, g.contentLength(), g.contentRange(), cm, q);
            g.transferTo(resp.getOutputStream());
        }
    }

    private void deleteObject(Route r, HttpServletRequest req, HttpServletResponse resp) {
        String version = r.query().get("versionId");
        boolean bypass = "true".equalsIgnoreCase(req.getHeader("x-amz-bypass-governance-retention"));
        DeleteOut d = store.delete(r.bucket(), r.key(), version, bypass);
        if (d.versionId() != null) {
            resp.setHeader("x-amz-version-id", d.versionId());
        }
        if (d.deleteMarker()) {
            resp.setHeader("x-amz-delete-marker", "true");
        }
        resp.setStatus(204);
    }

    private void deleteObjects(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        store.requireBucket(r.bucket());
        byte[] xml = S3Body.readSmall(req);
        S3Xml.DeleteIds d = S3Xml.parseDeleteIds(xml);
        if (d.ids().size() > 1000 || d.ids().isEmpty()) {
            throw new S3WireException(400, "MalformedXML", "The XML you provided was not well-formed or did not validate "
                    + "against our published schema");
        }
        boolean bypass = "true".equalsIgnoreCase(req.getHeader("x-amz-bypass-governance-retention"));
        xml(resp, 200, S3Xml.deleteResultRich(store.deleteMany(r.bucket(), d.ids(), bypass), d.quiet()));
    }

    private void copyObject(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        BucketInfo dst = store.requireBucket(r.bucket());
        CopySource src = copySource(req);
        String directive = req.getHeader("x-amz-metadata-directive");
        if (directive != null && !directive.equalsIgnoreCase("COPY") && !directive.equalsIgnoreCase("REPLACE")) {
            throw new S3WireException(400, "InvalidArgument", "Unknown metadata directive.", null,
                    Map.of("ArgumentName", "x-amz-metadata-directive", "ArgumentValue", directive));
        }
        String tdir = req.getHeader("x-amz-tagging-directive");
        if (tdir != null && !tdir.equalsIgnoreCase("COPY") && !tdir.equalsIgnoreCase("REPLACE")) {
            throw new S3WireException(400, "InvalidArgument", "Unknown tagging directive.", null,
                    Map.of("ArgumentName", "x-amz-tagging-directive", "ArgumentValue", tdir));
        }
        boolean replaceTags = "REPLACE".equalsIgnoreCase(tdir);
        Attrs replace = "REPLACE".equalsIgnoreCase(directive) ? attrsOf(req) : null;
        String adir = req.getHeader("x-amz-object-annotation-directive") != null
                ? req.getHeader("x-amz-object-annotation-directive") : req.getHeader("x-amz-annotation-directive");
        Set<String> algos = new LinkedHashSet<>();
        String ca = req.getHeader("x-amz-checksum-algorithm");
        if (ca != null) {
            String a = Checksums.canonical(ca);
            if (a == null) {
                throw new S3WireException(400, "InvalidRequest", "Checksum algorithm provided is unsupported.");
            }
            algos.add(a);
        }
        JsonObject extra = extraOf(req, dst, replaceTags);
        CopyOut out = store.copy(new CopyIn(src.bucket(), src.key(), src.versionId(), r.bucket(), r.key(), replace, extra,
                replaceTags, "EXCLUDE".equalsIgnoreCase(adir), copyConditions(req), algos, dst));
        Meta m = out.meta();
        if (out.srcVersionId() != null) {
            resp.setHeader("x-amz-copy-source-version-id", out.srcVersionId());
        }
        if (dst.versioned() && m.versionId() != null) {
            resp.setHeader("x-amz-version-id", m.versionId());
        }
        resp.setHeader("x-amz-server-side-encryption", m.str("sse") != null ? m.str("sse") : "AES256");
        xml(resp, 200, S3Xml.copyResultRich(m));
    }

    private void objectAttributes(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String h = String.join(",", Collections.list(req.getHeaders("x-amz-object-attributes")));
        if (h.isBlank()) {
            throw new S3WireException(400, "InvalidArgument", "Invalid Attribute Name specified.", null,
                    Map.of("ArgumentName", "x-amz-object-attributes"));
        }
        Set<String> want = new LinkedHashSet<>();
        for (String a : h.split(",")) {
            String t = a.trim();
            if (!List.of("ETag", "Checksum", "ObjectParts", "StorageClass", "ObjectSize").contains(t)) {
                throw new S3WireException(400, "InvalidArgument", "Invalid Attribute Name specified.", null,
                        Map.of("ArgumentName", "x-amz-object-attributes", "ArgumentValue", t));
            }
            want.add(t);
        }
        int maxParts = 1000;
        int marker = 0;
        try {
            if (req.getHeader("x-amz-max-parts") != null) {
                maxParts = Integer.parseInt(req.getHeader("x-amz-max-parts").trim());
            }
            if (req.getHeader("x-amz-part-number-marker") != null) {
                marker = Integer.parseInt(req.getHeader("x-amz-part-number-marker").trim());
            }
        } catch (NumberFormatException e) {
            throw new S3WireException(400, "InvalidArgument", "Invalid max-parts or part-number-marker");
        }
        BucketInfo b = store.requireBucket(r.bucket());
        Meta m = store.head(r.bucket(), new GetIn(r.bucket(), r.key(), r.query().get("versionId"), 0, conditions(req), null));
        resp.setHeader("Last-Modified", S3Xml.httpDate(m.lastModified()));
        if (b.versioned() && m.versionId() != null) {
            resp.setHeader("x-amz-version-id", m.versionId());
        }
        xml(resp, 200, S3Xml.objectAttributes(m, want, Math.min(Math.max(maxParts, 0), 1000), marker));
    }

    // ---- tagging / acl / lock ------------------------------------------------------------------------

    private void objectTagging(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String version = r.query().get("versionId");
        BucketInfo b = store.requireBucket(r.bucket());
        if (r.op().equals("GetObjectTagging")) {
            Meta m = store.head(r.bucket(), new GetIn(r.bucket(), r.key(), version, 0, ObjectStore.Conditions.NONE, null));
            Map<String, String> tags = new LinkedHashMap<>();
            if (m.extra().has("tags")) {
                m.extra().getAsJsonObject("tags").entrySet().forEach(e -> tags.put(e.getKey(), e.getValue().getAsString()));
            }
            if (b.versioned() && m.versionId() != null) {
                resp.setHeader("x-amz-version-id", m.versionId());
            }
            xml(resp, 200, S3Cfg.taggingXml(tags));
            return;
        }
        Meta m;
        if (r.op().equals("PutObjectTagging")) {
            Map<String, String> tags = S3Cfg.parseTaggingXml(S3Body.readSmall(req), 10);
            m = store.mutateExtra(r.bucket(), r.key(), version, x -> {
                x.remove("tags");
                if (!tags.isEmpty()) {
                    JsonObject t = new JsonObject();
                    tags.forEach(t::addProperty);
                    x.add("tags", t);
                }
            });
            resp.setStatus(200);
        } else {
            m = store.mutateExtra(r.bucket(), r.key(), version, x -> x.remove("tags"));
            resp.setStatus(204);
        }
        if (b.versioned() && m.versionId() != null) {
            resp.setHeader("x-amz-version-id", m.versionId());
        }
    }

    private void objectAcl(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String version = r.query().get("versionId");
        BucketInfo b = store.requireBucket(r.bucket());
        if (r.op().equals("GetObjectAcl")) {
            Meta m = store.head(r.bucket(), new GetIn(r.bucket(), r.key(), version, 0, ObjectStore.Conditions.NONE, null));
            if (b.versioned() && m.versionId() != null) {
                resp.setHeader("x-amz-version-id", m.versionId());
            }
            xml(resp, 200, m.str("acl") != null ? m.str("acl") : S3Cfg.defaultAclXml());
            return;
        }
        if ("BucketOwnerEnforced".equals(b.ownership())) {
            throw new S3WireException(400, "AccessControlListNotSupported", "The bucket does not allow ACLs");
        }
        String acl = S3Cfg.aclFromRequest(req::getHeader, S3Body.readSmall(req));
        if (acl == null) {
            throw new S3WireException(400, "MissingSecurityHeader", "Your request was missing a required header");
        }
        enforceBlockPublicAcls(r.bucket(), acl);
        Meta m = store.mutateExtra(r.bucket(), r.key(), version, x -> x.addProperty("acl", acl));
        if (b.versioned() && m.versionId() != null) {
            resp.setHeader("x-amz-version-id", m.versionId());
        }
        resp.setStatus(200);
    }

    private void objectLock(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String version = r.query().get("versionId");
        BucketInfo b = store.requireBucket(r.bucket());
        if (!b.objectLock()) {
            throw new S3WireException(400, "InvalidRequest", "Bucket is missing Object Lock Configuration");
        }
        boolean retention = r.op().endsWith("Retention");
        if (r.op().startsWith("Get")) {
            Meta m = store.head(r.bucket(), new GetIn(r.bucket(), r.key(), version, 0, ObjectStore.Conditions.NONE, null));
            if (retention) {
                if (m.str("lm") == null) {
                    throw new S3WireException(404, "NoSuchObjectLockConfiguration",
                            "The specified object does not have a ObjectLock configuration");
                }
                xml(resp, 200, S3Xml.DECL + "<Retention xmlns=\"" + S3Xml.NS + "\"><Mode>" + m.str("lm")
                        + "</Mode><RetainUntilDate>" + m.str("lu") + "</RetainUntilDate></Retention>");
            } else {
                if (m.str("lh") == null) {
                    throw new S3WireException(404, "NoSuchObjectLockConfiguration",
                            "The specified object does not have a ObjectLock configuration");
                }
                xml(resp, 200, S3Xml.DECL + "<LegalHold xmlns=\"" + S3Xml.NS + "\"><Status>" + m.str("lh") + "</Status></LegalHold>");
            }
            return;
        }
        byte[] body = S3Body.readSmall(req);
        boolean bypass = "true".equalsIgnoreCase(req.getHeader("x-amz-bypass-governance-retention"));
        if (retention) {
            String[] d = S3Cfg.parseRetention(body);
            store.mutateExtra(r.bucket(), r.key(), version, x -> {
                if (x.has("lm") && x.has("lu") && Instant.parse(x.get("lu").getAsString()).isAfter(Instant.now())) {
                    boolean shorten = Instant.parse(d[1]).isBefore(Instant.parse(x.get("lu").getAsString()));
                    if ("COMPLIANCE".equals(x.get("lm").getAsString()) && (shorten || !d[0].equals("COMPLIANCE"))
                            || "GOVERNANCE".equals(x.get("lm").getAsString()) && shorten && !bypass) {
                        throw new S3WireException(403, "AccessDenied", "Access Denied because object protected by object lock.");
                    }
                }
                x.addProperty("lm", d[0]);
                x.addProperty("lu", d[1]);
            });
        } else {
            String st = S3Cfg.parseLegalHold(body);
            store.mutateExtra(r.bucket(), r.key(), version, x -> x.addProperty("lh", st));
        }
        resp.setStatus(200);
    }

    // ---- annotations -------------------------------------------------------------------------------

    private static final Pattern ANNOTATION_NAME = Pattern.compile("^[A-Za-z0-9._:-]{1,255}$");

    private void annotation(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Map<String, String> q = r.query();
        String version = q.get("versionId");
        BucketInfo b = store.requireBucket(r.bucket());
        String name = q.get("annotationName");
        switch (r.op()) {
            case "PutObjectAnnotation" -> {
                if (name == null || !ANNOTATION_NAME.matcher(name).matches()) {
                    throw new S3WireException(400, "InvalidArgument", "Invalid annotation name");
                }
                S3Body body = S3Body.open(req);
                if (body.length() < 1 || body.length() > 1024 * 1024) {
                    throw new S3WireException(400, "InvalidArgument",
                            "The annotation payload must be between 1 byte and 1 MiB");
                }
                byte[] payload = body.stream().readNBytes((int) body.length());
                if (!body.verifyDigest()) {
                    throw new S3WireException(400, "XAmzContentSHA256Mismatch",
                            "The provided 'x-amz-content-sha256' header does not match what was computed.");
                }
                Annotation a = store.putAnnotation(r.bucket(), r.key(), version, name, payload, req.getHeader("Content-Type"),
                        S3Body.declaredAlgos(req), body.claimed());
                resp.setHeader("ETag", a.eTag());
                if (a.versionId() != null) {
                    resp.setHeader("x-amz-object-version-id", a.versionId());
                }
                for (Map.Entry<String, String> c : a.checksums().entrySet()) {
                    resp.setHeader(Checksums.headerName(c.getKey()), c.getValue());
                }
                resp.setHeader("x-amz-checksum-type", "FULL_OBJECT");
                if (b.encryptionAlgo() != null) {
                    resp.setHeader("x-amz-server-side-encryption", b.encryptionAlgo());
                }
                xml(resp, 200, S3Xml.putAnnotationOutput(r.key(), name));
            }
            case "GetObjectAnnotation" -> {
                Annotation a = store.getAnnotation(r.bucket(), r.key(), version, name);
                if (a == null) {
                    throw new S3WireException(404, "NoSuchAnnotation", "The specified annotation does not exist.", null,
                            Map.of("AnnotationName", name));
                }
                resp.setStatus(200);
                resp.setContentType(a.contentType() != null ? a.contentType() : "binary/octet-stream");
                resp.setContentLengthLong(a.size());
                resp.setHeader("ETag", a.eTag());
                resp.setHeader("Last-Modified", S3Xml.httpDate(a.modified()));
                if (a.versionId() != null) {
                    resp.setHeader("x-amz-object-version-id", a.versionId());
                }
                if (checksumMode(req)) {
                    for (Map.Entry<String, String> c : a.checksums().entrySet()) {
                        resp.setHeader(Checksums.headerName(c.getKey()), c.getValue());
                    }
                    resp.setHeader("x-amz-checksum-type", "FULL_OBJECT");
                }
                if (!"HEAD".equals(req.getMethod())) {
                    resp.getOutputStream().write(a.payload());
                }
            }
            case "ListObjectAnnotations" -> {
                List<Annotation> all = store.listAnnotations(r.bucket(), r.key(), version);
                String prefix = q.get("annotation-prefix");
                String token = q.get("continuation-token");
                int max = 1000;
                if (q.get("max-annotation-results") != null && !q.get("max-annotation-results").isBlank()) {
                    try {
                        max = Math.min(1000, Math.max(1, Integer.parseInt(q.get("max-annotation-results"))));
                    } catch (NumberFormatException e) {
                        throw new S3WireException(400, "InvalidArgument", "max-annotation-results must be an integer.");
                    }
                }
                List<Annotation> page = new ArrayList<>();
                boolean truncated = false;
                for (Annotation a : all) {
                    if (prefix != null && !a.name().startsWith(prefix) || token != null && a.name().compareTo(token) <= 0) {
                        continue;
                    }
                    if (page.size() == max) {
                        truncated = true;
                        break;
                    }
                    page.add(a);
                }
                String vid = page.isEmpty() ? version : page.get(0).versionId();
                if (vid != null) {
                    resp.setHeader("x-amz-object-version-id", vid);
                }
                if ("HEAD".equals(req.getMethod())) {
                    resp.setStatus(200);
                } else {
                    xml(resp, 200, S3Xml.listAnnotations(r.bucket(), r.key(), prefix, max, token, page, truncated,
                            page.isEmpty() ? null : page.get(page.size() - 1).name()));
                }
            }
            default -> {
                if (name == null || name.isEmpty()) {
                    throw new S3WireException(400, "InvalidArgument", "annotationName is required");
                }
                String v = store.deleteAnnotation(r.bucket(), r.key(), version, name);
                if (v != null) {
                    resp.setHeader("x-amz-object-version-id", v);
                }
                resp.setStatus(204);
            }
        }
    }

    // ---- multipart ---------------------------------------------------------------------------------

    private void createMultipart(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        BucketInfo b = store.requireBucket(r.bucket());
        JsonObject extra = extraOf(req, b, true);
        String algoH = req.getHeader("x-amz-checksum-algorithm");
        String algo = null;
        if (algoH != null) {
            algo = Checksums.canonical(algoH);
            if (algo == null) {
                throw new S3WireException(400, "InvalidRequest", "Checksum algorithm provided is unsupported. Please try "
                        + "again with any of the valid types: [CRC32, CRC32C, SHA1, SHA256, CRC64NVME]");
            }
        }
        String type = req.getHeader("x-amz-checksum-type");
        if (type != null && !List.of("COMPOSITE", "FULL_OBJECT").contains(type)) {
            throw new S3WireException(400, "InvalidRequest", "Checksum type provided is unsupported.");
        }
        String id = store.createMultipart(r.bucket(), r.key(), attrsOf(req), extra, algo, type);
        if (algo != null) {
            resp.setHeader("x-amz-checksum-algorithm", algo);
            resp.setHeader("x-amz-checksum-type", type != null ? type : "CRC64NVME".equals(algo) ? "FULL_OBJECT" : "COMPOSITE");
        }
        resp.setHeader("x-amz-server-side-encryption", extra.has("sse") ? extra.get("sse").getAsString() : "AES256");
        xml(resp, 200, S3Xml.initiateMultipart(r.bucket(), r.key(), id));
    }

    private static int partNumber(Map<String, String> q) {
        int n;
        try {
            n = Integer.parseInt(q.get("partNumber"));
        } catch (NumberFormatException e) {
            n = 0;
        }
        if (n < 1 || n > 10000) {
            throw new S3WireException(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive",
                    null, Map.of("ArgumentName", "partNumber", "ArgumentValue", String.valueOf(q.get("partNumber"))));
        }
        return n;
    }

    private void uploadPart(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        int n = partNumber(r.query());
        S3Body body = S3Body.open(req);
        Set<String> declared = S3Body.declaredAlgos(req);
        PartOut out = store.uploadPart(new UploadPartIn(r.bucket(), r.key(), r.query().get("uploadId"), n, body.stream(),
                body.length(), req.getHeader("Content-MD5"), body::verifyDigest, body::claimed), declared);
        resp.setHeader("ETag", out.eTag());
        boolean onlyDefault = declared.isEmpty() && out.checksums().size() == 1
                && out.checksums().containsKey(PostgresObjectStore.DEFAULT_CRC);
        if (!onlyDefault) {
            for (Map.Entry<String, String> c : out.checksums().entrySet()) {
                resp.setHeader(Checksums.headerName(c.getKey()), c.getValue());
            }
        }
        resp.setStatus(200);
    }

    private void uploadPartCopy(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        int n = partNumber(r.query());
        CopySource src = copySource(req);
        PartCopyOut out = store.uploadPartCopy(new UploadPartCopyIn(r.bucket(), r.key(), r.query().get("uploadId"), n,
                src.bucket(), src.key(), src.versionId(), req.getHeader("x-amz-copy-source-range"), copyConditions(req)));
        if (out.srcVersionId() != null) {
            resp.setHeader("x-amz-copy-source-version-id", out.srcVersionId());
        }
        xml(resp, 200, S3Xml.copyPartResult(out));
    }

    private void completeMultipart(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        BucketInfo b = store.requireBucket(r.bucket());
        List<CompletePart> parts = S3Xml.parseCompleteParts(S3Body.readSmall(req));
        CompleteOut out = store.completeMultipart(r.bucket(), r.key(), r.query().get("uploadId"), parts,
                Checksums.claimedFrom(req::getHeader), req.getHeader("x-amz-checksum-type"), req.getHeader("If-None-Match"));
        if (b.versioned() && out.versionId() != null) {
            resp.setHeader("x-amz-version-id", out.versionId());
        }
        xml(resp, 200, S3Xml.completeMultipartRich(req.getRequestURL().toString(), r.bucket(), r.key(), out));
    }

    private void listParts(Route r, HttpServletResponse resp) throws IOException {
        Map<String, String> q = r.query();
        int max = 1000;
        int marker = 0;
        try {
            if (q.containsKey("max-parts")) {
                max = Integer.parseInt(q.get("max-parts"));
            }
            if (q.containsKey("part-number-marker")) {
                marker = Integer.parseInt(q.get("part-number-marker"));
            }
        } catch (NumberFormatException e) {
            throw new S3WireException(400, "InvalidArgument", "Provided max-parts or part-number-marker is not an integer");
        }
        if (max < 0) {
            throw new S3WireException(400, "InvalidArgument", "Argument max-parts must be an integer between 0 and 2147483647");
        }
        max = Math.min(max, 1000);
        PartListing l = store.listParts(r.bucket(), r.key(), q.get("uploadId"), marker, Math.max(max, 1));
        if (max == 0) {
            l = new PartListing(l.upload(), List.of(), false, 0);
        }
        xml(resp, 200, S3Xml.listParts(r.bucket(), r.key(), marker, max, l));
    }

    // ---- POST object (browser form upload) -----------------------------------------------------------

    static boolean isAnonymousPostForm(HttpServletRequest req, String key) {
        String ct = req.getContentType();
        return "POST".equals(req.getMethod()) && (key == null || key.isEmpty()) && req.getHeader("Authorization") == null
                && ct != null && ct.toLowerCase(Locale.ROOT).startsWith("multipart/form-data")
                && (req.getQueryString() == null || !req.getQueryString().contains("X-Amz-Signature"));
    }

    private static final long POST_MAX = Long.getLong("warp.s3wire.postMaxBytes",
            envLong("WARP_S3WIRE_POST_MAX_BYTES", 64L << 20));

    private static long envLong(String n, long d) {
        String v = System.getenv(n);
        return v == null || v.isBlank() ? d : Long.parseLong(v.trim());
    }

    void postObject(String bucket, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        long len = req.getContentLengthLong();
        if (len > POST_MAX) {
            throw new S3WireException(400, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size for a "
                    + "browser form upload (" + POST_MAX + " bytes); use PutObject or a multipart upload");
        }
        byte[] body = req.getInputStream().readNBytes((int) Math.min(POST_MAX, Integer.MAX_VALUE) + 1);
        if (body.length > POST_MAX) {
            throw new S3WireException(400, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size");
        }
        S3PostForm.Form form = S3PostForm.parse(body, S3PostForm.boundary(req.getContentType()));
        Map<String, String> f = form.fields();
        BucketInfo b = store.requireBucket(bucket);
        String policy = f.get("policy");
        String cred = f.get("x-amz-credential");
        String sig = f.get("x-amz-signature");
        if (policy == null || cred == null || sig == null || !"AWS4-HMAC-SHA256".equals(f.get("x-amz-algorithm"))) {
            throw new S3WireException(403, "AccessDenied", "Access Denied: browser uploads require a signed policy");
        }
        if (!S3PostForm.validCredentialScope(cred)) {
            throw new S3WireException(403, "AccessDenied", "Invalid credential scope");
        }
        String secret = config.clientCredentials().secretFor(cred.split("/")[0]);
        if (secret == null) {
            throw new S3WireException(403, "InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.");
        }
        if (!S3PostForm.verifySignature(policy, cred, sig, secret)) {
            throw new S3WireException(403, "SignatureDoesNotMatch",
                    "The request signature we calculated does not match the signature you provided.");
        }
        String policyJson;
        try {
            policyJson = new String(java.util.Base64.getDecoder().decode(policy), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new S3WireException(400, "InvalidPolicyDocument", "Invalid Policy: not base64");
        }
        S3PostForm.checkPolicy(policyJson, f, form.file().length, bucket, Instant.now());
        String key = f.get("key");
        if (key == null || key.isEmpty()) {
            throw new S3WireException(400, "InvalidArgument", "Bucket POST must contain a field named 'key'.", null,
                    Map.of("ArgumentName", "key"));
        }
        key = key.replace("${filename}", form.filename() == null ? "" : form.filename());
        S3Keys.validate(key);
        Map<String, String> meta = new LinkedHashMap<>();
        f.forEach((k, v) -> {
            if (k.startsWith("x-amz-meta-")) {
                meta.put(k.substring("x-amz-meta-".length()), v);
            }
        });
        String ct = f.get("content-type") != null ? f.get("content-type") : form.fileContentType();
        Attrs attrs = new Attrs(ct, f.get("cache-control"), f.get("content-disposition"), f.get("content-encoding"),
                f.get("content-language"), f.get("expires"), meta);
        JsonObject extra = new JsonObject();
        if (f.get("acl") != null) {
            extra.addProperty("acl", S3Cfg.aclXml(S3Cfg.cannedGrants(f.get("acl"))));
        }
        if (f.get("x-amz-server-side-encryption") != null) {
            extra.addProperty("sse", f.get("x-amz-server-side-encryption"));
        }
        if (f.get("x-amz-storage-class") != null && !f.get("x-amz-storage-class").equals("STANDARD")) {
            extra.addProperty("sc", f.get("x-amz-storage-class"));
        }
        if (f.get("tagging") != null) {
            Map<String, String> tags = S3Cfg.parseTaggingXml(S3Xml.utf8(f.get("tagging")), 10);
            if (!tags.isEmpty()) {
                JsonObject t = new JsonObject();
                tags.forEach(t::addProperty);
                extra.add("tags", t);
            }
        }
        Set<String> algos = new LinkedHashSet<>();
        Map<String, String> claimed = Checksums.claimedFrom(n -> f.get(n));
        algos.addAll(claimed.keySet());
        PutIn in = new PutIn(bucket, key, attrs, extra, new java.io.ByteArrayInputStream(form.file()), form.file().length,
                null, () -> true, algos, () -> claimed, null, null, b);
        Meta m = store.put(in);
        String status = f.getOrDefault("success_action_status", "204");
        String redirect = f.get("success_action_redirect");
        resp.setHeader("ETag", m.eTag());
        if (b.versioned() && m.versionId() != null) {
            resp.setHeader("x-amz-version-id", m.versionId());
        }
        String location = "http://" + req.getHeader("Host") + "/" + bucket + "/" + key;
        if (redirect != null && !redirect.isEmpty()) {
            String sep = redirect.contains("?") ? "&" : "?";
            resp.setHeader("Location", redirect + sep + "bucket=" + S3SigV4Verifier.encode(bucket, true) + "&key="
                    + S3SigV4Verifier.encode(key, true) + "&etag=" + S3SigV4Verifier.encode(m.eTag(), true));
            resp.setStatus(303);
            return;
        }
        resp.setHeader("Location", location);
        switch (status) {
            case "200" -> resp.setStatus(200);
            case "201" -> xml(resp, 201, S3Xml.DECL + "<PostResponse><Location>" + S3Xml.escape(location) + "</Location><Bucket>"
                    + S3Xml.escape(bucket) + "</Bucket><Key>" + S3Xml.escape(key) + "</Key><ETag>" + S3Xml.escape(m.eTag())
                    + "</ETag></PostResponse>");
            default -> resp.setStatus(204);
        }
    }

    // ---- SelectObjectContent -------------------------------------------------------------------------

    private static final long SELECT_MAX = 128L << 20;

    private void selectObject(Route r, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        S3Select.Request q = S3Select.parseRequest(S3Body.readSmall(req));
        if (q.inType().equals("Parquet")) {
            throw notImplemented("SelectObjectContent over Parquet");
        }
        GetIn in = new GetIn(r.bucket(), r.key(), r.query().get("versionId"), 0, ObjectStore.Conditions.NONE, null);
        byte[] data;
        try (Read g = store.get(in)) {
            if (g.contentLength() > SELECT_MAX) {
                throw new S3WireException(400, "OverMaxRecordSize", "The object is larger than the "
                        + SELECT_MAX + " bytes SelectObjectContent supports in s3wire");
            }
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream((int) Math.max(32, g.contentLength()));
            g.transferTo(bos);
            data = bos.toByteArray();
        }
        long scanned = data.length;
        if (q.compression().equalsIgnoreCase("GZIP")) {
            try (java.util.zip.GZIPInputStream z = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(data))) {
                data = z.readAllBytes();
            } catch (IOException e) {
                throw new S3WireException(400, "InvalidCompressionFormat", "GZIP is not applicable to the object");
            }
        } else if (q.compression().equalsIgnoreCase("BZIP2")) {
            throw notImplemented("BZIP2 compression");
        }
        byte[] records = S3Select.run(data, q);
        byte[] events = S3Select.eventStream(records, scanned, false);
        resp.setStatus(200);
        resp.setContentType("application/octet-stream");
        resp.setContentLength(events.length);
        resp.getOutputStream().write(events);
    }
}
