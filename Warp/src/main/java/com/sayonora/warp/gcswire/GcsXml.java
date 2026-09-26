package com.sayonora.warp.gcswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.gcswire.GcsModel.Bucket;
import com.sayonora.warp.gcswire.GcsModel.Obj;
import com.sayonora.warp.gcswire.GcsModel.Pre;
import com.sayonora.warp.gcswire.GcsModel.Seg;
import com.sayonora.warp.gcswire.GcsModel.Session;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** The GCS XML API (S3-interoperable): buckets, objects, XML multipart uploads, XML resumable uploads, CORS preflight. */
final class GcsXml {

    private static final String NS = "http://doc.s3.amazonaws.com/2006-03-01";
    private static final String XML_HEAD = "<?xml version='1.0' encoding='UTF-8'?>";

    private final GcsStore st;
    private final GcsConfig cfg;
    private final GcsOps ops;
    private final GcsResumable resumable;

    GcsXml(GcsStore st, GcsConfig cfg, GcsOps ops, GcsResumable resumable) {
        this.st = st;
        this.cfg = cfg;
        this.ops = ops;
        this.resumable = resumable;
    }

    static String esc(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&apos;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** The XML {@code <Error>} document of an exception. */
    static String error(GcsException e) {
        String msg = e.getMessage();
        String details = msg;
        String code = e.xmlCode;
        switch (code) {
            case "NoSuchKey" -> {
                details = msg;
                msg = "The specified key does not exist.";
            }
            case "NoSuchBucket" -> msg = "The specified bucket does not exist.";
            case "BucketNotEmpty" -> msg = "The bucket you tried to delete is not empty.";
            case "PreconditionFailed" -> details = "At least one of the pre-conditions you specified did not hold.";
            case "NoSuchUpload" -> msg = "The specified multipart upload does not exist. The upload ID might be invalid, or the "
                    + "multipart upload might have been aborted or completed.";
            default -> {
            }
        }
        return XML_HEAD + "<Error><Code>" + esc(code) + "</Code><Message>" + esc(msg) + "</Message>"
                + (details == null ? "" : "<Details>" + esc(details) + "</Details>") + "</Error>";
    }

    private static void xml(GcsResp w, int status, String body) throws IOException {
        w.bytes(status, "application/xml; charset=UTF-8", body.getBytes(StandardCharsets.UTF_8));
    }

    /** Splits the path into (bucket, object) honouring virtual-hosted addressing; object null = bucket-level. */
    private String[] target(GcsReq r) {
        String h = r.host == null ? "" : r.host.toLowerCase(Locale.ROOT);
        int colon = h.lastIndexOf(':');
        if (colon > 0) {
            h = h.substring(0, colon);
        }
        String suffix = "." + cfg.domain;
        String rawObj;
        String bucket;
        String p = r.rawPath.startsWith("/") ? r.rawPath.substring(1) : r.rawPath;
        if (h.endsWith(suffix) && h.length() > suffix.length()) {
            bucket = h.substring(0, h.length() - suffix.length());
            rawObj = p;
        } else {
            int slash = p.indexOf('/');
            bucket = GcsReq.decode(slash < 0 ? p : p.substring(0, slash), false);
            rawObj = slash < 0 ? "" : p.substring(slash + 1);
        }
        return new String[] {bucket, rawObj.isEmpty() ? null : GcsReq.decode(rawObj, false)};
    }

    void handle(GcsReq r, GcsResp w) throws IOException {
        String[] t = target(r);
        String bucket = t[0];
        String object = t[1];
        w.header("Cache-Control", null);
        if (bucket.isEmpty()) {
            if (!r.method.equals("GET")) {
                throw GcsService.badMethod(r.method);
            }
            r.op = "xml.listBuckets";
            listBuckets(r, w);
            return;
        }
        Bucket b = ops.st.getBucket(bucket);
        String origin = r.header("origin");
        if (b != null && origin != null) {
            cors(b, r, w, origin, r.method, null, false);
        }
        if (object == null) {
            bucketOp(r, w, bucket, b);
        } else {
            if (b == null) {
                throw GcsException.noSuchBucket();
            }
            objectOp(r, w, b, object);
        }
    }

    // ------------------------------------------------------------------------------------------ CORS

    /** Adds CORS response headers when the bucket's config allows {@code origin}; a preflight without a match is a 403. */
    void cors(Bucket b, GcsReq r, GcsResp w, String origin, String method, String reqHeaders, boolean preflight) {
        JsonArray cors = b.doc.has("cors") ? b.doc.getAsJsonArray("cors") : new JsonArray();
        for (JsonElement e : cors) {
            JsonObject c = e.getAsJsonObject();
            if (!listHas(c, "origin", origin, true) || !listHas(c, "method", method, false)) {
                continue;
            }
            w.header("Access-Control-Allow-Origin", listHas(c, "origin", "*", false) ? "*" : origin);
            w.header("Vary", "Origin");
            if (preflight) {
                w.header("Access-Control-Allow-Methods", joined(c, "method"));
                String rh = reqHeaders;
                if (rh != null && !rh.isBlank()) {
                    w.header("Access-Control-Allow-Headers", rh);
                } else if (c.has("responseHeader")) {
                    w.header("Access-Control-Allow-Headers", joined(c, "responseHeader"));
                }
                if (c.has("maxAgeSeconds")) {
                    w.header("Access-Control-Max-Age", c.get("maxAgeSeconds").getAsString());
                }
            } else if (c.has("responseHeader")) {
                w.header("Access-Control-Expose-Headers", joined(c, "responseHeader"));
            }
            return;
        }
        if (preflight) {
            throw new GcsException(403, "forbidden", "AccessDenied", "Preflight response did not match any CORS configuration of the bucket.");
        }
    }

    private static boolean listHas(JsonObject c, String key, String v, boolean wildcardOk) {
        if (!c.has(key)) {
            return false;
        }
        for (JsonElement e : c.getAsJsonArray(key)) {
            String s = e.getAsString();
            if (s.equalsIgnoreCase(v) || wildcardOk && s.equals("*")) {
                return true;
            }
        }
        return false;
    }

    private static String joined(JsonObject c, String key) {
        List<String> l = new ArrayList<>();
        c.getAsJsonArray(key).forEach(e -> l.add(e.getAsString()));
        return String.join(", ", l);
    }

    /** OPTIONS on the XML API. */
    void preflight(GcsReq r, GcsResp w) throws IOException {
        String[] t = target(r);
        String origin = r.header("origin");
        String method = r.header("access-control-request-method");
        if (origin == null || method == null) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Preflight requests need Origin and Access-Control-Request-Method.");
        }
        Bucket b = t[0].isEmpty() ? null : st.getBucket(t[0]);
        if (b == null) {
            throw GcsException.noSuchBucket();
        }
        cors(b, r, w, origin, method, r.header("access-control-request-headers"), true);
        w.empty(200);
    }

    // ------------------------------------------------------------------------------------------ buckets

    private void listBuckets(GcsReq r, GcsResp w) throws IOException {
        String project = r.header("x-goog-project-id");
        if (project == null) {
            throw new GcsException(400, "invalid", "MissingSecurityHeader", "Your request was missing a required header.");
        }
        StringBuilder sb = new StringBuilder(XML_HEAD + "<ListAllMyBucketsResult xmlns='" + NS + "'><Owner><ID>" + esc(GcsJson.projectNumber(project))
                + "</ID></Owner><Buckets>");
        for (Bucket b : st.listBuckets(project, null, null, 1000)) {
            sb.append("<Bucket><Name>").append(esc(b.name)).append("</Name><CreationDate>").append(GcsJson.ts(b.created))
                    .append("</CreationDate></Bucket>");
        }
        sb.append("</Buckets></ListAllMyBucketsResult>");
        xml(w, 200, sb.toString());
    }

    private void bucketOp(GcsReq r, GcsResp w, String bucket, Bucket b) throws IOException {
        String m = r.method;
        if (m.equals("PUT") && r.query.isEmpty()) {
            r.op = "xml.createBucket";
            r.write = true;
            createBucket(r, w, bucket);
            return;
        }
        if (b == null) {
            throw GcsException.noSuchBucket();
        }
        if (r.has("location")) {
            xml(w, 200, XML_HEAD + "<LocationConstraint xmlns='" + NS + "'>" + esc(b.doc.get("location").getAsString()) + "</LocationConstraint>");
            return;
        }
        if (r.has("versioning")) {
            if (m.equals("GET")) {
                xml(w, 200, XML_HEAD + "<VersioningConfiguration xmlns='" + NS + "'>" + (b.versioning ? "<Status>Enabled</Status>" : "")
                        + "</VersioningConfiguration>");
            } else if (m.equals("PUT")) {
                r.write = true;
                String body = new String(r.readBody(1 << 20), StandardCharsets.UTF_8);
                boolean on = body.contains("<Status>Enabled</Status>");
                st.mutateBucket(bucket, x -> {
                    x.versioning = on;
                    JsonObject v = new JsonObject();
                    v.addProperty("enabled", on);
                    x.doc.add("versioning", v);
                    x.metageneration++;
                    x.updated = Instant.now();
                    return x;
                });
                w.empty(200);
            } else {
                throw GcsService.badMethod(m);
            }
            return;
        }
        if (r.has("uploads") && m.equals("GET")) {
            listUploads(r, w, b);
            return;
        }
        if (r.has("cors")) {
            corsConfig(r, w, b);
            return;
        }
        for (String unsupported : new String[] {"acl", "lifecycle", "logging", "website", "tagging", "policy", "encryption", "billing"}) {
            if (r.has(unsupported)) {
                throw new GcsException(501, "notImplemented", "NotImplemented", "The XML API sub-resource ?" + unsupported
                        + " is not supported by gcswire; use the JSON API.");
            }
        }
        switch (m) {
            case "HEAD" -> {
                r.op = "xml.headBucket";
                w.header("x-goog-metageneration", String.valueOf(b.metageneration));
                w.header("x-goog-bucket-location", b.doc.get("location").getAsString());
                w.header("x-goog-bucket-storage-class", b.doc.get("storageClass").getAsString());
                w.empty(200);
            }
            case "GET" -> {
                r.op = "xml.listObjects";
                listObjects(r, w, b);
            }
            case "DELETE" -> {
                r.op = "xml.deleteBucket";
                r.write = true;
                if (st.bucketHasObjects(bucket)) {
                    throw new GcsException(409, "conflict", "BucketNotEmpty", "The bucket you tried to delete is not empty.");
                }
                st.deleteBucket(bucket);
                w.empty(204);
            }
            default -> throw GcsService.badMethod(m);
        }
    }

    private void corsConfig(GcsReq r, GcsResp w, Bucket b) throws IOException {
        String m = r.method;
        if (m.equals("GET")) {
            if (!b.doc.has("cors")) {
                throw new GcsException(404, "notFound", "NoSuchCors", "The CORS configuration does not exist.");
            }
            StringBuilder sb = new StringBuilder(XML_HEAD + "<CorsConfig>");
            for (JsonElement e : b.doc.getAsJsonArray("cors")) {
                JsonObject c = e.getAsJsonObject();
                sb.append("<Cors>");
                for (String[] k : new String[][] {{"origin", "Origin"}, {"method", "Method"}, {"responseHeader", "ResponseHeader"}}) {
                    if (c.has(k[0])) {
                        sb.append("<").append(k[1]).append("s>");
                        c.getAsJsonArray(k[0]).forEach(x -> sb.append("<").append(k[1]).append(">").append(esc(x.getAsString()))
                                .append("</").append(k[1]).append(">"));
                        sb.append("</").append(k[1]).append("s>");
                    }
                }
                if (c.has("maxAgeSeconds")) {
                    sb.append("<MaxAgeSec>").append(c.get("maxAgeSeconds").getAsString()).append("</MaxAgeSec>");
                }
                sb.append("</Cors>");
            }
            xml(w, 200, sb.append("</CorsConfig>").toString());
        } else if (m.equals("PUT")) {
            r.write = true;
            Document d = parse(r.readBody(1 << 20));
            JsonArray out = new JsonArray();
            NodeList cs = d.getElementsByTagName("Cors");
            for (int i = 0; i < cs.getLength(); i++) {
                Element c = (Element) cs.item(i);
                JsonObject o = new JsonObject();
                for (String[] k : new String[][] {{"origin", "Origin"}, {"method", "Method"}, {"responseHeader", "ResponseHeader"}}) {
                    NodeList l = c.getElementsByTagName(k[1]);
                    if (l.getLength() > 0) {
                        JsonArray a = new JsonArray();
                        for (int j = 0; j < l.getLength(); j++) {
                            a.add(l.item(j).getTextContent().trim());
                        }
                        o.add(k[0], a);
                    }
                }
                NodeList ma = c.getElementsByTagName("MaxAgeSec");
                if (ma.getLength() > 0) {
                    o.addProperty("maxAgeSeconds", Integer.parseInt(ma.item(0).getTextContent().trim()));
                }
                out.add(o);
            }
            st.mutateBucket(b.name, x -> {
                x.doc.add("cors", out);
                x.metageneration++;
                x.updated = Instant.now();
                return x;
            });
            w.empty(200);
        } else if (m.equals("DELETE")) {
            r.write = true;
            st.mutateBucket(b.name, x -> {
                x.doc.remove("cors");
                x.metageneration++;
                x.updated = Instant.now();
                return x;
            });
            w.empty(204);
        } else {
            throw GcsService.badMethod(m);
        }
    }

    private static Document parse(byte[] body) {
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            return f.newDocumentBuilder().parse(new ByteArrayInputStream(body));
        } catch (Exception e) {
            throw new GcsException(400, "invalid", "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.");
        }
    }

    private void createBucket(GcsReq r, GcsResp w, String bucket) throws IOException {
        String project = r.header("x-goog-project-id");
        if (project == null || project.isBlank()) {
            throw new GcsException(400, "invalid", "MissingSecurityHeader", "Your request was missing a required header.");
        }
        GcsOps.validateBucketName(bucket);
        byte[] body = r.readBody(1 << 20);
        Bucket b = new Bucket();
        b.name = bucket;
        b.project = project;
        b.created = Instant.now();
        b.updated = b.created;
        String loc = cfg.location;
        String sc = r.header("x-goog-storage-class");
        if (body.length > 0) {
            Document d = parse(body);
            NodeList l = d.getElementsByTagName("LocationConstraint");
            if (l.getLength() > 0 && !l.item(0).getTextContent().isBlank()) {
                loc = l.item(0).getTextContent().trim().toUpperCase(Locale.ROOT);
            }
            NodeList c = d.getElementsByTagName("StorageClass");
            if (c.getLength() > 0 && !c.item(0).getTextContent().isBlank()) {
                sc = c.item(0).getTextContent().trim();
            }
        }
        b.doc.addProperty("location", loc);
        b.doc.addProperty("locationType", java.util.Set.of("US", "EU", "ASIA").contains(loc) ? "multi-region" : "region");
        b.doc.addProperty("storageClass", sc == null ? "STANDARD" : sc.toUpperCase(Locale.ROOT));
        if (!GcsOps.CLASSES.contains(b.doc.get("storageClass").getAsString())) {
            throw new GcsException(400, "invalid", "InvalidStorageClass", "The storage class you specified is not valid.");
        }
        b.doc.add("acl", GcsJson.predefined(null, project, true));
        b.doc.add("defaultObjectAcl", GcsJson.predefined(null, project, false));
        JsonObject ic = new JsonObject();
        JsonObject off = new JsonObject();
        off.addProperty("enabled", false);
        ic.add("bucketPolicyOnly", off);
        ic.add("uniformBucketLevelAccess", off.deepCopy());
        ic.addProperty("publicAccessPrevention", "inherited");
        b.doc.add("iamConfiguration", ic);
        if (!st.createBucket(b)) {
            throw new GcsException(409, "conflict", "BucketAlreadyOwnedByYou",
                    "Your previous request to create the named bucket succeeded and you already own it.");
        }
        w.header("Location", "/" + bucket);
        w.empty(200);
    }

    // ------------------------------------------------------------------------------------------ listing

    private void listObjects(GcsReq r, GcsResp w, Bucket b) throws IOException {
        boolean v2 = "2".equals(r.q("list-type"));
        int max = 1000;
        if (r.q("max-keys") != null) {
            try {
                max = Integer.parseInt(r.q("max-keys").trim());
            } catch (NumberFormatException e) {
                throw new GcsException(400, "invalid", "InvalidArgument", "Invalid argument: max-keys");
            }
            if (max < 0) {
                throw new GcsException(400, "invalid", "InvalidArgument", "Invalid argument: max-keys");
            }
            max = Math.min(max, 1000);
        }
        String prefix = r.q("prefix") == null ? "" : r.q("prefix");
        String delim = r.q("delimiter");
        String afterName = null;
        long afterGen = -1;
        String cont = v2 ? r.q("continuation-token") : null;
        String marker = v2 ? r.q("start-after") : r.q("marker");
        if (cont != null && !cont.isEmpty()) {
            Object[] pos = GcsStore.decodeToken(cont);
            afterName = (String) pos[0];
            afterGen = (Long) pos[1];
        } else if (marker != null && !marker.isEmpty()) {
            afterName = marker;
            afterGen = Long.MAX_VALUE;
        }
        boolean versions = r.qBool("versions");
        GcsStore.ListSpec spec = new GcsStore.ListSpec(b.name, prefix, delim, null, null, versions, false, null, afterName, afterGen);
        GcsStore.Page p = st.list(spec, max);
        boolean enc = "url".equals(r.q("encoding-type"));
        StringBuilder sb = new StringBuilder(XML_HEAD + "<ListBucketResult xmlns='" + NS + "'><Name>" + esc(b.name) + "</Name><Prefix>"
                + esc(prefix) + "</Prefix>");
        if (v2) {
            sb.append("<KeyCount>").append(p.objects().size() + p.prefixes().size()).append("</KeyCount>");
            if (cont != null) {
                sb.append("<ContinuationToken>").append(esc(cont)).append("</ContinuationToken>");
            }
            if (marker != null) {
                sb.append("<StartAfter>").append(esc(marker)).append("</StartAfter>");
            }
        } else {
            sb.append("<Marker>").append(esc(marker == null ? "" : marker)).append("</Marker>");
        }
        sb.append("<MaxKeys>").append(max).append("</MaxKeys>");
        if (delim != null) {
            sb.append("<Delimiter>").append(esc(delim)).append("</Delimiter>");
        }
        sb.append("<IsTruncated>").append(p.nextToken() != null).append("</IsTruncated>");
        if (p.nextToken() != null) {
            if (v2) {
                sb.append("<NextContinuationToken>").append(esc(p.nextToken())).append("</NextContinuationToken>");
            } else {
                sb.append("<NextMarker>").append(esc(lastKey(p))).append("</NextMarker>");
            }
        }
        if (enc) {
            sb.append("<EncodingType>url</EncodingType>");
        }
        for (Obj o : p.objects()) {
            sb.append("<Contents><Key>").append(esc(enc ? GcsReq.enc(o.name).replace("%2F", "/") : o.name)).append("</Key><Generation>")
                    .append(o.generation).append("</Generation><MetaGeneration>").append(o.metageneration).append("</MetaGeneration>")
                    .append("<LastModified>").append(GcsJson.ts(o.updated)).append("</LastModified><ETag>")
                    .append(esc(GcsOps.etagHeader(o, true))).append("</ETag><Size>").append(o.size).append("</Size>");
            if (versions) {
                sb.append("<IsLatest>").append(o.live()).append("</IsLatest>");
            }
            sb.append("</Contents>");
        }
        for (String cp : p.prefixes()) {
            sb.append("<CommonPrefixes><Prefix>").append(esc(cp)).append("</Prefix></CommonPrefixes>");
        }
        sb.append("</ListBucketResult>");
        xml(w, 200, sb.toString());
    }

    private static String lastKey(GcsStore.Page p) {
        String a = p.objects().isEmpty() ? "" : p.objects().get(p.objects().size() - 1).name;
        String c = p.prefixes().isEmpty() ? "" : p.prefixes().get(p.prefixes().size() - 1);
        return a.compareTo(c) >= 0 ? a : c;
    }

    // ------------------------------------------------------------------------------------------ objects

    private static Pre xmlPre(GcsReq r) {
        return new Pre(hl(r, "x-goog-if-generation-match"), hl(r, "x-goog-if-generation-not-match"),
                hl(r, "x-goog-if-metageneration-match"), hl(r, "x-goog-if-metageneration-not-match"));
    }

    private static Long hl(GcsReq r, String h) {
        String v = r.header(h);
        if (v == null) {
            return null;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Invalid value for " + h);
        }
    }

    private static final Map<String, String> XML_ACL = Map.of("private", "private", "public-read", "publicRead", "authenticated-read",
            "authenticatedRead", "bucket-owner-read", "bucketOwnerRead", "bucket-owner-full-control", "bucketOwnerFullControl",
            "project-private", "projectPrivate", "public-read-write", "publicReadWrite");

    private JsonObject metaFromHeaders(GcsReq r) {
        JsonObject m = new JsonObject();
        String ct = r.header("content-type");
        m.addProperty("contentType", ct == null || ct.isBlank() ? "application/octet-stream" : ct);
        for (String[] k : new String[][] {{"content-encoding", "contentEncoding"}, {"content-disposition", "contentDisposition"},
            {"cache-control", "cacheControl"}, {"content-language", "contentLanguage"}, {"x-goog-custom-time", "customTime"},
            {"x-goog-storage-class", "storageClass"}}) {
            if (r.header(k[0]) != null) {
                m.addProperty(k[1], r.header(k[0]));
            }
        }
        JsonObject md = new JsonObject();
        r.headers.forEach((k, v) -> {
            if (k.startsWith("x-goog-meta-")) {
                md.addProperty(k.substring("x-goog-meta-".length()), v.get(0));
            }
        });
        if (md.size() > 0) {
            m.add("metadata", md);
        }
        return m;
    }

    private void putHeaders(GcsResp w, Obj o) {
        w.header("ETag", GcsOps.etagHeader(o, true));
        w.header("x-goog-generation", String.valueOf(o.generation));
        w.header("x-goog-metageneration", String.valueOf(o.metageneration));
        w.header("x-goog-hash", GcsHash.googHash(o.crc32c, o.md5));
        w.header("x-goog-stored-content-length", String.valueOf(o.size));
        w.header("x-goog-stored-content-encoding", o.contentEncoding == null ? "identity" : o.contentEncoding);
    }

    private void objectOp(GcsReq r, GcsResp w, Bucket b, String object) throws IOException {
        String m = r.method;
        String uploadId = r.q("uploadId");
        if (r.has("uploads") && m.equals("POST")) {
            r.op = "xml.initiateMultipart";
            r.write = true;
            initiateMpu(r, w, b, object);
            return;
        }
        if (uploadId != null) {
            mpuOp(r, w, b, object, uploadId);
            return;
        }
        if (r.q("upload_id") != null) {
            xmlResumable(r, w, b, object);
            return;
        }
        if (r.has("acl") || r.has("tagging") || r.has("legal-hold") || r.has("retention")) {
            throw new GcsException(501, "notImplemented", "NotImplemented", "The XML API sub-resource is not supported by gcswire; use the JSON API.");
        }
        switch (m) {
            case "GET", "HEAD" -> {
                r.op = "xml.getObject";
                Long gen = GcsOps.generation(r, "generation");
                Obj o = st.find(b.name, object, gen);
                if (o == null) {
                    throw GcsException.noSuchObject(b.name, object);
                }
                Pre pre = xmlPre(r);
                pre.check(o);
                ops.serve(r, w, o, true);
            }
            case "DELETE" -> {
                r.op = "xml.deleteObject";
                r.write = true;
                st.delete(b, b.name, object, GcsOps.generation(r, "generation"), xmlPre(r));
                w.empty(204);
            }
            case "POST" -> {
                if ("start".equalsIgnoreCase(r.header("x-goog-resumable"))) {
                    r.op = "xml.resumable.start";
                    r.write = true;
                    startXmlResumable(r, w, b, object);
                } else {
                    throw GcsService.badMethod(m);
                }
            }
            case "PUT" -> {
                r.write = true;
                if (r.header("x-goog-copy-source") != null || r.header("x-amz-copy-source") != null) {
                    r.op = "xml.copyObject";
                    copy(r, w, b, object);
                } else {
                    r.op = "xml.putObject";
                    put(r, w, b, object);
                }
            }
            default -> throw GcsService.badMethod(m);
        }
    }

    private void put(GcsReq r, GcsResp w, Bucket b, String object) throws IOException {
        GcsOps.validateObjectName(object);
        JsonObject meta = metaFromHeaders(r);
        Obj tmpl = ops.template(b, object, meta);
        String acl = r.header("x-goog-acl");
        if (acl != null) {
            String p = XML_ACL.get(acl);
            if (p == null) {
                throw new GcsException(400, "invalid", "InvalidArgument", "Invalid canned ACL: " + acl);
            }
            ops.applyPredefinedAcl(b, tmpl, p);
        }
        Map<String, String> expect = new LinkedHashMap<>(GcsHash.parseGoogHash(r.header("x-goog-hash")));
        String cmd5 = r.header("content-md5");
        if (cmd5 != null) {
            expect.put("md5", cmd5.trim());
        }
        Obj o = ops.upload(b, tmpl, r.body, -1, xmlPre(r), expect);
        putHeaders(w, o);
        w.empty(200);
    }

    private void copy(GcsReq r, GcsResp w, Bucket b, String object) throws IOException {
        GcsOps.validateObjectName(object);
        String src = r.header("x-goog-copy-source") != null ? r.header("x-goog-copy-source") : r.header("x-amz-copy-source");
        src = GcsReq.decode(src, false);
        Long gen = null;
        int q = src.indexOf("?generation=");
        if (q >= 0) {
            gen = Long.parseLong(src.substring(q + 12));
            src = src.substring(0, q);
        }
        if (src.startsWith("/")) {
            src = src.substring(1);
        }
        int slash = src.indexOf('/');
        if (slash <= 0) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Invalid x-goog-copy-source.");
        }
        String sb = src.substring(0, slash);
        String sn = src.substring(slash + 1);
        ops.requireBucket(sb);
        Obj so = st.find(sb, sn, gen);
        if (so == null) {
            throw GcsException.noSuchObject(sb, sn);
        }
        String directive = r.header("x-goog-metadata-directive");
        JsonObject meta = directive != null && directive.equalsIgnoreCase("REPLACE") ? metaFromHeaders(r) : null;
        Obj o = ops.copy(b, so, object, meta, xmlPre(r));
        putHeaders(w, o);
        xml(w, 200, XML_HEAD + "<CopyObjectResult><LastModified>" + GcsJson.ts(o.updated) + "</LastModified><ETag>"
                + esc(GcsOps.etagHeader(o, true)) + "</ETag></CopyObjectResult>");
    }

    // ------------------------------------------------------------------------------------------ XML multipart

    private void initiateMpu(GcsReq r, GcsResp w, Bucket b, String object) throws IOException {
        GcsOps.validateObjectName(object);
        JsonObject req = new JsonObject();
        req.add("meta", metaFromHeaders(r));
        req.add("pre", GcsResumable.preJson(xmlPre(r)));
        Session s = st.createSession("mpu", b.name, object, req);
        xml(w, 200, XML_HEAD + "<InitiateMultipartUploadResult xmlns='" + NS + "'><Bucket>" + esc(b.name) + "</Bucket><Key>"
                + esc(object) + "</Key><UploadId>" + s.id + "</UploadId></InitiateMultipartUploadResult>");
    }

    private Session requireMpu(String id, Bucket b, String object) {
        Session s = st.findSession(id, b.name, object);
        if (s == null || !"mpu".equals(s.kind) || !"open".equals(s.state)) {
            throw new GcsException(404, "notFound", "NoSuchUpload", "The specified multipart upload does not exist.");
        }
        return s;
    }

    private void mpuOp(GcsReq r, GcsResp w, Bucket b, String object, String uploadId) throws IOException {
        Session s = requireMpu(uploadId, b, object);
        String host = st.ownerHost(b.name, object);
        switch (r.method) {
            case "PUT" -> {
                r.op = "xml.uploadPart";
                r.write = true;
                int n;
                try {
                    n = Integer.parseInt(r.q("partNumber"));
                } catch (RuntimeException e) {
                    throw new GcsException(400, "invalid", "InvalidArgument", "Invalid partNumber.");
                }
                if (n < 1 || n > 10000) {
                    throw new GcsException(400, "invalid", "InvalidArgument", "Part number must be between 1 and 10000.");
                }
                GcsStore.Ingested ing = st.ingest(host, r.body, -1, false, b.name, object, "T", s.id, n);
                String hex = GcsHash.md5Hex(GcsHash.b64(ing.md5()));
                String cmd5 = r.header("content-md5");
                if (cmd5 != null && !cmd5.trim().equals(ing.md5())) {
                    st.dropData(host, List.of(ing.dataId()));
                    throw new GcsException(400, "invalid", "BadDigest", "The Content-MD5 you specified did not match what we received.");
                }
                st.putPart(s, n, ing, hex);
                w.header("ETag", "\"" + hex + "\"");
                w.header("x-goog-hash", GcsHash.googHash(GcsHash.crc32cB64(ing.crc()), ing.md5()));
                w.empty(200);
            }
            case "DELETE" -> {
                r.op = "xml.abortMultipart";
                r.write = true;
                st.cancelSession(s);
                w.empty(204);
            }
            case "GET" -> {
                r.op = "xml.listParts";
                StringBuilder sb = new StringBuilder(XML_HEAD + "<ListPartsResult xmlns='" + NS + "'><Bucket>" + esc(b.name) + "</Bucket><Key>"
                        + esc(object) + "</Key><UploadId>" + s.id + "</UploadId><IsTruncated>false</IsTruncated>");
                for (GcsStore.Part p : st.parts(s)) {
                    sb.append("<Part><PartNumber>").append(p.number()).append("</PartNumber><ETag>&quot;").append(p.etag())
                            .append("&quot;</ETag><Size>").append(p.size()).append("</Size></Part>");
                }
                xml(w, 200, sb.append("</ListPartsResult>").toString());
            }
            case "POST" -> {
                r.op = "xml.completeMultipart";
                r.write = true;
                completeMpu(r, w, b, object, s, host);
            }
            default -> throw GcsService.badMethod(r.method);
        }
    }

    private void completeMpu(GcsReq r, GcsResp w, Bucket b, String object, Session s, String host) throws IOException {
        Document d = parse(r.readBody(4 << 20));
        NodeList ps = d.getElementsByTagName("Part");
        Map<Integer, GcsStore.Part> have = new LinkedHashMap<>();
        for (GcsStore.Part p : st.parts(s)) {
            have.put(p.number(), p);
        }
        List<Seg> segs = new ArrayList<>();
        MessageDigest md = GcsHash.md5();
        int prev = 0;
        long size = 0;
        if (ps.getLength() == 0) {
            throw new GcsException(400, "invalid", "MalformedXML", "You must specify at least one part.");
        }
        for (int i = 0; i < ps.getLength(); i++) {
            Element e = (Element) ps.item(i);
            int n = Integer.parseInt(e.getElementsByTagName("PartNumber").item(0).getTextContent().trim());
            if (n <= prev) {
                throw new GcsException(400, "invalid", "InvalidPartOrder", "The list of parts was not in ascending order.");
            }
            prev = n;
            GcsStore.Part p = have.get(n);
            NodeList et = e.getElementsByTagName("ETag");
            String claimed = et.getLength() == 0 ? null : et.item(0).getTextContent().trim().replace("\"", "");
            if (p == null || claimed != null && !claimed.equalsIgnoreCase(p.etag())) {
                throw new GcsException(400, "invalid", "InvalidPart", "One or more of the specified parts could not be found or the "
                        + "specified ETag did not match.");
            }
            segs.add(new Seg(p.dataId(), p.size(), cfg.chunkBytes));
            size += p.size();
            md.update(hex(p.etag()));
        }
        String xmlEtag = GcsHash.md5Hex(md.digest()) + "-" + segs.size();
        JsonObject meta = s.request.has("meta") ? s.request.getAsJsonObject("meta") : new JsonObject();
        Obj tmpl = ops.template(b, object, meta);
        tmpl.doc.addProperty("xmlEtag", xmlEtag);
        tmpl.componentCount = segs.size() > 1 ? segs.size() : null;
        String crc = GcsHash.crc32cB64(st.crcOf(host, segs));
        Pre pre = s.request.has("pre") ? GcsResumable.preOf(s.request.getAsJsonObject("pre")) : Pre.NONE;
        Obj n = tmpl.copy();
        n.size = size;
        n.md5 = null;
        n.crc32c = crc;
        n.segments = segs;
        Obj o;
        try {
            o = st.commit(b, n, pre);
        } catch (RuntimeException e) {
            throw e;
        }
        st.cancelSession(s); // drops the parts that were not used; adopted ones now belong to the object
        putHeaders(w, o);
        xml(w, 200, XML_HEAD + "<CompleteMultipartUploadResult xmlns='" + NS + "'><Location>" + esc(r.baseUrl() + "/" + b.name + "/"
                + object) + "</Location><Bucket>" + esc(b.name) + "</Bucket><Key>" + esc(object) + "</Key><ETag>&quot;" + xmlEtag
                + "&quot;</ETag></CompleteMultipartUploadResult>");
    }

    private static byte[] hex(String h) {
        byte[] out = new byte[h.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(h.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    private void listUploads(GcsReq r, GcsResp w, Bucket b) throws IOException {
        r.op = "xml.listMultipartUploads";
        StringBuilder sb = new StringBuilder(XML_HEAD + "<ListMultipartUploadsResult xmlns='" + NS + "'><Bucket>" + esc(b.name)
                + "</Bucket><IsTruncated>false</IsTruncated>");
        String prefix = r.q("prefix") == null ? "" : r.q("prefix");
        for (Session s : st.listSessions(b.name, "mpu")) {
            if (s.name.startsWith(prefix)) {
                sb.append("<Upload><Key>").append(esc(s.name)).append("</Key><UploadId>").append(s.id).append("</UploadId></Upload>");
            }
        }
        xml(w, 200, sb.append("</ListMultipartUploadsResult>").toString());
    }

    // ------------------------------------------------------------------------------------------ XML resumable

    private void startXmlResumable(GcsReq r, GcsResp w, Bucket b, String object) throws IOException {
        Pre pre = xmlPre(r);
        JsonObject req = new JsonObject();
        req.add("meta", metaFromHeaders(r));
        req.add("pre", GcsResumable.preJson(pre));
        req.addProperty("xml", true);
        Session s = resumable.start(b, object, req, pre);
        w.header("Location", r.baseUrl() + "/" + GcsReq.enc(b.name) + "/" + GcsReq.enc(object).replace("%2F", "/") + "?upload_id=" + s.id);
        w.header("X-GUploader-UploadID", s.id);
        xml(w, 201, XML_HEAD + "<InitiateResumableUploadResult><Bucket>" + esc(b.name) + "</Bucket><Key>" + esc(object)
                + "</Key></InitiateResumableUploadResult>");
    }

    private void xmlResumable(GcsReq r, GcsResp w, Bucket b, String object) throws IOException {
        Session s = resumable.require(r.q("upload_id"), b.name, object);
        r.write = true;
        if (r.method.equals("DELETE")) {
            r.op = "xml.resumable.cancel";
            resumable.cancel(s);
            w.empty(499);
            return;
        }
        if (!r.method.equals("PUT")) {
            throw GcsService.badMethod(r.method);
        }
        r.op = "xml.resumable.put";
        GcsResumable.Outcome out = resumable.put(r, s);
        w.header("X-GUploader-UploadID", s.id);
        if (out.object() != null) {
            putHeaders(w, out.object());
            w.empty(200);
        } else if (out.session().result != null) {
            w.empty(200);
        } else {
            w.header("Range", GcsRange.rangeHeader(out.persisted()));
            w.empty(308);
        }
    }
}
