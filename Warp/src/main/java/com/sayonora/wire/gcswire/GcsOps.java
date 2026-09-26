package com.sayonora.wire.gcswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.wire.gcswire.GcsModel.Bucket;
import com.sayonora.wire.gcswire.GcsModel.Obj;
import com.sayonora.wire.gcswire.GcsModel.Pre;
import com.sayonora.wire.gcswire.GcsModel.Seg;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/** Object semantics shared by the JSON and the XML API: validation, metadata, upload finalisation, copy, compose, download. */
final class GcsOps {

    static final Set<String> CLASSES = Set.of("STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE", "MULTI_REGIONAL", "REGIONAL",
            "DURABLE_REDUCED_AVAILABILITY");
    private static final Pattern BUCKET = Pattern.compile("[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]");
    private static final Pattern IPV4 = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
    static final DateTimeFormatter HTTP_DATE = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
            .withZone(ZoneOffset.UTC);

    final GcsStore st;
    final GcsConfig cfg;

    GcsOps(GcsStore st, GcsConfig cfg) {
        this.st = st;
        this.cfg = cfg;
    }

    // ------------------------------------------------------------------------------------------ validation

    static void validateBucketName(String n) {
        boolean ok = n != null && (BUCKET.matcher(n).matches() || n.length() > 63 && n.length() <= 222 && n.contains(".")
                && Pattern.matches("[a-z0-9][a-z0-9._-]*[a-z0-9]", n));
        if (ok && n.contains(".")) {
            for (String label : n.split("\\.", -1)) {
                if (label.isEmpty() || label.length() > 63) {
                    ok = false;
                }
            }
        }
        if (!ok || IPV4.matcher(n).matches() || n.startsWith("goog") || n.contains("google") || n.contains("..")) {
            throw new GcsException(400, "invalid", "InvalidBucketName", "Invalid bucket name: '" + n + "'");
        }
    }

    static void validateObjectName(String n) {
        if (n == null || n.isEmpty()) {
            throw GcsException.required("Required");
        }
        if (n.getBytes(StandardCharsets.UTF_8).length > 1024 || n.equals(".") || n.equals("..") || n.indexOf('\r') >= 0
                || n.indexOf('\n') >= 0 || n.startsWith(".well-known/acme-challenge/")) {
            throw new GcsException(400, "invalid", "InvalidObjectName", "Invalid object name.");
        }
    }

    Bucket requireBucket(String name) {
        Bucket b = st.getBucket(name);
        if (b == null) {
            throw GcsException.noSuchBucket();
        }
        return b;
    }

    // ------------------------------------------------------------------------------------------ preconditions

    private static Long longParam(GcsReq r, String name) {
        String v = r.q(name);
        if (v == null) {
            return null;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw GcsException.invalid("Invalid value for " + name + ": " + v).at("parameter", name);
        }
    }

    /** ifGenerationMatch... (prefix "if") or ifSourceGenerationMatch... (prefix "ifSource") */
    static Pre pre(GcsReq r, String prefix) {
        return new Pre(longParam(r, prefix + "GenerationMatch"), longParam(r, prefix + "GenerationNotMatch"),
                longParam(r, prefix + "MetagenerationMatch"), longParam(r, prefix + "MetagenerationNotMatch"));
    }

    static Long generation(GcsReq r, String name) {
        return longParam(r, name);
    }

    // ------------------------------------------------------------------------------------------ metadata

    private static String str(JsonObject o, String k) {
        JsonElement e = o.get(k);
        if (e == null || e.isJsonNull()) {
            return null;
        }
        if (!e.isJsonPrimitive()) {
            throw GcsException.invalid("Invalid value for " + k + ".");
        }
        return e.getAsString();
    }

    /**
     * Applies user-supplied object metadata. {@code merge} (PATCH): only present fields change, a JSON null clears one and a
     * {@code metadata} key with null is removed; otherwise (insert / PUT update) absent mutable fields are cleared.
     */
    static void applyMeta(Obj o, JsonObject m, boolean merge) {
        String[] simple = {"contentType", "contentEncoding", "contentDisposition", "cacheControl", "contentLanguage", "customTime"};
        for (String k : simple) {
            if (m.has(k) || !merge) {
                String v = str(m, k);
                switch (k) {
                    case "contentType" -> o.contentType = v;
                    case "contentEncoding" -> o.contentEncoding = v;
                    case "contentDisposition" -> o.contentDisposition = v;
                    case "cacheControl" -> o.cacheControl = v;
                    case "contentLanguage" -> o.contentLanguage = v;
                    default -> o.customTime = v;
                }
            }
        }
        if (m.has("metadata") || !merge) {
            JsonElement e = m.get("metadata");
            if (e != null && !e.isJsonNull() && !e.isJsonObject()) {
                throw GcsException.invalid("Invalid value for metadata.");
            }
            if (!merge || e == null || e.isJsonNull()) {
                o.metadata = new LinkedHashMap<>();
            }
            if (e != null && e.isJsonObject()) {
                for (Map.Entry<String, JsonElement> en : e.getAsJsonObject().entrySet()) {
                    if (en.getValue().isJsonNull()) {
                        o.metadata.remove(en.getKey());
                    } else {
                        o.metadata.put(en.getKey(), en.getValue().getAsString());
                    }
                }
            }
        }
        if (m.has("storageClass") && !m.get("storageClass").isJsonNull()) {
            String sc = m.get("storageClass").getAsString().toUpperCase(Locale.ROOT);
            if (!CLASSES.contains(sc)) {
                throw GcsException.invalid("Invalid storage class: " + sc);
            }
            if (!sc.equals(o.storageClass)) {
                o.classUpdated = Instant.now().truncatedTo(ChronoUnit.MICROS);
            }
            o.storageClass = sc;
        }
        for (String h : new String[] {"eventBasedHold", "temporaryHold"}) {
            if (m.has(h)) {
                if (m.get(h).isJsonNull()) {
                    o.doc.remove(h);
                } else {
                    o.doc.addProperty(h, m.get(h).getAsBoolean());
                }
            }
        }
        if (m.has("acl") && m.get("acl").isJsonArray()) {
            JsonArray acl = new JsonArray();
            for (JsonElement e : m.getAsJsonArray("acl")) {
                JsonObject a = e.getAsJsonObject();
                JsonObject n = new JsonObject();
                n.add("entity", a.get("entity"));
                n.add("role", a.get("role"));
                acl.add(n);
            }
            o.doc.add("acl", acl);
        }
        if (m.has("kmsKeyName") && !m.get("kmsKeyName").isJsonNull()) {
            o.doc.addProperty("kmsKeyName", m.get("kmsKeyName").getAsString());
        }
    }

    /** A new object template for {@code name} with the bucket's defaults and the given user metadata. */
    Obj template(Bucket b, String name, JsonObject meta) {
        Obj o = new Obj();
        o.bucket = b.name;
        o.name = name;
        o.storageClass = b.doc.has("storageClass") ? b.doc.get("storageClass").getAsString() : "STANDARD";
        if (meta != null) {
            applyMeta(o, meta, false);
        }
        if (o.contentType == null) {
            o.contentType = "application/octet-stream";
        }
        applyRetention(b, o);
        return o;
    }

    static void applyRetention(Bucket b, Obj o) {
        if (b.doc.has("retentionPolicy") && b.doc.getAsJsonObject("retentionPolicy").has("retentionPeriod")) {
            long p = b.doc.getAsJsonObject("retentionPolicy").get("retentionPeriod").getAsLong();
            o.doc.addProperty("retentionExpirationTime", GcsJson.ts(Instant.now().plusSeconds(p)));
        }
        if (b.doc.has("defaultEventBasedHold") && b.doc.get("defaultEventBasedHold").getAsBoolean()) {
            o.doc.addProperty("eventBasedHold", true);
        }
    }

    void applyPredefinedAcl(Bucket b, Obj o, String predefined) {
        if (predefined == null) {
            return;
        }
        if (!GcsJson.validPredefined(predefined, false)) {
            throw GcsException.invalid("Invalid Value").at("parameter", "predefinedAcl");
        }
        o.doc.add("acl", GcsJson.predefined(predefined, b.project, false));
    }

    // ------------------------------------------------------------------------------------------ writing

    /** Verifies client-declared hashes against what was stored. */
    static void checkHashes(Map<String, String> expected, String md5, String crc) {
        String em = expected.get("md5");
        String ec = expected.get("crc32c");
        if (ec != null && !ec.equals(crc)) {
            throw new GcsException(400, "invalid", "BadDigest", "Provided CRC32C \"" + ec + "\" doesn't match calculated CRC32C \"" + crc + "\".");
        }
        if (em != null && md5 != null && !em.equals(md5)) {
            throw new GcsException(400, "invalid", "BadDigest", "Provided MD5 hash \"" + em + "\" doesn't match calculated MD5 hash \"" + md5 + "\".");
        }
    }

    /** Streams {@code body} into a new generation of the template object (single-request upload). */
    Obj upload(Bucket b, Obj tmpl, InputStream body, long max, Pre pre, Map<String, String> expected) throws IOException {
        String host = st.ownerHost(b.name, tmpl.name);
        // fail fast on a precondition that can already be decided
        Obj live = st.find(b.name, tmpl.name, null);
        pre.check(live);
        GcsStore.Ingested ing = st.ingest(host, body, max, false, b.name, tmpl.name, "T", "", 0);
        try {
            checkHashes(expected, ing.md5(), GcsHash.crc32cB64(ing.crc()));
        } catch (GcsException e) {
            st.dropData(host, List.of(ing.dataId()));
            throw e;
        }
        return publish(b, tmpl, List.of(new Seg(ing.dataId(), ing.size(), cfg.chunkBytes)), ing.size(), ing.md5(),
                GcsHash.crc32cB64(ing.crc()), pre, host);
    }

    Obj publish(Bucket b, Obj tmpl, List<Seg> segs, long size, String md5, String crc, Pre pre, String host) {
        Obj n = tmpl.copy();
        n.size = size;
        n.md5 = md5;
        n.crc32c = crc;
        n.segments = new ArrayList<>(segs);
        try {
            return st.commit(b, n, pre);
        } catch (RuntimeException e) {
            st.dropData(host, segs.stream().map(Seg::d).toList());
            throw e;
        }
    }

    /** Server-side copy of {@code src} to (dst bucket, name); {@code meta} non-null replaces the source's metadata. */
    Obj copy(Bucket dstBucket, Obj src, String dstName, JsonObject meta, Pre dstPre) {
        String srcHost = st.ownerHost(src.bucket, src.name);
        String dstHost = st.ownerHost(dstBucket.name, dstName);
        Obj tmpl = src.copy();
        tmpl.bucket = dstBucket.name;
        tmpl.name = dstName;
        tmpl.deleted = null;
        tmpl.doc.remove("retentionExpirationTime");
        tmpl.doc.remove("acl");
        if (meta != null && meta.size() > 0) {
            String keepClass = tmpl.storageClass;
            tmpl.metadata = new LinkedHashMap<>();
            tmpl.contentType = null;
            tmpl.contentEncoding = null;
            tmpl.contentDisposition = null;
            tmpl.cacheControl = null;
            tmpl.contentLanguage = null;
            tmpl.customTime = null;
            applyMeta(tmpl, meta, false);
            if (!meta.has("storageClass")) {
                tmpl.storageClass = keepClass;
            }
            if (tmpl.contentType == null) {
                tmpl.contentType = src.contentType;
            }
        }
        applyRetention(dstBucket, tmpl);
        dstPre.check(st.find(dstBucket.name, dstName, null));
        List<Seg> segs = st.copySegments(srcHost, src.segments, dstHost, dstBucket.name, dstName);
        return publish(dstBucket, tmpl, segs, src.size, src.md5, src.crc32c, dstPre, dstHost);
    }

    /** Concatenates {@code sources} (already validated) into (bucket, name). */
    Obj compose(Bucket b, String name, List<Obj> sources, JsonObject destMeta, Pre pre) {
        String dstHost = st.ownerHost(b.name, name);
        Obj tmpl = template(b, name, destMeta);
        pre.check(st.find(b.name, name, null));
        List<Seg> segs = new ArrayList<>();
        long size = 0;
        int components = 0;
        try {
            for (Obj s : sources) {
                segs.addAll(st.copySegments(st.ownerHost(s.bucket, s.name), s.segments, dstHost, b.name, name));
                size += s.size;
                components += s.componentCount == null ? 1 : s.componentCount;
            }
        } catch (RuntimeException e) {
            st.dropData(dstHost, segs.stream().map(Seg::d).toList());
            throw e;
        }
        if (components > 1024) {
            st.dropData(dstHost, segs.stream().map(Seg::d).toList());
            throw GcsException.invalid("The total component count after composition would exceed the maximum allowed (1024).");
        }
        String crc = GcsHash.crc32cB64(st.crcOf(dstHost, segs));
        Obj n = tmpl.copy();
        n.componentCount = components;
        n.size = size;
        n.md5 = null;
        n.crc32c = crc;
        n.segments = segs;
        try {
            return st.commit(b, n, pre);
        } catch (RuntimeException e) {
            st.dropData(dstHost, segs.stream().map(Seg::d).toList());
            throw e;
        }
    }

    // ------------------------------------------------------------------------------------------ reading

    /** An InputStream over an object's bytes, one chunk slice per read (each a short borrow of a connection). */
    final class ObjectStream extends InputStream {
        private final String host;
        private final List<Seg> segs;
        private long pos;
        private final long end;
        private byte[] cur = new byte[0];
        private int curPos;

        ObjectStream(String host, List<Seg> segs, long start, long end) {
            this.host = host;
            this.segs = segs;
            this.pos = start;
            this.end = end;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (curPos >= cur.length) {
                if (pos > end) {
                    return -1;
                }
                long to = Math.min(end, pos + cfg.chunkBytes - 1);
                var out = new java.io.ByteArrayOutputStream();
                st.stream(host, segs, pos, to, out);
                cur = out.toByteArray();
                curPos = 0;
                pos = to + 1;
            }
            int n = Math.min(len, cur.length - curPos);
            System.arraycopy(cur, curPos, b, off, n);
            curPos += n;
            return n;
        }
    }

    private static String unquote(String etag) {
        String t = etag.trim();
        if (t.startsWith("W/")) {
            t = t.substring(2);
        }
        if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
            t = t.substring(1, t.length() - 1);
        }
        return t;
    }

    /** The ETag header value: quoted md5 hex on the XML API (as GCS does), the object's opaque etag on the JSON API. */
    static String etagHeader(Obj o, boolean xml) {
        if (xml && o.md5 != null) {
            return "\"" + GcsHash.md5Hex(GcsHash.b64(o.md5)) + "\"";
        }
        if (xml && o.doc.has("xmlEtag")) {
            return "\"" + o.doc.get("xmlEtag").getAsString() + "\"";
        }
        return "\"" + o.etag() + "\"";
    }

    private static Instant httpDate(String v) {
        try {
            return ZonedDateTime.parse(v.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Sets the metadata response headers of an object (both APIs). */
    void objectHeaders(GcsResp w, Obj o, boolean xml) {
        w.header("ETag", etagHeader(o, xml));
        w.header("Last-Modified", HTTP_DATE.format(o.updated));
        w.header("x-goog-generation", String.valueOf(o.generation));
        w.header("x-goog-metageneration", String.valueOf(o.metageneration));
        w.header("x-goog-stored-content-length", String.valueOf(o.size));
        w.header("x-goog-stored-content-encoding", o.contentEncoding == null ? "identity" : o.contentEncoding);
        w.header("x-goog-hash", GcsHash.googHash(o.crc32c, o.md5));
        w.header("x-goog-storage-class", o.storageClass);
        w.header("Cache-Control", o.cacheControl);
        w.header("Content-Disposition", o.contentDisposition);
        w.header("Content-Language", o.contentLanguage);
        if (o.componentCount != null) {
            w.header("x-goog-component-count", String.valueOf(o.componentCount));
        }
        if (xml) {
            o.metadata.forEach((k, v) -> w.header("x-goog-meta-" + k, v));
        }
    }

    /** Serves an object (or a range of it): conditionals, Range, decompressive transcoding. GET and HEAD. */
    void serve(GcsReq r, GcsResp w, Obj o, boolean xml) throws IOException {
        w.headOnly = "HEAD".equals(r.method);
        // conditionals on the headers
        String inm = r.header("if-none-match");
        String im = r.header("if-match");
        String ims = r.header("if-modified-since");
        String iums = r.header("if-unmodified-since");
        Set<String> tags = new java.util.HashSet<>(List.of(unquote(etagHeader(o, xml)), o.etag(), o.md5 == null ? "-" : GcsHash.md5Hex(GcsHash.b64(o.md5))));
        if (im != null && !"*".equals(im.trim()) && java.util.Arrays.stream(im.split(",")).noneMatch(t -> tags.contains(unquote(t)))) {
            throw new GcsException(412, "conditionNotMet", "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold.").at("header", "If-Match");
        }
        Instant t;
        if (iums != null && (t = httpDate(iums)) != null && o.updated.truncatedTo(ChronoUnit.SECONDS).isAfter(t)) {
            throw new GcsException(412, "conditionNotMet", "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold.").at("header", "If-Unmodified-Since");
        }
        boolean notModified = inm != null && ("*".equals(inm.trim()) || java.util.Arrays.stream(inm.split(",")).anyMatch(x -> tags.contains(unquote(x))));
        if (!notModified && inm == null && ims != null && (t = httpDate(ims)) != null && !o.updated.truncatedTo(ChronoUnit.SECONDS).isAfter(t)) {
            notModified = true;
        }
        objectHeaders(w, o, xml);
        String host = st.ownerHost(o.bucket, o.name);
        w.header("Accept-Ranges", "bytes");
        if (notModified) {
            w.status(304);
            w.header("Content-Length", "0");
            w.body();
            return;
        }
        boolean gzipStored = o.contentEncoding != null && o.contentEncoding.equalsIgnoreCase("gzip");
        String ae = r.header("accept-encoding");
        boolean transcode = gzipStored && (ae == null || !ae.toLowerCase(Locale.ROOT).contains("gzip"));
        w.header("Content-Type", o.contentType == null ? "application/octet-stream" : o.contentType);
        if (transcode) {
            w.status(200);
            w.headers.remove("x-goog-stored-content-length");
            w.header("x-goog-stored-content-length", String.valueOf(o.size));
            w.headers.remove("Accept-Ranges");
            OutputStream out = w.body();
            if (!w.headOnly) {
                try (InputStream in = new GZIPInputStream(new ObjectStream(host, o.segments, 0, o.size - 1))) {
                    in.transferTo(out);
                }
            }
            return;
        }
        w.header("Content-Encoding", o.contentEncoding);
        GcsRange.Span span = GcsRange.parseRange(r.header("range"), o.size);
        if (span != null && span.start() < 0) {
            throw new GcsException(416, "requestedRangeNotSatisfiable", "InvalidRange", "The requested range cannot be satisfied.")
                    .header("Content-Range", "bytes */" + o.size);
        }
        long start = 0;
        long end = o.size - 1;
        if (span != null) {
            start = span.start();
            end = span.end();
            w.status(206);
            w.header("Content-Range", "bytes " + start + "-" + end + "/" + o.size);
        } else {
            w.status(200);
        }
        w.header("Content-Length", String.valueOf(o.size == 0 ? 0 : end - start + 1));
        OutputStream out = w.body();
        if (!w.headOnly && o.size > 0) {
            st.stream(host, o.segments, start, end, out);
        }
    }
}
