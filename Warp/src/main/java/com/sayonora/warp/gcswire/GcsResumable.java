package com.sayonora.warp.gcswire;

import com.google.gson.JsonObject;
import com.sayonora.warp.gcswire.GcsModel.Bucket;
import com.sayonora.warp.gcswire.GcsModel.Obj;
import com.sayonora.warp.gcswire.GcsModel.Pre;
import com.sayonora.warp.gcswire.GcsModel.Seg;
import com.sayonora.warp.gcswire.GcsModel.Session;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The resumable upload protocol (JSON and XML APIs): a session row on the owning shard, each accepted chunk becomes one data
 * segment (no byte is ever copied again), the final chunk turns the segment list into a new object generation. Alignment
 * rule: every chunk but the last must be a multiple of 256 KiB; the server persists only the aligned prefix of a non-final
 * chunk and reports the persisted size in {@code Range}, so the client re-sends the rest.
 */
final class GcsResumable {

    /** Result of a chunk / status request. */
    record Outcome(Session session, long persisted, Obj object) {
        boolean done() {
            return object != null || session.result != null;
        }
    }

    private final GcsStore st;
    private final GcsOps ops;

    GcsResumable(GcsStore st, GcsOps ops) {
        this.st = st;
        this.ops = ops;
    }

    /**
     * Starts a session. {@code request} carries: {@code meta} (object metadata JSON), {@code pre} (preconditions), {@code params}
     * (predefinedAcl, projection, contentEncoding), {@code declaredLength}, {@code expect} (declared hashes), {@code xml}.
     */
    Session start(Bucket b, String name, JsonObject request, Pre pre) {
        GcsOps.validateObjectName(name);
        pre.check(st.find(b.name, name, null));
        return st.createSession("resumable", b.name, name, request);
    }

    Session require(String id, String bucket, String name) {
        Session s = id == null ? null : st.findSession(id, bucket, name);
        if (s == null || !"resumable".equals(s.kind) || "cancelled".equals(s.state)
                || bucket != null && !bucket.equals(s.bucket)) {
            throw new GcsException(404, "notFound", "NoSuchUpload", "Not Found");
        }
        return s;
    }

    /** Handles one PUT of the session URI: a chunk, a final empty request or a status query. */
    Outcome put(GcsReq r, Session s) throws IOException {
        if ("done".equals(s.state)) {
            drain(r.body);
            return new Outcome(s, s.persisted, null);
        }
        String crHeader = r.header("content-range");
        long contentLength = r.contentLength();
        GcsRange.ContentRange cr;
        if (crHeader == null) {
            // whole body in one request
            cr = contentLength == 0 ? new GcsRange.ContentRange(-1, -1, 0)
                    : new GcsRange.ContentRange(0, contentLength - 1, contentLength >= 0 ? contentLength : -1);
            if (contentLength < 0) {
                cr = null;
            }
        } else {
            cr = GcsRange.parseContentRange(crHeader);
            if (cr == null) {
                throw GcsException.invalid("Invalid request. The Content-Range header \"" + crHeader + "\" is not valid.");
            }
        }
        String host = st.ownerHost(s.bucket, s.name);
        long persisted = s.persisted;
        if (cr == null) {
            // no length and no range: read the entire (chunked) body as the final chunk
            long before = persisted;
            GcsStore.Ingested ing = st.ingest(host, r.body, -1, false, s.bucket, s.name, "S", s.id, 0);
            return finalizeAfter(r, s, before, ing);
        }
        if (cr.query()) {
            drain(r.body);
            if (cr.total() >= 0) {
                if (cr.total() == persisted) {
                    return new Outcome(s, persisted, finalizeObject(r, s));
                }
                if (cr.total() < persisted) {
                    throw GcsException.invalid("Invalid request. According to the Content-Range header, the upload size is "
                            + cr.total() + " byte(s), which is less than the already uploaded size of " + persisted + " byte(s).");
                }
            }
            return new Outcome(s, persisted, null);
        }
        if (contentLength >= 0 && contentLength != cr.length()) {
            drain(r.body);
            throw GcsException.invalid("Invalid request. The Content-Length of " + contentLength
                    + " does not match the Content-Range " + crHeader + ".");
        }
        if (cr.first() > persisted) {
            drain(r.body);
            throw GcsException.invalid("Invalid request. According to the Content-Range header, the upload offset is " + cr.first()
                    + " byte(s), which exceeds already uploaded size of " + persisted + " byte(s).");
        }
        boolean last = cr.total() >= 0 && cr.last() + 1 == cr.total();
        long skip = persisted - cr.first();
        long remaining = cr.length() - skip;
        if (remaining <= 0) {
            drain(r.body);
            return new Outcome(s, persisted, last && cr.total() == persisted ? finalizeObject(r, s) : null);
        }
        if (skip > 0) {
            InputStream in = r.body;
            long left = skip;
            byte[] tmp = new byte[8192];
            while (left > 0) {
                int n = in.read(tmp, 0, (int) Math.min(tmp.length, left));
                if (n < 0) {
                    break;
                }
                left -= n;
            }
        }
        long keep = GcsRange.persistable(remaining, last);
        if (keep > 0) {
            GcsStore.Ingested ing = st.ingest(host, r.body, keep, true, s.bucket, s.name, "S", s.id, 0);
            if (ing.size() != keep) {
                st.dropData(host, List.of(ing.dataId()));
                throw GcsException.invalid("Upload interrupted: " + ing.size() + " of " + keep + " bytes were received.");
            }
            if (!st.appendSegment(s, new Seg(ing.dataId(), ing.size(), ops.cfg.chunkBytes), persisted)) {
                st.dropData(host, List.of(ing.dataId()));
                throw new GcsException(503, "backendError", "ServiceUnavailable", "Concurrent upload to the same session; retry.");
            }
            s.segments.add(new Seg(ing.dataId(), ing.size(), ops.cfg.chunkBytes));
            s.persisted = persisted + ing.size();
        } else {
            drain(r.body);
        }
        if (last) {
            if (s.persisted != cr.total()) {
                throw GcsException.invalid("Invalid request. The upload has " + s.persisted + " byte(s) but the Content-Range says " + cr.total() + ".");
            }
            return new Outcome(s, s.persisted, finalizeObject(r, s));
        }
        return new Outcome(s, s.persisted, null);
    }

    private Outcome finalizeAfter(GcsReq r, Session s, long before, GcsStore.Ingested ing) {
        if (!st.appendSegment(s, new Seg(ing.dataId(), ing.size(), ops.cfg.chunkBytes), before)) {
            st.dropData(st.ownerHost(s.bucket, s.name), List.of(ing.dataId()));
            throw new GcsException(503, "backendError", "ServiceUnavailable", "Concurrent upload to the same session; retry.");
        }
        s.segments.add(new Seg(ing.dataId(), ing.size(), ops.cfg.chunkBytes));
        s.persisted = before + ing.size();
        return new Outcome(s, s.persisted, finalizeObject(r, s));
    }

    private static void drain(InputStream in) throws IOException {
        byte[] b = new byte[8192];
        while (in.read(b) >= 0) {
            // discard
        }
    }

    /** Turns the session's segments into a new generation; on any failure the session and its data are dropped. */
    private Obj finalizeObject(GcsReq r, Session s) {
        String host = st.ownerHost(s.bucket, s.name);
        try {
            Bucket b = ops.requireBucket(s.bucket);
            JsonObject req = s.request;
            JsonObject meta = req.has("meta") ? req.getAsJsonObject("meta") : new JsonObject();
            long size = s.persisted;
            if (req.has("declaredLength") && req.get("declaredLength").getAsLong() != size) {
                throw GcsException.invalid("Invalid request. The upload size (" + size + " byte(s)) does not match the declared "
                        + "X-Upload-Content-Length (" + req.get("declaredLength").getAsLong() + " byte(s)).");
            }
            String[] h = st.hashesOf(host, s.segments);
            Map<String, String> expect = new LinkedHashMap<>();
            if (req.has("expect")) {
                req.getAsJsonObject("expect").entrySet().forEach(e -> expect.put(e.getKey(), e.getValue().getAsString()));
            }
            expect.putAll(GcsHash.parseGoogHash(r.header("x-goog-hash")));
            GcsOps.checkHashes(expect, h[0], h[1]);
            Obj tmpl = ops.template(b, s.name, meta);
            JsonObject params = req.has("params") ? req.getAsJsonObject("params") : new JsonObject();
            if (params.has("contentEncoding")) {
                tmpl.contentEncoding = params.get("contentEncoding").getAsString();
            }
            if (params.has("predefinedAcl")) {
                ops.applyPredefinedAcl(b, tmpl, params.get("predefinedAcl").getAsString());
            }
            if (req.has("contentType") && !meta.has("contentType")) {
                tmpl.contentType = req.get("contentType").getAsString();
            }
            Pre pre = req.has("pre") ? preOf(req.getAsJsonObject("pre")) : Pre.NONE;
            Obj o = ops.publish(b, tmpl, s.segments, size, h[0], h[1], pre, host);
            st.finishSession(s, GcsJson.object(o, "@BASE@", "noAcl"));
            s.state = "done";
            return o;
        } catch (RuntimeException e) {
            st.cancelSession(s);
            throw e;
        }
    }

    static JsonObject preJson(Pre p) {
        JsonObject o = new JsonObject();
        if (p.genMatch() != null) {
            o.addProperty("gm", p.genMatch());
        }
        if (p.genNotMatch() != null) {
            o.addProperty("gnm", p.genNotMatch());
        }
        if (p.metaMatch() != null) {
            o.addProperty("mm", p.metaMatch());
        }
        if (p.metaNotMatch() != null) {
            o.addProperty("mnm", p.metaNotMatch());
        }
        return o;
    }

    static Pre preOf(JsonObject o) {
        return new Pre(o.has("gm") ? o.get("gm").getAsLong() : null, o.has("gnm") ? o.get("gnm").getAsLong() : null,
                o.has("mm") ? o.get("mm").getAsLong() : null, o.has("mnm") ? o.get("mnm").getAsLong() : null);
    }

    void cancel(Session s) {
        st.cancelSession(s);
    }

    /** Placeholder so callers can list what they need without importing collections. */
    static List<String> none() {
        return new ArrayList<>();
    }
}
