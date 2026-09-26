package com.sayonora.warp.gcswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sayonora.warp.gcswire.GcsModel.Bucket;
import com.sayonora.warp.gcswire.GcsModel.HmacKey;
import com.sayonora.warp.gcswire.GcsModel.Obj;
import com.sayonora.warp.gcswire.GcsModel.Pre;
import com.sayonora.warp.gcswire.GcsModel.Session;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/** The GCS JSON API: storage/v1 (buckets, objects, ACLs, IAM, notifications, HMAC keys), the upload and download endpoints, batch. */
final class GcsService {

    private static final long MAX_JSON = 4 << 20;
    private static final SecureRandom RND = new SecureRandom();

    final GcsStore st;
    final GcsConfig cfg;
    final GcsOps ops;
    final GcsResumable resumable;

    GcsService(GcsStore st, GcsConfig cfg) {
        this.st = st;
        this.cfg = cfg;
        this.ops = new GcsOps(st, cfg);
        this.resumable = new GcsResumable(st, ops);
    }

    /** @return true when the path belongs to the JSON API */
    static boolean isJsonPath(List<String> s) {
        if (s.isEmpty()) {
            return false;
        }
        return switch (s.get(0)) {
            case "storage", "batch" -> s.size() >= 2 && s.get(1).equals("v1") || s.get(0).equals("batch");
            case "upload", "download" -> s.size() >= 3 && s.get(1).equals("storage");
            case "resumable" -> s.size() >= 4 && s.get(1).equals("upload");
            default -> false;
        };
    }

    void handle(GcsReq r, GcsResp w, List<String> s, boolean inBatch) throws IOException {
        w.header("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
        String m = r.method;
        String head = s.get(0);
        if (head.equals("batch")) {
            r.op = "batch";
            r.write = true;
            batch(r, w);
            return;
        }
        if (head.equals("upload") || head.equals("resumable")) {
            List<String> t = s.subList(head.equals("upload") ? 3 : 4, s.size());
            upload(r, w, t);
            return;
        }
        if (head.equals("download")) {
            List<String> t = s.subList(3, s.size());
            if (t.size() == 4 && t.get(0).equals("b") && t.get(2).equals("o") && (m.equals("GET") || m.equals("HEAD"))) {
                r.op = "objects.get";
                getObject(r, w, t.get(1), t.get(3), true);
                return;
            }
            throw notFound();
        }
        List<String> t = s.subList(2, s.size());
        route(r, w, t);
    }

    static GcsException notFound() {
        return new GcsException(404, "notFound", "NoSuchKey", "Not Found");
    }

    static GcsException badMethod(String m) {
        return new GcsException(405, "methodNotAllowed", "MethodNotAllowed", "The requested method (" + m + ") is not allowed for this resource.");
    }

    // ------------------------------------------------------------------------------------------ routing

    private void route(GcsReq r, GcsResp w, List<String> t) throws IOException {
        String m = r.method;
        int n = t.size();
        if (n == 0) {
            throw notFound();
        }
        if (t.get(0).equals("projects")) {
            projects(r, w, t);
            return;
        }
        if (!t.get(0).equals("b")) {
            throw notFound();
        }
        if (n == 1) {
            switch (m) {
                case "GET" -> listBuckets(r, w);
                case "POST" -> insertBucket(r, w);
                default -> throw badMethod(m);
            }
            return;
        }
        String bucket = t.get(1);
        if (n == 2) {
            switch (m) {
                case "GET", "HEAD" -> getBucket(r, w, bucket);
                case "PATCH" -> updateBucket(r, w, bucket, true);
                case "PUT" -> updateBucket(r, w, bucket, false);
                case "DELETE" -> deleteBucket(r, w, bucket);
                default -> throw badMethod(m);
            }
            return;
        }
        String sub = t.get(2);
        switch (sub) {
            case "o" -> objectsRoute(r, w, bucket, t.subList(3, n));
            case "acl", "defaultObjectAcl" -> aclRoute(r, w, bucket, null, sub, t.subList(3, n));
            case "iam" -> iam(r, w, bucket, t.subList(3, n));
            case "notificationConfigs" -> notifications(r, w, bucket, t.subList(3, n));
            case "lockRetentionPolicy" -> {
                r.op = "buckets.lockRetentionPolicy";
                r.write = true;
                lockRetention(r, w, bucket);
            }
            default -> throw notFound();
        }
    }

    private void objectsRoute(GcsReq r, GcsResp w, String bucket, List<String> t) throws IOException {
        String m = r.method;
        if (t.isEmpty()) {
            if (m.equals("GET")) {
                listObjects(r, w, bucket);
                return;
            }
            throw badMethod(m);
        }
        String name = t.get(0);
        if (t.size() == 1) {
            switch (m) {
                case "GET", "HEAD" -> getObject(r, w, bucket, name, false);
                case "PATCH" -> patchObject(r, w, bucket, name, true);
                case "PUT" -> patchObject(r, w, bucket, name, false);
                case "DELETE" -> deleteObject(r, w, bucket, name);
                default -> throw badMethod(m);
            }
            return;
        }
        switch (t.get(1)) {
            case "acl" -> aclRoute(r, w, bucket, name, "acl", t.subList(2, t.size()));
            case "copyTo", "rewriteTo" -> {
                if (t.size() == 6 && t.get(2).equals("b") && t.get(4).equals("o") && m.equals("POST")) {
                    copyOrRewrite(r, w, bucket, name, t.get(3), t.get(5), t.get(1).equals("rewriteTo"));
                } else {
                    throw notFound();
                }
            }
            case "moveTo" -> {
                if (t.size() == 4 && t.get(2).equals("o") && m.equals("POST")) {
                    moveObject(r, w, bucket, name, t.get(3));
                } else {
                    throw notFound();
                }
            }
            case "compose" -> {
                if (!m.equals("POST")) {
                    throw badMethod(m);
                }
                compose(r, w, bucket, name);
            }
            default -> throw notFound();
        }
    }

    // ------------------------------------------------------------------------------------------ helpers

    private JsonObject bodyJson(GcsReq r, boolean required) throws IOException {
        byte[] b = r.readBody(MAX_JSON);
        if (b.length == 0 || new String(b, StandardCharsets.UTF_8).isBlank()) {
            if (required) {
                throw GcsException.required("Required");
            }
            return new JsonObject();
        }
        try {
            JsonElement e = JsonParser.parseString(new String(b, StandardCharsets.UTF_8));
            if (!e.isJsonObject()) {
                throw GcsException.invalid("Invalid JSON payload received. Expected a JSON object.");
            }
            return e.getAsJsonObject();
        } catch (JsonParseException e) {
            throw new GcsException(400, "parseError", "MalformedPOSTRequest", "Parse Error");
        }
    }

    private void respond(GcsReq r, GcsResp w, int status, JsonElement e) throws IOException {
        String fields = r.q("fields");
        if (fields != null && !fields.isBlank()) {
            GcsFields f = GcsFields.parse(fields);
            e = f.apply(e);
        }
        w.json(status, e);
    }

    private void checkBucketPre(GcsReq r, Bucket b) {
        Long mm = GcsOps.generation(r, "ifMetagenerationMatch");
        Long mnm = GcsOps.generation(r, "ifMetagenerationNotMatch");
        if (mm != null && b.metageneration != mm) {
            throw GcsException.precondition("parameter", "ifMetagenerationMatch");
        }
        if (mnm != null && b.metageneration == mnm) {
            throw GcsException.precondition("parameter", "ifMetagenerationNotMatch");
        }
    }

    // ------------------------------------------------------------------------------------------ buckets

    private static final java.util.Set<String> BUCKET_SYSTEM = java.util.Set.of("kind", "id", "selfLink", "name", "projectNumber",
            "metageneration", "etag", "timeCreated", "updated", "owner", "locationType");
    private static final java.util.Set<String> LOC_MULTI = java.util.Set.of("US", "EU", "ASIA");
    private static final java.util.Set<String> LOC_DUAL = java.util.Set.of("NAM4", "EUR4", "ASIA1", "EUR5", "EUR7", "EUR8");

    private void applyBucketBody(Bucket b, JsonObject body, boolean merge) {
        JsonObject doc = b.doc;
        for (Map.Entry<String, JsonElement> e : body.entrySet()) {
            String k = e.getKey();
            if (BUCKET_SYSTEM.contains(k) || (k.equals("location") && merge)) {
                continue;
            }
            if (e.getValue().isJsonNull()) {
                doc.remove(k);
                continue;
            }
            if (merge && e.getValue().isJsonObject() && doc.has(k) && doc.get(k).isJsonObject() && !k.equals("cors")) {
                mergeInto(doc.getAsJsonObject(k), e.getValue().getAsJsonObject());
            } else {
                doc.add(k, e.getValue());
            }
        }
        if (doc.has("storageClass")) {
            String sc = doc.get("storageClass").getAsString().toUpperCase(Locale.ROOT);
            if (!GcsOps.CLASSES.contains(sc)) {
                throw GcsException.invalid("Invalid argument: storageClass " + sc);
            }
            doc.addProperty("storageClass", sc);
        }
        if (doc.has("versioning")) {
            JsonObject v = doc.getAsJsonObject("versioning");
            b.versioning = v.has("enabled") && v.get("enabled").getAsBoolean();
        } else if (!merge) {
            b.versioning = false;
        }
        if (doc.has("labels") && doc.get("labels").isJsonObject()) {
            JsonObject l = doc.getAsJsonObject("labels");
            l.entrySet().removeIf(en -> en.getValue().isJsonNull());
            if (l.size() == 0) {
                doc.remove("labels");
            }
        }
        if (doc.has("retentionPolicy") && doc.get("retentionPolicy").isJsonObject()) {
            JsonObject rp = doc.getAsJsonObject("retentionPolicy");
            if (rp.has("retentionPeriod")) {
                rp.addProperty("retentionPeriod", rp.get("retentionPeriod").getAsString());
                if (!rp.has("effectiveTime")) {
                    rp.addProperty("effectiveTime", GcsJson.ts(Instant.now()));
                }
            }
        }
        if (doc.has("iamConfiguration") && doc.get("iamConfiguration").isJsonObject()) {
            JsonObject ic = doc.getAsJsonObject("iamConfiguration");
            JsonObject ubla = ic.has("uniformBucketLevelAccess") ? ic.getAsJsonObject("uniformBucketLevelAccess") : null;
            JsonObject bpo = ic.has("bucketPolicyOnly") ? ic.getAsJsonObject("bucketPolicyOnly") : null;
            JsonObject use = ubla != null ? ubla : bpo;
            if (use != null) {
                boolean en = use.has("enabled") && use.get("enabled").getAsBoolean();
                JsonObject u = new JsonObject();
                u.addProperty("enabled", en);
                if (en) {
                    u.addProperty("lockedTime", GcsJson.ts(Instant.now().plus(90, ChronoUnit.DAYS)));
                }
                ic.add("uniformBucketLevelAccess", u);
                ic.add("bucketPolicyOnly", u.deepCopy());
            }
            if (!ic.has("publicAccessPrevention")) {
                ic.addProperty("publicAccessPrevention", "inherited");
            }
        }
    }

    private static void mergeInto(JsonObject dst, JsonObject src) {
        for (Map.Entry<String, JsonElement> e : src.entrySet()) {
            if (e.getValue().isJsonNull()) {
                dst.remove(e.getKey());
            } else if (e.getValue().isJsonObject() && dst.has(e.getKey()) && dst.get(e.getKey()).isJsonObject()) {
                mergeInto(dst.getAsJsonObject(e.getKey()), e.getValue().getAsJsonObject());
            } else {
                dst.add(e.getKey(), e.getValue());
            }
        }
    }

    private void defaults(Bucket b) {
        JsonObject d = b.doc;
        if (!d.has("location")) {
            d.addProperty("location", cfg.location);
        }
        String loc = d.get("location").getAsString().toUpperCase(Locale.ROOT);
        d.addProperty("location", loc);
        d.addProperty("locationType", LOC_MULTI.contains(loc) ? "multi-region" : LOC_DUAL.contains(loc) ? "dual-region" : "region");
        if (!d.has("storageClass")) {
            d.addProperty("storageClass", "STANDARD");
        }
        if (!d.has("iamConfiguration")) {
            JsonObject ic = new JsonObject();
            JsonObject off = new JsonObject();
            off.addProperty("enabled", false);
            ic.add("bucketPolicyOnly", off);
            ic.add("uniformBucketLevelAccess", off.deepCopy());
            ic.addProperty("publicAccessPrevention", "inherited");
            d.add("iamConfiguration", ic);
        }
        if (!d.has("softDeletePolicy")) {
            JsonObject sd = new JsonObject();
            sd.addProperty("retentionDurationSeconds", "604800");
            sd.addProperty("effectiveTime", GcsJson.ts(b.created));
            d.add("softDeletePolicy", sd);
        }
        if (!d.has("rpo")) {
            d.addProperty("rpo", "DEFAULT");
        }
    }

    private void insertBucket(GcsReq r, GcsResp w) throws IOException {
        r.op = "buckets.insert";
        r.write = true;
        String project = r.q("project");
        JsonObject body = bodyJson(r, false);
        if (project == null || project.isBlank()) {
            throw GcsException.required("Required parameter: project").at("parameter", "project");
        }
        String name = body.has("name") ? body.get("name").getAsString() : null;
        GcsOps.validateBucketName(name);
        Bucket b = new Bucket();
        b.name = name;
        b.project = project;
        b.created = Instant.now().truncatedTo(ChronoUnit.MICROS);
        b.updated = b.created;
        applyBucketBody(b, body, false);
        String pa = r.q("predefinedAcl");
        String pd = r.q("predefinedDefaultObjectAcl");
        if (pa != null && !GcsJson.validPredefined(pa, true)) {
            throw GcsException.invalid("Invalid Value").at("parameter", "predefinedAcl");
        }
        if (pd != null && !GcsJson.validPredefined(pd, false)) {
            throw GcsException.invalid("Invalid Value").at("parameter", "predefinedDefaultObjectAcl");
        }
        defaults(b);
        if (!b.doc.has("acl")) {
            b.doc.add("acl", GcsJson.predefined(pa, project, true));
        }
        if (!b.doc.has("defaultObjectAcl")) {
            b.doc.add("defaultObjectAcl", GcsJson.predefined(pd, project, false));
        }
        if (!st.createBucket(b)) {
            throw new GcsException(409, "conflict", "BucketAlreadyOwnedByYou",
                    "Your previous request to create the named bucket succeeded and you already own it.");
        }
        respond(r, w, 200, GcsJson.bucket(b, r.baseUrl(), r.q("projection")));
    }

    private void getBucket(GcsReq r, GcsResp w, String name) throws IOException {
        r.op = "buckets.get";
        Bucket b = ops.requireBucket(name);
        checkBucketPre(r, b);
        respond(r, w, 200, GcsJson.bucket(b, r.baseUrl(), r.q("projection")));
    }

    private void listBuckets(GcsReq r, GcsResp w) throws IOException {
        r.op = "buckets.list";
        String project = r.q("project");
        if (project == null || project.isBlank()) {
            throw GcsException.required("Required parameter: project").at("parameter", "project");
        }
        int max = maxResults(r);
        String after = r.q("pageToken");
        List<Bucket> l = st.listBuckets(project, r.q("prefix"), after, max + 1);
        JsonObject o = new JsonObject();
        o.addProperty("kind", "storage#buckets");
        JsonArray items = new JsonArray();
        for (int i = 0; i < Math.min(max, l.size()); i++) {
            items.add(GcsJson.bucket(l.get(i), r.baseUrl(), r.q("projection")));
        }
        if (l.size() > max) {
            o.addProperty("nextPageToken", l.get(max - 1).name);
        }
        if (items.size() > 0) {
            o.add("items", items);
        }
        respond(r, w, 200, o);
    }

    private static int maxResults(GcsReq r) {
        String v = r.q("maxResults");
        if (v == null) {
            return 1000;
        }
        try {
            int n = Integer.parseInt(v.trim());
            if (n < 0) {
                throw GcsException.invalid("Invalid argument: maxResults").at("parameter", "maxResults");
            }
            return n == 0 ? 1000 : Math.min(n, 1000);
        } catch (NumberFormatException e) {
            throw GcsException.invalid("Invalid argument: maxResults").at("parameter", "maxResults");
        }
    }

    private void updateBucket(GcsReq r, GcsResp w, String name, boolean patch) throws IOException {
        r.op = patch ? "buckets.patch" : "buckets.update";
        r.write = true;
        JsonObject body = bodyJson(r, false);
        Bucket b = st.mutateBucket(name, cur -> {
            checkBucketPre(r, cur);
            Bucket n = new Bucket();
            n.name = cur.name;
            n.project = cur.project;
            n.created = cur.created;
            n.metageneration = cur.metageneration + 1;
            n.updated = Instant.now().truncatedTo(ChronoUnit.MICROS);
            if (patch) {
                n.doc = cur.doc.deepCopy();
                n.versioning = cur.versioning;
            } else {
                // update replaces every mutable field; location/locationType stay, ACLs stay unless supplied
                for (String k : new String[] {"location", "locationType", "acl", "defaultObjectAcl", "notificationConfigs", "iamPolicy"}) {
                    if (cur.doc.has(k)) {
                        n.doc.add(k, cur.doc.get(k));
                    }
                }
            }
            applyBucketBody(n, body, patch);
            if (patch && body.has("versioning") && body.get("versioning").isJsonNull()) {
                n.versioning = false;
            }
            if (!n.doc.has("storageClass")) {
                n.doc.addProperty("storageClass", "STANDARD");
            }
            n.doc.remove("softDeletePolicy");
            if (cur.doc.has("softDeletePolicy") && !(body.has("softDeletePolicy"))) {
                n.doc.add("softDeletePolicy", cur.doc.get("softDeletePolicy"));
            }
            defaults(n);
            String pa = r.q("predefinedAcl");
            if (pa != null) {
                if (!GcsJson.validPredefined(pa, true)) {
                    throw GcsException.invalid("Invalid Value").at("parameter", "predefinedAcl");
                }
                n.doc.add("acl", GcsJson.predefined(pa, n.project, true));
            }
            String pd = r.q("predefinedDefaultObjectAcl");
            if (pd != null) {
                n.doc.add("defaultObjectAcl", GcsJson.predefined(pd, n.project, false));
            }
            return n;
        });
        respond(r, w, 200, GcsJson.bucket(b, r.baseUrl(), r.q("projection")));
    }

    private void deleteBucket(GcsReq r, GcsResp w, String name) throws IOException {
        r.op = "buckets.delete";
        r.write = true;
        Bucket b = ops.requireBucket(name);
        checkBucketPre(r, b);
        if (st.bucketHasObjects(name)) {
            throw new GcsException(409, "conflict", "BucketNotEmpty", "The bucket you tried to delete is not empty.");
        }
        st.deleteBucket(name);
        w.empty(204);
    }

    private void lockRetention(GcsReq r, GcsResp w, String name) throws IOException {
        Long mm = GcsOps.generation(r, "ifMetagenerationMatch");
        if (mm == null) {
            throw GcsException.required("Required parameter: ifMetagenerationMatch").at("parameter", "ifMetagenerationMatch");
        }
        Bucket b = st.mutateBucket(name, cur -> {
            if (cur.metageneration != mm) {
                throw GcsException.precondition("parameter", "ifMetagenerationMatch");
            }
            if (!cur.doc.has("retentionPolicy")) {
                throw GcsException.invalid("Bucket has no retention policy to lock.");
            }
            cur.doc.getAsJsonObject("retentionPolicy").addProperty("isLocked", true);
            cur.metageneration++;
            cur.updated = Instant.now().truncatedTo(ChronoUnit.MICROS);
            return cur;
        });
        respond(r, w, 200, GcsJson.bucket(b, r.baseUrl(), r.q("projection")));
    }

    // ------------------------------------------------------------------------------------------ ACLs

    private void aclRoute(GcsReq r, GcsResp w, String bucket, String object, String kindPath, List<String> rest) throws IOException {
        boolean isObj = object != null;
        boolean isDefault = kindPath.equals("defaultObjectAcl");
        r.op = (isObj ? "objectAccessControls." : isDefault ? "defaultObjectAccessControls." : "bucketAccessControls.")
                + (rest.isEmpty() ? (r.method.equals("GET") ? "list" : "insert") : switch (r.method) {
                    case "GET" -> "get";
                    case "DELETE" -> "delete";
                    default -> "update";
                });
        r.write = !r.method.equals("GET");
        Bucket b = ops.requireBucket(bucket);
        String kind = isObj || isDefault ? "storage#objectAccessControl" : "storage#bucketAccessControl";
        Long gen = isObj ? GcsOps.generation(r, "generation") : null;
        Obj o = null;
        JsonArray acl;
        if (isObj) {
            o = st.find(bucket, object, gen);
            if (o == null) {
                throw GcsException.noSuchObject(bucket, object);
            }
            acl = o.doc.has("acl") ? o.doc.getAsJsonArray("acl") : GcsJson.predefined(null, b.project, false);
        } else {
            String key = isDefault ? "defaultObjectAcl" : "acl";
            acl = b.doc.has(key) ? b.doc.getAsJsonArray(key) : GcsJson.predefined(null, b.project, !isDefault);
        }
        String base = r.baseUrl();
        Long genForRender = isObj ? o.generation : null;
        if (rest.isEmpty()) {
            if (r.method.equals("GET")) {
                JsonObject out = new JsonObject();
                out.addProperty("kind", isObj || isDefault ? "storage#objectAccessControls" : "storage#bucketAccessControls");
                out.add("items", GcsJson.renderAcl(acl, kind, bucket, object, genForRender, base));
                respond(r, w, 200, out);
                return;
            }
            if (!r.method.equals("POST")) {
                throw badMethod(r.method);
            }
            JsonObject body = bodyJson(r, true);
            if (!body.has("entity") || !body.has("role")) {
                throw GcsException.required("Required");
            }
            JsonArray next = setAcl(acl, body.get("entity").getAsString(), body.get("role").getAsString());
            saveAcl(b, o, isDefault, next);
            respond(r, w, 200, GcsJson.renderAcl(pick(next, body.get("entity").getAsString()), kind, bucket, object, genForRender, base).get(0));
            return;
        }
        String entity = rest.get(0);
        JsonArray found = pick(acl, entity);
        switch (r.method) {
            case "GET" -> {
                if (found.size() == 0) {
                    throw notFound();
                }
                respond(r, w, 200, GcsJson.renderAcl(found, kind, bucket, object, genForRender, base).get(0));
            }
            case "PUT", "PATCH" -> {
                if (found.size() == 0) {
                    throw notFound();
                }
                JsonObject body = bodyJson(r, true);
                String role = body.has("role") ? body.get("role").getAsString() : found.get(0).getAsJsonObject().get("role").getAsString();
                JsonArray next = setAcl(acl, entity, role);
                saveAcl(b, o, isDefault, next);
                respond(r, w, 200, GcsJson.renderAcl(pick(next, entity), kind, bucket, object, genForRender, base).get(0));
            }
            case "DELETE" -> {
                if (found.size() == 0) {
                    throw notFound();
                }
                JsonArray next = new JsonArray();
                for (JsonElement e : acl) {
                    if (!e.getAsJsonObject().get("entity").getAsString().equals(entity)) {
                        next.add(e);
                    }
                }
                saveAcl(b, o, isDefault, next);
                w.empty(204);
            }
            default -> throw badMethod(r.method);
        }
    }

    private static JsonArray pick(JsonArray acl, String entity) {
        JsonArray out = new JsonArray();
        for (JsonElement e : acl) {
            if (e.getAsJsonObject().get("entity").getAsString().equals(entity)) {
                out.add(e);
            }
        }
        return out;
    }

    private static JsonArray setAcl(JsonArray acl, String entity, String role) {
        JsonArray next = new JsonArray();
        boolean done = false;
        for (JsonElement e : acl) {
            JsonObject a = e.getAsJsonObject();
            if (a.get("entity").getAsString().equals(entity)) {
                JsonObject n = a.deepCopy();
                n.addProperty("role", role);
                next.add(n);
                done = true;
            } else {
                next.add(a);
            }
        }
        if (!done) {
            JsonObject n = new JsonObject();
            n.addProperty("entity", entity);
            n.addProperty("role", role);
            next.add(n);
        }
        return next;
    }

    private void saveAcl(Bucket b, Obj o, boolean isDefault, JsonArray next) {
        if (o != null) {
            st.mutate(o.bucket, o.name, o.generation, Pre.NONE, x -> {
                x.doc.add("acl", next);
                return x;
            });
        } else {
            String key = isDefault ? "defaultObjectAcl" : "acl";
            st.mutateBucket(b.name, x -> {
                x.doc.add(key, next);
                x.metageneration++;
                x.updated = Instant.now().truncatedTo(ChronoUnit.MICROS);
                return x;
            });
        }
    }

    // ------------------------------------------------------------------------------------------ IAM, notifications, projects

    private void iam(GcsReq r, GcsResp w, String bucket, List<String> rest) throws IOException {
        Bucket b = ops.requireBucket(bucket);
        if (rest.size() == 1 && rest.get(0).equals("testPermissions")) {
            r.op = "buckets.testIamPermissions";
            JsonObject out = new JsonObject();
            out.addProperty("kind", "storage#testIamPermissionsResponse");
            JsonArray perms = new JsonArray();
            for (String p : r.query.getOrDefault("permissions", List.of())) {
                perms.add(p);
            }
            out.add("permissions", perms);
            respond(r, w, 200, out);
            return;
        }
        if (!rest.isEmpty()) {
            throw notFound();
        }
        if (r.method.equals("GET")) {
            r.op = "buckets.getIamPolicy";
            JsonObject p = b.doc.has("iamPolicy") ? b.doc.getAsJsonObject("iamPolicy").deepCopy() : new JsonObject();
            respond(r, w, 200, policy(bucket, p));
        } else if (r.method.equals("PUT")) {
            r.op = "buckets.setIamPolicy";
            r.write = true;
            JsonObject body = bodyJson(r, true);
            body.addProperty("etag", Base64.getEncoder().encodeToString(("e" + System.nanoTime()).getBytes(StandardCharsets.UTF_8)));
            st.mutateBucket(bucket, x -> {
                x.doc.add("iamPolicy", body);
                return x;
            });
            respond(r, w, 200, policy(bucket, body.deepCopy()));
        } else {
            throw badMethod(r.method);
        }
    }

    private static JsonObject policy(String bucket, JsonObject p) {
        JsonObject o = new JsonObject();
        o.addProperty("kind", "storage#policy");
        o.addProperty("resourceId", "projects/_/buckets/" + bucket);
        o.addProperty("version", p.has("version") ? p.get("version").getAsInt() : 1);
        o.add("bindings", p.has("bindings") ? p.get("bindings") : new JsonArray());
        o.addProperty("etag", p.has("etag") ? p.get("etag").getAsString() : "CAE=");
        return o;
    }

    private void notifications(GcsReq r, GcsResp w, String bucket, List<String> rest) throws IOException {
        Bucket b = ops.requireBucket(bucket);
        JsonArray list = b.doc.has("notificationConfigs") ? b.doc.getAsJsonArray("notificationConfigs") : new JsonArray();
        r.write = !r.method.equals("GET");
        if (rest.isEmpty()) {
            if (r.method.equals("GET")) {
                r.op = "notifications.list";
                JsonObject out = new JsonObject();
                out.addProperty("kind", "storage#notifications");
                JsonArray items = new JsonArray();
                list.forEach(e -> items.add(notification(bucket, e.getAsJsonObject(), r)));
                out.add("items", items);
                respond(r, w, 200, out);
                return;
            }
            if (!r.method.equals("POST")) {
                throw badMethod(r.method);
            }
            r.op = "notifications.insert";
            JsonObject body = bodyJson(r, true);
            if (!body.has("topic") || body.get("topic").getAsString().isBlank()) {
                throw GcsException.required("Required");
            }
            JsonObject cfgN = new JsonObject();
            String id = String.valueOf(1 + list.size() + (int) (System.nanoTime() % 1000));
            cfgN.addProperty("id", id);
            cfgN.add("topic", body.get("topic"));
            cfgN.addProperty("payload_format", body.has("payload_format") ? body.get("payload_format").getAsString() : "JSON_API_V1");
            for (String k : new String[] {"event_types", "custom_attributes", "object_name_prefix"}) {
                if (body.has(k)) {
                    cfgN.add(k, body.get(k));
                }
            }
            st.mutateBucket(bucket, x -> {
                JsonArray a = x.doc.has("notificationConfigs") ? x.doc.getAsJsonArray("notificationConfigs") : new JsonArray();
                a.add(cfgN);
                x.doc.add("notificationConfigs", a);
                return x;
            });
            respond(r, w, 200, notification(bucket, cfgN, r));
            return;
        }
        String id = rest.get(0);
        JsonObject found = null;
        for (JsonElement e : list) {
            if (e.getAsJsonObject().get("id").getAsString().equals(id)) {
                found = e.getAsJsonObject();
            }
        }
        if (found == null) {
            throw notFound();
        }
        if (r.method.equals("GET")) {
            r.op = "notifications.get";
            respond(r, w, 200, notification(bucket, found, r));
        } else if (r.method.equals("DELETE")) {
            r.op = "notifications.delete";
            st.mutateBucket(bucket, x -> {
                JsonArray a = new JsonArray();
                x.doc.getAsJsonArray("notificationConfigs").forEach(e -> {
                    if (!e.getAsJsonObject().get("id").getAsString().equals(id)) {
                        a.add(e);
                    }
                });
                x.doc.add("notificationConfigs", a);
                return x;
            });
            w.empty(204);
        } else {
            throw badMethod(r.method);
        }
    }

    private static JsonObject notification(String bucket, JsonObject c, GcsReq r) {
        JsonObject o = new JsonObject();
        o.addProperty("kind", "storage#notification");
        o.addProperty("id", c.get("id").getAsString());
        o.addProperty("selfLink", r.baseUrl() + "/storage/v1/b/" + bucket + "/notificationConfigs/" + c.get("id").getAsString());
        o.addProperty("etag", GcsHash.etag(c.get("id").getAsString().hashCode() & 0xffffffffL, 1));
        c.entrySet().forEach(e -> {
            if (!e.getKey().equals("id")) {
                o.add(e.getKey(), e.getValue());
            }
        });
        return o;
    }

    private void projects(GcsReq r, GcsResp w, List<String> t) throws IOException {
        if (t.size() < 3) {
            throw notFound();
        }
        String project = t.get(1);
        if (t.get(2).equals("serviceAccount") && t.size() == 3) {
            r.op = "projects.serviceAccount.get";
            JsonObject o = new JsonObject();
            o.addProperty("kind", "storage#serviceAccount");
            o.addProperty("email_address", "service-" + GcsJson.projectNumber(project) + "@gs-project-accounts.iam.gserviceaccount.com");
            respond(r, w, 200, o);
            return;
        }
        if (!t.get(2).equals("hmacKeys")) {
            throw notFound();
        }
        r.write = !r.method.equals("GET");
        if (t.size() == 3) {
            if (r.method.equals("POST")) {
                r.op = "hmacKeys.create";
                String sa = r.q("serviceAccountEmail");
                if (sa == null || sa.isBlank()) {
                    throw GcsException.required("Required parameter: serviceAccountEmail").at("parameter", "serviceAccountEmail");
                }
                StringBuilder id = new StringBuilder("GOOG1E");
                String alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
                for (int i = 0; i < 56; i++) {
                    id.append(alpha.charAt(RND.nextInt(alpha.length())));
                }
                byte[] sec = new byte[30];
                RND.nextBytes(sec);
                Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
                HmacKey k = new HmacKey(id.toString(), Base64.getEncoder().encodeToString(sec), project, sa, "ACTIVE", now, now);
                st.putHmac(k);
                JsonObject o = new JsonObject();
                o.addProperty("kind", "storage#hmacKey");
                o.addProperty("secret", k.secret());
                o.add("metadata", hmacMeta(k, r));
                respond(r, w, 200, o);
            } else if (r.method.equals("GET")) {
                r.op = "hmacKeys.list";
                JsonObject o = new JsonObject();
                o.addProperty("kind", "storage#hmacKeysMetadata");
                JsonArray items = new JsonArray();
                for (HmacKey k : st.listHmac(project, r.q("serviceAccountEmail"))) {
                    items.add(hmacMeta(k, r));
                }
                if (items.size() > 0) {
                    o.add("items", items);
                }
                respond(r, w, 200, o);
            } else {
                throw badMethod(r.method);
            }
            return;
        }
        String accessId = t.get(3);
        HmacKey k = st.getHmac(accessId);
        if (k == null || !k.project().equals(project)) {
            throw notFound();
        }
        switch (r.method) {
            case "GET" -> {
                r.op = "hmacKeys.get";
                respond(r, w, 200, hmacMeta(k, r));
            }
            case "PUT" -> {
                r.op = "hmacKeys.update";
                JsonObject body = bodyJson(r, true);
                String state = body.has("state") ? body.get("state").getAsString() : null;
                if (!"ACTIVE".equals(state) && !"INACTIVE".equals(state)) {
                    throw GcsException.invalid("Invalid state: " + state);
                }
                st.setHmacState(accessId, state);
                respond(r, w, 200, hmacMeta(st.getHmac(accessId), r));
            }
            case "DELETE" -> {
                r.op = "hmacKeys.delete";
                if ("ACTIVE".equals(k.state())) {
                    throw GcsException.invalid("Cannot delete an ACTIVE HMAC key; deactivate it first.");
                }
                st.deleteHmac(accessId);
                w.empty(204);
            }
            default -> throw badMethod(r.method);
        }
    }

    private static JsonObject hmacMeta(HmacKey k, GcsReq r) {
        JsonObject o = new JsonObject();
        o.addProperty("kind", "storage#hmacKeyMetadata");
        o.addProperty("id", k.project() + "/" + k.accessId());
        o.addProperty("selfLink", r.baseUrl() + "/storage/v1/projects/" + k.project() + "/hmacKeys/" + k.accessId());
        o.addProperty("projectId", k.project());
        o.addProperty("accessId", k.accessId());
        o.addProperty("state", k.state());
        o.addProperty("serviceAccountEmail", k.saEmail());
        o.addProperty("timeCreated", GcsJson.ts(k.created()));
        o.addProperty("updated", GcsJson.ts(k.updated()));
        o.addProperty("etag", GcsHash.etag(k.created().toEpochMilli(), k.updated().toEpochMilli()));
        return o;
    }

    // ------------------------------------------------------------------------------------------ objects: read

    private void getObject(GcsReq r, GcsResp w, String bucket, String name, boolean forceMedia) throws IOException {
        Long gen = GcsOps.generation(r, "generation");
        Bucket b = ops.requireBucket(bucket);
        Obj o = st.find(bucket, name, gen);
        if (o == null) {
            throw GcsException.noSuchObject(bucket, name);
        }
        Pre pre = GcsOps.pre(r, "if");
        String alt = r.q("alt");
        if (alt != null && !alt.equals("json") && !alt.equals("media")) {
            throw GcsException.invalid("Invalid alt value: " + alt).at("parameter", "alt");
        }
        boolean media = forceMedia || "media".equals(alt);
        try {
            pre.check(o.live() || gen != null ? o : null);
        } catch (GcsException e) {
            if (e.location != null && e.location.endsWith("NotMatch")) {
                w.empty(304);
                return;
            }
            throw e;
        }
        if (media) {
            r.op = "objects.get.media";
            ops.serve(r, w, o, false);
            return;
        }
        r.op = "objects.get";
        String proj = r.q("projection");
        JsonObject res = GcsJson.object(o, r.baseUrl(), proj);
        if ("HEAD".equals(r.method)) {
            w.headOnly = true;
        }
        respond(r, w, 200, res);
        if (b == null) {
            return;
        }
    }

    private void listObjects(GcsReq r, GcsResp w, String bucket) throws IOException {
        r.op = "objects.list";
        ops.requireBucket(bucket);
        int max = maxResults(r);
        String prefix = r.q("prefix") == null ? "" : r.q("prefix");
        String delimiter = r.q("delimiter");
        String glob = r.q("matchGlob");
        String start = r.q("startOffset");
        String end = r.q("endOffset");
        String token = r.q("pageToken");
        String afterName = null;
        long afterGen = -1;
        if (token != null && !token.isEmpty()) {
            Object[] pos = GcsStore.decodeToken(token);
            afterName = (String) pos[0];
            afterGen = (Long) pos[1];
        }
        boolean versions = r.qBool("versions");
        GcsStore.ListSpec spec = new GcsStore.ListSpec(bucket, prefix, delimiter, start, end, versions, r.qBool("includeTrailingDelimiter"),
                glob == null ? null : GcsMatch.glob(glob), afterName, afterGen);
        GcsStore.Page p = st.list(spec, max);
        JsonObject o = new JsonObject();
        o.addProperty("kind", "storage#objects");
        if (p.nextToken() != null) {
            o.addProperty("nextPageToken", p.nextToken());
        }
        if (!p.prefixes().isEmpty()) {
            JsonArray a = new JsonArray();
            p.prefixes().forEach(a::add);
            o.add("prefixes", a);
        }
        if (!p.objects().isEmpty()) {
            JsonArray items = new JsonArray();
            for (Obj x : p.objects()) {
                items.add(GcsJson.object(x, r.baseUrl(), r.q("projection")));
            }
            o.add("items", items);
        }
        respond(r, w, 200, o);
    }

    // ------------------------------------------------------------------------------------------ objects: write

    private void patchObject(GcsReq r, GcsResp w, String bucket, String name, boolean patch) throws IOException {
        r.op = patch ? "objects.patch" : "objects.update";
        r.write = true;
        JsonObject body = bodyJson(r, false);
        Bucket b = ops.requireBucket(bucket);
        Long gen = GcsOps.generation(r, "generation");
        String pa = r.q("predefinedAcl");
        Obj o = st.mutate(bucket, name, gen, GcsOps.pre(r, "if"), cur -> {
            Obj n = cur.copy();
            if (!patch) {
                n.doc.remove("acl");
                n.doc.remove("eventBasedHold");
                n.doc.remove("temporaryHold");
            }
            GcsOps.applyMeta(n, body, patch);
            if (!patch && n.contentType == null) {
                n.contentType = cur.contentType;
            }
            if (pa != null) {
                ops.applyPredefinedAcl(b, n, pa);
            }
            return n;
        });
        respond(r, w, 200, GcsJson.object(o, r.baseUrl(), r.q("projection")));
    }

    private void deleteObject(GcsReq r, GcsResp w, String bucket, String name) throws IOException {
        r.op = "objects.delete";
        r.write = true;
        Bucket b = ops.requireBucket(bucket);
        st.delete(b, bucket, name, GcsOps.generation(r, "generation"), GcsOps.pre(r, "if"));
        w.empty(204);
    }

    private Obj requireSource(GcsReq r, String bucket, String name) {
        Bucket sb = ops.requireBucket(bucket);
        Obj src = st.find(bucket, name, GcsOps.generation(r, "sourceGeneration"));
        if (src == null) {
            throw GcsException.noSuchObject(bucket, name);
        }
        GcsOps.pre(r, "ifSource").check(src);
        if (sb == null) {
            throw GcsException.noSuchBucket();
        }
        return src;
    }

    private void copyOrRewrite(GcsReq r, GcsResp w, String sb, String sn, String db, String dn, boolean rewrite) throws IOException {
        r.op = rewrite ? "objects.rewrite" : "objects.copy";
        r.write = true;
        JsonObject body = bodyJson(r, false);
        Bucket dst = ops.requireBucket(db);
        GcsOps.validateObjectName(dn);
        Obj src = requireSource(r, sb, sn);
        if (rewrite && r.q("rewriteToken") != null && !r.q("rewriteToken").isEmpty()) {
            throw GcsException.invalid("Invalid rewrite token.").at("parameter", "rewriteToken");
        }
        Obj o = ops.copy(dst, src, dn, body, GcsOps.pre(r, "if"));
        String pa = r.q("destinationPredefinedAcl");
        if (pa != null) {
            o = st.mutate(o.bucket, o.name, o.generation, Pre.NONE, x -> {
                ops.applyPredefinedAcl(dst, x, pa);
                x.metageneration--;
                return x;
            });
        }
        if (rewrite) {
            JsonObject out = new JsonObject();
            out.addProperty("kind", "storage#rewriteResponse");
            out.addProperty("totalBytesRewritten", String.valueOf(src.size));
            out.addProperty("objectSize", String.valueOf(src.size));
            out.addProperty("done", true);
            out.add("resource", GcsJson.object(o, r.baseUrl(), r.q("projection")));
            respond(r, w, 200, out);
        } else {
            respond(r, w, 200, GcsJson.object(o, r.baseUrl(), r.q("projection")));
        }
    }

    private void moveObject(GcsReq r, GcsResp w, String bucket, String src, String dstName) throws IOException {
        r.op = "objects.move";
        r.write = true;
        Bucket b = ops.requireBucket(bucket);
        GcsOps.validateObjectName(dstName);
        Obj s = st.find(bucket, src, GcsOps.generation(r, "sourceGeneration"));
        if (s == null) {
            throw GcsException.noSuchObject(bucket, src);
        }
        if (src.equals(dstName)) {
            throw GcsException.invalid("Source and destination object names must differ.");
        }
        GcsOps.pre(r, "ifSource").check(s);
        Obj o = ops.copy(b, s, dstName, null, GcsOps.pre(r, "if"));
        st.delete(b, bucket, src, s.generation, Pre.NONE);
        respond(r, w, 200, GcsJson.object(o, r.baseUrl(), r.q("projection")));
    }

    private void compose(GcsReq r, GcsResp w, String bucket, String name) throws IOException {
        r.op = "objects.compose";
        r.write = true;
        JsonObject body = bodyJson(r, true);
        Bucket b = ops.requireBucket(bucket);
        GcsOps.validateObjectName(name);
        if (!body.has("sourceObjects") || !body.get("sourceObjects").isJsonArray() || body.getAsJsonArray("sourceObjects").size() == 0) {
            throw GcsException.required("Required");
        }
        JsonArray srcs = body.getAsJsonArray("sourceObjects");
        if (srcs.size() > 32) {
            throw GcsException.invalid("The number of source components provided (" + srcs.size() + ") exceeds the maximum (32)");
        }
        List<Obj> objs = new ArrayList<>();
        for (JsonElement e : srcs) {
            JsonObject so = e.getAsJsonObject();
            String sn = so.has("name") ? so.get("name").getAsString() : null;
            if (sn == null) {
                throw GcsException.required("Required");
            }
            Long g = so.has("generation") ? so.get("generation").getAsLong() : null;
            Obj x = st.find(bucket, sn, g);
            if (x == null) {
                throw GcsException.noSuchObject(bucket, sn);
            }
            if (so.has("objectPreconditions") && so.getAsJsonObject("objectPreconditions").has("ifGenerationMatch")) {
                long need = so.getAsJsonObject("objectPreconditions").get("ifGenerationMatch").getAsLong();
                if (x.generation != need) {
                    throw GcsException.precondition("parameter", "ifGenerationMatch");
                }
            }
            objs.add(x);
        }
        JsonObject dest = body.has("destination") && body.get("destination").isJsonObject() ? body.getAsJsonObject("destination") : new JsonObject();
        Obj o = ops.compose(b, name, objs, dest, GcsOps.pre(r, "if"));
        String pa = r.q("destinationPredefinedAcl");
        if (pa != null) {
            o = st.mutate(o.bucket, o.name, o.generation, Pre.NONE, x -> {
                ops.applyPredefinedAcl(b, x, pa);
                x.metageneration--;
                return x;
            });
        }
        respond(r, w, 200, GcsJson.object(o, r.baseUrl(), r.q("projection")));
    }

    // ------------------------------------------------------------------------------------------ uploads

    private void upload(GcsReq r, GcsResp w, List<String> t) throws IOException {
        // t = b/{bucket}/o
        if (t.size() != 3 || !t.get(0).equals("b") || !t.get(2).equals("o")) {
            throw notFound();
        }
        String bucket = t.get(1);
        r.write = true;
        String type = r.q("uploadType");
        String uploadId = r.q("upload_id");
        if (r.method.equals("PUT") || r.method.equals("DELETE") || uploadId != null && r.method.equals("POST") && type == null) {
            resumablePut(r, w, bucket, uploadId);
            return;
        }
        if (!r.method.equals("POST")) {
            throw badMethod(r.method);
        }
        if (type == null) {
            throw GcsException.required("Upload type is required").at("parameter", "uploadType");
        }
        Bucket b = ops.requireBucket(bucket);
        switch (type) {
            case "media" -> {
                r.op = "objects.insert.media";
                String name = r.q("name");
                GcsOps.validateObjectName(name);
                Obj tmpl = ops.template(b, name, null);
                String ct = r.header("content-type");
                tmpl.contentType = ct == null || ct.isBlank() ? "application/octet-stream" : ct;
                finishInsert(r, w, b, tmpl, r.body, GcsHash.parseGoogHash(r.header("x-goog-hash")));
            }
            case "multipart" -> {
                r.op = "objects.insert.multipart";
                String boundary = GcsMime.boundaryOf(r.header("content-type"));
                if (boundary == null) {
                    throw GcsException.invalid("Invalid multipart upload: missing boundary.");
                }
                GcsMime mime = new GcsMime(r.body, boundary);
                GcsMime.Part meta = mime.next();
                if (meta == null) {
                    throw GcsException.invalid("Invalid multipart upload: missing metadata part.");
                }
                JsonObject md;
                try {
                    String json = new String(meta.body().readNBytes((int) MAX_JSON), StandardCharsets.UTF_8);
                    md = json.isBlank() ? new JsonObject() : JsonParser.parseString(json).getAsJsonObject();
                } catch (RuntimeException e) {
                    throw new GcsException(400, "parseError", "MalformedPOSTRequest", "Parse Error");
                }
                GcsMime.Part data = mime.next();
                if (data == null) {
                    throw GcsException.invalid("Invalid multipart upload: missing media part.");
                }
                String name = r.q("name") != null ? r.q("name") : md.has("name") ? md.get("name").getAsString() : null;
                GcsOps.validateObjectName(name);
                Obj tmpl = ops.template(b, name, md);
                if (!md.has("contentType") && data.headers().get("content-type") != null) {
                    tmpl.contentType = data.headers().get("content-type");
                }
                Map<String, String> expect = new LinkedHashMap<>(GcsHash.parseGoogHash(r.header("x-goog-hash")));
                if (md.has("md5Hash")) {
                    expect.put("md5", md.get("md5Hash").getAsString());
                }
                if (md.has("crc32c")) {
                    expect.put("crc32c", md.get("crc32c").getAsString());
                }
                finishInsert(r, w, b, tmpl, data.body(), expect);
            }
            case "resumable" -> {
                r.op = "objects.insert.resumable.start";
                startResumable(r, w, b);
            }
            default -> throw GcsException.invalid("Invalid uploadType: " + type).at("parameter", "uploadType");
        }
    }

    private void finishInsert(GcsReq r, GcsResp w, Bucket b, Obj tmpl, InputStream body, Map<String, String> expect) throws IOException {
        if (r.q("contentEncoding") != null) {
            tmpl.contentEncoding = r.q("contentEncoding");
        }
        ops.applyPredefinedAcl(b, tmpl, r.q("predefinedAcl"));
        Obj o = ops.upload(b, tmpl, body, -1, GcsOps.pre(r, "if"), expect);
        respond(r, w, 200, GcsJson.object(o, r.baseUrl(), r.q("projection")));
    }

    private void startResumable(GcsReq r, GcsResp w, Bucket b) throws IOException {
        JsonObject md = bodyJson(r, false);
        String name = r.q("name") != null ? r.q("name") : md.has("name") ? md.get("name").getAsString() : null;
        GcsOps.validateObjectName(name);
        Pre pre = GcsOps.pre(r, "if");
        JsonObject req = new JsonObject();
        req.add("meta", md);
        req.add("pre", GcsResumable.preJson(pre));
        JsonObject params = new JsonObject();
        for (String k : new String[] {"contentEncoding", "predefinedAcl", "projection"}) {
            if (r.q(k) != null) {
                params.addProperty(k, r.q(k));
            }
        }
        req.add("params", params);
        String ct = r.header("x-upload-content-type");
        if (ct != null) {
            req.addProperty("contentType", ct);
        }
        String len = r.header("x-upload-content-length");
        if (len != null) {
            try {
                req.addProperty("declaredLength", Long.parseLong(len.trim()));
            } catch (NumberFormatException e) {
                throw GcsException.invalid("Invalid X-Upload-Content-Length: " + len);
            }
        }
        JsonObject expect = new JsonObject();
        if (md.has("md5Hash")) {
            expect.add("md5", md.get("md5Hash"));
        }
        if (md.has("crc32c")) {
            expect.add("crc32c", md.get("crc32c"));
        }
        req.add("expect", expect);
        Session s = resumable.start(b, name, req, pre);
        w.header("Location", r.baseUrl() + "/upload/storage/v1/b/" + GcsReq.enc(b.name) + "/o?uploadType=resumable&name="
                + GcsReq.enc(name) + "&upload_id=" + s.id);
        w.header("X-GUploader-UploadID", s.id);
        w.header("Content-Type", "text/plain; charset=utf-8");
        w.empty(200);
    }

    private void resumablePut(GcsReq r, GcsResp w, String bucket, String uploadId) throws IOException {
        r.op = "objects.insert.resumable.put";
        Session s = resumable.require(uploadId, bucket, r.q("name"));
        if (r.method.equals("DELETE")) {
            r.op = "objects.insert.resumable.cancel";
            resumable.cancel(s);
            w.header("Content-Type", "text/plain; charset=utf-8");
            w.empty(499);
            return;
        }
        GcsResumable.Outcome out = resumable.put(r, s);
        w.header("X-GUploader-UploadID", s.id);
        if (out.object() != null) {
            respond(r, w, 200, GcsJson.object(out.object(), r.baseUrl(), s.request.has("params") && s.request.getAsJsonObject("params")
                    .has("projection") ? s.request.getAsJsonObject("params").get("projection").getAsString() : null));
        } else if (out.session().result != null) {
            String text = out.session().result.toString().replace("@BASE@", r.baseUrl());
            respond(r, w, 200, JsonParser.parseString(text));
        } else {
            w.header("Range", GcsRange.rangeHeader(out.persisted()));
            w.header("Content-Type", "text/plain; charset=utf-8");
            w.empty(308);
        }
    }

    // ------------------------------------------------------------------------------------------ batch

    private void batch(GcsReq r, GcsResp w) throws IOException {
        String ct = r.header("content-type");
        String boundary = GcsMime.boundaryOf(ct);
        if (ct == null || !ct.toLowerCase(Locale.ROOT).startsWith("multipart/mixed") || boundary == null) {
            throw GcsException.invalid("Batch requests must use Content-Type multipart/mixed with a boundary.");
        }
        GcsMime mime = new GcsMime(r.body, boundary);
        String outBoundary = "batch_" + UUID.randomUUID().toString().replace("-", "");
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int count = 0;
        GcsMime.Part p;
        while ((p = mime.next()) != null) {
            if (++count > 100) {
                throw GcsException.invalid("Too many requests in the batch (maximum 100).");
            }
            byte[] raw = p.body().readAllBytes();
            String cid = p.headers().get("content-id");
            GcsResp sub = new GcsResp();
            try {
                sub = runSub(r, raw);
            } catch (GcsException e) {
                sub = new GcsResp();
                sub.status = e.status;
                sub.header("Content-Type", "application/json; charset=UTF-8");
                byte[] b = GcsJson.error(e).toString().getBytes(StandardCharsets.UTF_8);
                sub.bytes(e.status, "application/json; charset=UTF-8", b);
            } catch (RuntimeException e) {
                sub = new GcsResp();
                sub.bytes(500, "application/json; charset=UTF-8", GcsJson.error(GcsException.internal("Internal Error")).toString()
                        .getBytes(StandardCharsets.UTF_8));
            }
            StringBuilder sb = new StringBuilder();
            sb.append("--").append(outBoundary).append("\r\nContent-Type: application/http\r\n");
            if (cid != null) {
                String id = cid.trim();
                if (id.startsWith("<") && id.endsWith(">")) {
                    id = id.substring(1, id.length() - 1);
                }
                sb.append("Content-ID: <response-").append(id).append(">\r\n");
            }
            sb.append("\r\nHTTP/1.1 ").append(sub.status).append(' ').append(reason(sub.status)).append("\r\n");
            byte[] body = sub.buffered();
            sub.headers.forEach((k, v) -> {
                if (!k.equalsIgnoreCase("Content-Length")) {
                    sb.append(k).append(": ").append(v).append("\r\n");
                }
            });
            sb.append("Content-Length: ").append(body.length).append("\r\n\r\n");
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            out.write(body);
            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        out.write(("--" + outBoundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        w.bytes(200, "multipart/mixed; boundary=" + outBoundary, out.toByteArray());
    }

    private GcsResp runSub(GcsReq parent, byte[] raw) throws IOException {
        String text = new String(raw, StandardCharsets.UTF_8);
        int split = text.indexOf("\r\n\r\n");
        String head = split < 0 ? text : text.substring(0, split);
        byte[] body = split < 0 ? new byte[0] : text.substring(split + 4).getBytes(StandardCharsets.UTF_8);
        String[] lines = head.split("\r?\n");
        String[] rl = lines[0].trim().split("\\s+");
        if (rl.length < 2) {
            throw GcsException.invalid("Malformed batch sub-request.");
        }
        Map<String, List<String>> h = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int c = lines[i].indexOf(':');
            if (c > 0) {
                h.computeIfAbsent(lines[i].substring(0, c).trim(), k -> new ArrayList<>()).add(lines[i].substring(c + 1).trim());
            }
        }
        String target = rl[1];
        if (target.startsWith("http://") || target.startsWith("https://")) {
            int slash = target.indexOf('/', target.indexOf("//") + 2);
            target = slash < 0 ? "/" : target.substring(slash);
        }
        // a sub-request without its own credentials inherits the batch request's
        GcsReq sub = GcsReq.sub(rl[0], target, h, body, parent);
        sub.principal = parent.principal;
        List<String> segs = sub.segments();
        if (segs.size() < 2 || !segs.get(0).equals("storage") || !segs.get(1).equals("v1")) {
            throw GcsException.invalid("Batch requests support only the storage/v1 metadata API.");
        }
        GcsResp resp = new GcsResp();
        try {
            handle(sub, resp, segs, true);
        } catch (GcsException e) {
            resp = new GcsResp();
            resp.bytes(e.status, "application/json; charset=UTF-8", GcsJson.error(e).toString().getBytes(StandardCharsets.UTF_8));
        }
        return resp;
    }

    private static String reason(int s) {
        return switch (s) {
            case 200 -> "OK";
            case 204 -> "No Content";
            case 304 -> "Not Modified";
            case 400 -> "Bad Request";
            case 401 -> "Unauthorized";
            case 403 -> "Forbidden";
            case 404 -> "Not Found";
            case 409 -> "Conflict";
            case 412 -> "Precondition Failed";
            default -> "Status";
        };
    }
}
