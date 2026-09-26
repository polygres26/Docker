package com.sayonora.wire.gcswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.wire.gcswire.GcsModel.Bucket;
import com.sayonora.wire.gcswire.GcsModel.Obj;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

/** JSON renderings of GCS resources and errors, exactly in the shape of the real service. */
final class GcsJson {

    private GcsJson() {
    }

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    /** RFC 3339 with millisecond precision, the way GCS prints timestamps. */
    static String ts(Instant i) {
        return TS.format(i);
    }

    static String projectNumber(String project) {
        long h = 0;
        for (char c : project.toCharArray()) {
            h = h * 31 + c;
        }
        return String.valueOf(100000000000L + Math.abs(h) % 899999999999L);
    }

    // ------------------------------------------------------------------------------------------ errors

    static JsonObject error(GcsException e) {
        JsonObject err = new JsonObject();
        err.addProperty("code", e.status);
        err.addProperty("message", e.getMessage());
        JsonArray errors = new JsonArray();
        JsonObject one = new JsonObject();
        one.addProperty("message", e.getMessage());
        one.addProperty("domain", "global");
        one.addProperty("reason", e.reason);
        if (e.locationType != null) {
            one.addProperty("locationType", e.locationType);
            one.addProperty("location", e.location);
        }
        errors.add(one);
        err.add("errors", errors);
        JsonObject o = new JsonObject();
        o.add("error", err);
        return o;
    }

    // ------------------------------------------------------------------------------------------ ACLs

    private static final String[] PREDEFINED_OBJECT = {"authenticatedRead", "bucketOwnerFullControl", "bucketOwnerRead", "private",
        "projectPrivate", "publicRead"};
    private static final String[] PREDEFINED_BUCKET = {"authenticatedRead", "private", "projectPrivate", "publicRead", "publicReadWrite"};

    static boolean validPredefined(String p, boolean bucket) {
        for (String s : bucket ? PREDEFINED_BUCKET : PREDEFINED_OBJECT) {
            if (s.equals(p)) {
                return true;
            }
        }
        return false;
    }

    private static JsonObject entry(String entity, String role) {
        JsonObject o = new JsonObject();
        o.addProperty("entity", entity);
        o.addProperty("role", role);
        return o;
    }

    /** The entries a predefined ACL stands for (owner = project-owners team of the bucket's project). */
    static JsonArray predefined(String p, String project, boolean bucketAcl) {
        String n = projectNumber(project);
        JsonArray a = new JsonArray();
        switch (p == null ? "projectPrivate" : p) {
            case "private" -> a.add(entry("project-owners-" + n, "OWNER"));
            case "publicRead" -> {
                a.add(entry("project-owners-" + n, "OWNER"));
                a.add(entry("allUsers", "READER"));
            }
            case "publicReadWrite" -> {
                a.add(entry("project-owners-" + n, "OWNER"));
                a.add(entry("allUsers", "WRITER"));
            }
            case "authenticatedRead" -> {
                a.add(entry("project-owners-" + n, "OWNER"));
                a.add(entry("allAuthenticatedUsers", "READER"));
            }
            case "bucketOwnerRead" -> {
                a.add(entry("project-owners-" + n, "READER"));
            }
            case "bucketOwnerFullControl" -> {
                a.add(entry("project-owners-" + n, "OWNER"));
            }
            default -> {
                a.add(entry("project-owners-" + n, "OWNER"));
                a.add(entry("project-editors-" + n, "OWNER"));
                a.add(entry("project-viewers-" + n, "READER"));
            }
        }
        return a;
    }

    static JsonArray renderAcl(JsonArray stored, String kind, String bucket, String object, Long generation, String base) {
        JsonArray out = new JsonArray();
        for (JsonElement e : stored) {
            JsonObject in = e.getAsJsonObject();
            JsonObject o = new JsonObject();
            o.addProperty("kind", kind);
            String entity = in.get("entity").getAsString();
            String id = bucket + (object == null ? "" : "/" + object + (generation == null ? "" : "/" + generation)) + "/" + entity;
            o.addProperty("id", id);
            o.addProperty("selfLink", base + "/storage/v1/b/" + GcsReq.enc(bucket) + (object == null
                    ? (kind.equals("storage#bucketAccessControl") ? "/acl/" : "/defaultObjectAcl/")
                    : "/o/" + GcsReq.enc(object) + "/acl/") + GcsReq.enc(entity));
            o.addProperty("bucket", bucket);
            if (object != null) {
                o.addProperty("object", object);
                if (generation != null) {
                    o.addProperty("generation", String.valueOf(generation));
                }
            }
            o.addProperty("entity", entity);
            o.addProperty("role", in.get("role").getAsString());
            for (String k : new String[] {"email", "entityId", "domain", "projectTeam"}) {
                if (in.has(k)) {
                    o.add(k, in.get(k));
                }
            }
            o.addProperty("etag", GcsHash.etag(id.hashCode() & 0xffffffffL, 0));
            out.add(o);
        }
        return out;
    }

    // ------------------------------------------------------------------------------------------ buckets

    /** Fields kept in the doc that are served by dedicated endpoints and not part of the plain bucket body. */
    private static final Set<String> BUCKET_INTERNAL = Set.of("acl", "defaultObjectAcl", "iamPolicy", "notificationConfigs");

    static JsonObject bucket(Bucket b, String base, String projection) {
        JsonObject o = new JsonObject();
        o.addProperty("kind", "storage#bucket");
        o.addProperty("selfLink", base + "/storage/v1/b/" + GcsReq.enc(b.name));
        o.addProperty("id", b.name);
        o.addProperty("name", b.name);
        o.addProperty("projectNumber", projectNumber(b.project));
        o.addProperty("metageneration", String.valueOf(b.metageneration));
        for (Map.Entry<String, JsonElement> e : b.doc.entrySet()) {
            if (!BUCKET_INTERNAL.contains(e.getKey())) {
                o.add(e.getKey(), e.getValue());
            }
        }
        o.addProperty("etag", GcsHash.etag(b.created.toEpochMilli(), b.metageneration));
        o.addProperty("timeCreated", ts(b.created));
        o.addProperty("updated", ts(b.updated));
        if ("full".equalsIgnoreCase(projection)) {
            if (b.doc.has("acl")) {
                o.add("acl", renderAcl(b.doc.getAsJsonArray("acl"), "storage#bucketAccessControl", b.name, null, null, base));
            }
            if (b.doc.has("defaultObjectAcl")) {
                o.add("defaultObjectAcl", renderAcl(b.doc.getAsJsonArray("defaultObjectAcl"), "storage#objectAccessControl", b.name,
                        null, null, base));
            }
            JsonObject owner = new JsonObject();
            owner.addProperty("entity", "project-owners-" + projectNumber(b.project));
            o.add("owner", owner);
        }
        return o;
    }

    // ------------------------------------------------------------------------------------------ objects

    static JsonObject object(Obj x, String base, String projection) {
        JsonObject o = new JsonObject();
        String enc = GcsReq.enc(x.name);
        o.addProperty("kind", "storage#object");
        o.addProperty("id", x.bucket + "/" + x.name + "/" + x.generation);
        o.addProperty("selfLink", base + "/storage/v1/b/" + GcsReq.enc(x.bucket) + "/o/" + enc);
        o.addProperty("mediaLink", base + "/download/storage/v1/b/" + GcsReq.enc(x.bucket) + "/o/" + enc + "?generation="
                + x.generation + "&alt=media");
        o.addProperty("name", x.name);
        o.addProperty("bucket", x.bucket);
        o.addProperty("generation", String.valueOf(x.generation));
        o.addProperty("metageneration", String.valueOf(x.metageneration));
        if (x.contentType != null) {
            o.addProperty("contentType", x.contentType);
        }
        o.addProperty("storageClass", x.storageClass);
        o.addProperty("size", String.valueOf(x.size));
        if (x.md5 != null) {
            o.addProperty("md5Hash", x.md5);
        }
        o.addProperty("crc32c", x.crc32c);
        if (x.componentCount != null) {
            o.addProperty("componentCount", x.componentCount);
        }
        o.addProperty("etag", x.etag());
        o.addProperty("timeCreated", ts(x.created));
        o.addProperty("updated", ts(x.updated));
        o.addProperty("timeStorageClassUpdated", ts(x.classUpdated));
        if (x.deleted != null) {
            o.addProperty("timeDeleted", ts(x.deleted));
        }
        if (x.contentEncoding != null) {
            o.addProperty("contentEncoding", x.contentEncoding);
        }
        if (x.contentDisposition != null) {
            o.addProperty("contentDisposition", x.contentDisposition);
        }
        if (x.cacheControl != null) {
            o.addProperty("cacheControl", x.cacheControl);
        }
        if (x.contentLanguage != null) {
            o.addProperty("contentLanguage", x.contentLanguage);
        }
        if (x.customTime != null) {
            o.addProperty("customTime", x.customTime);
        }
        if (!x.metadata.isEmpty()) {
            JsonObject md = new JsonObject();
            x.metadata.forEach(md::addProperty);
            o.add("metadata", md);
        }
        for (String k : new String[] {"eventBasedHold", "temporaryHold", "retentionExpirationTime", "kmsKeyName"}) {
            if (x.doc.has(k)) {
                o.add(k, x.doc.get(k));
            }
        }
        if ("full".equalsIgnoreCase(projection)) {
            if (x.doc.has("acl")) {
                o.add("acl", renderAcl(x.doc.getAsJsonArray("acl"), "storage#objectAccessControl", x.bucket, x.name, x.generation, base));
            }
            JsonObject owner = new JsonObject();
            owner.addProperty("entity", "project-owners");
            o.add("owner", owner);
        }
        return o;
    }
}
