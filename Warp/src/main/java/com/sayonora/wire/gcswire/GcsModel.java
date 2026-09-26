package com.sayonora.wire.gcswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Rows of the gcswire store. */
final class GcsModel {

    private GcsModel() {
    }

    static final class Bucket {
        String name;
        String project;
        Instant created;
        Instant updated;
        long metageneration = 1;
        boolean versioning;
        /** all other bucket resource fields (location, storageClass, labels, cors, lifecycle, acl, ...) */
        JsonObject doc = new JsonObject();
    }

    /** One stored piece of an object's bytes: data blob {@code d}, {@code n} bytes, chunked with {@code c}-byte rows. */
    record Seg(String d, long n, int c) {
    }

    static final class Obj {
        String bucket;
        String name;
        long generation;
        long metageneration = 1;
        long size;
        String md5;
        String crc32c;
        Integer componentCount;
        String contentType;
        String contentEncoding;
        String contentDisposition;
        String cacheControl;
        String contentLanguage;
        String storageClass = "STANDARD";
        String customTime;
        Instant created;
        Instant updated;
        Instant classUpdated;
        Instant deleted;
        Map<String, String> metadata = new LinkedHashMap<>();
        /** acl, holds, retentionExpirationTime, kmsKeyName, ... */
        JsonObject doc = new JsonObject();
        List<Seg> segments = new ArrayList<>();

        boolean live() {
            return deleted == null;
        }

        String etag() {
            return GcsHash.etag(generation, metageneration);
        }

        boolean hold() {
            return bool("temporaryHold") || bool("eventBasedHold");
        }

        boolean bool(String k) {
            return doc.has(k) && !doc.get(k).isJsonNull() && doc.get(k).getAsBoolean();
        }

        Obj copy() {
            Obj o = new Obj();
            o.bucket = bucket;
            o.name = name;
            o.generation = generation;
            o.metageneration = metageneration;
            o.size = size;
            o.md5 = md5;
            o.crc32c = crc32c;
            o.componentCount = componentCount;
            o.contentType = contentType;
            o.contentEncoding = contentEncoding;
            o.contentDisposition = contentDisposition;
            o.cacheControl = cacheControl;
            o.contentLanguage = contentLanguage;
            o.storageClass = storageClass;
            o.customTime = customTime;
            o.created = created;
            o.updated = updated;
            o.classUpdated = classUpdated;
            o.deleted = deleted;
            o.metadata = new LinkedHashMap<>(metadata);
            o.doc = doc.deepCopy();
            o.segments = new ArrayList<>(segments);
            return o;
        }
    }

    /** The four generation preconditions of a request (null = not given). */
    record Pre(Long genMatch, Long genNotMatch, Long metaMatch, Long metaNotMatch) {
        static final Pre NONE = new Pre(null, null, null, null);

        boolean any() {
            return genMatch != null || genNotMatch != null || metaMatch != null || metaNotMatch != null;
        }

        /** @param live the current live object or null; @throws GcsException 412 */
        void check(Obj live) {
            if (genMatch != null && (genMatch == 0 ? live != null : live == null || live.generation != genMatch)) {
                throw GcsException.precondition("parameter", "ifGenerationMatch");
            }
            if (genNotMatch != null && (genNotMatch == 0 ? live == null : live != null && live.generation == genNotMatch)) {
                throw GcsException.precondition("parameter", "ifGenerationNotMatch");
            }
            if (metaMatch != null && (live == null || live.metageneration != metaMatch)) {
                throw GcsException.precondition("parameter", "ifMetagenerationMatch");
            }
            if (metaNotMatch != null && live != null && live.metageneration == metaNotMatch) {
                throw GcsException.precondition("parameter", "ifMetagenerationNotMatch");
            }
        }
    }

    record HmacKey(String accessId, String secret, String project, String saEmail, String state, Instant created,
            Instant updated) {
    }

    static final class Session {
        String id;
        String kind;
        String bucket;
        String name;
        JsonObject request = new JsonObject();
        List<Seg> segments = new ArrayList<>();
        long persisted;
        String state;
        JsonObject result;
    }

    static JsonArray segsJson(List<Seg> segs) {
        JsonArray a = new JsonArray();
        for (Seg s : segs) {
            JsonObject o = new JsonObject();
            o.addProperty("d", s.d());
            o.addProperty("n", s.n());
            o.addProperty("c", s.c());
            a.add(o);
        }
        return a;
    }

    static List<Seg> segsFrom(String json) {
        List<Seg> out = new ArrayList<>();
        if (json == null) {
            return out;
        }
        for (var e : com.google.gson.JsonParser.parseString(json).getAsJsonArray()) {
            JsonObject o = e.getAsJsonObject();
            out.add(new Seg(o.get("d").getAsString(), o.get("n").getAsLong(), o.get("c").getAsInt()));
        }
        return out;
    }
}
