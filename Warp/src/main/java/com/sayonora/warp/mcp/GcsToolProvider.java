package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.gcswire.GcsEmbedded;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Google Cloud Storage vocabulary (gcs_* tools: list/create/delete/describe buckets, list objects with prefix and delimiter,
 * object metadata, read/write/copy/delete objects). Each tool issues the equivalent JSON API request to gcswire's own
 * {@code GcsService} in process, so preconditions, generations, naming rules and error reasons are the wire protocol's.
 */
final class GcsToolProvider extends StoreToolProvider {

    private static final int DEFAULT_GET_BYTES = 65_536;
    private static final int MAX_GET_BYTES = 1_048_576;

    GcsToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.GCS, describer, stores);
    }

    private GcsEmbedded gcs() {
        return stores.engine("gcs", GcsEmbedded::new);
    }

    private static String project(JsonObject a) {
        return gcpProject(a);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject bucket = str("Bucket name");
        JsonObject object = str("Object name (may contain '/')");
        JsonObject project = str("Project id (default: WARP_MCP_GCP_PROJECT, else \"warp-project\")");
        return List.of(
                new Tool("gcs_list_buckets", "List the buckets of a project.",
                        schema(List.of(), "project", project, "prefix", str("Only names starting with this prefix"),
                                "maxResults", num("Max buckets (default 100, max 1000)"), "pageToken", str("nextPageToken of a truncated listing")), false),
                new Tool("gcs_get_bucket_metadata", "Bucket metadata: location, storage class, versioning, labels, created.",
                        schema(List.of("bucket"), "bucket", bucket), false),
                new Tool("gcs_create_bucket", "Create a bucket.", schema(List.of("bucket"), "project", project, "bucket", bucket,
                        "location", str("Location (default US)"), "storageClass", str("Storage class (default STANDARD)")), true),
                new Tool("gcs_delete_bucket", "Delete an empty bucket.", schema(List.of("bucket"), "bucket", bucket), true),
                new Tool("gcs_list_objects", "List objects of a bucket, optionally one directory level (delimiter \"/\").",
                        schema(List.of("bucket"), "bucket", bucket, "prefix", str("Only objects starting with this prefix"),
                                "delimiter", str("Group object names at this delimiter into prefixes"),
                                "maxResults", num("Max objects (default 100, max 1000)"), "pageToken", str("nextPageToken of a truncated listing"),
                                "versions", bool("Include noncurrent object versions")), false),
                new Tool("gcs_get_object_metadata", "Object metadata: size, content type, generation, md5/crc32c, updated.",
                        schema(List.of("bucket", "object"), "bucket", bucket, "object", object), false),
                new Tool("gcs_get_object", "Read an object's content (UTF-8 text, or base64 for binary), up to maxBytes.",
                        schema(List.of("bucket", "object"), "bucket", bucket, "object", object,
                                "maxBytes", num("Max bytes to return (default 65536, max 1048576)")), false),
                new Tool("gcs_put_object", "Write an object from text (content) or base64 (contentBase64), replacing any live object.",
                        schema(List.of("bucket", "object"), "bucket", bucket, "object", object, "content", str("UTF-8 text body"),
                                "contentBase64", str("Base64 body"), "contentType", str("Content-Type (default application/octet-stream)")), true),
                new Tool("gcs_copy_object", "Copy an object within or across buckets.",
                        schema(List.of("sourceBucket", "sourceObject", "destinationBucket", "destinationObject"),
                                "sourceBucket", bucket, "sourceObject", object, "destinationBucket", bucket, "destinationObject", object), true),
                new Tool("gcs_delete_object", "Delete an object (or one generation of it).",
                        schema(List.of("bucket", "object"), "bucket", bucket, "object", object, "generation", str("Specific generation")), true));
    }

    private Outcome fail(GcsEmbedded.Response r) {
        String message = r.text();
        try {
            JsonObject e = JsonParser.parseString(message).getAsJsonObject().getAsJsonObject("error");
            String reason = e.has("errors") && e.getAsJsonArray("errors").size() > 0
                    ? e.getAsJsonArray("errors").get(0).getAsJsonObject().get("reason").getAsString() : null;
            message = (reason == null ? "" : reason + ": ") + e.get("message").getAsString();
        } catch (RuntimeException ignored) {
            // not the JSON error shape
        }
        return Outcome.error(message + " (HTTP " + r.status() + ")");
    }

    private static String q(Object... kv) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i + 1] != null) {
                sb.append(sb.length() == 0 ? "" : "&").append(enc((String) kv[i])).append('=').append(enc(String.valueOf(kv[i + 1])));
            }
        }
        return sb.toString();
    }

    private GcsEmbedded.Response get(String path, String query, Map<String, List<String>> h) {
        return gcs().request("GET", path, query, h, null);
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        String b = a.has("bucket") ? enc(requireString(a, "bucket")) : null;
        String o = a.has("object") ? enc(requireString(a, "object")) : null;
        switch (tool) {
            case "gcs_list_buckets": {
                GcsEmbedded.Response r = get("/storage/v1/b", q("project", project(a), "prefix", optString(a, "prefix"),
                        "maxResults", String.valueOf(limit(a, "maxResults", 100, 1000)), "pageToken", optString(a, "pageToken")), null);
                if (r.status() >= 300) {
                    return fail(r);
                }
                JsonObject in = JsonParser.parseString(r.text()).getAsJsonObject();
                JsonObject out = new JsonObject();
                out.add("buckets", in.has("items") ? in.get("items") : new JsonArray());
                out.add("nextPageToken", in.has("nextPageToken") ? in.get("nextPageToken") : com.google.gson.JsonNull.INSTANCE);
                return json(out);
            }
            case "gcs_get_bucket_metadata": {
                GcsEmbedded.Response r = get("/storage/v1/b/" + b, null, null);
                return r.status() >= 300 ? fail(r) : json(JsonParser.parseString(r.text()));
            }
            case "gcs_create_bucket": {
                JsonObject body = new JsonObject();
                body.addProperty("name", requireString(a, "bucket"));
                if (optString(a, "location") != null) {
                    body.addProperty("location", optString(a, "location"));
                }
                if (optString(a, "storageClass") != null) {
                    body.addProperty("storageClass", optString(a, "storageClass"));
                }
                GcsEmbedded.Response r = gcs().request("POST", "/storage/v1/b", q("project", project(a)),
                        Map.of("Content-Type", List.of("application/json")), body.toString().getBytes(StandardCharsets.UTF_8));
                return r.status() >= 300 ? fail(r) : json(JsonParser.parseString(r.text()));
            }
            case "gcs_delete_bucket": {
                GcsEmbedded.Response r = gcs().request("DELETE", "/storage/v1/b/" + b, null, null, null);
                return r.status() >= 300 ? fail(r) : json(ok(requireString(a, "bucket")));
            }
            case "gcs_list_objects": {
                GcsEmbedded.Response r = get("/storage/v1/b/" + b + "/o", q("prefix", optString(a, "prefix"),
                        "delimiter", optString(a, "delimiter"), "maxResults", String.valueOf(limit(a, "maxResults", 100, 1000)),
                        "pageToken", optString(a, "pageToken"), "versions", optBool(a, "versions", false) ? "true" : null), null);
                if (r.status() >= 300) {
                    return fail(r);
                }
                JsonObject in = JsonParser.parseString(r.text()).getAsJsonObject();
                JsonObject out = new JsonObject();
                out.addProperty("bucket", requireString(a, "bucket"));
                out.add("objects", in.has("items") ? slim(in.getAsJsonArray("items")) : new JsonArray());
                out.add("prefixes", in.has("prefixes") ? in.get("prefixes") : new JsonArray());
                out.add("nextPageToken", in.has("nextPageToken") ? in.get("nextPageToken") : com.google.gson.JsonNull.INSTANCE);
                return json(out);
            }
            case "gcs_get_object_metadata": {
                GcsEmbedded.Response r = get("/storage/v1/b/" + b + "/o/" + o, null, null);
                return r.status() >= 300 ? fail(r) : json(JsonParser.parseString(r.text()));
            }
            case "gcs_get_object": {
                GcsEmbedded.Response meta = get("/storage/v1/b/" + b + "/o/" + o, null, null);
                if (meta.status() >= 300) {
                    return fail(meta);
                }
                JsonObject m = JsonParser.parseString(meta.text()).getAsJsonObject();
                long size = m.has("size") ? Long.parseLong(m.get("size").getAsString()) : 0;
                int max = limit(a, "maxBytes", DEFAULT_GET_BYTES, MAX_GET_BYTES);
                byte[] data = new byte[0];
                if (size > 0) {
                    GcsEmbedded.Response r = get("/download/storage/v1/b/" + b + "/o/" + o, "alt=media",
                            Map.of("Range", List.of("bytes=0-" + (max - 1))));
                    if (r.status() >= 300) {
                        return fail(r);
                    }
                    data = r.body();
                }
                JsonObject out = new JsonObject();
                out.addProperty("object", requireString(a, "object"));
                out.add("contentType", m.get("contentType"));
                out.add("generation", m.get("generation"));
                putBody(out, data, size);
                return json(out);
            }
            case "gcs_put_object": {
                byte[] body = bodyArg(a, "content", "contentBase64");
                String ct = optString(a, "contentType") == null ? "application/octet-stream" : optString(a, "contentType");
                GcsEmbedded.Response r = gcs().request("POST", "/upload/storage/v1/b/" + b + "/o",
                        q("uploadType", "media", "name", requireString(a, "object")), Map.of("Content-Type", List.of(ct)), body);
                return r.status() >= 300 ? fail(r) : json(slim(JsonParser.parseString(r.text()).getAsJsonObject()));
            }
            case "gcs_copy_object": {
                String path = "/storage/v1/b/" + enc(requireString(a, "sourceBucket")) + "/o/" + enc(requireString(a, "sourceObject"))
                        + "/copyTo/b/" + enc(requireString(a, "destinationBucket")) + "/o/" + enc(requireString(a, "destinationObject"));
                GcsEmbedded.Response r = gcs().request("POST", path, null, null, null);
                return r.status() >= 300 ? fail(r) : json(slim(JsonParser.parseString(r.text()).getAsJsonObject()));
            }
            case "gcs_delete_object": {
                GcsEmbedded.Response r = gcs().request("DELETE", "/storage/v1/b/" + b + "/o/" + o, q("generation", optString(a, "generation")), null, null);
                return r.status() >= 300 ? fail(r) : json(ok(requireString(a, "object")));
            }
            default:
                return Outcome.error("unknown gcs tool: " + tool);
        }
    }

    private static JsonObject ok(String name) {
        JsonObject o = new JsonObject();
        o.addProperty("ok", true);
        o.addProperty("name", name);
        return o;
    }

    private static final List<String> KEEP = List.of("bucket", "name", "size", "contentType", "generation", "metageneration", "md5Hash",
            "crc32c", "etag", "updated", "timeCreated", "storageClass", "metadata");

    private static JsonObject slim(JsonObject o) {
        JsonObject s = new JsonObject();
        for (String k : KEEP) {
            if (o.has(k)) {
                s.add(k, o.get(k));
            }
        }
        return s;
    }

    private static JsonArray slim(JsonArray arr) {
        JsonArray out = new JsonArray();
        for (JsonElement e : arr) {
            out.add(slim(e.getAsJsonObject()));
        }
        return out;
    }
}
