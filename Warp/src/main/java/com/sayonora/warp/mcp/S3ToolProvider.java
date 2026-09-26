package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * S3 vocabulary for a REAL {@code s3://<bucket>} backend registered in {@code WARP_BACKENDS}
 * (AWS S3 or any S3-compatible store such as MinIO), through the AWS SDK client the connector
 * factory builds. A backend is bound to its one configured bucket: {@code bucket} arguments naming a
 * different bucket are refused, so an endpoint scoped to this backend cannot wander to other buckets
 * the credentials happen to reach. Tool names follow the S3 API operations (list_buckets,
 * list_objects, head_object, get_object, put_object, delete_object).
 */
final class S3ToolProvider implements BackendToolProvider {

    private static final int MAX_GET_BYTES = 1_048_576;
    private static final int DEFAULT_GET_BYTES = 65_536;

    private final ExternalClients clients;

    S3ToolProvider(ExternalClients clients) {
        this.clients = clients;
    }

    @Override
    public BackendKind kind() {
        return BackendKind.S3;
    }

    @Override
    public List<Tool> tools() {
        JsonObject bucket = str("Bucket name (optional: a backend is bound to its configured bucket)");
        return List.of(
                new Tool("list_buckets", "List the bucket this S3 backend is bound to.", schema(List.of()), false),
                new Tool("list_objects", "List objects (and common prefixes when a delimiter is given) in the bucket.",
                        schema(List.of(), "bucket", bucket, "prefix", str("Only keys starting with this prefix"),
                                "delimiter", str("Group keys at this delimiter (usually \"/\") into commonPrefixes"),
                                "maxKeys", num("Max keys (default 100, max 1000)"),
                                "continuationToken", str("nextContinuationToken from a truncated listing")), false),
                new Tool("head_object", "Object metadata: size, content type, last modified, etag.",
                        schema(List.of("key"), "bucket", bucket, "key", str("Object key")), false),
                new Tool("get_object", "Read an object's content (UTF-8 text, or base64 for binary), up to maxBytes.",
                        schema(List.of("key"), "bucket", bucket, "key", str("Object key"),
                                "maxBytes", num("Max bytes to return (default 65536, max 1048576)")), false),
                new Tool("put_object", "Write an object from text (content) or base64 (contentBase64).",
                        schema(List.of("key"), "bucket", bucket, "key", str("Object key"),
                                "content", str("UTF-8 text body"), "contentBase64", str("Base64 body"),
                                "contentType", str("Content-Type")), true),
                new Tool("delete_object", "Delete an object.",
                        schema(List.of("key"), "bucket", bucket, "key", str("Object key")), true));
    }

    private static String bucketOf(McpBackend b) {
        return ExternalClients.operand(b.target()).get("bucket") instanceof String s ? s : null;
    }

    @Override
    public JsonObject describe(Ctx ctx) {
        S3Client s3 = clients.s3(ctx.backend().target());
        String bucket = bucketOf(ctx.backend());
        ListObjectsV2Response r = s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).delimiter("/")
                .maxKeys(1000).build());
        JsonObject out = new JsonObject();
        out.addProperty("bucket", bucket);
        JsonArray prefixes = new JsonArray();
        r.commonPrefixes().forEach(p -> prefixes.add(p.prefix()));
        out.add("prefixes", prefixes);
        JsonArray objects = new JsonArray();
        r.contents().stream().limit(50).forEach(o -> {
            JsonObject j = new JsonObject();
            j.addProperty("key", o.key());
            j.addProperty("size", o.size());
            objects.add(j);
        });
        out.add("rootObjects", objects);
        out.addProperty("truncated", Boolean.TRUE.equals(r.isTruncated()) || r.contents().size() > 50);
        return out;
    }

    @Override
    public Outcome call(String tool, JsonObject a, Ctx ctx) {
        String bucket = bucketOf(ctx.backend());
        if (bucket == null) {
            return Outcome.error("backend \"" + ctx.backend().name() + "\" has no bucket configured (s3://<bucket>)");
        }
        String asked = optString(a, "bucket");
        if (asked != null && !asked.equals(bucket)) {
            return Outcome.error("ERROR [42501]: bucket \"" + asked + "\" is outside this backend, which is bound to "
                    + "bucket \"" + bucket + "\"");
        }
        try {
            S3Client s3 = clients.s3(ctx.backend().target());
            switch (tool) {
                case "list_buckets" -> {
                    JsonObject o = new JsonObject();
                    JsonArray arr = new JsonArray();
                    JsonObject b = new JsonObject();
                    b.addProperty("name", bucket);
                    arr.add(b);
                    o.add("buckets", arr);
                    return Outcome.ok(o.toString());
                }
                case "list_objects" -> {
                    ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(bucket);
                    Integer max = optInt(a, "maxKeys");
                    b.maxKeys(Math.max(1, Math.min(max == null ? 100 : max, 1000)));
                    if (optString(a, "prefix") != null) {
                        b.prefix(optString(a, "prefix"));
                    }
                    if (optString(a, "delimiter") != null) {
                        b.delimiter(optString(a, "delimiter"));
                    }
                    if (optString(a, "continuationToken") != null) {
                        b.continuationToken(optString(a, "continuationToken"));
                    }
                    ListObjectsV2Response r = s3.listObjectsV2(b.build());
                    JsonObject o = new JsonObject();
                    o.addProperty("bucket", bucket);
                    JsonArray prefixes = new JsonArray();
                    r.commonPrefixes().forEach(p -> prefixes.add(p.prefix()));
                    o.add("commonPrefixes", prefixes);
                    JsonArray objects = new JsonArray();
                    r.contents().forEach(x -> {
                        JsonObject j = new JsonObject();
                        j.addProperty("key", x.key());
                        j.addProperty("size", x.size());
                        j.addProperty("lastModified", x.lastModified() == null ? null : x.lastModified().toString());
                        j.addProperty("etag", x.eTag());
                        objects.add(j);
                    });
                    o.add("objects", objects);
                    o.addProperty("isTruncated", Boolean.TRUE.equals(r.isTruncated()));
                    o.addProperty("nextContinuationToken", r.nextContinuationToken());
                    return Outcome.ok(o.toString());
                }
                case "head_object" -> {
                    var h = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(requireString(a, "key")).build());
                    JsonObject o = new JsonObject();
                    o.addProperty("key", requireString(a, "key"));
                    o.addProperty("contentLength", h.contentLength());
                    o.addProperty("contentType", h.contentType());
                    o.addProperty("lastModified", h.lastModified() == null ? null : h.lastModified().toString());
                    o.addProperty("etag", h.eTag());
                    return Outcome.ok(o.toString());
                }
                case "get_object" -> {
                    Integer requested = optInt(a, "maxBytes");
                    int max = Math.max(1, Math.min(requested == null ? DEFAULT_GET_BYTES : requested, MAX_GET_BYTES));
                    ResponseBytes<GetObjectResponse> rb = s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket)
                            .key(requireString(a, "key")).range("bytes=0-" + (max - 1)).build());
                    byte[] bytes = rb.asByteArray();
                    JsonObject o = new JsonObject();
                    o.addProperty("key", requireString(a, "key"));
                    o.addProperty("contentType", rb.response().contentType());
                    String range = rb.response().contentRange();
                    long total = range != null && range.contains("/") ? Long.parseLong(range.substring(range.indexOf('/') + 1))
                            : bytes.length;
                    o.addProperty("size", total);
                    o.addProperty("truncated", total > bytes.length);
                    String text = strictUtf8(bytes);
                    if (text != null) {
                        o.addProperty("encoding", "utf-8");
                        o.addProperty("body", text);
                    } else {
                        o.addProperty("encoding", "base64");
                        o.addProperty("body", Base64.getEncoder().encodeToString(bytes));
                    }
                    return Outcome.ok(o.toString());
                }
                case "put_object" -> {
                    byte[] body;
                    if (optString(a, "contentBase64") != null) {
                        body = Base64.getDecoder().decode(optString(a, "contentBase64"));
                    } else if (optString(a, "content") != null) {
                        body = optString(a, "content").getBytes(StandardCharsets.UTF_8);
                    } else {
                        return Outcome.error("put_object needs content or contentBase64");
                    }
                    PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(bucket).key(requireString(a, "key"));
                    if (optString(a, "contentType") != null) {
                        b.contentType(optString(a, "contentType"));
                    }
                    s3.putObject(b.build(), RequestBody.fromBytes(body));
                    return Outcome.ok("{\"key\":\"" + requireString(a, "key").replace("\"", "\\\"") + "\",\"size\":"
                            + body.length + "}");
                }
                case "delete_object" -> {
                    s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(requireString(a, "key")).build());
                    return Outcome.ok("{}");
                }
                default -> {
                    return Outcome.error("unknown S3 tool: " + tool);
                }
            }
        } catch (S3Exception e) {
            String code = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "S3Exception";
            String msg = e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage();
            return Outcome.error(code + ": " + msg);
        } catch (SdkClientException e) {
            return Outcome.error("could not reach the S3 endpoint: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            return Outcome.error("invalid arguments: " + e.getMessage());
        }
    }

    private static String strictUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
