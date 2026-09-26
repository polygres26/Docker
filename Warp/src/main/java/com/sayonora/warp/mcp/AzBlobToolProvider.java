package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.AzureToolSupport.*;
import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonObject;
import com.sayonora.warp.azurewire.AzureEmbedded;
import java.util.List;
import java.util.Map;

/**
 * Azure Blob Storage vocabulary (azblob_* tools mirroring the container/blob operations of Azure's own tooling: list
 * containers, create/delete container, list blobs with prefix/delimiter, get and upload/delete a blob, blob properties).
 * Each tool sends the equivalent Blob REST request to azurewire's own {@code BlobService} in process, so ETags, errors and
 * naming rules are the wire protocol's. {@code account} defaults to the only configured storage account.
 */
final class AzBlobToolProvider extends StoreToolProvider {

    private static final int DEFAULT_GET_BYTES = 65_536;
    private static final int MAX_GET_BYTES = 1_048_576;

    AzBlobToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.AZBLOB, describer, stores);
    }

    private AzureEmbedded az() {
        return stores.engine("azblob", AzureEmbedded::blob);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject acct = str("Storage account (default: the only configured account)");
        JsonObject container = str("Container name");
        JsonObject blob = str("Blob name (may contain '/')");
        return List.of(
                new Tool("azblob_list_containers", "List the containers of a storage account.",
                        schema(List.of(), "account", acct, "prefix", str("Only names starting with this prefix"),
                                "maxResults", num("Max containers (default 100, max 1000)"), "marker", str("nextMarker of a truncated listing")), false),
                new Tool("azblob_create_container", "Create a container.", schema(List.of("container"), "account", acct,
                        "container", container), true),
                new Tool("azblob_delete_container", "Delete a container and everything in it.", schema(List.of("container"),
                        "account", acct, "container", container), true),
                new Tool("azblob_list_blobs", "List the blobs of a container (flat, or one level with a delimiter).",
                        schema(List.of("container"), "account", acct, "container", container, "prefix", str("Only blobs starting with this prefix"),
                                "delimiter", str("Group blob names at this delimiter (usually \"/\") into prefixes"),
                                "maxResults", num("Max entries (default 100, max 1000)"), "marker", str("nextMarker of a truncated listing")), false),
                new Tool("azblob_get_blob_properties", "Blob properties: size, content type, ETag, last modified, metadata.",
                        schema(List.of("container", "blob"), "account", acct, "container", container, "blob", blob), false),
                new Tool("azblob_get_blob", "Read a blob's content (UTF-8 text, or base64 for binary), up to maxBytes.",
                        schema(List.of("container", "blob"), "account", acct, "container", container, "blob", blob,
                                "maxBytes", num("Max bytes to return (default 65536, max 1048576)")), false),
                new Tool("azblob_upload_blob", "Upload a block blob from text (content) or base64 (contentBase64), replacing any existing blob.",
                        schema(List.of("container", "blob"), "account", acct, "container", container, "blob", blob,
                                "content", str("UTF-8 text body"), "contentBase64", str("Base64 body"), "contentType", str("Content-Type")), true),
                new Tool("azblob_delete_blob", "Delete a blob.", schema(List.of("container", "blob"), "account", acct,
                        "container", container, "blob", blob), true));
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) throws Exception {
        String account = account(a);
        switch (tool) {
            case "azblob_list_containers": {
                AzureEmbedded.Response r = az().request(account, "GET", "", query(List.of(q("comp", "list"), q("prefix", optString(a, "prefix")),
                        q("maxresults", String.valueOf(limit(a, "maxResults", 100, 1000))), q("marker", optString(a, "marker")))), Map.of(), null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                var doc = parse(r.text());
                JsonObject o = new JsonObject();
                o.add("containers", all(doc, "Container", 1000));
                o.addProperty("nextMarker", childText(doc.getDocumentElement(), "NextMarker"));
                return json(o);
            }
            case "azblob_create_container": {
                AzureEmbedded.Response r = az().request(account, "PUT", "/" + enc(requireString(a, "container")), "restype=container", Map.of(), null);
                return r.status() >= 300 ? failure(r) : json(status(r, "container", requireString(a, "container")));
            }
            case "azblob_delete_container": {
                AzureEmbedded.Response r = az().request(account, "DELETE", "/" + enc(requireString(a, "container")), "restype=container", Map.of(), null);
                return r.status() >= 300 ? failure(r) : json(status(r, "container", requireString(a, "container")));
            }
            case "azblob_list_blobs": {
                AzureEmbedded.Response r = az().request(account, "GET", "/" + enc(requireString(a, "container")),
                        query(List.of(q("restype", "container"), q("comp", "list"), q("prefix", optString(a, "prefix")),
                                q("delimiter", optString(a, "delimiter")), q("maxresults", String.valueOf(limit(a, "maxResults", 100, 1000))),
                                q("marker", optString(a, "marker")))), Map.of(), null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                var doc = parse(r.text());
                JsonObject o = new JsonObject();
                o.addProperty("container", requireString(a, "container"));
                o.add("blobs", all(doc, "Blob", 1000));
                o.add("prefixes", all(doc, "BlobPrefix", 1000));
                o.addProperty("nextMarker", childText(doc.getDocumentElement(), "NextMarker"));
                return json(o);
            }
            case "azblob_get_blob_properties": {
                AzureEmbedded.Response r = az().request(account, "HEAD", blobPath(a), null, Map.of(), null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                o.addProperty("blob", requireString(a, "blob"));
                JsonObject props = new JsonObject();
                r.headers().forEach((k, v) -> {
                    String l = k.toLowerCase(java.util.Locale.ROOT);
                    if (l.startsWith("x-ms-meta-") || l.equals("content-length") || l.equals("content-type") || l.equals("etag")
                            || l.equals("last-modified") || l.equals("x-ms-blob-type") || l.equals("content-md5") || l.equals("x-ms-lease-state")) {
                        props.addProperty(l, v);
                    }
                });
                o.add("properties", props);
                return json(o);
            }
            case "azblob_get_blob": {
                int max = limit(a, "maxBytes", DEFAULT_GET_BYTES, MAX_GET_BYTES);
                AzureEmbedded.Response r = az().request(account, "GET", blobPath(a), null, Map.of("Range", "bytes=0-" + (max - 1)), null);
                JsonObject o = new JsonObject();
                o.addProperty("blob", requireString(a, "blob"));
                if (r.status() == 416) {
                    o.addProperty("size", 0);
                    o.addProperty("encoding", "utf-8");
                    o.addProperty("body", "");
                    o.addProperty("truncated", false);
                    return json(o);
                }
                if (r.status() >= 300) {
                    return failure(r);
                }
                String range = r.headers().get("Content-Range");
                long total = range != null && range.contains("/") ? Long.parseLong(range.substring(range.indexOf('/') + 1))
                        : r.body().length;
                o.addProperty("contentType", r.headers().get("Content-Type"));
                o.addProperty("etag", r.headers().get("ETag"));
                putBody(o, r.body(), total);
                return json(o);
            }
            case "azblob_upload_blob": {
                byte[] body = bodyArg(a, "content", "contentBase64");
                Map<String, String> h = new java.util.LinkedHashMap<>();
                h.put("x-ms-blob-type", "BlockBlob");
                if (optString(a, "contentType") != null) {
                    h.put("Content-Type", optString(a, "contentType"));
                }
                AzureEmbedded.Response r = az().request(account, "PUT", blobPath(a), null, h, body);
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                o.addProperty("blob", requireString(a, "blob"));
                o.addProperty("size", body.length);
                o.addProperty("etag", r.headers().get("ETag"));
                return json(o);
            }
            case "azblob_delete_blob": {
                AzureEmbedded.Response r = az().request(account, "DELETE", blobPath(a), null, Map.of(), null);
                return r.status() >= 300 ? failure(r) : json(status(r, "blob", requireString(a, "blob")));
            }
            default:
                return Outcome.error("unknown azblob tool: " + tool);
        }
    }

    private static String blobPath(JsonObject a) {
        return "/" + enc(requireString(a, "container")) + "/" + encPath(requireString(a, "blob"));
    }

    private static String query(List<String[]> pairs) {
        return queryOf(pairs);
    }

    private static JsonObject status(AzureEmbedded.Response r, String k, String v) {
        JsonObject o = new JsonObject();
        o.addProperty(k, v);
        o.addProperty("ok", true);
        o.addProperty("status", r.status());
        return o;
    }
}
