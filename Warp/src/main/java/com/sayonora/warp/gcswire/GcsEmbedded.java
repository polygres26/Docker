package com.sayonora.warp.gcswire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.StoreType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * In-process access to the Google Cloud Storage JSON API of gcswire for the MCP tools: a request goes through the same
 * {@link GcsService} routing, validation and {@link GcsStore} Postgres storage a wire client reaches, minus the HTTP
 * listener and its authentication (the MCP endpoint's own token, scope and governance apply instead).
 */
public final class GcsEmbedded {

    /** One answer: HTTP status, content type, headers and body. */
    public record Response(int status, String contentType, Map<String, String> headers, byte[] body) {
        public String text() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private final GcsService json;

    public GcsEmbedded(BackendRegistry registry) {
        GcsConfig cfg = GcsConfig.fromEnv();
        this.json = new GcsService(new GcsStore(new GcsShards(registry, StoreType.GCS), cfg), cfg);
    }

    /** {@code path} is a JSON API path such as {@code /storage/v1/b/my-bucket/o}; {@code rawQuery} is already encoded. */
    public Response request(String method, String path, String rawQuery, Map<String, List<String>> headers, byte[] body) {
        GcsReq r = new GcsReq(method, path, rawQuery, headers == null ? Map.of() : headers, "localhost", "http",
                new ByteArrayInputStream(body == null ? new byte[0] : body));
        GcsResp w = new GcsResp();
        try {
            json.handle(r, w, r.segments(), false);
        } catch (GcsException e) {
            w = new GcsResp();
            try {
                w.bytes(e.status, "application/json; charset=UTF-8", GcsJson.error(e).toString().getBytes(StandardCharsets.UTF_8));
            } catch (IOException io) {
                throw new IllegalStateException(io);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return new Response(w.status, w.headers.get("Content-Type"), w.headers, w.buffered());
    }

    public static String encode(String s) {
        return GcsReq.enc(s);
    }
}
