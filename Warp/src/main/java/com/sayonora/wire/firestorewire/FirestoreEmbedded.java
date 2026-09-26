package com.sayonora.wire.firestorewire;

import com.sayonora.wire.core.BackendRegistry;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In-process access to the Firestore v1 REST surface of firestorewire for the MCP tools: {@link FsRest} over the same
 * {@link FsService}/{@link FsStore} the gRPC and REST listener use (documents, structured queries, aggregations, commits),
 * without the network listener or its bearer-token check (the MCP endpoint's own governance applies instead).
 */
public final class FirestoreEmbedded {

    /** HTTP-style answer: status and proto3 JSON body. */
    public record Response(int status, String body) {
    }

    private final FsRest rest;

    public FirestoreEmbedded(BackendRegistry registry) {
        FsConfig cfg = new FsConfig(Set.of(), FsConfig.fromEnv().historySeconds);
        FsService svc = new FsService(new FsStore(registry, cfg.historySeconds));
        this.rest = new FsRest(svc, cfg, (op, write, nanos) -> { });
    }

    /** {@code path} like {@code /v1/projects/p/databases/(default)/documents/users/u1}; {@code rawQuery} unencoded-or-encoded. */
    public Response request(String method, String path, String rawQuery, String body) {
        Map<String, List<String>> q = new LinkedHashMap<>();
        if (rawQuery != null && !rawQuery.isEmpty()) {
            for (String pair : rawQuery.split("&")) {
                int eq = pair.indexOf('=');
                String k = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
                String v = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
                q.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
            }
        }
        GrpcRestMux.Resp r = rest.handle(new GrpcRestMux.Req(method, path, q, Map.of(),
                body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8)));
        return new Response(r.status(), new String(r.body(), StandardCharsets.UTF_8));
    }
}
