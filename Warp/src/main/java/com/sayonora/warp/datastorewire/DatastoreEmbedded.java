package com.sayonora.warp.datastorewire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.firestorewire.GrpcRestMux;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/**
 * In-process access to the Datastore v1 REST surface of datastorewire for the MCP tools: {@link DsRest} over the same
 * {@link DsService}/{@link DsStore} the gRPC and REST listener use (lookup, runQuery incl. GQL, aggregation, commit), without
 * the network listener or its bearer-token check (the MCP endpoint's own governance applies instead).
 */
public final class DatastoreEmbedded {

    /** HTTP-style answer: status and proto3 JSON body. */
    public record Response(int status, String body) {
    }

    private final DsRest rest;

    public DatastoreEmbedded(BackendRegistry registry) {
        this.rest = new DsRest(new DsService(new DsStore(registry)), new DsConfig(Set.of()), (op, write, nanos) -> { });
    }

    /** {@code verb} is lookup, runQuery, runAggregationQuery, beginTransaction, commit, rollback, allocateIds or reserveIds. */
    public Response call(String project, String verb, String jsonBody) {
        GrpcRestMux.Resp r = rest.handle(new GrpcRestMux.Req("POST", "/v1/projects/" + project + ":" + verb, Map.of(), Map.of(),
                (jsonBody == null || jsonBody.isBlank() ? "{}" : jsonBody).getBytes(StandardCharsets.UTF_8)));
        return new Response(r.status(), new String(r.body(), StandardCharsets.UTF_8));
    }
}
