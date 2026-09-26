package com.sayonora.wire.datastorewire;

import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.ReserveIdsRequest;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RunAggregationQueryRequest;
import com.google.datastore.v1.RunQueryRequest;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Message;
import com.sayonora.wire.firestorewire.GrpcRestMux;
import com.sayonora.wire.firestorewire.ProtoJson;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Datastore REST v1 ({@code POST /v1/projects/{project}:{method}}); bodies and answers are proto3 JSON. */
final class DsRest implements GrpcRestMux.Rest {

    private static final Pattern P = Pattern.compile("^/v1/projects/([^/:]+):(lookup|runQuery|runAggregationQuery|beginTransaction|commit|rollback|allocateIds|reserveIds)$");

    private final DsService svc;
    private final DsConfig cfg;
    private final DsWireServer.Hooks hooks;

    DsRest(DsService svc, DsConfig cfg, DsWireServer.Hooks hooks) {
        this.svc = svc;
        this.cfg = cfg;
        this.hooks = hooks;
    }

    static GrpcRestMux.Resp error(int http, String status, String message) {
        JsonObject e = new JsonObject();
        e.addProperty("code", http);
        e.addProperty("message", message);
        e.addProperty("status", status);
        JsonObject o = new JsonObject();
        o.add("error", e);
        return GrpcRestMux.Resp.json(http, o.toString());
    }

    private static GrpcRestMux.Resp ok(Message m) {
        return GrpcRestMux.Resp.json(200, ProtoJson.toJson(m).toString());
    }

    @Override
    public GrpcRestMux.Resp handle(GrpcRestMux.Req r) {
        long t0 = System.nanoTime();
        String op = "Unknown";
        boolean write = false;
        try {
            if (r.path().equals("/") || r.path().isEmpty()) {
                return new GrpcRestMux.Resp(200, "text/plain; charset=UTF-8", "Ok\n".getBytes(StandardCharsets.UTF_8), java.util.Map.of());
            }
            if (!cfg.authorized(r.header("authorization"))) {
                return error(401, "UNAUTHENTICATED", "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other valid authentication credential.");
            }
            if (r.path().equals("/reset") && r.method().equals("POST")) {
                svc.store.clearAll();
                return new GrpcRestMux.Resp(200, "text/plain; charset=UTF-8", "Ok\n".getBytes(StandardCharsets.UTF_8), java.util.Map.of());
            }
            Matcher m = P.matcher(r.path());
            if (!m.matches() || !r.method().equals("POST")) {
                return new GrpcRestMux.Resp(404, "text/plain; charset=UTF-8", "Not Found\n".getBytes(StandardCharsets.UTF_8), java.util.Map.of());
            }
            String project = m.group(1);
            String verb = m.group(2);
            op = Character.toUpperCase(verb.charAt(0)) + verb.substring(1);
            String body = new String(r.body(), StandardCharsets.UTF_8).trim();
            JsonElement j = body.isEmpty() ? new JsonObject() : JsonParser.parseString(body);
            switch (verb) {
                case "lookup":
                    return ok(svc.lookup(ProtoJson.fromJson(j, LookupRequest.newBuilder()).setProjectId(project).build()));
                case "runQuery":
                    return ok(svc.runQuery(ProtoJson.fromJson(j, RunQueryRequest.newBuilder()).setProjectId(project).build()));
                case "runAggregationQuery":
                    return ok(svc.runAggregation(ProtoJson.fromJson(j, RunAggregationQueryRequest.newBuilder()).setProjectId(project).build()));
                case "beginTransaction":
                    return ok(svc.beginTransaction(ProtoJson.fromJson(j, BeginTransactionRequest.newBuilder()).setProjectId(project).build()));
                case "commit":
                    write = true;
                    return ok(svc.commit(ProtoJson.fromJson(j, CommitRequest.newBuilder()).setProjectId(project).build()));
                case "rollback":
                    return ok(svc.rollback(ProtoJson.fromJson(j, RollbackRequest.newBuilder()).setProjectId(project).build()));
                case "allocateIds":
                    write = true;
                    return ok(svc.allocateIds(ProtoJson.fromJson(j, AllocateIdsRequest.newBuilder()).setProjectId(project).build()));
                default:
                    write = true;
                    return ok(svc.reserveIds(ProtoJson.fromJson(j, ReserveIdsRequest.newBuilder()).setProjectId(project).build()));
            }
        } catch (DsException e) {
            return error(e.httpStatus(), e.code.name(), e.getMessage());
        } catch (ProtoJson.JsonError | com.google.gson.JsonParseException | IllegalStateException | ClassCastException e) {
            return error(400, "INVALID_ARGUMENT", e.getMessage() == null ? "Invalid JSON payload." : e.getMessage());
        } finally {
            hooks.record(op, write, System.nanoTime() - t0);
        }
    }
}
