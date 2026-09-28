package com.sayonora.warp.firestorewire;

import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BeginTransactionRequest;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CreateDocumentRequest;
import com.google.firestore.v1.DeleteDocumentRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentMask;
import com.google.firestore.v1.GetDocumentRequest;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.RollbackRequest;
import com.google.firestore.v1.RunAggregationQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.UpdateDocumentRequest;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Firestore REST v1 ({@code projects.databases.documents.*}) over {@link FsService}. Bodies and answers are proto3 JSON. */
final class FsRest implements GrpcRestMux.Rest {

    private static final Pattern VERB = Pattern.compile("^(.*):(batchGet|beginTransaction|commit|rollback|batchWrite|runQuery|runAggregationQuery|partitionQuery|listCollectionIds)$");

    private final FsService svc;
    private final FsConfig cfg;
    private final FsWireServer.Hooks hooks;

    FsRest(FsService svc, FsConfig cfg, FsWireServer.Hooks hooks) {
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

    static GrpcRestMux.Resp error(FsException e) {
        return error(e.httpStatus(), e.statusName(), e.getMessage());
    }

    @Override
    public GrpcRestMux.Resp handle(GrpcRestMux.Req r) {
        long t0 = System.nanoTime();
        String[] op = {"Unknown"};
        boolean[] write = {false};
        try {
            if (r.path().equals("/") || r.path().isEmpty()) {
                return new GrpcRestMux.Resp(200, "text/plain; charset=UTF-8", "Ok".getBytes(StandardCharsets.UTF_8), Map.of());
            }
            if (!cfg.authorized(r.header("authorization"))) {
                return error(401, "UNAUTHENTICATED", "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other valid authentication credential.");
            }
            if (r.path().startsWith("/emulator/v1/")) {
                return emulator(r, op, write);
            }
            String p = r.path();
            if (!p.startsWith("/v1/")) {
                return error(404, "NOT_FOUND", "Method not found.");
            }
            p = p.substring(4);
            return route(r, p, op, write);
        } catch (FsException e) {
            return error(e);
        } catch (ProtoJson.JsonError | com.google.gson.JsonParseException | IllegalStateException | ClassCastException e) {
            return error(400, "INVALID_ARGUMENT", e.getMessage() == null ? "Invalid JSON payload." : e.getMessage());
        } finally {
            hooks.record(op[0], write[0], System.nanoTime() - t0);
        }
    }

    private GrpcRestMux.Resp emulator(GrpcRestMux.Req r, String[] op, boolean[] write) {
        // DELETE /emulator/v1/projects/{p}/databases/{d}/documents : clear all data (a documented emulator convenience)
        Matcher m = Pattern.compile("^/emulator/v1/projects/([^/]+)/databases/([^/]+)/documents/?$").matcher(r.path());
        if (m.matches() && r.method().equals("DELETE")) {
            op[0] = "ClearDatabase";
            write[0] = true;
            svc.store.clear(new FsNames.Db(m.group(1), m.group(2)));
            return GrpcRestMux.Resp.json(200, "{}");
        }
        if (r.path().matches("^/emulator/v1/projects/[^/]+:securityRules$")) {
            op[0] = "SecurityRules";
            return GrpcRestMux.Resp.json(200, "{}"); // rules are not evaluated by Warp; accepted and ignored
        }
        return error(404, "NOT_FOUND", "Method not found.");
    }

    private static JsonElement body(GrpcRestMux.Req r) {
        String s = new String(r.body(), StandardCharsets.UTF_8).trim();
        return s.isEmpty() ? new JsonObject() : JsonParser.parseString(s);
    }

    private static GrpcRestMux.Resp ok(Message m) {
        return GrpcRestMux.Resp.json(200, ProtoJson.toJson(m).toString());
    }

    private static GrpcRestMux.Resp ok(List<? extends Message> ms) {
        JsonArray a = new JsonArray();
        for (Message m : ms) {
            a.add(ProtoJson.toJson(m));
        }
        return GrpcRestMux.Resp.json(200, a.toString());
    }

    private GrpcRestMux.Resp route(GrpcRestMux.Req r, String p, String[] op, boolean[] write) {
        Matcher vm = VERB.matcher(p);
        if (vm.matches() && r.method().equals("POST")) {
            String target = vm.group(1);
            String verb = vm.group(2);
            op[0] = capital(verb);
            JsonElement j = body(r);
            switch (verb) {
                case "batchGet": {
                    var b = ProtoJson.fromJson(j, BatchGetDocumentsRequest.newBuilder()).setDatabase(dbOf(target));
                    java.util.List<com.google.firestore.v1.BatchGetDocumentsResponse> out = new java.util.ArrayList<>();
                    svc.batchGet(b.build(), out::add);
                    return ok(out);
                }
                case "beginTransaction": {
                    var b = ProtoJson.fromJson(j, BeginTransactionRequest.newBuilder()).setDatabase(dbOf(target));
                    var t = svc.begin(FsNames.parseDb(b.getDatabase()), b.getOptions());
                    return ok(com.google.firestore.v1.BeginTransactionResponse.newBuilder().setTransaction(t.id).build());
                }
                case "commit": {
                    write[0] = true;
                    var b = ProtoJson.fromJson(j, CommitRequest.newBuilder()).setDatabase(dbOf(target));
                    return ok(svc.commit(b.build()));
                }
                case "rollback": {
                    var b = ProtoJson.fromJson(j, RollbackRequest.newBuilder()).setDatabase(dbOf(target));
                    svc.rollback(FsNames.parseDb(b.getDatabase()), b.getTransaction());
                    return GrpcRestMux.Resp.json(200, "{}");
                }
                case "batchWrite": {
                    write[0] = true;
                    var b = ProtoJson.fromJson(j, BatchWriteRequest.newBuilder()).setDatabase(dbOf(target));
                    return ok(svc.batchWrite(b.build()));
                }
                case "runQuery": {
                    var b = ProtoJson.fromJson(j, RunQueryRequest.newBuilder()).setParent(target);
                    java.util.List<com.google.firestore.v1.RunQueryResponse> out = new java.util.ArrayList<>();
                    svc.runQuery(b.build(), out::add);
                    return ok(out);
                }
                case "runAggregationQuery": {
                    var b = ProtoJson.fromJson(j, RunAggregationQueryRequest.newBuilder()).setParent(target);
                    return ok(List.of(svc.runAggregation(b.build())));
                }
                case "partitionQuery": {
                    var b = ProtoJson.fromJson(j, PartitionQueryRequest.newBuilder()).setParent(target);
                    return ok(svc.partitionQuery(b.build()));
                }
                default: {
                    var b = ProtoJson.fromJson(j, ListCollectionIdsRequest.newBuilder()).setParent(target);
                    return ok(svc.listCollectionIds(b.build()));
                }
            }
        }
        // resource paths: projects/p/databases/d/documents[/...]
        Matcher m = Pattern.compile("^(projects/[^/]+/databases/[^/]+/documents)(?:/(.+))?$").matcher(p);
        if (!m.matches()) {
            return error(404, "NOT_FOUND", "Method not found.");
        }
        String root = m.group(1);
        String rel = m.group(2) == null ? "" : m.group(2);
        int depth = FsNames.depth(rel);
        boolean isDoc = depth > 0 && (depth & 1) == 0;
        switch (r.method()) {
            case "GET": {
                if (isDoc) {
                    op[0] = "GetDocument";
                    GetDocumentRequest.Builder b = GetDocumentRequest.newBuilder().setName(root + "/" + rel);
                    mask(r, "mask.fieldPaths").ifPresent(b::setMask);
                    consistency(r, b::setTransaction, b::setReadTime);
                    return ok(svc.getDocument(b.build()));
                }
                op[0] = "ListDocuments";
                String parent = root;
                String coll = rel;
                int i = rel.lastIndexOf('/');
                if (i >= 0) {
                    parent = root + "/" + rel.substring(0, i);
                    coll = rel.substring(i + 1);
                }
                ListDocumentsRequest.Builder b = ListDocumentsRequest.newBuilder().setParent(parent).setCollectionId(coll);
                if (r.param("pageSize") != null) {
                    b.setPageSize(Integer.parseInt(r.param("pageSize")));
                }
                if (r.param("pageToken") != null) {
                    b.setPageToken(r.param("pageToken"));
                }
                if (r.param("orderBy") != null) {
                    b.setOrderBy(r.param("orderBy"));
                }
                if (r.param("showMissing") != null) {
                    b.setShowMissing(Boolean.parseBoolean(r.param("showMissing")));
                }
                mask(r, "mask.fieldPaths").ifPresent(b::setMask);
                consistency(r, b::setTransaction, b::setReadTime);
                return ok(svc.listDocuments(b.build()));
            }
            case "POST": {
                op[0] = "CreateDocument";
                write[0] = true;
                if (depth == 0) {
                    return error(404, "NOT_FOUND", "Method not found.");
                }
                String parent = root;
                String coll = rel;
                int i = rel.lastIndexOf('/');
                if (i >= 0) {
                    parent = root + "/" + rel.substring(0, i);
                    coll = rel.substring(i + 1);
                }
                CreateDocumentRequest.Builder b = CreateDocumentRequest.newBuilder().setParent(parent).setCollectionId(coll);
                if (r.param("documentId") != null) {
                    b.setDocumentId(r.param("documentId"));
                }
                mask(r, "mask.fieldPaths").ifPresent(b::setMask);
                b.setDocument(ProtoJson.fromJson(body(r), Document.newBuilder()));
                return ok(svc.createDocument(b.build()));
            }
            case "PATCH": {
                op[0] = "UpdateDocument";
                write[0] = true;
                if (!isDoc) {
                    return error(404, "NOT_FOUND", "Method not found.");
                }
                UpdateDocumentRequest.Builder b = UpdateDocumentRequest.newBuilder();
                b.setDocument(ProtoJson.fromJson(body(r), Document.newBuilder()).setName(root + "/" + rel));
                mask(r, "updateMask.fieldPaths").ifPresent(b::setUpdateMask);
                mask(r, "mask.fieldPaths").ifPresent(b::setMask);
                precondition(r).ifPresent(b::setCurrentDocument);
                return ok(svc.updateDocument(b.build()));
            }
            case "DELETE": {
                op[0] = "DeleteDocument";
                write[0] = true;
                if (!isDoc) {
                    return error(404, "NOT_FOUND", "Method not found.");
                }
                DeleteDocumentRequest.Builder b = DeleteDocumentRequest.newBuilder().setName(root + "/" + rel);
                precondition(r).ifPresent(b::setCurrentDocument);
                svc.deleteDocument(b.build());
                return GrpcRestMux.Resp.json(200, "{}");
            }
            default:
                return error(405, "UNIMPLEMENTED", "Method not allowed.");
        }
    }

    private static String capital(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static String dbOf(String target) {
        int i = target.indexOf("/documents");
        return i < 0 ? target : target.substring(0, i);
    }

    private static java.util.Optional<DocumentMask> mask(GrpcRestMux.Req r, String key) {
        List<String> v = r.query().get(key);
        if (v == null) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(DocumentMask.newBuilder().addAllFieldPaths(v).build());
    }

    private static java.util.Optional<Precondition> precondition(GrpcRestMux.Req r) {
        if (r.param("currentDocument.exists") != null) {
            return java.util.Optional.of(Precondition.newBuilder().setExists(Boolean.parseBoolean(r.param("currentDocument.exists"))).build());
        }
        if (r.param("currentDocument.updateTime") != null) {
            var ts = ProtoJson.fromJson(new com.google.gson.JsonPrimitive(r.param("currentDocument.updateTime")), com.google.protobuf.Timestamp.newBuilder());
            return java.util.Optional.of(Precondition.newBuilder().setUpdateTime(ts).build());
        }
        return java.util.Optional.empty();
    }

    private static void consistency(GrpcRestMux.Req r, java.util.function.Consumer<ByteString> tx, java.util.function.Consumer<com.google.protobuf.Timestamp> rt) {
        if (r.param("transaction") != null) {
            tx.accept(ByteString.copyFrom(Base64.getDecoder().decode(r.param("transaction").replace('-', '+').replace('_', '/'))));
        }
        if (r.param("readTime") != null) {
            rt.accept(ProtoJson.fromJson(new com.google.gson.JsonPrimitive(r.param("readTime")), com.google.protobuf.Timestamp.newBuilder()).build());
        }
    }
}
