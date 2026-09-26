package com.sayonora.warp.pubsubwire;

import com.google.api.AnnotationsProto;
import com.google.api.HttpRule;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.ErrorInfo;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The REST/JSON API of pubsub.googleapis.com/v1. The routes are read from the {@code google.api.http} annotations of the
 * vendored protos (so every RPC with an HTTP binding is served, including the {@code :publish}-style custom verbs); JSON is
 * proto3 JSON. Errors use Google's {@code {"error":{code,message,status}}} format with the HTTP status of the gRPC code.
 * StreamingPull has no REST binding.
 */
final class PsRest {

    private static final Logger log = LoggerFactory.getLogger(PsRest.class);

    private record Route(String httpMethod, Pattern pattern, List<String> vars, String body, PsRpc.Rpc rpc) {
    }

    private final PsRpc rpc;
    private final PsConfig cfg;
    private final PsGrpc.Recorder recorder;
    private final List<Route> routes = new ArrayList<>();
    private final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
    private final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace()
            .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder().add(ErrorInfo.getDescriptor()).build());
    private final com.sayonora.warp.acl.ConnectionGate gate;
    private Server server;

    PsRest(PsRpc rpc, PsConfig cfg, PsGrpc.Recorder recorder, com.sayonora.warp.acl.ConnectionGate gate) {
        this.gate = gate;
        this.rpc = rpc;
        this.cfg = cfg;
        this.recorder = recorder;
        addServiceRoutes("google.pubsub.v1.Publisher", com.google.pubsub.v1.PubsubProto.getDescriptor());
        addServiceRoutes("google.pubsub.v1.Subscriber", com.google.pubsub.v1.PubsubProto.getDescriptor());
        addServiceRoutes("google.pubsub.v1.SchemaService", com.google.pubsub.v1.SchemaProto.getDescriptor());
        String res = "projects/[^/]+/(?:topics|subscriptions|snapshots|schemas)/[^/:]+";
        addRoute("POST", "/v1/{resource=" + res + "}:setIamPolicy", "*", "google.iam.v1.IAMPolicy/SetIamPolicy", true);
        addRoute("GET", "/v1/{resource=" + res + "}:getIamPolicy", null, "google.iam.v1.IAMPolicy/GetIamPolicy", true);
        addRoute("POST", "/v1/{resource=" + res + "}:testIamPermissions", "*", "google.iam.v1.IAMPolicy/TestIamPermissions", true);
    }

    private void addServiceRoutes(String service, Descriptors.FileDescriptor fd) {
        String shortName = service.substring(service.lastIndexOf('.') + 1);
        Descriptors.ServiceDescriptor sd = fd.findServiceByName(shortName);
        for (Descriptors.MethodDescriptor md : sd.getMethods()) {
            HttpRule rule = md.getOptions().getExtension(AnnotationsProto.http);
            if (rule == null || rule.getPatternCase() == HttpRule.PatternCase.PATTERN_NOT_SET) {
                continue;
            }
            addRule(rule, service + "/" + md.getName());
            for (HttpRule extra : rule.getAdditionalBindingsList()) {
                addRule(extra, service + "/" + md.getName());
            }
        }
    }

    private void addRule(HttpRule r, String key) {
        switch (r.getPatternCase()) {
            case GET -> addRoute("GET", r.getGet(), r.getBody(), key, false);
            case PUT -> addRoute("PUT", r.getPut(), r.getBody(), key, false);
            case POST -> addRoute("POST", r.getPost(), r.getBody(), key, false);
            case DELETE -> addRoute("DELETE", r.getDelete(), r.getBody(), key, false);
            case PATCH -> addRoute("PATCH", r.getPatch(), r.getBody(), key, false);
            default -> {
            }
        }
    }

    /** Turns a template like {@code /v1/{topic=projects/*}/topics:x} into a regex with one group per variable. */
    private void addRoute(String method, String template, String body, String rpcKey, boolean rawRegex) {
        PsRpc.Rpc r = rpc.byMethod.get(rpcKey);
        if (r == null) {
            return;
        }
        StringBuilder re = new StringBuilder();
        List<String> vars = new ArrayList<>();
        int i = 0;
        while (i < template.length()) {
            char c = template.charAt(i);
            if (c == '{') {
                int j = template.indexOf('}', i);
                String v = template.substring(i + 1, j);
                int eq = v.indexOf('=');
                String name = eq < 0 ? v : v.substring(0, eq);
                String tpl = eq < 0 ? "*" : v.substring(eq + 1);
                vars.add(name);
                String rx;
                if (rawRegex) {
                    rx = tpl;
                } else {
                    rx = tpl.replace("**", "\u0001").replace("*", "[^/:]+").replace("\u0001", ".+");
                }
                re.append('(').append(rx).append(')');
                i = j + 1;
            } else {
                re.append(Pattern.quote(String.valueOf(c)));
                i++;
            }
        }
        routes.add(new Route(method, Pattern.compile(re.toString()), vars, body == null ? "" : body, r));
    }

    void start(int port) throws Exception {
        server = new Server(new QueuedThreadPool((int) PsConfig.longEnv("WARP_PUBSUBWIRE_REST_THREADS", 300)));
        ServerConnector c = new ServerConnector(server);
        c.setPort(port);
        server.addConnector(c);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest req, HttpServletResponse resp) throws IOException {
                base.setHandled(true);
                serve(req, resp);
            }
        });
        server.start();
    }

    void stop() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    private void serve(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        long t0 = System.nanoTime();
        String opName = null;
        boolean write = false;
        try {
            if (gate != null && !gate.acceptHttp(req)) {
                throw new PsException(io.grpc.Status.Code.PERMISSION_DENIED, "The caller does not have permission");
            }
            if (!cfg.tokens.isEmpty()) {
                String h = req.getHeader("Authorization");
                String tok = h != null && h.regionMatches(true, 0, "Bearer ", 0, 7) ? h.substring(7).trim() : req.getParameter("access_token");
                if (tok == null || !cfg.tokens.contains(tok)) {
                    throw new PsException(io.grpc.Status.Code.UNAUTHENTICATED,
                            "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other "
                                    + "valid authentication credential.");
                }
            }
            String path = req.getRequestURI();
            String method = req.getMethod();
            Route hit = null;
            Matcher hm = null;
            boolean pathMatched = false;
            for (Route r : routes) {
                Matcher m = r.pattern.matcher(path);
                if (m.matches()) {
                    pathMatched = true;
                    if (r.httpMethod.equals(method)) {
                        hit = r;
                        hm = m;
                        break;
                    }
                }
            }
            if (hit == null) {
                if (pathMatched) {
                    throw new PsException(io.grpc.Status.Code.UNIMPLEMENTED, "Method not allowed: " + method + " " + path);
                }
                throw new PsException(io.grpc.Status.Code.NOT_FOUND, "Requested entity was not found.");
            }
            opName = hit.rpc.name();
            write = hit.rpc.write();
            Message.Builder b = hit.rpc.prototype().newBuilderForType();
            JsonObject json = new JsonObject();
            String bodyText = new String(req.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!hit.body.isEmpty() && !bodyText.isBlank()) {
                JsonElement be;
                try {
                    be = JsonParser.parseString(bodyText);
                } catch (RuntimeException e) {
                    throw PsException.invalid("Invalid JSON payload received. " + e.getMessage());
                }
                if (hit.body.equals("*")) {
                    if (!be.isJsonObject()) {
                        throw PsException.invalid("Invalid JSON payload received. Expected an object.");
                    }
                    json = be.getAsJsonObject();
                } else {
                    json.add(hit.body, be);
                }
            }
            for (int i = 0; i < hit.vars.size(); i++) {
                setPath(json, hit.vars.get(i), hm.group(i + 1));
            }
            for (Map.Entry<String, String[]> q : req.getParameterMap().entrySet()) {
                if (q.getKey().equals("access_token") || q.getKey().equals("key") || q.getKey().equals("alt") || q.getKey().equals("$alt")
                        || q.getKey().equals("prettyPrint") || q.getKey().equals("fields")) {
                    continue;
                }
                setQuery(hit.rpc.prototype().getDescriptorForType(), json, q.getKey(), q.getValue());
            }
            try {
                parser.merge(json.toString(), b);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw PsException.invalid("Invalid JSON payload received. " + e.getMessage().replace("\n", " "));
            }
            Message out = hit.rpc.impl().apply(b.build());
            write(resp, 200, printer.print(out));
        } catch (PsException e) {
            error(resp, e);
        } catch (RuntimeException e) {
            log.error("pubsubwire REST {} {} failed", req.getMethod(), req.getRequestURI(), e);
            error(resp, PsException.internal("Internal error encountered."));
        } finally {
            if (opName != null) {
                recorder.record(opName, write, System.nanoTime() - t0);
            }
        }
    }

    private static void setPath(JsonObject root, String dotted, String value) {
        String[] p = dotted.split("\\.");
        JsonObject o = root;
        for (int i = 0; i < p.length - 1; i++) {
            JsonElement e = o.get(p[i]);
            if (e == null || !e.isJsonObject()) {
                JsonObject n = new JsonObject();
                o.add(p[i], n);
                o = n;
            } else {
                o = e.getAsJsonObject();
            }
        }
        o.addProperty(p[p.length - 1], value);
    }

    private static void setQuery(Descriptors.Descriptor d, JsonObject json, String name, String[] values) {
        String[] p = name.split("\\.");
        Descriptors.FieldDescriptor fd = null;
        Descriptors.Descriptor cur = d;
        for (int i = 0; i < p.length; i++) {
            fd = null;
            for (Descriptors.FieldDescriptor f : cur.getFields()) {
                if (f.getName().equals(p[i]) || f.getJsonName().equals(p[i])) {
                    fd = f;
                    break;
                }
            }
            if (fd == null) {
                return;
            }
            if (i < p.length - 1) {
                if (fd.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    return;
                }
                cur = fd.getMessageType();
            }
        }
        JsonObject o = json;
        for (int i = 0; i < p.length - 1; i++) {
            JsonElement e = o.get(p[i]);
            if (e == null || !e.isJsonObject()) {
                JsonObject n = new JsonObject();
                o.add(p[i], n);
                o = n;
            } else {
                o = e.getAsJsonObject();
            }
        }
        String key = p[p.length - 1];
        if (fd.isRepeated()) {
            JsonArray a = new JsonArray();
            for (String v : values) {
                a.add(scalar(fd, v));
            }
            o.add(key, a);
        } else {
            o.add(key, scalar(fd, values[values.length - 1]));
        }
    }

    private static JsonElement scalar(Descriptors.FieldDescriptor fd, String v) {
        try {
            return switch (fd.getJavaType()) {
                case BOOLEAN -> new com.google.gson.JsonPrimitive(Boolean.parseBoolean(v));
                case INT -> new com.google.gson.JsonPrimitive(Integer.parseInt(v));
                case LONG -> new com.google.gson.JsonPrimitive(v);
                default -> new com.google.gson.JsonPrimitive(v);
            };
        } catch (NumberFormatException e) {
            throw PsException.invalid("Invalid value for query parameter " + fd.getName() + ": " + v);
        }
    }

    private void error(HttpServletResponse resp, PsException e) throws IOException {
        JsonObject err = new JsonObject();
        err.addProperty("code", e.httpStatus());
        err.addProperty("message", e.getMessage());
        err.addProperty("status", e.code.name());
        if (e.info != null) {
            try {
                JsonArray details = new JsonArray();
                details.add(JsonParser.parseString(printer.print(com.google.protobuf.Any.pack(e.info))));
                err.add("details", details);
            } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
                // details are optional
            }
        }
        JsonObject top = new JsonObject();
        top.add("error", err);
        write(resp, e.httpStatus(), top.toString());
    }

    private static void write(HttpServletResponse resp, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        resp.setStatus(status);
        resp.setContentType("application/json; charset=UTF-8");
        resp.setContentLength(b.length);
        resp.getOutputStream().write(b);
    }
}
