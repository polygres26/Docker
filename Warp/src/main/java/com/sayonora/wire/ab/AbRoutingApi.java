package com.sayonora.wire.ab;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sayonora.wire.secrets.FieldCipher;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Admin API of the A/B routing feature (see {@link AbRouting}). Cloud secrets are accepted on write and NEVER returned:
 * every response goes through {@link AbTarget#publicJson}.
 * <pre>
 *   GET    /api/ab-routing                         policies, targets (secrets redacted), kill switch, version
 *   PUT    /api/ab-routing/policies/{store}        one policy (s3 | dynamodb | sqs)
 *   DELETE /api/ab-routing/policies/{store}
 *   PUT    /api/ab-routing/targets/{name}          a cloud target with its auth provider (omitted secrets are kept)
 *   DELETE /api/ab-routing/targets/{name}          refused while a policy uses it
 *   POST   /api/ab-routing/targets/{name}/test     resolve credentials once (STS etc.); returns ok/expiry, never the credentials
 *   POST   /api/ab-routing/kill-switch             {side: "local"|"cloud", store?, reason?}
 *   DELETE /api/ab-routing/kill-switch[?store=]    lift it
 *   GET    /api/ab-routing/compare[?store=&onlyDiff=true&limit=N]   the compare ring buffer (this node)
 *   DELETE /api/ab-routing/compare                 clear it
 *   GET    /api/ab-routing/stats                   per-side counters, error rates, latency histograms (this node)
 * </pre>
 */
public final class AbRoutingApi {

    private static final Pattern POLICY = Pattern.compile("^/api/ab-routing/policies/([^/]+)/?$");
    private static final Pattern TARGET = Pattern.compile("^/api/ab-routing/targets/([^/]+)/?$");
    private static final Pattern TARGET_TEST = Pattern.compile("^/api/ab-routing/targets/([^/]+)/test/?$");
    private static final Set<String> STORES = Set.of("s3", "dynamodb", "sqs");

    private AbRoutingApi() {
    }

    public static boolean handles(String target) {
        return target.equals("/api/ab-routing") || target.startsWith("/api/ab-routing/");
    }

    public static void handle(String target, HttpServletRequest request, HttpServletResponse response, String user) throws IOException {
        response.setContentType("application/json; charset=utf-8");
        String method = request.getMethod();
        AbRouting rt = AbRouting.get();
        try {
            if (rt == null) {
                error(response, 503, "A/B routing is not initialised on this node");
                return;
            }
            Matcher m;
            if (target.equals("/api/ab-routing") && "GET".equals(method)) {
                JsonObject o = rt.state().toPublicJson();
                o.addProperty("persistent", rt.persistent());
                o.addProperty("secretsEncryptedAtRest", encryptionAvailable());
                write(response, 200, o);
            } else if ((m = POLICY.matcher(target)).matches()) {
                String store = m.group(1);
                switch (method) {
                    case "PUT", "POST" -> putPolicy(rt, store, body(request), response);
                    case "DELETE" -> {
                        rt.mutate(doc -> {
                            obj(doc, "policies").remove(store);
                            return doc;
                        });
                        write(response, 200, rt.state().toPublicJson());
                    }
                    default -> error(response, 405, "method not allowed");
                }
            } else if ((m = TARGET_TEST.matcher(target)).matches() && "POST".equals(method)) {
                testTarget(rt, m.group(1), response);
            } else if ((m = TARGET.matcher(target)).matches()) {
                String name = m.group(1);
                switch (method) {
                    case "PUT", "POST" -> putTarget(rt, name, body(request), response);
                    case "GET" -> {
                        AbTarget t = rt.state().targets.get(name);
                        if (t == null) {
                            error(response, 404, "no such cloud target");
                        } else {
                            write(response, 200, t.publicJson());
                        }
                    }
                    case "DELETE" -> {
                        rt.mutate(doc -> {
                            for (var e : obj(doc, "policies").entrySet()) {
                                JsonElement t = e.getValue().getAsJsonObject().get("target");
                                if (t != null && !t.isJsonNull() && t.getAsString().equals(name)) {
                                    throw new IllegalArgumentException("target '" + name + "' is used by the " + e.getKey() + " policy");
                                }
                            }
                            obj(doc, "targets").remove(name);
                            return doc;
                        });
                        write(response, 200, rt.state().toPublicJson());
                    }
                    default -> error(response, 405, "method not allowed");
                }
            } else if (target.equals("/api/ab-routing/kill-switch")) {
                switch (method) {
                    case "POST", "PUT" -> kill(rt, body(request), user, response);
                    case "DELETE" -> {
                        String store = request.getParameter("store");
                        rt.mutate(doc -> {
                            if (store == null) {
                                doc.remove("killSwitch");
                                doc.remove("storeKill");
                            } else {
                                obj(doc, "storeKill").remove(store);
                            }
                            return doc;
                        });
                        write(response, 200, rt.state().toPublicJson());
                    }
                    default -> error(response, 405, "method not allowed");
                }
            } else if (target.equals("/api/ab-routing/compare")) {
                if ("DELETE".equals(method)) {
                    rt.stats().clearEntries();
                    write(response, 200, new JsonObject());
                } else if ("GET".equals(method)) {
                    int limit = 100;
                    try {
                        limit = Integer.parseInt(request.getParameter("limit"));
                    } catch (NumberFormatException ignored) {
                        // default
                    }
                    JsonArray arr = new JsonArray();
                    rt.stats().entries(request.getParameter("store"), "true".equals(request.getParameter("onlyDiff")),
                            Math.max(1, Math.min(limit, 5000))).forEach(e -> arr.add(e.toJson()));
                    JsonObject o = new JsonObject();
                    o.add("entries", arr);
                    o.addProperty("scope", "this node only; the ring buffer is in memory");
                    write(response, 200, o);
                } else {
                    error(response, 405, "method not allowed");
                }
            } else if (target.equals("/api/ab-routing/stats") && "GET".equals(method)) {
                write(response, 200, rt.stats().toJson());
            } else {
                error(response, 404, "no such route");
            }
        } catch (IllegalArgumentException | JsonParseException | ClassCastException | IllegalStateException e) {
            error(response, 400, e.getMessage() == null ? "invalid request" : e.getMessage());
        } catch (SQLException e) {
            error(response, 502, "control-plane database error: " + e.getMessage());
        }
    }

    static boolean encryptionAvailable() {
        return FieldCipher.encrypt("probe").startsWith("encv1:");
    }

    private static void putPolicy(AbRouting rt, String store, JsonObject in, HttpServletResponse response) throws IOException, SQLException {
        if (!STORES.contains(store)) {
            throw new IllegalArgumentException("A/B routing supports the stores " + STORES + ", not '" + store + "'");
        }
        AbPolicy p = AbPolicy.parse(store, in);
        rt.mutate(doc -> {
            if (p.target != null && !obj(doc, "targets").has(p.target)) {
                throw new IllegalArgumentException("policy target '" + p.target + "' is not a configured cloud target");
            }
            obj(doc, "policies").add(store, p.toJson());
            return doc;
        });
        write(response, 200, rt.state().toPublicJson());
    }

    private static void putTarget(AbRouting rt, String name, JsonObject in, HttpServletResponse response) throws IOException, SQLException {
        if (!name.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("target name must be 1-64 characters of [A-Za-z0-9_.-]");
        }
        if (AbTarget.hasSecret(in) && !encryptionAvailable() && !"true".equals(System.getenv("WARP_AB_ALLOW_PLAINTEXT_SECRETS"))) {
            throw new IllegalArgumentException("refusing to store cloud secrets in plaintext: set SAYONORA_ENCRYPTION_KEY "
                    + "(base64, 32 bytes) on every Warp node, or use an auth type without stored secrets (default-chain, web-identity)");
        }
        rt.mutate(doc -> {
            JsonObject stored = obj(doc, "targets").has(name) ? AbTarget.decrypt(obj(doc, "targets").getAsJsonObject(name)) : null;
            JsonObject merged = AbTarget.mergeSecrets(in, stored);
            merged.remove("name");
            new AbTarget(name, merged); // validates auth config; throws IllegalArgumentException
            obj(doc, "targets").add(name, AbTarget.encrypt(merged));
            return doc;
        });
        write(response, 200, rt.state().targets.get(name).publicJson());
    }

    private static void testTarget(AbRouting rt, String name, HttpServletResponse response) throws IOException {
        AbTarget t = rt.state().targets.get(name);
        if (t == null) {
            error(response, 404, "no such cloud target");
            return;
        }
        JsonObject o = new JsonObject();
        o.addProperty("type", t.provider().type());
        try {
            AbAuthProvider.CloudCredential c = t.provider().resolve(null);
            o.addProperty("ok", true);
            o.addProperty("expiresAt", c.expiresAt() == null ? null : c.expiresAt().toString());
            if (c instanceof AbAuthProvider.AwsCreds a) {
                o.addProperty("accessKeyId", a.accessKeyId());
            }
        } catch (AbAuthProvider.AuthException e) {
            o.addProperty("ok", false);
            o.addProperty("error", e.getMessage());
        }
        write(response, 200, o);
    }

    private static void kill(AbRouting rt, JsonObject in, String user, HttpServletResponse response) throws IOException, SQLException {
        AbSide side = AbSide.parse(in.get("side").getAsString());
        String store = in.has("store") && !in.get("store").isJsonNull() ? in.get("store").getAsString() : null;
        if (side == AbSide.CLOUD) {
            for (String s : store == null ? rt.state().policies.keySet() : Set.of(store)) {
                if (rt.state().targetFor(s) == null) {
                    throw new IllegalArgumentException("cannot switch " + s + " to the cloud: no cloud target is configured for it");
                }
            }
            if (store == null && rt.state().policies.isEmpty() && rt.state().targets.size() != 1) {
                throw new IllegalArgumentException("cannot switch to the cloud: configure exactly one cloud target or a policy first");
            }
        }
        JsonObject k = new AbState.Kill(side, in.has("reason") && !in.get("reason").isJsonNull() ? in.get("reason").getAsString() : null,
                user, System.currentTimeMillis()).toJson();
        rt.mutate(doc -> {
            if (store == null) {
                doc.add("killSwitch", k);
            } else {
                obj(doc, "storeKill").add(store, k);
            }
            return doc;
        });
        write(response, 200, rt.state().toPublicJson());
    }

    private static JsonObject obj(JsonObject doc, String k) {
        if (!doc.has(k) || !doc.get(k).isJsonObject()) {
            doc.add(k, new JsonObject());
        }
        return doc.getAsJsonObject(k);
    }

    private static JsonObject body(HttpServletRequest r) throws IOException {
        JsonElement e = JsonParser.parseReader(r.getReader());
        if (e == null || !e.isJsonObject()) {
            throw new IllegalArgumentException("a JSON object body is required");
        }
        return e.getAsJsonObject();
    }

    private static void write(HttpServletResponse r, int status, JsonElement body) throws IOException {
        r.setStatus(status);
        r.getWriter().write(body.toString());
    }

    private static void error(HttpServletResponse r, int status, String message) throws IOException {
        JsonObject o = new JsonObject();
        o.addProperty("error", message);
        write(r, status, o);
    }
}
