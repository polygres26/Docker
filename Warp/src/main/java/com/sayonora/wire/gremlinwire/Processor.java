package com.sayonora.wire.gremlinwire;

import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.gremlinwire.Serializers.Req;
import com.sayonora.wire.gremlinwire.Serializers.Resp;
import com.sayonora.wire.gremlinwire.Serializers.Ser;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The Gremlin Server op processors: {@code eval} (standard and session), {@code bytecode} (traversal), {@code close} and SASL
 * {@code authentication}, with Gremlin Server's status codes and result chunking. Transport-independent: a request goes in, response
 * messages are handed to a sender as they are produced.
 */
final class Processor {

    /** Per-connection state: authentication and the request that triggered the challenge. */
    static final class ConnState {
        boolean authenticated;
        Req pending;
        Ser pendingSer;
        String remote = "/127.0.0.1:0";
    }

    private static final class SessionState {
        final Script.Session session;
        final Script.Env env;
        volatile long lastUse = System.nanoTime();

        SessionState(Script.Session s, Script.Env e) {
            this.session = s;
            this.env = e;
        }
    }

    final GraphStore store;
    private final SqlMetricsCollector metrics;
    private final Supplier<String> backendName;
    private final boolean authRequired;
    private final CredentialStore credentials;
    private final int defaultBatch;
    private final long evalTimeoutMs;
    private final long sessionTimeoutMs;
    private final boolean readOnly;
    private final Map<String, SessionState> sessions = new ConcurrentHashMap<>();

    Processor(GraphStore store, SqlMetricsCollector metrics, Supplier<String> backendName, boolean authRequired, CredentialStore credentials) {
        this.store = store;
        this.metrics = metrics;
        this.backendName = backendName;
        this.authRequired = authRequired;
        this.credentials = credentials;
        this.defaultBatch = (int) envLong("WARP_GREMLINWIRE_BATCH_SIZE", 64);
        this.evalTimeoutMs = envLong("WARP_GREMLINWIRE_EVAL_TIMEOUT_MS", 30000);
        this.sessionTimeoutMs = envLong("WARP_GREMLINWIRE_SESSION_TIMEOUT_MS", 28_800_000L);
        this.readOnly = "true".equalsIgnoreCase(System.getenv("WARP_GREMLINWIRE_READ_ONLY"));
    }

    static long envLong(String n, long d) {
        String v = System.getenv(n);
        try {
            return v == null || v.isBlank() ? d : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return d;
        }
    }

    boolean authRequired() {
        return authRequired;
    }

    boolean checkPassword(String user, byte[] password) {
        byte[] expected = credentials.lookupPassword(user);
        return expected != null && MessageDigest.isEqual(expected, password);
    }

    // ------------------------------------------------------------------ entry

    void handle(Req req, Ser ser, ConnState cs, Consumer<byte[]> send) {
        long t0 = System.nanoTime();
        boolean write = false;
        String opName = String.valueOf(req.op);
        try {
            if (authRequired && !cs.authenticated) {
                if ("authentication".equals(req.op)) {
                    authenticate(req, ser, cs, send);
                } else {
                    cs.pending = req;
                    cs.pendingSer = ser;
                    respond(send, ser, req, 407, "", new LinkedHashMap<>(), null, false);
                }
                return;
            }
            write = dispatch(req, ser, cs, send);
        } catch (G.GremlinError e) {
            error(send, ser, req, e.code, e.getMessage(), e);
        } catch (Throwable t) {
            error(send, ser, req, t instanceof StackOverflowError ? 597 : 597, String.valueOf(t.getMessage() == null ? t.toString() : t.getMessage()), t);
        } finally {
            record(opName, write, System.nanoTime() - t0);
        }
    }

    private void record(String op, boolean write, long nanos) {
        if (metrics == null) {
            return;
        }
        metrics.recordOperation("gremlinwire", backendName.get(), write ? SqlMetricsCollector.StatementKind.WRITE : SqlMetricsCollector.StatementKind.READ, op, nanos,
                nanos);
    }

    private void authenticate(Req req, Ser ser, ConnState cs, Consumer<byte[]> send) {
        Object sasl = req.args.get("sasl");
        if (!(sasl instanceof String s)) {
            respond(send, ser, req, 407, "", new LinkedHashMap<>(), null, false);
            return;
        }
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException e) {
            respond(send, ser, req, 401, "Username and/or password are incorrect", new LinkedHashMap<>(), null, false);
            return;
        }
        // SASL PLAIN: [authzid] NUL authcid NUL passwd
        int a = -1;
        int b = -1;
        for (int i = 0; i < raw.length; i++) {
            if (raw[i] == 0) {
                if (a < 0) {
                    a = i;
                } else if (b < 0) {
                    b = i;
                }
            }
        }
        if (a < 0 || b < 0) {
            respond(send, ser, req, 401, "Username and/or password are incorrect", new LinkedHashMap<>(), null, false);
            return;
        }
        String user = new String(raw, a + 1, b - a - 1, java.nio.charset.StandardCharsets.UTF_8);
        byte[] pw = java.util.Arrays.copyOfRange(raw, b + 1, raw.length);
        if (!checkPassword(user, pw)) {
            respond(send, ser, req, 401, "Username and/or password are incorrect", new LinkedHashMap<>(), null, false);
            return;
        }
        cs.authenticated = true;
        Req pending = cs.pending;
        Ser pser = cs.pendingSer;
        cs.pending = null;
        if (pending != null) {
            handle(pending, pser, cs, send);
        } else {
            respond(send, ser, req, 200, "", null, null, false);
        }
    }

    /** @return true when the request wrote (for the metric kind) */
    private boolean dispatch(Req req, Ser ser, ConnState cs, Consumer<byte[]> send) {
        if (req.processor == null) {
            throw new G.GremlinError(499, "Invalid OpProcessor requested [null]");
        }
        String proc = req.processor;
        String op = req.op;
        switch (proc) {
            case "", "standard":
                if ("eval".equals(op)) {
                    return eval(req, ser, cs, send, false);
                }
                if ("authentication".equals(op)) {
                    respond(send, ser, req, 200, "", null, null, false);
                    return false;
                }
                throw new G.GremlinError(498, "Message with op code [" + op + "] is not recognized.");
            case "traversal":
                if ("bytecode".equals(op)) {
                    return bytecode(req, ser, cs, send);
                }
                if ("authentication".equals(op)) {
                    respond(send, ser, req, 200, "", null, null, false);
                    return false;
                }
                throw new G.GremlinError(498, "Message with op code [" + op + "] is not recognized.");
            case "session":
                if ("eval".equals(op)) {
                    return eval(req, ser, cs, send, true);
                }
                if ("close".equals(op)) {
                    Object sid = req.args.get("session");
                    if (sid == null) {
                        throw new G.GremlinError(499, "A message with an [close] op code requires a [session] argument");
                    }
                    sessions.remove(String.valueOf(sid));
                    respond(send, ser, req, 204, "", null, null, false);
                    return false;
                }
                throw new G.GremlinError(498, "Message with op code [" + op + "] is not recognized.");
            default:
                throw new G.GremlinError(499, "Invalid OpProcessor requested [" + proc + "]");
        }
    }

    // ------------------------------------------------------------------ eval

    private static Map<String, Object> checkAliases(Object aliases) {
        Map<String, Object> binds = new LinkedHashMap<>();
        if (aliases instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                String target = String.valueOf(e.getValue());
                if (!target.equals("g") && !target.equals("graph")) {
                    throw new G.GremlinError(499, "Could not alias [" + e.getKey() + "] to [" + target + "] as [" + target
                            + "] not in the Graph or TraversalSource global bindings");
                }
                binds.put(String.valueOf(e.getKey()), target);
            }
        }
        return binds;
    }

    @SuppressWarnings("unchecked")
    private boolean eval(Req req, Ser ser, ConnState cs, Consumer<byte[]> send, boolean session) {
        Object g = req.args.get("gremlin");
        if (!(g instanceof String script)) {
            throw new G.GremlinError(499, "A message with an [eval] op code requires a [gremlin] argument.");
        }
        String sid = null;
        if (session) {
            Object s = req.args.get("session");
            if (s == null) {
                throw new G.GremlinError(499, "A message with an [eval] op code requires a [session] argument");
            }
            sid = String.valueOf(s);
        }
        Object lang = req.args.get("language");
        if (lang != null && !"gremlin-groovy".equals(lang) && !"gremlin-lang".equals(lang)) {
            throw new G.GremlinError(597, lang + " is not an available GremlinScriptEngine");
        }
        Map<String, Object> aliasTargets = checkAliases(req.args.get("aliases"));
        Map<String, Object> bindings = new LinkedHashMap<>();
        Object b = req.args.get("bindings");
        if (b instanceof Map<?, ?> bm) {
            for (Map.Entry<?, ?> e : bm.entrySet()) {
                bindings.put(String.valueOf(e.getKey()), e.getValue());
            }
        }
        long timeoutMs = req.args.get("evaluationTimeout") instanceof Number n ? n.longValue() : evalTimeoutMs;
        if (req.args.get("scriptEvaluationTimeout") instanceof Number n) {
            timeoutMs = n.longValue();
        }
        long deadline = timeoutMs > 0 ? System.nanoTime() + timeoutMs * 1_000_000L : 0;
        Script.Session ss;
        Script.Env env;
        if (session) {
            evictSessions();
            SessionState st = sessions.computeIfAbsent(sid, k -> {
                Script.Session x = new Script.Session(store, readOnly, 0);
                return new SessionState(x, Script.newRoot(x, null));
            });
            st.lastUse = System.nanoTime();
            ss = st.session;
            env = st.env;
            ss.deadlineNanos = deadline;
            env.vars.putAll(bindings);
        } else {
            ss = new Script.Session(store, readOnly, deadline);
            env = Script.newRoot(ss, bindings);
        }
        for (Map.Entry<String, Object> a : aliasTargets.entrySet()) {
            env.vars.put(a.getKey(), env.vars.get(String.valueOf(a.getValue())));
        }
        boolean write = looksLikeWrite(script);
        Object result = Script.eval(script, env);
        Iterator<Object> items = resultItems(result, ss, env);
        stream(req, ser, cs, send, items, batchOf(req));
        return write;
    }

    private void evictSessions() {
        if (sessions.size() < 64) {
            return;
        }
        long now = System.nanoTime();
        sessions.values().removeIf(s -> (now - s.lastUse) / 1_000_000L > sessionTimeoutMs);
    }

    static boolean looksLikeWrite(String script) {
        return script.contains("addV") || script.contains("addE") || script.contains(".drop(") || script.contains(".property(") || script.contains("mergeV")
                || script.contains("mergeE");
    }

    private int batchOf(Req req) {
        Object b = req.args.get("batchSize");
        int n = b instanceof Number x ? x.intValue() : defaultBatch;
        return Math.max(1, n);
    }

    /** What the server iterates for a script's final value. */
    static Iterator<Object> resultItems(Object result, Script.Session ss, Script.Env env) {
        if (result instanceof G.Bytecode bc) {
            if (bc.consumed) {
                return Collections.emptyIterator();
            }
            if (bc.steps.isEmpty()) {
                if (bc.bound != null) {
                    throw new G.GremlinError(599, "Error during serialization: Could not find a type identifier for the class : class TraversalSource");
                }
                return Collections.emptyIterator();
            }
            Engine.Ctx ctx = ss.ctx(env);
            return Engine.execute(bc, ctx);
        }
        if (result == null) {
            return Collections.singletonList((Object) null).iterator();
        }
        if (result instanceof Script.GraphHandle) {
            List<G.Vertex> vs = new ArrayList<>();
            Iterator<G.Vertex> vi = ss.store.allVertices(GraphStore.VFilter.NONE);
            while (vi.hasNext() && vs.size() < 100_000) {
                vs.add(vi.next());
            }
            List<G.Edge> es = new ArrayList<>();
            Iterator<G.Edge> ei = ss.store.allEdges();
            while (ei.hasNext() && es.size() < 100_000) {
                es.add(ei.next());
            }
            return Collections.singletonList((Object) new G.GraphValue(vs, es)).iterator();
        }
        if (result instanceof Collection<?> || result instanceof Map<?, ?> || result instanceof Object[] || result instanceof Iterator<?>) {
            return Steps.iter(result);
        }
        return Collections.singletonList(result).iterator();
    }

    // ------------------------------------------------------------------ bytecode

    private boolean bytecode(Req req, Ser ser, ConnState cs, Consumer<byte[]> send) {
        Object g = req.args.get("gremlin");
        if (!(g instanceof G.Bytecode bc)) {
            throw new G.GremlinError(499, "A message with [bytecode] op code requires a [gremlin] argument.");
        }
        if (!(req.args.get("aliases") instanceof Map<?, ?>)) {
            throw new G.GremlinError(499, "A message with [bytecode] op code requires a [aliases] argument.");
        }
        checkAliases(req.args.get("aliases"));
        long timeoutMs = req.args.get("evaluationTimeout") instanceof Number n ? n.longValue() : evalTimeoutMs;
        long deadline = timeoutMs > 0 ? System.nanoTime() + timeoutMs * 1_000_000L : 0;
        Script.Session ss = new Script.Session(store, readOnly, deadline);
        Script.Env env = Script.newRoot(ss, null);
        boolean write = false;
        for (Object[] s : bc.steps) {
            String n = (String) s[0];
            if (n.equals("addV") || n.equals("addE") || n.equals("drop") || n.equals("property") || n.equals("mergeV") || n.equals("mergeE")) {
                write = true;
            }
        }
        Iterator<Object> items;
        try {
            // like Gremlin Server, remote traversals answer with traversers (value + bulk), which drivers expand
            Iterator<Object> raw = Engine.execute(bc, ss.ctx(env));
            items = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return raw.hasNext();
                }

                @Override
                public Object next() {
                    return new G.WireTraverser(raw.next(), 1L);
                }
            };
        } catch (G.GremlinError e) {
            throw e.code == 597 ? new G.GremlinError(e.translation ? 599 : 500, e.getMessage()) : e;
        }
        stream(req, ser, cs, send, items, batchOf(req));
        return write;
    }

    // ------------------------------------------------------------------ streaming

    private void stream(Req req, Ser ser, ConnState cs, Consumer<byte[]> send, Iterator<Object> items, int batch) {
        boolean bytecode = "bytecode".equals(req.op);
        boolean any = false;
        while (true) {
            List<Object> buf = new ArrayList<>();
            try {
                while (buf.size() < batch && items.hasNext()) {
                    buf.add(items.next());
                }
                boolean more = items.hasNext();
                Resp r = new Resp();
                r.requestId = req.requestId;
                if (more) {
                    r.code = 206;
                    r.hasData = true;
                    r.data = buf;
                } else if (buf.isEmpty() && !any) {
                    r.code = 204;
                    r.attributes.put("host", cs.remote);
                } else {
                    r.code = 200;
                    r.hasData = true;
                    r.data = buf;
                    r.attributes.put("host", cs.remote);
                }
                any = true;
                byte[] out;
                try {
                    out = ser.encode(r);
                } catch (G.GremlinError e) {
                    throw e;
                } catch (RuntimeException e) {
                    throw new G.GremlinError(599, "Error during serialization: " + e.getMessage());
                }
                send.accept(out);
                if (!more) {
                    return;
                }
            } catch (G.GremlinError e) {
                // a bytecode request that fails while its traversal runs answers 500, unlike a script (597)
                if (bytecode && e.code == 597) {
                    throw new G.GremlinError(e.translation ? 599 : 500, e.getMessage());
                }
                throw e;
            }
        }
    }

    // ------------------------------------------------------------------ responses

    private static void respond(Consumer<byte[]> send, Ser ser, Req req, int code, String message, Map<String, Object> attrs, Object data, boolean hasData) {
        Resp r = new Resp();
        r.requestId = req.requestId;
        r.code = code;
        r.message = message;
        if (attrs != null) {
            r.attributes.putAll(attrs);
        }
        r.data = data;
        r.hasData = hasData;
        send.accept(ser.encode(r));
    }

    private void error(Consumer<byte[]> send, Ser ser, Req req, int code, String message, Throwable t) {
        Resp r = new Resp();
        r.requestId = req.requestId;
        r.code = code;
        r.message = message == null ? String.valueOf(t) : message;
        if (code >= 500) {
            r.attributes.put("stackTrace", t.toString());
            List<Object> ex = new ArrayList<>();
            ex.add(t.getClass().getName());
            r.attributes.put("exceptions", ex);
        }
        send.accept(ser.encode(r));
    }

    void closeSession(String id) {
        sessions.remove(id);
    }
}
