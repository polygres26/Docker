package com.sayonora.wire.ab;

import com.google.gson.JsonObject;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.server.ServerOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A/B routing between the real cloud service and Warp's own Postgres-backed emulation, at the HTTP layer of the
 * AWS-family frontends. Each frontend (s3wire, dynamowire, sqswire) wraps its Jetty handler with
 * {@link #wrap}; the unified awswire endpoint dispatches to those very handlers, so it is covered too. Clients
 * keep pointing at Warp; per store a policy ({@link AbPolicy}) decides local, cloud or compare per request;
 * a kill switch overrides everything. The configuration ({@link AbState}) lives in the control-plane database and is
 * hot-reloaded on every node ({@link AbStore}). See docs/WARP_GUIDE.md "A/B routing".
 */
public final class AbRouting {

    private static final Logger log = LoggerFactory.getLogger(AbRouting.class);
    private static volatile AbRouting instance;

    /** Outcome of the frontend's own authentication of the client, run BEFORE anything is sent to the cloud. */
    public record AuthResult(boolean ok, String accessKey, int status, String code, String message, boolean responded,
            Function<InputStream, InputStream> bodyDecoder, String cloudPath) {
        public static AuthResult ok(String accessKey) {
            return new AuthResult(true, accessKey, 200, null, null, false, null, null);
        }

        public static AuthResult ok(String accessKey, Function<InputStream, InputStream> decoder, String cloudPath) {
            return new AuthResult(true, accessKey, 200, null, null, false, decoder, cloudPath);
        }

        public static AuthResult denied(int status, String code, String message) {
            return new AuthResult(false, null, status, code, message, false, null, null);
        }

        /** The authenticator already wrote the error response itself. */
        public static AuthResult respondedAlready() {
            return new AuthResult(false, null, 0, null, null, true, null, null);
        }
    }

    /** A frontend's client authentication (connection ACL, SigV4 / OAuth), applied to requests routed away from local. */
    public interface Authenticator {
        AuthResult authenticate(HttpServletRequest request, HttpServletResponse response, byte[] bodyOrNull) throws IOException;
    }

    final AbStats stats = new AbStats();
    private final SqlMetricsCollector metrics;
    private final AbStore store;
    private volatile AbState state = AbState.EMPTY;
    final ExecutorService pool = Executors.newFixedThreadPool(16, r -> {
        Thread t = new Thread(r, "warp-ab-compare");
        t.setDaemon(true);
        return t;
    });

    private AbRouting(AbStore store, SqlMetricsCollector metrics) {
        this.store = store;
        this.metrics = metrics;
    }

    public static AbRouting get() {
        return instance;
    }

    /** Called once by Main after the control plane is up. Never throws: A/B routing simply stays off when the store fails. */
    public static synchronized AbRouting init(ServerOptions options, SqlMetricsCollector metrics) {
        if (instance != null) {
            return instance;
        }
        AbStore s = new AbStore(options);
        AbRouting r = new AbRouting(s, metrics);
        try {
            s.ensureSchema();
            s.readLatest().ifPresent(r::apply);
            s.listen(r::apply);
        } catch (SQLException | RuntimeException e) {
            log.warn("ab-routing: control-plane store unavailable, A/B routing stays OFF (everything local): {}", e.getMessage());
        }
        instance = r;
        return r;
    }

    /** For unit tests / embedding: an instance with no persistence. */
    public static AbRouting forTesting(AbState st) {
        AbRouting r = new AbRouting(null, null);
        r.state = st;
        instance = r;
        return r;
    }

    public static void resetForTesting() {
        instance = null;
    }

    private void apply(AbStore.Version v) {
        try {
            AbState prev = state;
            state = AbState.fromStored(v.version(), v.doc(), prev);
            log.info("ab-routing: configuration version {} applied ({} policies, {} targets, kill switch {})", v.version(),
                    state.policies.size(), state.targets.size(), state.kill == null ? "off" : state.kill.side().id());
        } catch (RuntimeException e) {
            log.warn("ab-routing: version {} could not be applied, keeping the previous configuration: {}", v.version(), e.getMessage());
        }
    }

    public AbState state() {
        return state;
    }

    public AbStats stats() {
        return stats;
    }

    public boolean persistent() {
        return store != null;
    }

    /** Apply a mutation of the stored document cluster-wide; effective on this node immediately, on peers via NOTIFY. */
    public AbState mutate(UnaryOperator<JsonObject> mutator) throws SQLException {
        if (store == null) {
            throw new IllegalStateException("A/B routing has no control-plane store (database unavailable at startup)");
        }
        AbStore.Version v = store.mutate(mutator);
        apply(v);
        return state;
    }

    // ------------------------------------------------------------------------------------------------------------
    // request path
    // ------------------------------------------------------------------------------------------------------------

    /**
     * @param store the store id ({@code s3}, {@code dynamodb}, {@code sqs})
     * @param auth  null = the frontend performs no authentication of its own for this store
     */
    public static Handler wrap(String store, Handler inner, Authenticator auth) {
        AbOps.Kind kind = switch (store) {
            case "s3" -> AbOps.Kind.S3;
            case "dynamodb" -> AbOps.Kind.DYNAMO;
            case "sqs" -> AbOps.Kind.SQS;
            default -> throw new IllegalArgumentException("A/B routing does not support store " + store);
        };
        String protocol = switch (kind) {
            case S3 -> "s3wire";
            case DYNAMO -> "dynamowire";
            case SQS -> "sqswire";
        };
        org.eclipse.jetty.server.handler.HandlerWrapper w = new org.eclipse.jetty.server.handler.HandlerWrapper() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response)
                    throws IOException, jakarta.servlet.ServletException {
                AbRouting rt = instance;
                AbState st = rt == null ? null : rt.state;
                Handler in = getHandler();
                if (st == null || st.alwaysLocal(store)) {
                    in.handle(target, base, request, response);
                    return;
                }
                rt.route(store, kind, protocol, st, in, auth, target, base, request, response);
            }
        };
        w.setHandler(inner);
        return w;
    }

    private static final int MAX_BUFFERED_BODY = 64 * 1024 * 1024;

    private void route(String store, AbOps.Kind kind, String protocol, AbState st, Handler inner, Authenticator auth,
            String target, Request base, HttpServletRequest request, HttpServletResponse response)
            throws IOException, jakarta.servlet.ServletException {
        HttpServletRequest r = request;
        byte[] body = null;
        boolean hasBody = request.getContentLengthLong() != 0 && !List.of("GET", "HEAD", "OPTIONS").contains(request.getMethod());
        if (kind != AbOps.Kind.S3 && hasBody) {
            body = request.getInputStream().readAllBytes();
            r = new AbHttp.CachedBody(request, body);
        }
        AbOps.Op op = AbOps.classify(kind, r.getMethod(), r.getRequestURI(), r.getQueryString(), r.getHeader("X-Amz-Target"), body,
                r.getHeader("x-amz-copy-source") != null);
        AbPolicy pol = st.policies.get(store);
        AbPolicy defaults = pol != null ? pol : AbPolicy.parse(store, new JsonObject());
        final HttpServletRequest fr = r;
        String accessKey = accessKeyOf(fr);
        AbPolicy.Ctx ctx = new AbPolicy.Ctx() {
            @Override
            public String accessKey() {
                return accessKey;
            }

            @Override
            public String ip() {
                String xff = defaults.trustXForwardedFor ? fr.getHeader("X-Forwarded-For") : null;
                return xff != null && !xff.isBlank() ? xff.split(",")[0].trim() : fr.getRemoteAddr();
            }

            @Override
            public String header(String name) {
                return fr.getHeader(name);
            }

            @Override
            public boolean read() {
                return op.read();
            }
        };
        AbPolicy.Route route = st.decide(store, ctx);
        String client = defaults.clientKey(ctx);
        if (route == AbPolicy.Route.LOCAL) {
            long t0 = System.nanoTime();
            inner.handle(target, base, r, response);
            stats.record(store, AbSide.LOCAL, response.getStatus(), System.nanoTime() - t0);
            return;
        }
        // anything that leaves the local path is authenticated by Warp first, exactly like a local request
        AuthResult a = auth == null ? AuthResult.ok(accessKey) : auth.authenticate(r, response, body);
        if (!a.ok()) {
            base.setHandled(true);
            if (!a.responded()) {
                writeError(kind, response, a.status(), a.code(), a.message());
            }
            return;
        }
        base.setHandled(true);
        String service = kind == AbOps.Kind.DYNAMO ? "dynamodb" : store;
        AbTarget tgt = st.targetFor(store);
        if (tgt == null) {
            writeError(kind, response, 503, "ServiceUnavailable", "A/B routing: no cloud target is configured for store " + store);
            stats.record(store, AbSide.CLOUD, 0, 0);
            return;
        }
        String role = defaults.roleOverrides.get(accessKey != null ? accessKey : client);
        if (role == null) {
            role = defaults.roleOverrides.get(client);
        }
        boolean needBuffer = route == AbPolicy.Route.COMPARE || route == AbPolicy.Route.DUAL_WRITE;
        if (kind == AbOps.Kind.S3 && needBuffer && hasBody) {
            long limit = route == AbPolicy.Route.DUAL_WRITE ? defaults.dualWriteMaxBytes : defaults.compareMaxBodyBytes;
            body = request.getInputStream().readNBytes((int) Math.min(limit + 1, MAX_BUFFERED_BODY));
            if (body.length > limit) {
                writeError(kind, response, 413, "EntityTooLarge", "A/B " + (route == AbPolicy.Route.DUAL_WRITE ? "dual-write" : "compare")
                        + " buffers the request body and it exceeds the policy limit of " + limit + " bytes");
                return;
            }
            r = new AbHttp.CachedBody(request, body);
        }
        Ctx2 c2 = new Ctx2(store, kind, protocol, service, tgt, role, op, client, defaults, a, st,
                kind == AbOps.Kind.SQS ? sqsOutPair(tgt, r) : null);
        switch (route) {
            case CLOUD -> cloud(c2, r, body, response, null);
            case COMPARE -> compare(c2, inner, target, base, r, body, response);
            case DUAL_WRITE -> dualWrite(c2, inner, target, base, r, body, response);
            default -> throw new IllegalStateException();
        }
    }

    /** Per-request routing facts bundled for the flows below. */
    private record Ctx2(String store, AbOps.Kind kind, String protocol, String service, AbTarget target, String role,
            AbOps.Op op, String client, AbPolicy policy, AuthResult auth, AbState state, String[] sqsPair) {
    }

    private static String accessKeyOf(HttpServletRequest r) {
        String h = r.getHeader("Authorization");
        if (h != null) {
            int i = h.indexOf("Credential=");
            if (i >= 0) {
                int end = h.indexOf('/', i);
                if (end > i) {
                    return h.substring(i + 11, end);
                }
            }
        }
        String q = r.getQueryString();
        if (q != null) {
            String v = AbOps.formParam(q, "X-Amz-Credential");
            if (v != null && v.contains("/")) {
                return v.substring(0, v.indexOf('/'));
            }
        }
        return null;
    }

    private AbForwarder.OutReq outReq(Ctx2 c, HttpServletRequest r, byte[] body) throws IOException {
        AbForwarder.OutReq o = new AbForwarder.OutReq();
        o.method = r.getMethod();
        o.rawPath = c.auth().cloudPath() != null ? c.auth().cloudPath() : r.getRequestURI();
        o.rawQuery = r.getQueryString();
        for (String n : java.util.Collections.list(r.getHeaderNames())) {
            o.headers.put(n.toLowerCase(Locale.ROOT), r.getHeader(n));
        }
        o.s3 = c.kind() == AbOps.Kind.S3;
        o.clientPayloadHash = r.getHeader("x-amz-content-sha256");
        if (body != null) {
            o.body = body;
            if (o.s3 && c.auth().bodyDecoder() != null && isChunked(r)) {
                InputStream dec = c.auth().bodyDecoder().apply(new java.io.ByteArrayInputStream(body));
                o.body = dec.readAllBytes();
            }
        } else if (o.s3 && r.getContentLengthLong() != 0 && !List.of("GET", "HEAD").contains(r.getMethod())) {
            InputStream in = r.getInputStream();
            if (isChunked(r) && c.auth().bodyDecoder() != null) {
                in = c.auth().bodyDecoder().apply(in);
                o.bodyDecoded = true;
                String dl = r.getHeader("x-amz-decoded-content-length");
                o.streamLength = dl == null ? -1 : Long.parseLong(dl.trim());
            } else {
                o.streamLength = r.getContentLengthLong();
            }
            o.stream = in;
        }
        if (c.kind() == AbOps.Kind.SQS) {
            sqsOut(c, o, r);
        }
        return o;
    }

    private static boolean isChunked(HttpServletRequest r) {
        String sha = r.getHeader("x-amz-content-sha256");
        String ce = r.getHeader("content-encoding");
        return (sha != null && sha.startsWith("STREAMING-")) || (ce != null && ce.toLowerCase(Locale.ROOT).contains("aws-chunked"));
    }

    // ----- SQS queue URL translation ---------------------------------------------------------------------------

    private static String localAccount() {
        String a = System.getenv("WARP_SQSWIRE_ACCOUNT_ID");
        return a == null || a.isBlank() ? "000000000000" : a;
    }

    private static String cloudBase(AbTarget t) {
        String b = t.endpointFor("sqs");
        return b.endsWith("/") ? b.substring(0, b.length() - 1) : b;
    }

    private static String cloudAccount(AbTarget t) {
        return t.sqsAccountId != null ? t.sqsAccountId : localAccount();
    }

    /** Client-side prefix (Warp's queue URL base) -> cloud prefix, for one request. */
    private static String[] sqsOutPair(AbTarget t, HttpServletRequest r) {
        String host = r.getHeader("Host");
        String clientBase = r.getScheme() + "://" + (host == null ? r.getServerName() + ":" + r.getServerPort() : host);
        return new String[] {clientBase + "/" + localAccount() + "/", cloudBase(t) + "/" + cloudAccount(t) + "/"};
    }

    private void sqsOut(Ctx2 c, AbForwarder.OutReq o, HttpServletRequest r) {
        String[] p = c.sqsPair();
        if (o.body != null) {
            String s = new String(o.body, StandardCharsets.UTF_8);
            s = s.replace(p[0], p[1]).replace(urlEnc(p[0]), urlEnc(p[1]));
            o.body = s.getBytes(StandardCharsets.UTF_8);
        }
        String prefix = "/" + localAccount() + "/";
        if (o.rawPath != null && o.rawPath.startsWith(prefix)) {
            o.rawPath = "/" + cloudAccount(c.target()) + "/" + o.rawPath.substring(prefix.length());
        }
    }

    private static String urlEnc(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static byte[] sqsBack(Ctx2 c, HttpServletRequest r, byte[] resp) {
        String[] p = c.sqsPair();
        String s = new String(resp, StandardCharsets.UTF_8);
        return s.replace(p[1], p[0]).getBytes(StandardCharsets.UTF_8);
    }

    // ----- CLOUD ----------------------------------------------------------------------------------------------

    /** Forward to the cloud and stream its answer to {@code out}. Returns the cloud status (0 on transport failure). */
    private int cloud(Ctx2 c, HttpServletRequest r, byte[] body, HttpServletResponse out, AbDiff.Capture capture) throws IOException {
        long t0 = System.nanoTime();
        int status = 0;
        try {
            AbForwarder.OutReq o = outReq(c, r, body);
            AbForwarder.CloudResponse cr = AbForwarder.send(c.target(), c.service(), o, c.role());
            status = cr.status;
            pump(c, r, cr, out);
        } catch (AbAuthProvider.AuthException e) {
            log.warn("ab-routing: cloud credentials unavailable for target '{}': {}", c.target().name, e.getMessage());
            writeError(c.kind(), out, 502, "WarpCloudAuthFailure", "Warp could not obtain cloud credentials for target '" + c.target().name + "'");
        } catch (IOException | RuntimeException e) {
            log.warn("ab-routing: cloud request failed for target '{}': {}", c.target().name, e.toString());
            if (!out.isCommitted()) {
                writeError(c.kind(), out, 503, "ServiceUnavailable", "The cloud service is unreachable: " + e.getClass().getSimpleName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long n = System.nanoTime() - t0;
        stats.record(c.store(), AbSide.CLOUD, status, n);
        if (metrics != null) {
            metrics.recordOperation(c.protocol(), "ab-cloud:" + c.target().name,
                    c.op().read() ? SqlMetricsCollector.StatementKind.READ : SqlMetricsCollector.StatementKind.WRITE, c.op().name(), n, n);
        }
        return status;
    }

    private void pump(Ctx2 c, HttpServletRequest r, AbForwarder.CloudResponse cr, HttpServletResponse out) throws IOException {
        out.setStatus(cr.status);
        for (var e : cr.headers.entrySet()) {
            if (e.getKey() == null || AbForwarder.DROP_RESP.contains(e.getKey().toLowerCase(Locale.ROOT))) {
                continue;
            }
            for (String v : e.getValue()) {
                out.addHeader(e.getKey(), v);
            }
        }
        try (InputStream in = cr.body) {
            if (c.kind() == AbOps.Kind.SQS) {
                byte[] b = sqsBack(c, r, in.readAllBytes());
                out.setContentLength(b.length);
                out.getOutputStream().write(b);
                return;
            }
            if (cr.contentLength >= 0) {
                out.setContentLengthLong(cr.contentLength);
            }
            byte[] buf = new byte[64 * 1024];
            var os = out.getOutputStream();
            int n;
            while ((n = in.read(buf)) > 0) {
                os.write(buf, 0, n);
            }
        }
    }

    // ----- COMPARE --------------------------------------------------------------------------------------------

    private void compare(Ctx2 c, Handler inner, String target, Request base, HttpServletRequest r, byte[] body,
            HttpServletResponse response) throws IOException, jakarta.servlet.ServletException {
        AbPolicy p = c.policy();
        long cap = p.compareMaxBodyBytes;
        boolean structured = !(c.kind() == AbOps.Kind.S3 && (c.op().name().equals("GetObject")));
        AbForwarder.OutReq o = outReq(c, r, body);
        String rl = c.role();
        if (p.comparePrimary == AbSide.LOCAL) {
            CompletableFuture<AbDiff.Capture> localF = new CompletableFuture<>();
            pool.submit(() -> {
                AbDiff.Capture cloudCap = fetch(c, o, rl, cap);
                try {
                    AbDiff.Capture localCap = localF.get(120, TimeUnit.SECONDS);
                    recordCompare(c, cloudCap, localCap, structured, AbSide.LOCAL);
                } catch (Exception e) {
                    log.debug("ab-routing: compare abandoned: {}", e.toString());
                }
            });
            AbHttp.CaptureResponse tee = new AbHttp.CaptureResponse(response, true, cap);
            long t0 = System.nanoTime();
            try {
                inner.handle(target, base, r, tee);
            } catch (IOException | RuntimeException | jakarta.servlet.ServletException e) {
                AbDiff.Capture lc = tee.capture(System.nanoTime() - t0);
                lc.error = e.getClass().getSimpleName();
                localF.complete(lc);
                throw e;
            }
            AbDiff.Capture lc = tee.capture(System.nanoTime() - t0);
            localF.complete(lc);
        } else {
            AbHttp.CaptureResponse det = new AbHttp.CaptureResponse(response, false, cap);
            long t0 = System.nanoTime();
            AbDiff.Capture lc;
            try {
                inner.handle(target, base, r, det);
                lc = det.capture(System.nanoTime() - t0);
            } catch (IOException | RuntimeException | jakarta.servlet.ServletException e) {
                lc = det.capture(System.nanoTime() - t0);
                lc.error = e.getClass().getSimpleName();
            }
            AbHttp.CaptureResponse tee = new AbHttp.CaptureResponse(response, true, cap);
            long t1 = System.nanoTime();
            int st = cloud(c, r, body, tee, null);
            AbDiff.Capture cc = tee.capture(System.nanoTime() - t1);
            if (st == 0) {
                cc.error = "cloud request failed";
            }
            recordCompare(c, cc, lc, structured, AbSide.CLOUD);
        }
    }

    private AbDiff.Capture fetch(Ctx2 c, AbForwarder.OutReq o, String role, long cap) {
        AbDiff.Capture cap0 = new AbDiff.Capture();
        long t0 = System.nanoTime();
        try {
            AbForwarder.CloudResponse cr = AbForwarder.send(c.target(), c.service(), o, role);
            cap0.status = cr.status;
            cr.headers.forEach((k, v) -> {
                if (k != null && !v.isEmpty()) {
                    cap0.headers.put(k.toLowerCase(Locale.ROOT), v.get(0));
                }
            });
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            java.io.ByteArrayOutputStream head = new java.io.ByteArrayOutputStream();
            try (InputStream in = cr.body) {
                byte[] buf = new byte[64 * 1024];
                int n;
                while ((n = in.read(buf)) > 0) {
                    md.update(buf, 0, n);
                    cap0.total += n;
                    long room = cap - head.size();
                    if (room > 0) {
                        head.write(buf, 0, (int) Math.min(room, n));
                    }
                }
            }
            cap0.head = head.toByteArray();
            cap0.truncated = cap0.total > cap;
            cap0.sha256 = AbSigV4.hex(md.digest());
        } catch (Exception e) {
            cap0.error = e instanceof AbAuthProvider.AuthException ? "cloud credentials unavailable" : e.getClass().getSimpleName();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        cap0.nanos = System.nanoTime() - t0;
        stats.record(c.store(), AbSide.CLOUD, cap0.error != null ? 0 : cap0.status, cap0.nanos);
        if (metrics != null && cap0.error == null) {
            metrics.recordOperation(c.protocol(), "ab-cloud:" + c.target().name, SqlMetricsCollector.StatementKind.READ, c.op().name(),
                    cap0.nanos, cap0.nanos);
        }
        return cap0;
    }

    private void recordCompare(Ctx2 c, AbDiff.Capture cloudCap, AbDiff.Capture localCap, boolean structured, AbSide primary) {
        if (c.kind() == AbOps.Kind.SQS && cloudCap.error == null) {
            // queue URLs differ by design (Warp vs cloud host/account): translate the cloud capture back before diffing
            if (cloudCap.head.length > 0) {
                cloudCap.head = sqsBackHead(c, cloudCap.head);
            }
        }
        List<String> d = AbDiff.diff(localCap, cloudCap, c.policy().ignoreKeys, c.policy().recordValues, structured);
        AbStats.Entry e = new AbStats.Entry(System.currentTimeMillis(), c.store(), c.op().name(), c.client(), primary.id(),
                localCap.status, cloudCap.status, localCap.nanos / 1_000_000, cloudCap.nanos / 1_000_000, d.isEmpty(), d);
        stats.addCompare(e, c.policy().bufferSize, localCap.error != null || cloudCap.error != null);
    }

    private static byte[] sqsBackHead(Ctx2 c, byte[] head) {
        String s = new String(head, StandardCharsets.UTF_8);
        return s.replace(c.sqsPair()[1], c.sqsPair()[0]).getBytes(StandardCharsets.UTF_8);
    }

    // ----- DUAL WRITE -----------------------------------------------------------------------------------------

    private void dualWrite(Ctx2 c, Handler inner, String target, Request base, HttpServletRequest r, byte[] body,
            HttpServletResponse response) throws IOException, jakarta.servlet.ServletException {
        AbPolicy p = c.policy();
        AbStats.Cmp cmp = stats.cmp(c.store());
        if (p.writeOwner == AbSide.LOCAL) {
            AbForwarder.OutReq o = outReq(c, r, body);
            long t0 = System.nanoTime();
            inner.handle(target, base, r, response);
            stats.record(c.store(), AbSide.LOCAL, response.getStatus(), System.nanoTime() - t0);
            if (response.getStatus() < 400) {
                try {
                    response.flushBuffer();
                } catch (IOException ignored) {
                    // client gone; still replicate
                }
                pool.submit(() -> {
                    AbDiff.Capture cc = fetch(c, o, c.role(), 4096);
                    if (cc.error == null && cc.status < 400) {
                        cmp.dualWriteOk.increment();
                    } else {
                        cmp.dualWriteFailed.increment();
                        log.warn("ab-routing: dual-write to the cloud failed for {} {} (status {} {}); the two sides now DIFFER",
                                c.store(), c.op().name(), cc.status, cc.error == null ? "" : cc.error);
                    }
                });
            }
        } else {
            int st = cloud(c, r, body, response, null);
            if (st > 0 && st < 400) {
                AbHttp.CaptureResponse det = new AbHttp.CaptureResponse(response, false, 4096);
                long t0 = System.nanoTime();
                try {
                    inner.handle(target, base, r, det);
                    int ls = det.capture(0).status;
                    stats.record(c.store(), AbSide.LOCAL, ls, System.nanoTime() - t0);
                    if (ls < 400) {
                        cmp.dualWriteOk.increment();
                    } else {
                        cmp.dualWriteFailed.increment();
                        log.warn("ab-routing: dual-write to local failed for {} {} (status {}); the two sides now DIFFER",
                                c.store(), c.op().name(), ls);
                    }
                } catch (IOException | RuntimeException | jakarta.servlet.ServletException e) {
                    cmp.dualWriteFailed.increment();
                    log.warn("ab-routing: dual-write to local threw for {} {}: {}; the two sides now DIFFER", c.store(), c.op().name(), e.toString());
                }
            }
        }
    }

    // ----- errors ---------------------------------------------------------------------------------------------

    static void writeError(AbOps.Kind kind, HttpServletResponse resp, int status, String code, String message) throws IOException {
        if (resp.isCommitted()) {
            return;
        }
        resp.setStatus(status);
        if (kind == AbOps.Kind.S3) {
            resp.setContentType("application/xml");
            resp.getWriter().write("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + esc(code) + "</Code><Message>"
                    + esc(message) + "</Message></Error>");
        } else {
            resp.setContentType("application/x-amz-json-1.0");
            JsonObject e = new JsonObject();
            e.addProperty("__type", code);
            e.addProperty("message", message);
            resp.getWriter().write(e.toString());
        }
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
