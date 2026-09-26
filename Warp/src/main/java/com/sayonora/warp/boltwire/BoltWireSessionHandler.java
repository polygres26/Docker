package com.sayonora.warp.boltwire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ConnectionRoute;
import com.sayonora.warp.core.SessionConnectionLease;
import com.sayonora.warp.core.SqlMetricsCollector;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * boltwire -- Neo4j's Bolt wire protocol (binary TCP), so the official Neo4j drivers can point at Warp directly. Speaks
 * Bolt 5.0-5.4 and 4.4 (the highest version the client offers wins), the full message set (HELLO/LOGON/LOGOFF, RUN, PULL and
 * DISCARD with {@code n} and {@code qid}, BEGIN/COMMIT/ROLLBACK, RESET with FAILED-state IGNORED semantics, ROUTE, TELEMETRY,
 * GOODBYE) and PackStream's whole type system (see {@link PackStream}).
 *
 * <p>Cypher text goes through {@link CypherParser}, {@link Analyzer} (compile-time errors, like Neo4j) and {@link Executor},
 * which runs it over the property graph in Postgres ({@link PgGraphStore}); there is no dialect translation, so the shared
 * StatementPipeline is not involved (metrics are reported from here). Read-only statements execute lazily at the first
 * PULL/DISCARD (runtime errors surface there, like Neo4j); updating statements execute at RUN, in one backend transaction.
 *
 * <p>Connection multiplexing: an auto-commit statement borrows a backend connection for its execution and returns it;
 * an explicit transaction (BEGIN) pins one until COMMIT/ROLLBACK/RESET (see {@link SessionConnectionLease}).
 */
public final class BoltWireSessionHandler implements Runnable {

    static final String NEO4J_VERSION = "5.26.30";

    private static final Logger log = LoggerFactory.getLogger(BoltWireSessionHandler.class);

    private static final byte[] BOLT_MAGIC = {0x60, 0x60, (byte) 0xB0, 0x17};
    private static final AtomicLong CONNECTION_IDS = new AtomicLong();
    private static final AtomicLong BOOKMARKS = new AtomicLong(System.currentTimeMillis() % 1_000_000);

    private final Socket clientSocket;
    private final BackendRegistry backendRegistry;
    private final PgGraphStore graphStore;
    private final SqlMetricsCollector sqlMetrics;
    private final SessionConnectionLease lease;

    private DataInputStream in;
    private DataOutputStream out;
    private int major = 4;
    private int minor = 4;
    private boolean utc;
    private String routingAddress;
    private boolean failed;
    private boolean hello;

    // route of the current database (auto-commit: per RUN; in a transaction: fixed at BEGIN)
    private ConnectionRoute route = ConnectionRoute.UNROUTED;
    private String heldGraphBackend;
    private boolean inTx;
    private String txDatabase;
    private long txDeadlineMillis;
    private long txStartedNanos;
    private final Map<Long, Open> results = new LinkedHashMap<>();
    private long nextQid;
    private Open lastOpen;

    public BoltWireSessionHandler(Socket clientSocket, BackendRegistry backendRegistry) {
        this(clientSocket, backendRegistry, null);
    }

    public BoltWireSessionHandler(Socket clientSocket, BackendRegistry backendRegistry, SqlMetricsCollector sqlMetrics) {
        this.clientSocket = clientSocket;
        this.backendRegistry = backendRegistry;
        this.graphStore = new PgGraphStore(backendRegistry);
        this.lease = new SessionConnectionLease(this::openGraphConnection);
        this.sqlMetrics = sqlMetrics;
    }

    private Connection openGraphConnection() throws SQLException {
        BackendTarget target = graphStore.targetFor(route);
        if (target == null) {
            throw new SQLException("backend " + route.description() + " cannot host a graph (only Postgres backends can)");
        }
        heldGraphBackend = target.name();
        return graphStore.connect(target);
    }

    @Override
    public void run() {
        try (Socket socket = clientSocket) {
            in = new DataInputStream(new java.io.BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new java.io.BufferedOutputStream(socket.getOutputStream()));
            if (!performHandshake()) {
                return;
            }
            sessionLoop();
        } catch (IOException e) {
            log.debug("boltwire: session ended ({})", e.getMessage());
        } catch (RuntimeException e) {
            log.warn("boltwire: session failed", e);
        } finally {
            try {
                if (inTx) {
                    lease.rollback();
                }
            } catch (SQLException | RuntimeException e) {
                log.debug("boltwire: rollback at disconnect failed: {}", e.toString());
            }
            lease.close();
        }
    }

    // ------------------------------------------------------------------------------------------ handshake

    /** Picks the highest version both sides speak: Bolt 5.0-5.4 or 4.4. A manifest-style proposal (0x000001FF) is skipped:
     * the classic four-byte reply is always understood. */
    private boolean performHandshake() throws IOException {
        byte[] preamble = new byte[4];
        in.readFully(preamble);
        for (int i = 0; i < 4; i++) {
            if (preamble[i] != BOLT_MAGIC[i]) {
                log.warn("boltwire: bad handshake preamble (not a real Bolt client?): {}", bytesToHex(preamble));
                return false;
            }
        }
        byte[] proposals = new byte[16];
        in.readFully(proposals);
        int bestMajor = 0, bestMinor = 0;
        for (int i = 0; i < 4; i++) {
            int base = i * 4;
            int range = proposals[base + 1] & 0xFF;
            int pMinor = proposals[base + 2] & 0xFF;
            int pMajor = proposals[base + 3] & 0xFF;
            if (pMajor == 0xFF || pMajor == 0) {
                continue;
            }
            int lowMinor = Math.max(0, pMinor - range);
            int hiMinor = pMinor;
            int m = 0, mn = -1;
            if (pMajor == 5) {
                int cap = Math.min(hiMinor, 4);
                if (cap >= lowMinor) {
                    m = 5;
                    mn = cap;
                }
            } else if (pMajor == 4) {
                if (lowMinor <= 4 && hiMinor >= 4) {
                    m = 4;
                    mn = 4;
                }
            }
            if (m > bestMajor || (m == bestMajor && mn > bestMinor)) {
                bestMajor = m;
                bestMinor = mn;
            }
        }
        if (bestMajor == 0) {
            log.warn("boltwire: no proposed Bolt version this handler supports (5.0-5.4 or 4.4) -- proposals: {}",
                    bytesToHex(proposals));
            out.write(new byte[4]);
            out.flush();
            return false;
        }
        major = bestMajor;
        minor = bestMinor;
        out.write(new byte[] {0, 0, (byte) minor, (byte) major});
        out.flush();
        return true;
    }

    // ------------------------------------------------------------------------------------------ message loop

    private void sessionLoop() throws IOException {
        while (true) {
            byte[] messageBytes = readChunkedMessage(in);
            if (messageBytes == null) {
                return;
            }
            PackStream.Struct msg = new PackStream.Reader(messageBytes).readMessage();
            int tag = msg.tag();
            if (tag == BoltMessages.GOODBYE) {
                return;
            }
            if (tag == BoltMessages.RESET) {
                handleReset();
                continue;
            }
            if (failed) {
                writeIgnored();
                continue;
            }
            try {
                switch (tag) {
                    case BoltMessages.HELLO -> handleHello(msg);
                    case BoltMessages.LOGON -> writeSuccess(Map.of());
                    case BoltMessages.LOGOFF -> writeSuccess(Map.of());
                    case BoltMessages.TELEMETRY -> writeSuccess(Map.of());
                    case BoltMessages.RUN -> handleRun(msg);
                    case BoltMessages.PULL -> handlePull(msg, false);
                    case BoltMessages.DISCARD -> handlePull(msg, true);
                    case BoltMessages.BEGIN -> handleBegin(msg);
                    case BoltMessages.COMMIT -> handleCommit();
                    case BoltMessages.ROLLBACK -> handleRollback();
                    case BoltMessages.ROUTE -> handleRoute(msg);
                    default -> writeFailure("Neo.ClientError.Request.Invalid",
                            "boltwire: unsupported message 0x" + Integer.toHexString(tag));
                }
            } catch (CypherException e) {
                failStatement(e.code(), e.getMessage());
            } catch (SQLException e) {
                log.warn("boltwire: backend error: {}", e.getMessage());
                failStatement(e instanceof com.sayonora.warp.core.BackendPoolExhaustedException
                        ? "Neo.TransientError.General.DatabaseUnavailable" : "Neo.DatabaseError.General.UnknownError",
                        e.getMessage());
            } catch (RuntimeException e) {
                log.warn("boltwire: unexpected error handling message 0x{}", Integer.toHexString(tag), e);
                failStatement("Neo.DatabaseError.General.UnknownError", String.valueOf(e));
            }
        }
    }

    /** A message failed: FAILURE goes out, the session ignores everything until RESET, an open transaction is rolled back. */
    private void failStatement(String code, String message) throws IOException {
        results.clear();
        lastOpen = null;
        if (inTx) {
            try {
                lease.rollback();
            } catch (SQLException e) {
                log.debug("boltwire: rollback after failure: {}", e.toString());
            }
            // the transaction is dead; the connection is released once RESET/ROLLBACK acknowledges it
            inTx = false;
            lease.releaseIfIdle();
        }
        writeFailure(code, message);
    }

    private void handleHello(PackStream.Struct msg) throws IOException {
        Map<String, Object> extra = msg.fields().isEmpty() || !(msg.fields().get(0) instanceof Map<?, ?> m) ? Map.of()
                : castMap(m);
        if (extra.get("routing") instanceof Map<?, ?> r && r.get("address") instanceof String a) {
            routingAddress = a;
        }
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("server", "Neo4j/" + NEO4J_VERSION);
        metadata.put("connection_id", "bolt-" + CONNECTION_IDS.incrementAndGet());
        if (major == 4 && extra.get("patch_bolt") instanceof List<?> patches && patches.contains("utc")) {
            utc = true;
            metadata.put("patch_bolt", List.of("utc"));
        }
        Map<String, Object> hints = new LinkedHashMap<>();
        hints.put("telemetry.enabled", false);
        metadata.put("hints", hints);
        hello = true;
        writeSuccess(metadata);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> m) {
        return (Map<String, Object>) m;
    }

    private void handleReset() throws IOException {
        results.clear();
        lastOpen = null;
        try {
            if (inTx) {
                lease.rollback();
            }
        } catch (SQLException e) {
            log.debug("boltwire: rollback at RESET: {}", e.toString());
        }
        inTx = false;
        lease.releaseIfIdle();
        failed = false;
        writeSuccess(Map.of());
    }

    // ------------------------------------------------------------------------------------------ transactions

    private void handleBegin(PackStream.Struct msg) throws IOException, SQLException {
        Map<String, Object> extra = extraOf(msg, 0);
        if (inTx) {
            throw new CypherException("Neo.ClientError.Request.Invalid", "Transaction already open");
        }
        String db = extra.get("db") instanceof String s ? s : null;
        resolveRoute(db);
        txDatabase = db;
        txDeadlineMillis = extra.get("tx_timeout") instanceof Long t ? t : 0;
        txStartedNanos = System.nanoTime();
        results.clear();
        lastOpen = null;
        if (lease.isHeld() && !sameBackend()) {
            lease.close();
        }
        lease.begin();
        inTx = true;
        writeSuccess(Map.of());
    }

    private void handleCommit() throws IOException, SQLException {
        if (!inTx) {
            throw new CypherException("Neo.ClientError.Request.Invalid", "No transaction to commit");
        }
        drainOpenResults();
        try {
            lease.commit();
        } finally {
            inTx = false;
            lease.releaseIfIdle();
        }
        Map<String, Object> md = new LinkedHashMap<>();
        md.put("bookmark", "warp:bookmark:" + BOOKMARKS.incrementAndGet());
        writeSuccess(md);
    }

    private void handleRollback() throws IOException, SQLException {
        if (!inTx) {
            throw new CypherException("Neo.ClientError.Request.Invalid", "No transaction to roll back");
        }
        results.clear();
        lastOpen = null;
        try {
            lease.rollback();
        } finally {
            inTx = false;
            lease.releaseIfIdle();
        }
        writeSuccess(Map.of());
    }

    /** Unexecuted (lazy) statements of the transaction run before COMMIT so their effects are not lost. */
    private void drainOpenResults() {
        for (Open o : new ArrayList<>(results.values())) {
            if (!o.executed) {
                execute(o);
            }
        }
    }

    private boolean sameBackend() {
        BackendTarget want = graphStore.targetFor(route);
        return want != null && want.name().equals(heldGraphBackend);
    }

    private void resolveRoute(String database) {
        route = backendRegistry.connectionRouter().resolve(com.sayonora.warp.core.ConnectionRouter.PROTO_BOLT, database, null);
        if (route.isRejected()) {
            route = ConnectionRoute.UNROUTED;
            throw new CypherException("Neo.ClientError.Database.DatabaseNotFound",
                    "Database does not exist. Database name: '" + database + "'.");
        }
    }

    private Map<String, Object> extraOf(PackStream.Struct msg, int idx) {
        if (msg.fields().size() > idx && msg.fields().get(idx) instanceof Map<?, ?> m) {
            return castMap(m);
        }
        return Map.of();
    }

    // ------------------------------------------------------------------------------------------ ROUTE

    private void handleRoute(PackStream.Struct msg) throws IOException {
        Map<String, Object> ctx = extraOf(msg, 0);
        String db = null;
        if (msg.fields().size() > 2) {
            Object f = msg.fields().get(2);
            if (f instanceof String s) {
                db = s;
            } else if (f instanceof Map<?, ?> m && m.get("db") instanceof String s) {
                db = s;
            }
        }
        if (db != null && !db.equalsIgnoreCase("neo4j") && !db.equalsIgnoreCase("system")) {
            resolveRoute(db);
        }
        String addr = ctx.get("address") instanceof String a ? a : routingAddress;
        if (addr == null) {
            addr = clientSocket.getLocalAddress().getHostAddress() + ":" + clientSocket.getLocalPort();
        }
        List<Object> servers = new ArrayList<>();
        for (String role : new String[] {"WRITE", "READ", "ROUTE"}) {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("addresses", List.of(addr));
            s.put("role", role);
            servers.add(s);
        }
        Map<String, Object> rt = new LinkedHashMap<>();
        rt.put("ttl", 300L);
        rt.put("db", db == null ? "neo4j" : db);
        rt.put("servers", servers);
        Map<String, Object> md = new LinkedHashMap<>();
        md.put("rt", rt);
        writeSuccess(md);
    }

    // ------------------------------------------------------------------------------------------ statements

    /** A statement between RUN and the end of its stream. */
    private static final class Open {
        long qid;
        String cypher;
        Cy.Query query;
        Analyzer.Info info;
        Map<String, Object> params;
        long deadlineMillis;
        ConnectionRoute route;
        boolean inTx;
        boolean executed;
        List<String> columns = List.of();
        List<List<Object>> rows = List.of();
        int pos;
        Exec.Stats stats;
        long tFirst;
        String bookmark;
        long startedNanos;
    }

    private static final int CACHE_SIZE = 512;
    private static final Map<String, Object[]> PREPARED = java.util.Collections.synchronizedMap(
            new LinkedHashMap<String, Object[]>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Object[]> e) {
                    return size() > CACHE_SIZE;
                }
            });

    /** Parses (cached) and analyses. A statement whose analysis depends on parameter types (it uses a parameter) is analysed
     * again per RUN with the actual parameters, like Neo4j's parameter-type-aware planning; others are cached whole. */
    private static Object[] prepare(String cypher, Map<String, Object> params) {
        Object[] hit = PREPARED.get(cypher);
        if (hit != null && !(Boolean) hit[2]) {
            return hit;
        }
        Cy.Query q = hit != null ? (Cy.Query) hit[0] : CypherParser.parse(cypher);
        Analyzer.Info info = Analyzer.analyze(q, params);
        Object[] v = {q, info, info.usesParams};
        if (hit == null) {
            PREPARED.put(cypher, v);
        }
        return v;
    }

    private void handleRun(PackStream.Struct msg) throws IOException, SQLException {
        String cypher = (String) msg.fields().get(0);
        Map<String, Object> params = msg.fields().size() > 1 && msg.fields().get(1) instanceof Map<?, ?> m ? castMap(m) : Map.of();
        Map<String, Object> extra = extraOf(msg, 2);
        Open o = new Open();
        o.cypher = cypher;
        o.params = params;
        o.inTx = inTx;
        if (!inTx) {
            String database = extra.get("db") instanceof String db ? db : null;
            resolveRoute(database);
            if (lease.isHeld() && !sameBackend()) {
                lease.close();
            }
            o.deadlineMillis = extra.get("tx_timeout") instanceof Long t ? t : 0;
            results.clear();
            lastOpen = null;
        }
        o.route = route;
        Object[] prepared = prepare(cypher, params);
        o.query = (Cy.Query) prepared[0];
        o.info = (Analyzer.Info) prepared[1];
        o.qid = inTx ? nextQid++ : -1;
        o.columns = o.info.columns;
        o.startedNanos = System.nanoTime();
        results.put(o.qid, o);
        lastOpen = o;
        boolean eager = o.info.updates || o.info.schema;
        if (eager) {
            execute(o);
        }
        Map<String, Object> md = new LinkedHashMap<>();
        md.put("fields", new ArrayList<Object>(o.columns));
        md.put("t_first", 0L);
        if (inTx) {
            md.put("qid", o.qid);
        }
        writeSuccess(md);
    }

    /** Runs the statement to completion (rows fully materialised) on the session's connection. */
    private void execute(Open o) {
        long rttStart = System.nanoTime();
        Connection conn = null;
        boolean autoTx = false;
        boolean ok = false;
        try {
            if (!o.inTx) {
                if (o.route != route) {
                    route = o.route;
                }
                conn = lease.acquire();
                if (o.info.updates || o.info.schema) {
                    conn.setAutoCommit(false);
                    autoTx = true;
                }
            } else {
                conn = lease.acquire();
            }
            BackendTarget target = graphStore.targetFor(route);
            Exec x = new Exec(graphStore, target, conn, o.params);
            long timeoutMs = o.inTx ? txDeadlineMillis : o.deadlineMillis;
            if (timeoutMs > 0) {
                x.deadlineNanos = (o.inTx ? txStartedNanos : System.nanoTime()) + timeoutMs * 1_000_000L;
            }
            Executor ex = new Executor(x, o.info);
            Executor.Result r = ex.run(o.query);
            o.columns = r.columns();
            o.rows = r.rows();
            o.stats = x.stats;
            if (autoTx) {
                conn.commit();
                o.bookmark = "warp:bookmark:" + BOOKMARKS.incrementAndGet();
            }
            ok = true;
        } catch (SQLException e) {
            throw new CypherException(e instanceof com.sayonora.warp.core.BackendPoolExhaustedException
                    ? "Neo.TransientError.General.DatabaseUnavailable" : "Neo.DatabaseError.General.UnknownError",
                    e instanceof com.sayonora.warp.core.BackendPoolExhaustedException ? e.getMessage() : "boltwire: " + e.getMessage());
        } finally {
            if (autoTx && conn != null) {
                try {
                    if (!ok) {
                        conn.rollback();
                    }
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    log.debug("boltwire: transaction cleanup: {}", e.toString());
                }
            }
            if (!o.inTx) {
                lease.releaseIfIdle();
            }
            o.executed = true;
            recordMetrics(o, rttStart);
        }
    }

    private void handlePull(PackStream.Struct msg, boolean discard) throws IOException {
        Map<String, Object> extra = extraOf(msg, 0);
        long n = extra.get("n") instanceof Long l ? l : -1;
        long qid = extra.get("qid") instanceof Long q ? q : -1;
        Open o = qid == -1 ? (inTx ? lastOpen : results.get(-1L)) : results.get(qid);
        if (o == null) {
            throw new CypherException("Neo.ClientError.Request.Invalid", "boltwire: no open result to "
                    + (discard ? "DISCARD" : "PULL"));
        }
        if (!o.executed) {
            execute(o);
        }
        int total = o.rows.size();
        int take = n < 0 ? total - o.pos : (int) Math.min(n, total - o.pos);
        if (!discard) {
            for (int i = 0; i < take; i++) {
                List<Object> row = o.rows.get(o.pos + i);
                writeRecord(row);
            }
        }
        o.pos += take;
        Map<String, Object> md = new LinkedHashMap<>();
        if (o.pos < total) {
            md.put("has_more", true);
            writeSuccess(md);
            return;
        }
        results.remove(o.qid);
        if (lastOpen == o) {
            lastOpen = null;
        }
        if (o.bookmark != null && !o.inTx) {
            md.put("bookmark", o.bookmark);
        }
        md.put("t_last", (System.nanoTime() - o.startedNanos) / 1_000_000L);
        boolean updates = o.info.updates && o.stats != null && (o.stats.containsUpdates() || o.stats.containsSystemUpdates()
                || true);
        String type = o.info.schema && o.info.updates ? "s" : (o.info.updates ? (o.columns.isEmpty() ? "w" : "rw") : "r");
        md.put("type", type);
        if (o.stats != null && (o.stats.containsUpdates() || o.stats.containsSystemUpdates())) {
            md.put("stats", o.stats.toMap());
        }
        md.put("db", txDatabase != null && o.inTx ? txDatabase : "neo4j");
        md.put("has_more", false);
        writeSuccess(md);
    }

    private void recordMetrics(Open o, long rttStart) {
        if (sqlMetrics == null) {
            return;
        }
        long elapsedNanos = System.nanoTime() - rttStart;
        SqlMetricsCollector.StatementKind kind = o.info.updates || o.info.schema ? SqlMetricsCollector.StatementKind.WRITE
                : SqlMetricsCollector.StatementKind.READ;
        sqlMetrics.recordOperation("boltwire", null, kind, normalizeCypherForLabel(o.cypher), elapsedNanos, elapsedNanos);
    }

    private static final java.util.regex.Pattern STRING_LITERAL_LABEL = java.util.regex.Pattern.compile("'[^']*'|\"[^\"]*\"");
    private static final java.util.regex.Pattern NUMBER_LITERAL_LABEL = java.util.regex.Pattern.compile("\\b\\d+(?:\\.\\d+)?\\b");

    /** Collapses literal values out of the Cypher text so e.g. every {@code RETURN 1}, {@code RETURN 2} lands in one bucket. */
    private static String normalizeCypherForLabel(String cypher) {
        String normalized = STRING_LITERAL_LABEL.matcher(cypher).replaceAll("?");
        normalized = NUMBER_LITERAL_LABEL.matcher(normalized).replaceAll("?");
        normalized = normalized.strip().replaceAll("\\s+", " ");
        return normalized.isEmpty() ? "(empty)" : normalized;
    }

    // ------------------------------------------------------------------------------------------ wire output

    private void writeSuccess(Map<String, Object> metadata) throws IOException {
        writeMessage(w -> {
            w.writeStructHeader(1, BoltMessages.SUCCESS);
            w.writeMap(metadata);
        });
    }

    private void writeRecord(List<Object> row) throws IOException {
        writeMessage(w -> {
            w.writeStructHeader(1, BoltMessages.RECORD);
            w.writeListHeader(row.size());
            for (Object v : row) {
                w.writeValue(v);
            }
        });
    }

    private void writeFailure(String code, String message) throws IOException {
        failed = true;
        writeMessage(w -> {
            w.writeStructHeader(1, BoltMessages.FAILURE);
            w.writeMapHeader(2);
            w.writeString("code");
            w.writeString(code);
            w.writeString("message");
            w.writeString(message == null ? "" : message);
        });
    }

    private void writeIgnored() throws IOException {
        writeMessage(w -> w.writeStructHeader(0, BoltMessages.IGNORED));
    }

    private interface WriterAction {
        void write(PackStream.Writer w);
    }

    /** One PackStream message in Bolt chunked framing: chunks of at most 65535 bytes, then a zero-length end marker. */
    private void writeMessage(WriterAction action) throws IOException {
        PackStream.Writer w = new PackStream.Writer(major, utc);
        try {
            action.write(w);
        } catch (RuntimeException e) {
            // an unencodable value must fail this statement, not the connection
            log.warn("boltwire: failed to encode a message", e);
            throw new CypherException("Neo.DatabaseError.General.UnknownError", "boltwire: cannot encode value: " + e.getMessage());
        }
        byte[] body = w.toByteArray();
        int off = 0;
        while (off < body.length) {
            int n = Math.min(0xFFFF, body.length - off);
            out.writeShort(n);
            out.write(body, off, n);
            off += n;
        }
        out.writeShort(0);
        out.flush();
    }

    /** Reads one full Bolt message across chunks; null on a clean EOF between messages. */
    private static byte[] readChunkedMessage(DataInputStream in) throws IOException {
        java.io.ByteArrayOutputStream message = new java.io.ByteArrayOutputStream();
        while (true) {
            int chunkLen;
            try {
                chunkLen = in.readUnsignedShort();
            } catch (java.io.EOFException e) {
                return message.size() == 0 ? null : message.toByteArray();
            }
            if (chunkLen == 0) {
                if (message.size() == 0) {
                    continue; // NOOP chunk (keep-alive)
                }
                return message.toByteArray();
            }
            byte[] chunk = new byte[chunkLen];
            in.readFully(chunk);
            message.write(chunk);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b));
        }
        return sb.toString().strip();
    }

    /** Kept for tests that build a handler without a live hello. */
    boolean helloSeen() {
        return hello;
    }

    static Map<String, Object> unusedMap() {
        return new HashMap<>();
    }
}
