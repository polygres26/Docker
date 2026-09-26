package com.sayonora.wire.cqlwire;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One client connection speaking CQL native protocol v3/v4 (v5 is refused so drivers negotiate down to v4). */
final class CqlSession implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(CqlSession.class);

    static final int ERROR = 0, STARTUP = 1, READY = 2, AUTHENTICATE = 3, OPTIONS = 5, SUPPORTED = 6, QUERY = 7, RESULT = 8, PREPARE = 9,
            EXECUTE = 0xA, REGISTER = 0xB, EVENT = 0xC, BATCH = 0xD, AUTH_CHALLENGE = 0xE, AUTH_RESPONSE = 0xF, AUTH_SUCCESS = 0x10;

    private final CqlWireServer server;
    private final Socket socket;
    private final Engine engine;
    private final ClientState cs = new ClientState();
    private final Runnable onClose;
    private OutputStream out;
    private int version = 4;
    private boolean ready;
    private final Consumer<Result.SchemaEvent> eventListener = this::sendEvent;
    private boolean registered;

    CqlSession(CqlWireServer server, Socket socket, Runnable onClose) {
        this.server = server;
        this.socket = socket;
        this.engine = server.engine();
        this.onClose = onClose;
        cs.localAddress = socket.getLocalAddress().getHostAddress();
    }

    @Override
    public void run() {
        try (Socket s = socket) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 65536));
            out = s.getOutputStream();
            while (true) {
                byte[] hdr = new byte[9];
                try {
                    in.readFully(hdr);
                } catch (EOFException e) {
                    return;
                }
                int ver = hdr[0] & 0xFF;
                int flags = hdr[1] & 0xFF;
                int stream = (short) (((hdr[2] & 0xFF) << 8) | (hdr[3] & 0xFF));
                int opcode = hdr[4] & 0xFF;
                int len = ((hdr[5] & 0xFF) << 24) | ((hdr[6] & 0xFF) << 16) | ((hdr[7] & 0xFF) << 8) | (hdr[8] & 0xFF);
                if ((ver & 0x80) != 0 || (ver & 0x7F) < 3 || (ver & 0x7F) > 4) {
                    // an unsupported version: answer with our highest, drivers then retry with it
                    if (len > 0 && len < (1 << 20)) {
                        in.skipNBytes(len);
                    }
                    version = 4;
                    send(stream, ERROR, protocolError("Invalid or unsupported protocol version (" + (ver & 0x7F)
                            + "); supported versions are (3/v3, 4/v4)"));
                    if ((ver & 0x80) != 0 || len >= (1 << 20)) {
                        return;
                    }
                    continue;
                }
                if (len < 0 || len > 256 * 1024 * 1024) {
                    send(stream, ERROR, protocolError("Frame body too large"));
                    return;
                }
                version = ver & 0x7F;
                cs.protocolVersion = version;
                byte[] body = new byte[len];
                in.readFully(body);
                byte[] resp;
                int respOp;
                try {
                    if ((flags & 0x01) != 0) {
                        throw new CqlError(CqlError.PROTOCOL, "Compression is not supported");
                    }
                    Wire.In bi = new Wire.In(body);
                    if ((flags & 0x04) != 0) {
                        int n = bi.u16();
                        for (int i = 0; i < n; i++) {
                            bi.string();
                            bi.bytes();
                        }
                    }
                    long t0 = System.nanoTime();
                    Object[] r = handle(opcode, bi);
                    respOp = (Integer) r[0];
                    resp = (byte[]) r[1];
                    if (r.length > 2) {
                        server.record((String) r[2], (Boolean) r[3], System.nanoTime() - t0);
                    }
                } catch (CqlError e) {
                    respOp = ERROR;
                    resp = errorBody(e);
                } catch (RuntimeException e) {
                    log.error("cqlwire: internal error", e);
                    respOp = ERROR;
                    resp = errorBody(new CqlError(CqlError.SERVER, "Internal error: " + e));
                }
                send(stream, respOp, resp);
                if (respOp == ERROR && opcode == AUTH_RESPONSE) {
                    return;
                }
            }
        } catch (IOException e) {
            // client gone
        } finally {
            if (registered) {
                engine.removeListener(eventListener);
            }
            onClose.run();
        }
    }

    private byte[] protocolError(String msg) {
        return errorBody(new CqlError(CqlError.PROTOCOL, msg));
    }

    static byte[] errorBody(CqlError e) {
        Wire.Out o = new Wire.Out().i32(e.code).string(e.getMessage() == null ? "" : e.getMessage());
        if (e.code == CqlError.ALREADY_EXISTS) {
            o.string(e.keyspace).string(e.table);
        } else if (e.code == CqlError.UNPREPARED) {
            o.shortBytes(e.id);
        } else if (e.code == CqlError.UNAVAILABLE) {
            o.u16(1).i32(1).i32(0);
        } else if (e.code == CqlError.FUNCTION_FAILURE) {
            o.string(e.keyspace).string(e.function).u16(0);
        }
        return o.toBytes();
    }

    private synchronized void send(int stream, int opcode, byte[] body) throws IOException {
        byte[] f = new byte[9 + body.length];
        f[0] = (byte) (0x80 | version);
        f[1] = 0;
        f[2] = (byte) (stream >> 8);
        f[3] = (byte) stream;
        f[4] = (byte) opcode;
        f[5] = (byte) (body.length >>> 24);
        f[6] = (byte) (body.length >>> 16);
        f[7] = (byte) (body.length >>> 8);
        f[8] = (byte) body.length;
        System.arraycopy(body, 0, f, 9, body.length);
        out.write(f);
        out.flush();
    }

    private void sendEvent(Result.SchemaEvent ev) {
        try {
            Wire.Out o = new Wire.Out().string("SCHEMA_CHANGE");
            schemaBody(o, ev);
            send(-1, EVENT, o.toBytes());
        } catch (IOException e) {
            // the session loop notices the dead socket
        }
    }

    private static void schemaBody(Wire.Out o, Result.SchemaEvent ev) {
        o.string(ev.change()).string(ev.target()).string(ev.ks());
        if (!ev.target().equals("KEYSPACE")) {
            o.string(ev.name());
        }
    }

    // ------------------------------------------------------------------ requests

    private Object[] handle(int opcode, Wire.In in) {
        if (!ready && server.authRequired() && opcode != OPTIONS && opcode != STARTUP && opcode != AUTH_RESPONSE) {
            throw new CqlError(CqlError.PROTOCOL, "Unexpected message " + opcode + ", expecting STARTUP or CREDENTIALS");
        }
        switch (opcode) {
            case OPTIONS -> {
                Map<String, List<String>> m = new LinkedHashMap<>();
                m.put("CQL_VERSION", List.of("3.4.7"));
                m.put("COMPRESSION", List.of());
                m.put("PROTOCOL_VERSIONS", List.of("3/v3", "4/v4"));
                return new Object[] {SUPPORTED, new Wire.Out().stringMultimap(m).toBytes()};
            }
            case STARTUP -> {
                Map<String, String> opts = in.stringMap();
                if (!opts.containsKey("CQL_VERSION")) {
                    throw new CqlError(CqlError.PROTOCOL, "Missing value CQL_VERSION in STARTUP message");
                }
                if (opts.containsKey("COMPRESSION")) {
                    throw new CqlError(CqlError.PROTOCOL, "Unknown compression algorithm: " + opts.get("COMPRESSION"));
                }
                if (server.authRequired()) {
                    return new Object[] {AUTHENTICATE, new Wire.Out().string("org.apache.cassandra.auth.PasswordAuthenticator").toBytes()};
                }
                ready = true;
                return new Object[] {READY, new byte[0]};
            }
            case AUTH_RESPONSE -> {
                byte[] token = in.bytes();
                if (token == null) {
                    throw new CqlError(CqlError.PROTOCOL, "Expecting an SASL PLAIN token");
                }
                // SASL PLAIN: [authzid] NUL authcid NUL passwd
                int a = -1, b = -1;
                for (int i = 0; i < token.length; i++) {
                    if (token[i] == 0) {
                        if (a < 0) {
                            a = i;
                        } else if (b < 0) {
                            b = i;
                        }
                    }
                }
                if (a < 0 || b < 0) {
                    throw new CqlError(CqlError.BAD_CREDENTIALS, "Invalid SASL PLAIN token");
                }
                String user = new String(token, a + 1, b - a - 1, StandardCharsets.UTF_8);
                byte[] pw = java.util.Arrays.copyOfRange(token, b + 1, token.length);
                if (!server.checkPassword(user, pw)) {
                    throw new CqlError(CqlError.BAD_CREDENTIALS, "Provided username " + user + " and/or password are incorrect");
                }
                cs.user = user;
                ready = true;
                return new Object[] {AUTH_SUCCESS, new Wire.Out().bytes(null).toBytes()};
            }
            case REGISTER -> {
                int n = in.u16();
                boolean schema = false;
                for (int i = 0; i < n; i++) {
                    String t = in.string();
                    schema |= t.equals("SCHEMA_CHANGE");
                }
                if (schema && !registered) {
                    registered = true;
                    engine.addListener(eventListener);
                }
                return new Object[] {READY, new byte[0]};
            }
            case QUERY -> {
                String q = in.longString();
                QueryOpts o = new QueryOpts();
                byte[][] values = readParams(in, o, true);
                Parser.Parsed p = server.parse(q);
                if (p.binds() != values.length) {
                    throw CqlError.invalid("Invalid amount of bind variables");
                }
                return exec(p.stmt(), new Eval.Binds(values.length == 0 ? new byte[0][] : values, lastNames), o, cs);
            }
            case PREPARE -> {
                String q = in.longString();
                Engine.Prepared p = server.prepare(q, cs);
                return new Object[] {RESULT, preparedBody(p)};
            }
            case EXECUTE -> {
                byte[] id = in.shortBytes();
                Engine.Prepared p = server.prepared(id);
                if (p == null) {
                    CqlError e = new CqlError(CqlError.UNPREPARED, "Prepared query with ID " + CqlType.hex(id) + " not found (either the query was not prepared on this host "
                            + "(maybe the host has been restarted?) or you have prepared too many queries and it has been evicted from the internal cache)");
                    e.id = id;
                    throw e;
                }
                QueryOpts o = new QueryOpts();
                byte[][] values = readParams(in, o, true);
                if (values.length != p.binds.size()) {
                    throw CqlError.invalid("Invalid amount of bind variables");
                }
                ClientState ecs = new ClientState();
                ecs.keyspace = p.keyspace != null ? p.keyspace : cs.keyspace;
                ecs.user = cs.user;
                ecs.localAddress = cs.localAddress;
                ecs.protocolVersion = cs.protocolVersion;
                Object[] r = exec(p.stmt, new Eval.Binds(values, lastNames), o, ecs);
                return r;
            }
            case BATCH -> {
                return batch(in);
            }
            default -> throw new CqlError(CqlError.PROTOCOL, "Unsupported opcode " + opcode);
        }
    }

    private String[] lastNames;

    /** Reads the query parameters (consistency, flags, values, paging, timestamps) and returns the bound values. */
    private byte[][] readParams(Wire.In in, QueryOpts o, boolean withValues) {
        o.consistency = in.u16();
        int flags = in.u8();
        byte[][] values = new byte[0][];
        lastNames = null;
        if ((flags & 0x01) != 0) {
            int n = in.u16();
            values = new byte[n][];
            boolean names = (flags & 0x40) != 0;
            String[] nm = names ? new String[n] : null;
            for (int i = 0; i < n; i++) {
                if (names) {
                    nm[i] = in.string();
                }
                values[i] = in.value();
            }
            lastNames = nm;
        }
        o.skipMetadata = (flags & 0x02) != 0;
        if ((flags & 0x04) != 0) {
            o.pageSize = in.i32();
            if (o.pageSize <= 0) {
                o.pageSize = -1;
            }
        }
        if ((flags & 0x08) != 0) {
            o.pagingState = in.bytes();
        }
        if ((flags & 0x10) != 0) {
            in.u16();
        }
        if ((flags & 0x20) != 0) {
            o.timestamp = in.i64();
        }
        return values;
    }

    private Object[] batch(Wire.In in) {
        int type = in.u8();
        int n = in.u16();
        List<Ast.Stmt> stmts = new ArrayList<>();
        List<Eval.Binds> binds = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int kind = in.u8();
            Ast.Stmt st;
            if (kind == 0) {
                st = server.parse(in.longString()).stmt();
            } else {
                byte[] id = in.shortBytes();
                Engine.Prepared p = server.prepared(id);
                if (p == null) {
                    CqlError e = new CqlError(CqlError.UNPREPARED, "Prepared query with ID " + CqlType.hex(id) + " not found");
                    e.id = id;
                    throw e;
                }
                st = p.stmt;
            }
            int nv = in.u16();
            byte[][] vals = new byte[nv][];
            for (int k = 0; k < nv; k++) {
                vals[k] = in.value();
            }
            stmts.add(st);
            binds.add(new Eval.Binds(vals, null));
        }
        QueryOpts o = new QueryOpts();
        o.consistency = in.u16();
        int flags = in.u8();
        if ((flags & 0x10) != 0) {
            in.u16();
        }
        if ((flags & 0x20) != 0) {
            o.timestamp = in.i64();
        }
        String kind = type == 1 ? "UNLOGGED" : type == 2 ? "COUNTER" : "LOGGED";
        long t0 = System.nanoTime();
        Result r = engine.dml.batch(kind, null, stmts, binds, Eval.Binds.NONE, cs, o);
        return new Object[] {RESULT, resultBody(r, o, null), "BATCH", true};
    }

    private Object[] exec(Ast.Stmt st, Eval.Binds b, QueryOpts o, ClientState state) {
        Result r = engine.execute(st, b, o, state);
        if (r.kind == Result.Kind.SET_KEYSPACE) {
            cs.keyspace = r.keyspace;
        }
        return new Object[] {RESULT, resultBody(r, o, null), Engine.opName(st), Engine.isWrite(st)};
    }

    // ------------------------------------------------------------------ result encoding

    private static void writeSpecs(Wire.Out o, List<Result.ColSpec> cols, boolean global) {
        for (Result.ColSpec c : cols) {
            if (!global) {
                o.string(c.ks()).string(c.table());
            }
            o.string(c.name());
            c.type().writeOption(o);
        }
    }

    private static boolean sameTable(List<Result.ColSpec> cols) {
        if (cols.isEmpty()) {
            return false;
        }
        Result.ColSpec f = cols.get(0);
        for (Result.ColSpec c : cols) {
            if (!c.ks().equals(f.ks()) || !c.table().equals(f.table())) {
                return false;
            }
        }
        return true;
    }

    byte[] resultBody(Result r, QueryOpts opts, Engine.Prepared prepared) {
        Wire.Out o = new Wire.Out();
        switch (r.kind) {
            case VOID -> o.i32(1);
            case SET_KEYSPACE -> o.i32(3).string(r.keyspace);
            case SCHEMA_CHANGE -> {
                o.i32(5);
                schemaBody(o, r.event);
            }
            default -> {
                o.i32(2);
                boolean global = sameTable(r.cols);
                int flags = (global ? 1 : 0) | (r.pagingState != null ? 2 : 0);
                o.i32(flags).i32(r.cols.size());
                if (r.pagingState != null) {
                    o.bytes(r.pagingState);
                }
                if (global) {
                    o.string(r.cols.get(0).ks()).string(r.cols.get(0).table());
                }
                writeSpecs(o, r.cols, global);
                o.i32(r.rows.size());
                for (byte[][] row : r.rows) {
                    for (byte[] c : row) {
                        o.bytes(c);
                    }
                }
            }
        }
        return o.toBytes();
    }

    byte[] preparedBody(Engine.Prepared p) {
        Wire.Out o = new Wire.Out().i32(4).shortBytes(p.idBytes());
        boolean global = sameTable(p.binds);
        o.i32(global ? 1 : 0).i32(p.binds.size()).i32(p.pkIndexes.length);
        for (int i : p.pkIndexes) {
            o.u16(i);
        }
        if (global) {
            o.string(p.binds.get(0).ks()).string(p.binds.get(0).table());
        }
        writeSpecs(o, p.binds, global);
        if (p.results.isEmpty()) {
            o.i32(4).i32(0);
        } else {
            boolean g2 = sameTable(p.results);
            o.i32(g2 ? 1 : 0).i32(p.results.size());
            if (g2) {
                o.string(p.results.get(0).ks()).string(p.results.get(0).table());
            }
            writeSpecs(o, p.results, g2);
        }
        return o.toBytes();
    }

    static byte[] md5(String s) {
        try {
            return MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
