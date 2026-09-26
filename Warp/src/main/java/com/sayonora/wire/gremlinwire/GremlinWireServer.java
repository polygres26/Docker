package com.sayonora.wire.gremlinwire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.wire.acl.ConnectionGate;
import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import com.sayonora.wire.gremlinwire.Serializers.Req;
import com.sayonora.wire.gremlinwire.Serializers.Resp;
import com.sayonora.wire.gremlinwire.Serializers.Ser;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gremlinwire: the Apache TinkerPop Gremlin Server frontend. One port (default 8182) speaks the Gremlin Server WebSocket protocol
 * (GraphSON 3.0 / 2.0 and GraphBinary 1.0 negotiated by mimetype, ops {@code eval}, {@code bytecode}, sessions, SASL PLAIN) and the
 * HTTP endpoint ({@code POST /} with {@code {"gremlin": ...}}). Data lives in the {@code gremlin} store of the backend set's Postgres
 * hosts (see docs/WARP_GUIDE.md, "The Gremlin store (gremlinwire)"); the Cosmos DB Gremlin API speaks the same protocol.
 */
public final class GremlinWireServer {

    private static final Logger log = LoggerFactory.getLogger(GremlinWireServer.class);
    private static final String WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private static final int MAX_CONTENT = (int) Processor.envLong("WARP_GREMLINWIRE_MAX_CONTENT_LENGTH", 10 * 1024 * 1024);

    private final int port;
    private final ConnectionGate gate;
    private final Processor processor;
    private volatile ServerSocket serverSocket;
    private final ExecutorService sessions = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "gremlinwire-session");
        t.setDaemon(true);
        return t;
    });

    public GremlinWireServer(int port, BackendRegistry registry, ConnectionGate gate, SqlMetricsCollector metrics) {
        this(port, new PgGraph(registry), gate, metrics, () -> {
            List<String> h = registry.storeHosts(StoreType.GREMLIN);
            return h.isEmpty() ? "default" : h.size() == 1 ? h.get(0) : String.join(",", h);
        });
    }

    GremlinWireServer(int port, GraphStore store, ConnectionGate gate, SqlMetricsCollector metrics, java.util.function.Supplier<String> backend) {
        this.port = port;
        this.gate = gate == null ? ConnectionGate.DISABLED : gate;
        String a = System.getenv("WARP_GREMLINWIRE_AUTH");
        String creds = System.getenv("WARP_AUTH_CREDENTIALS");
        boolean auth = a != null ? "true".equalsIgnoreCase(a) : creds != null && !creds.isBlank();
        this.processor = new Processor(store, metrics, backend, auth, new CredentialStore());
    }

    Processor processor() {
        return processor;
    }

    public int port() {
        ServerSocket s = serverSocket;
        return s == null ? port : s.getLocalPort();
    }

    public void start() throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(port), 1024);
        serverSocket = ss;
        Thread t = new Thread(this::acceptLoop, "gremlinwire-accept");
        t.setDaemon(true);
        t.start();
        log.info("warp gremlinwire listening on port {}", ss.getLocalPort());
    }

    public void close() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // closing
        }
        sessions.shutdownNow();
    }

    private void acceptLoop() {
        ServerSocket ss = serverSocket;
        while (!ss.isClosed()) {
            Socket client;
            try {
                client = ss.accept();
            } catch (IOException e) {
                if (ss.isClosed()) {
                    return;
                }
                continue;
            }
            boolean gated = false;
            try {
                client.setTcpNoDelay(true);
                if (!gate.acceptTcp(client)) {
                    continue;
                }
                gated = true;
                Socket c = client;
                sessions.execute(() -> {
                    try {
                        serve(c);
                    } finally {
                        gate.release();
                    }
                });
                gated = false;
            } catch (Exception e) {
                log.warn("gremlinwire: dropping a connection that could not be served: {}", e.toString());
                if (gated) {
                    gate.release();
                }
                try {
                    client.close();
                } catch (IOException ignored) {
                    // gone
                }
            }
        }
    }

    // ================================================================== connection

    private void serve(Socket sock) {
        try (sock) {
            InputStream in = new BufferedInputStream(sock.getInputStream(), 16384);
            OutputStream out = new BufferedOutputStream(sock.getOutputStream(), 16384);
            Processor.ConnState cs = new Processor.ConnState();
            cs.remote = String.valueOf(sock.getRemoteSocketAddress());
            while (true) {
                Head h = readHead(in);
                if (h == null) {
                    return;
                }
                if ("websocket".equalsIgnoreCase(h.headers.get("upgrade"))) {
                    webSocket(h, in, out, cs);
                    return;
                }
                if (!http(h, in, out, cs)) {
                    return;
                }
            }
        } catch (IOException | UncheckedIOException e) {
            // client went away
        } catch (RuntimeException e) {
            log.debug("gremlinwire connection failed: {}", e.toString());
        }
    }

    private static final class Head {
        String method;
        String target;
        String version;
        final Map<String, String> headers = new LinkedHashMap<>();
    }

    private static Head readHead(InputStream in) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        int state = 0;
        while (true) {
            int c = in.read();
            if (c < 0) {
                if (b.size() == 0) {
                    return null;
                }
                throw new EOFException();
            }
            b.write(c);
            if (b.size() > 32768) {
                throw new IOException("header too large");
            }
            state = c == '\r' && (state == 0 || state == 2) ? state + 1 : c == '\n' && (state == 1 || state == 3) ? state + 1 : 0;
            if (state == 4) {
                break;
            }
        }
        String[] lines = b.toString(StandardCharsets.ISO_8859_1).split("\r\n");
        String[] rl = lines[0].split(" ");
        if (rl.length < 2) {
            throw new IOException("bad request line");
        }
        Head h = new Head();
        h.method = rl[0];
        h.target = rl[1];
        h.version = rl.length > 2 ? rl[2] : "HTTP/1.1";
        for (int i = 1; i < lines.length; i++) {
            int c = lines[i].indexOf(':');
            if (c > 0) {
                h.headers.put(lines[i].substring(0, c).trim().toLowerCase(Locale.ROOT), lines[i].substring(c + 1).trim());
            }
        }
        return h;
    }

    // ================================================================== WebSocket

    private void webSocket(Head h, InputStream in, OutputStream out, Processor.ConnState cs) throws IOException {
        String key = h.headers.get("sec-websocket-key");
        if (key == null) {
            writeHttp(out, 400, "Bad Request", "text/plain", "missing Sec-WebSocket-Key".getBytes(StandardCharsets.UTF_8), false, null);
            return;
        }
        String accept;
        try {
            accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest((key + WS_GUID).getBytes(StandardCharsets.ISO_8859_1)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
        out.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n")
                .getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
        Object lock = new Object();
        ByteArrayOutputStream frag = new ByteArrayOutputStream();
        int fragOp = 0;
        while (true) {
            int b0 = in.read();
            if (b0 < 0) {
                return;
            }
            int b1 = readByte(in);
            boolean fin = (b0 & 0x80) != 0;
            int op = b0 & 0x0f;
            boolean masked = (b1 & 0x80) != 0;
            long len = b1 & 0x7f;
            if (len == 126) {
                len = (readByte(in) << 8) | readByte(in);
            } else if (len == 127) {
                len = 0;
                for (int i = 0; i < 8; i++) {
                    len = (len << 8) | readByte(in);
                }
            }
            if (len > MAX_CONTENT) {
                writeFrame(out, lock, 8, new byte[] {0x03, (byte) 0xf1});
                return;
            }
            byte[] mask = new byte[4];
            if (masked) {
                readFully(in, mask, 4);
            }
            byte[] payload = new byte[(int) len];
            readFully(in, payload, payload.length);
            if (masked) {
                for (int i = 0; i < payload.length; i++) {
                    payload[i] ^= mask[i & 3];
                }
            }
            switch (op) {
                case 8:
                    writeFrame(out, lock, 8, payload.length >= 2 ? new byte[] {payload[0], payload[1]} : new byte[0]);
                    return;
                case 9:
                    writeFrame(out, lock, 10, payload);
                    continue;
                case 10:
                    continue;
                case 0, 1, 2: {
                    if (op != 0) {
                        frag.reset();
                        fragOp = op;
                    }
                    frag.write(payload);
                    if (frag.size() > MAX_CONTENT) {
                        writeFrame(out, lock, 8, new byte[] {0x03, (byte) 0xf1});
                        return;
                    }
                    if (!fin) {
                        continue;
                    }
                    byte[] msg = frag.toByteArray();
                    frag.reset();
                    message(fragOp == 2, msg, out, lock, cs);
                    continue;
                }
                default:
                    writeFrame(out, lock, 8, new byte[] {0x03, (byte) 0xea});
                    return;
            }
        }
    }

    private static int readByte(InputStream in) throws IOException {
        int c = in.read();
        if (c < 0) {
            throw new EOFException();
        }
        return c;
    }

    private static void readFully(InputStream in, byte[] b, int n) throws IOException {
        int o = 0;
        while (o < n) {
            int r = in.read(b, o, n - o);
            if (r < 0) {
                throw new EOFException();
            }
            o += r;
        }
    }

    private void writeFrame(OutputStream out, Object lock, int op, byte[] payload) throws IOException {
        synchronized (lock) {
            out.write(0x80 | op);
            if (payload.length < 126) {
                out.write(payload.length);
            } else if (payload.length < 65536) {
                out.write(126);
                out.write(payload.length >> 8);
                out.write(payload.length);
            } else {
                out.write(127);
                long l = payload.length;
                for (int i = 7; i >= 0; i--) {
                    out.write((int) (l >> (8 * i)));
                }
            }
            out.write(payload);
            out.flush();
        }
    }

    private void message(boolean binary, byte[] msg, OutputStream out, Object lock, Processor.ConnState cs) throws IOException {
        Ser ser;
        int offset;
        if (binary) {
            if (msg.length == 0) {
                return;
            }
            int ml = msg[0] & 0xff;
            if (ml + 1 > msg.length) {
                sendRaw(out, lock, true, errorJson(498, "Message could not be parsed. Check the format of the request. [bad mimetype header]"));
                return;
            }
            String mime = new String(msg, 1, ml, StandardCharsets.UTF_8);
            ser = Serializers.forMime(mime);
            offset = 1 + ml;
            if (ser == null) {
                sendRaw(out, lock, true, errorJson(498, "Serializer for requested Content-Type=" + mime + " not found"));
                return;
            }
        } else {
            ser = Serializers.forMime("application/json");
            offset = 0;
        }
        boolean useBinary = binary;
        Req req;
        try {
            req = ser.decode(msg, offset);
        } catch (G.GremlinError e) {
            Resp r = new Resp();
            r.code = e.code;
            r.message = e.getMessage();
            sendRaw(out, lock, useBinary, ser.encode(r));
            return;
        }
        processor.handle(req, ser, cs, bytes -> {
            try {
                sendRaw(out, lock, useBinary, bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void sendRaw(OutputStream out, Object lock, boolean binary, byte[] payload) throws IOException {
        writeFrame(out, lock, binary ? 2 : 1, payload);
    }

    private static byte[] errorJson(int code, String message) {
        Resp r = new Resp();
        r.code = code;
        r.message = message;
        return Serializers.forMime("application/json").encode(r);
    }

    // ================================================================== HTTP

    /** Captures the processor's response messages instead of encoding them. */
    private static final class Capture implements Ser {
        final List<Resp> resps = new ArrayList<>();

        @Override
        public String mime() {
            return "capture";
        }

        @Override
        public boolean binary() {
            return false;
        }

        @Override
        public Req decode(byte[] payload, int offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] encode(Resp r) {
            resps.add(r);
            return new byte[0];
        }
    }

    private boolean http(Head h, InputStream in, OutputStream out, Processor.ConnState cs) throws IOException {
        boolean keepAlive = !"close".equalsIgnoreCase(h.headers.get("connection")) && !"HTTP/1.0".equals(h.version);
        String expect = h.headers.get("expect");
        if (expect != null && expect.equalsIgnoreCase("100-continue")) {
            out.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
        }
        byte[] body = new byte[0];
        String cl = h.headers.get("content-length");
        if (cl != null) {
            long n = Long.parseLong(cl.trim());
            if (n > MAX_CONTENT) {
                writeHttp(out, 413, "Payload Too Large", "application/json", jsonMsg("Request body too large"), false, null);
                return false;
            }
            body = new byte[(int) n];
            readFully(in, body, body.length);
        } else if ("chunked".equalsIgnoreCase(h.headers.get("transfer-encoding"))) {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            while (true) {
                String line = readLine(in);
                int n = Integer.parseInt(line.trim().split(";")[0], 16);
                if (n == 0) {
                    readLine(in);
                    break;
                }
                byte[] chunk = new byte[n];
                readFully(in, chunk, n);
                b.write(chunk);
                readLine(in);
                if (b.size() > MAX_CONTENT) {
                    writeHttp(out, 413, "Payload Too Large", "application/json", jsonMsg("Request body too large"), false, null);
                    return false;
                }
            }
            body = b.toByteArray();
        }
        String path = h.target;
        String query = null;
        int q = path.indexOf('?');
        if (q >= 0) {
            query = path.substring(q + 1);
            path = path.substring(0, q);
        }
        if (processor.authRequired()) {
            String auth = h.headers.get("authorization");
            boolean ok = false;
            if (auth != null && auth.regionMatches(true, 0, "Basic ", 0, 6)) {
                try {
                    String dec = new String(Base64.getDecoder().decode(auth.substring(6).trim()), StandardCharsets.UTF_8);
                    int c = dec.indexOf(':');
                    ok = c > 0 && processor.checkPassword(dec.substring(0, c), dec.substring(c + 1).getBytes(StandardCharsets.UTF_8));
                } catch (IllegalArgumentException ignored) {
                    ok = false;
                }
            }
            if (!ok) {
                writeHttp(out, 401, "Unauthorized", "application/json", jsonMsg("Username and/or password are incorrect"), keepAlive,
                        "WWW-Authenticate: Basic realm=\"Gremlin Server\"");
                return keepAlive;
            }
            cs.authenticated = true;
        }
        String method = h.method.toUpperCase(Locale.ROOT);
        JsonObject json;
        if (method.equals("POST")) {
            json = Serializers.Json.parseHttpBody(new String(body, StandardCharsets.UTF_8));
            if (json == null) {
                writeHttp(out, 400, "Bad Request", "application/json", jsonMsg("body could not be parsed"), keepAlive, null);
                return keepAlive;
            }
        } else if (method.equals("GET")) {
            json = new JsonObject();
            if (query != null) {
                for (String kv : query.split("&")) {
                    int e = kv.indexOf('=');
                    if (e > 0) {
                        json.addProperty(URLDecoder.decode(kv.substring(0, e), StandardCharsets.UTF_8), URLDecoder.decode(kv.substring(e + 1), StandardCharsets.UTF_8));
                    }
                }
            }
        } else {
            writeHttp(out, 405, "Method Not Allowed", "application/json", jsonMsg("Method not allowed"), keepAlive, "Allow: GET, POST");
            return keepAlive;
        }
        if (!json.has("gremlin") || !json.get("gremlin").isJsonPrimitive()) {
            writeHttp(out, 400, "Bad Request", "application/json", jsonMsg("no gremlin script supplied"), keepAlive, null);
            return keepAlive;
        }
        Ser ser = httpSerializer(h.headers.get("accept"), h.headers.get("content-type"));
        Req req = new Req();
        req.requestId = java.util.UUID.randomUUID();
        req.op = "eval";
        req.processor = "";
        for (Map.Entry<String, JsonElement> e : json.entrySet()) {
            req.args.put(e.getKey(), GraphSon.V3.read(e.getValue()));
        }
        req.args.put("batchSize", Integer.MAX_VALUE);
        Capture cap = new Capture();
        processor.handle(req, cap, cs, x -> {
        });
        Resp last = cap.resps.isEmpty() ? null : cap.resps.get(cap.resps.size() - 1);
        if (last == null) {
            writeHttp(out, 500, "Internal Server Error", "application/json", jsonMsg("no response"), keepAlive, null);
            return keepAlive;
        }
        if (last.code == 200 || last.code == 204) {
            Resp r = new Resp();
            r.requestId = req.requestId;
            r.code = 200;
            r.hasData = true;
            r.data = last.code == 204 || last.data == null ? new ArrayList<>() : last.data;
            byte[] payload;
            try {
                payload = ser.encode(r);
            } catch (RuntimeException e) {
                writeHttp(out, 500, "Internal Server Error", "application/json", jsonMsg("Error during serialization: " + e.getMessage()), keepAlive, null);
                return keepAlive;
            }
            writeHttp(out, 200, "OK", ser.binary() ? ser.mime() : "application/json", payload, keepAlive, null);
            return keepAlive;
        }
        int status = last.code == 401 || last.code == 407 ? 401 : last.code >= 400 && last.code < 500 ? 400 : 500;
        JsonObject err = new JsonObject();
        err.addProperty("message", last.message);
        if (status == 500) {
            Object ex = last.attributes.get("exceptions");
            if (ex instanceof List<?> l && !l.isEmpty()) {
                err.addProperty("Exception-Class", String.valueOf(l.get(0)));
                com.google.gson.JsonArray arr = new com.google.gson.JsonArray();
                l.forEach(x -> arr.add(String.valueOf(x)));
                err.add("exceptions", arr);
            }
            Object st = last.attributes.get("stackTrace");
            if (st != null) {
                err.addProperty("stackTrace", String.valueOf(st));
            }
        }
        writeHttp(out, status, status == 401 ? "Unauthorized" : status == 400 ? "Bad Request" : "Internal Server Error", "application/json",
                err.toString().getBytes(StandardCharsets.UTF_8), keepAlive, null);
        return keepAlive;
    }

    private static Ser httpSerializer(String accept, String contentType) {
        if (accept != null) {
            for (String a : accept.split(",")) {
                String m = a.trim();
                if (m.equals("*/*") || m.equals("application/json") || m.isEmpty()) {
                    continue;
                }
                Ser s = Serializers.forMime(m);
                if (s != null) {
                    return s;
                }
            }
        }
        return Serializers.forMime("application/vnd.gremlin-v3.0+json;types=false");
    }

    private static byte[] jsonMsg(String m) {
        JsonObject o = new JsonObject();
        o.addProperty("message", m);
        return o.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = readByte(in);
            if (c == '\n') {
                return sb.toString().trim();
            }
            sb.append((char) c);
        }
    }

    private static void writeHttp(OutputStream out, int code, String reason, String contentType, byte[] body, boolean keepAlive, String extraHeader)
            throws IOException {
        StringBuilder sb = new StringBuilder("HTTP/1.1 ").append(code).append(' ').append(reason).append("\r\n");
        sb.append("content-type: ").append(contentType).append("\r\n");
        sb.append("content-length: ").append(body.length).append("\r\n");
        if (extraHeader != null) {
            sb.append(extraHeader).append("\r\n");
        }
        if (!keepAlive) {
            sb.append("connection: close\r\n");
        }
        sb.append("\r\n");
        out.write(sb.toString().getBytes(StandardCharsets.ISO_8859_1));
        out.write(body);
        out.flush();
    }
}
