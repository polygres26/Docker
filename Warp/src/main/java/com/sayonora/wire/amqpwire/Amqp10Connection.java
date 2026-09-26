package com.sayonora.wire.amqpwire;

import com.sayonora.wire.amqpwire.Amqp10Codec.AMap;
import com.sayonora.wire.amqpwire.Amqp10Codec.Described;
import com.sayonora.wire.amqpwire.Amqp10Codec.Sym;
import com.sayonora.wire.amqpwire.Amqp10Codec.UInt;
import com.sayonora.wire.amqpwire.AmqpBroker.Consumer;
import com.sayonora.wire.amqpwire.AmqpStore.MsgRow;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AMQP 1.0 connection (SASL PLAIN / ANONYMOUS, open, begin, attach, flow, transfer, disposition, detach, end, close) served on the same
 * port as AMQP 0-9-1 after protocol-header sniffing. A sender link of the client publishes into an exchange or a queue ({@code /exchanges/<x>/<key>},
 * {@code /queues/<q>}, or the {@code to} of each message on an anonymous link); a receiver link consumes a queue ({@code /queues/<q>}) with the
 * link credit as the prefetch window. Outcomes map onto the store: accepted = ack, released = requeue, modified (delivery-failed) = requeue counted as
 * a failed delivery, rejected = dead-letter. Messages published over AMQP 1.0 keep their original sections for AMQP 1.0 consumers and are converted
 * to properties + body for AMQP 0-9-1 consumers, and the other way round.
 */
final class Amqp10Connection {

    private static final Logger log = LoggerFactory.getLogger(Amqp10Connection.class);

    static final byte[] HEADER_AMQP = {'A', 'M', 'Q', 'P', 0, 1, 0, 0};
    static final byte[] HEADER_SASL = {'A', 'M', 'Q', 'P', 3, 1, 0, 0};

    private static final long OPEN = 0x10, BEGIN = 0x11, ATTACH = 0x12, FLOW = 0x13, TRANSFER = 0x14, DISPOSITION = 0x15, DETACH = 0x16, END = 0x17,
            CLOSE = 0x18, ERROR = 0x1d, SOURCE = 0x28, TARGET = 0x29, ACCEPTED = 0x24, REJECTED = 0x25, RELEASED = 0x26, MODIFIED = 0x27;
    private static final long SASL_MECHANISMS = 0x40, SASL_INIT = 0x41, SASL_OUTCOME = 0x44;
    private static final long SESSION_WINDOW = 400;
    private static final long LINK_CREDIT = 170;
    private static final long MAX_MESSAGE_SIZE = 16L * 1024 * 1024;

    private final AmqpWireServer server;
    private final AmqpBroker br;
    private final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;
    private final Object writeLock = new Object();
    private final Map<Integer, Session> sessions = new HashMap<>();
    private final String connId;
    private String vhost = "/";
    private String user = "";
    private int maxFrame = 131072;
    private long peerIdleMs;
    private volatile boolean closing;
    private volatile long lastWrite = System.currentTimeMillis();
    private ScheduledFuture<?> hbTask;

    Amqp10Connection(AmqpWireServer server, AmqpBroker br, Socket socket, DataInputStream in, OutputStream out, String connId) {
        this.server = server;
        this.br = br;
        this.socket = socket;
        this.in = in;
        this.out = out;
        this.connId = connId;
    }

    // ------------------------------------------------------------------------------------------ frames

    private record Frame(int type, int channel, byte[] payload) {
    }

    private void writeFrame(int type, int channel, byte[] payload) throws IOException {
        byte[] f = new byte[payload.length + 8];
        int size = f.length;
        f[0] = (byte) (size >> 24);
        f[1] = (byte) (size >> 16);
        f[2] = (byte) (size >> 8);
        f[3] = (byte) size;
        f[4] = 2;
        f[5] = (byte) type;
        f[6] = (byte) (channel >> 8);
        f[7] = (byte) channel;
        System.arraycopy(payload, 0, f, 8, payload.length);
        out.write(f);
    }

    private void send(int channel, byte[] payload) {
        try {
            synchronized (writeLock) {
                writeFrame(0, channel, payload);
                out.flush();
                lastWrite = System.currentTimeMillis();
            }
        } catch (IOException e) {
            abort();
        }
    }

    private void sendSasl(byte[] payload) throws IOException {
        synchronized (writeLock) {
            writeFrame(1, 0, payload);
            out.flush();
        }
    }

    private void abort() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // gone
        }
    }

    private Frame readFrame() throws IOException {
        long size = in.readInt() & 0xffffffffL;
        int doff = in.readUnsignedByte();
        int type = in.readUnsignedByte();
        int channel = in.readUnsignedShort();
        if (size < 8 || doff < 2 || doff * 4L > size || size > 131072L * 2) {
            throw new Amqp10Codec.AmqpFormatException("bad frame size " + size);
        }
        byte[] rest = new byte[(int) size - 8];
        in.readFully(rest);
        int skip = doff * 4 - 8;
        return new Frame(type, channel, java.util.Arrays.copyOfRange(rest, skip, rest.length));
    }

    // ------------------------------------------------------------------------------------------ encoding helpers

    private static byte[] perf(long code, Object... fields) {
        List<Object> l = new ArrayList<>(java.util.Arrays.asList(fields));
        while (!l.isEmpty() && l.get(l.size() - 1) == null) {
            l.remove(l.size() - 1);
        }
        Amqp10Codec.Writer w = new Amqp10Codec.Writer();
        w.described(code).value(l);
        return w.toBytes();
    }

    private static Described error(String condition, String description) {
        return new Described(new Amqp10Codec.U64(ERROR), new ArrayList<Object>(List.of(new Sym(condition), description)));
    }

    private static Described described(long code, Object... fields) {
        List<Object> l = new ArrayList<>(java.util.Arrays.asList(fields));
        while (!l.isEmpty() && l.get(l.size() - 1) == null) {
            l.remove(l.size() - 1);
        }
        return new Described(new Amqp10Codec.U64(code), l);
    }

    private static Amqp10Codec.U32 u32(long v) {
        return new Amqp10Codec.U32(v);
    }

    private static Object field(List<?> l, int i) {
        return l != null && i < l.size() ? l.get(i) : null;
    }

    private static List<?> fields(Object o) {
        return o instanceof Described d && d.value() instanceof List<?> l ? l : o instanceof List<?> l ? l : List.of();
    }

    // ------------------------------------------------------------------------------------------ entry

    /** Runs the connection after the 8 header bytes were read ({@code header} tells SASL from plain AMQP). */
    void run(byte[] header) {
        try {
            socket.setSoTimeout(30_000);
            boolean sasl = header[4] == 3;
            if (sasl) {
                if (!doSasl()) {
                    return;
                }
                byte[] h = new byte[8];
                in.readFully(h);
                if (!java.util.Arrays.equals(h, HEADER_AMQP)) {
                    synchronized (writeLock) {
                        out.write(HEADER_AMQP);
                        out.flush();
                    }
                    return;
                }
            } else if (server.authRequired()) {
                // a login is mandatory: answer with the SASL header and hang up, as RabbitMQ does for an unauthenticated AMQP header
                synchronized (writeLock) {
                    out.write(HEADER_SASL);
                    out.flush();
                }
                return;
            } else {
                user = "anonymous";
            }
            synchronized (writeLock) {
                out.write(HEADER_AMQP);
                out.flush();
            }
            loop();
        } catch (EOFException | SocketTimeoutException e) {
            // client gone or silent beyond the idle timeout
        } catch (Amqp10Codec.AmqpFormatException e) {
            log.debug("amqpwire 1.0: connection {} framing error: {}", connId, e.getMessage());
            sendClose("amqp:connection:framing-error", e.getMessage());
        } catch (IOException e) {
            log.debug("amqpwire 1.0: connection {} ended: {}", connId, e.toString());
        } catch (RuntimeException e) {
            log.warn("amqpwire 1.0: connection {} failed", connId, e);
        } finally {
            cleanup();
        }
    }

    private boolean doSasl() throws IOException {
        synchronized (writeLock) {
            out.write(HEADER_SASL);
            out.flush();
        }
        boolean anon = !server.authRequired();
        sendSasl(perf(SASL_MECHANISMS, new Amqp10Codec.SymArr(List.of("PLAIN", "AMQPLAIN", "ANONYMOUS"))));
        Frame f = readFrame();
        if (f.type() != 1) {
            return false;
        }
        Amqp10Codec.Reader r = new Amqp10Codec.Reader(f.payload());
        Object v = r.read();
        if (!(v instanceof Described d) || d.code() != SASL_INIT) {
            return false;
        }
        List<?> l = fields(d);
        String mechName = Amqp10Codec.asString(field(l, 0));
        Object ir = field(l, 1);
        byte[] resp = ir instanceof byte[] b ? b : new byte[0];
        int code = 1;
        if ("PLAIN".equals(mechName)) {
            int a = indexOf(resp, 0);
            int b = a < 0 ? -1 : indexOf(resp, a + 1);
            if (a >= 0 && b >= 0) {
                String u = new String(resp, a + 1, b - a - 1, StandardCharsets.UTF_8);
                byte[] p = java.util.Arrays.copyOfRange(resp, b + 1, resp.length);
                if (server.authenticate(u, p)) {
                    code = 0;
                    user = u;
                }
            }
        } else if ("AMQPLAIN".equals(mechName)) {
            try {
                byte[] withLen = new byte[resp.length + 4];
                withLen[0] = (byte) (resp.length >> 24);
                withLen[1] = (byte) (resp.length >> 16);
                withLen[2] = (byte) (resp.length >> 8);
                withLen[3] = (byte) resp.length;
                System.arraycopy(resp, 0, withLen, 4, resp.length);
                Map<String, Object> t = new AmqpCodec.Reader(withLen).table();
                String u = t.get("LOGIN") instanceof String s ? s : null;
                byte[] p = t.get("PASSWORD") instanceof String s ? s.getBytes(StandardCharsets.UTF_8) : null;
                if (server.authenticate(u, p)) {
                    code = 0;
                    user = u == null ? "" : u;
                }
            } catch (AmqpException e) {
                code = 1;
            }
        } else if ("ANONYMOUS".equals(mechName) && anon) {
            code = 0;
            user = "anonymous";
        }
        if (!"PLAIN".equals(mechName) && !"AMQPLAIN".equals(mechName) && !"ANONYMOUS".equals(mechName)) {
            return false;                    // RabbitMQ closes the socket on an unknown mechanism
        }
        int c = code;
        sendSasl(perf(SASL_OUTCOME, new Amqp10Codec.U8(c)));
        return code == 0;
    }

    private static int indexOf(byte[] a, int from) {
        for (int i = from; i < a.length; i++) {
            if (a[i] == 0) {
                return i;
            }
        }
        return -1;
    }

    // ------------------------------------------------------------------------------------------ session / link model

    private final class Session {
        final int channel;
        long nextOutgoingId;          // our next transfer-id (one per transfer frame)
        long nextDeliveryId;          // our next delivery-id (one per delivery)
        long nextIncomingId;          // the peer's next transfer id we expect
        long incomingWindow = SESSION_WINDOW;
        long remoteIncomingWindow = 2048;
        long remoteNextIncomingId;
        final Map<Long, Link> links = new HashMap<>();
        final Map<Long, Unsettled> unsettled = new LinkedHashMap<>();   // outgoing delivery id -> message
        final Object lock = new Object();
        /** Ids are assigned and the frames written under one lock, so delivery-ids reach the client in order; the state lock is only held briefly inside. */
        final Object deliverLock = new Object();

        Session(int channel) {
            this.channel = channel;
        }
    }

    private record Unsettled(Link link, String vhost, String queue, long seq, MsgRow row) {
    }

    private final class Link implements DeliveryTarget {
        final Session session;
        final String name;
        final long handle;
        final boolean serverSends;      // the client is the receiver
        final int sndSettleMode;
        final int rcvSettleMode;
        String address;
        // server sender
        Consumer consumer;
        long deliveryCount;             // outgoing delivery-count of a sending link
        long credit;                    // link credit available to us
        boolean drain;
        long tagCounter;
        // server receiver
        String exchange = "";
        String routingKey = "";
        boolean anonymous;
        long incomingCount;             // deliveries received
        long grantedCredit;             // credit outstanding on a receiving link
        ByteArrayOutputStream partial;
        long partialId = -1;
        boolean partialSettled;
        boolean detached;

        Link(Session s, String name, long handle, boolean serverSends, int snd, int rcv) {
            this.session = s;
            this.name = name;
            this.handle = handle;
            this.serverSends = serverSends;
            this.sndSettleMode = snd;
            this.rcvSettleMode = rcv;
        }

        @Override
        public int available(Consumer c) {
            synchronized (session.lock) {
                if (detached || credit <= 0 || session.remoteIncomingWindow <= 0) {
                    return 0;
                }
                return (int) Math.min(credit, Math.min(session.remoteIncomingWindow, 1000));
            }
        }

        @Override
        public boolean deliver(Consumer c, MsgRow m) {
            byte[] msg = Amqp10Message.toWire(m, m.redelivered());
            boolean settled = c.noAck;
            synchronized (session.deliverLock) {
                long id;
                byte[] tag = new byte[4];
                synchronized (session.lock) {
                    if (detached || c.cancelled || credit <= 0) {
                        return false;
                    }
                    int frames = framesFor(msg.length);
                    id = session.nextDeliveryId++;
                    session.nextOutgoingId += frames;               // every transfer frame takes a transfer-id, every delivery a delivery-id
                    credit--;
                    deliveryCount++;
                    session.remoteIncomingWindow -= frames;
                    long t = m.seq() & 0xffffffffL;
                    tag[0] = (byte) (t >> 24);
                    tag[1] = (byte) (t >> 16);
                    tag[2] = (byte) (t >> 8);
                    tag[3] = (byte) t;
                    if (!settled) {
                        session.unsettled.put(id, new Unsettled(this, m.vhost(), m.queue(), m.seq(), m));
                    }
                }
                sendTransfer(session, this, id, tag, settled, msg);
                return true;
            }
        }

        @Override
        public void serverCancel(Consumer c) {
            synchronized (session.lock) {
                detached = true;
            }
            sendDetach(session, this, true, "amqp:resource-deleted", "queue was deleted");
            session.links.remove(handle);
        }
    }

    private int framePayload() {
        return Math.max(512, maxFrame) - 8 - 48;
    }

    private int framesFor(int messageLength) {
        return Math.max(1, (messageLength + framePayload() - 1) / framePayload());
    }

    private void sendTransfer(Session s, Link l, long deliveryId, byte[] tag, boolean settled, byte[] message) {
        int max = framePayload();
        int off = 0;
        boolean first = true;
        do {
            int n = Math.min(max, message.length - off);
            boolean more = off + n < message.length;
            Amqp10Codec.Writer w = new Amqp10Codec.Writer();
            byte[] head = first
                    ? perf(TRANSFER, u32(l.handle), u32(deliveryId), tag, u32(0), settled, more)
                    : perf(TRANSFER, u32(l.handle), null, null, null, settled, more);
            w.raw(head);
            w.raw(message, off, n);
            send(s.channel, w.toBytes());
            off += n;
            first = false;
        } while (off < message.length);
    }

    // ------------------------------------------------------------------------------------------ loop

    private void loop() throws IOException {
        while (!socket.isClosed() && !closing) {
            Frame f = readFrame();
            if (f.payload().length == 0) {
                continue;               // heartbeat
            }
            Amqp10Codec.Reader r = new Amqp10Codec.Reader(f.payload());
            Object v = r.read();
            if (!(v instanceof Described d)) {
                throw new Amqp10Codec.AmqpFormatException("expected a performative");
            }
            List<?> fl = fields(d);
            long code = d.code();
            if (code == OPEN) {
                onOpen(fl);
            } else if (code == CLOSE) {
                send(0, perf(CLOSE));
                closing = true;
                for (Session ss : new ArrayList<>(sessions.values())) {
                    endSession(ss);              // unsettled deliveries go back to their queues at once
                }
                sessions.clear();
                try {
                    socket.setSoTimeout(2000);
                    while (in.read() >= 0) {
                        // the peer closes the socket after close/close
                    }
                } catch (IOException ignored) {
                    // timeout or reset
                }
                return;
            } else if (code == BEGIN) {
                onBegin(f.channel(), fl);
            } else {
                Session s = sessions.get(f.channel());
                if (s == null) {
                    continue;                // a frame for a session that was never begun is ignored
                }
                byte[] rest = java.util.Arrays.copyOfRange(f.payload(), r.p, f.payload().length);
                long t0 = System.nanoTime();
                try {
                    if (code == ATTACH) {
                        onAttach(s, fl);
                    } else if (code == FLOW) {
                        onFlow(s, fl);
                    } else if (code == TRANSFER) {
                        onTransfer(s, fl, rest);
                    } else if (code == DISPOSITION) {
                        onDisposition(s, fl);
                    } else if (code == DETACH) {
                        onDetach(s, fl);
                    } else if (code == END) {
                        endSession(s);
                        sessions.remove(f.channel());
                        send(s.channel, perf(END));
                    }
                } finally {
                    server.record("amqp10." + name(code), code == TRANSFER || code == DISPOSITION, System.nanoTime() - t0);
                }
            }
        }
    }

    private static String name(long code) {
        return code == ATTACH ? "attach" : code == FLOW ? "flow" : code == TRANSFER ? "transfer" : code == DISPOSITION ? "disposition" : code == DETACH ? "detach"
                : code == END ? "end" : "frame";
    }

    private void onOpen(List<?> l) {
        String host = Amqp10Codec.asString(field(l, 1));
        if (host != null && host.startsWith("vhost:")) {
            vhost = host.substring("vhost:".length());
        }
        long peerMax = Amqp10Codec.asLong(field(l, 2), 0);
        if (peerMax > 0) {
            maxFrame = (int) Math.min(peerMax, 131072);        // what we may send; we accept up to 131072
        }
        long channelMax = Amqp10Codec.asLong(field(l, 3), 65535);
        peerIdleMs = Amqp10Codec.asLong(field(l, 4), 0);
        if (!br.cfg.vhostAllowed(vhost)) {
            sendClose("amqp:not-allowed", "vhost " + vhost + " not found");
            return;
        }
        br.ensureVhost(vhost);
        final int mf = 131072;
        AMap props = new AMap();
        props.put(new Sym("node"), "warp@" + br.nodeId);
        props.put(new Sym("cluster_name"), "warp@" + br.nodeId);
        props.put(new Sym("copyright"), "Warp");
        props.put(new Sym("information"), "Warp amqpwire: AMQP 1.0 on Postgres");
        props.put(new Sym("platform"), "Java");
        props.put(new Sym("product"), br.cfg.productName);
        props.put(new Sym("version"), "1.0");
        send(0, perf(OPEN, "warp@" + br.nodeId, null, u32(mf), new Amqp10Codec.U16(63), u32(30_000), null, null,
                new Amqp10Codec.SymArr(List.of("LINK_PAIR_V1_0", "ANONYMOUS-RELAY")), null, props));
        try {
            socket.setSoTimeout(90_000);
        } catch (IOException ignored) {
            // socket closing
        }
        if (peerIdleMs > 0) {
            long every = Math.max(200, peerIdleMs / 2);
            hbTask = br.timers().scheduleWithFixedDelay(this::sendHeartbeat, every, every, TimeUnit.MILLISECONDS);
        }
    }

    private void sendHeartbeat() {
        if (closing || System.currentTimeMillis() - lastWrite < peerIdleMs / 2) {
            return;
        }
        try {
            synchronized (writeLock) {
                writeFrame(0, 0, new byte[0]);
                out.flush();
                lastWrite = System.currentTimeMillis();
            }
        } catch (IOException e) {
            abort();
        }
    }

    private void sendClose(String condition, String description) {
        closing = true;
        send(0, perf(CLOSE, error(condition, description)));
    }

    private void onBegin(int channel, List<?> l) {
        Session s = new Session(channel);
        s.nextIncomingId = Amqp10Codec.asLong(field(l, 1), 0);
        s.remoteNextIncomingId = 0;
        s.remoteIncomingWindow = Amqp10Codec.asLong(field(l, 2), 2048);
        sessions.put(channel, s);
        send(channel, perf(BEGIN, new Amqp10Codec.U16(channel), u32(0), u32(SESSION_WINDOW), u32(4294967295L), u32(255)));
    }

    // ------------------------------------------------------------------------------------------ attach

    /** A parsed link address: queue, or exchange + routing key (null = take it from the message subject), or anonymous; {@code error} when refused. */
    private record Address(String queue, String exchange, String key, boolean anonymous, String error) {
    }

    private static String dec(String s) {
        return URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    /** RabbitMQ's refusal text: an Erlang term, wrapped after the first comma once the line would pass 80 columns. */
    private static String v1Refused(String a) {
        String head = "{amqp_address_v1_not_permitted,";
        String tail = "{utf8,<<\"" + a + "\">>}}";
        return "Attach refused: " + (16 + head.length() + tail.length() > 80 ? head + "\n" + " ".repeat(20) + tail : head + tail);
    }

    /** Only the v2 address forms exist: {@code /queues/<q>}, {@code /exchanges/<x>[/<key>]}, or none (anonymous relay). */
    private static Address parseAddress(String a, boolean receiver) {
        if (a == null || a.isEmpty()) {
            return new Address(null, null, null, true, null);
        }
        if (a.startsWith("/queues/")) {
            String q = dec(a.substring("/queues/".length()));
            return q.isEmpty() ? new Address(null, null, null, false, "Attach refused: {bad_address_string,<<\"" + a + "\">>}")
                    : new Address(q, null, null, false, null);
        }
        if (!receiver && a.startsWith("/exchanges/")) {
            String rest = a.substring("/exchanges/".length());
            int slash = rest.indexOf('/');
            String x = dec(slash < 0 ? rest : rest.substring(0, slash));
            String k = slash < 0 ? null : dec(rest.substring(slash + 1));
            if (x.isEmpty()) {
                return new Address(null, null, null, false, "Attach refused: {bad_address_string,<<\"" + a + "\">>}");
            }
            return new Address(null, x, k, false, null);
        }
        return new Address(null, null, null, false, v1Refused(a));
    }

    private void onAttach(Session s, List<?> l) {
        String name = Amqp10Codec.asString(field(l, 0));
        long handle = Amqp10Codec.asLong(field(l, 1), 0);
        boolean clientReceives = Amqp10Codec.asBool(field(l, 2), false);
        int snd = (int) Amqp10Codec.asLong(field(l, 3), 2);
        int rcv = (int) Amqp10Codec.asLong(field(l, 4), 0);
        List<?> source = fields(field(l, 5));
        List<?> target = fields(field(l, 6));
        if (s.links.containsKey(handle)) {
            endSessionWithError(s, "amqp:session:handle-in-use", "handle " + handle + " is already associated with a link");
            return;
        }
        Link link = new Link(s, name, handle, clientReceives, snd, rcv);
        s.links.put(handle, link);
        if (clientReceives && rcv == 1) {
            refuse(s, link, "amqp:not-implemented", "rcv-settle-mode second not supported", true);
            return;
        }
        if (clientReceives) {
            attachReceiver(s, link, source);
        } else {
            attachSender(s, link, target);
        }
    }

    private void refuse(Session s, Link link, String condition, String description, boolean clientReceives) {
        // the attach is answered first (with an empty terminus), then the link is closed with the error
        if (clientReceives) {
            send(s.channel, perf(ATTACH, link.name, u32(link.handle), false, new Amqp10Codec.U8(link.sndSettleMode), new Amqp10Codec.U8(link.rcvSettleMode), null,
                    described(TARGET), null, null, u32(0)));
        } else {
            send(s.channel, perf(ATTACH, link.name, u32(link.handle), true, new Amqp10Codec.U8(link.sndSettleMode), new Amqp10Codec.U8(link.rcvSettleMode),
                    described(SOURCE)));
        }
        sendDetach(s, link, true, condition, description);
        s.links.remove(link.handle);
    }

    private void attachSender(Session s, Link link, List<?> target) {
        String addr = Amqp10Codec.asString(field(target, 0));
        if (Amqp10Codec.asBool(field(target, 4), false) || (target.isEmpty() && addr == null && false)) {
            refuse(s, link, "amqp:not-implemented", "dynamic sender links are not supported", false);
            return;
        }
        Address a = parseAddress(addr, false);
        if (a.error() != null) {
            refuse(s, link, "amqp:invalid-field", a.error(), false);
            return;
        }
        try {
            if (a.queue() != null) {
                var q = br.store.queue(vhost, a.queue());
                if (q == null) {
                    refuse(s, link, "amqp:not-found", "no queue '" + a.queue() + "' in vhost '" + vhost + "'", false);
                    return;
                }
                link.exchange = "";
                link.routingKey = a.queue();
            } else if (a.exchange() != null) {
                if (!a.exchange().isEmpty() && br.exchange(vhost, a.exchange()) == null) {
                    refuse(s, link, "amqp:not-found", "no exchange '" + a.exchange() + "' in vhost '" + vhost + "'", false);
                    return;
                }
                link.exchange = a.exchange();
                link.routingKey = a.key();
            } else {
                link.anonymous = true;
            }
        } catch (AmqpException e) {
            refuse(s, link, e.code == 405 ? "amqp:resource-locked" : "amqp:not-found", e.text.substring(e.text.indexOf(" - ") + 3), false);
            return;
        }
        link.address = addr;
        link.grantedCredit = LINK_CREDIT;
        send(s.channel, perf(ATTACH, link.name, u32(link.handle), true, new Amqp10Codec.U8(link.sndSettleMode), new Amqp10Codec.U8(link.rcvSettleMode),
                described(SOURCE), described(TARGET, addr), null, null, null, new Amqp10Codec.U64(MAX_MESSAGE_SIZE)));
        sendFlow(s, link);
    }

    private void sendFlow(Session s, Link link) {
        sendFlow(s, link, false);
    }

    /** {@code echo}: the answer to a flow with echo=true, which RabbitMQ sends with available/drain/echo spelled out. */
    private void sendFlow(Session s, Link link, boolean echo) {
        synchronized (s.lock) {
            long dc = link.serverSends ? link.deliveryCount : link.incomingCount;
            long cr = link.serverSends ? link.credit : link.grantedCredit;
            if (echo) {
                send(s.channel, perf(FLOW, u32(s.nextIncomingId), u32(s.incomingWindow), u32(s.nextOutgoingId), u32(4294967295L), u32(link.handle), u32(dc), u32(cr),
                        u32(0), Boolean.FALSE, Boolean.FALSE));
            } else {
                send(s.channel, perf(FLOW, u32(s.nextIncomingId), u32(s.incomingWindow), u32(s.nextOutgoingId), u32(4294967295L), u32(link.handle), u32(dc), u32(cr)));
            }
        }
    }

    private void attachReceiver(Session s, Link link, List<?> source) {
        String addr = Amqp10Codec.asString(field(source, 0));
        boolean dynamic = Amqp10Codec.asBool(field(source, 4), false);
        if (dynamic) {
            // RabbitMQ 4 has no dynamic sources; its refusal prints the source record as an Erlang term
            String pad = " ".repeat(34);
            refuse(s, link, "amqp:not-implemented", "Dynamic source not supported: {'v1_0.source',undefined,undefined,undefined,\n" + pad
                    + "undefined,true,undefined,undefined,\n" + pad + "undefined,undefined,undefined,undefined}", true);
            return;
        }
        String queue;
        try {
            Address a = parseAddress(addr, true);
            if (a.error() != null || a.queue() == null) {
                refuse(s, link, "amqp:invalid-field", a.error() != null ? a.error() : "Attach refused: missing source address", true);
                return;
            }
            queue = a.queue();
            link.address = addr;
            boolean preSettled = link.sndSettleMode == 1;
            Consumer c = br.addConsumer(link, vhost, queue, "amqp10-" + link.handle + "-" + link.name, preSettled, false, 0, new LinkedHashMap<>(), connId);
            link.consumer = c;
        } catch (AmqpException e) {
            String why = e.text.substring(e.text.indexOf(" - ") + 3);
            if (why.contains(". It could be originally declared")) {
                why = why.substring(0, why.indexOf(". It could be originally declared"));
            }
            refuse(s, link, e.code == 405 ? "amqp:resource-locked" : e.code == 403 ? "amqp:unauthorized-access" : "amqp:not-found", why, true);
            return;
        }
        final String finalAddr = addr;
        send(s.channel, perf(ATTACH, link.name, u32(link.handle), false, new Amqp10Codec.U8(link.sndSettleMode), new Amqp10Codec.U8(link.rcvSettleMode),
                described(SOURCE, finalAddr, null, null, null, null, null, new Sym("move"), null, described(RELEASED),
                        new Amqp10Codec.SymArr(List.of("amqp:accepted:list", "amqp:rejected:list", "amqp:released:list", "amqp:modified:list"))),
                null, null, null, u32(0)));
        link.consumer.active = true;
    }

    private final List<AmqpBroker.QKey> dynQueues = new ArrayList<>();

    private void sendDetach(Session s, Link link, boolean closed, String condition, String description) {
        send(s.channel, perf(DETACH, u32(link.handle), closed, condition == null ? null : error(condition, description)));
    }

    // ------------------------------------------------------------------------------------------ flow

    private void onFlow(Session s, List<?> l) {
        long nextIncoming = Amqp10Codec.asLong(field(l, 0), -1);
        long incomingWindow = Amqp10Codec.asLong(field(l, 1), 0);
        synchronized (s.lock) {
            if (nextIncoming > s.nextOutgoingId) {
                // RabbitMQ's wording
                endSessionWithError(s, "amqp:session:window-violation", "next-incoming-id from FLOW (" + nextIncoming + ") leads next-outgoing-id ("
                        + s.nextOutgoingId + ")");
                return;
            }
            long peerNextOutgoing = Amqp10Codec.asLong(field(l, 2), -1);
            if (peerNextOutgoing >= 0) {
                s.nextIncomingId = peerNextOutgoing;
            }
            if (nextIncoming >= 0) {
                s.remoteNextIncomingId = nextIncoming;
                s.remoteIncomingWindow = nextIncoming + incomingWindow - s.nextOutgoingId;
            }
        }
        Object h = field(l, 4);
        if (h == null) {
            wakeSession(s);
            return;
        }
        Link link = s.links.get(Amqp10Codec.asLong(h, -1));
        if (link == null) {
            endSessionWithError(s, "amqp:session:unattached-handle", "Unattached link handle: " + Amqp10Codec.asLong(h, -1));
            return;
        }
        boolean drain = Amqp10Codec.asBool(field(l, 8), false);
        boolean echo = Amqp10Codec.asBool(field(l, 9), false);
        if (link.serverSends) {
            long peerDc = Amqp10Codec.asLong(field(l, 5), 0);
            long peerCredit = Amqp10Codec.asLong(field(l, 6), 0);
            synchronized (s.lock) {
                link.credit = Math.max(0, peerDc + peerCredit - link.deliveryCount);
                link.drain = drain;
            }
            if (link.consumer != null) {
                br.signal(link.consumer.key.vhost(), link.consumer.key.name());
                if (drain) {
                    br.dispatchAndWait(link.consumer.key.vhost(), link.consumer.key.name(), 2000);
                    synchronized (s.lock) {
                        link.deliveryCount += link.credit;
                        link.credit = 0;
                        link.drain = false;
                    }
                    sendFlowWithDrain(s, link);
                    return;
                }
            }
            if (echo) {
                sendFlow(s, link, true);
            }
        } else if (echo) {
            sendFlow(s, link, true);
        }
    }

    private void sendFlowWithDrain(Session s, Link link) {
        synchronized (s.lock) {
            send(s.channel, perf(FLOW, u32(s.nextIncomingId), u32(s.incomingWindow), u32(s.nextOutgoingId), u32(4294967295L), u32(link.handle), u32(link.deliveryCount),
                    u32(0), u32(0), Boolean.TRUE, Boolean.FALSE));
        }
    }

    private void wakeSession(Session s) {
        for (Link l : new ArrayList<>(s.links.values())) {
            if (l.consumer != null) {
                br.signal(l.consumer.key.vhost(), l.consumer.key.name());
            }
        }
    }

    // ------------------------------------------------------------------------------------------ transfer (client publishes)

    private void onTransfer(Session s, List<?> l, byte[] payload) {
        Link link = s.links.get(Amqp10Codec.asLong(field(l, 0), -1));
        if (link == null || link.serverSends) {
            endSessionWithError(s, "amqp:session:unattached-handle", "Unknown link handle: " + Amqp10Codec.asLong(field(l, 0), -1));
            return;
        }
        s.nextIncomingId++;
        s.incomingWindow = Math.max(0, s.incomingWindow - 1);
        boolean more = Amqp10Codec.asBool(field(l, 5), false);
        boolean aborted = Amqp10Codec.asBool(field(l, 9), false);
        boolean settled = Amqp10Codec.asBool(field(l, 4), false);
        Object idObj = field(l, 1);
        if (link.partial == null) {
            link.partial = new ByteArrayOutputStream();
            link.partialId = Amqp10Codec.asLong(idObj, -1);
            link.partialSettled = settled;
        }
        if (aborted) {
            link.partial = null;
            return;
        }
        link.partial.write(payload, 0, payload.length);
        if (link.partial.size() > MAX_MESSAGE_SIZE) {
            link.partial = null;
            sendDetach(s, link, true, "amqp:link:message-size-exceeded", "message size exceeds the maximum " + MAX_MESSAGE_SIZE);
            s.links.remove(link.handle);
            return;
        }
        if (more) {
            return;
        }
        byte[] msg = link.partial.toByteArray();
        long id = link.partialId;
        boolean preSettled = link.partialSettled || settled;
        link.partial = null;
        link.incomingCount++;
        link.grantedCredit = Math.max(0, link.grantedCredit - 1);
        if (link.sndSettleMode == 0 && preSettled) {
            sendDetach(s, link, true, "amqp:invalid-field", "sender settle mode is 'unsettled' but transfer settled flag is interpreted as being 'true'");
            s.links.remove(link.handle);
            return;
        }
        if (link.sndSettleMode == 1 && !preSettled) {
            sendDetach(s, link, true, "amqp:invalid-field", "sender settle mode is 'settled' but transfer settled flag is interpreted as being 'false'");
            s.links.remove(link.handle);
            return;
        }
        Amqp10Message parsed;
        try {
            parsed = Amqp10Message.parse(msg);
        } catch (Amqp10Codec.AmqpFormatException e) {
            sendDetach(s, link, true, "amqp:decode-error", e.getMessage());
            s.links.remove(link.handle);
            return;
        }
        if (parsed != null) {
            String uid = parsed.toProps().userId;
            if (uid != null && !uid.equals(user)) {
                sendDetach(s, link, true, "amqp:unauthorized-access", "user_id property set to '" + uid + "' but authenticated user was '" + user + "'");
                s.links.remove(link.handle);
                return;
            }
        }
        Described state = publish(link, msg);
        if (!preSettled) {
            send(s.channel, perf(DISPOSITION, Boolean.TRUE, u32(id), null, Boolean.TRUE, state));
        }
        if (pendingDetach != null) {
            String[] d = pendingDetach;
            pendingDetach = null;
            sendDetach(s, link, true, d[0], d[1]);
            s.links.remove(link.handle);
            return;
        }
        if (link.grantedCredit < LINK_CREDIT / 2 || s.incomingWindow < SESSION_WINDOW / 2) {
            link.grantedCredit = LINK_CREDIT;
            s.incomingWindow = SESSION_WINDOW;
            sendFlow(s, link);
        }
    }

    /** Publishes one AMQP 1.0 message; returns the outcome descriptor (accepted / released / rejected). */
    /** Set by {@link #publish} when the link must be closed after the outcome was sent (condition, description). */
    private String[] pendingDetach;

    private Described publish(Link link, byte[] wire) {
        try {
            Amqp10Message m = Amqp10Message.parse(wire);
            AmqpProps props = m.toProps();
            byte[] body = m.toBody();
            String exchange = link.exchange;
            String key = link.routingKey;
            if (link.anonymous) {
                Amqp10Message.Section pr = m.first(Amqp10Message.PROPERTIES);
                String to = pr == null ? null : Amqp10Codec.asString(field(fields(pr.value()), 2));
                if (to == null) {
                    return rejected("amqp:precondition-failed", "anonymous terminus requires 'to' address to be set");
                }
                Address a = parseAddress(to, false);
                if (a.error() != null || a.anonymous()) {
                    return rejected("amqp:precondition-failed", "bad 'to' address string: " + to);
                }
                if (a.queue() != null) {
                    exchange = "";
                    key = a.queue();
                } else {
                    exchange = a.exchange();
                    key = a.key();
                }
            }
            if (key == null) {
                key = "";
            }
            AmqpBroker.PubResult r = br.publish(vhost, exchange, key, props.toBytes(), props, body, null, false, null, m.rawWithoutHeader());
            if (r.rejected()) {
                AMap info = new AMap();
                info.put(new Sym("queue"), r.rejectedQueue());
                info.put(new Sym("reason"), new Sym("maxlen"));
                return described(REJECTED, new Described(new Amqp10Codec.U64(ERROR), new ArrayList<Object>(java.util.Arrays.asList(new Sym("amqp:resource-limit-exceeded"),
                        "queue '" + r.rejectedQueue() + "' exceeded maximum length", info))));
            }
            return described(r.routed() > 0 ? ACCEPTED : RELEASED);
        } catch (AmqpException e) {
            String why = e.text.substring(e.text.indexOf(" - ") + 3);
            if (e.code == 404) {
                if (why.startsWith("no exchange")) {
                    pendingDetach = new String[] {"amqp:not-found", why};       // the target of the link is gone
                }
                return described(RELEASED);
            }
            return rejected("amqp:precondition-failed", why);
        } catch (Amqp10Codec.AmqpFormatException e) {
            return rejected("amqp:decode-error", e.getMessage());
        }
    }

    private static Described rejected(String condition, String description) {
        return described(REJECTED, error(condition, description));
    }

    // ------------------------------------------------------------------------------------------ disposition (client settles what we sent)

    private void onDisposition(Session s, List<?> l) {
        boolean clientIsReceiver = Amqp10Codec.asBool(field(l, 0), false);
        if (!clientIsReceiver) {
            return;
        }
        long first = Amqp10Codec.asLong(field(l, 1), 0);
        long last = Amqp10Codec.asLong(field(l, 2), first);
        boolean settled = Amqp10Codec.asBool(field(l, 3), false);
        Object stateObj = field(l, 4);
        long state = stateObj instanceof Described d ? d.code() : -1;
        List<?> sf = fields(stateObj);
        Map<AmqpBroker.QKey, List<Unsettled>> acks = new LinkedHashMap<>();
        Map<AmqpBroker.QKey, List<Unsettled>> releases = new LinkedHashMap<>();
        Map<AmqpBroker.QKey, List<Unsettled>> failed = new LinkedHashMap<>();
        Map<AmqpBroker.QKey, List<Unsettled>> rejects = new LinkedHashMap<>();
        synchronized (s.lock) {
            for (long id = first; id <= last; id++) {
                Unsettled u = state < 0 ? s.unsettled.get(id) : s.unsettled.remove(id);
                if (u == null) {
                    continue;
                }
                AmqpBroker.QKey k = new AmqpBroker.QKey(u.vhost(), u.queue());
                if (state == ACCEPTED) {
                    acks.computeIfAbsent(k, x -> new ArrayList<>()).add(u);
                } else if (state == REJECTED) {
                    rejects.computeIfAbsent(k, x -> new ArrayList<>()).add(u);
                } else if (state == MODIFIED) {
                    boolean deliveryFailed = Amqp10Codec.asBool(field(sf, 0), false);
                    boolean undeliverable = Amqp10Codec.asBool(field(sf, 1), false);
                    if (undeliverable) {
                        rejects.computeIfAbsent(k, x -> new ArrayList<>()).add(u);
                    } else if (deliveryFailed) {
                        failed.computeIfAbsent(k, x -> new ArrayList<>()).add(u);
                    } else {
                        releases.computeIfAbsent(k, x -> new ArrayList<>()).add(u);
                    }
                } else if (state == RELEASED) {
                    releases.computeIfAbsent(k, x -> new ArrayList<>()).add(u);
                } else if (state < 0 && settled) {
                    s.unsettled.remove(id);
                    releases.computeIfAbsent(k, x -> new ArrayList<>()).add(u);     // settled without an outcome: the default outcome, released
                }
            }
        }
        settle(acks, releases, failed, rejects);
        if (settled || state >= 0) {
            synchronized (s.lock) {
                // window space frees up as the client settles
            }
        }
    }

    private void settle(Map<AmqpBroker.QKey, List<Unsettled>> acks, Map<AmqpBroker.QKey, List<Unsettled>> releases, Map<AmqpBroker.QKey, List<Unsettled>> failed,
            Map<AmqpBroker.QKey, List<Unsettled>> rejects) {
        Map<AmqpBroker.QKey, Boolean> wake = new LinkedHashMap<>();
        acks.forEach((k, v) -> {
            br.store.ack(k.vhost(), k.name(), seqs(v));
            wake.put(k, true);
        });
        releases.forEach((k, v) -> {
            br.store.requeue(k.vhost(), k.name(), seqs(v), false);
            wake.put(k, true);
        });
        failed.forEach((k, v) -> {
            br.store.requeue(k.vhost(), k.name(), seqs(v), true);
            wake.put(k, true);
        });
        rejects.forEach((k, v) -> {
            AmqpStore.QueueDef d = br.qdef(k.vhost(), k.name());
            if (d != null && d.dlx() != null) {
                List<MsgRow> rows = new ArrayList<>();
                v.forEach(u -> rows.add(u.row()));
                br.deadLetter(d, rows, "rejected", true);
            }
            br.store.ack(k.vhost(), k.name(), seqs(v));
            wake.put(k, true);
        });
        wake.keySet().forEach(k -> br.signal(k.vhost(), k.name()));
    }

    private static List<Long> seqs(List<Unsettled> l) {
        List<Long> out = new ArrayList<>();
        l.forEach(u -> out.add(u.seq()));
        return out;
    }

    // ------------------------------------------------------------------------------------------ detach / end / cleanup

    private void onDetach(Session s, List<?> l) {
        Link link = s.links.remove(Amqp10Codec.asLong(field(l, 0), -1));
        if (link == null) {
            return;
        }
        releaseLink(s, link);
        sendDetach(s, link, true, null, null);
    }

    private void releaseLink(Session s, Link link) {
        synchronized (s.lock) {
            link.detached = true;
        }
        if (link.consumer != null) {
            br.removeConsumer(link.consumer);
            link.consumer.cancelled = true;
        }
        Map<AmqpBroker.QKey, List<Unsettled>> back = new LinkedHashMap<>();
        synchronized (s.lock) {
            var it = s.unsettled.entrySet().iterator();
            while (it.hasNext()) {
                Unsettled u = it.next().getValue();
                if (u.link() == link) {
                    back.computeIfAbsent(new AmqpBroker.QKey(u.vhost(), u.queue()), x -> new ArrayList<>()).add(u);
                    it.remove();
                }
            }
        }
        back.forEach((k, v) -> {
            br.store.requeue(k.vhost(), k.name(), seqs(v), true);         // RabbitMQ counts an abandoned delivery as a failed one
            br.signal(k.vhost(), k.name());
        });
    }

    private void endSession(Session s) {
        for (Link l : new ArrayList<>(s.links.values())) {
            releaseLink(s, l);
        }
        s.links.clear();
    }

    private void endSessionWithError(Session s, String condition, String description) {
        endSession(s);
        sessions.remove(s.channel);
        send(s.channel, perf(END, error(condition, description)));
    }

    private void cleanup() {
        closing = true;
        abort();
        if (hbTask != null) {
            hbTask.cancel(false);
        }
        for (Session s : new ArrayList<>(sessions.values())) {
            try {
                endSession(s);
            } catch (RuntimeException e) {
                log.debug("amqpwire 1.0: session cleanup failed: {}", e.toString());
            }
        }
        sessions.clear();
        for (AmqpBroker.QKey k : dynQueues) {
            try {
                var d = br.store.queue(k.vhost(), k.name());
                if (d != null && connId.equals(d.exclOwner())) {
                    br.removeQueue(k.vhost(), k.name());
                }
            } catch (RuntimeException e) {
                log.debug("amqpwire 1.0: dynamic queue cleanup failed: {}", e.toString());
            }
        }
    }
}
