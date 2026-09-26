package com.sayonora.wire.amqpwire;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One client connection: handshake (PLAIN/AMQPLAIN), tune, open, then the frame loop feeding {@link AmqpChannel}s. */
final class AmqpConnection implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AmqpConnection.class);
    private static final byte[] HEADER_0_9_1 = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};

    private final AmqpWireServer server;
    private final AmqpBroker br;
    private final Socket socket;
    private final Runnable onClose;
    private DataInputStream in;
    private OutputStream out;
    private final Object writeLock = new Object();
    private final Map<Integer, AmqpChannel> channels = new ConcurrentHashMap<>();
    final Set<AmqpBroker.QKey> exclusiveQueues = ConcurrentHashMap.newKeySet();
    final String connId;
    String vhost = "/";
    boolean clientCancelNotify;
    String user = "";
    private int frameMax;
    private int heartbeat;
    private volatile boolean closing;
    private volatile long lastWrite = System.currentTimeMillis();
    private ScheduledFuture<?> hbTask;

    AmqpConnection(AmqpWireServer server, AmqpBroker br, Socket socket, Runnable onClose) {
        this.server = server;
        this.br = br;
        this.socket = socket;
        this.onClose = onClose;
        this.connId = br.newConnId();
    }

    // ------------------------------------------------------------------------------------------ output

    private void writeFrame(int type, int channel, byte[] payload) throws IOException {
        byte[] f = new byte[payload.length + 8];
        f[0] = (byte) type;
        f[1] = (byte) (channel >> 8);
        f[2] = (byte) channel;
        f[3] = (byte) (payload.length >> 24);
        f[4] = (byte) (payload.length >> 16);
        f[5] = (byte) (payload.length >> 8);
        f[6] = (byte) payload.length;
        System.arraycopy(payload, 0, f, 7, payload.length);
        f[f.length - 1] = (byte) 0xCE;
        out.write(f);
    }

    private byte[] methodPayload(int cls, int mth, AmqpCodec.Writer args) {
        AmqpCodec.Writer w = new AmqpCodec.Writer();
        w.u16(cls).u16(mth);
        if (args != null) {
            w.raw(args.buf, 0, args.n);
        }
        return w.toBytes();
    }

    void sendMethod(int channel, int cls, int mth, AmqpCodec.Writer args) {
        try {
            synchronized (writeLock) {
                writeFrame(1, channel, methodPayload(cls, mth, args));
                out.flush();
                lastWrite = System.currentTimeMillis();
            }
        } catch (IOException e) {
            abort();
        }
    }

    /** A method followed by its content header and body frames, written atomically. */
    void sendContent(int channel, int cls, int mth, AmqpCodec.Writer args, byte[] propsRaw, byte[] body) {
        try {
            synchronized (writeLock) {
                writeFrame(1, channel, methodPayload(cls, mth, args));
                AmqpCodec.Writer h = new AmqpCodec.Writer();
                h.u16(cls == 60 ? 60 : 60).u16(0).i64(body.length).raw(propsRaw);
                writeFrame(2, channel, h.toBytes());
                int max = frameMax - 8;
                for (int off = 0; off < body.length; off += max) {
                    int n = Math.min(max, body.length - off);
                    byte[] part = new byte[n];
                    System.arraycopy(body, off, part, 0, n);
                    writeFrame(3, channel, part);
                }
                out.flush();
                lastWrite = System.currentTimeMillis();
            }
        } catch (IOException e) {
            abort();
        }
    }

    private void abort() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // gone
        }
    }

    // ------------------------------------------------------------------------------------------ input

    private record Frame(int type, int channel, byte[] payload) {
    }

    private Frame readFrame() throws IOException {
        int type = in.read();
        if (type < 0) {
            throw new EOFException();
        }
        int ch = in.readUnsignedShort();
        long size = in.readInt() & 0xffffffffL;
        if (frameMax > 0 && size > frameMax) {
            throw AmqpException.conn(501, "FRAME_ERROR - type " + type + ", frame payload of " + size + " bytes exceeds the negotiated frame_max " + frameMax);
        }
        byte[] payload = new byte[(int) size];
        in.readFully(payload);
        int end = in.read();
        if (end != 0xCE) {
            throw AmqpException.conn(501, "FRAME_ERROR - " + frameDesc(type, payload) + ": {invalid_frame_end_marker," + end + "}").at(0, 0);
        }
        return new Frame(type, ch, payload);
    }

    @Override
    public void run() {
        try {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(30_000);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536));
            out = new BufferedOutputStream(socket.getOutputStream(), 65536);
            byte[] hdr = new byte[8];
            in.readFully(hdr);
            if (hdr[0] == 'A' && hdr[1] == 'M' && hdr[2] == 'Q' && hdr[3] == 'P' && (hdr[4] == 0 || hdr[4] == 3) && hdr[5] == 1 && hdr[6] == 0 && hdr[7] == 0) {
                // AMQP 1.0 (plain or after SASL) on the same port
                new Amqp10Connection(server, br, socket, in, out, connId).run(hdr);
                return;
            }
            if (!java.util.Arrays.equals(hdr, HEADER_0_9_1)) {
                synchronized (writeLock) {
                    out.write(HEADER_0_9_1);
                    out.flush();
                }
                return;
            }
            handshake();
            loop();
        } catch (AmqpException e) {
            if (!e.silent) {
                closeWithError(e, Math.max(e.classId, 0), Math.max(e.methodId, 0));
            }
        } catch (EOFException | SocketTimeoutException e) {
            // client gone or idle beyond the heartbeat allowance
        } catch (IOException e) {
            log.debug("amqpwire: connection {} ended: {}", connId, e.toString());
        } catch (RuntimeException e) {
            log.warn("amqpwire: connection {} failed", connId, e);
        } finally {
            cleanup();
        }
    }

    private void expect(Frame f, int cls, int mth, String what) {
        if (f.type() != 1 || f.payload().length < 4) {
            throw AmqpException.conn(503, "COMMAND_INVALID - expected '" + what + "'");
        }
        AmqpCodec.Reader r = new AmqpCodec.Reader(f.payload());
        if (f.channel() != 0 || r.u16() != cls || r.u16() != mth) {
            throw AmqpException.conn(503, "COMMAND_INVALID - expected '" + what + "'");
        }
    }

    private void handshake() throws IOException {
        AmqpCodec.Writer caps = new AmqpCodec.Writer();
        Map<String, Object> capMap = new LinkedHashMap<>();
        for (String c : new String[] {"publisher_confirms", "exchange_exchange_bindings", "basic.nack", "consumer_cancel_notify", "connection.blocked",
            "consumer_priorities", "authentication_failure_close", "per_consumer_qos", "direct_reply_to"}) {
            capMap.put(c, Boolean.TRUE);
        }
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("capabilities", capMap);
        props.put("product", br.cfg.productName);
        props.put("platform", "Java");
        props.put("version", "1.0");
        props.put("information", "Warp amqpwire: AMQP 0-9-1 on Postgres");
        AmqpCodec.Writer start = new AmqpCodec.Writer();
        start.u8(0).u8(9).table(props).longStr("PLAIN AMQPLAIN").longStr("en_US");
        sendMethod(0, 10, 10, start);

        Frame f = readFrame();
        expect(f, 10, 11, "connection.start_ok");
        AmqpCodec.Reader r = new AmqpCodec.Reader(f.payload(), 4, f.payload().length);
        Map<String, Object> clientProps = r.table();
        String mech = r.shortStr();
        byte[] resp = r.longStrBytes();
        r.shortStr();
        if (clientProps.get("capabilities") instanceof Map<?, ?> cm) {
            clientCancelNotify = Boolean.TRUE.equals(cm.get("consumer_cancel_notify"));
        }
        authenticate(mech, resp, clientProps);

        frameMax = br.cfg.frameMax;
        AmqpCodec.Writer tune = new AmqpCodec.Writer();
        tune.u16(br.cfg.channelMax).u32(br.cfg.frameMax).u16(br.cfg.heartbeat);
        sendMethod(0, 10, 30, tune);
        f = readFrame();
        expect(f, 10, 31, "connection.tune_ok");
        r = new AmqpCodec.Reader(f.payload(), 4, f.payload().length);
        int chMax = r.u16();
        long fMax = r.u32();
        int hb = r.u16();
        if (fMax != 0) {
            frameMax = (int) Math.min(fMax, br.cfg.frameMax);
        }
        if (fMax != 0 && fMax < 8192) {
            throw AmqpException.silentClose(); // RabbitMQ drops such a connection without a word
        }
        heartbeat = hb;
        if (hb > 0) {
            socket.setSoTimeout(hb * 2 * 1000 + 500);
            long every = Math.max(1, hb / 2);
            hbTask = br.timers().scheduleWithFixedDelay(this::sendHeartbeat, every, every, TimeUnit.SECONDS);
        } else {
            socket.setSoTimeout(0);
        }
        f = readFrame();
        expect(f, 10, 40, "connection.open");
        r = new AmqpCodec.Reader(f.payload(), 4, f.payload().length);
        String vh = r.shortStr();
        if (!br.cfg.vhostAllowed(vh)) {
            throw AmqpException.notAllowed("vhost " + vh + " not found").at(10, 40);
        }
        vhost = vh;
        br.ensureVhost(vhost);
        sendMethod(0, 10, 41, new AmqpCodec.Writer().shortStr(""));
    }

    private void sendHeartbeat() {
        if (closing || System.currentTimeMillis() - lastWrite < heartbeat * 500L) {
            return;
        }
        try {
            synchronized (writeLock) {
                writeFrame(8, 0, new byte[0]);
                out.flush();
                lastWrite = System.currentTimeMillis();
            }
        } catch (IOException e) {
            abort();
        }
    }

    private void authenticate(String mech, byte[] resp, Map<String, Object> clientProps) {
        String u = null;
        byte[] p = null;
        if (mech.equals("PLAIN")) {
            int a = indexOf(resp, 0, 0);
            int b = a < 0 ? -1 : indexOf(resp, 0, a + 1);
            if (a >= 0 && b >= 0) {
                u = new String(resp, a + 1, b - a - 1, StandardCharsets.UTF_8);
                p = java.util.Arrays.copyOfRange(resp, b + 1, resp.length);
            }
        } else if (mech.equals("AMQPLAIN")) {
            AmqpCodec.Reader t = new AmqpCodec.Reader(prefixLen(resp));
            Map<String, Object> m = t.table();
            u = m.get("LOGIN") instanceof String s ? s : null;
            p = m.get("PASSWORD") instanceof String s ? s.getBytes(StandardCharsets.UTF_8) : null;
        } else {
            throw AmqpException.silentClose(); // RabbitMQ closes the socket on an unknown mechanism
        }
        if (!server.authenticate(u, p)) {
            throw new AmqpException(true, 403, "ACCESS_REFUSED", "Login was refused using authentication mechanism " + mech
                    + ". For details see the broker logfile.");
        }
        user = u == null ? "" : u;
    }

    /** RabbitMQ prints frame payloads as Erlang binaries: {@code <<"text">>} when printable, else {@code <<1,2,3>>}. */
    static String erlBin(byte[] payload) {
        int n = Math.min(payload.length, 16);
        boolean printable = n > 0;
        for (int i = 0; i < n; i++) {
            int c = payload[i] & 0xff;
            printable &= c >= 32 && c < 127 && c != '"' && c != '\\';
        }
        StringBuilder sb = new StringBuilder("<<");
        if (printable) {
            sb.append('"').append(new String(payload, 0, n, StandardCharsets.ISO_8859_1)).append('"');
        } else {
            for (int i = 0; i < n; i++) {
                sb.append(i > 0 ? "," : "").append(payload[i] & 0xff);
            }
        }
        return sb.append(">>").toString();
    }

    static String frameDesc(int type, byte[] payload) {
        return "type " + type + (payload.length <= 16 ? ", all octets = " : ", first 16 bytes of payload: ") + erlBin(payload);
    }

    private static byte[] prefixLen(byte[] b) {
        byte[] r = new byte[b.length + 4];
        r[0] = (byte) (b.length >> 24);
        r[1] = (byte) (b.length >> 16);
        r[2] = (byte) (b.length >> 8);
        r[3] = (byte) b.length;
        System.arraycopy(b, 0, r, 4, b.length);
        return r;
    }

    private static int indexOf(byte[] a, int v, int from) {
        for (int i = from; i < a.length; i++) {
            if (a[i] == v) {
                return i;
            }
        }
        return -1;
    }

    // ------------------------------------------------------------------------------------------ frame loop

    private void loop() throws IOException {
        while (!socket.isClosed()) {
            Frame f;
            try {
                f = readFrame();
            } catch (AmqpException e) {
                closeWithError(e, 0, 0);
                return;
            }
            if (f.type() == 8) {
                continue;
            }
            if (closing) {
                // waiting for close-ok after a connection error: everything else is ignored
                if (f.type() == 1 && f.channel() == 0 && f.payload().length >= 4) {
                    AmqpCodec.Reader r = new AmqpCodec.Reader(f.payload());
                    if (r.u16() == 10 && r.u16() == 51) {
                        return;
                    }
                }
                continue;
            }
            int cls = 0;
            int mth = 0;
            try {
                if (f.type() == 1) {
                    if (f.payload().length < 4) {
                        throw AmqpException.conn(501, "FRAME_ERROR - short method frame");
                    }
                    cls = ((f.payload()[0] & 0xff) << 8) | (f.payload()[1] & 0xff);
                    mth = ((f.payload()[2] & 0xff) << 8) | (f.payload()[3] & 0xff);
                }
                if (!dispatch(f)) {
                    return;
                }
            } catch (AmqpException e) {
                if (e.silent) {
                    return;
                }
                if (e.connection) {
                    closeWithError(e, e.classId >= 0 ? e.classId : cls, e.methodId >= 0 ? e.methodId : mth);
                    return;
                } else {
                    channelError(f.channel(), e, e.classId >= 0 ? e.classId : cls, e.methodId >= 0 ? e.methodId : mth);
                }
            }
        }
    }

    /** @return false when the connection must end */
    private boolean dispatch(Frame f) {
        int chNo = f.channel();
        if (chNo == 0) {
            if (f.type() != 1) {
                throw AmqpException.conn(505, "UNEXPECTED_FRAME - " + frameDesc(f.type(), f.payload())).at(0, 0);
            }
            AmqpCodec.Reader r = new AmqpCodec.Reader(f.payload());
            int cls = r.u16();
            int mth = r.u16();
            if (cls == 10 && mth == 50) {
                sendMethod(0, 10, 51, null);
                return false;
            }
            if (cls == 10 && mth == 51) {
                return false;
            }
            throw AmqpException.conn(503, "COMMAND_INVALID - unexpected method " + cls + "." + mth + " on channel 0");
        }
        AmqpChannel ch = channels.get(chNo);
        if (f.type() == 1) {
            AmqpCodec.Reader r = new AmqpCodec.Reader(f.payload());
            int cls = r.u16();
            int mth = r.u16();
            if (ch == null) {
                if (cls == 20 && mth == 10) {
                    r.shortStr();
                    channels.put(chNo, new AmqpChannel(this, chNo, br));
                    sendMethod(chNo, 20, 11, new AmqpCodec.Writer().longStr(""));
                    return true;
                }
                throw AmqpException.conn(504, "CHANNEL_ERROR - expected 'channel.open'");
            }
            if (cls == 20 && mth == 10) {
                throw AmqpException.conn(504, "CHANNEL_ERROR - second 'channel.open' seen");
            }
            if (ch.errored) {
                if (cls == 20 && mth == 41) {
                    channels.remove(chNo);
                }
                return true;
            }
            if (cls == 20 && mth == 40) {
                ch.cleanup();
                channels.remove(chNo);
                sendMethod(chNo, 20, 41, null);
                return true;
            }
            if (cls == 20 && mth == 41) {
                channels.remove(chNo);
                return true;
            }
            long t0 = System.nanoTime();
            try {
                ch.onMethod(cls, mth, r);
            } finally {
                server.record(opName(cls, mth), isWrite(cls, mth), System.nanoTime() - t0);
            }
            return true;
        }
        if (ch == null) {
            throw AmqpException.conn(504, "CHANNEL_ERROR - expected 'channel.open'");
        }
        if (ch.errored) {
            return true;
        }
        try {
            if (f.type() == 2) {
                AmqpCodec.Reader r = new AmqpCodec.Reader(f.payload());
                int cls = r.u16();
                r.u16();
                long size = r.i64();
                byte[] props = r.bytes(r.remaining());
                long t0 = System.nanoTime();
                ch.onHeader(cls, size, props);
                if (size == 0) {
                    server.record("basic.publish", true, System.nanoTime() - t0);
                }
            } else if (f.type() == 3) {
                long t0 = System.nanoTime();
                ch.onBody(f.payload());
                if (!ch.expectingContent()) {
                    server.record("basic.publish", true, System.nanoTime() - t0);
                }
            } else {
                throw AmqpException.conn(501, "FRAME_ERROR - unknown frame type " + f.type());
            }
        } catch (AmqpException e) {
            if (e.connection) {
                throw e;
            }
            channelError(chNo, e, 60, 40);
        }
        return true;
    }

    private static boolean isWrite(int cls, int mth) {
        return !(cls == 50 && mth == 10 || cls == 40 && mth == 10 || cls == 60 && (mth == 10 || mth == 20 || mth == 30));   // declares of existing things read
    }

    private static String opName(int cls, int mth) {
        return switch (cls * 1000 + mth) {
            case 20020 -> "channel.flow";
            case 40010 -> "exchange.declare";
            case 40020 -> "exchange.delete";
            case 40030 -> "exchange.bind";
            case 40040 -> "exchange.unbind";
            case 50010 -> "queue.declare";
            case 50020 -> "queue.bind";
            case 50030 -> "queue.purge";
            case 50040 -> "queue.delete";
            case 50050 -> "queue.unbind";
            case 60010 -> "basic.qos";
            case 60020 -> "basic.consume";
            case 60030 -> "basic.cancel";
            case 60070 -> "basic.get";
            case 60080 -> "basic.ack";
            case 60090 -> "basic.reject";
            case 60100 -> "basic.recover-async";
            case 60110 -> "basic.recover";
            case 60120 -> "basic.nack";
            case 85010 -> "confirm.select";
            case 90010 -> "tx.select";
            case 90020 -> "tx.commit";
            case 90030 -> "tx.rollback";
            default -> "class" + cls + "." + mth;
        };
    }

    private void channelError(int chNo, AmqpException e, int cls, int mth) {
        AmqpChannel ch = channels.get(chNo);
        if (ch != null) {
            ch.errored = true;
            ch.cleanup();
        }
        AmqpCodec.Writer w = new AmqpCodec.Writer();
        w.u16(e.code).shortStr(e.text.length() > 255 ? e.text.substring(0, 255) : e.text).u16(cls).u16(mth);
        sendMethod(chNo, 20, 40, w);
    }

    private void closeWithError(AmqpException e, int cls, int mth) {
        if (closing) {
            return;
        }
        closing = true;
        AmqpCodec.Writer w = new AmqpCodec.Writer();
        w.u16(e.code).shortStr(e.text.length() > 255 ? e.text.substring(0, 255) : e.text).u16(cls).u16(mth);
        sendMethod(0, 10, 50, w);
        // wait briefly for close-ok (or EOF) so the client can read the reason, then drop the socket
        try {
            socket.setSoTimeout(3000);
            while (true) {
                Frame f = readFrame();
                if (f.type() == 1 && f.channel() == 0 && f.payload().length >= 4) {
                    AmqpCodec.Reader r = new AmqpCodec.Reader(f.payload());
                    if (r.u16() == 10 && r.u16() == 51) {
                        break;
                    }
                }
            }
        } catch (IOException | RuntimeException ignored) {
            // timeout or close
        }
    }

    private void cleanup() {
        closing = true;
        abort();
        if (hbTask != null) {
            hbTask.cancel(false);
        }
        for (AmqpChannel ch : channels.values()) {
            try {
                ch.cleanup();
            } catch (RuntimeException e) {
                log.debug("amqpwire: channel cleanup failed: {}", e.toString());
            }
        }
        channels.clear();
        for (AmqpBroker.QKey k : exclusiveQueues) {
            try {
                var d = br.store.queue(k.vhost(), k.name());
                if (d != null && connId.equals(d.exclOwner())) {
                    br.removeQueue(k.vhost(), k.name());
                }
            } catch (RuntimeException e) {
                log.debug("amqpwire: exclusive queue cleanup failed: {}", e.toString());
            }
        }
        exclusiveQueues.clear();
        abort();
        onClose.run();
    }
}
