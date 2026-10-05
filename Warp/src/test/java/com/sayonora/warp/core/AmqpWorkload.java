package com.sayonora.warp.core;

import com.sayonora.warp.testsupport.BrownoutHarness.Client;
import com.sayonora.warp.testsupport.BrownoutHarness.StoreConfig;
import com.sayonora.warp.testsupport.BrownoutHarness.Workload;
import com.sayonora.warp.testsupport.WarpProcess;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * AMQP 0-9-1 brownout workload with a minimal hand-written client (no RabbitMQ client library is on the classpath): a durable
 * queue, publisher confirms for writes (a write counts as acknowledged only on basic.ack), a passive queue.declare for reads,
 * and a basic.get drain afterwards to read back what is present.
 */
final class AmqpWorkload {

    private static final String QUEUE = "bo-amqp";

    private AmqpWorkload() {
    }

    /** One AMQP connection with one channel. Any protocol error or timeout throws and the harness opens a new one. */
    static final class Conn implements AutoCloseable {
        private final Socket socket = new Socket();
        private final DataInputStream in;
        private final DataOutputStream out;
        private long publishSeq;

        Conn(int port, boolean confirms) throws IOException {
            socket.connect(new InetSocketAddress("localhost", port), 5000);
            socket.setSoTimeout(10_000);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
            out.write(new byte[] {'A', 'M', 'Q', 'P', 0, 0, 9, 1});
            expect(10, 10); // connection.start
            Buf b = new Buf();
            b.u32(0); // client properties
            b.shortStr("PLAIN");
            b.longStr("\0guest\0guest");
            b.shortStr("en_US");
            method(0, 10, 11, b);
            Buf tune = expect(10, 30);
            int channelMax = tune.in.readUnsignedShort();
            int frameMax = tune.in.readInt();
            Buf ok = new Buf();
            ok.u16(channelMax);
            ok.u32(frameMax);
            ok.u16(0); // no heartbeats
            method(0, 10, 31, ok);
            Buf open = new Buf();
            open.shortStr("/");
            open.shortStr("");
            open.u8(0);
            method(0, 10, 40, open);
            expect(10, 41);
            Buf ch = new Buf();
            ch.shortStr("");
            method(1, 20, 10, ch);
            expect(20, 11);
            if (confirms) {
                Buf c = new Buf();
                c.u8(0);
                method(1, 85, 10, c);
                expect(85, 11);
            }
        }

        void declare(boolean passive) throws IOException {
            Buf b = new Buf();
            b.u16(0);
            b.shortStr(QUEUE);
            b.u8((passive ? 1 : 0) | 2); // passive, durable
            b.u32(0);
            method(1, 50, 10, b);
            expect(50, 11);
        }

        void publish(long id) throws IOException {
            Buf b = new Buf();
            b.u16(0);
            b.shortStr("");
            b.shortStr(QUEUE);
            b.u8(0);
            method(1, 60, 40, b);
            byte[] body = Long.toString(id).getBytes(StandardCharsets.US_ASCII);
            Buf h = new Buf();
            h.u16(60);
            h.u16(0);
            h.u64(body.length);
            h.u16(0x1000); // delivery-mode present
            h.u8(2); // persistent
            frame(2, 1, h.bytes());
            frame(3, 1, body);
            long tag = ++publishSeq;
            Buf r = expectAny(60, 80, 60, 120);
            long got = r.in.readLong();
            boolean nack = r.classId == 60 && r.methodId == 120;
            if (nack) {
                throw new IOException("publish " + tag + " nacked");
            }
            if (got < tag && (r.in.readUnsignedByte() & 1) == 0) {
                throw new IOException("unexpected confirm tag " + got + " for " + tag);
            }
        }

        /** Gets one message with no-ack; returns its body or null when the queue is empty. */
        String get() throws IOException {
            Buf b = new Buf();
            b.u16(0);
            b.shortStr(QUEUE);
            b.u8(1); // no-ack
            method(1, 60, 70, b);
            Buf r = expectAny(60, 71, 60, 72);
            if (r.methodId == 72) {
                return null;
            }
            Frame header = frame();
            if (header.type != 2) {
                throw new IOException("expected content header, got frame type " + header.type);
            }
            long size = new DataInputStream(new java.io.ByteArrayInputStream(header.payload, 4, 8)).readLong();
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            while (body.size() < size) {
                Frame f = frame();
                if (f.type != 3) {
                    throw new IOException("expected content body, got frame type " + f.type);
                }
                body.write(f.payload);
            }
            return body.toString(StandardCharsets.US_ASCII);
        }

        // ---- framing ----------------------------------------------------------------------------------------

        private record Frame(int type, int channel, byte[] payload) {
        }

        private static final class Buf {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            final DataOutputStream o = new DataOutputStream(bytes);
            DataInputStream in;
            int classId;
            int methodId;

            void u8(int v) throws IOException {
                o.writeByte(v);
            }

            void u16(int v) throws IOException {
                o.writeShort(v);
            }

            void u32(int v) throws IOException {
                o.writeInt(v);
            }

            void u64(long v) throws IOException {
                o.writeLong(v);
            }

            void shortStr(String s) throws IOException {
                byte[] b = s.getBytes(StandardCharsets.UTF_8);
                o.writeByte(b.length);
                o.write(b);
            }

            void longStr(String s) throws IOException {
                byte[] b = s.getBytes(StandardCharsets.UTF_8);
                o.writeInt(b.length);
                o.write(b);
            }

            byte[] bytes() {
                return bytes.toByteArray();
            }
        }

        private void frame(int type, int channel, byte[] payload) throws IOException {
            ByteArrayOutputStream f = new ByteArrayOutputStream();
            DataOutputStream d = new DataOutputStream(f);
            d.writeByte(type);
            d.writeShort(channel);
            d.writeInt(payload.length);
            d.write(payload);
            d.writeByte(0xCE);
            out.write(f.toByteArray());
            out.flush();
        }

        private void method(int channel, int cls, int mth, Buf args) throws IOException {
            Buf p = new Buf();
            p.u16(cls);
            p.u16(mth);
            p.o.write(args.bytes());
            frame(1, channel, p.bytes());
        }

        private Frame frame() throws IOException {
            while (true) {
                int type = in.readUnsignedByte();
                int channel = in.readUnsignedShort();
                byte[] payload = new byte[in.readInt()];
                in.readFully(payload);
                if (in.readUnsignedByte() != 0xCE) {
                    throw new IOException("bad frame end");
                }
                if (type != 8) { // skip heartbeats
                    return new Frame(type, channel, payload);
                }
            }
        }

        private Buf expect(int cls, int mth) throws IOException {
            return expectAny(cls, mth, -1, -1);
        }

        /** Reads the next method frame; it must be (cls1, mth1) or (cls2, mth2). A connection.close or channel.close throws. */
        private Buf expectAny(int cls1, int mth1, int cls2, int mth2) throws IOException {
            Frame f = frame();
            if (f.type != 1) {
                throw new IOException("expected a method frame, got type " + f.type);
            }
            Buf b = new Buf();
            b.in = new DataInputStream(new java.io.ByteArrayInputStream(f.payload));
            b.classId = b.in.readUnsignedShort();
            b.methodId = b.in.readUnsignedShort();
            if ((b.classId == 10 || b.classId == 20) && b.methodId == 40 || b.classId == 10 && b.methodId == 50) {
                int code = b.in.readUnsignedShort();
                String text = new String(b.in.readNBytes(b.in.readUnsignedByte()), StandardCharsets.UTF_8);
                throw new IOException("broker closed: " + code + " " + text);
            }
            if (!(b.classId == cls1 && b.methodId == mth1) && !(b.classId == cls2 && b.methodId == mth2)) {
                throw new IOException("unexpected method " + b.classId + "." + b.methodId);
            }
            return b;
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing
            }
        }
    }

    static Workload amqp() {
        return new Workload() {
            @Override
            public String name() {
                return "amqpwire";
            }

            @Override
            public void configure(WarpProcess.Builder warp, StoreConfig stores) {
                warp.frontend("amqpwire", "WARP_AMQPWIRE_PORT");
                stores.enable("amqp");
            }

            @Override
            public void prepare(WarpProcess warp) throws Exception {
                try (Conn c = new Conn(warp.port("amqpwire"), false)) {
                    c.declare(false);
                }
            }

            @Override
            public Client open(WarpProcess warp) throws Exception {
                Conn c = new Conn(warp.port("amqpwire"), true);
                return new Client() {
                    @Override
                    public void write(long id) throws Exception {
                        c.publish(id);
                    }

                    @Override
                    public void read(long id) throws Exception {
                        c.declare(true);
                    }

                    @Override
                    public void close() {
                        c.close();
                    }
                };
            }

            @Override
            public Set<Long> presentIds(WarpProcess warp) throws Exception {
                Set<Long> ids = new HashSet<>();
                try (Conn c = new Conn(warp.port("amqpwire"), false)) {
                    String m;
                    while ((m = c.get()) != null) {
                        ids.add(Long.parseLong(m.trim()));
                    }
                }
                return ids;
            }
        };
    }
}
