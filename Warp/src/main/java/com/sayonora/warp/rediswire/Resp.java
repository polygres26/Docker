package com.sayonora.warp.rediswire;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RESP2 / RESP3 codec: a command reader (multibulk and inline forms, binary-safe, 512 MB bulk limit)
 * and a reply writer that renders the same reply tree as RESP2 or RESP3 depending on the session's
 * negotiated protocol.
 *
 * <p>Reply tree (what command handlers return): {@code null} = null bulk; {@code byte[]} / {@code String} = bulk
 * string; {@code Long}/{@code Integer} = integer; {@code Double} = double (RESP3 {@code ,}, RESP2 bulk);
 * {@code Boolean} = RESP3 {@code #}, RESP2 integer; {@link Simple}; {@link Err}; {@link List} = array;
 * {@link RMap}/{@link RSet}/{@link Push}/{@link Verbatim}; {@link #NIL_ARRAY}; {@link Multi} = several
 * consecutive top-level replies.
 */
final class Resp {

    private Resp() {
    }

    static final long MAX_BULK = 512L * 1024 * 1024;
    static final int MAX_MULTIBULK = 1024 * 1024;
    static final int MAX_INLINE = 64 * 1024;

    /** A "+text" reply. */
    record Simple(String text) {
    }

    /** An error reply; {@code text} is the whole message including the code word (e.g. {@code ERR syntax error}). */
    record Err(String text) {
    }

    /** RESP3 map (RESP2: flat array of alternating key, value). */
    record RMap(List<Object> flat) {
    }

    /** RESP3 set (RESP2: array). */
    record RSet(List<Object> items) {
    }

    /** RESP3 push (RESP2: array). */
    record Push(List<Object> items) {
    }

    /** RESP3 verbatim string (RESP2: bulk). */
    record Verbatim(String text) {
    }

    /** Several top-level replies written back to back (e.g. SUBSCRIBE a b c). */
    record Multi(List<Object> replies) {
    }

    /** Pre-encoded bytes written verbatim. */
    record Raw(byte[] bytes) {
    }

    /** The RESP2 null array ({@code *-1}); RESP3 null ({@code _}). */
    static final Object NIL_ARRAY = new Object() {
        @Override
        public String toString() {
            return "NIL_ARRAY";
        }
    };

    static final Simple OK = new Simple("OK");
    static final Simple PONG = new Simple("PONG");
    static final Simple QUEUED = new Simple("QUEUED");

    /** HELP output: an array of status (simple string) lines. */
    static List<Object> help(String... lines) {
        List<Object> out = new ArrayList<>();
        for (String l : lines) {
            out.add(new Simple(l));
        }
        return out;
    }

    static Err err(String text) {
        return new Err(text);
    }

    static class ProtocolError extends IOException {
        ProtocolError(String m) {
            super(m);
        }
    }

    // ------------------------------------------------------------------------------------------
    // reader
    // ------------------------------------------------------------------------------------------

    static final class Reader {
        private final InputStream in;
        private final byte[] buf = new byte[64 * 1024];
        private int pos;
        private int lim;
        private final Runnable beforeBlock;

        Reader(InputStream in, Runnable beforeBlock) {
            this.in = in;
            this.beforeBlock = beforeBlock;
        }

        /** Bytes already buffered (so a pipelined client's next command needs no blocking read). */
        boolean hasBuffered() {
            return pos < lim;
        }

        private boolean fill() throws IOException {
            if (pos < lim) {
                return true;
            }
            beforeBlock.run();
            int n = in.read(buf, 0, buf.length);
            if (n < 0) {
                return false;
            }
            pos = 0;
            lim = n;
            return true;
        }

        private int read() throws IOException {
            if (pos >= lim && !fill()) {
                return -1;
            }
            return buf[pos++] & 0xff;
        }

        /** Next command, or {@code null} at EOF. */
        byte[][] readCommand() throws IOException {
            while (true) {
                int b = read();
                if (b < 0) {
                    return null;
                }
                if (b == '*') {
                    long n = readLong();
                    if (n > MAX_MULTIBULK) {
                        throw new ProtocolError("invalid multibulk length");
                    }
                    if (n <= 0) {
                        continue; // Redis ignores empty/null multibulk
                    }
                    byte[][] out = new byte[(int) n][];
                    for (int i = 0; i < n; i++) {
                        int t = read();
                        if (t < 0) {
                            return null;
                        }
                        if (t != '$') {
                            throw new ProtocolError("expected '$', got '" + (char) t + "'");
                        }
                        long len = readLong();
                        if (len < 0 || len > MAX_BULK) {
                            throw new ProtocolError("invalid bulk length");
                        }
                        out[i] = readBytes((int) len);
                        if (out[i] == null) {
                            return null;
                        }
                        if (read() != '\r' || read() != '\n') {
                            throw new ProtocolError("expected CRLF after bulk string");
                        }
                    }
                    return out;
                }
                // inline command
                pos--;
                byte[] line = readLine();
                if (line == null) {
                    return null;
                }
                List<byte[]> args = splitArgs(line);
                if (args == null) {
                    throw new ProtocolError("unbalanced quotes in request");
                }
                if (args.isEmpty()) {
                    continue;
                }
                return args.toArray(new byte[0][]);
            }
        }

        private long readLong() throws IOException {
            long v = 0;
            boolean neg = false;
            int digits = 0;
            while (true) {
                int c = read();
                if (c < 0) {
                    throw new ProtocolError("unexpected EOF");
                }
                if (c == '\r') {
                    if (read() != '\n') {
                        throw new ProtocolError("expected LF");
                    }
                    break;
                }
                if (c == '-' && digits == 0 && !neg) {
                    neg = true;
                } else if (c >= '0' && c <= '9') {
                    v = v * 10 + (c - '0');
                    if (++digits > 18) {
                        throw new ProtocolError("invalid length");
                    }
                } else {
                    throw new ProtocolError("invalid length");
                }
            }
            return neg ? -v : v;
        }

        private byte[] readBytes(int len) throws IOException {
            byte[] out = new byte[len];
            int got = 0;
            while (got < len) {
                if (pos >= lim && !fill()) {
                    return null;
                }
                int n = Math.min(len - got, lim - pos);
                System.arraycopy(buf, pos, out, got, n);
                pos += n;
                got += n;
            }
            return out;
        }

        private byte[] readLine() throws IOException {
            java.io.ByteArrayOutputStream line = new java.io.ByteArrayOutputStream();
            while (true) {
                int c = read();
                if (c < 0) {
                    return line.size() == 0 ? null : line.toByteArray();
                }
                if (c == '\n') {
                    byte[] raw = line.toByteArray();
                    int n = raw.length;
                    if (n > 0 && raw[n - 1] == '\r') {
                        return java.util.Arrays.copyOf(raw, n - 1);
                    }
                    return raw;
                }
                line.write(c);
                if (line.size() > MAX_INLINE) {
                    throw new ProtocolError("too big inline request");
                }
            }
        }
    }

    /** sdssplitargs: whitespace separated, with "double quoted" (escapes) and 'single quoted' arguments. */
    static List<byte[]> splitArgs(byte[] line) {
        List<byte[]> out = new ArrayList<>();
        int i = 0;
        int n = line.length;
        while (true) {
            while (i < n && isSpace(line[i])) {
                i++;
            }
            if (i >= n) {
                return out;
            }
            java.io.ByteArrayOutputStream cur = new java.io.ByteArrayOutputStream();
            boolean inq = false;
            boolean insq = false;
            boolean done = false;
            while (!done) {
                if (inq) {
                    if (i >= n) {
                        return null;
                    }
                    byte c = line[i];
                    if (c == '\\' && i + 3 < n && line[i + 1] == 'x' && isHex(line[i + 2]) && isHex(line[i + 3])) {
                        cur.write(Integer.parseInt(new String(line, i + 2, 2, StandardCharsets.ISO_8859_1), 16));
                        i += 3;
                    } else if (c == '\\' && i + 1 < n) {
                        i++;
                        byte e = line[i];
                        cur.write(switch (e) {
                            case 'n' -> '\n';
                            case 'r' -> '\r';
                            case 't' -> '\t';
                            case 'b' -> '\b';
                            case 'a' -> 7;
                            default -> e;
                        });
                    } else if (c == '"') {
                        if (i + 1 < n && !isSpace(line[i + 1])) {
                            return null;
                        }
                        done = true;
                    } else {
                        cur.write(c);
                    }
                } else if (insq) {
                    if (i >= n) {
                        return null;
                    }
                    byte c = line[i];
                    if (c == '\\' && i + 1 < n && line[i + 1] == '\'') {
                        i++;
                        cur.write('\'');
                    } else if (c == '\'') {
                        if (i + 1 < n && !isSpace(line[i + 1])) {
                            return null;
                        }
                        done = true;
                    } else {
                        cur.write(c);
                    }
                } else {
                    if (i >= n) {
                        done = true;
                        break;
                    }
                    byte c = line[i];
                    if (isSpace(c)) {
                        done = true;
                    } else if (c == '"') {
                        inq = true;
                    } else if (c == '\'') {
                        insq = true;
                    } else {
                        cur.write(c);
                    }
                }
                if (i < n) {
                    i++;
                }
            }
            out.add(cur.toByteArray());
        }
    }

    private static boolean isSpace(byte c) {
        return c == ' ' || c == '\n' || c == '\r' || c == '\t' || c == 0x0b || c == 0x0c;
    }

    private static boolean isHex(byte c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    // ------------------------------------------------------------------------------------------
    // writer
    // ------------------------------------------------------------------------------------------

    /** Growable output buffer; flushes to the socket when told to (and when it grows past 256 KiB). */
    static final class Writer {
        private final OutputStream out;
        private byte[] buf = new byte[8192];
        private int len;

        Writer(OutputStream out) {
            this.out = out;
        }

        int buffered() {
            return len;
        }

        private void ensure(int extra) {
            if (len + extra > buf.length) {
                buf = java.util.Arrays.copyOf(buf, Math.max(buf.length * 2, len + extra));
            }
        }

        void raw(byte[] b) {
            ensure(b.length);
            System.arraycopy(b, 0, buf, len, b.length);
            len += b.length;
        }

        private void raw(String s) {
            raw(s.getBytes(StandardCharsets.UTF_8));
        }

        private void b(int c) {
            ensure(1);
            buf[len++] = (byte) c;
        }

        private void crlf() {
            ensure(2);
            buf[len++] = '\r';
            buf[len++] = '\n';
        }

        private void num(long v) {
            raw(Long.toString(v));
        }

        void flush() throws IOException {
            if (len > 0) {
                out.write(buf, 0, len);
                len = 0;
            }
            out.flush();
            if (buf.length > 1 << 20) {
                buf = new byte[8192];
            }
        }

        void maybeFlush() throws IOException {
            if (len > 256 * 1024) {
                flush();
            }
        }

        /** Writes {@code reply} in the given protocol. */
        void write(Object reply, boolean resp3) {
            switch (reply) {
                case null -> {
                    if (resp3) {
                        raw("_\r\n");
                    } else {
                        raw("$-1\r\n");
                    }
                }
                case byte[] v -> {
                    b('$');
                    num(v.length);
                    crlf();
                    raw(v);
                    crlf();
                }
                case String s -> write(s.getBytes(StandardCharsets.UTF_8), resp3);
                case Long l -> {
                    b(':');
                    num(l);
                    crlf();
                }
                case Integer i -> {
                    b(':');
                    num(i);
                    crlf();
                }
                case Boolean bo -> {
                    if (resp3) {
                        raw(bo ? "#t\r\n" : "#f\r\n");
                    } else {
                        raw(bo ? ":1\r\n" : ":0\r\n");
                    }
                }
                case Double d -> {
                    if (resp3) {
                        b(',');
                        raw(Num.fmtDoubleResp3(d));
                        crlf();
                    } else {
                        write(Num.fmtDouble(d).getBytes(StandardCharsets.UTF_8), false);
                    }
                }
                case Simple s -> {
                    b('+');
                    raw(s.text());
                    crlf();
                }
                case Err e -> {
                    b('-');
                    raw(e.text());
                    crlf();
                }
                case Verbatim v -> {
                    if (resp3) {
                        byte[] t = v.text().getBytes(StandardCharsets.UTF_8);
                        b('=');
                        num(t.length + 4);
                        crlf();
                        raw("txt:");
                        raw(t);
                        crlf();
                    } else {
                        write(v.text(), false);
                    }
                }
                case RMap m -> {
                    List<Object> f = m.flat();
                    if (resp3) {
                        b('%');
                        num(f.size() / 2);
                    } else {
                        b('*');
                        num(f.size());
                    }
                    crlf();
                    for (Object o : f) {
                        write(o, resp3);
                    }
                }
                case RSet s -> {
                    b(resp3 ? '~' : '*');
                    num(s.items().size());
                    crlf();
                    for (Object o : s.items()) {
                        write(o, resp3);
                    }
                }
                case Push p -> {
                    b(resp3 ? '>' : '*');
                    num(p.items().size());
                    crlf();
                    for (Object o : p.items()) {
                        write(o, resp3);
                    }
                }
                case Multi m -> {
                    for (Object o : m.replies()) {
                        write(o, resp3);
                    }
                }
                case Raw r -> raw(r.bytes());
                case List<?> l -> {
                    b('*');
                    num(l.size());
                    crlf();
                    for (Object o : l) {
                        write(o, resp3);
                    }
                }
                default -> {
                    if (reply == NIL_ARRAY) {
                        raw(resp3 ? "_\r\n" : "*-1\r\n");
                    } else {
                        throw new IllegalStateException("cannot encode " + reply.getClass());
                    }
                }
            }
        }
    }
}
