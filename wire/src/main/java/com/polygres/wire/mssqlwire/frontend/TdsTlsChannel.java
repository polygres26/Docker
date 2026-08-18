package com.polygres.wire.mssqlwire.frontend;

import com.polygres.wire.mssqlwire.wireformat.TdsPacketType;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

/**
 * Drives mssqlwire's in-band TLS session directly off an {@link SSLEngine} instead of layering an
 * {@link javax.net.ssl.SSLSocket} over the raw socket (the way pgwire's {@code SSLRequest}/
 * mywire's {@code CLIENT_SSL} upgrade both do — see {@code
 * com.polygres.wire.pgwire.PgWireSessionHandler#readStartupMessage}'s javadoc for that simpler
 * pattern). TDS needs the extra control an {@code SSLEngine} gives over exactly which bytes get an
 * extra TDS-packet envelope, for a reason a raw-socket wrap can't handle:
 *
 * <p><b>Found live against real {@code mssql-jdbc}</b>: MS-TDS §2.2.6.4 wraps the TLS handshake's
 * own bytes in ordinary 8-byte TDS packets (type {@code PRE_LOGIN}, {@code 0x12}) while the
 * handshake is in progress — an earlier, simpler attempt at this (a raw-socket {@code SSLSocket}
 * wrap that TDS-wrapped every write/read while a single "handshaking" flag was set, mirroring
 * pgwire's pattern) did exactly that and got real {@code mssql-jdbc} through most of the SSL
 * handshake, including a successful {@code LOGIN7} send from the client's side. But real
 * {@code mssql-jdbc} stops TDS-wrapping <em>its own outbound</em>
 * handshake bytes slightly earlier than a naive server-side implementation expects to stop reading
 * wrapped input — its very last handshake flight (the client's {@code Finished}, following its
 * dummy {@code ChangeCipherSpec}) arrives as a <b>bare TLS record</b>, not a TDS-wrapped one, even
 * though the server hasn't validated that {@code Finished} yet and is, by MS-TDS's own letter,
 * still "in the handshake." A raw-socket {@code SSLSocket} wrap has no hook to detect that
 * mid-handshake shift; this class does, by inspecting each incoming chunk's leading byte before
 * deciding whether to strip a TDS envelope:
 * <ul>
 *   <li>leading byte {@code 0x12} (TDS {@code PRE_LOGIN}) → strip the 8-byte TDS header, feed the
 *   payload to {@link SSLEngine#unwrap}.</li>
 *   <li>leading byte {@code 0x14}/{@code 0x15}/{@code 0x16}/{@code 0x17} (real TLS record content
 *   types — ChangeCipherSpec/Alert/Handshake/ApplicationData) → no TDS envelope present; parse the
 *   bare 5-byte TLS record header directly and feed the whole record to {@link SSLEngine#unwrap}
 *   unchanged.</li>
 * </ul>
 * These byte ranges never collide, so the detection is unambiguous. The same {@link #readNextRecord}
 * path is reused for post-handshake application data (LOGIN7 onward) — by then only the bare-record
 * branch is ever taken, since a real client has no reason to keep TDS-wrapping after its own
 * handshake work is done, which is exactly the behavior this class was built to tolerate.
 *
 * <p><b>Outbound framing has no such ambiguity</b> — this server's own handshake bytes are TDS-
 * wrapped for as long as {@link #handshake()} is still running ({@link #serverWriteHandshaking}),
 * then sent as bare TLS records once it returns; there is nothing to detect on the write side since
 * this side fully controls when it's done handshaking.
 */
public final class TdsTlsChannel {

    private final SSLEngine engine;
    private final DataInputStream rawIn;
    private final OutputStream rawOut;
    private boolean serverWriteHandshaking = true;

    private final ByteBuffer netOutScratch;
    private final ByteBuffer appInScratch;

    private final ChannelInputStream in = new ChannelInputStream();
    private final ChannelOutputStream out = new ChannelOutputStream();

    public TdsTlsChannel(SSLContext sslContext, Socket socket) throws IOException {
        this.engine = sslContext.createSSLEngine();
        engine.setUseClientMode(false);
        // Pin to TLS 1.2: found live that TLS 1.3's dummy-CCS/coalesced-NewSessionTicket handshake
        // shape (JDK 25 negotiates 1.3 by default) desyncs this hand-rolled SSLEngine loop's
        // record bookkeeping against real mssql-jdbc, producing an AEADBadTagException on the
        // client's Finished record. TLS 1.2's simpler two-flight handshake (no dummy CCS, no
        // post-handshake tickets) round-trips correctly; real SQL Server itself has supported
        // TLS 1.2 for years, so this isn't a materially narrower client compatibility surface.
        engine.setEnabledProtocols(new String[] {"TLSv1.2"});
        this.rawIn = new DataInputStream(socket.getInputStream());
        this.rawOut = socket.getOutputStream();
        int packetSize = engine.getSession().getPacketBufferSize();
        int appSize = engine.getSession().getApplicationBufferSize();
        this.netOutScratch = ByteBuffer.allocate(packetSize);
        this.appInScratch = ByteBuffer.allocate(appSize);
    }

    /** Runs the TLS handshake to completion (blocking), TDS-wrapping this server's own handshake output. */
    public void handshake() throws IOException {
        engine.beginHandshake();
        SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
        ByteBuffer empty = ByteBuffer.allocate(0);
        while (hs != SSLEngineResult.HandshakeStatus.FINISHED
                && hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (hs) {
                case NEED_TASK -> {
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    hs = engine.getHandshakeStatus();
                }
                case NEED_WRAP -> {
                    netOutScratch.clear();
                    SSLEngineResult res = engine.wrap(empty, netOutScratch);
                    netOutScratch.flip();
                    if (netOutScratch.hasRemaining()) {
                        writeFramed(netOutScratch);
                    }
                    if (res.getStatus() == SSLEngineResult.Status.CLOSED) {
                        throw new IOException("mssqlwire: TLS handshake closed by peer during wrap");
                    }
                    hs = res.getHandshakeStatus();
                }
                case NEED_UNWRAP -> {
                    ByteBuffer record = nextUnwrapChunk();
                    if (record == null) {
                        throw new EOFException("mssqlwire: peer closed during TLS handshake");
                    }
                    appInScratch.clear();
                    SSLEngineResult res = engine.unwrap(record, appInScratch);
                    if (res.getStatus() == SSLEngineResult.Status.CLOSED) {
                        throw new IOException("mssqlwire: TLS handshake closed by peer during unwrap");
                    }
                    hs = res.getHandshakeStatus();
                }
                default -> throw new IOException("mssqlwire: unexpected TLS handshake status " + hs);
            }
        }
        serverWriteHandshaking = false;
    }

    // Leftover, not-yet-fully-consumed bytes from the last readNextRecord() call -- a single TDS
    // envelope (or, post-handshake, a single OS-level read) can legitimately contain more than one
    // back-to-back raw TLS record (found live: mssql-jdbc's client Finished and CCS, or Finished
    // and its first LOGIN7 application-data record, can arrive batched in one chunk). Each
    // SSLEngine#unwrap call only ever consumes exactly one record's worth of bytes, so both the
    // handshake loop and post-handshake reads must keep draining this buffer -- via
    // nextUnwrapChunk() -- before pulling a fresh chunk off the wire, or the trailing record(s)
    // silently get dropped and every subsequent decrypt fails with a tag/sequence mismatch.
    private ByteBuffer pendingChunk;

    private ByteBuffer nextUnwrapChunk() throws IOException {
        if (pendingChunk == null || !pendingChunk.hasRemaining()) {
            pendingChunk = readNextRecord();
        }
        return pendingChunk;
    }

    public InputStream inputStream() {
        return in;
    }

    public OutputStream outputStream() {
        return out;
    }

    /** Sends one wrapped TLS record: TDS-{@code PRE_LOGIN}-enveloped while {@link #serverWriteHandshaking}, bare after. */
    private void writeFramed(ByteBuffer data) throws IOException {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        if (serverWriteHandshaking) {
            int total = 8 + bytes.length;
            byte[] header = new byte[8];
            header[0] = TdsPacketType.PRE_LOGIN;
            header[1] = TdsPacketType.STATUS_EOM;
            header[2] = (byte) ((total >>> 8) & 0xFF);
            header[3] = (byte) (total & 0xFF);
            rawOut.write(header);
        }
        rawOut.write(bytes);
        rawOut.flush();
    }

    /**
     * Reads exactly one raw TLS record's bytes, transparently stripping a TDS {@code PRE_LOGIN}
     * envelope if the leading byte says one is present — see class javadoc for why this is
     * decided per-chunk rather than by a single mode flag. Returns {@code null} on a clean EOF
     * before any bytes of the next record arrived.
     */
    private ByteBuffer readNextRecord() throws IOException {
        int first;
        try {
            first = rawIn.readUnsignedByte();
        } catch (EOFException e) {
            return null;
        }
        if (first == (TdsPacketType.PRE_LOGIN & 0xFF)) {
            byte[] restHeader = new byte[7];
            rawIn.readFully(restHeader);
            int length = ((restHeader[1] & 0xFF) << 8) | (restHeader[2] & 0xFF);
            int bodyLen = length - 8;
            if (bodyLen < 0) {
                throw new IOException("mssqlwire: TDS-wrapped TLS record length " + length + " smaller than header");
            }
            byte[] body = new byte[bodyLen];
            rawIn.readFully(body);
            return ByteBuffer.wrap(body);
        }
        // Bare TLS record: ContentType (already consumed as `first`) + ProtocolVersion(2) + Length(2, BE).
        byte[] rest = new byte[4];
        rawIn.readFully(rest);
        int length = ((rest[2] & 0xFF) << 8) | (rest[3] & 0xFF);
        byte[] payload = new byte[length];
        rawIn.readFully(payload);
        ByteBuffer full = ByteBuffer.allocate(5 + length);
        full.put((byte) first).put(rest).put(payload);
        full.flip();
        return full;
    }

    private final class ChannelOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            ByteBuffer app = ByteBuffer.wrap(b, off, len);
            while (app.hasRemaining()) {
                netOutScratch.clear();
                SSLEngineResult res = engine.wrap(app, netOutScratch);
                netOutScratch.flip();
                if (netOutScratch.hasRemaining()) {
                    writeFramed(netOutScratch);
                }
                if (res.getStatus() == SSLEngineResult.Status.CLOSED) {
                    throw new IOException("mssqlwire: TLS session closed on write");
                }
            }
        }

        @Override
        public void flush() {
            // Every write() call above already flushes rawOut immediately after each wrap.
        }
    }

    private final class ChannelInputStream extends InputStream {
        private ByteBuffer peerAppData = ByteBuffer.allocate(0);

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (!peerAppData.hasRemaining() && !fillAppData()) {
                return -1;
            }
            int n = Math.min(len, peerAppData.remaining());
            peerAppData.get(b, off, n);
            return n;
        }

        private boolean fillAppData() throws IOException {
            while (true) {
                ByteBuffer record = nextUnwrapChunk();
                if (record == null) {
                    return false;
                }
                appInScratch.clear();
                SSLEngineResult res = engine.unwrap(record, appInScratch);
                appInScratch.flip();
                if (res.getStatus() == SSLEngineResult.Status.CLOSED) {
                    return false;
                }
                if (appInScratch.hasRemaining()) {
                    peerAppData = appInScratch;
                    return true;
                }
                // A handshake-only record (e.g. a post-handshake NewSessionTicket) produces no
                // application bytes -- move on to the next record instead of returning an empty read.
            }
        }
    }
}
