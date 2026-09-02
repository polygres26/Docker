package com.sayonora.wire.mssqlwire.frontend;

import com.sayonora.wire.mssqlwire.wireformat.TdsPacketType;
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
        
        engine.setEnabledProtocols(new String[] {"TLSv1.2"});
        this.rawIn = new DataInputStream(socket.getInputStream());
        this.rawOut = socket.getOutputStream();
        int packetSize = engine.getSession().getPacketBufferSize();
        int appSize = engine.getSession().getApplicationBufferSize();
        this.netOutScratch = ByteBuffer.allocate(packetSize);
        this.appInScratch = ByteBuffer.allocate(appSize);
    }

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
                
            }
        }
    }
}
