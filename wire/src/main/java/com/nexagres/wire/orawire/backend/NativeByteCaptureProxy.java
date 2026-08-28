package com.nexagres.wire.orawire.backend;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NativeByteCaptureProxy {

    private static final Logger LOG = Logger.getLogger(NativeByteCaptureProxy.class.getName());

    public static final class CapturedSession {
        private final ConcurrentLinkedDeque<byte[]> serverToClientChunks = new ConcurrentLinkedDeque<>();
        private volatile boolean closed = false;

        public byte[] snapshotServerBytes() {
            int total = 0;
            for (byte[] chunk : serverToClientChunks) {
                total += chunk.length;
            }
            byte[] out = new byte[total];
            int pos = 0;
            for (byte[] chunk : serverToClientChunks) {
                System.arraycopy(chunk, 0, out, pos, chunk.length);
                pos += chunk.length;
            }
            return out;
        }

        public void clear() {
            serverToClientChunks.clear();
        }

        public boolean isClosed() {
            return closed;
        }
    }

    private final ServerSocket serverSocket;
    private final BlockingQueue<CapturedSession> pendingSessions = new LinkedBlockingQueue<>();
    private final Thread acceptThread;
    private volatile boolean stopped = false;

    private NativeByteCaptureProxy(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
        this.acceptThread = new Thread(this::acceptLoop, "native-ttc-capture-accept");
        this.acceptThread.setDaemon(true);
    }

    public static NativeByteCaptureProxy start() throws IOException {
        ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        NativeByteCaptureProxy proxy = new NativeByteCaptureProxy(ss);
        proxy.acceptThread.start();
        return proxy;
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public CapturedSession expectNextSession() throws InterruptedException {
        return pendingSessions.take();
    }

    public void stop() {
        stopped = true;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }
    }

    private void acceptLoop() {
        while (!stopped) {
            try {
                Socket client = serverSocket.accept();
                new Thread(() -> handleClient(client), "native-ttc-capture-session").start();
            } catch (IOException e) {
                if (!stopped) {
                    LOG.log(Level.WARNING, "accept loop error", e);
                }
            }
        }
    }

    private void handleClient(Socket client) {
        Socket upstream = null;
        try {
            InputStream clientIn = client.getInputStream();
            OutputStream clientOut = client.getOutputStream();

            int ver = readByteOrThrow(clientIn);
            if (ver != 5) {
                throw new IOException("unsupported SOCKS version: " + ver);
            }
            int nMethods = readByteOrThrow(clientIn);
            byte[] methods = readFully(clientIn, nMethods);
            
            clientOut.write(new byte[] { 5, 0 });
            clientOut.flush();

            int rver = readByteOrThrow(clientIn);
            int cmd = readByteOrThrow(clientIn);
            readByteOrThrow(clientIn);
            int atyp = readByteOrThrow(clientIn);
            if (rver != 5 || cmd != 1) {
                throw new IOException("unsupported SOCKS request ver=" + rver + " cmd=" + cmd);
            }
            String targetHost;
            if (atyp == 1) {
                byte[] addr = readFully(clientIn, 4);
                targetHost = (addr[0] & 0xFF) + "." + (addr[1] & 0xFF) + "." + (addr[2] & 0xFF) + "." + (addr[3] & 0xFF);
            } else if (atyp == 3) {
                int len = readByteOrThrow(clientIn);
                byte[] nameBytes = readFully(clientIn, len);
                targetHost = new String(nameBytes, java.nio.charset.StandardCharsets.US_ASCII);
            } else if (atyp == 4) {
                throw new IOException("IPv6 SOCKS targets not supported by this capture proxy");
            } else {
                throw new IOException("unknown SOCKS address type: " + atyp);
            }
            byte[] portBytes = readFully(clientIn, 2);
            int targetPort = ((portBytes[0] & 0xFF) << 8) | (portBytes[1] & 0xFF);

            upstream = new Socket(targetHost, targetPort);

            clientOut.write(new byte[] { 5, 0, 0, 1, 0, 0, 0, 0, 0, 0 });
            clientOut.flush();

            CapturedSession session = new CapturedSession();
            pendingSessions.put(session);

            InputStream upstreamIn = upstream.getInputStream();
            OutputStream upstreamOut = upstream.getOutputStream();
            Socket finalUpstream = upstream;

            Thread c2u = new Thread(() -> relay(clientIn, upstreamOut, null), "native-ttc-capture-c2u");
            c2u.setDaemon(true);
            c2u.start();

            relay(upstreamIn, clientOut, session);

            session.closed = true;
            c2u.join(2000);
        } catch (IOException | InterruptedException e) {
            LOG.log(Level.FINE, "capture session ended", e);
        } finally {
            closeQuietly(client);
            closeQuietly(upstream);
        }
    }

    private static void relay(InputStream in, OutputStream out, CapturedSession captureInto) {
        byte[] buf = new byte[8192];
        try {
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
                if (captureInto != null) {
                    byte[] chunk = new byte[n];
                    System.arraycopy(buf, 0, chunk, 0, n);
                    captureInto.serverToClientChunks.add(chunk);
                }
            }
        } catch (IOException ignored) {
            
        }
    }

    private static int readByteOrThrow(InputStream in) throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new IOException("unexpected EOF reading SOCKS handshake");
        }
        return b;
    }

    private static byte[] readFully(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            int r = in.read(buf, off, n - off);
            if (r < 0) {
                throw new IOException("unexpected EOF reading " + n + " bytes");
            }
            off += r;
        }
        return buf;
    }

    private static void closeQuietly(Socket s) {
        if (s != null) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
    }
}
