package com.polygres.wire.orawire.backend;

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

/**
 * A minimal local SOCKS5 proxy (CONNECT command only, no auth) that ojdbc11 is pointed at via
 * {@code oracle.net.socksProxyHost}/{@code socksProxyPort} connection properties, so every byte
 * the real Oracle wire protocol driver receives passes through here first — with zero changes to
 * the driver itself, zero new O5LOGON/crypto code, and zero coupling to orawire's internals.
 *
 * <p>Exists because of a concrete, live-tested finding (see {@link
 * com.polygres.wire.orawire.ttc.ResponseWriter}'s DESCRIBE_INFO/inline-exhaustion javadoc history):
 * {@code java.sql.ResultSet}/{@code ResultSetMetaData} never exposes several backend-computed,
 * semantically-opaque TTC fields (the {@code al8o4}/RPA array in particular) that a real client
 * still needs bytes for — confirmed live by pointing the SAME JDBC connection at a real Oracle
 * backend directly vs. through orawire (itself proven byte-correct) and getting the identical
 * failure either way, meaning the gap is JDBC's own abstraction, not backend fidelity. Capturing
 * the raw bytes here, alongside the JDBC call that's already happening, sidesteps that entirely:
 * the driver still does its own real O5LOGON/EXECUTE round trip unmodified, this class just keeps
 * a copy of what came back so {@link com.polygres.wire.orawire.session.RequestLoop} can relay the real
 * bytes to its own frontend client instead of reconstructing them from JDBC metadata.
 *
 * <p>First-cut correlation model: FIFO. {@link #expectNextSession()} must be called by the same
 * logical caller immediately before opening the JDBC connection that will route through this
 * proxy — the next accepted SOCKS session is handed to whichever caller is waiting, in order.
 * Correct as long as "start expecting" happens before "start connecting" for each session, which
 * is naturally true (JDBC connect() must complete before any query runs) — but does NOT yet
 * disambiguate multiple concurrent connection opens by identity (only by order). Fine for the
 * current one-native-connection-at-a-time slice; a real pooled/concurrent version needs a
 * stronger correlation key (e.g. tagging each JDBC connection's CID PROGRAM string and having this
 * proxy parse it out of the raw O5LOGON CONNECT packet), left as documented follow-up.
 */
public final class NativeByteCaptureProxy {

    private static final Logger LOG = Logger.getLogger(NativeByteCaptureProxy.class.getName());

    /** One accepted SOCKS session: relays bytes transparently and keeps a copy of what the server sent. */
    public static final class CapturedSession {
        private final ConcurrentLinkedDeque<byte[]> serverToClientChunks = new ConcurrentLinkedDeque<>();
        private volatile boolean closed = false;

        /** All bytes the backend has sent so far on this connection, concatenated in order. */
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

        /** Discards captured bytes older than the current point — call after each logical request/response
         *  round trip is fully consumed, so the next call's capture starts clean. */
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

    /** Starts listening on 127.0.0.1 (loopback only — never exposed externally) at an ephemeral port. */
    public static NativeByteCaptureProxy start() throws IOException {
        ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        NativeByteCaptureProxy proxy = new NativeByteCaptureProxy(ss);
        proxy.acceptThread.start();
        return proxy;
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    /** Blocks until the next SOCKS session this proxy accepts is available — see class javadoc's
     *  FIFO correlation caveat. Call this immediately before opening the JDBC connection. */
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

            // --- SOCKS5 greeting: version(1) nmethods(1) methods(nmethods) ---
            int ver = readByteOrThrow(clientIn);
            if (ver != 5) {
                throw new IOException("unsupported SOCKS version: " + ver);
            }
            int nMethods = readByteOrThrow(clientIn);
            byte[] methods = readFully(clientIn, nMethods);
            // We only offer "no authentication required" (0x00) — ojdbc11's SOCKS client defaults
            // to that when no socksProxyUsername/Password is configured.
            clientOut.write(new byte[] { 5, 0 });
            clientOut.flush();

            // --- SOCKS5 request: version(1) cmd(1) rsv(1) atyp(1) dst.addr dst.port(2) ---
            int rver = readByteOrThrow(clientIn);
            int cmd = readByteOrThrow(clientIn);
            readByteOrThrow(clientIn); // reserved
            int atyp = readByteOrThrow(clientIn);
            if (rver != 5 || cmd != 1) { // 1 = CONNECT
                throw new IOException("unsupported SOCKS request ver=" + rver + " cmd=" + cmd);
            }
            String targetHost;
            if (atyp == 1) { // IPv4
                byte[] addr = readFully(clientIn, 4);
                targetHost = (addr[0] & 0xFF) + "." + (addr[1] & 0xFF) + "." + (addr[2] & 0xFF) + "." + (addr[3] & 0xFF);
            } else if (atyp == 3) { // domain name
                int len = readByteOrThrow(clientIn);
                byte[] nameBytes = readFully(clientIn, len);
                targetHost = new String(nameBytes, java.nio.charset.StandardCharsets.US_ASCII);
            } else if (atyp == 4) { // IPv6 - not needed for our loopback/backend-host use case
                throw new IOException("IPv6 SOCKS targets not supported by this capture proxy");
            } else {
                throw new IOException("unknown SOCKS address type: " + atyp);
            }
            byte[] portBytes = readFully(clientIn, 2);
            int targetPort = ((portBytes[0] & 0xFF) << 8) | (portBytes[1] & 0xFF);

            upstream = new Socket(targetHost, targetPort);

            // Success reply: ver(5) rep(0=succeeded) rsv(0) atyp(1=ipv4) bnd.addr(4) bnd.port(2)
            clientOut.write(new byte[] { 5, 0, 0, 1, 0, 0, 0, 0, 0, 0 });
            clientOut.flush();

            CapturedSession session = new CapturedSession();
            pendingSessions.put(session);

            InputStream upstreamIn = upstream.getInputStream();
            OutputStream upstreamOut = upstream.getOutputStream();
            Socket finalUpstream = upstream;

            // client -> upstream: plain relay, nothing to capture (that's the request we already know)
            Thread c2u = new Thread(() -> relay(clientIn, upstreamOut, null), "native-ttc-capture-c2u");
            c2u.setDaemon(true);
            c2u.start();

            // upstream -> client: relay AND capture — this is the real backend's own response bytes
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
            // Normal on connection close from either side.
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
