package com.polygres.wire.orawire.backend;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Full-session raw TCP relay from an PolyWire frontend client straight through to orawire's own
 * listener — used instead of {@link NativeOracleExecutor}'s per-statement byte capture when the
 * whole session should be handled by orawire's already-complete TTC implementation rather than
 * PolyWire's own frontend dispatcher.
 *
 * <p>Why this exists on top of {@link NativeOracleExecutor}, not instead of it everywhere: found
 * live that sqlcl (unlike JDBC/python-oracledb) exercises TTC function codes PolyWire's own
 * frontend dispatcher never implemented (confirmed: {@code unsupported TTC function code: 59}) —
 * and, separately, confirmed live that the SAME sqlcl session connecting directly to orawire
 * (bypassing PolyWire's frontend entirely) works cleanly, because orawire's frontend is a real,
 * complete Oracle TTC server implementation, not a narrow slice. Rather than keep growing
 * PolyWire's own dispatcher one function code at a time to catch up, this relays the whole
 * session — auth, every function code, fetch continuation, all of it — through orawire's proven
 * frontend, and PolyWire steps out of the protocol-implementing business for this mode entirely.
 *
 * <p>Trade-off, matching what was scoped when native mode was designed: a session relayed this
 * way gets none of PolyWire's own per-statement pipeline (firewall/router/QoS/stats) — those
 * stages need to see and act on individual statements, which a byte-for-byte session relay never
 * exposes. {@code ORAPG_ORACLE_BACKEND_MODE=native} choosing this over {@link
 * NativeOracleExecutor}'s narrower per-statement capture is a real trade being made explicitly,
 * not a strict improvement — see ServerOptions.OracleBackendMode's javadoc.
 *
 * <p>NOT a fix for sqlplus specifically: confirmed live that sqlplus (real OCI, not a JDBC-based
 * tool despite sqlcl's name) still hangs even connecting directly to orawire with no PolyWire in
 * the path at all — a genuine, already-characterized gap in orawire's own OCI/rich-auth handling
 * (see orawire's own git history / ARCHITECTURE.md), not something a relay at any layer changes.
 */
public final class NativeSessionRelay {

    private static final Logger LOG = Logger.getLogger(NativeSessionRelay.class.getName());

    private NativeSessionRelay() {
    }

    /** Blocks for the whole session's lifetime, relaying bytes bidirectionally between {@code
     *  clientSocket} and a fresh connection to {@code (backendHost, backendPort)}. Returns once
     *  either side closes. */
    public static void relay(Socket clientSocket, String backendHost, int backendPort) throws IOException {
        try (Socket backendSocket = new Socket(backendHost, backendPort)) {
            InputStream clientIn = clientSocket.getInputStream();
            OutputStream clientOut = clientSocket.getOutputStream();
            InputStream backendIn = backendSocket.getInputStream();
            OutputStream backendOut = backendSocket.getOutputStream();

            Thread c2b = new Thread(() -> pump(clientIn, backendOut, backendSocket), "native-session-relay-c2b");
            c2b.setDaemon(true);
            c2b.start();

            // backend -> client on the calling thread, so this method only returns once the
            // session is genuinely over (matches the existing SessionHandler.run() contract of
            // blocking for the session's lifetime).
            pump(backendIn, clientOut, clientSocket);
            try {
                c2b.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void pump(InputStream in, OutputStream out, Socket peerToCloseOnExit) {
        byte[] buf = new byte[16384];
        try {
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException e) {
            LOG.log(Level.FINE, "native session relay side ended", e);
        } finally {
            try {
                peerToCloseOnExit.close();
            } catch (IOException ignored) {
            }
        }
    }
}
