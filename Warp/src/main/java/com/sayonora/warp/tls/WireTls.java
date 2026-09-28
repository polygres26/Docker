package com.sayonora.warp.tls;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TLS glue for the raw-TCP and gRPC protocol frontends (everything that is not an HTTP/Jetty listener).
 *
 * <ul>
 *   <li>{@link #provider(String, Map)}: the shared {@link TlsProvider} for a listener, or null with a single
 *       {@code TLS is NOT enabled: <reason>} log line (a bad TLS config never stops Warp);</li>
 *   <li>{@link Mode}: {@code WARP_<NAME>_TLS_MODE=disabled|allow|require} for protocols whose clients negotiate TLS on the
 *       plaintext port (MongoDB, Bolt, CQL): {@link #accept(Socket, Mode, TlsProvider)} sniffs the first bytes of a
 *       connection for a TLS ClientHello and serves plaintext and TLS on the one port when the mode is {@code allow};</li>
 *   <li>{@link #tlsServerSocket}: a separate TLS server socket for protocols with a dedicated TLS port (Redis, Kafka, AMQP);</li>
 *   <li>{@link #register}/{@link #info}: what /api/interfaces reports per frontend.</li>
 * </ul>
 */
public final class WireTls {

    private static final Logger log = LoggerFactory.getLogger(WireTls.class);
    private static final Map<String, Info> INFOS = new ConcurrentHashMap<>();

    private WireTls() {
    }

    /** {@code disabled}: plaintext only. {@code allow}: plaintext and TLS on the same port. {@code require}: TLS only. */
    public enum Mode {
        DISABLED, ALLOW, REQUIRE;

        /** @param value the WARP_*_TLS_MODE text or null; @param tlsAvailable whether TLS material is usable. */
        public static Mode parse(String value, boolean tlsAvailable) {
            if (value == null || value.isBlank()) {
                return tlsAvailable ? ALLOW : DISABLED;
            }
            return switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "disabled", "disable", "off", "false", "none" -> DISABLED;
                case "allow", "prefer", "prefertls", "allowtls", "on", "true" -> ALLOW;
                case "require", "required", "requiretls" -> REQUIRE;
                default -> throw new TlsException("TLS_MODE must be disabled, allow or require (got \"" + value + "\")");
            };
        }
    }

    /**
     * What a frontend ended up with. {@code mode}: off | in-band (same port, TLS negotiated in the protocol) |
     * sniff-allow | sniff-require | separate-port. {@code tlsPort} is the port TLS is served on (== the plaintext port for
     * in-band/sniff), -1 when off.
     */
    public record Info(String id, boolean tlsEnabled, String mode, int tlsPort, boolean selfSigned, boolean clientAuth,
            String error) {
    }

    public static void register(Info info) {
        INFOS.put(info.id(), info);
    }

    public static Info info(String id) {
        return INFOS.get(id);
    }

    public static Info off(String id, String error) {
        Info i = new Info(id, false, "off", -1, false, false, error);
        register(i);
        return i;
    }

    /** The provider for listener {@code name}, or null (logging the reason) when TLS is off or unusable. */
    public static TlsProvider provider(String name, Map<String, String> env) {
        String n = name.toUpperCase(Locale.ROOT);
        try {
            TlsSettings s = TlsSettings.resolve(n, env);
            return s == null ? null : TlsProvider.get(s);
        } catch (RuntimeException e) {
            log.error("{} TLS is NOT enabled: {}. Plaintext keeps working.", n,
                    e instanceof TlsException ? e.getMessage() : e.toString());
            lastError.put(n, e instanceof TlsException ? String.valueOf(e.getMessage()) : e.toString());
            return null;
        }
    }

    private static final Map<String, String> lastError = new ConcurrentHashMap<>();

    public static String lastError(String name) {
        return lastError.get(name.toUpperCase(Locale.ROOT));
    }

    /** True when the environment asks for TLS on {@code name}: listener-level or global material, not disabled. */
    public static boolean requested(String name, Map<String, String> env) {
        try {
            return TlsSettings.resolve(name, env) != null;
        } catch (RuntimeException e) {
            return true; // configured but broken: provider() reports it
        }
    }

    public static int intEnv(Map<String, String> env, String key, int dflt) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            return dflt;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            log.warn("{} must be an integer (got \"{}\"); using {}", key, v, dflt);
            return dflt;
        }
    }

    // ---- separate TLS port ----

    /**
     * Opens the TLS twin of a plaintext TCP listener: port {@code WARP_<NAME>_TLS_PORT} (default {@code defaultPort}), 0
     * or negative disables it. Returns null (after logging why) when TLS is not configured or cannot be set up. Records the
     * {@link Info} for {@code id} either way.
     */
    public static ServerSocket tlsServerSocket(String id, String name, int defaultPort, Map<String, String> env) {
        String n = name.toUpperCase(Locale.ROOT);
        int port = intEnv(env, "WARP_" + n + "_TLS_PORT", defaultPort);
        if (port <= 0) {
            off(id, null);
            return null;
        }
        TlsProvider p = provider(n, env);
        if (p == null) {
            off(id, lastError(n));
            return null;
        }
        try {
            ServerSocket ss = p.serverSocketFactory().createServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new java.net.InetSocketAddress(port), 1024);
            register(new Info(id, true, "separate-port", ss.getLocalPort(), p.selfSigned(),
                    p.settings().clientAuth() != TlsSettings.ClientAuth.NONE, null));
            log.info("{} TLS listening on port {} ({}) {}", n, ss.getLocalPort(), p.settings().origin(),
                    p.material().describe());
            return ss;
        } catch (IOException | RuntimeException e) {
            log.error("{} TLS is NOT enabled: cannot listen on port {}: {}. Plaintext keeps working.", n, port,
                    e.toString());
            off(id, "cannot listen on port " + port + ": " + e.getMessage());
            return null;
        }
    }

    // ---- gRPC TLS twin ----

    /**
     * Builds (not starts) the TLS twin of a gRPC frontend on {@code WARP_<NAME>_TLS_PORT} (default {@code defaultPort}, 0
     * disables), or returns null with a logged {@code TLS is NOT enabled: <reason>} when TLS is off or unusable.
     */
    public static io.grpc.Server grpcTwin(String id, String name, int defaultPort, Map<String, String> env,
            java.util.function.IntFunction<io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder> builder,
            java.util.function.Function<TlsProvider,
                    io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator.ProtocolNegotiator> negotiator) {
        String n = name.toUpperCase(Locale.ROOT);
        int port = intEnv(env, "WARP_" + n + "_TLS_PORT", defaultPort);
        if (port <= 0) {
            off(id, null);
            return null;
        }
        TlsProvider p = provider(n, env);
        if (p == null) {
            off(id, lastError(n));
            return null;
        }
        try {
            io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator.ProtocolNegotiator neg = negotiator.apply(p);
            io.grpc.Server s = builder.apply(port).protocolNegotiator(neg).build();
            register(new Info(id, true, "separate-port", port, p.selfSigned(),
                    p.settings().clientAuth() != TlsSettings.ClientAuth.NONE, null));
            return s;
        } catch (RuntimeException e) {
            log.error("{} TLS is NOT enabled: {}. Plaintext keeps working.", n, e.toString());
            off(id, e.toString());
            return null;
        }
    }

    /** Starts a server from {@link #grpcTwin}; a bind failure is logged and recorded, never thrown. */
    public static boolean startTwin(String id, String name, io.grpc.Server s) {
        if (s == null) {
            return false;
        }
        try {
            s.start();
            log.info("{} gRPC TLS listening on port {}", name.toUpperCase(Locale.ROOT), s.getPort());
            return true;
        } catch (IOException | RuntimeException e) {
            log.error("{} TLS is NOT enabled: cannot listen: {}. Plaintext keeps working.", name.toUpperCase(Locale.ROOT),
                    e.toString());
            off(id, e.toString());
            return false;
        }
    }

    // ---- ClientHello sniffing (same port) ----

    /** The resolved same-port TLS policy of a sniffing frontend (MongoDB, Bolt, CQL). */
    public record Sniff(Mode mode, TlsProvider provider) {
        /** Session runnable that first decides plaintext vs TLS, then runs the real session on the resulting socket. */
        public Runnable wrap(Socket raw, java.util.function.Function<Socket, Runnable> sessionFactory) {
            return wrap(raw, sessionFactory, () -> {
            });
        }

        /** As {@link #wrap(Socket, java.util.function.Function)}; {@code onDrop} runs when no session was started. */
        public Runnable wrap(Socket raw, java.util.function.Function<Socket, Runnable> sessionFactory, Runnable onDrop) {
            return () -> {
                Socket s;
                try {
                    s = accept(raw, mode, provider);
                } catch (IOException | RuntimeException e) {
                    log.debug("TLS sniff failed: {}", e.toString());
                    try {
                        raw.close();
                    } catch (IOException ignored) {
                        // gone
                    }
                    onDrop.run();
                    return;
                }
                if (s != null) {
                    sessionFactory.apply(s).run();
                } else {
                    onDrop.run();
                }
            };
        }
    }

    /**
     * Resolves {@code WARP_<NAME>_TLS_MODE} and the TLS material for a sniffing frontend and records its {@link Info}.
     * A bad configuration logs {@code TLS is NOT enabled: <reason>} and yields mode {@code disabled}; an explicit
     * {@code require} that cannot be honoured also falls back to plaintext (loudly) rather than refusing to start.
     */
    public static Sniff sniff(String id, String name, int port, Map<String, String> env) {
        String n = name.toUpperCase(Locale.ROOT);
        Mode want;
        try {
            want = Mode.parse(env.get("WARP_" + n + "_TLS_MODE"), true);
        } catch (TlsException e) {
            log.error("{} TLS is NOT enabled: {}. Plaintext keeps working.", n, e.getMessage());
            off(id, e.getMessage());
            return new Sniff(Mode.DISABLED, null);
        }
        if (want == Mode.DISABLED || isTrueEnv(env.get("WARP_" + n + "_TLS_DISABLED"))) {
            off(id, null);
            return new Sniff(Mode.DISABLED, null);
        }
        TlsProvider p = provider(n, env);
        if (p == null) {
            String err = lastError(n);
            if (err == null && env.get("WARP_" + n + "_TLS_MODE") != null) {
                err = "no TLS material configured (set WARP_TLS_CERT/WARP_TLS_KEY, WARP_TLS_KEYSTORE or WARP_" + n
                        + "_TLS_*)";
                log.error("{} TLS is NOT enabled: {}. Plaintext keeps working.", n, err);
            }
            off(id, err);
            return new Sniff(Mode.DISABLED, null);
        }
        register(new Info(id, true, want == Mode.REQUIRE ? "sniff-require" : "sniff-allow", port, p.selfSigned(),
                p.settings().clientAuth() != TlsSettings.ClientAuth.NONE, null));
        log.info("{} TLS enabled on port {} (mode {}, {}) {}", n, port, want.name().toLowerCase(Locale.ROOT),
                p.settings().origin(), p.material().describe());
        return new Sniff(want, p);
    }

    private static boolean isTrueEnv(String v) {
        return v != null && (v.equalsIgnoreCase("true") || v.equals("1") || v.equalsIgnoreCase("yes"));
    }

    /**
     * Is this the start of a TLS handshake record? {@code 0x16} (handshake) {@code 0x03} (SSLv3/TLS major)
     * {@code 0x01..0x04}. None of the sniffed protocols can start like this: a MongoDB message would need a length of at
     * least 66 KiB in its first frame, a Bolt handshake starts {@code 0x60 0x60 0xB0 0x17}, a CQL frame starts with its
     * protocol version {@code 0x03..0x05}.
     */
    public static boolean isClientHello(byte[] b, int n) {
        return n >= 3 && b[0] == 0x16 && b[1] == 0x03 && b[2] >= 0x01 && b[2] <= 0x04;
    }

    /** Could {@code b} (first {@code n} bytes seen) still turn out to be a ClientHello? Used to decide whether to read more. */
    public static boolean maybeClientHello(byte[] b, int n) {
        if (n == 0) {
            return true;
        }
        if (b[0] != 0x16) {
            return false;
        }
        return n < 2 || b[1] == 0x03 && (n < 3 || b[2] >= 0x01 && b[2] <= 0x04);
    }

    /**
     * Decides plaintext vs TLS for a freshly accepted connection. Blocks up to {@code sniffTimeoutMs} for the first bytes
     * (all sniffed protocols are client-first). Returns the socket the session should use, or null when the connection was
     * rejected (mode {@code require} and the client spoke plaintext) and has been closed.
     */
    public static Socket accept(Socket raw, Mode mode, TlsProvider provider) throws IOException {
        if (mode == Mode.DISABLED || provider == null) {
            return raw;
        }
        int oldTimeout = raw.getSoTimeout();
        raw.setSoTimeout(30_000);
        byte[] head = new byte[3];
        int n = 0;
        InputStream in = raw.getInputStream();
        try {
            while (n < 3 && maybeClientHello(head, n)) {
                int r = in.read(head, n, 3 - n);
                if (r < 0) {
                    break;
                }
                n += r;
            }
        } finally {
            raw.setSoTimeout(oldTimeout);
        }
        if (n == 0) {
            raw.close();
            return null;
        }
        byte[] seen = java.util.Arrays.copyOf(head, n);
        if (isClientHello(head, n)) {
            return provider.wrapServer(raw, seen);
        }
        if (mode == Mode.REQUIRE) {
            log.debug("TLS is required on this port: closing a plaintext connection from {}", raw.getRemoteSocketAddress());
            raw.close();
            return null;
        }
        return new ReplaySocket(raw, seen);
    }

    /** A socket whose input stream first replays bytes that were consumed while sniffing. */
    static final class ReplaySocket extends Socket {
        private final Socket d;
        private final InputStream in;

        ReplaySocket(Socket d, byte[] replay) throws IOException {
            this.d = d;
            this.in = new java.io.SequenceInputStream(new ByteArrayInputStream(replay), d.getInputStream());
        }

        @Override
        public InputStream getInputStream() {
            return in;
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            return d.getOutputStream();
        }

        @Override
        public synchronized void close() throws IOException {
            d.close();
        }

        @Override
        public boolean isClosed() {
            return d.isClosed();
        }

        @Override
        public boolean isConnected() {
            return d.isConnected();
        }

        @Override
        public boolean isInputShutdown() {
            return d.isInputShutdown();
        }

        @Override
        public boolean isOutputShutdown() {
            return d.isOutputShutdown();
        }

        @Override
        public void shutdownInput() throws IOException {
            d.shutdownInput();
        }

        @Override
        public void shutdownOutput() throws IOException {
            d.shutdownOutput();
        }

        @Override
        public InetAddress getInetAddress() {
            return d.getInetAddress();
        }

        @Override
        public InetAddress getLocalAddress() {
            return d.getLocalAddress();
        }

        @Override
        public int getPort() {
            return d.getPort();
        }

        @Override
        public int getLocalPort() {
            return d.getLocalPort();
        }

        @Override
        public SocketAddress getRemoteSocketAddress() {
            return d.getRemoteSocketAddress();
        }

        @Override
        public SocketAddress getLocalSocketAddress() {
            return d.getLocalSocketAddress();
        }

        @Override
        public void setSoTimeout(int t) throws SocketException {
            d.setSoTimeout(t);
        }

        @Override
        public int getSoTimeout() throws SocketException {
            return d.getSoTimeout();
        }

        @Override
        public void setTcpNoDelay(boolean on) throws SocketException {
            d.setTcpNoDelay(on);
        }

        @Override
        public boolean getTcpNoDelay() throws SocketException {
            return d.getTcpNoDelay();
        }

        @Override
        public void setKeepAlive(boolean on) throws SocketException {
            d.setKeepAlive(on);
        }

        @Override
        public boolean getKeepAlive() throws SocketException {
            return d.getKeepAlive();
        }

        @Override
        public void setSoLinger(boolean on, int linger) throws SocketException {
            d.setSoLinger(on, linger);
        }

        @Override
        public void setReceiveBufferSize(int size) throws SocketException {
            d.setReceiveBufferSize(size);
        }

        @Override
        public void setSendBufferSize(int size) throws SocketException {
            d.setSendBufferSize(size);
        }
    }
}
