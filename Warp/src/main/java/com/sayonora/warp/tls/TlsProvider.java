package com.sayonora.warp.tls;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single hot-reloadable TLS server identity every Warp listener shares. One provider hands out the same key
 * material in the three shapes the frontends need:
 * <ul>
 *   <li>{@link #jettyFactory()}: a Jetty {@code SslContextFactory.Server} (admin, MCP, A2A, other HTTP frontends);</li>
 *   <li>{@link #nettySslContext()}: a (grpc-shaded) Netty {@code SslContext} for gRPC;</li>
 *   <li>{@link #sslContext()} / {@link #serverSocketFactory()}: JDK {@code SSLContext} / {@code SSLServerSocketFactory}
 *       for raw-TCP frontends.</li>
 * </ul>
 * Hot reload: the key and trust managers delegate to the current {@link TlsMaterial}; a background poll (default
 * 30 s, {@code WARP_TLS_RELOAD_SECONDS}) compares the source files' mtime/size and swaps in the new material, so new
 * TLS handshakes on the Jetty and JDK paths present the renewed certificate without a restart. A failed reload keeps
 * the previous material and logs a warning. The Netty context is a snapshot: call {@link #nettySslContext()} again after
 * a reload (see {@link #generation()}).
 */
public final class TlsProvider {

    private static final Logger log = LoggerFactory.getLogger(TlsProvider.class);
    private static final Map<String, TlsProvider> CACHE = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService POLLER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "warp-tls-reload");
        t.setDaemon(true);
        return t;
    });

    private final TlsSettings settings;
    private final ReloadableKeyManager keyManager = new ReloadableKeyManager();
    private final ReloadableTrustManager trustManager;
    private final SSLContext sslContext;
    private volatile TlsMaterial material;
    private volatile String fileSignature;
    private volatile int generation;

    private TlsProvider(TlsSettings settings) {
        this.settings = settings;
        TlsMaterial first = TlsMaterial.load(settings);
        this.trustManager = first.trust().length > 0 || settings.clientAuth() != TlsSettings.ClientAuth.NONE
                ? new ReloadableTrustManager() : null;
        install(first);
        this.fileSignature = signature();
        try {
            this.sslContext = SSLContext.getInstance("TLS");
            sslContext.init(new KeyManager[] {keyManager}, trustManager == null ? null : new TrustManager[] {trustManager},
                    null);
        } catch (GeneralSecurityException e) {
            throw new TlsException("cannot create SSLContext: " + e.getMessage(), e);
        }
        if (settings.reloadSeconds() > 0 && settings.source() != TlsSettings.Source.SELF_SIGNED) {
            POLLER.scheduleWithFixedDelay(this::pollOnce, settings.reloadSeconds(), settings.reloadSeconds(),
                    TimeUnit.SECONDS);
        }
    }

    /** Returns the (shared) provider for these settings. @throws TlsException if the material cannot be loaded. */
    public static TlsProvider get(TlsSettings settings) {
        TlsProvider existing = CACHE.get(settings.cacheKey());
        if (existing != null) {
            return existing;
        }
        synchronized (CACHE) {
            existing = CACHE.get(settings.cacheKey());
            if (existing == null) {
                existing = new TlsProvider(settings);
                CACHE.put(settings.cacheKey(), existing);
            }
            return existing;
        }
    }

    /** Reloads every file-based provider now (used after the ACME service writes a renewed certificate). */
    public static void reloadAll() {
        for (TlsProvider p : CACHE.values()) {
            if (p.settings.source() == TlsSettings.Source.PEM) {
                p.reloadNow();
            }
        }
    }

    /** For tests: drops the shared-provider cache. */
    public static void clearCache() {
        CACHE.clear();
    }

    private void install(TlsMaterial m) {
        this.material = m;
        keyManager.delegate = (X509ExtendedKeyManager) m.keyManagerFactory().getKeyManagers()[0];
        if (trustManager != null && m.trustManagerFactory() != null) {
            trustManager.delegate = (X509ExtendedTrustManager) m.trustManagerFactory().getTrustManagers()[0];
        }
        generation++;
    }

    public TlsSettings settings() {
        return settings;
    }

    public TlsMaterial material() {
        return material;
    }

    /** Incremented on every successful (re)load. */
    public int generation() {
        return generation;
    }

    public boolean selfSigned() {
        return settings.source() == TlsSettings.Source.SELF_SIGNED;
    }

    private List<Path> files() {
        List<Path> f = new ArrayList<>();
        switch (settings.source()) {
            case KEYSTORE -> f.add(settings.keystore());
            case PEM -> {
                f.add(settings.cert());
                f.add(settings.key());
            }
            default -> {
            }
        }
        if (settings.ca() != null) {
            f.add(settings.ca());
        }
        return f;
    }

    private String signature() {
        StringBuilder sb = new StringBuilder();
        for (Path p : files()) {
            try {
                sb.append(Files.getLastModifiedTime(p).toMillis()).append(':').append(Files.size(p)).append(';');
            } catch (IOException e) {
                sb.append("missing;");
            }
        }
        return sb.toString();
    }

    private void pollOnce() {
        try {
            String now = signature();
            if (!now.equals(fileSignature)) {
                reloadNow();
            }
        } catch (RuntimeException e) {
            log.warn("TLS reload poll failed: {}", e.toString());
        }
    }

    /** Re-reads the key material now. On failure the previous material stays in use. @return true if reloaded. */
    public boolean reloadNow() {
        String sig = signature();
        try {
            install(TlsMaterial.load(settings));
            fileSignature = sig;
            log.info("TLS material reloaded ({}): {}", settings.origin(), material.describe());
            return true;
        } catch (TlsException e) {
            fileSignature = sig; // do not retry every tick until the files change again
            log.warn("TLS reload failed ({}); continuing with the previous certificate: {}", settings.origin(),
                    e.getMessage());
            return false;
        }
    }

    // ---- (a) Jetty ----

    public SslContextFactory.Server jettyFactory() {
        SslContextFactory.Server f = new SslContextFactory.Server();
        f.setSslContext(sslContext);
        f.setIncludeProtocols(settings.protocols());
        f.setSniRequired(false);
        f.setRenegotiationAllowed(false);
        switch (settings.clientAuth()) {
            case NEED -> f.setNeedClientAuth(true);
            case WANT -> f.setWantClientAuth(true);
            default -> {
            }
        }
        return f;
    }

    // ---- (b) Netty / gRPC ----

    /** A snapshot Netty context of the current material (re-call after {@link #generation()} changes). */
    public SslContext nettySslContext() {
        try {
            TlsMaterial m = material;
            SslContextBuilder b = SslContextBuilder.forServer(m.keyManagerFactory())
                    .protocols(settings.protocols());
            if (m.trustManagerFactory() != null) {
                b.trustManager(m.trustManagerFactory());
            }
            b.clientAuth(switch (settings.clientAuth()) {
                case NEED -> ClientAuth.REQUIRE;
                case WANT -> ClientAuth.OPTIONAL;
                default -> ClientAuth.NONE;
            });
            return GrpcSslContexts.configure(b).build();
        } catch (javax.net.ssl.SSLException e) {
            throw new TlsException("cannot build Netty SslContext: " + e.getMessage(), e);
        }
    }

    private volatile int nettyCacheGeneration = -1;
    private volatile SslContext nettyCache;
    private volatile SslContext nettyCacheMux;

    /**
     * A Netty context that follows reloads: rebuilt lazily when {@link #generation()} changed. {@code mux=true} advertises
     * ALPN {@code h2} and {@code http/1.1} (gRPC + REST on one port); otherwise gRPC's default ({@code h2} only).
     */
    public synchronized SslContext nettyContext(boolean mux) {
        if (nettyCacheGeneration != generation) {
            nettyCache = nettySslContext();
            nettyCacheMux = buildNettyMux();
            nettyCacheGeneration = generation;
        }
        return mux ? nettyCacheMux : nettyCache;
    }

    private SslContext buildNettyMux() {
        try {
            TlsMaterial m = material;
            SslContextBuilder b = SslContextBuilder.forServer(m.keyManagerFactory()).protocols(settings.protocols());
            if (m.trustManagerFactory() != null) {
                b.trustManager(m.trustManagerFactory());
            }
            b.clientAuth(switch (settings.clientAuth()) {
                case NEED -> ClientAuth.REQUIRE;
                case WANT -> ClientAuth.OPTIONAL;
                default -> ClientAuth.NONE;
            });
            GrpcSslContexts.configure(b);
            b.applicationProtocolConfig(new io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig(
                    io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.Protocol.ALPN,
                    io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    "h2", "http/1.1"));
            return b.build();
        } catch (javax.net.ssl.SSLException e) {
            throw new TlsException("cannot build Netty SslContext: " + e.getMessage(), e);
        }
    }

    /** Wraps an accepted plain socket in a server-mode TLS socket; {@code consumed} are bytes already read from it. */
    public javax.net.ssl.SSLSocket wrapServer(Socket raw, byte[] consumed) throws IOException {
        javax.net.ssl.SSLSocket s = (javax.net.ssl.SSLSocket) sslContext.getSocketFactory().createSocket(raw,
                new java.io.ByteArrayInputStream(consumed == null ? new byte[0] : consumed), true);
        s.setUseClientMode(false);
        s.setEnabledProtocols(settings.protocols());
        switch (settings.clientAuth()) {
            case NEED -> s.setNeedClientAuth(true);
            case WANT -> s.setWantClientAuth(true);
            default -> {
            }
        }
        return s;
    }

    // ---- (c) JDK / raw TCP ----

    public SSLContext sslContext() {
        return sslContext;
    }

    public SSLServerSocketFactory serverSocketFactory() {
        return new ConfiguredServerSocketFactory(sslContext.getServerSocketFactory(), settings);
    }

    /** Applies protocol floor and client-auth mode to every socket it creates. */
    private static final class ConfiguredServerSocketFactory extends SSLServerSocketFactory {
        private final SSLServerSocketFactory delegate;
        private final TlsSettings settings;

        ConfiguredServerSocketFactory(SSLServerSocketFactory delegate, TlsSettings settings) {
            this.delegate = delegate;
            this.settings = settings;
        }

        private ServerSocket cfg(ServerSocket s) {
            SSLServerSocket ss = (SSLServerSocket) s;
            ss.setEnabledProtocols(settings.protocols());
            switch (settings.clientAuth()) {
                case NEED -> ss.setNeedClientAuth(true);
                case WANT -> ss.setWantClientAuth(true);
                default -> {
                }
            }
            return ss;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public ServerSocket createServerSocket() throws IOException {
            return cfg(delegate.createServerSocket());
        }

        @Override
        public ServerSocket createServerSocket(int port) throws IOException {
            return cfg(delegate.createServerSocket(port));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog) throws IOException {
            return cfg(delegate.createServerSocket(port, backlog));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
            return cfg(delegate.createServerSocket(port, backlog, ifAddress));
        }
    }

    // ---- delegating managers (hot swap) ----

    private static final class ReloadableKeyManager extends X509ExtendedKeyManager {
        volatile X509ExtendedKeyManager delegate;

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return delegate.getClientAliases(keyType, issuers);
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return delegate.chooseClientAlias(keyType, issuers, socket);
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return delegate.getServerAliases(keyType, issuers);
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return delegate.chooseServerAlias(keyType, issuers, socket);
        }

        @Override
        public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
            return delegate.chooseEngineServerAlias(keyType, issuers, engine);
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return delegate.chooseEngineClientAlias(keyType, issuers, engine);
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return delegate.getCertificateChain(alias);
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return delegate.getPrivateKey(alias);
        }
    }

    private static final class ReloadableTrustManager extends X509ExtendedTrustManager {
        volatile X509ExtendedTrustManager delegate;

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkClientTrusted(chain, authType, socket);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkServerTrusted(chain, authType, socket);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            delegate.checkClientTrusted(chain, authType, engine);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            delegate.checkServerTrusted(chain, authType, engine);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate == null ? new X509Certificate[0] : delegate.getAcceptedIssuers();
        }
    }
}
