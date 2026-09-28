package com.sayonora.warp.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public final class TlsSupport {

    private TlsSupport() {
    }

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TlsSupport.class);
    private static final java.util.Set<String> REPORTED = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final String[] SQL_LISTENERS = {"GRPC", "ORAWIRE", "PGWIRE", "MYWIRE", "MSSQLWIRE"};

    /**
     * Does the environment ask for the SQL-side TLS (orawire TCPS, pgwire/mywire/mssqlwire in-band TLS, gRPC TLS)? True
     * for the historic {@code WARP_TLS_KEYSTORE}, and now also for global PEM ({@code WARP_TLS_CERT} + {@code WARP_TLS_KEY}),
     * {@code WARP_TLS_SELF_SIGNED=true}, or a per-listener {@code WARP_<PGWIRE|MYWIRE|MSSQLWIRE|ORAWIRE|GRPC>_TLS_*} set.
     */
    public static boolean configured(java.util.Map<String, String> env) {
        for (String l : SQL_LISTENERS) {
            if (com.sayonora.warp.tls.WireTls.requested(l, env)) {
                return true;
            }
        }
        return false;
    }

    /** The environment TLS settings are resolved from: the process env, plus a keystore given through {@link ServerOptions}. */
    static java.util.Map<String, String> envFor(ServerOptions options) {
        java.util.Map<String, String> env = new java.util.HashMap<>(System.getenv());
        String ks = options.tlsKeystorePath();
        if (ks != null && !ks.isBlank() && !env.containsKey("WARP_TLS_KEYSTORE")) {
            env.put("WARP_TLS_KEYSTORE", ks);
            if (options.tlsKeystorePassword() != null) {
                env.put("WARP_TLS_KEYSTORE_PASSWORD", options.tlsKeystorePassword());
            }
        }
        return env;
    }

    /**
     * The shared, hot-reloading TLS provider for a SQL-side listener ({@code GRPC}, {@code ORAWIRE}, {@code PGWIRE},
     * {@code MYWIRE}, {@code MSSQLWIRE}), or null when TLS is off for it or its configuration is unusable (then one
     * {@code TLS is NOT enabled: <reason>} line is logged and plaintext keeps working).
     */
    public static com.sayonora.warp.tls.TlsProvider provider(ServerOptions options, String listener) {
        if (!options.tlsEnabled()) {
            return null;
        }
        try {
            com.sayonora.warp.tls.TlsSettings s = com.sayonora.warp.tls.TlsSettings.resolve(listener, envFor(options));
            return s == null ? null : com.sayonora.warp.tls.TlsProvider.get(s);
        } catch (RuntimeException e) {
            if (REPORTED.add(listener + "|" + e.getMessage())) {
                log.error("{} TLS is NOT enabled: {}. Plaintext keeps working.", listener,
                        e instanceof com.sayonora.warp.tls.TlsException ? e.getMessage() : e.toString());
            }
            return null;
        }
    }

    public static boolean enabled(ServerOptions options, String listener) {
        return provider(options, listener) != null;
    }

    /** The JDK context of the shared provider for an in-band-TLS listener (hot reload: the key manager is swapped live). */
    public static SSLContext contextFor(ServerOptions options, String listener) throws IOException {
        com.sayonora.warp.tls.TlsProvider p = provider(options, listener);
        if (p == null) {
            throw new IOException(listener + " TLS is not enabled");
        }
        return p.sslContext();
    }

    public static KeyManagerFactory buildKeyManagerFactory(ServerOptions options)
            throws GeneralSecurityException, IOException {
        char[] password = options.tlsKeystorePassword() == null
                ? new char[0]
                : options.tlsKeystorePassword().toCharArray();
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(options.tlsKeystorePath())) {
            keyStore.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password);
        return kmf;
    }

    public static SSLServerSocketFactory buildTlsFactory(ServerOptions options)
            throws GeneralSecurityException, IOException {
        return buildTlsContext(options).getServerSocketFactory();
    }

    public static SSLContext buildTlsContext(ServerOptions options) throws GeneralSecurityException, IOException {
        KeyManagerFactory kmf = buildKeyManagerFactory(options);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }

    public static SSLContext buildMutualSslContext(String keystorePath, String keystorePassword)
            throws GeneralSecurityException, IOException {
        MutualTlsManagers managers = loadMutualTlsManagers(keystorePath, keystorePassword);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(managers.keyManagerFactory().getKeyManagers(), managers.trustManagerFactory().getTrustManagers(), null);
        return context;
    }

    /** A {@link KeyManagerFactory}/{@link TrustManagerFactory} pair loaded from the SAME PKCS12
     * file used as both keystore and truststore -- the same "one file, one identity, trusted by
     * anyone holding the matching CA" shape {@link #buildMutualSslContext} already uses for
     * Ignite's peer TLS. Netty's gRPC {@code SslContextBuilder} (see {@code
     * grpc/WarpPeerGrpcServer.java}) wants the managers directly rather than a JDK {@link
     * SSLContext}, which is why this is exposed as a separate method instead of only the
     * SSLContext-returning one above. */
    public record MutualTlsManagers(KeyManagerFactory keyManagerFactory, TrustManagerFactory trustManagerFactory) {
    }

    public static MutualTlsManagers loadMutualTlsManagers(String keystorePath, String keystorePassword)
            throws GeneralSecurityException, IOException {
        char[] password = keystorePassword == null ? new char[0] : keystorePassword.toCharArray();
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(keystorePath)) {
            keyStore.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        return new MutualTlsManagers(kmf, tmf);
    }
}
