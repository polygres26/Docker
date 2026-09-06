package com.sayonora.wire.server;

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
