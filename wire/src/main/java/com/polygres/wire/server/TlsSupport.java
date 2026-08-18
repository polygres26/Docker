package com.polygres.wire.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * Shared TLS wiring for all four client-facing frontends (orawire/pgwire/mywire/gRPC). Ported
 * from Omnigate's {@code ProxyServer.buildTlsFactory()} (see that class for the original): one
 * PKCS12 keystore ({@code POLYWIRE_TLS_KEYSTORE}/{@code POLYWIRE_TLS_KEYSTORE_PASSWORD}, renamed
 * here from Omnigate's {@code ORAPG_TLS_*} for consistency with everything else already renamed
 * in this port) backs every protocol's TLS listener. orawire/pgwire/mywire each wrap their
 * existing plain {@code ServerSocket} accept loop in an {@code SSLServerSocket} one instead — a
 * full-socket wrap done immediately after accept, before any protocol bytes, so none of the
 * three session handlers need any TLS-specific code (a {@code Socket} and an {@code SSLSocket}
 * are interchangeable from the handler's point of view). gRPC (Netty-based, not a raw
 * {@code ServerSocket} loop) instead needs a {@code KeyManagerFactory}, built here from the same
 * keystore, that {@link com.polygres.wire.grpc.PolyWireGrpcServer} feeds directly into Netty's
 * {@code SslContextBuilder.forServer(KeyManagerFactory)} — no PEM extraction/temp files needed;
 * grpc-netty-shaded accepts a {@code KeyManagerFactory} the same as any other Java TLS consumer.
 */
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
        KeyManagerFactory kmf = buildKeyManagerFactory(options);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, null);
        return context.getServerSocketFactory();
    }

    /**
     * Builds a fully mutual-auth {@link SSLContext} (key managers <em>and</em> trust managers,
     * unlike {@link #buildTlsFactory}'s server-only one) from the same PKCS12 keystore used for
     * client-facing TLS, treating it as its own truststore too. Used for Apache Ignite's
     * intra-cluster discovery/communication SPIs (see {@code PolyWireCluster}) — every PolyWire
     * node is symmetric (each one both dials and accepts inter-node connections), so unlike the
     * client-facing listeners, both sides of an Ignite link need to verify the other's
     * certificate, not just present their own. Reusing one shared keystore as both key material
     * and trust anchor works because every node in a deployment is provisioned with the same
     * keystore (the same dev cert in local testing, the same operator-distributed cert in
     * production) — not the general "any two arbitrary parties trust each other" case a public CA
     * chain would cover, just this project's existing shared-keystore model applied a second time.
     */
    public static SSLContext buildMutualSslContext(String keystorePath, String keystorePassword)
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
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return context;
    }
}
