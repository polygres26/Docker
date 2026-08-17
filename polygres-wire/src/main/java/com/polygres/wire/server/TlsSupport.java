package com.polygres.wire.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

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
}
