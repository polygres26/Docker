package com.polygres.wire.server;

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
