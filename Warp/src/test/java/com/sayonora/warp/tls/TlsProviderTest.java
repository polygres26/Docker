package com.sayonora.warp.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The shared provider hands out working JDK, Jetty and Netty TLS shapes and hot-swaps the certificate. */
class TlsProviderTest {

    @AfterEach
    void clear() {
        TlsProvider.clearCache();
    }

    private static Path copy(String name, Path dir, String as) throws Exception {
        Path p = dir.resolve(as);
        Files.copy(PemLoaderTest.res(name), p, StandardCopyOption.REPLACE_EXISTING);
        return p;
    }

    private static SSLContext clientTrusting(Path caPem) throws Exception {
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ca", PemLoader.readCertificates(caPem)[0]);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext c = SSLContext.getInstance("TLS");
        c.init(null, tmf.getTrustManagers(), null);
        return c;
    }

    /** One handshake against the provider's raw-TCP server socket factory; returns the leaf the client saw. */
    private static X509Certificate handshake(TlsProvider p, SSLContext client) throws Exception {
        try (SSLServerSocket server = (SSLServerSocket) p.serverSocketFactory().createServerSocket(0)) {
            Thread t = new Thread(() -> {
                try (SSLSocket s = (SSLSocket) server.accept()) {
                    s.startHandshake();
                    OutputStream out = s.getOutputStream();
                    out.write(1);
                    out.flush();
                    InputStream in = s.getInputStream();
                    in.read();
                } catch (Exception ignored) {
                    // client-side assertions report failures
                }
            });
            t.setDaemon(true);
            t.start();
            try (SSLSocket c = (SSLSocket) client.getSocketFactory().createSocket("localhost", server.getLocalPort())) {
                c.startHandshake();
                c.getInputStream().read();
                c.getOutputStream().write(1);
                return (X509Certificate) c.getSession().getPeerCertificates()[0];
            }
        }
    }

    @Test
    void jdkSocketHandshakeUsesTheConfiguredChainAndAnUntrustingClientFails(@TempDir Path dir) throws Exception {
        Path cert = copy("rsa.fullchain.pem", dir, "cert.pem");
        Path key = copy("rsa.pkcs8.key", dir, "key.pem");
        TlsProvider p = TlsProvider.get(TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT", cert.toString(),
                "WARP_MCP_TLS_KEY", key.toString(), "WARP_TLS_RELOAD_SECONDS", "0")));
        X509Certificate seen = handshake(p, clientTrusting(PemLoaderTest.res("ca.pem")));
        assertEquals(p.material().leaf(), seen);
        assertThrows(SSLHandshakeException.class, () -> handshake(p, SSLContext.getDefault()));
    }

    @Test
    void reloadSwapsTheServedCertificateWithoutARestart(@TempDir Path dir) throws Exception {
        Path cert = copy("rsa.fullchain.pem", dir, "cert.pem");
        Path key = copy("rsa.pkcs8.key", dir, "key.pem");
        TlsProvider p = TlsProvider.get(TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT", cert.toString(),
                "WARP_MCP_TLS_KEY", key.toString(), "WARP_TLS_RELOAD_SECONDS", "0")));
        SSLContext client = clientTrusting(PemLoaderTest.res("ca.pem"));
        assertEquals("RSA", handshake(p, client).getPublicKey().getAlgorithm());
        int gen = p.generation();
        copy("ec.pem", dir, "cert.pem");
        copy("ec.pkcs8.key", dir, "key.pem");
        assertTrue(p.reloadNow());
        assertTrue(p.generation() > gen);
        assertEquals("EC", handshake(p, client).getPublicKey().getAlgorithm());
        // a broken renewal keeps serving the previous certificate
        Files.writeString(cert, "garbage");
        assertFalse(p.reloadNow());
        assertEquals("EC", handshake(p, client).getPublicKey().getAlgorithm());
    }

    @Test
    void mutualTlsRequiresAClientCertificateSignedByTheConfiguredCa(@TempDir Path dir) throws Exception {
        TlsProvider p = TlsProvider.get(TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT",
                PemLoaderTest.res("rsa.fullchain.pem").toString(), "WARP_MCP_TLS_KEY",
                PemLoaderTest.res("rsa.pkcs8.key").toString(), "WARP_TLS_CA", PemLoaderTest.res("ca.pem").toString(),
                "WARP_TLS_CLIENT_AUTH", "need", "WARP_TLS_RELOAD_SECONDS", "0")));
        SSLContext withoutCert = clientTrusting(PemLoaderTest.res("ca.pem"));
        assertThrows(Exception.class, () -> handshake(p, withoutCert));
        // client presents the CA-signed localhost cert (it also has clientAuth EKU) from the PKCS12 keystore
        KeyStore ks = KeyStore.getInstance(PemLoaderTest.res("rsa.p12").toFile(), "changeit".toCharArray());
        javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, "changeit".toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ca", PemLoader.readCertificates(PemLoaderTest.res("ca.pem"))[0]);
        tmf.init(ts);
        SSLContext withCert = SSLContext.getInstance("TLS");
        withCert.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        assertNotNull(handshake(p, withCert));
    }

    @Test
    void jettyAndNettyShapesBuildFromTheSameMaterial() throws Exception {
        TlsProvider p = TlsProvider.get(TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT",
                PemLoaderTest.res("ec.pem").toString(), "WARP_MCP_TLS_KEY", PemLoaderTest.res("ec.sec1.key").toString(),
                "WARP_TLS_MIN_VERSION", "1.3", "WARP_TLS_RELOAD_SECONDS", "0")));
        org.eclipse.jetty.util.ssl.SslContextFactory.Server f = p.jettyFactory();
        f.start();
        try {
            assertEquals(1, f.getIncludeProtocols().length);
            assertEquals("TLSv1.3", f.getIncludeProtocols()[0]);
        } finally {
            f.stop();
        }
        assertTrue(p.nettySslContext().isServer());
        assertNotNull(p.sslContext());
    }

    @Test
    void identicalSettingsShareOneProvider() {
        Map<String, String> env = Map.of("WARP_TLS_SELF_SIGNED", "true");
        assertTrue(TlsProvider.get(TlsSettings.resolve("MCP", env)) == TlsProvider.get(TlsSettings.resolve("ADMIN", env)));
    }
}
