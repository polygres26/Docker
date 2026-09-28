package com.sayonora.warp.tls;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** ClientHello sniffing (MongoDB / Bolt / CQL same-port TLS), mode parsing and the fail-open configuration handling. */
class WireTlsTest {

    @AfterEach
    void clear() {
        TlsProvider.clearCache();
    }

    @Test
    void recognisesATlsClientHelloAndNothingElse() {
        assertTrue(WireTls.isClientHello(new byte[] {0x16, 0x03, 0x01}, 3));
        assertTrue(WireTls.isClientHello(new byte[] {0x16, 0x03, 0x03}, 3));
        assertTrue(WireTls.isClientHello(new byte[] {0x16, 0x03, 0x04}, 3));
        assertFalse(WireTls.isClientHello(new byte[] {0x16, 0x03, 0x01}, 2), "needs three bytes");
        // a MongoDB frame of 790 bytes starts 16 03 00 00 (little endian length): NOT a ClientHello
        assertFalse(WireTls.isClientHello(new byte[] {0x16, 0x03, 0x00}, 3));
        // Bolt handshake magic 60 60 B0 17, CQL v4 request 04 00 .., AMQP 'AMQP', HTTP 'GET'
        assertFalse(WireTls.isClientHello(new byte[] {0x60, 0x60, (byte) 0xB0}, 3));
        assertFalse(WireTls.isClientHello(new byte[] {0x04, 0x00, 0x00}, 3));
        assertFalse(WireTls.isClientHello(new byte[] {'A', 'M', 'Q'}, 3));
        assertFalse(WireTls.isClientHello(new byte[] {0x17, 0x03, 0x03}, 3), "application data record is not a hello");
    }

    @Test
    void readsMoreBytesOnlyWhileTheStartCouldStillBeAHello() {
        assertTrue(WireTls.maybeClientHello(new byte[3], 0));
        assertTrue(WireTls.maybeClientHello(new byte[] {0x16, 0, 0}, 1));
        assertTrue(WireTls.maybeClientHello(new byte[] {0x16, 0x03, 0}, 2));
        assertFalse(WireTls.maybeClientHello(new byte[] {0x16, 0x04, 0}, 2));
        assertFalse(WireTls.maybeClientHello(new byte[] {0x60, 0, 0}, 1));
        assertFalse(WireTls.maybeClientHello(new byte[] {0x16, 0x03, 0x00}, 3));
    }

    @Test
    void modeDefaultsFollowAvailabilityAndAcceptsTheDocumentedSpellings() {
        assertEquals(WireTls.Mode.ALLOW, WireTls.Mode.parse(null, true));
        assertEquals(WireTls.Mode.DISABLED, WireTls.Mode.parse(null, false));
        assertEquals(WireTls.Mode.DISABLED, WireTls.Mode.parse("disabled", true));
        assertEquals(WireTls.Mode.ALLOW, WireTls.Mode.parse("preferTLS", true));
        assertEquals(WireTls.Mode.ALLOW, WireTls.Mode.parse("allow", true));
        assertEquals(WireTls.Mode.REQUIRE, WireTls.Mode.parse("requireTLS", true));
        assertEquals(WireTls.Mode.REQUIRE, WireTls.Mode.parse("REQUIRE", true));
        assertThrows(TlsException.class, () -> WireTls.Mode.parse("sometimes", true));
    }

    private static Path copy(String name, Path dir, String as) throws Exception {
        Path p = dir.resolve(as);
        Files.copy(PemLoaderTest.res(name), p, StandardCopyOption.REPLACE_EXISTING);
        return p;
    }

    private static TlsProvider provider(Path dir) throws Exception {
        Path cert = copy("rsa.fullchain.pem", dir, "cert.pem");
        Path key = copy("rsa.pkcs8.key", dir, "key.pem");
        return TlsProvider.get(TlsSettings.resolve("MONGOWIRE", Map.of("WARP_TLS_CERT", cert.toString(),
                "WARP_TLS_KEY", key.toString(), "WARP_TLS_RELOAD_SECONDS", "0")));
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

    @Test
    void allowModeServesPlaintextAndTlsOnOnePortAndReplaysTheSniffedBytes(@TempDir Path dir) throws Exception {
        TlsProvider p = provider(dir);
        try (ServerSocket ss = new ServerSocket(0)) {
            // plaintext client: first bytes must arrive intact at the session
            Socket[] res = new Socket[1];
            Thread t1 = new Thread(() -> {
                try {
                    res[0] = WireTls.accept(ss.accept(), WireTls.Mode.ALLOW, p);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t1.start();
            try (Socket c = new Socket("localhost", ss.getLocalPort())) {
                c.getOutputStream().write(new byte[] {0x60, 0x60, (byte) 0xB0, 0x17, 1, 2, 3});
                c.getOutputStream().flush();
                t1.join(10_000);
                assertNotNull(res[0]);
                assertFalse(res[0] instanceof SSLSocket);
                byte[] got = res[0].getInputStream().readNBytes(7);
                assertArrayEquals(new byte[] {0x60, 0x60, (byte) 0xB0, 0x17, 1, 2, 3}, got);
                res[0].getOutputStream().write(9);
                assertEquals(9, c.getInputStream().read());
                res[0].close();
            }
            // TLS client on the same port
            Thread t2 = new Thread(() -> {
                try {
                    res[0] = WireTls.accept(ss.accept(), WireTls.Mode.ALLOW, p);
                    assertInstanceOf(SSLSocket.class, res[0]);
                    InputStream in = res[0].getInputStream();
                    int b = in.read();
                    OutputStream out = res[0].getOutputStream();
                    out.write(b + 1);
                    out.flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t2.start();
            try (SSLSocket c = (SSLSocket) clientTrusting(PemLoaderTest.res("ca.pem")).getSocketFactory()
                    .createSocket("localhost", ss.getLocalPort())) {
                c.startHandshake();
                c.getOutputStream().write(41);
                c.getOutputStream().flush();
                assertEquals(42, c.getInputStream().read());
            }
            t2.join(10_000);
        }
    }

    @Test
    void requireModeClosesPlaintextButServesTls(@TempDir Path dir) throws Exception {
        TlsProvider p = provider(dir);
        try (ServerSocket ss = new ServerSocket(0)) {
            Socket[] res = new Socket[] {new Socket()};
            Thread t = new Thread(() -> {
                try {
                    res[0] = WireTls.accept(ss.accept(), WireTls.Mode.REQUIRE, p);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            try (Socket c = new Socket("localhost", ss.getLocalPort())) {
                c.getOutputStream().write(new byte[] {0x04, 0, 0, 1});
                c.getOutputStream().flush();
                t.join(10_000);
                assertNull(res[0], "plaintext must be rejected in require mode");
                int closed;
                try {
                    closed = c.getInputStream().read();
                } catch (java.net.SocketException reset) {
                    closed = -1; // the server closed with unread bytes pending: a reset is a close too
                }
                assertEquals(-1, closed, "server closed the connection");
            }
        }
    }

    @Test
    void disabledModeOrNoProviderLeavesTheSocketAlone() throws Exception {
        try (ServerSocket ss = new ServerSocket(0); Socket c = new Socket("localhost", ss.getLocalPort());
                Socket s = ss.accept()) {
            assertEquals(s, WireTls.accept(s, WireTls.Mode.DISABLED, null));
            assertEquals(s, WireTls.accept(s, WireTls.Mode.ALLOW, null));
        }
    }

    @Test
    void unusableTlsConfigurationLogsAndYieldsNoProviderInsteadOfThrowing() {
        TlsProvider p = WireTls.provider("MONGOWIRE", Map.of("WARP_TLS_CERT", "/no/such/cert.pem",
                "WARP_TLS_KEY", "/no/such/key.pem"));
        assertNull(p);
        assertNotNull(WireTls.lastError("MONGOWIRE"));
        WireTls.Sniff s = WireTls.sniff("mongowire-test-bad", "MONGOWIRE", 27017, Map.of("WARP_TLS_CERT", "/no/such/cert.pem",
                "WARP_TLS_KEY", "/no/such/key.pem"));
        assertEquals(WireTls.Mode.DISABLED, s.mode());
        WireTls.Info info = WireTls.info("mongowire-test-bad");
        assertFalse(info.tlsEnabled());
        assertNotNull(info.error());
    }

    @Test
    void perListenerDisableAndExplicitModeOverrideTheGlobalConfig(@TempDir Path dir) throws Exception {
        Path cert = copy("rsa.fullchain.pem", dir, "cert.pem");
        Path key = copy("rsa.pkcs8.key", dir, "key.pem");
        Map<String, String> global = Map.of("WARP_TLS_CERT", cert.toString(), "WARP_TLS_KEY", key.toString(),
                "WARP_TLS_RELOAD_SECONDS", "0");
        assertEquals(WireTls.Mode.ALLOW, WireTls.sniff("bolt-t1", "BOLTWIRE", 7687, global).mode());
        assertEquals("sniff-allow", WireTls.info("bolt-t1").mode());
        assertEquals(WireTls.Mode.DISABLED, WireTls.sniff("bolt-t2", "BOLTWIRE", 7687,
                Map.of("WARP_TLS_CERT", cert.toString(), "WARP_TLS_KEY", key.toString(), "WARP_BOLTWIRE_TLS_DISABLED", "true")).mode());
        assertEquals(WireTls.Mode.DISABLED, WireTls.sniff("bolt-t3", "BOLTWIRE", 7687,
                Map.of("WARP_TLS_CERT", cert.toString(), "WARP_TLS_KEY", key.toString(), "WARP_BOLTWIRE_TLS_MODE", "disabled")).mode());
        assertEquals(WireTls.Mode.REQUIRE, WireTls.sniff("bolt-t4", "BOLTWIRE", 7687,
                Map.of("WARP_TLS_CERT", cert.toString(), "WARP_TLS_KEY", key.toString(), "WARP_BOLTWIRE_TLS_MODE", "require")).mode());
        assertEquals("sniff-require", WireTls.info("bolt-t4").mode());
        // nothing configured at all: plaintext only, and an explicit require without material says why
        assertEquals(WireTls.Mode.DISABLED, WireTls.sniff("bolt-t5", "BOLTWIRE", 7687, Map.of()).mode());
        assertEquals(WireTls.Mode.DISABLED, WireTls.sniff("bolt-t6", "BOLTWIRE", 7687, Map.of("WARP_BOLTWIRE_TLS_MODE", "require")).mode());
        assertNotNull(WireTls.info("bolt-t6").error());
    }

    @Test
    void separateTlsPortListensWhenConfiguredAndReportsItsPort(@TempDir Path dir) throws Exception {
        Path cert = copy("rsa.fullchain.pem", dir, "cert.pem");
        Path key = copy("rsa.pkcs8.key", dir, "key.pem");
        Map<String, String> env = Map.of("WARP_TLS_CERT", cert.toString(), "WARP_TLS_KEY", key.toString(),
                "WARP_TLS_RELOAD_SECONDS", "0", "WARP_REDISWIRE_TLS_PORT", "0");
        assertNull(WireTls.tlsServerSocket("redis-t1", "REDISWIRE", 17379, env), "port 0 disables the TLS twin");
        try (ServerSocket ss = WireTls.tlsServerSocket("redis-t2", "REDISWIRE", 0,
                Map.of("WARP_TLS_CERT", cert.toString(), "WARP_TLS_KEY", key.toString(), "WARP_TLS_RELOAD_SECONDS", "0",
                        "WARP_REDISWIRE_TLS_PORT", String.valueOf(freePort())))) {
            assertNotNull(ss);
            assertEquals("separate-port", WireTls.info("redis-t2").mode());
            assertEquals(ss.getLocalPort(), WireTls.info("redis-t2").tlsPort());
        }
        assertNull(WireTls.tlsServerSocket("redis-t3", "REDISWIRE", 17379, Map.of()), "no material: no TLS port");
        assertFalse(WireTls.info("redis-t3").tlsEnabled());
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
