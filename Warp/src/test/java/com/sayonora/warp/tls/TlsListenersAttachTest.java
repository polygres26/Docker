package com.sayonora.warp.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** {@link TlsListeners#attach} adds an HTTPS connector to a frontend's own Jetty server and never breaks plaintext. */
class TlsListenersAttachTest {

    @AfterEach
    void clear() {
        TlsProvider.clearCache();
    }

    private static Server plain() {
        Server s = new Server();
        ServerConnector c = new ServerConnector(s, new HttpConnectionFactory());
        c.setPort(0);
        s.addConnector(c);
        return s;
    }

    private static long https(Server s) {
        return java.util.Arrays.stream(s.getConnectors()).filter(c -> c.getConnectionFactory(SslConnectionFactory.class) != null).count();
    }

    @Test
    void noTlsConfiguredLeavesPlaintextOnly() {
        Server s = plain();
        TlsListeners.Info i = TlsListeners.attach(s, "T2ANONE", 18999, Map.of());
        assertFalse(i.tlsEnabled());
        assertEquals(-1, i.httpsPort());
        assertEquals(1, s.getConnectors().length);
        assertNull(i.tlsError());
        assertNotNull(TlsListeners.info("T2ANONE"));
    }

    @Test
    void selfSignedAddsHttpsConnectorAndPlaintextStays() throws Exception {
        Server s = plain();
        TlsListeners.Info i = TlsListeners.attach(s, "T2ASS", 18999, Map.of("WARP_T2ASS_TLS_SELF_SIGNED", "true"));
        assertTrue(i.tlsEnabled());
        assertTrue(i.selfSigned());
        assertEquals(2, s.getConnectors().length);
        assertEquals(1, https(s));
        s.start();
        try {
            assertTrue(((ServerConnector) s.getConnectors()[1]).getLocalPort() > 0);
        } finally {
            s.stop();
        }
    }

    @Test
    void explicitHttpsPortWinsAndGlobalSettingsApply() {
        Server s = plain();
        TlsListeners.Info i = TlsListeners.attach(s, "T2AEXP", 18999,
                Map.of("WARP_TLS_SELF_SIGNED", "true", "WARP_T2AEXP_HTTPS_PORT", "18777"));
        assertEquals(18777, i.httpsPort());
        assertEquals(18777, ((ServerConnector) s.getConnectors()[1]).getPort());
    }

    @Test
    void perListenerDisableBeatsGlobalConfiguration() {
        Server s = plain();
        TlsListeners.Info i = TlsListeners.attach(s, "T2ADIS", 18999,
                Map.of("WARP_TLS_SELF_SIGNED", "true", "WARP_T2ADIS_TLS_DISABLED", "true"));
        assertFalse(i.tlsEnabled());
        assertEquals(1, s.getConnectors().length);
    }

    @Test
    void badTlsConfigurationIsReportedAndPlaintextKeeps() {
        Server s = plain();
        TlsListeners.Info i = TlsListeners.attach(s, "T2ABAD", 18999,
                Map.of("WARP_T2ABAD_TLS_CERT", "/nonexistent/c.pem", "WARP_T2ABAD_TLS_KEY", "/nonexistent/k.pem"));
        assertFalse(i.tlsEnabled());
        assertNotNull(i.tlsError());
        assertEquals(1, s.getConnectors().length);
    }

    @Test
    void httpDisabledRemovesPlaintextOrFailsClosed() {
        Server s = plain();
        TlsListeners.Info i = TlsListeners.attach(s, "T2AHD", 18999,
                Map.of("WARP_TLS_SELF_SIGNED", "true", "WARP_T2AHD_HTTP_DISABLED", "true"));
        assertEquals(-1, i.httpPort());
        assertEquals(1, s.getConnectors().length);
        assertEquals(1, https(s));
        assertThrows(TlsException.class,
                () -> TlsListeners.attach(plain(), "T2AHD2", 18999, Map.of("WARP_T2AHD2_HTTP_DISABLED", "true")));
        assertThrows(TlsException.class, () -> TlsListeners.attach(plain(), "T2AHD3", 18999,
                Map.of("WARP_T2AHD3_HTTP_DISABLED", "true", "WARP_T2AHD3_TLS_CERT", "/nope", "WARP_T2AHD3_TLS_KEY", "/nope")));
    }

    @Test
    void badPortIsRejected() {
        assertThrows(TlsException.class, () -> TlsListeners.httpsPort("X", 1, 2, Map.of("WARP_X_HTTPS_PORT", "abc")));
        assertEquals(0, TlsListeners.httpsPort("X", 0, 2, Map.of()));
        assertEquals(2, TlsListeners.httpsPort("X", 5, 2, Map.of()));
    }
}
