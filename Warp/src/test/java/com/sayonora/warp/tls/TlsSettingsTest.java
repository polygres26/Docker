package com.sayonora.warp.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TlsSettingsTest {

    @Test
    void nothingConfiguredMeansNoTls() {
        assertNull(TlsSettings.resolve("MCP", Map.of()));
        assertNull(TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT", " ")));
    }

    @Test
    void globalConfigAppliesToEveryListenerAndAListenerPrefixOverridesIt() {
        Map<String, String> env = Map.of("WARP_TLS_CERT", "/g/cert.pem", "WARP_TLS_KEY", "/g/key.pem",
                "WARP_MCP_TLS_KEYSTORE", "/m/ks.p12", "WARP_MCP_TLS_KEYSTORE_PASSWORD", "pw");
        TlsSettings admin = TlsSettings.resolve("ADMIN", env);
        assertEquals(TlsSettings.Source.PEM, admin.source());
        assertEquals(Path.of("/g/cert.pem"), admin.cert());
        TlsSettings mcp = TlsSettings.resolve("MCP", env);
        assertEquals(TlsSettings.Source.KEYSTORE, mcp.source());
        assertEquals("pw", mcp.keystorePassword());
    }

    @Test
    void existingGlobalKeystoreVariableIsAcceptedAsASuperset() {
        TlsSettings s = TlsSettings.resolve("A2A", Map.of("WARP_TLS_KEYSTORE", "/etc/warp.p12",
                "WARP_TLS_KEYSTORE_PASSWORD", "changeit"));
        assertEquals(TlsSettings.Source.KEYSTORE, s.source());
        assertEquals("changeit", s.keystorePassword());
    }

    @Test
    void perListenerDisableBeatsGlobalConfig() {
        assertNull(TlsSettings.resolve("A2A", Map.of("WARP_TLS_SELF_SIGNED", "true", "WARP_A2A_TLS_DISABLED", "true")));
    }

    @Test
    void certWithoutKeyAndClientAuthWithoutCaAreRejected() {
        assertThrows(TlsException.class, () -> TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT", "/c")));
        assertThrows(TlsException.class, () -> TlsSettings.resolve("MCP", Map.of("WARP_TLS_SELF_SIGNED", "true",
                "WARP_TLS_CLIENT_AUTH", "need")));
        assertThrows(TlsException.class, () -> TlsSettings.resolve("MCP", Map.of("WARP_TLS_SELF_SIGNED", "true",
                "WARP_TLS_MIN_VERSION", "1.0")));
    }

    @Test
    void tunablesAndDefaults() {
        TlsSettings s = TlsSettings.resolve("MCP", Map.of("WARP_TLS_SELF_SIGNED", "true"));
        assertEquals("TLSv1.2", s.minVersion());
        assertEquals(TlsSettings.ClientAuth.NONE, s.clientAuth());
        assertEquals(30, s.reloadSeconds());
        TlsSettings t = TlsSettings.resolve("MCP", Map.of("WARP_TLS_SELF_SIGNED", "true", "WARP_TLS_MIN_VERSION", "TLS1.3",
                "WARP_MCP_TLS_RELOAD_SECONDS", "0", "WARP_TLS_CLIENT_AUTH", "want", "WARP_TLS_CA", "/ca.pem"));
        assertEquals("TLSv1.3", t.minVersion());
        assertEquals(0, t.reloadSeconds());
        assertEquals(TlsSettings.ClientAuth.WANT, t.clientAuth());
    }
}
