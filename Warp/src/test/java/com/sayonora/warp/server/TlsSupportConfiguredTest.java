package com.sayonora.warp.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** When the SQL-side TLS (orawire TCPS, pgwire/mywire/mssqlwire in-band, gRPC TLS) is requested by the environment. */
class TlsSupportConfiguredTest {

    @Test
    void nothingConfiguredMeansOff() {
        assertFalse(TlsSupport.configured(Map.of()));
        assertFalse(TlsSupport.configured(Map.of("WARP_TLS_CA", "/x/ca.pem", "WARP_TLS_CLIENT_AUTH", "need")));
    }

    @Test
    void theHistoricKeystoreStillTurnsItOn() {
        assertTrue(TlsSupport.configured(Map.of("WARP_TLS_KEYSTORE", "/x/warp.p12", "WARP_TLS_KEYSTORE_PASSWORD", "pw")));
    }

    @Test
    void globalPemAndSelfSignedTurnItOn() {
        assertTrue(TlsSupport.configured(Map.of("WARP_TLS_CERT", "/x/c.pem", "WARP_TLS_KEY", "/x/k.pem")));
        assertTrue(TlsSupport.configured(Map.of("WARP_TLS_SELF_SIGNED", "true")));
    }

    @Test
    void aPerListenerSettingAloneTurnsItOnAndADisableFlagTurnsThatListenerOff() {
        assertTrue(TlsSupport.configured(Map.of("WARP_PGWIRE_TLS_CERT", "/x/c.pem", "WARP_PGWIRE_TLS_KEY", "/x/k.pem")));
        Map<String, String> all = Map.of("WARP_TLS_CERT", "/x/c.pem", "WARP_TLS_KEY", "/x/k.pem",
                "WARP_GRPC_TLS_DISABLED", "true", "WARP_ORAWIRE_TLS_DISABLED", "true", "WARP_PGWIRE_TLS_DISABLED", "true",
                "WARP_MYWIRE_TLS_DISABLED", "true", "WARP_MSSQLWIRE_TLS_DISABLED", "true");
        assertFalse(TlsSupport.configured(all));
    }
}
