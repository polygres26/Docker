package com.sayonora.warp.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Proves the credential-shape logic added for Bridge mode's "transparent" login (a migrated
 * Oracle app's real credential, verified independently of the ordinary WARP_AUTH_* secret and
 * independently of the backend pool's own shared service account -- see
 * orawire.session.SessionHandler#bridgeLoginCredentials): a plain-literal password still works
 * unchanged (backward compatible), the multi-user spec still shadows the single-user fallback,
 * and -- the behavior that specifically differs from the ordinary no-arg CredentialStore() --
 * an entirely unconfigured credential set denies every login instead of silently accepting the
 * historical "orapg"/"orapg" default, since that default isn't a real backend account for a
 * Bridge-mode deployment that hasn't configured anything yet.
 */
class CredentialStoreTest {

    @Test
    void ordinaryDefaultConstructorFallsBackToSharedOrapgCredential() {
        CredentialStore store = new CredentialStore(null, null, null, true);
        assertFalse(store.isMultiUser());
        assertArrayEquals("orapg".getBytes(StandardCharsets.UTF_8), store.lookupPassword("orapg"));
        assertArrayEquals("orapg".getBytes(StandardCharsets.UTF_8), store.lookupPassword("ORAPG"));
        assertNull(store.lookupPassword("someone_else"));
    }

    @Test
    void unconfiguredBridgeStyleCredentialSetDeniesEveryLogin() {
        CredentialStore store = new CredentialStore(null, null, null, false);
        assertNull(store.lookupPassword("orapg"));
        assertNull(store.lookupPassword("system"));
        assertNull(store.lookupPassword("anyone"));
    }

    @Test
    void multiUserSpecAcceptsRealPerAppUserCredential() {
        CredentialStore store = new CredentialStore("system=OraPass1;app_user=AppSecret1", null, null, false);
        assertTrue(store.isMultiUser());
        assertArrayEquals("OraPass1".getBytes(StandardCharsets.UTF_8), store.lookupPassword("system"));
        // Case-insensitive, matching Oracle's own unquoted-identifier semantics.
        assertArrayEquals("OraPass1".getBytes(StandardCharsets.UTF_8), store.lookupPassword("SYSTEM"));
        assertArrayEquals("AppSecret1".getBytes(StandardCharsets.UTF_8), store.lookupPassword("app_user"));
        assertNull(store.lookupPassword("unknown_user"));
    }

    @Test
    void singleUserOverrideWorksWithoutTheInsecureDefault() {
        CredentialStore store = new CredentialStore(null, "system", "OraPass1", false);
        assertFalse(store.isMultiUser());
        assertArrayEquals("OraPass1".getBytes(StandardCharsets.UTF_8), store.lookupPassword("system"));
        assertNull(store.lookupPassword("orapg"));
    }
}
