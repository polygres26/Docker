package com.nexagres.dms.secrets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Live-network paths (a genuine AWS/Azure/GCP round trip) aren't exercised here -- no credentials
 * or hyperscaler test doubles are available in this environment, the same reasoning this module's
 * pre-existing Vault/CyberArk resolvers were never live-tested either. What IS tested: the
 * fail-loud behavior when required env vars are absent (true in this test environment by
 * construction -- nothing in this module's test suite sets the Azure service-principal or GCP
 * service-account-key env vars), and that a plain password round-trips unchanged regardless of
 * which resolver would eventually run. */
class SecretResolverTest {

    @Test
    void plainPasswordRoundTripsUnchanged() {
        assertEquals("hunter2", SecretResolver.resolve("hunter2"));
        assertNull(SecretResolver.resolve((String) null));
    }

    @Test
    void azureKeyVaultFailsLoudWithoutServicePrincipalEnvVars() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SecretResolver.resolve("azurekv:my-vault/oracle-password"));
        assertTrue(e.getMessage().contains("AZURE_TENANT_ID"));
        assertTrue(e.getMessage().contains("AZURE_CLIENT_ID"));
        assertTrue(e.getMessage().contains("AZURE_CLIENT_SECRET"));
    }

    @Test
    void gcpSecretManagerFailsLoudWithoutServiceAccountKey() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SecretResolver.resolve("gcpsm:my-project/oracle-password"));
        assertTrue(e.getMessage().contains("GOOGLE_APPLICATION_CREDENTIALS"));
    }
}
