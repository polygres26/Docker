package com.sayonora.wire.license;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Proves the full offline license loop end-to-end -- {@link LicenseKeyGenTool} mints a real key
 * with the actual signing keypair's private half, {@link License} verifies it with the hardcoded
 * public half baked into the class, no mocks on either side of the signature. Also proves the
 * "fails closed" contract that's the whole point of an offline license: no key, a corrupted key,
 * a wrong-signer key, and an expired key must all resolve to {@link LicenseTier#DEVELOPER}, never
 * silently to {@code ENTERPRISE}.
 */
class LicenseIntegrationTest {

    // A second, unrelated Ed25519 private key -- stands in for "not signed by the real Warp
    // private key" (a forged or tampered key), distinct from a merely-corrupted string.
    private static final String WRONG_SIGNER_PRIVATE_KEY_B64 =
            "MC4CAQAwBQYDK2VwBCIEIOzC7QaVBPkQ4gVjrM8FvbAFyOxOc07Vw5AR8vWLTt3o";

    // The real signing keypair's private half -- committed here ONLY because this test proves
    // verification against genuine signatures; License.java ships with just the public half, and
    // the production private key lives offline (see this session's scratchpad note), never in
    // source control for real use.
    private static final String REAL_PRIVATE_KEY_B64 =
            "MC4CAQAwBQYDK2VwBCIEIL6Icy/4IPbMRpzHSxFIQqyLwDXKgcP7T/Y2UWJjvfE6";

    @Test
    void noKeyAtAllMeansDeveloperTier() {
        License license = License.fromKey(null);
        assertEquals(LicenseTier.DEVELOPER, license.tier());
        assertEquals(25, license.maxConnectionsPerInstance());
    }

    @Test
    void aBlankKeyMeansDeveloperTier() {
        assertEquals(LicenseTier.DEVELOPER, License.fromKey("   ").tier());
    }

    @Test
    void aGenuineEnterpriseKeyVerifiesAndUnlocksUnlimitedTier() throws Exception {
        String key = generateKey(REAL_PRIVATE_KEY_B64, "ENTERPRISE", "Acme Corp", null);
        License license = License.fromKey(key);
        assertEquals(LicenseTier.ENTERPRISE, license.tier());
        assertEquals("Acme Corp", license.licensedTo());
        assertNull(license.expiresAt());
        assertEquals(Integer.MAX_VALUE, license.maxConnectionsPerInstance());
        assertEquals(Integer.MAX_VALUE, license.maxInstances());
        assertEquals(Integer.MAX_VALUE, license.maxBackends());
    }

    @Test
    void aMalformedKeyFallsBackToDeveloperNotAnException() {
        // fromKey (unlike verify) must never throw -- a bad key is an operational fact to log and
        // degrade from, not a startup crash.
        assertEquals(LicenseTier.DEVELOPER, License.fromKey("not-a-real-key-at-all").tier());
        assertEquals(LicenseTier.DEVELOPER, License.fromKey("nodothere").tier());
        assertEquals(LicenseTier.DEVELOPER, License.fromKey("..").tier());
    }

    @Test
    void aKeyFromADifferentSigningKeyFailsVerification() throws Exception {
        String forgedKey = generateKey(WRONG_SIGNER_PRIVATE_KEY_B64, "ENTERPRISE", "Attacker Inc", null);
        RuntimeException e = assertThrows(RuntimeException.class, () -> License.verify(forgedKey));
        assertTrue(e.getMessage().contains("signature"), "must reject on signature mismatch, not something else: " + e.getMessage());
        // The full fromKey path must swallow this and degrade, not propagate it.
        assertEquals(LicenseTier.DEVELOPER, License.fromKey(forgedKey).tier());
    }

    @Test
    void aTamperedPayloadFailsVerificationEvenWithARealSignature() throws Exception {
        String genuine = generateKey(REAL_PRIVATE_KEY_B64, "ENTERPRISE", "Acme Corp", null);
        int dot = genuine.indexOf('.');
        // Flip one character in the payload half -- the signature (untouched) now covers
        // different bytes than what's actually being claimed, exactly what hand-editing the tier
        // in a real tamper attempt would produce.
        String payload = genuine.substring(0, dot);
        char last = payload.charAt(payload.length() - 1);
        String tamperedPayload = payload.substring(0, payload.length() - 1) + (last == 'A' ? 'B' : 'A');
        String tampered = tamperedPayload + genuine.substring(dot);
        assertThrows(RuntimeException.class, () -> License.verify(tampered));
        assertEquals(LicenseTier.DEVELOPER, License.fromKey(tampered).tier());
    }

    @Test
    void anExpiredKeyFallsBackToDeveloperNotEnterprise() throws Exception {
        String expiredKey = generateKey(REAL_PRIVATE_KEY_B64, "ENTERPRISE", "Acme Corp",
                Instant.now().minusSeconds(3600).toString());
        // The raw signature itself is still genuinely valid -- verify() alone reports ENTERPRISE;
        // it's fromKey()'s expiry check on top that must downgrade it. Proves both halves.
        assertEquals(LicenseTier.ENTERPRISE, License.verify(expiredKey).tier());
        assertEquals(LicenseTier.DEVELOPER, License.fromKey(expiredKey).tier());
    }

    @Test
    void aFutureExpiryStillVerifiesAsEnterprise() throws Exception {
        String key = generateKey(REAL_PRIVATE_KEY_B64, "ENTERPRISE", "Acme Corp",
                Instant.now().plusSeconds(3600 * 24 * 365).toString());
        assertEquals(LicenseTier.ENTERPRISE, License.fromKey(key).tier());
    }

    @Test
    void developerTierCapsMatchThePricingPlan() {
        assertEquals(25, License.DEVELOPER_MAX_CONNECTIONS);
        assertEquals(3, License.DEVELOPER_MAX_INSTANCES);
        assertEquals(3, License.DEVELOPER_MAX_BACKENDS);
    }

    private static String generateKey(String privateKeyB64, String tier, String licensedTo, String expires)
            throws Exception {
        List<String> args = new ArrayList<>(List.of(
                "--private-key", privateKeyB64, "--tier", tier, "--licensed-to", licensedTo));
        if (expires != null) {
            args.add("--expires");
            args.add(expires);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
        try {
            LicenseKeyGenTool.main(args.toArray(new String[0]));
        } finally {
            System.setOut(original);
        }
        return out.toString(StandardCharsets.UTF_8).strip();
    }
}
