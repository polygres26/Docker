package com.sayonora.warp.secrets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FieldCipherTest {

    private static String newKey() {
        byte[] raw = new byte[32];
        new SecureRandom().nextBytes(raw);
        return Base64.getEncoder().encodeToString(raw);
    }

    @AfterEach
    void restore() {
        FieldCipher.useKeysForTesting(null, null);
        FieldCipher.requireKeyForTesting(null);
    }

    /** What the cipher wrote before key ids existed: encv1:base64(iv || ciphertext). */
    private static String legacyV1(String key, String plaintext) throws Exception {
        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(Base64.getDecoder().decode(key), "AES"), new GCMParameterSpec(128, iv));
        byte[] ct = c.doFinal(plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        byte[] all = new byte[iv.length + ct.length];
        System.arraycopy(iv, 0, all, 0, iv.length);
        System.arraycopy(ct, 0, all, iv.length, ct.length);
        return "encv1:" + Base64.getEncoder().encodeToString(all);
    }

    @Test
    void aValueRoundTripsAndNamesItsKey() {
        String a = newKey();
        FieldCipher.useKeysForTesting(a, null);
        String stored = FieldCipher.encrypt("s3cret-password");
        assertTrue(stored.matches("encv2:[0-9a-f]{8}:.+"), stored);
        assertFalse(stored.contains("s3cret"));
        assertEquals(FieldCipher.activeKeyId(), FieldCipher.protection(stored));
        assertEquals("s3cret-password", FieldCipher.decrypt(stored));
        assertNotEquals(stored, FieldCipher.encrypt("s3cret-password"), "a fresh IV every time");
        assertTrue(FieldCipher.isCurrent(stored));
        assertTrue(FieldCipher.enabled());
    }

    @Test
    void plaintextAndBlankPassThrough() {
        FieldCipher.useKeysForTesting(newKey(), null);
        assertEquals("plain", FieldCipher.decrypt("plain"));
        assertNull(FieldCipher.decrypt(null));
        assertEquals("", FieldCipher.encrypt(""));
        assertNull(FieldCipher.encrypt(null));
        assertEquals("plaintext", FieldCipher.protection("plain"));
        assertFalse(FieldCipher.isCurrent("plain"), "plaintext is not current once a key is configured");
    }

    @Test
    void withoutAKeyTheDefaultStoresPlaintextAndFailClosedRefuses() {
        FieldCipher.useKeysForTesting("", "");
        assertFalse(FieldCipher.enabled());
        assertEquals("pw", FieldCipher.encrypt("pw"), "default: backward compatible, plaintext");
        FieldCipher.requireKeyForTesting(true);
        var e = assertThrows(IllegalStateException.class, () -> FieldCipher.encrypt("pw"));
        assertTrue(e.getMessage().contains("refusing to store a secret in plaintext"), e.getMessage());
        var start = assertThrows(IllegalStateException.class, FieldCipher::validateAtStartup);
        assertTrue(start.getMessage().contains("refusing to start"), start.getMessage());
        assertEquals("", FieldCipher.encrypt(""), "nothing to protect, nothing to refuse");
        FieldCipher.useKeysForTesting(newKey(), null);
        FieldCipher.validateAtStartup(); // a key is configured: starts
    }

    @Test
    void rotationNeedsOnlyThePreviousKeyListedUntilEverythingIsReencrypted() {
        String a = newKey();
        String b = newKey();
        FieldCipher.useKeysForTesting(a, null);
        String underA = FieldCipher.encrypt("pw");
        String idA = FieldCipher.activeKeyId();

        FieldCipher.useKeysForTesting(b, a); // B is active, A can still be read
        String idB = FieldCipher.activeKeyId();
        assertNotEquals(idA, idB);
        assertEquals(List.of(idA), FieldCipher.previousKeyIds());
        assertEquals("pw", FieldCipher.decrypt(underA));
        assertFalse(FieldCipher.isCurrent(underA));
        String underB = FieldCipher.reencrypt(underA);
        assertEquals(idB, FieldCipher.protection(underB));
        assertEquals(underB, FieldCipher.reencrypt(underB), "already current: untouched");

        FieldCipher.useKeysForTesting(b, null); // the previous key can be dropped once nothing uses it
        assertEquals("pw", FieldCipher.decrypt(underB));
        var e = assertThrows(IllegalStateException.class, () -> FieldCipher.decrypt(underA));
        assertTrue(e.getMessage().contains(idA) && e.getMessage().contains("SAYONORA_ENCRYPTION_KEY_PREVIOUS"), e.getMessage());
    }

    @Test
    void theOldFormatWithoutAKeyIdStillOpensAndIsUpgradedOnReencrypt() throws Exception {
        String a = newKey();
        String b = newKey();
        String v1 = legacyV1(a, "legacy-pw");
        FieldCipher.useKeysForTesting(a, null);
        assertEquals("legacy-pw", FieldCipher.decrypt(v1));
        assertEquals("encv1", FieldCipher.protection(v1));
        assertFalse(FieldCipher.isCurrent(v1));
        FieldCipher.useKeysForTesting(b, a);
        assertEquals("legacy-pw", FieldCipher.decrypt(v1), "found by trying the previous key");
        String upgraded = FieldCipher.reencrypt(v1);
        assertEquals(FieldCipher.activeKeyId(), FieldCipher.protection(upgraded));
        assertEquals("legacy-pw", FieldCipher.decrypt(upgraded));
        FieldCipher.useKeysForTesting(b, null);
        var e = assertThrows(IllegalStateException.class, () -> FieldCipher.decrypt(v1));
        assertTrue(e.getMessage().contains("none of the configured keys"), e.getMessage());
    }

    @Test
    void anEncryptedValueWithNoKeyConfiguredIsAnErrorNotGarbage() {
        String a = newKey();
        FieldCipher.useKeysForTesting(a, null);
        String stored = FieldCipher.encrypt("pw");
        FieldCipher.useKeysForTesting("", "");
        var e = assertThrows(IllegalStateException.class, () -> FieldCipher.decrypt(stored));
        assertTrue(e.getMessage().contains("SAYONORA_ENCRYPTION_KEY is not set"), e.getMessage());
    }

    @Test
    void aMalformedKeyFailsLoudlyAndNamesTheVariable() {
        var shortKey = assertThrows(IllegalStateException.class,
                () -> FieldCipher.useKeysForTesting(Base64.getEncoder().encodeToString(new byte[16]), null));
        assertTrue(shortKey.getMessage().contains("SAYONORA_ENCRYPTION_KEY") && shortKey.getMessage().contains("32 bytes"), shortKey.getMessage());
        var notBase64 = assertThrows(IllegalStateException.class, () -> FieldCipher.useKeysForTesting("not base64!!", null));
        assertTrue(notBase64.getMessage().contains("base64"), notBase64.getMessage());
        var badPrevious = assertThrows(IllegalStateException.class, () -> FieldCipher.useKeysForTesting(newKey(), "short"));
        assertTrue(badPrevious.getMessage().contains("SAYONORA_ENCRYPTION_KEY_PREVIOUS"), badPrevious.getMessage());
    }

    @Test
    void reencryptWithNoActiveKeyLeavesTheValueAlone() {
        FieldCipher.useKeysForTesting("", "");
        assertEquals("pw", FieldCipher.reencrypt("pw"));
    }
}
