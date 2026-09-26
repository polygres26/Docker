package com.sayonora.wire.awswire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The KMS envelope and algorithm layer, with no database. */
class KmsCryptoTest {

    private static final byte[] MSG = "message to sign".getBytes(StandardCharsets.UTF_8);

    @Test
    void symmetricEnvelopeRoundTripsAndNamesItsKey() {
        byte[] key = KmsCrypto.random(32);
        byte[] blob = KmsCrypto.encrypt("11111111-2222-3333-4444-555555555555", key, "secret".getBytes(StandardCharsets.UTF_8), Map.of("a", "b"));
        assertEquals("11111111-2222-3333-4444-555555555555", KmsCrypto.keyIdOf(blob));
        assertArrayEquals("secret".getBytes(StandardCharsets.UTF_8),
                KmsCrypto.decrypt("11111111-2222-3333-4444-555555555555", key, blob, Map.of("a", "b")));
    }

    @Test
    void wrongContextWrongKeyOrTamperingFailsAuthentication() {
        byte[] key = KmsCrypto.random(32);
        byte[] blob = KmsCrypto.encrypt("k", key, "x".getBytes(StandardCharsets.UTF_8), Map.of("a", "b"));
        assertNull(KmsCrypto.decrypt("k", key, blob, Map.of("a", "other")), "encryption context is authenticated");
        assertNull(KmsCrypto.decrypt("k", key, blob, Map.of()));
        assertNull(KmsCrypto.decrypt("k", KmsCrypto.random(32), blob, Map.of("a", "b")));
        assertNull(KmsCrypto.decrypt("other-key-id", key, blob, Map.of("a", "b")), "the key id is part of the AAD");
        blob[blob.length - 1] ^= 1;
        assertNull(KmsCrypto.decrypt("k", key, blob, Map.of("a", "b")));
    }

    @Test
    void contextOrderDoesNotMatter() {
        byte[] key = KmsCrypto.random(32);
        Map<String, String> c1 = new java.util.LinkedHashMap<>();
        c1.put("x", "1");
        c1.put("y", "2");
        Map<String, String> c2 = new java.util.LinkedHashMap<>();
        c2.put("y", "2");
        c2.put("x", "1");
        assertNotEquals(null, KmsCrypto.decrypt("k", key, KmsCrypto.encrypt("k", key, MSG, c1), c2));
    }

    @Test
    void ciphertextsOfTheSamePlaintextDiffer() {
        byte[] key = KmsCrypto.random(32);
        assertFalse(java.util.Arrays.equals(KmsCrypto.encrypt("k", key, MSG, Map.of()), KmsCrypto.encrypt("k", key, MSG, Map.of())));
    }

    @Test
    void keyMaterialWrappingBindsTheKeyIdAndTheMasterKey() {
        byte[] master = KmsCrypto.deriveMaster("a master secret");
        byte[] sealed = KmsCrypto.wrap(master, "key-1", "material".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals("material".getBytes(StandardCharsets.UTF_8), KmsCrypto.unwrap(master, "key-1", sealed));
        assertThrows(IllegalStateException.class, () -> KmsCrypto.unwrap(master, "key-2", sealed));
        assertThrows(IllegalStateException.class, () -> KmsCrypto.unwrap(KmsCrypto.deriveMaster("another"), "key-1", sealed));
        assertArrayEquals(master, KmsCrypto.deriveMaster("a master secret"), "the derivation is deterministic");
    }

    @Test
    void keyIdOfRejectsForeignBlobs() {
        assertNull(KmsCrypto.keyIdOf(new byte[] {1, 2, 3}));
        assertNull(KmsCrypto.keyIdOf(null));
        assertNull(KmsCrypto.keyIdOf(new byte[64]));
    }

    @Test
    void signVerifyEcdsaEd25519AndRsa() {
        for (String[] c : new String[][] {{"ECC_NIST_P256", "ECDSA_SHA_256"}, {"ECC_NIST_P384", "ECDSA_SHA_384"},
            {"ECC_NIST_P521", "ECDSA_SHA_512"}, {"ECC_SECG_P256K1", "ECDSA_SHA_256"}, {"ECC_NIST_EDWARDS25519", "ED25519_SHA_512"},
            {"RSA_2048", "RSASSA_PKCS1_V1_5_SHA_256"}, {"RSA_2048", "RSASSA_PSS_SHA_256"}, {"SM2", "SM2DSA"}}) {
            KmsCrypto.Generated g = KmsCrypto.generatePair(c[0]);
            byte[] sig = KmsCrypto.sign(c[0], g.privateKey(), c[1], MSG, false);
            assertTrue(KmsCrypto.verify(c[0], g.publicKey(), c[1], MSG, false, sig), c[0] + " " + c[1]);
            assertFalse(KmsCrypto.verify(c[0], g.publicKey(), c[1], "other".getBytes(StandardCharsets.UTF_8), false, sig), c[0] + " tamper");
        }
    }

    @Test
    void digestModeSignsAPrecomputedHash() throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(MSG);
        for (String spec : new String[] {"ECC_NIST_P256", "RSA_2048"}) {
            String alg = spec.startsWith("RSA") ? "RSASSA_PKCS1_V1_5_SHA_256" : "ECDSA_SHA_256";
            KmsCrypto.Generated g = KmsCrypto.generatePair(spec);
            byte[] sigDigest = KmsCrypto.sign(spec, g.privateKey(), alg, digest, true);
            assertTrue(KmsCrypto.verify(spec, g.publicKey(), alg, MSG, false, sigDigest) || spec.startsWith("ECC"),
                    "an RSA PKCS#1 signature over a digest equals the signature over the message");
            assertTrue(KmsCrypto.verify(spec, g.publicKey(), alg, digest, true, sigDigest));
        }
        KmsCrypto.Generated rsa = KmsCrypto.generatePair("RSA_2048");
        assertThrows(IllegalArgumentException.class, () -> KmsCrypto.sign("RSA_2048", rsa.privateKey(), "RSASSA_PSS_SHA_256", digest, true));
    }

    @Test
    void ed25519PrehashIsADifferentSignatureOverTheDigest() throws Exception {
        KmsCrypto.Generated g = KmsCrypto.generatePair("ECC_NIST_EDWARDS25519");
        byte[] digest = MessageDigest.getInstance("SHA-512").digest(MSG);
        byte[] ph = KmsCrypto.sign("ECC_NIST_EDWARDS25519", g.privateKey(), "ED25519_PH_SHA_512", digest, true);
        assertEquals(64, ph.length);
        assertTrue(KmsCrypto.verify("ECC_NIST_EDWARDS25519", g.publicKey(), "ED25519_PH_SHA_512", digest, true, ph));
        assertFalse(KmsCrypto.verify("ECC_NIST_EDWARDS25519", g.publicKey(), "ED25519_SHA_512", digest, false, ph));
        assertEquals(44, g.publicKey().length, "a 44 byte SubjectPublicKeyInfo like real KMS");
    }

    @Test
    void rsaOaepEncryptDecrypt() {
        KmsCrypto.Generated g = KmsCrypto.generatePair("RSA_2048");
        for (String alg : new String[] {"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"}) {
            byte[] ct = KmsCrypto.rsaEncrypt(g.publicKey(), alg, MSG);
            assertEquals(256, ct.length);
            assertArrayEquals(MSG, KmsCrypto.rsaDecrypt(g.privateKey(), alg, ct));
        }
        byte[] ct = KmsCrypto.rsaEncrypt(g.publicKey(), "RSAES_OAEP_SHA_256", MSG);
        assertNull(KmsCrypto.rsaDecrypt(g.privateKey(), "RSAES_OAEP_SHA_1", ct));
    }

    @Test
    void hmacMatchesRfc4231Vector() throws Exception {
        byte[] key = new byte[20];
        java.util.Arrays.fill(key, (byte) 0x0b);
        byte[] mac = KmsCrypto.mac(key, "HMAC_SHA_256", "Hi There".getBytes(StandardCharsets.UTF_8));
        assertEquals("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7", java.util.HexFormat.of().formatHex(mac));
        assertTrue(KmsCrypto.constantTimeEquals(mac, mac.clone()));
    }
}
