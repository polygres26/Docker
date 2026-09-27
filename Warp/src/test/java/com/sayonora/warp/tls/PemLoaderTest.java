package com.sayonora.warp.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** PEM chain and private-key loading with the JDK only, for every key encoding certbot/openssl/Java tooling emits. */
class PemLoaderTest {

    static Path res(String name) {
        try {
            return Path.of(PemLoaderTest.class.getResource("/tls/" + name).toURI());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void fullchainYieldsLeafThenCa() {
        X509Certificate[] chain = PemLoader.readCertificates(res("rsa.fullchain.pem"));
        assertEquals(2, chain.length);
        assertTrue(chain[0].getSubjectX500Principal().getName().contains("CN=localhost"));
        assertTrue(chain[1].getSubjectX500Principal().getName().contains("Warp Unit Test CA"));
    }

    @Test
    void pkcs8Pkcs1AndEncryptedRsaKeysAreTheSameKey() {
        PrivateKey a = PemLoader.readPrivateKey(res("rsa.pkcs8.key"), null);
        PrivateKey b = PemLoader.readPrivateKey(res("rsa.pkcs1.key"), null);
        PrivateKey c = PemLoader.readPrivateKey(res("rsa.enc.key"), "keypass");
        assertEquals("RSA", a.getAlgorithm());
        assertEquals(a, b);
        assertEquals(a, c);
    }

    @Test
    void sec1AndPkcs8EcKeysAreTheSameKey() {
        PrivateKey a = PemLoader.readPrivateKey(res("ec.sec1.key"), null);
        PrivateKey b = PemLoader.readPrivateKey(res("ec.pkcs8.key"), null);
        assertEquals("EC", a.getAlgorithm());
        assertEquals(((java.security.interfaces.ECPrivateKey) a).getS(), ((java.security.interfaces.ECPrivateKey) b).getS());
    }

    @Test
    void encryptedKeyNeedsTheRightPassword() {
        TlsException none = assertThrows(TlsException.class, () -> PemLoader.readPrivateKey(res("rsa.enc.key"), null));
        assertTrue(none.getMessage().contains("WARP_TLS_KEY_PASSWORD"));
        assertThrows(TlsException.class, () -> PemLoader.readPrivateKey(res("rsa.enc.key"), "wrong"));
    }

    @Test
    void garbageAndMissingFilesGiveOperatorReadableErrors(@TempDir Path dir) throws Exception {
        Path junk = dir.resolve("junk.pem");
        Files.writeString(junk, "not a pem at all");
        assertTrue(assertThrows(TlsException.class, () -> PemLoader.readCertificates(junk)).getMessage()
                .contains("no -----BEGIN CERTIFICATE-----"));
        assertTrue(assertThrows(TlsException.class, () -> PemLoader.readPrivateKey(junk, null)).getMessage()
                .contains("no supported private key block"));
        Path missing = dir.resolve("nope.pem");
        assertTrue(assertThrows(TlsException.class, () -> PemLoader.readCertificates(missing)).getMessage()
                .contains(missing.toString()));
    }

    @Test
    void materialLoadsEveryKeySourceAndDetectsMismatchedPair(@TempDir Path dir) {
        for (String key : new String[] {"rsa.pkcs8.key", "rsa.pkcs1.key"}) {
            TlsSettings s = TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT", res("rsa.fullchain.pem").toString(),
                    "WARP_MCP_TLS_KEY", res(key).toString()));
            assertEquals(2, TlsMaterial.load(s).chain().length);
        }
        TlsSettings ec = TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT", res("ec.pem").toString(),
                "WARP_MCP_TLS_KEY", res("ec.sec1.key").toString()));
        assertEquals("EC", TlsMaterial.load(ec).key().getAlgorithm());
        TlsSettings bad = TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_CERT", res("rsa.pem").toString(),
                "WARP_MCP_TLS_KEY", res("ec.pkcs8.key").toString()));
        assertTrue(assertThrows(TlsException.class, () -> TlsMaterial.load(bad)).getMessage().contains("does not match"));
    }

    @Test
    void pkcs12AndJksKeystoresLoadAndABadPasswordIsExplained() {
        for (String ks : new String[] {"rsa.p12", "rsa.jks"}) {
            TlsSettings s = TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_KEYSTORE", res(ks).toString(),
                    "WARP_MCP_TLS_KEYSTORE_PASSWORD", "changeit"));
            assertTrue(TlsMaterial.load(s).describe().contains("CN=localhost"), ks);
        }
        TlsSettings wrong = TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_KEYSTORE", res("rsa.p12").toString(),
                "WARP_MCP_TLS_KEYSTORE_PASSWORD", "nope"));
        assertTrue(assertThrows(TlsException.class, () -> TlsMaterial.load(wrong)).getMessage()
                .contains("wrong keystore password"));
    }

    @Test
    void selfSignedDevCertIsGeneratedInMemoryForLocalhost() {
        TlsSettings s = TlsSettings.resolve("MCP", Map.of("WARP_MCP_TLS_SELF_SIGNED", "true"));
        TlsMaterial m = TlsMaterial.load(s);
        assertTrue(m.leaf().getSubjectX500Principal().getName().contains("CN=localhost"));
        assertEquals(m.leaf().getIssuerX500Principal(), m.leaf().getSubjectX500Principal());
    }
}
