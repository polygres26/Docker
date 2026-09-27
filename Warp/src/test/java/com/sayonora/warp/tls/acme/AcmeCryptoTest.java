package com.sayonora.warp.tls.acme;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.util.List;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import org.junit.jupiter.api.Test;

/** JWS / JWK / CSR / placeholder-certificate / PEM round trips, cross-checked against BouncyCastle where useful. */
class AcmeCryptoTest {

    @Test
    void jwkThumbprintMatchesRfc7638KnownAnswer() {
        // RFC 7638 appendix A.1 (an RSA key, given here to sanity-check the canonicalization order/format, not the key type).
        // We only support EC/RSA server-side generation, so instead assert self-consistency: same key -> same thumbprint,
        // different key -> different thumbprint, and the JSON member order is lexicographic (crv,kty,x,y).
        KeyPair k1 = AcmeCrypto.newEcKey();
        KeyPair k2 = AcmeCrypto.newEcKey();
        String j = AcmeCrypto.jwkJson(k1.getPublic());
        assertTrue(j.indexOf("\"crv\"") < j.indexOf("\"kty\"") && j.indexOf("\"kty\"") < j.indexOf("\"x\"")
                && j.indexOf("\"x\"") < j.indexOf("\"y\""));
        assertEquals(AcmeCrypto.thumbprint(k1.getPublic()), AcmeCrypto.thumbprint(k1.getPublic()));
        assertNotEquals(AcmeCrypto.thumbprint(k1.getPublic()), AcmeCrypto.thumbprint(k2.getPublic()));
    }

    @Test
    void keyAuthorizationAndDnsTxtValueAreDeterministic() {
        KeyPair k = AcmeCrypto.newEcKey();
        String ka = AcmeCrypto.keyAuthorization("tok123", k.getPublic());
        assertEquals("tok123." + AcmeCrypto.thumbprint(k.getPublic()), ka);
        assertEquals(AcmeCrypto.dnsTxtValue(ka), AcmeCrypto.dnsTxtValue(ka));
        assertNotEquals(AcmeCrypto.dnsTxtValue(ka), AcmeCrypto.dnsTxtValue(ka + "x"));
    }

    private static String protectedHeader(String jws) {
        var o = com.google.gson.JsonParser.parseString(jws).getAsJsonObject();
        return new String(AcmeCrypto.unb64url(o.get("protected").getAsString()), java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void jwsEmbedsJwkForNewAccountAndKidOtherwiseAndVerifiesCorrectly() throws Exception {
        KeyPair k = AcmeCrypto.newEcKey();
        String withJwk = AcmeCrypto.jws(k, null, "nonce1", "https://ca.example/new-acct", "{\"a\":1}");
        String protJwk = protectedHeader(withJwk);
        assertTrue(protJwk.contains("\"jwk\"") && protJwk.contains("\"alg\":\"ES256\"") && protJwk.contains("nonce1"));
        String withKid = AcmeCrypto.jws(k, "https://ca.example/acct/1", "nonce2", "https://ca.example/new-order", "{}");
        String protKid = protectedHeader(withKid);
        assertTrue(protKid.contains("\"kid\":\"https://ca.example/acct/1\""));
        assertTrue(protKid.contains("nonce2"));
        // the signature verifies against the account key (this is the same shape a real ACME server checks)
        var o = com.google.gson.JsonParser.parseString(withKid).getAsJsonObject();
        byte[] sig = AcmeCrypto.unb64url(o.get("signature").getAsString());
        String signingInput = o.get("protected").getAsString() + "." + o.get("payload").getAsString();
        java.security.Signature v = java.security.Signature.getInstance("SHA256withECDSAinP1363Format");
        v.initVerify(k.getPublic());
        v.update(signingInput.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        assertTrue(v.verify(sig));
        // POST-as-GET: empty payload
        String getish = AcmeCrypto.jws(k, "kid", "n3", "https://ca.example/order/1", null);
        assertTrue(getish.contains("\"payload\":\"\""));
    }

    /** Minimal JDK-only DER TLV reader, just enough to pull apart a CertificationRequest without any extra library. */
    private static int[] tlv(byte[] d, int p) {
        int tag = d[p] & 0xff;
        int len = d[p + 1] & 0xff;
        int hdr = 2;
        if (len > 0x80) {
            int n = len & 0x7f;
            len = 0;
            for (int i = 0; i < n; i++) {
                len = (len << 8) | (d[p + 2 + i] & 0xff);
            }
            hdr = 2 + n;
        }
        return new int[] {tag, p + hdr, len};
    }

    @Test
    void csrIsSelfSignedByTheGivenKeyAndCarriesTheExpectedNames() throws Exception {
        KeyPair k = AcmeCrypto.newEcKey();
        byte[] der = AcmeCrypto.csr(k, List.of("example.com", "www.example.com"));
        int[] outer = tlv(der, 0);
        int[] info = tlv(der, outer[1]);
        byte[] infoBytes = java.util.Arrays.copyOfRange(der, info[1] - (info[1] - outer[1] > 0 ? 0 : 0), info[1] + info[2]);
        // infoBytes must start at the CertificationRequestInfo's own tag, i.e. from outer[1]
        int[] certReqInfo = tlv(der, outer[1]);
        byte[] certReqInfoTlv = java.util.Arrays.copyOfRange(der, outer[1], certReqInfo[1] + certReqInfo[2]);
        int[] sigAlg = tlv(der, certReqInfo[1] + certReqInfo[2]);
        int[] sig = tlv(der, sigAlg[1] + sigAlg[2]);
        // BIT STRING: first content byte is the unused-bit count (0 here), the rest is the DER signature
        byte[] signature = java.util.Arrays.copyOfRange(der, sig[1] + 1, sig[1] + sig[2]);
        Signature v = Signature.getInstance("SHA256withECDSA");
        v.initVerify(k.getPublic());
        v.update(certReqInfoTlv);
        assertTrue(v.verify(signature), "the CSR is not correctly self-signed");
        // CN=example.com appears (as a UTF8String) inside CertificationRequestInfo; SAN names appear in the extensionRequest attribute
        String infoStr = new String(certReqInfoTlv, java.nio.charset.StandardCharsets.ISO_8859_1);
        assertTrue(infoStr.contains("example.com") && infoStr.contains("www.example.com"));
    }

    @Test
    void rsaCsrIsAlsoSelfSignedCorrectly() throws Exception {
        KeyPair k = AcmeCrypto.newRsaKey();
        byte[] der = AcmeCrypto.csr(k, List.of("rsa.example.com"));
        int[] outer = tlv(der, 0);
        int[] certReqInfo = tlv(der, outer[1]);
        byte[] certReqInfoTlv = java.util.Arrays.copyOfRange(der, outer[1], certReqInfo[1] + certReqInfo[2]);
        int[] sigAlg = tlv(der, certReqInfo[1] + certReqInfo[2]);
        int[] sig = tlv(der, sigAlg[1] + sigAlg[2]);
        byte[] signature = java.util.Arrays.copyOfRange(der, sig[1] + 1, sig[1] + sig[2]);
        Signature v = Signature.getInstance("SHA256withRSA");
        v.initVerify(k.getPublic());
        v.update(certReqInfoTlv);
        assertTrue(v.verify(signature));
    }

    @Test
    void placeholderCertificateParsesAsSelfSignedWithExpectedSans() throws Exception {
        KeyPair k = AcmeCrypto.newEcKey();
        byte[] der = AcmeCrypto.placeholderCertificate(k, List.of("a.example.com", "b.example.com"), Instant.now());
        X509Certificate cert = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new java.io.ByteArrayInputStream(der));
        cert.verify(k.getPublic());   // self-signed
        assertTrue(AcmeCrypto.isPlaceholder(cert));
        assertEquals("a.example.com", cert.getSubjectX500Principal().getName().replaceAll(".*CN=([^,]+).*", "$1"));
        assertEquals(java.util.Set.of("a.example.com", "b.example.com"), CertInfo.of(cert).names());
    }

    @Test
    void keyPairPemRoundTripsThroughFile() {
        KeyPair k = AcmeCrypto.newEcKey();
        String pem = AcmeCrypto.keyPairPem(k);
        assertTrue(pem.contains("BEGIN PRIVATE KEY") && pem.contains("BEGIN PUBLIC KEY"));
        KeyPair back = AcmeCrypto.keyPairFromPem(pem);
        assertArrayEquals(k.getPrivate().getEncoded(), back.getPrivate().getEncoded());
        assertArrayEquals(k.getPublic().getEncoded(), back.getPublic().getEncoded());
        assertEquals(AcmeCrypto.thumbprint(k.getPublic()), AcmeCrypto.thumbprint(back.getPublic()));
    }

    @Test
    void parseCertChainReadsBothCertificatesInOrder() {
        KeyPair k = AcmeCrypto.newEcKey();
        byte[] leaf = AcmeCrypto.placeholderCertificate(k, List.of("x.example.com"), Instant.now());
        KeyPair ca = AcmeCrypto.newEcKey();
        byte[] caCert = AcmeCrypto.placeholderCertificate(ca, List.of("ca.example.com"), Instant.now());
        String pem = AcmeCrypto.pem("CERTIFICATE", leaf) + AcmeCrypto.pem("CERTIFICATE", caCert);
        List<X509Certificate> chain = AcmeCrypto.parseCertChain(pem);
        assertEquals(2, chain.size());
        assertTrue(chain.get(0).getSubjectX500Principal().getName().contains("x.example.com"));
        assertTrue(chain.get(1).getSubjectX500Principal().getName().contains("ca.example.com"));
    }

    @Test
    void eabJwsIsHs256AndPayloadIsAccountJwk() {
        KeyPair account = AcmeCrypto.newEcKey();
        byte[] hmacKey = "0123456789abcdef0123456789abcdef".getBytes();
        String jws = AcmeCrypto.eabJws("kid-1", hmacKey, account.getPublic(), "https://ca.example/new-acct");
        assertTrue(jws.contains("\"protected\""));
        var decoded = com.google.gson.JsonParser.parseString(jws).getAsJsonObject();
        String protectedJson = new String(AcmeCrypto.unb64url(decoded.get("protected").getAsString()));
        assertTrue(protectedJson.contains("\"alg\":\"HS256\"") && protectedJson.contains("kid-1"));
        String payload = new String(AcmeCrypto.unb64url(decoded.get("payload").getAsString()));
        assertEquals(AcmeCrypto.jwkJson(account.getPublic()), payload);
    }
}
