package com.sayonora.warp.tls.acme;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** JWS (RFC 7515/7638/8555 s6), CSR (PKCS#10), placeholder certificate and PEM helpers. JDK only. */
public final class AcmeCrypto {

    private AcmeCrypto() {
    }

    public static String b64url(byte[] b) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    public static byte[] unb64url(String s) {
        return Base64.getUrlDecoder().decode(s);
    }

    public static byte[] sha256(byte[] b) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(b);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------- keys ----------

    public static KeyPair newEcKey() {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
            g.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());
            return g.generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static KeyPair newRsaKey() {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
            g.initialize(2048, new SecureRandom());
            return g.generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static KeyPair newKey(String keyType) {
        return "rsa2048".equals(keyType) ? newRsaKey() : newEcKey();
    }

    /** Public key from a private key (EC: recomputed by scalar multiplication is not in the JDK, so we keep pairs together). */
    public static KeyPair pairFromPkcs8(byte[] pkcs8, PublicKey known) {
        try {
            String alg = known.getAlgorithm();
            PrivateKey pk = KeyFactory.getInstance(alg).generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
            return new KeyPair(known, pk);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("bad private key: " + e.getMessage(), e);
        }
    }

    // ---------- PEM ----------

    public static String pem(String label, byte[] der) {
        String b64 = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
        return "-----BEGIN " + label + "-----\n" + b64 + "\n-----END " + label + "-----\n";
    }

    public static List<X509Certificate> parseCertChain(String pem) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            List<X509Certificate> out = new ArrayList<>();
            for (var block : com.sayonora.warp.tls.PemLoader.parse(pem)) {
                if (block.label().contains("CERTIFICATE")) {
                    out.add((X509Certificate) cf.generateCertificate(new java.io.ByteArrayInputStream(block.der())));
                }
            }
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("bad certificate PEM: " + e.getMessage(), e);
        }
    }

    // ---------- JWK / JWS ----------

    /** The public JWK of an EC P-256 key (members in lexicographic order, as RFC 7638 requires for the thumbprint). */
    public static String ecJwkJson(ECPublicKey pub) {
        return "{\"crv\":\"P-256\",\"kty\":\"EC\",\"x\":\"" + b64url(fixed32(pub.getW().getAffineX())) + "\",\"y\":\""
                + b64url(fixed32(pub.getW().getAffineY())) + "\"}";
    }

    public static String rsaJwkJson(RSAPublicKey pub) {
        return "{\"e\":\"" + b64url(unsigned(pub.getPublicExponent())) + "\",\"kty\":\"RSA\",\"n\":\""
                + b64url(unsigned(pub.getModulus())) + "\"}";
    }

    public static String jwkJson(PublicKey pub) {
        if (pub instanceof ECPublicKey e) {
            return ecJwkJson(e);
        }
        return rsaJwkJson((RSAPublicKey) pub);
    }

    /** RFC 7638 thumbprint (base64url SHA-256 of the canonical JWK). */
    public static String thumbprint(PublicKey pub) {
        return b64url(sha256(jwkJson(pub).getBytes(StandardCharsets.UTF_8)));
    }

    /** RFC 8555 s8.1: token "." thumbprint. */
    public static String keyAuthorization(String token, PublicKey accountKey) {
        return token + "." + thumbprint(accountKey);
    }

    /** dns-01 TXT value: base64url(SHA-256(keyAuthorization)). */
    public static String dnsTxtValue(String keyAuthorization) {
        return b64url(sha256(keyAuthorization.getBytes(StandardCharsets.UTF_8)));
    }

    static byte[] fixed32(BigInteger v) {
        byte[] u = unsigned(v);
        if (u.length > 32) {
            throw new IllegalArgumentException("coordinate too long");
        }
        byte[] out = new byte[32];
        System.arraycopy(u, 0, out, 32 - u.length, u.length);
        return out;
    }

    static byte[] unsigned(BigInteger v) {
        byte[] b = v.toByteArray();
        if (b.length > 1 && b[0] == 0) {
            byte[] c = new byte[b.length - 1];
            System.arraycopy(b, 1, c, 0, c.length);
            return c;
        }
        return b;
    }

    /**
     * Builds a flattened JWS (RFC 8555 s6.2). {@code kid} null means "embed the jwk" (newAccount); {@code payload} null means
     * POST-as-GET (empty payload string). ES256 signs {@code SHA256withECDSA} in raw R||S form.
     */
    public static String jws(KeyPair key, String kid, String nonce, String url, String payloadJson) {
        boolean ec = key.getPublic() instanceof ECPublicKey;
        String alg = ec ? "ES256" : "RS256";
        String protectedJson = "{\"alg\":\"" + alg + "\","
                + (kid == null ? "\"jwk\":" + jwkJson(key.getPublic()) : "\"kid\":" + jsonString(kid))
                + ",\"nonce\":" + jsonString(nonce) + ",\"url\":" + jsonString(url) + "}";
        String p64 = b64url(protectedJson.getBytes(StandardCharsets.UTF_8));
        String pl64 = payloadJson == null ? "" : b64url(payloadJson.getBytes(StandardCharsets.UTF_8));
        byte[] sig = sign(key.getPrivate(), ec ? "SHA256withECDSAinP1363Format" : "SHA256withRSA",
                (p64 + "." + pl64).getBytes(StandardCharsets.US_ASCII));
        return "{\"protected\":\"" + p64 + "\",\"payload\":\"" + pl64 + "\",\"signature\":\"" + b64url(sig) + "\"}";
    }

    /** RFC 8555 s7.3.4 external account binding: an HS256 JWS over the account JWK, keyed with the CA-issued MAC key. */
    public static String eabJws(String eabKid, byte[] hmacKey, PublicKey accountKey, String newAccountUrl) {
        String protectedJson = "{\"alg\":\"HS256\",\"kid\":" + jsonString(eabKid) + ",\"url\":" + jsonString(newAccountUrl) + "}";
        String p64 = b64url(protectedJson.getBytes(StandardCharsets.UTF_8));
        String pl64 = b64url(jwkJson(accountKey).getBytes(StandardCharsets.UTF_8));
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(hmacKey, "HmacSHA256"));
            byte[] sig = mac.doFinal((p64 + "." + pl64).getBytes(StandardCharsets.US_ASCII));
            return "{\"protected\":\"" + p64 + "\",\"payload\":\"" + pl64 + "\",\"signature\":\"" + b64url(sig) + "\"}";
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] sign(PrivateKey key, String alg, byte[] data) {
        try {
            Signature s = Signature.getInstance(alg);
            s.initSign(key);
            s.update(data);
            return s.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("cannot sign: " + e.getMessage(), e);
        }
    }

    static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    // ---------- CSR (PKCS#10, RFC 2986) ----------

    private static byte[] sigAlgId(boolean ec) {
        return ec ? Der.seq(Der.oid("1.2.840.10045.4.3.2")) : Der.seq(Der.oid("1.2.840.113549.1.1.11"), Der.NULL);
    }

    private static byte[] name(String cn) {
        return Der.seq(Der.set(Der.seq(Der.oid("2.5.4.3"), Der.utf8(cn))));
    }

    private static byte[] sanExtension(List<String> domains) {
        List<byte[]> names = new ArrayList<>();
        for (String d : domains) {
            names.add(Der.ctxPrim(2, d.getBytes(StandardCharsets.US_ASCII))); // dNSName [2]
        }
        return Der.seq(names.toArray(new byte[0][]));
    }

    /** DER PKCS#10 CertificationRequest: CN = first domain, subjectAltName = all domains. */
    public static byte[] csr(KeyPair key, List<String> domains) {
        boolean ec = key.getPublic() instanceof ECPublicKey;
        byte[] san = sanExtension(domains);
        byte[] extension = Der.seq(Der.oid("2.5.29.17"), Der.octets(san));
        byte[] extReq = Der.seq(Der.oid("1.2.840.113549.1.9.14"), Der.set(Der.seq(extension)));
        byte[] info = Der.seq(Der.integer(0), name(domains.get(0)), key.getPublic().getEncoded(),
                Der.ctx(0, extReq));
        byte[] sig = sign(key.getPrivate(), ec ? "SHA256withECDSA" : "SHA256withRSA", info);
        return Der.seq(info, sigAlgId(ec), Der.bitString(sig));
    }

    // ---------- placeholder self-signed certificate ----------

    /** OU marker of the placeholder certificate that keeps HTTPS listeners bootable until the first real issuance. */
    public static final String PLACEHOLDER_OU = "warp-acme-placeholder";

    /** A short-lived self-signed leaf (CN = first domain, SAN = all) that lets HTTPS listeners start before issuance. */
    public static byte[] placeholderCertificate(KeyPair key, List<String> domains, Instant now) {
        boolean ec = key.getPublic() instanceof ECPublicKey;
        byte[] subject = Der.seq(
                Der.set(Der.seq(Der.oid("2.5.4.11"), Der.utf8(PLACEHOLDER_OU))),
                Der.set(Der.seq(Der.oid("2.5.4.3"), Der.utf8(domains.get(0)))));
        byte[] ext = Der.seq(Der.seq(Der.oid("2.5.29.17"), Der.octets(sanExtension(domains))));
        byte[] serial = new byte[16];
        new SecureRandom().nextBytes(serial);
        serial[0] &= 0x7f;
        byte[] tbs = Der.seq(Der.ctx(0, Der.integer(2)), Der.integer(new BigInteger(1, serial)), sigAlgId(ec), subject,
                Der.seq(Der.time(now.minusSeconds(3600)), Der.time(now.plusSeconds(30L * 86400))), subject,
                key.getPublic().getEncoded(), Der.ctx(3, ext));
        byte[] sig = sign(key.getPrivate(), ec ? "SHA256withECDSA" : "SHA256withRSA", tbs);
        return Der.seq(tbs, sigAlgId(ec), Der.bitString(sig));
    }

    public static boolean isPlaceholder(X509Certificate c) {
        return c.getSubjectX500Principal().getName().contains(PLACEHOLDER_OU);
    }

    public static void requireIo(boolean ok, String msg) throws IOException {
        if (!ok) {
            throw new IOException(msg);
        }
    }

    /** Account key file: a PKCS#8 "PRIVATE KEY" block plus the matching "PUBLIC KEY" block (the JDK cannot derive an EC public key). */
    public static String keyPairPem(KeyPair kp) {
        return pem("PRIVATE KEY", kp.getPrivate().getEncoded()) + pem("PUBLIC KEY", kp.getPublic().getEncoded());
    }

    public static KeyPair keyPairFromPem(String pem) {
        byte[] priv = null;
        byte[] pub = null;
        for (var b : com.sayonora.warp.tls.PemLoader.parse(pem)) {
            if (b.label().equals("PRIVATE KEY")) {
                priv = b.der();
            } else if (b.label().equals("PUBLIC KEY")) {
                pub = b.der();
            }
        }
        if (priv == null || pub == null) {
            throw new IllegalArgumentException("key file needs both a PRIVATE KEY and a PUBLIC KEY block");
        }
        try {
            java.security.spec.X509EncodedKeySpec ps = new java.security.spec.X509EncodedKeySpec(pub);
            PublicKey pk;
            try {
                pk = KeyFactory.getInstance("EC").generatePublic(ps);
            } catch (GeneralSecurityException e) {
                pk = KeyFactory.getInstance("RSA").generatePublic(ps);
            }
            return pairFromPkcs8(priv, pk);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("bad key file: " + e.getMessage(), e);
        }
    }
}
