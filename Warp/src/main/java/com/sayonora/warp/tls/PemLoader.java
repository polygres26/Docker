package com.sayonora.warp.tls;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Loads PEM certificate chains and PEM private keys with the JDK only (no BouncyCastle). Private keys may be PKCS#8
 * ({@code BEGIN PRIVATE KEY}), PKCS#1 RSA ({@code BEGIN RSA PRIVATE KEY}), SEC1 EC ({@code BEGIN EC PRIVATE KEY}) or
 * PKCS#8-encrypted ({@code BEGIN ENCRYPTED PRIVATE KEY}, needs a password). Let's Encrypt / certbot output
 * ({@code fullchain.pem}, {@code privkey.pem}) loads as-is.
 */
public final class PemLoader {

    private PemLoader() {
    }

    /** One {@code -----BEGIN X-----} block: its label and decoded DER bytes. */
    public record Block(String label, byte[] der) {
    }

    public static List<Block> parse(String pem) {
        List<Block> out = new ArrayList<>();
        int pos = 0;
        while (true) {
            int begin = pem.indexOf("-----BEGIN ", pos);
            if (begin < 0) {
                break;
            }
            int labelEnd = pem.indexOf("-----", begin + 11);
            if (labelEnd < 0) {
                throw new TlsException("malformed PEM: unterminated BEGIN line");
            }
            String label = pem.substring(begin + 11, labelEnd).trim();
            String endMarker = "-----END " + label + "-----";
            int end = pem.indexOf(endMarker, labelEnd);
            if (end < 0) {
                throw new TlsException("malformed PEM: no matching " + endMarker);
            }
            String body = pem.substring(labelEnd + 5, end);
            // Traditional encrypted PEM (Proc-Type/DEK-Info headers) is not supported; only PKCS#8 encryption is.
            if (body.contains("Proc-Type:")) {
                throw new TlsException("legacy OpenSSL-encrypted PEM key (Proc-Type/DEK-Info) is not supported; convert it: "
                        + "openssl pkcs8 -topk8 -in key.pem -out key-pkcs8.pem  (or remove the passphrase)");
            }
            String b64 = body.replaceAll("\\s+", "");
            try {
                out.add(new Block(label, Base64.getDecoder().decode(b64)));
            } catch (IllegalArgumentException e) {
                throw new TlsException("malformed PEM: bad base64 in " + label + " block", e);
            }
            pos = end + endMarker.length();
        }
        return out;
    }

    public static X509Certificate[] readCertificates(Path file) {
        String text = readText(file, "certificate");
        List<X509Certificate> certs = new ArrayList<>();
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            for (Block b : parse(text)) {
                if (b.label().equals("CERTIFICATE") || b.label().equals("TRUSTED CERTIFICATE")
                        || b.label().equals("X509 CERTIFICATE")) {
                    certs.add((X509Certificate) cf.generateCertificate(new ByteArrayInputStream(b.der())));
                }
            }
        } catch (GeneralSecurityException e) {
            throw new TlsException("cannot parse certificate(s) in " + file + ": " + e.getMessage(), e);
        }
        if (certs.isEmpty()) {
            throw new TlsException("no -----BEGIN CERTIFICATE----- block found in " + file);
        }
        return certs.toArray(new X509Certificate[0]);
    }

    public static PrivateKey readPrivateKey(Path file, String password) {
        String text = readText(file, "private key");
        for (Block b : parse(text)) {
            switch (b.label()) {
                case "PRIVATE KEY":
                    return fromPkcs8(b.der(), file);
                case "RSA PRIVATE KEY":
                    return fromPkcs8(wrapPkcs1Rsa(b.der()), file);
                case "EC PRIVATE KEY":
                    return fromPkcs8(wrapSec1Ec(b.der()), file);
                case "ENCRYPTED PRIVATE KEY":
                    return fromPkcs8(decrypt(b.der(), password, file), file);
                default:
                    break;
            }
        }
        throw new TlsException("no supported private key block (PRIVATE KEY, RSA PRIVATE KEY, EC PRIVATE KEY, "
                + "ENCRYPTED PRIVATE KEY) found in " + file);
    }

    private static String readText(Path file, String what) {
        try {
            return Files.readString(file, StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            throw new TlsException("cannot read TLS " + what + " file " + file + ": " + e.getMessage(), e);
        }
    }

    private static byte[] decrypt(byte[] der, String password, Path file) {
        if (password == null || password.isEmpty()) {
            throw new TlsException("private key " + file + " is encrypted; set WARP_TLS_KEY_PASSWORD");
        }
        try {
            EncryptedPrivateKeyInfo info = new EncryptedPrivateKeyInfo(der);
            SecretKey key = SecretKeyFactory.getInstance(info.getAlgName())
                    .generateSecret(new PBEKeySpec(password.toCharArray()));
            Cipher cipher = Cipher.getInstance(info.getAlgName());
            cipher.init(Cipher.DECRYPT_MODE, key, info.getAlgParameters());
            return info.getKeySpec(cipher).getEncoded();
        } catch (GeneralSecurityException | IOException e) {
            throw new TlsException("cannot decrypt private key " + file + " (wrong WARP_TLS_KEY_PASSWORD?): "
                    + e.getMessage(), e);
        }
    }

    private static PrivateKey fromPkcs8(byte[] pkcs8, Path file) {
        GeneralSecurityException last = null;
        for (String alg : new String[] {"RSA", "EC", "Ed25519", "RSASSA-PSS", "DSA"}) {
            try {
                return KeyFactory.getInstance(alg).generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
            } catch (GeneralSecurityException e) {
                last = e;
            }
        }
        throw new TlsException("cannot parse private key " + file + " (unsupported algorithm or corrupt key): "
                + (last == null ? "" : last.getMessage()), last);
    }

    // ---- minimal DER ----

    private static final byte[] OID_RSA = {0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01};
    private static final byte[] OID_EC_PUBLIC_KEY = {0x06, 0x07, 0x2a, (byte) 0x86, 0x48, (byte) 0xce, 0x3d, 0x02, 0x01};

    static byte[] wrapPkcs1Rsa(byte[] pkcs1) {
        byte[] algId = seq(concat(OID_RSA, new byte[] {0x05, 0x00}));
        return seq(concat(new byte[] {0x02, 0x01, 0x00}, algId, tlv(0x04, pkcs1)));
    }

    static byte[] wrapSec1Ec(byte[] sec1) {
        byte[] curveOid = findEcCurveOid(sec1);
        byte[] algId = seq(concat(OID_EC_PUBLIC_KEY, curveOid));
        return seq(concat(new byte[] {0x02, 0x01, 0x00}, algId, tlv(0x04, sec1)));
    }

    /** ECPrivateKey ::= SEQ { INT 1, OCTET STRING key, [0] ECParameters OPTIONAL, [1] BIT STRING OPTIONAL }. */
    private static byte[] findEcCurveOid(byte[] sec1) {
        int[] p = {0};
        expect(sec1, p, 0x30);
        readLen(sec1, p);
        expect(sec1, p, 0x02);
        int versionLen = readLen(sec1, p);
        p[0] += versionLen;
        expect(sec1, p, 0x04);
        int keyLen = readLen(sec1, p);
        p[0] += keyLen;
        if (p[0] < sec1.length && (sec1[p[0]] & 0xff) == 0xa0) {
            p[0]++;
            readLen(sec1, p);
            int start = p[0];
            expect(sec1, p, 0x06);
            int len = readLen(sec1, p);
            byte[] out = new byte[p[0] + len - start];
            System.arraycopy(sec1, start, out, 0, out.length);
            return out;
        }
        throw new TlsException("EC PRIVATE KEY has no embedded curve parameters; convert it to PKCS#8: "
                + "openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem");
    }

    private static void expect(byte[] d, int[] p, int tag) {
        if (p[0] >= d.length || (d[p[0]] & 0xff) != tag) {
            throw new TlsException("corrupt EC private key (unexpected DER structure)");
        }
        p[0]++;
    }

    private static int readLen(byte[] d, int[] p) {
        int b = d[p[0]++] & 0xff;
        if (b < 0x80) {
            return b;
        }
        int n = b & 0x7f;
        int len = 0;
        for (int i = 0; i < n; i++) {
            len = (len << 8) | (d[p[0]++] & 0xff);
        }
        return len;
    }

    private static byte[] seq(byte[] content) {
        return tlv(0x30, content);
    }

    private static byte[] tlv(int tag, byte[] content) {
        byte[] len;
        if (content.length < 0x80) {
            len = new byte[] {(byte) content.length};
        } else if (content.length < 0x100) {
            len = new byte[] {(byte) 0x81, (byte) content.length};
        } else if (content.length < 0x10000) {
            len = new byte[] {(byte) 0x82, (byte) (content.length >> 8), (byte) content.length};
        } else {
            len = new byte[] {(byte) 0x83, (byte) (content.length >> 16), (byte) (content.length >> 8), (byte) content.length};
        }
        return concat(new byte[] {(byte) tag}, len, content);
    }

    private static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) {
            n += p.length;
        }
        byte[] out = new byte[n];
        int o = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, o, p.length);
            o += p.length;
        }
        return out;
    }
}
