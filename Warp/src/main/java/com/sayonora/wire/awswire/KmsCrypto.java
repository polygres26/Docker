package com.sayonora.wire.awswire;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.EdDSAParameterSpec;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.PSSParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PSource;
import javax.crypto.spec.SecretKeySpec;

/**
 * The cryptography behind the KMS emulation, with no database or HTTP in it (so it is unit-tested on its own).
 *
 * <p><b>Envelope</b>: key material at rest is AES-256-GCM encrypted under a master key derived (PBKDF2-HMAC-SHA256) from
 * {@code WARP_KMS_MASTER_KEY}; a symmetric {@code Encrypt} produces a self-describing blob
 * {@code 0x01 | len(keyId) | keyId | 12 byte nonce | AES-256-GCM(ciphertext | tag)} whose AAD is the key id plus the sorted
 * encryption context, so a blob decrypts without naming the key, and a wrong context or a tampered blob fails. This is an
 * emulator, not an HSM: the master key lives in the Warp process, and no key material ever leaves through the API except
 * the plaintext data keys the API is defined to return.
 */
public final class KmsCrypto {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final byte VERSION = 1;

    private KmsCrypto() {
    }

    public static byte[] random(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    /** Derives the 256-bit master wrapping key from the configured secret. */
    public static byte[] deriveMaster(String secret) {
        try {
            SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            return f.generateSecret(new PBEKeySpec(secret.toCharArray(), "warp-kms-master-v1".getBytes(StandardCharsets.UTF_8), 100_000,
                    256)).getEncoded();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] contextBytes(String keyId, Map<String, String> context) {
        StringBuilder sb = new StringBuilder(keyId).append('\n');
        if (context != null) {
            new TreeMap<>(context).forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] gcm(int mode, byte[] key, byte[] nonce, byte[] aad, byte[] data) throws Exception {
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce));
        c.updateAAD(aad);
        return c.doFinal(data);
    }

    /** Seals raw key material under the master key (AAD = the key id). */
    public static byte[] wrap(byte[] master, String keyId, byte[] material) {
        try {
            byte[] nonce = random(12);
            byte[] ct = gcm(Cipher.ENCRYPT_MODE, master, nonce, keyId.getBytes(StandardCharsets.UTF_8), material);
            byte[] out = new byte[12 + ct.length];
            System.arraycopy(nonce, 0, out, 0, 12);
            System.arraycopy(ct, 0, out, 12, ct.length);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] unwrap(byte[] master, String keyId, byte[] sealed) {
        try {
            return gcm(Cipher.DECRYPT_MODE, master, Arrays.copyOfRange(sealed, 0, 12), keyId.getBytes(StandardCharsets.UTF_8),
                    Arrays.copyOfRange(sealed, 12, sealed.length));
        } catch (Exception e) {
            throw new IllegalStateException("key material cannot be unsealed: the master key changed or the row was altered");
        }
    }

    /** Symmetric encrypt: the self-describing blob. */
    public static byte[] encrypt(String keyId, byte[] aesKey, byte[] plaintext, Map<String, String> context) {
        try {
            byte[] id = keyId.getBytes(StandardCharsets.UTF_8);
            byte[] nonce = random(12);
            byte[] ct = gcm(Cipher.ENCRYPT_MODE, aesKey, nonce, contextBytes(keyId, context), plaintext);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(VERSION);
            out.write(id.length);
            out.write(id, 0, id.length);
            out.write(nonce, 0, 12);
            out.write(ct, 0, ct.length);
            return out.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** The key id a symmetric blob was produced under, or null when it is not one of ours. */
    public static String keyIdOf(byte[] blob) {
        if (blob == null || blob.length < 2 + 12 + 16 || blob[0] != VERSION) {
            return null;
        }
        int n = blob[1] & 0xff;
        if (blob.length < 2 + n + 12 + 16) {
            return null;
        }
        return new String(blob, 2, n, StandardCharsets.UTF_8);
    }

    /** @return the plaintext, or null when authentication fails (wrong key, wrong context, tampered) */
    public static byte[] decrypt(String keyId, byte[] aesKey, byte[] blob, Map<String, String> context) {
        try {
            int n = blob[1] & 0xff;
            byte[] nonce = Arrays.copyOfRange(blob, 2 + n, 2 + n + 12);
            byte[] ct = Arrays.copyOfRange(blob, 2 + n + 12, blob.length);
            return gcm(Cipher.DECRYPT_MODE, aesKey, nonce, contextBytes(keyId, context), ct);
        } catch (Exception e) {
            return null;
        }
    }

    // --------------------------------------------------------------------------------------------- key generation

    public record Generated(byte[] privateKey, byte[] publicKey) {
    }

    static void bc() {
        if (Security.getProvider("BC") == null) {
            try {
                Security.addProvider((java.security.Provider) Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider")
                        .getDeclaredConstructor().newInstance());
            } catch (ReflectiveOperationException | LinkageError e) {
                throw new IllegalStateException("ECC_SECG_P256K1 needs the BouncyCastle provider, which is not on the classpath");
            }
        }
    }

    /** Generates an asymmetric key pair for {@code keySpec} (PKCS#8 private, X.509 public). */
    public static Generated generatePair(String keySpec) {
        try {
            KeyPairGenerator g;
            switch (keySpec) {
                case "RSA_2048", "RSA_3072", "RSA_4096" -> {
                    g = KeyPairGenerator.getInstance("RSA");
                    g.initialize(Integer.parseInt(keySpec.substring(4)));
                }
                case "ECC_NIST_P256" -> {
                    g = KeyPairGenerator.getInstance("EC");
                    g.initialize(new ECGenParameterSpec("secp256r1"));
                }
                case "ECC_NIST_P384" -> {
                    g = KeyPairGenerator.getInstance("EC");
                    g.initialize(new ECGenParameterSpec("secp384r1"));
                }
                case "ECC_NIST_P521" -> {
                    g = KeyPairGenerator.getInstance("EC");
                    g.initialize(new ECGenParameterSpec("secp521r1"));
                }
                case "ECC_SECG_P256K1" -> {
                    bc();
                    g = KeyPairGenerator.getInstance("EC", "BC");
                    g.initialize(new ECGenParameterSpec("secp256k1"));
                }
                case "SM2" -> {
                    return KmsSm2.generate();
                }
                case "ECC_NIST_EDWARDS25519" -> g = KeyPairGenerator.getInstance("Ed25519");
                case "ML_DSA_44" -> g = KeyPairGenerator.getInstance("ML-DSA-44");
                case "ML_DSA_65" -> g = KeyPairGenerator.getInstance("ML-DSA-65");
                case "ML_DSA_87" -> g = KeyPairGenerator.getInstance("ML-DSA-87");
                default -> throw new IllegalArgumentException("unsupported key spec " + keySpec);
            }
            KeyPair kp = g.generateKeyPair();
            return new Generated(kp.getPrivate().getEncoded(), kp.getPublic().getEncoded());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("key spec " + keySpec + " needs a newer Java runtime (" + e.getMessage() + ")");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String family(String keySpec) {
        if (keySpec.startsWith("RSA_")) {
            return "RSA";
        }
        if (keySpec.equals("ECC_NIST_EDWARDS25519")) {
            return "Ed25519";
        }
        if (keySpec.startsWith("ML_DSA")) {
            return "ML-DSA";
        }
        return "EC";
    }

    static PrivateKey privateKey(String keySpec, byte[] der) throws Exception {
        String f = family(keySpec);
        KeyFactory kf = keySpec.equals("ECC_SECG_P256K1") || keySpec.equals("SM2") ? bcKf() : KeyFactory.getInstance(f.equals("ML-DSA") ? mlName(keySpec) : f);
        return kf.generatePrivate(new PKCS8EncodedKeySpec(der));
    }

    static PublicKey publicKey(String keySpec, byte[] der) throws Exception {
        String f = family(keySpec);
        KeyFactory kf = keySpec.equals("ECC_SECG_P256K1") || keySpec.equals("SM2") ? bcKf() : KeyFactory.getInstance(f.equals("ML-DSA") ? mlName(keySpec) : f);
        return kf.generatePublic(new X509EncodedKeySpec(der));
    }

    private static KeyFactory bcKf() throws Exception {
        bc();
        return KeyFactory.getInstance("EC", "BC");
    }

    private static String mlName(String keySpec) {
        return "ML-DSA-" + keySpec.substring(7);
    }

    // --------------------------------------------------------------------------------------------- sign / verify

    public static final Map<String, String[]> SIGNING = Map.of(
            "RSA", new String[] {"RSASSA_PSS_SHA_256", "RSASSA_PSS_SHA_384", "RSASSA_PSS_SHA_512", "RSASSA_PKCS1_V1_5_SHA_256",
                "RSASSA_PKCS1_V1_5_SHA_384", "RSASSA_PKCS1_V1_5_SHA_512"},
            "ECC_NIST_P256", new String[] {"ECDSA_SHA_256"},
            "ECC_NIST_P384", new String[] {"ECDSA_SHA_384"},
            "ECC_NIST_P521", new String[] {"ECDSA_SHA_512"},
            "ECC_SECG_P256K1", new String[] {"ECDSA_SHA_256"},
            "ECC_NIST_EDWARDS25519", new String[] {"ED25519_SHA_512", "ED25519_PH_SHA_512"},
            "SM2", new String[] {"SM2DSA"},
            "ML_DSA", new String[] {"ML_DSA_SHAKE_256"});

    public static String[] signingAlgorithms(String keySpec) {
        if (keySpec.startsWith("RSA_")) {
            return SIGNING.get("RSA");
        }
        if (keySpec.startsWith("ML_DSA")) {
            return SIGNING.get("ML_DSA");
        }
        return SIGNING.getOrDefault(keySpec, new String[0]);
    }

    private static String sha(String alg) {
        return alg.endsWith("_256") ? "SHA-256" : alg.endsWith("_384") ? "SHA-384" : "SHA-512";
    }

    private static int digestLen(String alg) {
        return alg.endsWith("_256") ? 32 : alg.endsWith("_384") ? 48 : 64;
    }

    /** PKCS#1 v1.5 DigestInfo prefixes for signing a pre-computed digest. */
    private static byte[] digestInfo(String hash, byte[] digest) {
        byte[] prefix = switch (hash) {
            case "SHA-256" -> new byte[] {0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, (byte) 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01,
                0x05, 0x00, 0x04, 0x20};
            case "SHA-384" -> new byte[] {0x30, 0x41, 0x30, 0x0d, 0x06, 0x09, 0x60, (byte) 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02,
                0x05, 0x00, 0x04, 0x30};
            default -> new byte[] {0x30, 0x51, 0x30, 0x0d, 0x06, 0x09, 0x60, (byte) 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x03,
                0x05, 0x00, 0x04, 0x40};
        };
        byte[] out = new byte[prefix.length + digest.length];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        System.arraycopy(digest, 0, out, prefix.length, digest.length);
        return out;
    }

    private static Signature signature(String keySpec, String alg, boolean digestMode) throws Exception {
        if (alg.startsWith("RSASSA_PKCS1")) {
            return Signature.getInstance(digestMode ? "NONEwithRSA" : sha(alg).replace("-", "") + "withRSA");
        }
        if (alg.startsWith("RSASSA_PSS")) {
            if (digestMode) {
                throw new IllegalArgumentException("MessageType DIGEST is not supported for " + alg);
            }
            Signature s = Signature.getInstance("RSASSA-PSS");
            String h = sha(alg);
            s.setParameter(new PSSParameterSpec(h, "MGF1", new MGF1ParameterSpec(h), digestLen(alg), 1));
            return s;
        }
        if (alg.startsWith("ECDSA")) {
            String n = digestMode ? "NONEwithECDSA" : sha(alg).replace("-", "") + "withECDSA";
            if (keySpec.equals("ECC_SECG_P256K1")) {
                bc();
                return Signature.getInstance(n, "BC");
            }
            return Signature.getInstance(n);
        }
        if (alg.equals("ED25519_SHA_512") || alg.equals("ED25519_PH_SHA_512")) {
            Signature s = Signature.getInstance("Ed25519");
            if (alg.equals("ED25519_PH_SHA_512")) {
                s.setParameter(new EdDSAParameterSpec(true));
            }
            return s;
        }
        if (alg.equals("ML_DSA_SHAKE_256")) {
            return Signature.getInstance(mlName(keySpec));
        }
        throw new IllegalArgumentException("unsupported signing algorithm " + alg);
    }

    private static byte[] signedInput(String alg, byte[] message, boolean digestMode) {
        if (digestMode && alg.startsWith("RSASSA_PKCS1")) {
            return digestInfo(sha(alg), message);
        }
        return message;
    }

    public static byte[] sign(String keySpec, byte[] privateDer, String alg, byte[] message, boolean digestMode) {
        try {
            if (alg.equals("SM2DSA")) {
                if (digestMode) {
                    throw new IllegalArgumentException("MessageType DIGEST is not supported for SM2DSA");
                }
                return KmsSm2.sign(privateDer, message);
            }
            Signature s = signature(keySpec, alg, digestMode);
            s.initSign(privateKey(keySpec, privateDer));
            s.update(signedInput(alg, message, digestMode));
            return s.sign();
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("signing algorithm " + alg + " needs a newer Java runtime (" + e.getMessage() + ")");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean verify(String keySpec, byte[] publicDer, String alg, byte[] message, boolean digestMode, byte[] sig) {
        try {
            if (alg.equals("SM2DSA")) {
                if (digestMode) {
                    throw new IllegalArgumentException("MessageType DIGEST is not supported for SM2DSA");
                }
                return KmsSm2.verify(publicDer, message, sig);
            }
            Signature s = signature(keySpec, alg, digestMode);
            s.initVerify(publicKey(keySpec, publicDer));
            s.update(signedInput(alg, message, digestMode));
            return s.verify(sig);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (java.security.SignatureException e) {
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static int expectedDigestLength(String alg) {
        if (alg.startsWith("ED25519")) {
            return -1;
        }
        return digestLen(alg);
    }

    // --------------------------------------------------------------------------------------------- RSA encryption

    public static byte[] rsaEncrypt(byte[] publicDer, String alg, byte[] plaintext) {
        try {
            Cipher c = Cipher.getInstance("RSA/ECB/OAEPPadding");
            c.init(Cipher.ENCRYPT_MODE, publicKey("RSA_2048", publicDer), oaep(alg));
            return c.doFinal(plaintext);
        } catch (Exception e) {
            throw new IllegalArgumentException("RSA encryption failed: " + e.getMessage());
        }
    }

    /** @return null when the ciphertext does not decrypt */
    public static byte[] rsaDecrypt(byte[] privateDer, String alg, byte[] ciphertext) {
        try {
            Cipher c = Cipher.getInstance("RSA/ECB/OAEPPadding");
            c.init(Cipher.DECRYPT_MODE, privateKey("RSA_2048", privateDer), oaep(alg));
            return c.doFinal(ciphertext);
        } catch (Exception e) {
            return null;
        }
    }

    private static OAEPParameterSpec oaep(String alg) {
        boolean s256 = alg.equals("RSAES_OAEP_SHA_256");
        return new OAEPParameterSpec(s256 ? "SHA-256" : "SHA-1", "MGF1", s256 ? MGF1ParameterSpec.SHA256 : MGF1ParameterSpec.SHA1,
                PSource.PSpecified.DEFAULT);
    }

    // --------------------------------------------------------------------------------------------- MAC

    public static byte[] mac(byte[] key, String macAlg, byte[] message) {
        try {
            String n = "Hmac" + macAlg.substring(macAlg.lastIndexOf('_') + 1).replace("SHA", "SHA");
            Mac m = Mac.getInstance(switch (macAlg) {
                case "HMAC_SHA_224" -> "HmacSHA224";
                case "HMAC_SHA_256" -> "HmacSHA256";
                case "HMAC_SHA_384" -> "HmacSHA384";
                case "HMAC_SHA_512" -> "HmacSHA512";
                default -> throw new IllegalArgumentException("unsupported MAC algorithm " + n);
            });
            m.init(new SecretKeySpec(key, "RAW"));
            return m.doFinal(message);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean constantTimeEquals(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a, b);
    }
}
