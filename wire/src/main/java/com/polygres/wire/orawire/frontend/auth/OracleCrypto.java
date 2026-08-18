package com.polygres.wire.orawire.frontend.auth;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-CBC and PBKDF2-HMAC-SHA512 primitives for the O5LOGON 12c/PBKDF2
 * scheme, transcribed from reference/o5logon_auth_spec.md §2.2 (itself from
 * python-oracledb's impl/thin/crypto.pyx). Fixed all-zero IV, PKCS7 padding
 * on encrypt (Java's "AES/CBC/PKCS5Padding" is PKCS7-compatible for a
 * 16-byte block size), no auto-unpad on decrypt (callers slice manually, to
 * match the reference's behavior of never trusting a padding-derived
 * length).
 */
public final class OracleCrypto {

    private static final byte[] ZERO_IV_16 = new byte[16];

    public static byte[] decryptCbcNoUnpad(byte[] key, byte[] ciphertext) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(ZERO_IV_16));
            return cipher.doFinal(ciphertext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] encryptCbcPkcs7(byte[] key, byte[] plaintext) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(ZERO_IV_16));
            return cipher.doFinal(plaintext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Strips PKCS7 padding after a NoPadding decrypt (caller trusts the pad byte). */
    public static byte[] stripPkcs7(byte[] decrypted) {
        int padLen = decrypted[decrypted.length - 1] & 0xFF;
        if (padLen < 1 || padLen > 16 || padLen > decrypted.length) {
            throw new IllegalArgumentException("invalid PKCS7 padding");
        }
        return Arrays.copyOf(decrypted, decrypted.length - padLen);
    }

    /**
     * PBKDF2-HMAC-SHA512 implemented directly against raw password bytes
     * (RFC 8018), rather than routing through Java's char[]-based
     * PBEKeySpec/SecretKeyFactory — that path forces a char<->byte
     * conversion inside the JCE provider whose exact encoding isn't
     * documented, which would silently diverge from python-oracledb's
     * get_derived_key(password_bytes, ...) for any non-ASCII password byte.
     * This implementation takes the password bytes verbatim as the HMAC
     * key, matching the reference exactly.
     */
    public static byte[] pbkdf2HmacSha512(byte[] password, byte[] salt, int lengthBytes, int iterations) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(password, "HmacSHA512"));
            int hashLen = mac.getMacLength();
            int numBlocks = (lengthBytes + hashLen - 1) / hashLen;
            byte[] result = new byte[numBlocks * hashLen];
            for (int blockIndex = 1; blockIndex <= numBlocks; blockIndex++) {
                byte[] blockInput = Arrays.copyOf(salt, salt.length + 4);
                blockInput[salt.length] = (byte) (blockIndex >>> 24);
                blockInput[salt.length + 1] = (byte) (blockIndex >>> 16);
                blockInput[salt.length + 2] = (byte) (blockIndex >>> 8);
                blockInput[salt.length + 3] = (byte) blockIndex;

                byte[] u = mac.doFinal(blockInput);
                byte[] block = u.clone();
                for (int iter = 1; iter < iterations; iter++) {
                    u = mac.doFinal(u);
                    for (int i = 0; i < hashLen; i++) {
                        block[i] ^= u[i];
                    }
                }
                System.arraycopy(block, 0, result, (blockIndex - 1) * hashLen, hashLen);
            }
            return Arrays.copyOf(result, lengthBytes);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private OracleCrypto() {
    }
}
