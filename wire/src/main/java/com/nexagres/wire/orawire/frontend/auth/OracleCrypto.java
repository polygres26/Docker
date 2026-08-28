package com.nexagres.wire.orawire.frontend.auth;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

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

    public static byte[] stripPkcs7(byte[] decrypted) {
        int padLen = decrypted[decrypted.length - 1] & 0xFF;
        if (padLen < 1 || padLen > 16 || padLen > decrypted.length) {
            throw new IllegalArgumentException("invalid PKCS7 padding");
        }
        return Arrays.copyOf(decrypted, decrypted.length - padLen);
    }

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
