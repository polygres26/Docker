package com.sayonora.wire.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.SecretKeyFactory;

public final class PostgresPasswordVerifier {

    private PostgresPasswordVerifier() {
    }

    public static boolean verify(String storedVerifier, String username, String presentedPassword) {
        if (storedVerifier == null || presentedPassword == null) {
            return false;
        }
        try {
            if (storedVerifier.startsWith("md5")) {
                return verifyMd5(storedVerifier, username, presentedPassword);
            }
            if (storedVerifier.startsWith("SCRAM-SHA-256$")) {
                return verifyScramSha256(storedVerifier, presentedPassword);
            }
        } catch (Exception e) {
            
            return false;
        }
        
        return false;
    }

    private static boolean verifyMd5(String storedVerifier, String username, String presentedPassword)
            throws NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] digest = md5.digest((presentedPassword + username).getBytes(StandardCharsets.UTF_8));
        String computed = "md5" + toHex(digest);
        return constantTimeEquals(computed, storedVerifier);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static boolean verifyScramSha256(String storedVerifier, String presentedPassword) throws Exception {
        String[] parts = storedVerifier.substring("SCRAM-SHA-256$".length()).split("\\$");
        if (parts.length != 2) {
            return false;
        }
        String[] iterAndSalt = parts[0].split(":");
        String[] storedAndServerKey = parts[1].split(":");
        if (iterAndSalt.length != 2 || storedAndServerKey.length != 2) {
            return false;
        }
        int iterations = Integer.parseInt(iterAndSalt[0]);
        byte[] salt = Base64.getDecoder().decode(iterAndSalt[1]);
        byte[] expectedStoredKey = Base64.getDecoder().decode(storedAndServerKey[0]);

        byte[] saltedPassword = pbkdf2HmacSha256(presentedPassword, salt, iterations);
        byte[] clientKey = hmacSha256(saltedPassword, "Client Key".getBytes(StandardCharsets.UTF_8));
        byte[] computedStoredKey = MessageDigest.getInstance("SHA-256").digest(clientKey);

        return MessageDigest.isEqual(computedStoredKey, expectedStoredKey);
    }

    private static byte[] pbkdf2HmacSha256(String password, byte[] salt, int iterations) throws Exception {
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 256);
        return factory.generateSecret(spec).getEncoded();
    }

    private static byte[] hmacSha256(byte[] key, byte[] data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data);
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
