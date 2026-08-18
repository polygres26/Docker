package com.polygres.wire.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.SecretKeyFactory;

/**
 * Verifies a plaintext password against a real Postgres {@code pg_authid.rolpassword} stored
 * verifier -- the same two formats Postgres itself stores server-side, reimplemented here purely
 * for local verification (no bind, no network call). Used by {@link PgRoleAuthCache} so PolyWire
 * can validate a client-presented password against a *real* Postgres role's actual password,
 * entirely offline, without opening a connection to the backend for each login attempt.
 *
 * <p>Deliberately covers only the two stored-verifier shapes {@code pg_authid.rolpassword} can
 * actually contain on a modern Postgres (13+ defaults to {@code scram-sha-256}; {@code md5} is
 * still readable for roles created under an older {@code password_encryption} setting or never
 * rotated since): {@code md5<32 hex chars>} and {@code SCRAM-SHA-256$<iterations>:<salt>$
 * <StoredKey>:<ServerKey>}. Both are Postgres's own documented on-disk formats -- see
 * {@code src/common/scram-common.c}/{@code md5_crypt_verify} in the Postgres source for the
 * canonical algorithm this mirrors.
 */
public final class PostgresPasswordVerifier {

    private PostgresPasswordVerifier() {
    }

    /** @return true if {@code presentedPassword} is the real password behind {@code storedVerifier}. */
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
            // Malformed verifier or crypto failure -- treat as "does not match", never throw out
            // of an auth check (same "fail closed, not loud" posture as a wrong password).
            return false;
        }
        // Any other/unknown verifier shape (e.g. plain, the pre-9.1-deprecated format) -- this
        // project doesn't attempt to reproduce it; caller should fall back to rejecting the login
        // rather than guessing at a format that was already unusual enough to reach this branch.
        return false;
    }

    // ---- md5 ----

    /** Postgres's md5 auth format: {@code "md5" + hex(md5(password || username))}. */
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

    // ---- SCRAM-SHA-256 ----

    /**
     * Format: {@code SCRAM-SHA-256$<iterations>:<salt base64>$<StoredKey base64>:<ServerKey base64>}.
     * Verification only needs {@code StoredKey}: derive {@code SaltedPassword} via PBKDF2-HMAC-
     * SHA256 from the presented password/salt/iterations (RFC 5802 §2.2), then
     * {@code ClientKey = HMAC(SaltedPassword, "Client Key")}, {@code StoredKey = SHA256(ClientKey)}
     * -- if that matches the stored {@code StoredKey}, the presented password is correct. This is
     * exactly the computation a real SCRAM exchange's server side performs against the client's
     * proof, just done here directly against a plaintext password instead of over a live
     * challenge-response, since every wire protocol this project speaks already collects the
     * client's password as cleartext (see class javadoc on why that's true for pgwire/mssqlwire but
     * not orawire/mywire).
     */
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
