package com.nexagres.wire.secrets;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encrypts individual config-field values before they hit disk (AES-256-GCM, key from
 * {@code NEXAGRES_ENCRYPTION_KEY} -- base64, 32 raw bytes). Applied narrowly, not as a whole-blob
 * wrapper: {@code warp_config.payload} is one jsonb column holding QoS/router/ACL/OAuth
 * config alongside the two fields that actually carry credentials ({@code backends}, whose
 * {@code name=url|user|password} spec embeds a literal password inline, and {@code
 * awsIamCredentials}) -- encrypting the whole payload would drag every non-secret setting through
 * the same at-rest protection for no benefit and would also break the column's {@code jsonb}
 * type (ciphertext isn't valid JSON). Encrypting just those two field values keeps the column a
 * real jsonb document; only the values that need protecting get it.
 *
 * <p><b>Backward-compatible by design:</b> {@link #decrypt} passes through any value that doesn't
 * carry the {@code encv1:} prefix unchanged -- rows written before {@code NEXAGRES_ENCRYPTION_KEY}
 * existed (including whatever's already in {@code warp_config} from earlier in this session)
 * keep working as plaintext until the next write re-encrypts them. There's no migration step;
 * the format just upgrades itself one write at a time.
 */
public final class FieldCipher {

    private static final Logger log = LoggerFactory.getLogger(FieldCipher.class);
    private static final String PREFIX = "encv1:";
    private static final String ALGO = "AES/GCM/NoPadding";
    private static final int GCM_TAG_BITS = 128;
    private static final int IV_BYTES = 12;

    private static volatile boolean warnedOnce = false;

    private FieldCipher() {
    }

    private static SecretKeySpec keyFromEnv() {
        String encoded = System.getenv("NEXAGRES_ENCRYPTION_KEY");
        if (encoded == null || encoded.isBlank()) {
            if (!warnedOnce) {
                warnedOnce = true;
                log.warn("NEXAGRES_ENCRYPTION_KEY is not set -- backend passwords and AWS IAM "
                        + "credentials in warp_config are being stored in PLAINTEXT. Set it "
                        + "(base64-encoded, 32 raw bytes -- e.g. `openssl rand -base64 32`) to "
                        + "encrypt them at rest.");
            }
            return null;
        }
        byte[] raw = Base64.getDecoder().decode(encoded);
        if (raw.length != 32) {
            throw new IllegalStateException("NEXAGRES_ENCRYPTION_KEY must decode to exactly 32 bytes "
                    + "(AES-256) -- got " + raw.length);
        }
        return new SecretKeySpec(raw, "AES");
    }

    /** Returns the plaintext unchanged (not an error) if no key is configured -- see class javadoc. */
    public static String encrypt(String plaintext) {
        if (plaintext == null || plaintext.isBlank()) {
            return plaintext;
        }
        SecretKeySpec key = keyFromEnv();
        if (key == null) {
            return plaintext;
        }
        try {
            byte[] iv = new byte[IV_BYTES];
            new SecureRandom().nextBytes(iv);
            Cipher cipher = Cipher.getInstance(ALGO);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] combined = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(ciphertext, 0, combined, iv.length, ciphertext.length);
            return PREFIX + Base64.getEncoder().encodeToString(combined);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("field encryption failed", e);
        }
    }

    /** Plaintext (no {@code encv1:} prefix) passes through unchanged -- see class javadoc. */
    public static String decrypt(String stored) {
        if (stored == null || !stored.startsWith(PREFIX)) {
            return stored;
        }
        SecretKeySpec key = keyFromEnv();
        if (key == null) {
            throw new IllegalStateException("field is encrypted (encv1:) but NEXAGRES_ENCRYPTION_KEY "
                    + "is not set on this process -- cannot decrypt it");
        }
        try {
            byte[] combined = Base64.getDecoder().decode(stored.substring(PREFIX.length()));
            byte[] iv = new byte[IV_BYTES];
            System.arraycopy(combined, 0, iv, 0, IV_BYTES);
            byte[] ciphertext = new byte[combined.length - IV_BYTES];
            System.arraycopy(combined, IV_BYTES, ciphertext, 0, ciphertext.length);
            Cipher cipher = Cipher.getInstance(ALGO);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(ciphertext), java.nio.charset.StandardCharsets.UTF_8);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("field decryption failed -- wrong NEXAGRES_ENCRYPTION_KEY?", e);
        }
    }
}
