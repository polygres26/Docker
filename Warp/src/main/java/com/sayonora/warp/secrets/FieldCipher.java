package com.sayonora.warp.secrets;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encrypts individual stored secrets (AES-256-GCM) before they are written to the control-plane database: the backend spec of
 * {@code warp_config} (which embeds each backend's and replica's password), the AWS IAM credentials, the LLM API key, the MCP upstream
 * tokens, the XA recovery log's backend password and the ACME state. Applied per value, not as a whole-blob wrapper, so the
 * {@code warp_config.payload} column stays a real jsonb document and only what needs protecting is protected.
 *
 * <p><b>Keys.</b> {@code SAYONORA_ENCRYPTION_KEY} is the active key (base64, 32 raw bytes: {@code openssl rand -base64 32}) and is the only
 * key ever used to encrypt. {@code SAYONORA_ENCRYPTION_KEY_PREVIOUS} is an optional comma-separated list of older keys that are used to
 * decrypt only, which is what makes rotation possible without losing anything: start with the new key as active and the old one as previous,
 * run the re-encrypt operation (the admin API), and drop the previous key once nothing is stored under it. Each key has an id, the first
 * four bytes of its SHA-256 as hex, which is not secret and is how a stored value names the key it needs.
 *
 * <p><b>Format.</b> {@code encv2:<keyId>:<base64(iv || ciphertext)>}. The earlier {@code encv1:<base64>} (no key id) is still read: it is
 * tried against the active key and then each previous one, so nothing needs migrating before the first rotation. A value without an
 * {@code encv} prefix is plaintext and passes through {@link #decrypt} unchanged.
 *
 * <p><b>No key configured.</b> By default {@link #encrypt} returns the value unchanged and logs one warning (backward compatible: a
 * deployment that never set a key keeps working). {@code WARP_REQUIRE_ENCRYPTION_KEY=true} makes that an error instead: Warp refuses to
 * start without a valid key ({@link #validateAtStartup}) and {@link #encrypt} never writes a secret in the clear.
 */
public final class FieldCipher {

    private static final Logger log = LoggerFactory.getLogger(FieldCipher.class);
    private static final String V1 = "encv1:";
    private static final String V2 = "encv2:";
    private static final String ALGO = "AES/GCM/NoPadding";
    private static final int GCM_TAG_BITS = 128;
    private static final int IV_BYTES = 12;

    private static volatile boolean warnedOnce = false;
    private static volatile Keys cached;
    private static volatile Keys testKeys;
    private static volatile Boolean requireOverride;

    private FieldCipher() {
    }

    /** The configured keys: the active one (null when none is set) and every key a value may have been encrypted with. */
    private record Keys(String envActive, String envPrevious, SecretKeySpec active, String activeId, Map<String, SecretKeySpec> byId) {
        List<SecretKeySpec> decryptOrder() {
            List<SecretKeySpec> order = new ArrayList<>();
            if (active != null) {
                order.add(active);
            }
            byId.forEach((id, k) -> {
                if (k != active) {
                    order.add(k);
                }
            });
            return order;
        }
    }

    private static Keys keys() {
        Keys t = testKeys;
        if (t != null) {
            return t;
        }
        String active = System.getenv("SAYONORA_ENCRYPTION_KEY");
        String previous = System.getenv("SAYONORA_ENCRYPTION_KEY_PREVIOUS");
        Keys c = cached;
        if (c != null && java.util.Objects.equals(c.envActive(), active) && java.util.Objects.equals(c.envPrevious(), previous)) {
            return c;
        }
        c = parse(active, previous);
        cached = c;
        return c;
    }

    private static Keys parse(String activeEnv, String previousEnv) {
        Map<String, SecretKeySpec> byId = new LinkedHashMap<>();
        SecretKeySpec active = null;
        String activeId = null;
        if (activeEnv != null && !activeEnv.isBlank()) {
            active = key(activeEnv.strip(), "SAYONORA_ENCRYPTION_KEY");
            activeId = id(active);
            byId.put(activeId, active);
        }
        if (previousEnv != null && !previousEnv.isBlank()) {
            for (String p : previousEnv.split(",")) {
                if (!p.isBlank()) {
                    SecretKeySpec k = key(p.strip(), "SAYONORA_ENCRYPTION_KEY_PREVIOUS");
                    byId.putIfAbsent(id(k), k);
                }
            }
        }
        return new Keys(activeEnv, previousEnv, active, activeId, byId);
    }

    private static SecretKeySpec key(String base64, String variable) {
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(variable + " is not valid base64 (expected `openssl rand -base64 32`)");
        }
        if (raw.length != 32) {
            throw new IllegalStateException(variable + " must decode to exactly 32 bytes (AES-256) -- got " + raw.length);
        }
        return new SecretKeySpec(raw, "AES");
    }

    private static String id(SecretKeySpec key) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(key.getEncoded());
            return String.format("%02x%02x%02x%02x", d[0], d[1], d[2], d[3]);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** True when {@code WARP_REQUIRE_ENCRYPTION_KEY=true}: no key means an error, not plaintext. */
    public static boolean requireKey() {
        Boolean o = requireOverride;
        return o != null ? o : "true".equalsIgnoreCase(System.getenv("WARP_REQUIRE_ENCRYPTION_KEY"));
    }

    /** True when an active key is configured, i.e. {@link #encrypt} really encrypts. */
    public static boolean enabled() {
        return keys().active() != null;
    }

    /** The id of the active key, or null when none is set. */
    public static String activeKeyId() {
        return keys().activeId();
    }

    /** The ids of the keys that are only used to decrypt. */
    public static List<String> previousKeyIds() {
        Keys k = keys();
        List<String> ids = new ArrayList<>(k.byId().keySet());
        ids.remove(k.activeId());
        return ids;
    }

    /** Fails fast on a malformed key, and (with {@code WARP_REQUIRE_ENCRYPTION_KEY=true}) on a missing one. */
    public static void validateAtStartup() {
        Keys k = keys(); // throws on a malformed key
        if (k.active() == null && requireKey()) {
            throw new IllegalStateException("WARP_REQUIRE_ENCRYPTION_KEY=true but SAYONORA_ENCRYPTION_KEY is not set: refusing to start, "
                    + "because backend passwords and credentials would be stored in plaintext. Set it (base64, 32 raw bytes: "
                    + "`openssl rand -base64 32`).");
        }
        if (k.active() != null) {
            log.info("secrets at rest: AES-256-GCM, active key {}, {} previous key(s) for decrypting", k.activeId(), k.byId().size() - 1);
        }
    }

    /** Encrypts with the active key. With no key it returns the value unchanged (and warns once), or throws under
     * {@code WARP_REQUIRE_ENCRYPTION_KEY=true}. Blank values are returned as they are. */
    public static String encrypt(String plaintext) {
        if (plaintext == null || plaintext.isBlank()) {
            return plaintext;
        }
        Keys k = keys();
        if (k.active() == null) {
            if (requireKey()) {
                throw new IllegalStateException("SAYONORA_ENCRYPTION_KEY is not set and WARP_REQUIRE_ENCRYPTION_KEY=true: "
                        + "refusing to store a secret in plaintext");
            }
            if (!warnedOnce) {
                warnedOnce = true;
                log.warn("SAYONORA_ENCRYPTION_KEY is not set -- backend passwords and AWS IAM credentials in warp_config are being "
                        + "stored in PLAINTEXT. Set it (base64-encoded, 32 raw bytes -- e.g. `openssl rand -base64 32`) to encrypt them "
                        + "at rest, and WARP_REQUIRE_ENCRYPTION_KEY=true to refuse to run without it.");
            }
            return plaintext;
        }
        try {
            byte[] iv = new byte[IV_BYTES];
            new SecureRandom().nextBytes(iv);
            Cipher cipher = Cipher.getInstance(ALGO);
            cipher.init(Cipher.ENCRYPT_MODE, k.active(), new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] combined = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(ciphertext, 0, combined, iv.length, ciphertext.length);
            return V2 + k.activeId() + ":" + Base64.getEncoder().encodeToString(combined);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("field encryption failed", e);
        }
    }

    /** Plaintext (no {@code encv} prefix) passes through unchanged; an {@code encv2:} value is decrypted with the key it names, an
     * {@code encv1:} value with the first configured key that opens it. */
    public static String decrypt(String stored) {
        if (stored == null || !(stored.startsWith(V2) || stored.startsWith(V1))) {
            return stored;
        }
        Keys k = keys();
        if (k.byId().isEmpty()) {
            throw new IllegalStateException("field is encrypted (" + stored.substring(0, 6) + ") but SAYONORA_ENCRYPTION_KEY is not set "
                    + "on this process -- cannot decrypt it");
        }
        if (stored.startsWith(V2)) {
            int colon = stored.indexOf(':', V2.length());
            if (colon < 0) {
                throw new IllegalStateException("malformed encrypted field");
            }
            String keyId = stored.substring(V2.length(), colon);
            SecretKeySpec key = k.byId().get(keyId);
            if (key == null) {
                throw new IllegalStateException("field is encrypted with key " + keyId + ", which is not configured: set it as "
                        + "SAYONORA_ENCRYPTION_KEY or list it in SAYONORA_ENCRYPTION_KEY_PREVIOUS (configured: " + k.byId().keySet() + ")");
            }
            return open(key, stored.substring(colon + 1))
                    .orElseThrow(() -> new IllegalStateException("field decryption failed for key " + keyId + " -- corrupted value?"));
        }
        String body = stored.substring(V1.length());
        for (SecretKeySpec candidate : k.decryptOrder()) {
            var opened = open(candidate, body);
            if (opened.isPresent()) {
                return opened.get();
            }
        }
        throw new IllegalStateException("field decryption failed -- none of the configured keys " + k.byId().keySet() + " opens it "
                + "(wrong SAYONORA_ENCRYPTION_KEY?)");
    }

    private static java.util.Optional<String> open(SecretKeySpec key, String base64) {
        try {
            byte[] combined = Base64.getDecoder().decode(base64);
            byte[] iv = new byte[IV_BYTES];
            System.arraycopy(combined, 0, iv, 0, IV_BYTES);
            byte[] ciphertext = new byte[combined.length - IV_BYTES];
            System.arraycopy(combined, IV_BYTES, ciphertext, 0, ciphertext.length);
            Cipher cipher = Cipher.getInstance(ALGO);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return java.util.Optional.of(new String(cipher.doFinal(ciphertext), java.nio.charset.StandardCharsets.UTF_8));
        } catch (GeneralSecurityException | IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
            return java.util.Optional.empty();
        }
    }

    /** True when {@code stored} needs no re-encryption: blank, or an {@code encv2:} value under the active key. */
    public static boolean isCurrent(String stored) {
        if (stored == null || stored.isBlank()) {
            return true;
        }
        Keys k = keys();
        return k.active() != null && stored.startsWith(V2 + k.activeId() + ":");
    }

    /** {@code stored} encrypted under the active key (decrypting it first when needed); unchanged when it already is, or when no key
     * is configured. */
    public static String reencrypt(String stored) {
        if (!enabled() || isCurrent(stored)) {
            return stored;
        }
        return encrypt(decrypt(stored));
    }

    /** What protects {@code stored}: {@code plaintext}, {@code encv1}, or the id of the {@code encv2} key. For status displays. */
    public static String protection(String stored) {
        if (stored == null || stored.isBlank()) {
            return "empty";
        }
        if (stored.startsWith(V2)) {
            int colon = stored.indexOf(':', V2.length());
            return colon < 0 ? "malformed" : stored.substring(V2.length(), colon);
        }
        return stored.startsWith(V1) ? "encv1" : "plaintext";
    }

    /** Tests only: use these keys instead of the environment ({@code null} active and previous restores the environment). */
    static void useKeysForTesting(String active, String previous) {
        testKeys = active == null && previous == null ? null : parse(active, previous);
        warnedOnce = false;
    }

    /** Tests only: pretend {@code WARP_REQUIRE_ENCRYPTION_KEY} is set to this ({@code null} restores the environment). */
    static void requireKeyForTesting(Boolean require) {
        requireOverride = require;
    }
}
