package com.polygres.wire.license;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PolyWire's commercial-tier gate. Every enforcement point ({@code ConnectionGate}'s
 * per-instance connection cap, {@code NodeRegistry}'s cluster-wide instance-count check at
 * startup, {@code BackendRegistry}'s registered-backend cap) reads its limit from {@link
 * #current()} rather than hardcoding a number, so raising a customer to Enterprise is exactly
 * "give them a valid key," never a rebuild or a different image.
 *
 * <p><b>Deliberately offline, no phone-home.</b> {@code POLYWIRE_LICENSE_KEY} is a self-contained,
 * Ed25519-signed token: {@code base64url(payload-json) + "." + base64url(signature)}. The public
 * key that verifies it is hardcoded below (safe to ship -- it can only verify, never sign); the
 * matching private key is held offline by whoever mints license keys (see {@link
 * LicenseKeyGenTool}) and never touches this codebase or any running PolyWire process. No
 * license server, no network call, no telemetry -- verification is a signature check against
 * static bytes, same trust model as a software update's GPG signature. This was a deliberate
 * design choice (see the pricing plan this implements): connections/instances/backends were
 * picked specifically because they're enforceable this way, unlike a queries-per-month metering
 * scheme that would need to phone home to be trustworthy.
 *
 * <p><b>Fails closed.</b> No key, an unparseable key, a bad signature, or an expired key all
 * resolve to {@link LicenseTier#DEVELOPER} -- never to {@code ENTERPRISE} on any kind of error.
 * The only way to get Enterprise's uncapped limits is a key that verifies successfully right now.
 */
public final class License {

    private static final Logger log = LoggerFactory.getLogger(License.class);

    /** X.509 SubjectPublicKeyInfo, DER, base64 -- generated once, published here permanently.
     * Verifying a signature with this key proves nothing was forged; it reveals nothing that
     * would let anyone forge a new one. */
    private static final String PUBLIC_KEY_B64 =
            "MCowBQYDK2VwAyEAciDIQE/bUrz8CfN70phvlGdwmh9FDrQqdlDSbW4oiWM=";

    public static final int DEVELOPER_MAX_CONNECTIONS = 25;
    public static final int DEVELOPER_MAX_INSTANCES = 3;
    public static final int DEVELOPER_MAX_BACKENDS = 3;

    private static final License DEVELOPER = new License(LicenseTier.DEVELOPER, null, null);

    private static volatile License cached;

    private final LicenseTier tier;
    private final String licensedTo;
    private final Instant expiresAt;

    private License(LicenseTier tier, String licensedTo, Instant expiresAt) {
        this.tier = tier;
        this.licensedTo = licensedTo;
        this.expiresAt = expiresAt;
    }

    public LicenseTier tier() {
        return tier;
    }

    public String licensedTo() {
        return licensedTo;
    }

    public Instant expiresAt() {
        return expiresAt;
    }

    public int maxConnectionsPerInstance() {
        return tier == LicenseTier.ENTERPRISE ? Integer.MAX_VALUE : DEVELOPER_MAX_CONNECTIONS;
    }

    public int maxInstances() {
        return tier == LicenseTier.ENTERPRISE ? Integer.MAX_VALUE : DEVELOPER_MAX_INSTANCES;
    }

    public int maxBackends() {
        return tier == LicenseTier.ENTERPRISE ? Integer.MAX_VALUE : DEVELOPER_MAX_BACKENDS;
    }

    /** Parsed and verified once per process (license doesn't change without a restart -- same
     * "read once at startup" convention {@code ServerOptions.parse} already uses), then cached. */
    public static License current() {
        License c = cached;
        if (c == null) {
            synchronized (License.class) {
                c = cached;
                if (c == null) {
                    c = fromEnv();
                    cached = c;
                }
            }
        }
        return c;
    }

    static License fromEnv() {
        return fromKey(System.getenv("POLYWIRE_LICENSE_KEY"));
    }

    /** The actual verify-and-apply-expiry logic {@link #fromEnv()} runs, taking the key as a
     * parameter instead of reading it from the environment -- package-private specifically so
     * {@code LicenseIntegrationTest} can exercise real generated keys (genuine signature,
     * genuine tamper/wrong-signer/expiry cases) without needing to fork a subprocess just to
     * control an environment variable. */
    static License fromKey(String key) {
        if (key == null || key.isBlank()) {
            log.info("license: no POLYWIRE_LICENSE_KEY set -- running as Developer edition "
                    + "(free forever, all features, capped at {} connections/instance, {} "
                    + "instances, {} backends). See the Pricing section of the docs for Enterprise.",
                    DEVELOPER_MAX_CONNECTIONS, DEVELOPER_MAX_INSTANCES, DEVELOPER_MAX_BACKENDS);
            return DEVELOPER;
        }
        try {
            License parsed = verify(key);
            if (parsed.expiresAt != null && parsed.expiresAt.isBefore(Instant.now())) {
                log.warn("license: POLYWIRE_LICENSE_KEY for '{}' expired at {} -- falling back "
                        + "to Developer edition until it's renewed", parsed.licensedTo, parsed.expiresAt);
                return DEVELOPER;
            }
            log.info("license: verified Enterprise license for '{}'{} -- no Developer-tier limits apply",
                    parsed.licensedTo, parsed.expiresAt == null ? " (perpetual)" : " (expires " + parsed.expiresAt + ")");
            return parsed;
        } catch (RuntimeException e) {
            log.error("license: POLYWIRE_LICENSE_KEY is set but failed verification ({}) -- "
                    + "falling back to Developer edition. Check the key was copied in full and "
                    + "hasn't been altered.", e.getMessage());
            return DEVELOPER;
        }
    }

    static License verify(String key) {
        int dot = key.indexOf('.');
        if (dot <= 0 || dot == key.length() - 1) {
            throw new IllegalArgumentException("malformed license key (expected payload.signature)");
        }
        byte[] payloadBytes = Base64.getUrlDecoder().decode(key.substring(0, dot));
        byte[] signatureBytes = Base64.getUrlDecoder().decode(key.substring(dot + 1));

        if (!verifySignature(payloadBytes, signatureBytes)) {
            throw new IllegalArgumentException("signature does not match -- key was altered or not "
                    + "signed by a genuine PolyWire Enterprise private key");
        }

        JsonObject payload = JsonParser.parseString(new String(payloadBytes, StandardCharsets.UTF_8)).getAsJsonObject();
        LicenseTier tier = LicenseTier.valueOf(payload.get("tier").getAsString());
        String licensedTo = payload.has("licensedTo") ? payload.get("licensedTo").getAsString() : null;
        Instant expiresAt = payload.has("expiresAt") && !payload.get("expiresAt").isJsonNull()
                ? Instant.parse(payload.get("expiresAt").getAsString())
                : null;
        return new License(tier, licensedTo, expiresAt);
    }

    private static boolean verifySignature(byte[] payloadBytes, byte[] signatureBytes) {
        try {
            PublicKey publicKey = KeyFactory.getInstance("Ed25519")
                    .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(PUBLIC_KEY_B64)));
            Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(publicKey);
            verifier.update(payloadBytes);
            return verifier.verify(signatureBytes);
        } catch (Exception e) {
            // Any crypto-layer failure (malformed signature bytes, wrong-length key, etc.) is a
            // verification failure, not an exceptional condition the caller should see a stack
            // trace for -- same "fails closed" contract as an actual bad signature.
            return false;
        }
    }
}
