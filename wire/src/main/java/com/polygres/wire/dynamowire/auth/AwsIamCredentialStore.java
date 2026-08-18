package com.polygres.wire.dynamowire.auth;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real AWS access-key-id → secret-access-key mapping for {@link SigV4Verifier} -- opt-in via
 * {@code POLYWIRE_AWS_IAM_CREDENTIALS} (bootstrap default) or {@code
 * polywire_config.awsIamCredentials} (hot-reloadable -- see {@link #reload}, called from {@code
 * Main}'s config-apply callback), {@code ;}-separated {@code accessKeyId=secretAccessKey} entries
 * (same shape convention as {@code POLYWIRE_ACL_RULES}/{@code POLYWIRE_BACKENDS}). No credentials
 * configured means {@link #DISABLED} -- dynamowire's SigV4 gate is skipped entirely, unchanged
 * behavior from before this feature existed (see {@code DynamoWireServer}'s own class javadoc on
 * that pre-existing, documented gap).
 *
 * <p>Deliberately a static, operator-provisioned list, not a call out to real AWS STS/IAM to
 * validate keys live -- PolyWire has no AWS account of its own to check against; this is "the set
 * of access keys this deployment has decided to trust," the same posture {@link
 * com.polygres.wire.auth.CredentialStore} already has for the wire-protocol frontends' shared
 * secret, just supporting more than one identity.
 *
 * <p><b>{@link #DISABLED} is a plain default value, never mutated</b> -- same reasoning as {@link
 * com.polygres.wire.acl.ClientAcl}'s identical class javadoc: {@link #reload} is only ever called
 * on an instance {@code Main} built explicitly via {@link #create} for that purpose, never on the
 * shared {@code DISABLED} constant other call sites use as a convenience default.
 */
public final class AwsIamCredentialStore {

    private static final Logger log = LoggerFactory.getLogger(AwsIamCredentialStore.class);

    public static final AwsIamCredentialStore DISABLED = new AwsIamCredentialStore(Map.of());

    // volatile, not final -- see ClientAcl's identical field javadoc for why: a reload (from
    // Main's polywire_config LISTEN callback) and a concurrent request's own read both need to see
    // a consistent, fully-built map, never a partially-applied one, without a lock on the hot path.
    private volatile Map<String, String> secretsByAccessKeyId;

    private AwsIamCredentialStore(Map<String, String> secretsByAccessKeyId) {
        this.secretsByAccessKeyId = secretsByAccessKeyId;
    }

    public static AwsIamCredentialStore fromEnv() {
        return parse(System.getenv("POLYWIRE_AWS_IAM_CREDENTIALS"));
    }

    public static AwsIamCredentialStore parse(String spec) {
        if (spec == null || spec.isBlank()) {
            return DISABLED;
        }
        return new AwsIamCredentialStore(parseCredentials(spec));
    }

    /** Builds a real, independently-reloadable instance -- unlike {@link #parse}/{@link #fromEnv}, never returns the shared {@link #DISABLED} constant, even when {@code spec} is blank. */
    public static AwsIamCredentialStore create(String spec) {
        return new AwsIamCredentialStore(parseCredentials(spec));
    }

    /** Swaps in a freshly-parsed credential set. */
    public void reload(String spec) {
        this.secretsByAccessKeyId = parseCredentials(spec);
        log.info("AwsIamCredentialStore: reloaded {} credential(s)", this.secretsByAccessKeyId.size());
    }

    private static Map<String, String> parseCredentials(String spec) {
        if (spec == null || spec.isBlank()) {
            return Map.of();
        }
        Map<String, String> secrets = new HashMap<>();
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "malformed AWS IAM credentials entry (expected accessKeyId=secretAccessKey): " + trimmed);
            }
            secrets.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
        }
        return Map.copyOf(secrets);
    }

    public boolean isEnabled() {
        return !secretsByAccessKeyId.isEmpty();
    }

    /** Null if {@code accessKeyId} isn't a known/trusted identity. */
    public String secretFor(String accessKeyId) {
        return secretsByAccessKeyId.get(accessKeyId);
    }
}
