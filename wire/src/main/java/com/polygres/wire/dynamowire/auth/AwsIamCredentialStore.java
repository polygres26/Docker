package com.polygres.wire.dynamowire.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * Real AWS access-key-id → secret-access-key mapping for {@link SigV4Verifier} -- opt-in via
 * {@code POLYWIRE_AWS_IAM_CREDENTIALS} ({@code ;}-separated {@code accessKeyId=secretAccessKey}
 * entries, same shape convention as {@code POLYWIRE_ACL_RULES}/{@code POLYWIRE_BACKENDS}). Unset
 * means {@link #DISABLED} -- dynamowire's SigV4 gate is skipped entirely, unchanged behavior from
 * before this feature existed (see {@code DynamoWireServer}'s own class javadoc on that
 * pre-existing, documented gap).
 *
 * <p>Deliberately a static, operator-provisioned list, not a call out to real AWS STS/IAM to
 * validate keys live -- PolyWire has no AWS account of its own to check against; this is "the set
 * of access keys this deployment has decided to trust," the same posture {@link
 * com.polygres.wire.auth.CredentialStore} already has for the wire-protocol frontends' shared
 * secret, just supporting more than one identity.
 */
public final class AwsIamCredentialStore {

    public static final AwsIamCredentialStore DISABLED = new AwsIamCredentialStore(Map.of());

    private final Map<String, String> secretsByAccessKeyId;

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
        Map<String, String> secrets = new HashMap<>();
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "malformed POLYWIRE_AWS_IAM_CREDENTIALS entry (expected accessKeyId=secretAccessKey): " + trimmed);
            }
            secrets.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
        }
        return new AwsIamCredentialStore(secrets);
    }

    public boolean isEnabled() {
        return this != DISABLED && !secretsByAccessKeyId.isEmpty();
    }

    /** Null if {@code accessKeyId} isn't a known/trusted identity. */
    public String secretFor(String accessKeyId) {
        return secretsByAccessKeyId.get(accessKeyId);
    }
}
