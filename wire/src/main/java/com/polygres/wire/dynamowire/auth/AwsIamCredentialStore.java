package com.polygres.wire.dynamowire.auth;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsIamCredentialStore {

    private static final Logger log = LoggerFactory.getLogger(AwsIamCredentialStore.class);

    public static final AwsIamCredentialStore DISABLED = new AwsIamCredentialStore(Map.of());

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

    public static AwsIamCredentialStore create(String spec) {
        return new AwsIamCredentialStore(parseCredentials(spec));
    }

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

    public String secretFor(String accessKeyId) {
        return secretsByAccessKeyId.get(accessKeyId);
    }
}
