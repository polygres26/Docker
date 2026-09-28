package com.sayonora.warp.ab;

import java.time.Instant;

/**
 * How Warp authenticates ITSELF to a cloud service on behalf of an (already Warp-authenticated)
 * client. One provider per cloud target. AWS providers produce {@link AwsCreds}; the interface is
 * cloud-neutral so Azure (service principal secret/cert, managed identity) and Google
 * (service-account key, workload identity, impersonation) providers can produce {@link BearerToken}s
 * later -- those two are registered as stubs that fail with {@link Unimplemented} (see
 * {@link AbAwsAuth#create}).
 */
public interface AbAuthProvider {

    /** What a provider hands out; never logged, never serialised by the admin API. */
    sealed interface CloudCredential permits AwsCreds, BearerToken {
        Instant expiresAt();
    }

    /** AWS access key triple. {@code expiresAt} null = does not expire. {@link #toString} is redacted. */
    record AwsCreds(String accessKeyId, String secretAccessKey, String sessionToken, Instant expiresAt)
            implements CloudCredential {
        @Override
        public String toString() {
            return "AwsCreds[accessKeyId=" + accessKeyId + ", secret=***]";
        }
    }

    /** Bearer token credential (Azure AAD / Google OAuth access token). */
    record BearerToken(String token, Instant expiresAt) implements CloudCredential {
        @Override
        public String toString() {
            return "BearerToken[***]";
        }
    }

    /** {@code roleOverride}: a role/identity to narrow to (AWS: role ARN to assume); null = the target's own identity. */
    CloudCredential resolve(String roleOverride) throws AuthException;

    /** Short id: static, assume-role, web-identity, default-chain, azure-*, google-*. */
    String type();

    class AuthException extends Exception {
        public AuthException(String message) {
            super(message);
        }

        public AuthException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Thrown by the provider types that are interface stubs only. */
    class Unimplemented extends AuthException {
        public Unimplemented(String type) {
            super("auth provider '" + type + "' is UNIMPLEMENTED in this Warp build (only static, assume-role, "
                    + "web-identity and default-chain AWS providers exist); the AbAuthProvider interface is the extension point");
        }
    }
}
