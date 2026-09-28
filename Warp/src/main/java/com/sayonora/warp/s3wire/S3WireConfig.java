package com.sayonora.warp.s3wire;

import com.sayonora.warp.dynamowire.auth.AwsIamCredentialStore;

/**
 * s3wire configuration, read from the environment. s3wire starts when the {@code s3} store is enabled
 * on a Postgres backend of its set (Postgres mode, needs no further config besides credentials), when
 * {@code WARP_S3WIRE_BACKEND_BUCKET} is set (proxy mode), or when {@code WARP_S3WIRE_ENABLED=true}
 * (start now, pick up the store when it is enabled later). Postgres mode wins when both are configured
 * -- see {@link S3WireServer}. {@code WARP_S3WIRE_SET} names the backend set it serves (default: the set
 * holding {@code default}); Postgres-mode tunables are in {@link S3StoreOptions}.
 *
 * <ul>
 *   <li>{@code WARP_S3WIRE_PORT} -- listen port (default 18020)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_BUCKET} -- the one real backend bucket that holds everything
 *       (required; each client-visible bucket is a key prefix inside it)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_ENDPOINT} -- S3-compatible endpoint, e.g. a MinIO URL
 *       (unset = real AWS S3)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_ACCESS_KEY} / {@code WARP_S3WIRE_BACKEND_SECRET_KEY} -- Warp's
 *       own credentials for the backend (unset = AWS default credential chain)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_REGION} -- default us-east-1</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_PATH_STYLE} -- default true</li>
 *   <li>{@code WARP_S3WIRE_CREDENTIALS} -- {@code accessKey=secret;accessKey2=secret2} pairs that
 *       clients must SigV4-sign with; falls back to {@code WARP_AWS_IAM_CREDENTIALS}. Required:
 *       s3wire refuses to start without at least one pair rather than silently skipping auth.</li>
 *   <li>{@code WARP_S3WIRE_VHOST_DOMAIN} -- comma separated domains for virtual-hosted-style addressing
 *       ({@code <bucket>.<domain>}); {@code *.localhost}, the public loopback test domains and real AWS endpoint names
 *       are always recognised (see {@link S3Addressing})</li>
 *   <li>{@code WARP_S3WIRE_POST_MAX_BYTES} -- largest browser POST-policy form upload, default 64 MiB</li>
 * </ul>
 */
public record S3WireConfig(String endpoint, String region, String accessKey, String secretKey,
        String backendBucket, boolean pathStyle, AwsIamCredentialStore clientCredentials) {

    /** @return true when a proxy-mode backend bucket is configured. */
    public boolean proxyConfigured() {
        return backendBucket != null;
    }

    /** @throws IllegalStateException when no client credentials are configured (s3wire never serves unauthenticated) */
    public void requireCredentials() {
        if (!clientCredentials.isEnabled()) {
            throw new IllegalStateException("s3wire requires WARP_S3WIRE_CREDENTIALS "
                    + "(accessKey=secret;...) -- refusing to serve unauthenticated S3");
        }
    }

    /** Never null; {@link #proxyConfigured()} says whether proxy mode is configured. */
    public static S3WireConfig fromEnv() {
        String bucket = env("WARP_S3WIRE_BACKEND_BUCKET");
        String creds = env("WARP_S3WIRE_CREDENTIALS");
        if (creds == null) {
            creds = env("WARP_AWS_IAM_CREDENTIALS");
        }
        AwsIamCredentialStore store = AwsIamCredentialStore.parse(creds);
        String region = env("WARP_S3WIRE_BACKEND_REGION");
        String pathStyle = env("WARP_S3WIRE_BACKEND_PATH_STYLE");
        return new S3WireConfig(env("WARP_S3WIRE_BACKEND_ENDPOINT"), region == null ? "us-east-1" : region,
                env("WARP_S3WIRE_BACKEND_ACCESS_KEY"), env("WARP_S3WIRE_BACKEND_SECRET_KEY"), bucket,
                pathStyle == null || Boolean.parseBoolean(pathStyle), store);
    }

    private static String env(String name) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? null : v.trim();
    }
}
