package com.sayonora.warp.awswire;

import com.sayonora.warp.dynamowire.auth.AwsIamCredentialStore;

/**
 * Identity and auth settings shared by every new AWS frontend and the unified endpoint. Environment:
 * {@code WARP_AWS_ACCOUNT_ID} (default: {@code WARP_SQSWIRE_ACCOUNT_ID}, else {@code 000000000000}),
 * {@code WARP_AWS_REGION} (default: {@code WARP_SQSWIRE_REGION}, else {@code us-east-1}),
 * {@code WARP_AWS_IAM_CREDENTIALS} (accessKey=secret;... -- when set, requests must carry a valid SigV4 signature made
 * with one of the pairs, or with STS temporary credentials issued by this Warp),
 * {@code WARP_KMS_MASTER_KEY} / {@code WARP_KMS_INSECURE_DEV_KEY} (see {@link KmsService}).
 */
public final class AwsConfig {

    public final String accountId;
    public final String region;
    public final AwsIamCredentialStore credentials;
    public final String kmsMasterKey;
    public final boolean kmsInsecureDevKey;

    public AwsConfig(String accountId, String region, AwsIamCredentialStore credentials, String kmsMasterKey,
            boolean kmsInsecureDevKey) {
        this.accountId = accountId;
        this.region = region;
        this.credentials = credentials;
        this.kmsMasterKey = kmsMasterKey;
        this.kmsInsecureDevKey = kmsInsecureDevKey;
    }

    public static AwsConfig fromEnv() {
        String account = first("WARP_AWS_ACCOUNT_ID", "WARP_SQSWIRE_ACCOUNT_ID");
        String region = first("WARP_AWS_REGION", "WARP_SQSWIRE_REGION");
        return new AwsConfig(account == null ? "000000000000" : account, region == null ? "us-east-1" : region,
                AwsIamCredentialStore.fromEnv(), blankToNull(System.getenv("WARP_KMS_MASTER_KEY")),
                "true".equalsIgnoreCase(System.getenv("WARP_KMS_INSECURE_DEV_KEY")));
    }

    private static String first(String... names) {
        for (String n : names) {
            String v = blankToNull(System.getenv(n));
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }

    public String arn(String service, String resource) {
        return "arn:aws:" + service + ":" + region + ":" + accountId + ":" + resource;
    }

    public String arnNoRegion(String service, String resource) {
        return "arn:aws:" + service + "::" + accountId + ":" + resource;
    }
}
