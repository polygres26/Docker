package com.nexagres.dms.secrets;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;

/**
 * Resolves a {@link SecretRef} to its actual value at connect time. Vault/CyberArk match
 * Warp's own {@code com.nexagres.wire.secrets.SecretResolver} (same behavior: opt-in via env
 * vars, fails loud rather than silently trying an empty password); AWS Secrets Manager/Azure Key
 * Vault/GCP Secret Manager are new to this module, added so a connection's credential can live in
 * whichever hyperscaler secret store an operator already uses instead of only HashiCorp Vault or
 * CyberArk.
 */
public final class SecretResolver {

    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private SecretResolver() {
    }

    public static String resolve(SecretRef ref) {
        return switch (ref) {
            case SecretRef.Plaintext p -> p.value();
            case SecretRef.Vault v -> resolveVault(v);
            case SecretRef.CyberArk c -> resolveCyberArk(c);
            case SecretRef.AwsSecretsManager a -> resolveAwsSecretsManager(a);
            case SecretRef.AzureKeyVault a -> resolveAzureKeyVault(a);
            case SecretRef.GcpSecretManager g -> resolveGcpSecretManager(g);
        };
    }

    public static String resolve(String raw) {
        return resolve(SecretRef.parse(raw));
    }

    private static String resolveVault(SecretRef.Vault ref) {
        String addr = System.getenv("VAULT_ADDR");
        String token = System.getenv("VAULT_TOKEN");
        if (addr == null || addr.isBlank() || token == null || token.isBlank()) {
            throw new IllegalStateException("secret ref \"vault:" + ref.path() + "\" needs VAULT_ADDR and "
                    + "VAULT_TOKEN set on the Advisor process -- neither is optional once any connection uses a "
                    + "vault: password reference");
        }
        String url = addr.replaceAll("/$", "") + "/v1/" + ref.path();
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .header("X-Vault-Token", token)
                    .GET()
                    .build();
            HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Vault returned HTTP " + response.statusCode() + " for " + url
                        + ": " + response.body());
            }
            JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
            JsonObject data = body.getAsJsonObject("data");
            JsonObject fields = data.has("data") && data.get("data").isJsonObject() ? data.getAsJsonObject("data") : data;
            if (!fields.has(ref.field())) {
                throw new IllegalStateException("Vault secret at " + ref.path() + " has no field \"" + ref.field() + "\"");
            }
            return fields.get(ref.field()).getAsString();
        } catch (java.io.IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("failed to reach Vault at " + addr + " for secret " + ref.path(), e);
        }
    }

    private static String resolveCyberArk(SecretRef.CyberArk ref) {
        String baseUrl = System.getenv("CYBERARK_CCP_URL");
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalStateException("secret ref \"cyberark:" + ref.query() + "\" needs CYBERARK_CCP_URL "
                    + "set on the Advisor process (base URL of the Central Credential Provider's "
                    + "AIMWebService) -- not optional once any connection uses a cyberark: password reference");
        }
        String url = baseUrl.replaceAll("/$", "") + "/AIMWebService/api/Accounts?" + ref.query();
        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET();
            String clientCert = System.getenv("CYBERARK_CLIENT_CERT_HEADER");
            if (clientCert != null && !clientCert.isBlank()) {
                requestBuilder.header("Authorization", clientCert);
            }
            HttpResponse<String> response = CLIENT.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("CyberArk CCP returned HTTP " + response.statusCode() + " for "
                        + ref.query() + ": " + response.body());
            }
            JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
            if (!body.has("Content")) {
                throw new IllegalStateException("CyberArk CCP response for " + ref.query() + " had no \"Content\" field");
            }
            return body.get("Content").getAsString();
        } catch (java.io.IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("failed to reach CyberArk CCP at " + baseUrl + " for " + ref.query(), e);
        }
    }

    /** Credentials come from the AWS SDK's own default provider chain (env vars, {@code
     * ~/.aws/credentials}, an EC2/ECS/EKS instance role, ...) -- this class never reads an AWS
     * key/secret itself, the same "let the vendor SDK own auth" choice every {@code
     * nexagres-migration} DynamoDB/SQS connector already makes. {@code region} on the ref
     * overrides the SDK's own region resolution (env {@code AWS_REGION}/profile/IMDS) when a
     * connection's secret lives in a different region than the process's default. */
    private static String resolveAwsSecretsManager(SecretRef.AwsSecretsManager ref) {
        var builder = software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.builder();
        if (ref.region() != null && !ref.region().isBlank()) {
            builder.region(software.amazon.awssdk.regions.Region.of(ref.region()));
        }
        try (var client = builder.build()) {
            String secretString = client.getSecretValue(
                    software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest.builder()
                            .secretId(ref.secretId())
                            .build())
                    .secretString();
            if (secretString == null) {
                throw new IllegalStateException("AWS Secrets Manager secret \"" + ref.secretId()
                        + "\" has no SecretString (binary secrets aren't supported here)");
            }
            if (ref.field() == null) {
                return secretString;
            }
            JsonObject fields = JsonParser.parseString(secretString).getAsJsonObject();
            if (!fields.has(ref.field())) {
                throw new IllegalStateException("AWS Secrets Manager secret \"" + ref.secretId()
                        + "\" has no field \"" + ref.field() + "\"");
            }
            return fields.get(ref.field()).getAsString();
        } catch (software.amazon.awssdk.core.exception.SdkException e) {
            throw new IllegalStateException("failed to read AWS Secrets Manager secret \"" + ref.secretId() + "\"", e);
        }
    }

    /** OAuth2 client-credentials flow against Azure AD ({@code AZURE_TENANT_ID}/{@code
     * AZURE_CLIENT_ID}/{@code AZURE_CLIENT_SECRET}, all required and not optional once any
     * connection uses an {@code azurekv:} reference -- a service principal with at least
     * "Get Secret" on the target vault), then a plain REST read of the secret -- no Azure SDK
     * dependency, same {@code java.net.http}-plus-Gson shape as this class's own Vault/CyberArk
     * resolvers. */
    private static String resolveAzureKeyVault(SecretRef.AzureKeyVault ref) {
        String tenantId = System.getenv("AZURE_TENANT_ID");
        String clientId = System.getenv("AZURE_CLIENT_ID");
        String clientSecret = System.getenv("AZURE_CLIENT_SECRET");
        if (isBlank(tenantId) || isBlank(clientId) || isBlank(clientSecret)) {
            throw new IllegalStateException("secret ref \"azurekv:" + ref.vaultName() + "/" + ref.secretName()
                    + "\" needs AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET all set on the "
                    + "Advisor process -- none are optional once any connection uses an azurekv: reference");
        }
        try {
            String tokenBody = "grant_type=client_credentials&client_id=" + urlEncode(clientId)
                    + "&client_secret=" + urlEncode(clientSecret)
                    + "&scope=" + urlEncode("https://vault.azure.net/.default");
            HttpRequest tokenRequest = HttpRequest.newBuilder(
                            URI.create("https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token"))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(tokenBody))
                    .build();
            HttpResponse<String> tokenResponse = CLIENT.send(tokenRequest, HttpResponse.BodyHandlers.ofString());
            if (tokenResponse.statusCode() != 200) {
                throw new IllegalStateException("Azure AD token request failed with HTTP " + tokenResponse.statusCode()
                        + ": " + tokenResponse.body());
            }
            String accessToken = JsonParser.parseString(tokenResponse.body()).getAsJsonObject()
                    .get("access_token").getAsString();

            String versionSegment = ref.version() == null ? "" : "/" + ref.version();
            String url = "https://" + ref.vaultName() + ".vault.azure.net/secrets/" + ref.secretName()
                    + versionSegment + "?api-version=7.4";
            HttpRequest secretRequest = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            HttpResponse<String> secretResponse = CLIENT.send(secretRequest, HttpResponse.BodyHandlers.ofString());
            if (secretResponse.statusCode() != 200) {
                throw new IllegalStateException("Azure Key Vault returned HTTP " + secretResponse.statusCode()
                        + " for " + url + ": " + secretResponse.body());
            }
            return JsonParser.parseString(secretResponse.body()).getAsJsonObject().get("value").getAsString();
        } catch (java.io.IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("failed to reach Azure Key Vault \"" + ref.vaultName() + "\" for secret "
                    + ref.secretName(), e);
        }
    }

    /** Service-account JWT-bearer flow ({@code GOOGLE_APPLICATION_CREDENTIALS} pointing at a
     * downloaded service-account JSON key -- the standard GCP convention every {@code
     * google-cloud-*} client library also reads), then a plain REST read of the secret version.
     * Deliberately scoped to the service-account-key path, not the GCE/GKE metadata-server
     * default-credentials path -- explicit here and in this class's own javadoc, not silently
     * implied to cover every GCP credential source (same honest-scoping choice {@code SsoAuth}
     * makes about HS256 vs. full OIDC). A DMS install running outside GCP (which is the common
     * case -- this is an on-prem/portable admin tool, not a GCP-only one) always has a key file
     * anyway; one running ON GCP can still use this path with a key, just not the metadata
     * server's own ambient credentials. */
    private static String resolveGcpSecretManager(SecretRef.GcpSecretManager ref) {
        String keyPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (isBlank(keyPath)) {
            throw new IllegalStateException("secret ref \"gcpsm:" + ref.projectId() + "/" + ref.secretId()
                    + "\" needs GOOGLE_APPLICATION_CREDENTIALS set on the Advisor process, pointing at a "
                    + "downloaded service-account JSON key with Secret Manager Secret Accessor on this secret "
                    + "-- not optional once any connection uses a gcpsm: reference");
        }
        try {
            String keyJson = java.nio.file.Files.readString(java.nio.file.Path.of(keyPath));
            JsonObject key = JsonParser.parseString(keyJson).getAsJsonObject();
            String clientEmail = key.get("client_email").getAsString();
            String privateKeyPem = key.get("private_key").getAsString();

            long now = System.currentTimeMillis() / 1000;
            String header = base64Url("{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
            String claims = base64Url(String.format(
                    "{\"iss\":\"%s\",\"scope\":\"https://www.googleapis.com/auth/cloud-platform\","
                            + "\"aud\":\"https://oauth2.googleapis.com/token\",\"exp\":%d,\"iat\":%d}",
                    clientEmail, now + 3600, now));
            String signingInput = header + "." + claims;
            byte[] signature = signRs256(signingInput, privateKeyPem);
            String assertion = signingInput + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(signature);

            String tokenBody = "grant_type=" + urlEncode("urn:ietf:params:oauth:grant-type:jwt-bearer")
                    + "&assertion=" + urlEncode(assertion);
            HttpRequest tokenRequest = HttpRequest.newBuilder(URI.create("https://oauth2.googleapis.com/token"))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(tokenBody))
                    .build();
            HttpResponse<String> tokenResponse = CLIENT.send(tokenRequest, HttpResponse.BodyHandlers.ofString());
            if (tokenResponse.statusCode() != 200) {
                throw new IllegalStateException("GCP token exchange failed with HTTP " + tokenResponse.statusCode()
                        + ": " + tokenResponse.body());
            }
            String accessToken = JsonParser.parseString(tokenResponse.body()).getAsJsonObject()
                    .get("access_token").getAsString();

            String version = ref.version() == null ? "latest" : ref.version();
            String url = "https://secretmanager.googleapis.com/v1/projects/" + ref.projectId() + "/secrets/"
                    + ref.secretId() + "/versions/" + version + ":access";
            HttpRequest secretRequest = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            HttpResponse<String> secretResponse = CLIENT.send(secretRequest, HttpResponse.BodyHandlers.ofString());
            if (secretResponse.statusCode() != 200) {
                throw new IllegalStateException("GCP Secret Manager returned HTTP " + secretResponse.statusCode()
                        + " for " + url + ": " + secretResponse.body());
            }
            String base64Data = JsonParser.parseString(secretResponse.body()).getAsJsonObject()
                    .getAsJsonObject("payload").get("data").getAsString();
            return new String(Base64.getDecoder().decode(base64Data), java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("failed to reach GCP Secret Manager for project " + ref.projectId()
                    + ", secret " + ref.secretId(), e);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("failed to sign the GCP service-account JWT from " + keyPath, e);
        }
    }

    private static byte[] signRs256(String signingInput, String pkcs8Pem) throws java.security.GeneralSecurityException {
        String cleaned = pkcs8Pem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        byte[] keyBytes = Base64.getDecoder().decode(cleaned);
        java.security.PrivateKey privateKey = java.security.KeyFactory.getInstance("RSA")
                .generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(keyBytes));
        java.security.Signature signature = java.security.Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(signingInput.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return signature.sign();
    }

    private static String base64Url(String json) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String urlEncode(String value) {
        return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
