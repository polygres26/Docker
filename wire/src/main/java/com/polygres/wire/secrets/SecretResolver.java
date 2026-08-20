package com.polygres.wire.secrets;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a {@link SecretRef} to the actual value at connect time. Both backends are opt-in via
 * env vars (unset = that scheme errors clearly instead of silently returning null) and both fail
 * loud rather than falling back to an empty password -- a wrong secret is a config bug worth
 * surfacing immediately, not a connection that quietly tries an empty credential.
 */
public final class SecretResolver {

    private static final Logger log = LoggerFactory.getLogger(SecretResolver.class);
    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private SecretResolver() {
    }

    public static String resolve(SecretRef ref) {
        return switch (ref) {
            case SecretRef.Plaintext p -> p.value();
            case SecretRef.Vault v -> resolveVault(v);
            case SecretRef.CyberArk c -> resolveCyberArk(c);
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
                    + "VAULT_TOKEN set on the PolyWire process -- neither is optional once any backend uses a "
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
            // KV v2 nests the actual secret under data.data; fall back to data.<field> for KV v1
            // mounts so both engine versions resolve without a separate config flag.
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
                    + "set on the PolyWire process (base URL of the Central Credential Provider's "
                    + "AIMWebService) -- not optional once any backend uses a cyberark: password reference");
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
}
