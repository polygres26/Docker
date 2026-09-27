package com.sayonora.warp.tls.acme;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** dns-01 through the Cloudflare v4 API (bearer token {@code CLOUDFLARE_API_TOKEN}; zone found by walking the name's labels). */
public final class CloudflareDnsProvider implements DnsProvider {

    private final String api;
    private final String token;
    private final HttpClient http;

    public CloudflareDnsProvider(String apiBase, String token, HttpClient http) {
        this.api = apiBase.endsWith("/") ? apiBase.substring(0, apiBase.length() - 1) : apiBase;
        this.token = token;
        this.http = http;
    }

    @Override
    public String name() {
        return "cloudflare";
    }

    private JsonObject call(String method, String path, String body) throws IOException {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(api + path)).timeout(Duration.ofSeconds(30))
                .header("Authorization", "Bearer " + token).header("Content-Type", "application/json");
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        try {
            HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            JsonObject j;
            try {
                j = JsonParser.parseString(r.body()).getAsJsonObject();
            } catch (RuntimeException e) {
                throw new IOException("Cloudflare answered HTTP " + r.statusCode() + " with a non-JSON body");
            }
            if (r.statusCode() / 100 != 2 || (j.has("success") && !j.get("success").getAsBoolean())) {
                String msg = "";
                if (j.has("errors") && j.get("errors").isJsonArray()) {
                    for (JsonElement e : j.getAsJsonArray("errors")) {
                        msg += (msg.isEmpty() ? "" : "; ") + (e.isJsonObject() && e.getAsJsonObject().has("message")
                                ? e.getAsJsonObject().get("message").getAsString() : e.toString());
                    }
                }
                throw new IOException("Cloudflare API " + method + " " + path.split("\\?")[0] + " failed (HTTP " + r.statusCode() + "): " + msg);
            }
            return j;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    /** The zone id: the longest suffix of {@code domain} that Cloudflare knows as an active zone. */
    String zoneId(String domain) throws IOException {
        String[] labels = domain.split("\\.");
        for (int i = 0; i <= labels.length - 2; i++) {
            String cand = String.join(".", java.util.Arrays.copyOfRange(labels, i, labels.length));
            JsonArray res = call("GET", "/zones?name=" + enc(cand), null).getAsJsonArray("result");
            if (res != null && res.size() > 0) {
                return res.get(0).getAsJsonObject().get("id").getAsString();
            }
        }
        throw new IOException("no Cloudflare zone found for " + domain + " (does the API token have Zone:Read?)");
    }

    @Override
    public void present(String domain, String txtName, String txtValue) throws IOException {
        String zone = zoneId(domain);
        JsonObject rec = new JsonObject();
        rec.addProperty("type", "TXT");
        rec.addProperty("name", txtName);
        rec.addProperty("content", txtValue);
        rec.addProperty("ttl", 60);
        call("POST", "/zones/" + zone + "/dns_records", rec.toString());
    }

    @Override
    public void cleanup(String domain, String txtName, String txtValue) throws IOException {
        String zone = zoneId(domain);
        JsonArray res = call("GET", "/zones/" + zone + "/dns_records?type=TXT&name=" + enc(txtName) + "&content=" + enc(txtValue), null)
                .getAsJsonArray("result");
        if (res == null) {
            return;
        }
        for (JsonElement e : res) {
            call("DELETE", "/zones/" + zone + "/dns_records/" + e.getAsJsonObject().get("id").getAsString(), null);
        }
    }
}
