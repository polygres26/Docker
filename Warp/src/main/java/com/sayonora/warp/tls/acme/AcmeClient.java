package com.sayonora.warp.tls.acme;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small RFC 8555 client: directory, newNonce, newAccount (optionally with external account binding), newOrder,
 * authorization + challenge, finalize with a CSR, order polling and certificate download. ES256 (or RS256) account keys.
 * badNonce is retried transparently (up to 3 times); every other problem document becomes an {@link AcmeException}.
 * Nothing secret (account key, private keys, key authorizations of dns-01 beyond the public TXT value) is ever logged.
 */
public final class AcmeClient {

    private static final Logger log = LoggerFactory.getLogger(AcmeClient.class);

    /** Publishes / removes a challenge response (http-01 file content or dns-01 TXT record). */
    public interface Responder {
        /** For http-01: serve {@code keyAuthorization} at /.well-known/acme-challenge/{token}. For dns-01: publish the TXT
         * record {@code _acme-challenge.<domain>} = {@code AcmeCrypto.dnsTxtValue(keyAuthorization)} and wait for propagation. */
        void present(String domain, String type, String token, String keyAuthorization) throws Exception;

        void cleanup(String domain, String type, String token, String keyAuthorization);
    }

    public record Issued(String fullchainPem, String orderUrl) {
    }

    /** Where polling stands, injectable for tests. */
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final String directoryUrl;
    private final HttpClient http;
    private final long pollMillis;
    private final Duration pollTimeout;
    private final Sleeper sleeper;
    private JsonObject directory;
    private String nonce;

    public AcmeClient(String directoryUrl, HttpClient http, long pollMillis, Duration pollTimeout) {
        this(directoryUrl, http, pollMillis, pollTimeout, Thread::sleep);
    }

    public AcmeClient(String directoryUrl, HttpClient http, long pollMillis, Duration pollTimeout, Sleeper sleeper) {
        this.directoryUrl = directoryUrl;
        this.http = http;
        this.pollMillis = pollMillis;
        this.pollTimeout = pollTimeout;
        this.sleeper = sleeper;
    }

    // ---------- transport ----------

    record Resp(int status, java.net.http.HttpHeaders headers, String body) {
        String header(String n) {
            return headers.firstValue(n).orElse(null);
        }

        JsonObject json() {
            return body == null || body.isBlank() ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
        }
    }

    private Resp send(HttpRequest.Builder b) {
        try {
            HttpResponse<String> r = http.send(b.timeout(Duration.ofSeconds(30)).build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return new Resp(r.statusCode(), r.headers(), r.body());
        } catch (IOException e) {
            throw new AcmeException("cannot reach the ACME server: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AcmeException("interrupted", e);
        }
    }

    private JsonObject directory() {
        if (directory == null) {
            Resp r = send(HttpRequest.newBuilder(URI.create(directoryUrl)).GET().header("Accept", "application/json"));
            if (r.status() != 200) {
                throw problem(r, "directory");
            }
            try {
                directory = r.json();
            } catch (RuntimeException e) {
                throw new AcmeException("the ACME directory " + directoryUrl + " did not return JSON", e);
            }
            for (String k : new String[] {"newNonce", "newAccount", "newOrder"}) {
                if (!directory.has(k)) {
                    throw new AcmeException("the ACME directory has no \"" + k + "\" entry");
                }
            }
        }
        return directory;
    }

    public JsonObject meta() {
        JsonObject d = directory();
        return d.has("meta") && d.get("meta").isJsonObject() ? d.getAsJsonObject("meta") : new JsonObject();
    }

    private String takeNonce() {
        if (nonce != null) {
            String n = nonce;
            nonce = null;
            return n;
        }
        Resp r = send(HttpRequest.newBuilder(URI.create(directory().get("newNonce").getAsString()))
                .method("HEAD", HttpRequest.BodyPublishers.noBody()));
        String n = r.header("Replay-Nonce");
        if (n == null) {
            throw new AcmeException("newNonce did not return a Replay-Nonce header");
        }
        return n;
    }

    /** Signed POST with badNonce retry. {@code payload} null = POST-as-GET. */
    Resp post(KeyPair key, String kid, String url, String payload, String accept) {
        AcmeException last = null;
        for (int attempt = 0; attempt < 4; attempt++) {
            String body = AcmeCrypto.jws(key, kid, takeNonce(), url, payload);
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                    .header("Content-Type", "application/jose+json")
                    .header("Accept", accept == null ? "application/json" : accept)
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            Resp r = send(b);
            if (r.header("Replay-Nonce") != null) {
                nonce = r.header("Replay-Nonce");
            }
            if (r.status() >= 200 && r.status() < 300) {
                return r;
            }
            AcmeException e = problem(r, url);
            if (e.isBadNonce()) {
                last = e;
                log.debug("ACME badNonce on {}, retrying with a fresh nonce", url);
                continue;
            }
            throw e;
        }
        throw last;
    }

    static AcmeException problem(Resp r, String what) {
        String type = null;
        String detail = null;
        try {
            JsonObject j = r.json();
            type = j.has("type") ? j.get("type").getAsString() : null;
            detail = j.has("detail") ? j.get("detail").getAsString() : null;
            if (j.has("subproblems") && j.get("subproblems").isJsonArray()) {
                List<String> subs = new ArrayList<>();
                for (JsonElement s : j.getAsJsonArray("subproblems")) {
                    JsonObject so = s.getAsJsonObject();
                    String id = so.has("identifier") && so.get("identifier").isJsonObject()
                            ? so.getAsJsonObject("identifier").get("value").getAsString() : "";
                    subs.add(id + ": " + (so.has("detail") ? so.get("detail").getAsString() : so.get("type").getAsString()));
                }
                detail = (detail == null ? "" : detail + " ") + "[" + String.join("; ", subs) + "]";
            }
        } catch (RuntimeException ignored) {
            // not JSON
        }
        if (detail == null) {
            String b = r.body() == null ? "" : r.body().strip();
            detail = b.length() > 200 ? b.substring(0, 200) : b;
        }
        String shortType = type == null ? "" : type.startsWith(AcmeException.URN) ? type.substring(AcmeException.URN.length()) : type;
        return new AcmeException("ACME server answered HTTP " + r.status() + " for " + what + (shortType.isEmpty() ? "" : " (" + shortType + ")")
                + (detail.isEmpty() ? "" : ": " + detail), r.status(), type, retryAfter(r.header("Retry-After")), null);
    }

    static Duration retryAfter(String v) {
        if (v == null || v.isBlank()) {
            return null;
        }
        try {
            return Duration.ofSeconds(Math.max(0, Long.parseLong(v.trim())));
        } catch (NumberFormatException e) {
            try {
                Duration d = Duration.between(Instant.now(), ZonedDateTime.parse(v.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
                return d.isNegative() ? Duration.ZERO : d;
            } catch (RuntimeException e2) {
                return null;
            }
        }
    }

    // ---------- account ----------

    /** Creates (or, if the key is already registered, looks up) the account. @return the account URL (the "kid"). */
    public String newAccount(KeyPair accountKey, String email, boolean termsAgreed, String eabKid, String eabHmacKeyB64) {
        String url = directory().get("newAccount").getAsString();
        JsonObject p = new JsonObject();
        p.addProperty("termsOfServiceAgreed", termsAgreed);
        if (email != null) {
            JsonArray c = new JsonArray();
            c.add("mailto:" + email);
            p.add("contact", c);
        }
        if (eabKid != null) {
            p.add("externalAccountBinding", JsonParser.parseString(
                    AcmeCrypto.eabJws(eabKid, AcmeCrypto.unb64url(eabHmacKeyB64), accountKey.getPublic(), url)));
        }
        Resp r = post(accountKey, null, url, p.toString(), null);
        String kid = r.header("Location");
        if (kid == null) {
            throw new AcmeException("newAccount did not return an account URL (Location header)");
        }
        return kid;
    }

    // ---------- order ----------

    /**
     * Runs a whole order: newOrder, every authorization through {@code responder}, finalize with a CSR for {@code certKey}, and
     * the certificate chain download.
     */
    public Issued obtain(KeyPair accountKey, String kid, List<String> domains, String challengeType, KeyPair certKey,
            Responder responder) {
        JsonArray ids = new JsonArray();
        for (String d : domains) {
            JsonObject o = new JsonObject();
            o.addProperty("type", "dns");
            // RFC 8555 s7.1.3: wildcard names are requested as "*.example.com" in the order identifier
            o.addProperty("value", d);
            ids.add(o);
        }
        JsonObject payload = new JsonObject();
        payload.add("identifiers", ids);
        Resp created = post(accountKey, kid, directory().get("newOrder").getAsString(), payload.toString(), null);
        String orderUrl = created.header("Location");
        if (orderUrl == null) {
            throw new AcmeException("newOrder did not return an order URL (Location header)");
        }
        JsonObject order = created.json();
        log.info("ACME order created for {}", domains);
        String status = str(order, "status");
        if ("pending".equals(status)) {
            for (JsonElement a : order.getAsJsonArray("authorizations")) {
                authorize(accountKey, kid, a.getAsString(), challengeType, responder);
            }
            order = pollOrder(accountKey, kid, orderUrl, "ready", "valid");
        } else if ("invalid".equals(status)) {
            throw orderInvalid(order);
        }
        status = str(order, "status");
        if ("ready".equals(status)) {
            JsonObject fin = new JsonObject();
            fin.addProperty("csr", AcmeCrypto.b64url(AcmeCrypto.csr(certKey, domains)));
            Resp fr = post(accountKey, kid, order.get("finalize").getAsString(), fin.toString(), null);
            order = fr.json();
            status = str(order, "status");
        }
        if (!"valid".equals(status)) {
            order = pollOrder(accountKey, kid, orderUrl, "valid");
        }
        if (!order.has("certificate")) {
            throw new AcmeException("the order is valid but carries no certificate URL");
        }
        Resp cert = post(accountKey, kid, order.get("certificate").getAsString(), null, "application/pem-certificate-chain");
        String pem = cert.body();
        if (pem == null || !pem.contains("BEGIN CERTIFICATE")) {
            throw new AcmeException("the certificate download did not return a PEM chain");
        }
        return new Issued(pem, orderUrl);
    }

    private void authorize(KeyPair key, String kid, String authzUrl, String type, Responder responder) {
        JsonObject authz = post(key, kid, authzUrl, null, null).json();
        String status = str(authz, "status");
        String domain = authz.getAsJsonObject("identifier").get("value").getAsString();
        if ("valid".equals(status)) {
            log.info("ACME authorization for {} is already valid", domain);
            return;
        }
        if (!"pending".equals(status)) {
            throw new AcmeException("authorization for " + domain + " is " + status + describeAuthzError(authz));
        }
        JsonObject chal = null;
        for (JsonElement c : authz.getAsJsonArray("challenges")) {
            if (type.equals(str(c.getAsJsonObject(), "type"))) {
                chal = c.getAsJsonObject();
            }
        }
        if (chal == null) {
            throw new AcmeException("the CA offers no " + type + " challenge for " + domain);
        }
        String token = str(chal, "token");
        String keyAuth = AcmeCrypto.keyAuthorization(token, key.getPublic());
        try {
            responder.present(domain, type, token, keyAuth);
            post(key, kid, chal.get("url").getAsString(), "{}", null);
            waitAuthz(key, kid, authzUrl, domain);
            log.info("ACME authorization for {} validated ({})", domain, type);
        } catch (AcmeException e) {
            throw e;
        } catch (Exception e) {
            throw new AcmeException("could not publish the " + type + " response for " + domain + ": " + e.getMessage(), e);
        } finally {
            try {
                responder.cleanup(domain, type, token, keyAuth);
            } catch (RuntimeException e) {
                log.warn("ACME challenge cleanup for {} failed: {}", domain, e.toString());
            }
        }
    }

    private void waitAuthz(KeyPair key, String kid, String authzUrl, String domain) {
        long deadline = System.nanoTime() + pollTimeout.toNanos();
        while (true) {
            Resp r = post(key, kid, authzUrl, null, null);
            JsonObject a = r.json();
            String s = str(a, "status");
            if ("valid".equals(s)) {
                return;
            }
            if ("invalid".equals(s) || "deactivated".equals(s) || "expired".equals(s) || "revoked".equals(s)) {
                throw new AcmeException("authorization for " + domain + " is " + s + describeAuthzError(a));
            }
            if (System.nanoTime() > deadline) {
                throw new AcmeException("timed out waiting for the CA to validate " + domain);
            }
            sleepPoll(r);
        }
    }

    private static String describeAuthzError(JsonObject authz) {
        if (authz.has("challenges")) {
            for (JsonElement c : authz.getAsJsonArray("challenges")) {
                JsonObject co = c.getAsJsonObject();
                if (co.has("error") && co.get("error").isJsonObject()) {
                    JsonObject e = co.getAsJsonObject("error");
                    return ": " + (e.has("detail") ? e.get("detail").getAsString() : str(e, "type"));
                }
            }
        }
        return "";
    }

    private JsonObject pollOrder(KeyPair key, String kid, String orderUrl, String... want) {
        long deadline = System.nanoTime() + pollTimeout.toNanos();
        while (true) {
            Resp r = post(key, kid, orderUrl, null, null);
            JsonObject o = r.json();
            String s = str(o, "status");
            for (String w : want) {
                if (w.equals(s)) {
                    return o;
                }
            }
            if ("invalid".equals(s)) {
                throw orderInvalid(o);
            }
            if (System.nanoTime() > deadline) {
                throw new AcmeException("timed out waiting for the order (still " + s + ")");
            }
            sleepPoll(r);
        }
    }

    private static AcmeException orderInvalid(JsonObject o) {
        String d = "";
        if (o.has("error") && o.get("error").isJsonObject() && o.getAsJsonObject("error").has("detail")) {
            d = ": " + o.getAsJsonObject("error").get("detail").getAsString();
        }
        return new AcmeException("the ACME order became invalid" + d);
    }

    private void sleepPoll(Resp r) {
        Duration ra = retryAfter(r.header("Retry-After"));
        long ms = ra == null ? pollMillis : Math.min(Math.max(ra.toMillis(), pollMillis), 30_000);
        try {
            sleeper.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AcmeException("interrupted", e);
        }
    }

    private static String str(JsonObject o, String k) {
        return o.has(k) && !o.get(k).isJsonNull() ? o.get(k).getAsString() : null;
    }
}
