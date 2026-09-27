package com.sayonora.warp.tls.acme;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * dns-01 through Route 53 (SigV4-signed REST + XML). Credentials from {@code AWS_ACCESS_KEY_ID} / {@code AWS_SECRET_ACCESS_KEY}
 * (+ {@code AWS_SESSION_TOKEN}); the hosted zone is found by walking the name's labels unless {@code WARP_ACME_ROUTE53_ZONE_ID}
 * is set. Waits for the change to reach INSYNC (bounded).
 */
public final class Route53DnsProvider implements DnsProvider {

    private static final Pattern ID = Pattern.compile("<Id>\\s*/?(?:hostedzone/|change/)?([^<\\s]+)\\s*</Id>");
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String fixedZone;
    private final HttpClient http;
    private final int insyncTimeoutSeconds;

    public Route53DnsProvider(String endpoint, String accessKey, String secretKey, String sessionToken, String fixedZone,
            HttpClient http, int insyncTimeoutSeconds) {
        this.endpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
        this.fixedZone = fixedZone;
        this.http = http;
        this.insyncTimeoutSeconds = insyncTimeoutSeconds;
    }

    @Override
    public String name() {
        return "route53";
    }

    private String call(String method, String path, String query, String body) throws IOException {
        URI uri = URI.create(endpoint + path + (query.isEmpty() ? "" : "?" + query));
        String host = uri.getPort() > 0 && uri.getPort() != 443 && uri.getPort() != 80 ? uri.getHost() + ":" + uri.getPort() : uri.getHost();
        byte[] payload = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);
        Map<String, String> headers = AwsSigner.sign(method, host, path, query, payload, "us-east-1", "route53", accessKey, secretKey,
                sessionToken, Instant.now());
        HttpRequest.Builder b = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30));
        headers.forEach(b::header);
        if (body != null) {
            b.header("Content-Type", "text/xml");
        }
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        try {
            HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (r.statusCode() / 100 != 2) {
                Matcher m = Pattern.compile("<Message>([^<]*)</Message>").matcher(r.body() == null ? "" : r.body());
                throw new IOException("Route 53 " + method + " " + path + " failed (HTTP " + r.statusCode() + ")" + (m.find() ? ": " + m.group(1) : ""));
            }
            return r.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    String zoneId(String domain) throws IOException {
        if (fixedZone != null) {
            return fixedZone;
        }
        String[] labels = domain.split("\\.");
        for (int i = 0; i <= labels.length - 2; i++) {
            String cand = String.join(".", java.util.Arrays.copyOfRange(labels, i, labels.length));
            String xml = call("GET", "/2013-04-01/hostedzonesbyname", "dnsname=" + URLEncoder.encode(cand, StandardCharsets.UTF_8) + "&maxitems=1", null);
            Matcher z = Pattern.compile("<HostedZone>\\s*<Id>\\s*/?hostedzone/([^<\\s]+)\\s*</Id>\\s*<Name>([^<]+)</Name>").matcher(xml);
            if (z.find() && z.group(2).equalsIgnoreCase(cand + ".")) {
                return z.group(1);
            }
        }
        throw new IOException("no Route 53 hosted zone found for " + domain);
    }

    static String changeXml(String action, String name, String value) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ChangeResourceRecordSetsRequest xmlns=\"https://route53.amazonaws.com/doc/2013-04-01/\">"
                + "<ChangeBatch><Comment>warp acme dns-01</Comment><Changes><Change><Action>" + action + "</Action><ResourceRecordSet><Name>"
                + name + "</Name><Type>TXT</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>&quot;" + value
                + "&quot;</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch>"
                + "</ChangeResourceRecordSetsRequest>";
    }

    @Override
    public void present(String domain, String txtName, String txtValue) throws IOException {
        String zone = zoneId(domain);
        String xml = call("POST", "/2013-04-01/hostedzone/" + zone + "/rrset", "", changeXml("UPSERT", txtName, txtValue));
        Matcher m = ID.matcher(xml);
        if (m.find()) {
            waitInSync(m.group(1));
        }
    }

    private void waitInSync(String changeId) throws IOException {
        long deadline = System.nanoTime() + Duration.ofSeconds(insyncTimeoutSeconds).toNanos();
        while (System.nanoTime() < deadline) {
            String xml = call("GET", "/2013-04-01/change/" + changeId, "", null);
            if (xml.contains("<Status>INSYNC</Status>")) {
                return;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        // not fatal: the CA retries validation lookups; the propagation wait in the caller continues
    }

    @Override
    public void cleanup(String domain, String txtName, String txtValue) throws IOException {
        String zone = zoneId(domain);
        call("POST", "/2013-04-01/hostedzone/" + zone + "/rrset", "", changeXml("DELETE", txtName, txtValue));
    }
}
