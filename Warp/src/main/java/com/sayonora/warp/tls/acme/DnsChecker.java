package com.sayonora.warp.tls.acme;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Hashtable;

/** Waits for a dns-01 TXT record to propagate: a fixed wait ({@code none}) or polling the system resolver / a DNS-over-HTTPS JSON API. */
public final class DnsChecker {

    private final String mode;
    private final String dohUrl;
    private final HttpClient http;

    public DnsChecker(String mode, String dohUrl, HttpClient http) {
        this.mode = mode;
        this.dohUrl = dohUrl;
        this.http = http;
    }

    /** @return true if the value became visible (or the mode is "none" and the wait elapsed), false if the wait ran out first. */
    public boolean await(String txtName, String value, int waitSeconds) throws InterruptedException {
        if ("none".equals(mode)) {
            Thread.sleep(waitSeconds * 1000L);
            return true;
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(waitSeconds).toNanos();
        while (true) {
            if (visible(txtName, value)) {
                return true;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(Math.min(2000, Math.max(50, waitSeconds * 100L)));
        }
    }

    boolean visible(String txtName, String value) {
        try {
            return "doh".equals(mode) ? viaDoh(txtName, value) : viaSystem(txtName, value);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean viaDoh(String name, String value) throws Exception {
        String sep = dohUrl.contains("?") ? "&" : "?";
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(dohUrl + sep + "name="
                + java.net.URLEncoder.encode(name, java.nio.charset.StandardCharsets.UTF_8) + "&type=TXT"))
                .header("Accept", "application/dns-json").timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonObject j = JsonParser.parseString(r.body()).getAsJsonObject();
        if (!j.has("Answer")) {
            return false;
        }
        for (JsonElement a : j.getAsJsonArray("Answer")) {
            if (a.getAsJsonObject().has("data") && a.getAsJsonObject().get("data").getAsString().replace("\"", "").contains(value)) {
                return true;
            }
        }
        return false;
    }

    private boolean viaSystem(String name, String value) throws Exception {
        Hashtable<String, String> env = new Hashtable<>();
        env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
        env.put("java.naming.provider.url", "dns:");
        env.put("com.sun.jndi.dns.timeout.initial", "2000");
        env.put("com.sun.jndi.dns.timeout.retries", "1");
        javax.naming.directory.DirContext ctx = new javax.naming.directory.InitialDirContext(env);
        try {
            javax.naming.directory.Attribute a = ctx.getAttributes(name, new String[] {"TXT"}).get("TXT");
            if (a == null) {
                return false;
            }
            for (int i = 0; i < a.size(); i++) {
                if (String.valueOf(a.get(i)).replace("\"", "").contains(value)) {
                    return true;
                }
            }
            return false;
        } finally {
            ctx.close();
        }
    }
}
