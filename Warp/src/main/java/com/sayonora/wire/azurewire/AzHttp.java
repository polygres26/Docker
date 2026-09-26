package com.sayonora.wire.azurewire;

import com.google.gson.JsonObject;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/** HTTP/Azure conventions shared by the three services: dates, etags, request-id headers, error bodies. */
final class AzHttp {

    private AzHttp() {
    }

    static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US).withZone(ZoneOffset.UTC);
    static final DateTimeFormatter ISO_MS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US).withZone(ZoneOffset.UTC);
    static final DateTimeFormatter ISO_S =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US).withZone(ZoneOffset.UTC);

    static String httpDate(Instant t) {
        return HTTP_DATE.format(t);
    }

    static Instant parseHttpDate(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            String t = s.trim();
            int comma = t.indexOf(", ");
            if (comma == 3) {
                t = t.substring(5);
            }
            return ZonedDateTime.parse(t, DateTimeFormatter.ofPattern("d MMM yyyy HH:mm:ss zzz", Locale.US)).toInstant();
        } catch (RuntimeException e) {
            try {
                return ZonedDateTime.parse(s.trim(), DateTimeFormatter.ofPattern("EEEE, dd-MMM-yy HH:mm:ss zzz", Locale.US)).toInstant();
            } catch (RuntimeException e2) {
                return null;
            }
        }
    }

    private static final AtomicLong ETAG_CLOCK = new AtomicLong();

    /** A quoted Azure-style etag {@code "0x8DBA..."}: unique, increasing per process. */
    static String newEtag() {
        long ticks = Instant.now().toEpochMilli() * 10_000L + 116_444_736_000_000_000L;
        long v = ETAG_CLOCK.updateAndGet(prev -> Math.max(prev + 1, ticks));
        return "\"0x" + Long.toHexString(v).toUpperCase(Locale.ROOT) + "\"";
    }

    private static final SecureRandom RND = new SecureRandom();

    static String randomToken(int bytes) {
        byte[] b = new byte[bytes];
        RND.nextBytes(b);
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    static void setCommon(HttpServletResponse resp, AzReq r, String serverName) {
        resp.setHeader("Server", serverName);
        resp.setHeader("x-ms-request-id", r.requestId);
        String v = r.header("x-ms-version");
        resp.setHeader("x-ms-version", v != null && !v.isBlank() ? v : "2021-08-06");
        String cid = r.header("x-ms-client-request-id");
        if (cid != null) {
            resp.setHeader("x-ms-client-request-id", cid);
        }
        resp.setHeader("Date", httpDate(Instant.now()));
    }

    static void xml(HttpServletResponse resp, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        resp.setStatus(status);
        resp.setContentType("application/xml");
        resp.setContentLength(b.length);
        resp.getOutputStream().write(b);
    }

    static String errorMessage(AzureException e, AzReq r) {
        String m = e.getMessage() == null ? "" : e.getMessage();
        if (m.contains("\nRequestId:")) {
            return m;
        }
        return m + "\nRequestId:" + r.requestId + "\nTime:" + AzHttp.ISO_MS.format(Instant.now());
    }

    /** XML error body of Blob/Queue: {@code <Error><Code/><Message/>...}. */
    static String xmlError(AzureException e, AzReq r) {
        AzXml x = new AzXml().open("Error").text("Code", e.code).text("Message", errorMessage(e, r));
        e.extra.forEach(x::text);
        return x.close().toString();
    }

    /** OData JSON error body of Table. */
    static String jsonError(AzureException e, AzReq r, String accept) {
        JsonObject err = new JsonObject();
        err.addProperty("code", e.code);
        JsonObject msg = new JsonObject();
        msg.addProperty("lang", "en-US");
        String m = errorMessage(e, r);
        String detail = e.extra.get("AuthenticationErrorDetail");
        if (detail != null) {
            m = m + "\n" + detail;
        }
        msg.addProperty("value", m);
        err.add("message", msg);
        JsonObject root = new JsonObject();
        root.add("odata.error", err);
        return root.toString();
    }
}
