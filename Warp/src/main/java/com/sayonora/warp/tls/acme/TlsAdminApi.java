package com.sayonora.warp.tls.acme;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.warp.tls.TlsProvider;
import com.sayonora.warp.tls.TlsSettings;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Bodies of {@code GET /api/tls/certificates} and {@code POST /api/tls/renew}. */
public final class TlsAdminApi {

    private TlsAdminApi() {
    }

    public static JsonObject certificates(Map<String, String> env) {
        JsonObject out = new JsonObject();
        AcmeService acme = AcmeService.instance();
        JsonObject a = acme == null ? null : acme.statusJson();
        out.add("acme", a == null ? disabled() : a);
        Instant now = Instant.now();
        Map<String, JsonObject> byKey = new LinkedHashMap<>();
        for (String listener : new String[] {"ADMIN", "MCP", "A2A"}) {
            try {
                TlsSettings s = TlsSettings.resolve(listener, env);
                if (s == null) {
                    continue;
                }
                JsonObject e = byKey.get(s.cacheKey());
                if (e == null) {
                    TlsProvider p = TlsProvider.get(s);
                    CertInfo c = CertInfo.of(p.material().leaf());
                    e = AcmeService.certJson(c, now);
                    boolean isAcme = s.origin() != null && s.origin().startsWith("ACME");
                    String source = isAcme ? (c.placeholder() ? "self-signed" : "acme")
                            : s.source() == TlsSettings.Source.SELF_SIGNED ? "self-signed" : "file";
                    e.addProperty("source", source);
                    e.addProperty("origin", s.origin());
                    e.addProperty("listeners", "");
                    if (isAcme && acme != null && acme.enabled()) {
                        JsonObject st = acme.statusJson();
                        for (String k : new String[] {"lastRenewal", "lastError", "nextCheck", "challenge", "directory", "staging", "issuing", "failures"}) {
                            if (st.has(k)) {
                                e.add(k, st.get(k));
                            }
                        }
                    } else {
                        e.add("lastRenewal", com.google.gson.JsonNull.INSTANCE);
                        e.add("lastError", com.google.gson.JsonNull.INSTANCE);
                    }
                    byKey.put(s.cacheKey(), e);
                }
                String l = e.get("listeners").getAsString();
                e.addProperty("listeners", l.isEmpty() ? listener : l + "," + listener);
            } catch (RuntimeException ex) {
                JsonObject e = new JsonObject();
                e.addProperty("source", "unavailable");
                e.addProperty("lastError", ex.getMessage());
                e.addProperty("listeners", listener);
                byKey.put("err:" + listener, e);
            }
        }
        JsonArray arr = new JsonArray();
        byKey.values().forEach(arr::add);
        out.add("certificates", arr);
        return out;
    }

    private static JsonObject disabled() {
        JsonObject o = new JsonObject();
        o.addProperty("enabled", false);
        return o;
    }

    /** @return {status, body} */
    public static Object[] renew() {
        AcmeService acme = AcmeService.instance();
        JsonObject o = new JsonObject();
        if (acme == null) {
            o.addProperty("error", "ACME is not configured (set WARP_ACME_DOMAINS)");
            return new Object[] {409, o};
        }
        AcmeService.RenewResult r = acme.requestRenewal();
        if (r.accepted()) {
            o.addProperty("status", "started");
            o.addProperty("message", r.message());
        } else {
            o.addProperty("error", r.message());
            if (r.retryAt() != null) {
                o.addProperty("retryAt", r.retryAt().toString());
            }
        }
        return new Object[] {r.httpStatus(), o};
    }
}
