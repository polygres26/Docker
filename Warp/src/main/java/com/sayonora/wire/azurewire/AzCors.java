package com.sayonora.wire.azurewire;

import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.w3c.dom.Element;

/** CORS rules of a storage service (stored with the service properties): OPTIONS preflight and actual-request headers. */
final class AzCors {

    private AzCors() {
    }

    record Rule(List<String> origins, List<String> methods, List<String> allowedHeaders, List<String> exposed, int maxAge) {
        boolean originOk(String origin) {
            for (String o : origins) {
                if (o.equals("*") || o.equalsIgnoreCase(origin)) {
                    return true;
                }
            }
            return false;
        }

        boolean methodOk(String m) {
            for (String x : methods) {
                if (x.equalsIgnoreCase(m)) {
                    return true;
                }
            }
            return false;
        }

        boolean headersOk(String requested) {
            if (requested == null || requested.isBlank()) {
                return true;
            }
            for (String h : requested.split(",")) {
                String hh = h.trim().toLowerCase(Locale.ROOT);
                boolean ok = false;
                for (String a : allowedHeaders) {
                    String al = a.toLowerCase(Locale.ROOT);
                    if (al.equals("*") || al.equals(hh) || al.endsWith("*") && hh.startsWith(al.substring(0, al.length() - 1))) {
                        ok = true;
                        break;
                    }
                }
                if (!ok) {
                    return false;
                }
            }
            return true;
        }
    }

    private static List<String> split(String s) {
        List<String> l = new ArrayList<>();
        if (s != null) {
            for (String p : s.split(",")) {
                if (!p.isBlank()) {
                    l.add(p.trim());
                }
            }
        }
        return l;
    }

    static List<Rule> parse(String corsXml) {
        List<Rule> out = new ArrayList<>();
        if (corsXml == null || corsXml.equals("<Cors/>")) {
            return out;
        }
        Element root = AzServiceProps.parse(corsXml.getBytes(java.nio.charset.StandardCharsets.UTF_8)).getDocumentElement();
        for (Element rule : AzServiceProps.children(root)) {
            String o = "";
            String m = "";
            String ah = "";
            String eh = "";
            int age = 0;
            for (Element c : AzServiceProps.children(rule)) {
                String t = AzServiceProps.text(c);
                switch (c.getTagName()) {
                    case "AllowedOrigins" -> o = t;
                    case "AllowedMethods" -> m = t;
                    case "AllowedHeaders" -> ah = t;
                    case "ExposedHeaders" -> eh = t;
                    case "MaxAgeInSeconds" -> age = Integer.parseInt(t.trim());
                    default -> {
                    }
                }
            }
            out.add(new Rule(split(o), split(m), split(ah), split(eh), age));
        }
        return out;
    }

    static void preflight(AzReq r, HttpServletResponse resp, List<Rule> rules) {
        String origin = r.header("Origin");
        String method = r.header("Access-Control-Request-Method");
        if (origin == null) {
            throw new AzureException(400, "MissingRequiredHeader", "Origin header is required for CORS preflight.")
                    .extra("HeaderName", "Origin");
        }
        String reqHeaders = r.header("Access-Control-Request-Headers");
        for (Rule rule : rules) {
            if (rule.originOk(origin) && method != null && rule.methodOk(method) && rule.headersOk(reqHeaders)) {
                resp.setHeader("Access-Control-Allow-Origin", rule.origins().contains("*") ? "*" : origin);
                resp.setHeader("Access-Control-Allow-Methods", method);
                if (reqHeaders != null) {
                    resp.setHeader("Access-Control-Allow-Headers", reqHeaders);
                }
                resp.setHeader("Access-Control-Max-Age", String.valueOf(rule.maxAge()));
                resp.setHeader("Access-Control-Allow-Credentials", "true");
                resp.setHeader("Vary", "Origin");
                resp.setStatus(200);
                return;
            }
        }
        throw new AzureException(403, "CorsPreflightFailure", "CORS not enabled or no matching rule found for this request.");
    }

    static void applyActual(AzReq r, HttpServletResponse resp, List<Rule> rules) {
        String origin = r.header("Origin");
        if (origin == null || rules.isEmpty()) {
            return;
        }
        for (Rule rule : rules) {
            if (rule.originOk(origin) && rule.methodOk(r.method)) {
                resp.setHeader("Access-Control-Allow-Origin", rule.origins().contains("*") ? "*" : origin);
                if (!rule.exposed().isEmpty()) {
                    resp.setHeader("Access-Control-Expose-Headers", String.join(",", rule.exposed()));
                }
                resp.setHeader("Access-Control-Allow-Credentials", "true");
                resp.setHeader("Vary", "Origin");
                return;
            }
        }
    }
}
