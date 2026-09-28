package com.sayonora.warp.azurewire;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Authentication for the three Azure Storage services: SharedKey and SharedKeyLite (HMAC-SHA256 over the
 * canonicalized string-to-sign, canonicalization exactly as Azure documents it, per service -- Table has its own
 * shorter rules), service and account SAS tokens (sv/ss/srt/sp/st/se/sip/spr/si/sig with permission, expiry, IP and
 * protocol checks) and anonymous access (the blob handler decides from the container's public access level).
 * Entra ID bearer tokens are not validated: only one static test token ({@code WARP_AZURE_BEARER_TOKEN}) is accepted.
 */
public final class AzureAuth {

    public enum Kind { KEY, SAS, BEARER, ANON }

    /** A stored access policy (signed identifier) of a container / queue / table. */
    public record Policy(String id, String start, String expiry, String permission) {
    }

    /** Outcome of authentication: what the caller may do. */
    public static final class Result {
        public final Kind kind;
        public final String account;
        final Sas sas;

        Result(Kind kind, String account, Sas sas) {
            this.kind = kind;
            this.account = account;
            this.sas = sas;
        }

        public boolean anonymous() {
            return kind == Kind.ANON;
        }

        /**
         * @param level 's' service, 'c' container/queue/table, 'o' object (blob / message / entity)
         * @param perms permission letters, ANY of which suffices (e.g. "cw")
         */
        public void authorize(char level, String perms) {
            if (kind == Kind.KEY || kind == Kind.BEARER) {
                return;
            }
            if (kind == Kind.ANON) {
                throw new AzureException(403, "AuthorizationFailure",
                        "This request is not authorized to perform this operation.");
            }
            if (!sas.satisfies(level, perms)) {
                throw new AzureException(403, "AuthorizationPermissionMismatch",
                        "This request is not authorized to perform this operation using this permission.");
            }
        }

        /** {@code tn}/{@code spk}.. limits of a table SAS (may be null). */
        public Sas sas() {
            return sas;
        }
    }

    public static final class Sas {
        public String sv, sp = "", se, st, spr, sip, sr, ss, srt, si, sig, tn, spk, srk, epk, erk;
        public boolean account;

        boolean satisfies(char level, String perms) {
            if (perms == null || perms.isEmpty()) {
                return true;
            }
            boolean any = false;
            for (char c : perms.toCharArray()) {
                if (sp.indexOf(c) >= 0) {
                    any = true;
                }
            }
            if (!any) {
                return false;
            }
            if (account) {
                return srt != null && srt.indexOf(level) >= 0;
            }
            return true;
        }
    }

    private final AzureConfig cfg;

    public AzureAuth(AzureConfig cfg) {
        this.cfg = cfg;
    }

    private static final DateTimeFormatter ISO_SECONDS = DateTimeFormatter.ISO_INSTANT;

    /**
     * @param policies looks up a stored access policy by signed identifier (null when unknown)
     * @param resource canonical SAS resource of the request: blob {@code container} or {@code container/blob}, queue
     *                 name, table name
     */
    public Result authenticate(AzReq r, Function<String, Policy> policies, String container, String blob) {
        String authz = r.header("Authorization");
        if (authz != null && !authz.isBlank()) {
            return sharedKey(r, authz.trim());
        }
        if (r.has("sig") && (r.has("sv") || r.has("se") || r.has("sp") || r.has("sr"))) {
            return sas(r, policies, container, blob);
        }
        return new Result(Kind.ANON, r.account, null);
    }

    // ---------------------------------------------------------------------------------------------- SharedKey

    private Result sharedKey(AzReq r, String authz) {
        if (authz.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String token = authz.substring(7).trim();
            if (cfg.bearerToken() != null && MessageDigest.isEqual(token.getBytes(StandardCharsets.UTF_8),
                    cfg.bearerToken().getBytes(StandardCharsets.UTF_8))) {
                return new Result(Kind.BEARER, r.account, null);
            }
            throw new AzureException(401, "InvalidAuthenticationInfo", "Server failed to authenticate the request. "
                    + "Please refer to the information in the www-authenticate header. (Warp accepts Entra ID bearer "
                    + "tokens only as the one static test token configured in WARP_AZURE_BEARER_TOKEN; no JWT "
                    + "validation is done.)").header("WWW-Authenticate", "Bearer authorization_uri=https://login.microsoftonline.com/"
                    + "common/oauth2/authorize resource_id=https://storage.azure.com");
        }
        boolean lite;
        String rest;
        if (authz.regionMatches(true, 0, "SharedKeyLite ", 0, 14)) {
            lite = true;
            rest = authz.substring(14).trim();
        } else if (authz.regionMatches(true, 0, "SharedKey ", 0, 10)) {
            lite = false;
            rest = authz.substring(10).trim();
        } else {
            throw authFailed(r, null, null, "Authorization header scheme must be SharedKey, SharedKeyLite or Bearer");
        }
        int colon = rest.indexOf(':');
        if (colon <= 0) {
            throw authFailed(r, null, null, null);
        }
        String acct = rest.substring(0, colon);
        String sig = rest.substring(colon + 1);
        AzureConfig.Account account = cfg.account(acct);
        if (account == null || !acct.equalsIgnoreCase(r.account)) {
            throw new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. Make sure the "
                    + "value of Authorization header is formed correctly including the signature."
                    + trailer(r)).extra("AuthenticationErrorDetail", "Authentication information is not given in the correct format. Check the value of Authorization header.");
        }
        List<String> candidates = stringsToSign(r, account.name(), lite);
        for (String sts : candidates) {
            if (MessageDigest.isEqual(sign(account.key(), sts).getBytes(StandardCharsets.US_ASCII),
                    sig.getBytes(StandardCharsets.US_ASCII))) {
                return new Result(Kind.KEY, r.account, null);
            }
        }
        throw authFailed(r, sig, candidates.get(0), null);
    }

    private static String trailer(AzReq r) {
        return "\nRequestId:" + r.requestId + "\nTime:" + DateTimeFormatter.ISO_INSTANT.format(Instant.now());
    }

    private AzureException authFailed(AzReq r, String sig, String sts, String why) {
        String msg = "Server failed to authenticate the request. Make sure the value of Authorization header is formed "
                + "correctly including the signature.";
        AzureException e = new AzureException(403, "AuthenticationFailed", msg + trailer(r));
        if (sts != null) {
            e.extra("AuthenticationErrorDetail", "The MAC signature found in the HTTP request '" + sig
                    + "' is not the same as any computed signature. Server used following string to sign: '" + sts + "'.");
        } else if (why != null) {
            e.extra("AuthenticationErrorDetail", why);
        }
        return e;
    }

    static String sign(byte[] key, String sts) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return Base64.getEncoder().encodeToString(mac.doFinal(sts.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String h(AzReq r, String name) {
        String v = r.header(name);
        return v == null ? "" : v;
    }

    /** Candidate strings to sign (encoded / decoded canonical path variants); the first is the reported one. */
    static List<String> stringsToSign(AzReq r, String account, boolean lite) {
        List<String> out = new ArrayList<>();
        String encoded = r.rawPath;
        String decoded = r.hostStyle ? r.path : "/" + r.account + r.path;
        if (r.hostStyle && r.path.isEmpty()) {
            decoded = "/";
        }
        for (String p : new String[] {encoded, decoded}) {
            for (String date : dates(r)) {
                String s = build(r, account, p, lite, date);
                if (!out.contains(s)) {
                    out.add(s);
                }
            }
        }
        return out;
    }

    private static List<String> dates(AzReq r) {
        String xd = h(r, "x-ms-date");
        String d = h(r, "Date");
        if (AzReq.TABLE.equals(r.service)) {
            List<String> l = new ArrayList<>();
            l.add(xd.isEmpty() ? d : xd);
            if (!xd.isEmpty()) {
                l.add(d);
                l.add("");
            }
            return l;
        }
        return List.of(xd.isEmpty() ? d : "");
    }

    private static String build(AzReq r, String account, String urlPath, boolean lite, String date) {
        String verb = r.method;
        if (AzReq.TABLE.equals(r.service)) {
            String res = "/" + account + urlPath + (lite || true ? compOnly(r) : "");
            if (lite) {
                return date + "\n" + res;
            }
            return verb + "\n" + h(r, "Content-MD5") + "\n" + h(r, "Content-Type") + "\n" + date + "\n" + res;
        }
        String headers = canonicalHeaders(r);
        if (lite) {
            return verb + "\n" + h(r, "Content-MD5") + "\n" + h(r, "Content-Type") + "\n" + date + "\n" + headers
                    + "/" + account + urlPath + compOnly(r);
        }
        String clen = h(r, "Content-Length");
        if ("0".equals(clen)) {
            clen = "";
        }
        return verb + "\n" + h(r, "Content-Encoding") + "\n" + h(r, "Content-Language") + "\n" + clen + "\n"
                + h(r, "Content-MD5") + "\n" + h(r, "Content-Type") + "\n" + date + "\n"
                + h(r, "If-Modified-Since") + "\n" + h(r, "If-Match") + "\n" + h(r, "If-None-Match") + "\n"
                + h(r, "If-Unmodified-Since") + "\n" + h(r, "Range") + "\n" + headers
                + canonicalResource(r, account, urlPath);
    }

    private static String compOnly(AzReq r) {
        String c = r.q("comp");
        return c == null ? "" : "?comp=" + c;
    }

    private static String canonicalHeaders(AzReq r) {
        TreeMap<String, String> m = new TreeMap<>();
        Enumeration<String> names = r.raw.getHeaderNames();
        for (String n : Collections.list(names)) {
            String ln = n.toLowerCase(Locale.ROOT);
            if (!ln.startsWith("x-ms-")) {
                continue;
            }
            List<String> vals = new ArrayList<>();
            for (String v : Collections.list(r.raw.getHeaders(n))) {
                vals.add(String.join(" ", v.trim().split("\\s+")));
            }
            m.merge(ln, String.join(",", vals), (a, b) -> a + "," + b);
        }
        StringBuilder sb = new StringBuilder();
        m.forEach((k, v) -> sb.append(k).append(':').append(v).append('\n'));
        return sb.toString();
    }

    private static String canonicalResource(AzReq r, String account, String urlPath) {
        StringBuilder sb = new StringBuilder("/").append(account).append(urlPath);
        TreeMap<String, List<String>> params = new TreeMap<>();
        r.query.forEach((k, v) -> params.computeIfAbsent(k.toLowerCase(Locale.ROOT), x -> new ArrayList<>()).addAll(v));
        params.forEach((k, v) -> {
            List<String> vs = new ArrayList<>(v);
            Collections.sort(vs);
            sb.append('\n').append(k).append(':').append(String.join(",", vs));
        });
        return sb.toString();
    }

    // ---------------------------------------------------------------------------------------------- SAS

    private Result sas(AzReq r, Function<String, Policy> policies, String container, String blob) {
        AzureConfig.Account account = cfg.account(r.account);
        if (account == null) {
            throw new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. Make sure "
                    + "the value of Authorization header is formed correctly including the signature." + trailer(r));
        }
        Sas s = new Sas();
        s.sv = r.q("sv");
        s.sp = r.q("sp") == null ? "" : r.q("sp");
        s.se = r.q("se");
        s.st = r.q("st");
        s.spr = r.q("spr");
        s.sip = r.q("sip");
        s.sr = r.q("sr");
        s.ss = r.q("ss");
        s.srt = r.q("srt");
        s.si = r.q("si");
        s.sig = r.q("sig");
        s.tn = r.q("tn");
        s.spk = r.q("spk");
        s.srk = r.q("srk");
        s.epk = r.q("epk");
        s.erk = r.q("erk");
        s.account = s.ss != null && s.srt != null;
        if (s.sv == null) {
            throw new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. Make sure the "
                    + "value of Authorization header is formed correctly including the signature." + trailer(r))
                    .extra("AuthenticationErrorDetail", "Signed version is required for SAS");
        }
        String sts;
        String startV = s.st == null ? "" : s.st;
        String expiryV = s.se == null ? "" : s.se;
        String perms = s.sp;
        if (s.account) {
            String enc = s.sv.compareTo("2020-12-06") >= 0 ? "\n" : "";
            sts = account.name() + "\n" + perms + "\n" + s.ss + "\n" + s.srt + "\n" + startV + "\n" + expiryV + "\n"
                    + nz(s.sip) + "\n" + nz(s.spr) + "\n" + s.sv + "\n" + (enc.isEmpty() ? "" : "\n");
            if (s.sv.compareTo("2020-12-06") < 0) {
                sts = account.name() + "\n" + perms + "\n" + s.ss + "\n" + s.srt + "\n" + startV + "\n" + expiryV + "\n"
                        + nz(s.sip) + "\n" + nz(s.spr) + "\n" + s.sv + "\n";
            }
            if (!s.ss.contains(r.service.substring(0, 1))) {
                throw new AzureException(403, "AuthorizationServiceMismatch",
                        "This request is not authorized to perform this operation using this service.");
            }
        } else {
            Policy pol = null;
            if (s.si != null) {
                pol = policies == null ? null : policies.apply(s.si);
                if (pol == null) {
                    throw new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. "
                            + "Make sure the value of Authorization header is formed correctly including the signature."
                            + trailer(r)).extra("AuthenticationErrorDetail", "Signed identifier is not found on the container's access policies.");
                }
                if (s.sp.isEmpty()) {
                    perms = "";
                    s.sp = pol.permission() == null ? "" : pol.permission();
                }
                if (s.se == null && pol.expiry() != null) {
                    s.se = pol.expiry();
                }
                if (s.st == null && pol.start() != null) {
                    s.st = pol.start();
                }
            }
            String canon;
            switch (r.service) {
                case AzReq.BLOB -> {
                    if (container == null) {
                        throw new AzureException(403, "AuthorizationFailure",
                                "This request is not authorized to perform this operation.");
                    }
                    if ("c".equals(s.sr) || "b".equals(s.sr) && blob == null) {
                        if ("b".equals(s.sr)) {
                            throw new AzureException(403, "AuthorizationFailure",
                                    "This request is not authorized to perform this operation.");
                        }
                        canon = "/blob/" + account.name() + "/" + container;
                    } else if ("b".equals(s.sr) || "bs".equals(s.sr)) {
                        canon = "/blob/" + account.name() + "/" + container + "/" + blob;
                    } else {
                        canon = "/blob/" + account.name() + "/" + container;
                    }
                    String snapshot = "bs".equals(s.sr) ? nz(r.q("snapshot")) : "";
                    boolean enc = s.sv.compareTo("2020-12-06") >= 0;
                    sts = String.join("\n", s.sp, nz(s.st), nz(s.se), canon, nz(s.si), nz(s.sip), nz(s.spr), s.sv,
                            nz(s.sr), snapshot, enc ? "" : null, nz(r.q("rscc")), nz(r.q("rscd")), nz(r.q("rsce")),
                            nz(r.q("rscl")), nz(r.q("rsct")));
                    if (!enc) {
                        sts = String.join("\n", s.sp, nz(s.st), nz(s.se), canon, nz(s.si), nz(s.sip), nz(s.spr), s.sv,
                                nz(s.sr), snapshot, nz(r.q("rscc")), nz(r.q("rscd")), nz(r.q("rsce")),
                                nz(r.q("rscl")), nz(r.q("rsct")));
                    }
                }
                case AzReq.QUEUE -> {
                    canon = "/queue/" + account.name() + "/" + (container == null ? "" : container);
                    sts = String.join("\n", s.sp, nz(s.st), nz(s.se), canon, nz(s.si), nz(s.sip), nz(s.spr), s.sv);
                }
                default -> {
                    canon = "/table/" + account.name() + "/" + (container == null ? nz(s.tn) : container);
                    sts = String.join("\n", s.sp, nz(s.st), nz(s.se), canon, nz(s.si), nz(s.sip), nz(s.spr), s.sv,
                            nz(s.spk), nz(s.srk), nz(s.epk), nz(s.erk));
                    if (s.tn != null && container != null && !s.tn.equalsIgnoreCase(container)) {
                        throw new AzureException(403, "AuthorizationFailure",
                                "This request is not authorized to perform this operation.");
                    }
                }
            }
            if (s.si != null && pol != null) {
                // when the policy supplies the fields the URL omitted, the string to sign carries the URL's own values
                sts = sts;
            }
        }
        String expected = sign(account.key(), sts);
        if (!MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII),
                nz(s.sig).getBytes(StandardCharsets.US_ASCII))) {
            throw new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. Make sure the "
                    + "value of Authorization header is formed correctly including the signature." + trailer(r))
                    .extra("AuthenticationErrorDetail", "Signature did not match. String to sign used was " + sts);
        }
        // validity window
        Instant now = Instant.now();
        Instant st = parseTime(s.st);
        Instant se = parseTime(s.se);
        if (se == null) {
            throw new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. Make sure "
                    + "the value of Authorization header is formed correctly including the signature." + trailer(r))
                    .extra("AuthenticationErrorDetail", "Signed expiry time is required");
        }
        if (now.isAfter(se) || st != null && now.isBefore(st)) {
            throw new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. Make sure "
                    + "the value of Authorization header is formed correctly including the signature." + trailer(r))
                    .extra("AuthenticationErrorDetail", "Signature not valid in the specified time frame: Start ["
                            + (s.st == null ? "" : s.st) + "] - Expiry [" + s.se + "] - Current ["
                            + ISO_SECONDS.format(now.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)) + "]");
        }
        if (s.spr != null && "https".equalsIgnoreCase(s.spr) && !"https".equalsIgnoreCase(r.raw.getScheme())) {
            throw new AzureException(403, "AuthorizationProtocolMismatch",
                    "This request is not authorized to perform this operation using this protocol.");
        }
        if (s.sip != null && !ipAllowed(s.sip, r.raw.getRemoteAddr())) {
            throw new AzureException(403, "AuthorizationSourceIPMismatch",
                    "This request is not authorized to perform this operation using this source IP address.");
        }
        return new Result(Kind.SAS, r.account, s);
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    static Instant parseTime(String v) {
        if (v == null || v.isEmpty()) {
            return null;
        }
        try {
            if (v.length() == 10) {
                return java.time.LocalDate.parse(v).atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
            }
            return java.time.OffsetDateTime.parse(v).toInstant();
        } catch (DateTimeParseException e) {
            try {
                return java.time.LocalDateTime.parse(v).toInstant(java.time.ZoneOffset.UTC);
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
    }

    static boolean ipAllowed(String spec, String remote) {
        int dash = spec.indexOf('-');
        try {
            if (dash < 0) {
                return spec.equals(remote);
            }
            long lo = ipv4(spec.substring(0, dash));
            long hi = ipv4(spec.substring(dash + 1));
            long ip = ipv4(remote);
            return ip >= lo && ip <= hi;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static long ipv4(String s) {
        String[] p = s.trim().split("\\.");
        if (p.length != 4) {
            throw new IllegalArgumentException();
        }
        long v = 0;
        for (String x : p) {
            v = v << 8 | Integer.parseInt(x);
        }
        return v;
    }

    /** Test seam: the {@code Map} form of {@link #stringsToSign} is exercised by unit tests through this helper. */
    public static String signForTest(byte[] key, String sts) {
        return sign(key, sts);
    }

    /** The unit-test entry for SAS string composition of an account SAS. */
    public static String accountSasStringToSign(String account, String sp, String ss, String srt, String st, String se,
            String sip, String spr, String sv) {
        return account + "\n" + sp + "\n" + ss + "\n" + srt + "\n" + nz(st) + "\n" + nz(se) + "\n" + nz(sip) + "\n"
                + nz(spr) + "\n" + sv + "\n" + (sv.compareTo("2020-12-06") >= 0 ? "\n" : "");
    }

    static Map<String, String> unused() {
        return Map.of();
    }
}
