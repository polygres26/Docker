package com.sayonora.wire.s3wire;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.w3c.dom.Element;

/**
 * Parsing, validation and rendering of the small XML/header documents S3 uses for subresources: tagging,
 * versioning, ACLs, public access block, CORS, encryption, object lock. Pure functions (unit tested).
 */
final class S3Cfg {
    static final String OWNER_ID = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";
    static final String OWNER_NAME = "warp-s3wire";
    static final String ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers";
    static final String AUTH_USERS = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
    static final String LOG_DELIVERY = "http://acs.amazonaws.com/groups/s3/LogDelivery";
    private static final String XSI = "http://www.w3.org/2001/XMLSchema-instance";

    private S3Cfg() {
    }

    static S3WireException malformed() {
        return new S3WireException(400, "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.");
    }

    private static Element root(byte[] body, String expected) {
        if (body == null || body.length == 0) {
            throw malformed();
        }
        Element r = S3Xml.parse(body);
        if (expected != null && !expected.equals(S3Xml.name(r))) {
            throw malformed();
        }
        return r;
    }

    // ---- encryption -------------------------------------------------------------------------------

    /** {algorithm, kmsKeyId} of the default rule; AES256 when none. */
    static String[] encryptionDefaults(String xml) {
        if (xml == null) {
            return new String[] {"AES256", null};
        }
        try {
            Element r = S3Xml.parse(xml.getBytes(StandardCharsets.UTF_8));
            for (Element rule : S3Xml.children(r, "Rule")) {
                for (Element d : S3Xml.children(rule, "ApplyServerSideEncryptionByDefault")) {
                    String alg = S3Xml.text(d, "SSEAlgorithm");
                    return new String[] {alg == null ? "AES256" : alg.trim(), S3Xml.text(d, "KMSMasterKeyID")};
                }
            }
        } catch (RuntimeException e) {
            // fall through to the default
        }
        return new String[] {"AES256", null};
    }

    static void validateEncryption(byte[] body) {
        Element r = root(body, "ServerSideEncryptionConfiguration");
        List<Element> rules = S3Xml.children(r, "Rule");
        if (rules.isEmpty()) {
            throw malformed();
        }
        for (Element rule : rules) {
            List<Element> d = S3Xml.children(rule, "ApplyServerSideEncryptionByDefault");
            if (d.isEmpty()) {
                continue;
            }
            String alg = S3Xml.text(d.get(0), "SSEAlgorithm");
            if (alg == null || !List.of("AES256", "aws:kms", "aws:kms:dsse").contains(alg.trim())) {
                throw malformed();
            }
        }
    }

    // ---- tagging ----------------------------------------------------------------------------------

    /** Validates one tag per the S3 rules and returns nothing; throws InvalidTag. */
    private static void checkTag(String k, String v) {
        if (k == null || k.isEmpty() || k.codePointCount(0, k.length()) > 128) {
            throw new S3WireException(400, "InvalidTag",
                    "The TagKey you have provided is invalid");
        }
        if (v == null || v.codePointCount(0, v.length()) > 256) {
            throw new S3WireException(400, "InvalidTag", "The TagValue you have provided is invalid");
        }
        if (k.toLowerCase(Locale.ROOT).startsWith("aws:")) {
            throw new S3WireException(400, "InvalidTag", "Your TagKey cannot be prefixed with aws:");
        }
    }

    static LinkedHashMap<String, String> parseTaggingXml(byte[] body, int max) {
        Element r = root(body, "Tagging");
        List<Element> sets = S3Xml.children(r, "TagSet");
        if (sets.isEmpty()) {
            throw malformed();
        }
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        for (Element t : S3Xml.children(sets.get(0), "Tag")) {
            String k = S3Xml.text(t, "Key");
            String v = S3Xml.text(t, "Value");
            if (k == null || v == null) {
                throw malformed();
            }
            checkTag(k, v);
            if (out.put(k, v) != null) {
                throw new S3WireException(400, "InvalidTag", "Cannot provide multiple Tags with the same key");
            }
        }
        if (out.size() > max) {
            throw new S3WireException(400, "BadRequest", max == 10 ? "Object tags cannot be greater than 10"
                    : "Bucket tags cannot be greater than 50");
        }
        return out;
    }

    /** {@code x-amz-tagging}: a URL query string of key=value pairs. */
    static LinkedHashMap<String, String> parseTagHeader(String header) {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        if (header == null || header.isEmpty()) {
            return out;
        }
        if (header.length() > 8 * 1024) {
            throw new S3WireException(400, "InvalidArgument", "The tagging header is too large.");
        }
        List<String[]> pairs = new ArrayList<>();
        for (String part : header.split("&", -1)) {
            int eq = part.indexOf('=');
            if (eq < 0) {
                throw new S3WireException(400, "InvalidArgument", "The header 'x-amz-tagging' shall be encoded as UTF-8 "
                        + "then URLEncoded URL query parameters without tag name duplicates.");
            }
            String k = URLDecoder.decode(part.substring(0, eq), StandardCharsets.UTF_8);
            String v = URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
            if (k.isEmpty()) {
                throw new S3WireException(400, "InvalidArgument", "The tag key cannot be empty.");
            }
            pairs.add(new String[] {k, v});
        }
        for (String[] p : pairs) {
            if (out.put(p[0], p[1]) != null) {
                throw new S3WireException(400, "InvalidArgument", "The header 'x-amz-tagging' shall be encoded as UTF-8 "
                        + "then URLEncoded URL query parameters without tag name duplicates.");
            }
        }
        if (out.size() > 10) {
            throw new S3WireException(400, "BadRequest", "Object tags cannot be greater than 10");
        }
        for (Map.Entry<String, String> e : out.entrySet()) {
            checkTag(e.getKey(), e.getValue());
        }
        return out;
    }

    static String taggingXml(Map<String, String> tags) {
        StringBuilder sb = new StringBuilder(S3Xml.DECL + "<Tagging xmlns=\"" + S3Xml.NS + "\"><TagSet>");
        for (Map.Entry<String, String> e : tags.entrySet()) {
            sb.append("<Tag>");
            S3Xml.tag(sb, "Key", e.getKey());
            S3Xml.tag(sb, "Value", e.getValue());
            sb.append("</Tag>");
        }
        return sb.append("</TagSet></Tagging>").toString();
    }

    // ---- versioning -------------------------------------------------------------------------------

    /** @return Enabled or Suspended */
    static String parseVersioning(byte[] body) {
        Element r = root(body, "VersioningConfiguration");
        String status = S3Xml.text(r, "Status");
        if (status == null || !List.of("Enabled", "Suspended").contains(status.trim())) {
            throw malformed();
        }
        return status.trim();
    }

    static String versioningXml(String status, boolean mfa) {
        return S3Xml.DECL + "<VersioningConfiguration xmlns=\"" + S3Xml.NS + "\">"
                + (status == null ? "" : "<Status>" + status + "</Status>") + "</VersioningConfiguration>";
    }

    // ---- ACL --------------------------------------------------------------------------------------

    record Grant(String type, String id, String uri, String email, String permission) {
    }

    private static Grant owner(String perm) {
        return new Grant("CanonicalUser", OWNER_ID, null, null, perm);
    }

    private static Grant group(String uri, String perm) {
        return new Grant("Group", null, uri, null, perm);
    }

    static List<Grant> cannedGrants(String canned) {
        List<Grant> g = new ArrayList<>();
        g.add(owner("FULL_CONTROL"));
        switch (canned == null ? "private" : canned) {
            case "private", "bucket-owner-read", "bucket-owner-full-control", "aws-exec-read" -> {
            }
            case "public-read" -> g.add(group(ALL_USERS, "READ"));
            case "public-read-write" -> {
                g.add(group(ALL_USERS, "READ"));
                g.add(group(ALL_USERS, "WRITE"));
            }
            case "authenticated-read" -> g.add(group(AUTH_USERS, "READ"));
            case "log-delivery-write" -> {
                g.add(group(LOG_DELIVERY, "WRITE"));
                g.add(group(LOG_DELIVERY, "READ_ACP"));
            }
            default -> throw new S3WireException(400, "InvalidArgument", "Invalid canned ACL: " + canned);
        }
        return g;
    }

    static String aclXml(List<Grant> grants) {
        StringBuilder sb = new StringBuilder(S3Xml.DECL + "<AccessControlPolicy xmlns=\"" + S3Xml.NS + "\"><Owner>");
        S3Xml.tag(sb, "ID", OWNER_ID);
        S3Xml.tag(sb, "DisplayName", OWNER_NAME);
        sb.append("</Owner><AccessControlList>");
        for (Grant g : grants) {
            sb.append("<Grant><Grantee xmlns:xsi=\"" + XSI + "\" xsi:type=\"").append(g.type()).append("\">");
            if ("CanonicalUser".equals(g.type())) {
                S3Xml.tag(sb, "ID", g.id());
                S3Xml.tag(sb, "DisplayName", g.id().equals(OWNER_ID) ? OWNER_NAME : g.id());
            } else if ("Group".equals(g.type())) {
                S3Xml.tag(sb, "URI", g.uri());
            } else {
                S3Xml.tag(sb, "EmailAddress", g.email());
            }
            sb.append("</Grantee>");
            S3Xml.tag(sb, "Permission", g.permission());
            sb.append("</Grant>");
        }
        return sb.append("</AccessControlList></AccessControlPolicy>").toString();
    }

    static String defaultAclXml() {
        return aclXml(cannedGrants("private"));
    }

    private static final Map<String, String> GRANT_HEADERS = Map.of("x-amz-grant-read", "READ",
            "x-amz-grant-write", "WRITE", "x-amz-grant-read-acp", "READ_ACP", "x-amz-grant-write-acp", "WRITE_ACP",
            "x-amz-grant-full-control", "FULL_CONTROL");

    /**
     * ACL document from a request: the {@code x-amz-acl} canned header, {@code x-amz-grant-*} headers, or an
     * XML body (exactly one style; mixing is InvalidRequest). Returns null when the request carries none.
     */
    static String aclFromRequest(Function<String, String> header, byte[] body) {
        String canned = header.apply("x-amz-acl");
        List<Grant> grants = new ArrayList<>();
        boolean grantHeaders = false;
        for (Map.Entry<String, String> h : GRANT_HEADERS.entrySet()) {
            String v = header.apply(h.getKey());
            if (v == null) {
                continue;
            }
            grantHeaders = true;
            for (String part : v.split(",")) {
                String p = part.trim();
                int eq = p.indexOf('=');
                if (eq < 0) {
                    throw new S3WireException(400, "InvalidArgument", "Invalid grant header value: " + v);
                }
                String kind = p.substring(0, eq).trim().toLowerCase(Locale.ROOT);
                String val = p.substring(eq + 1).trim().replace("\"", "");
                switch (kind) {
                    case "id" -> grants.add(new Grant("CanonicalUser", val, null, null, h.getValue()));
                    case "uri" -> grants.add(new Grant("Group", null, val, null, h.getValue()));
                    case "emailaddress" -> grants.add(new Grant("AmazonCustomerByEmail", null, null, val, h.getValue()));
                    default -> throw new S3WireException(400, "InvalidArgument", "Invalid grant header value: " + v);
                }
            }
        }
        boolean hasBody = body != null && body.length > 0;
        if ((canned != null ? 1 : 0) + (grantHeaders ? 1 : 0) + (hasBody ? 1 : 0) > 1) {
            throw new S3WireException(400, "InvalidRequest", "Specifying both Canned ACLs and Header Grants is not allowed");
        }
        if (canned != null) {
            return aclXml(cannedGrants(canned));
        }
        if (grantHeaders) {
            grants.add(0, owner("FULL_CONTROL"));
            return aclXml(grants);
        }
        if (hasBody) {
            Element r = root(body, "AccessControlPolicy");
            List<Grant> out = new ArrayList<>();
            List<Element> lists = S3Xml.children(r, "AccessControlList");
            if (lists.isEmpty()) {
                throw malformed();
            }
            for (Element g : S3Xml.children(lists.get(0), "Grant")) {
                List<Element> ge = S3Xml.children(g, "Grantee");
                String perm = S3Xml.text(g, "Permission");
                if (ge.isEmpty() || perm == null || !List.of("FULL_CONTROL", "READ", "WRITE", "READ_ACP", "WRITE_ACP")
                        .contains(perm.trim())) {
                    throw malformed();
                }
                String type = ge.get(0).getAttributeNS(XSI, "type");
                if (type == null || type.isEmpty()) {
                    type = ge.get(0).getAttribute("xsi:type");
                }
                String uri = S3Xml.text(ge.get(0), "URI");
                String id = S3Xml.text(ge.get(0), "ID");
                String email = S3Xml.text(ge.get(0), "EmailAddress");
                if (type == null || type.isEmpty()) {
                    type = uri != null ? "Group" : id != null ? "CanonicalUser" : "AmazonCustomerByEmail";
                }
                out.add(new Grant(type, id, uri, email, perm.trim()));
            }
            return aclXml(out);
        }
        return null;
    }

    /** True when the ACL grants any permission to AllUsers / AuthenticatedUsers. */
    static boolean aclIsPublic(String aclXml) {
        return aclXml != null && (aclXml.contains(ALL_USERS) || aclXml.contains(AUTH_USERS));
    }

    // ---- public access block ---------------------------------------------------------------------

    static String validatePublicAccessBlock(byte[] body) {
        Element r = root(body, "PublicAccessBlockConfiguration");
        StringBuilder sb = new StringBuilder(S3Xml.DECL + "<PublicAccessBlockConfiguration xmlns=\"" + S3Xml.NS + "\">");
        for (String k : List.of("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")) {
            String v = S3Xml.text(r, k);
            if (v != null && !v.trim().equals("true") && !v.trim().equals("false")) {
                throw malformed();
            }
            S3Xml.tag(sb, k, v == null ? "false" : v.trim());
        }
        return sb.append("</PublicAccessBlockConfiguration>").toString();
    }

    static boolean pabFlag(String xml, String flag) {
        return xml != null && xml.contains("<" + flag + ">true</" + flag + ">");
    }

    // ---- ownership controls ----------------------------------------------------------------------

    static String validateOwnership(byte[] body) {
        Element r = root(body, "OwnershipControls");
        List<Element> rules = S3Xml.children(r, "Rule");
        if (rules.size() != 1) {
            throw malformed();
        }
        String o = S3Xml.text(rules.get(0), "ObjectOwnership");
        if (o == null || !List.of("BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced").contains(o.trim())) {
            throw malformed();
        }
        return o.trim();
    }

    // ---- CORS -------------------------------------------------------------------------------------

    record CorsRule(String id, List<String> origins, List<String> methods, List<String> headers, List<String> expose,
            Integer maxAge) {
    }

    static List<CorsRule> parseCors(byte[] body) {
        Element r = root(body, "CORSConfiguration");
        List<CorsRule> rules = new ArrayList<>();
        for (Element e : S3Xml.children(r, "CORSRule")) {
            List<String> origins = texts(e, "AllowedOrigin");
            List<String> methods = texts(e, "AllowedMethod");
            if (origins.isEmpty() || methods.isEmpty()) {
                throw malformed();
            }
            for (String m : methods) {
                if (!List.of("GET", "PUT", "POST", "DELETE", "HEAD").contains(m)) {
                    throw new S3WireException(400, "InvalidRequest",
                            "Found unsupported HTTP method in CORS config. Unsupported method is " + m);
                }
            }
            for (String o : origins) {
                if (o.chars().filter(c -> c == '*').count() > 1) {
                    throw new S3WireException(400, "InvalidRequest",
                            "AllowedOrigin \"" + o + "\" can not have more than one wildcard.");
                }
            }
            String age = S3Xml.text(e, "MaxAgeSeconds");
            Integer maxAge = null;
            if (age != null) {
                try {
                    maxAge = Integer.parseInt(age.trim());
                } catch (NumberFormatException ex) {
                    throw malformed();
                }
            }
            rules.add(new CorsRule(S3Xml.text(e, "ID"), origins, methods, texts(e, "AllowedHeader"),
                    texts(e, "ExposeHeader"), maxAge));
        }
        if (rules.isEmpty() || rules.size() > 100) {
            throw malformed();
        }
        return rules;
    }

    private static List<String> texts(Element e, String name) {
        List<String> out = new ArrayList<>();
        for (Element c : S3Xml.children(e, name)) {
            out.add(c.getTextContent().trim());
        }
        return out;
    }

    /** Single-wildcard match ({@code http://*.example.com}), case-sensitive for origins. */
    static boolean wildcardMatch(String pattern, String value, boolean ignoreCase) {
        String p = ignoreCase ? pattern.toLowerCase(Locale.ROOT) : pattern;
        String v = ignoreCase ? value.toLowerCase(Locale.ROOT) : value;
        int star = p.indexOf('*');
        if (star < 0) {
            return p.equals(v);
        }
        return v.length() >= p.length() - 1 && v.startsWith(p.substring(0, star)) && v.endsWith(p.substring(star + 1));
    }

    /**
     * The first rule that allows {@code origin} and {@code method}; for a preflight also every header in
     * {@code requestHeaders} (comma list) must match an AllowedHeader.
     */
    static CorsRule matchCors(List<CorsRule> rules, String origin, String method, String requestHeaders) {
        for (CorsRule r : rules) {
            boolean o = r.origins().stream().anyMatch(p -> wildcardMatch(p, origin, false));
            boolean m = r.methods().contains(method);
            if (!o || !m) {
                continue;
            }
            boolean h = true;
            if (requestHeaders != null && !requestHeaders.isBlank()) {
                for (String rh : requestHeaders.split(",")) {
                    String hdr = rh.trim();
                    if (!hdr.isEmpty() && r.headers().stream().noneMatch(p -> wildcardMatch(p, hdr, true))) {
                        h = false;
                    }
                }
            }
            if (h) {
                return r;
            }
        }
        return null;
    }

    // ---- lifecycle / website / replication / notification: structural validation only ---------------

    static void validateLifecycle(byte[] body) {
        Element r = root(body, "LifecycleConfiguration");
        List<Element> rules = S3Xml.children(r, "Rule");
        if (rules.isEmpty() || rules.size() > 1000) {
            throw malformed();
        }
        java.util.Set<String> ids = new java.util.HashSet<>();
        for (Element rule : rules) {
            String st = S3Xml.text(rule, "Status");
            if (st == null || !List.of("Enabled", "Disabled").contains(st.trim())) {
                throw malformed();
            }
            String id = S3Xml.text(rule, "ID");
            if (id != null && !ids.add(id)) {
                throw new S3WireException(400, "InvalidArgument", "Rule ID must be unique. Found same ID for more than one rule.");
            }
        }
    }

    static void validateRoot(byte[] body, String expectedRoot) {
        root(body, expectedRoot);
    }

    // ---- object lock -------------------------------------------------------------------------------

    /** {mode, days, years} of a default retention rule or nulls; validates the document. */
    static String[] parseObjectLockConfig(byte[] body) {
        Element r = root(body, "ObjectLockConfiguration");
        String enabled = S3Xml.text(r, "ObjectLockEnabled");
        if (enabled != null && !enabled.trim().equals("Enabled")) {
            throw malformed();
        }
        List<Element> rule = S3Xml.children(r, "Rule");
        if (rule.isEmpty()) {
            return new String[] {null, null, null};
        }
        List<Element> dr = S3Xml.children(rule.get(0), "DefaultRetention");
        if (dr.isEmpty()) {
            throw malformed();
        }
        String mode = S3Xml.text(dr.get(0), "Mode");
        String days = S3Xml.text(dr.get(0), "Days");
        String years = S3Xml.text(dr.get(0), "Years");
        if (mode == null || !List.of("GOVERNANCE", "COMPLIANCE").contains(mode.trim()) || (days == null) == (years == null)) {
            throw malformed();
        }
        return new String[] {mode.trim(), days == null ? null : days.trim(), years == null ? null : years.trim()};
    }

    static Instant defaultRetentionUntil(String[] cfg, Instant now) {
        if (cfg[0] == null) {
            return null;
        }
        if (cfg[1] != null) {
            return now.plus(java.time.Duration.ofDays(Long.parseLong(cfg[1])));
        }
        return now.atZone(java.time.ZoneOffset.UTC).plusYears(Long.parseLong(cfg[2])).toInstant();
    }

    /** {mode, until} of a Retention document. */
    static String[] parseRetention(byte[] body) {
        Element r = root(body, "Retention");
        String mode = S3Xml.text(r, "Mode");
        String until = S3Xml.text(r, "RetainUntilDate");
        if (mode == null || until == null || !List.of("GOVERNANCE", "COMPLIANCE").contains(mode.trim())) {
            throw malformed();
        }
        try {
            return new String[] {mode.trim(), Instant.parse(until.trim()).toString()};
        } catch (RuntimeException e) {
            throw malformed();
        }
    }

    static String parseLegalHold(byte[] body) {
        Element r = root(body, "LegalHold");
        String st = S3Xml.text(r, "Status");
        if (st == null || !List.of("ON", "OFF").contains(st.trim())) {
            throw malformed();
        }
        return st.trim();
    }
}
