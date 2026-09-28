package com.sayonora.warp.ab;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The A/B routing policy of ONE store (s3, dynamodb, sqs, ...): which requests go to Warp's own
 * Postgres-backed emulation ("local"), which to the real cloud service ("cloud"), which to BOTH
 * for comparison. Pure data plus the pure decision function {@link #decide}, so it is unit-testable
 * without any server. See docs/WARP_GUIDE.md "A/B routing".
 *
 * <p>Decision order: kill switch (state level, handled by the caller) -> first matching rule -> mode.
 * Split-family routing is sticky: the client key (access key id, source IP or a header value, see
 * {@code stickyBy}) is hashed with the policy seed into a bucket 0..9999 and a client whose bucket is
 * below {@code cloudPercent * 100} is a cloud client -- the same client always lands on the same side
 * until the percentage changes. Writes in a split-family mode go to exactly ONE side, the
 * {@code writeOwner}, unless the matching rule pins its writes or {@code dualWrite} is enabled.
 */
public final class AbPolicy {

    public enum Mode { LOCAL, CLOUD, SPLIT, COMPARE }

    /** Where one request goes. */
    public enum Route {
        LOCAL, CLOUD,
        /** Reads only: both sides, the compare primary's answer is returned. */
        COMPARE,
        /** Writes only: the write owner answers, the other side gets the same write afterwards. */
        DUAL_WRITE
    }

    /** What decide() sees of a request; implemented by the HTTP layer and by tests. */
    public interface Ctx {
        String accessKey();

        String ip();

        String header(String name);

        boolean read();
    }

    public record Rule(String name, String accessKey, String ip, String header, String headerValue, AbSide route,
            boolean compare, boolean pinWrites) {
        boolean matches(Ctx c) {
            boolean any = false;
            if (accessKey != null) {
                any = true;
                if (c.accessKey() == null || !glob(accessKey, c.accessKey())) {
                    return false;
                }
            }
            if (ip != null) {
                any = true;
                if (c.ip() == null || !ipMatches(ip, c.ip())) {
                    return false;
                }
            }
            if (header != null) {
                any = true;
                String v = c.header(header);
                if (v == null || (headerValue != null && !glob(headerValue, v))) {
                    return false;
                }
            }
            return any;
        }
    }

    public final String store;
    public final Mode mode;
    public final String target;
    public final double cloudPercent;
    public final String stickyBy;
    public final String seed;
    public final List<Rule> rules;
    public final AbSide comparePrimary;
    public final Set<String> ignoreKeys;
    public final boolean recordValues;
    public final long compareMaxBodyBytes;
    public final int bufferSize;
    public final AbSide writeOwner;
    public final boolean dualWrite;
    public final long dualWriteMaxBytes;
    public final Map<String, String> roleOverrides;
    public final boolean trustXForwardedFor;

    private AbPolicy(JsonObject o, String store) {
        this.store = store;
        this.mode = Mode.valueOf(str(o, "mode", "local").toUpperCase(Locale.ROOT));
        this.target = str(o, "target", null);
        this.cloudPercent = o.has("cloudPercent") && !o.get("cloudPercent").isJsonNull() ? o.get("cloudPercent").getAsDouble() : 0;
        if (cloudPercent < 0 || cloudPercent > 100) {
            throw new IllegalArgumentException("cloudPercent must be between 0 and 100");
        }
        this.stickyBy = str(o, "stickyBy", "accessKey");
        if (!(stickyBy.equals("accessKey") || stickyBy.equals("ip") || stickyBy.startsWith("header:"))) {
            throw new IllegalArgumentException("stickyBy must be accessKey, ip or header:<Name>");
        }
        this.seed = str(o, "seed", store);
        List<Rule> rs = new ArrayList<>();
        if (o.has("rules") && o.get("rules").isJsonArray()) {
            for (JsonElement e : o.getAsJsonArray("rules")) {
                JsonObject r = e.getAsJsonObject();
                String route = str(r, "route", "local").toLowerCase(Locale.ROOT);
                boolean cmp = route.equals("compare");
                if (!(cmp || route.equals("local") || route.equals("cloud"))) {
                    throw new IllegalArgumentException("rule route must be local, cloud or compare");
                }
                Rule rule = new Rule(str(r, "name", ""), str(r, "accessKey", null), str(r, "ip", null),
                        str(r, "header", null), str(r, "headerValue", null),
                        cmp ? AbSide.LOCAL : AbSide.parse(route), cmp,
                        r.has("pinWrites") && r.get("pinWrites").getAsBoolean());
                if (rule.accessKey == null && rule.ip == null && rule.header == null) {
                    throw new IllegalArgumentException("a rule needs at least one of accessKey, ip, header");
                }
                if (rule.ip != null) {
                    ipMatches(rule.ip, "0.0.0.0"); // validates the CIDR/address syntax
                }
                rs.add(rule);
            }
        }
        this.rules = List.copyOf(rs);
        JsonObject cmp = o.has("compare") && o.get("compare").isJsonObject() ? o.getAsJsonObject("compare") : new JsonObject();
        this.comparePrimary = AbSide.parse(str(cmp, "primary", "local"));
        Set<String> ik = new java.util.LinkedHashSet<>(List.of("RequestId", "requestId", "HostId", "ResponseMetadata",
                "ConsumedCapacity", "x-amz-request-id", "x-amz-id-2", "x-amzn-RequestId"));
        if (cmp.has("ignoreKeys")) {
            ik.clear();
            for (JsonElement e : cmp.getAsJsonArray("ignoreKeys")) {
                ik.add(e.getAsString());
            }
        }
        this.ignoreKeys = Set.copyOf(ik);
        this.recordValues = cmp.has("recordValues") && cmp.get("recordValues").getAsBoolean();
        this.compareMaxBodyBytes = cmp.has("maxBodyBytes") ? Math.max(1024, cmp.get("maxBodyBytes").getAsLong()) : 4L * 1024 * 1024;
        this.bufferSize = cmp.has("bufferSize") ? Math.max(10, Math.min(5000, cmp.get("bufferSize").getAsInt())) : 500;
        this.writeOwner = AbSide.parse(str(o, "writeOwner", "local"));
        this.dualWrite = o.has("dualWrite") && o.get("dualWrite").getAsBoolean();
        this.dualWriteMaxBytes = o.has("dualWriteMaxBytes") ? Math.max(1024, o.get("dualWriteMaxBytes").getAsLong()) : 16L * 1024 * 1024;
        Map<String, String> ro = new LinkedHashMap<>();
        if (o.has("roleOverrides") && o.get("roleOverrides").isJsonObject()) {
            for (var e : o.getAsJsonObject("roleOverrides").entrySet()) {
                ro.put(e.getKey(), e.getValue().getAsString());
            }
        }
        this.roleOverrides = Map.copyOf(ro);
        this.trustXForwardedFor = o.has("trustXForwardedFor") && o.get("trustXForwardedFor").getAsBoolean();
        boolean usesCloud = mode != Mode.LOCAL || writeOwner == AbSide.CLOUD || dualWrite
                || rules.stream().anyMatch(r -> r.route == AbSide.CLOUD || r.compare);
        if (usesCloud && (target == null || target.isBlank())) {
            throw new IllegalArgumentException("policy routes to the cloud, so it needs a \"target\" (a configured cloud target name)");
        }
    }

    public static AbPolicy parse(String store, JsonObject o) {
        return new AbPolicy(o, store);
    }

    private static String str(JsonObject o, String k, String def) {
        return o.has(k) && !o.get(k).isJsonNull() ? o.get(k).getAsString() : def;
    }

    /** The routing decision for one request (kill switch not included: see {@link AbState#decide}). */
    public Route decide(Ctx c) {
        Rule hit = null;
        for (Rule r : rules) {
            if (r.matches(c)) {
                hit = r;
                break;
            }
        }
        Route base;
        boolean pinned = false;
        if (hit != null) {
            base = hit.compare ? Route.COMPARE : hit.route == AbSide.CLOUD ? Route.CLOUD : Route.LOCAL;
            pinned = hit.pinWrites;
        } else {
            base = switch (mode) {
                case LOCAL -> Route.LOCAL;
                case CLOUD -> Route.CLOUD;
                case COMPARE -> Route.COMPARE;
                case SPLIT -> bucket(clientKey(c)) < (int) Math.round(cloudPercent * 100) ? Route.CLOUD : Route.LOCAL;
            };
        }
        if (c.read()) {
            return base;
        }
        if (mode == Mode.LOCAL && hit == null) {
            return Route.LOCAL;
        }
        if (mode == Mode.CLOUD && hit == null) {
            return Route.CLOUD;
        }
        // a split-family situation (split, compare, or a rule): writes go to ONE side
        if (pinned && base != Route.COMPARE) {
            return base;
        }
        if (dualWrite) {
            return Route.DUAL_WRITE;
        }
        return writeOwner == AbSide.CLOUD ? Route.CLOUD : Route.LOCAL;
    }

    public String clientKey(Ctx c) {
        String v = null;
        if (stickyBy.startsWith("header:")) {
            v = c.header(stickyBy.substring(7));
        } else if (stickyBy.equals("ip")) {
            v = c.ip();
        } else {
            v = c.accessKey();
        }
        if (v == null || v.isBlank()) {
            v = c.accessKey() != null ? c.accessKey() : c.ip();
        }
        return v == null ? "" : v;
    }

    /** Deterministic sticky bucket 0..9999 of a client key (SHA-256 of seed|key, first 8 bytes). */
    public int bucket(String clientKey) {
        return bucketOf(seed, clientKey);
    }

    public static int bucketOf(String seed, String clientKey) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest((seed + "|" + clientKey).getBytes(StandardCharsets.UTF_8));
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (d[i] & 0xff);
            }
            return (int) Long.remainderUnsigned(v, 10000);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static boolean glob(String pattern, String value) {
        if (pattern.indexOf('*') < 0) {
            return pattern.equals(value);
        }
        StringBuilder re = new StringBuilder();
        for (String part : pattern.split("\\*", -1)) {
            if (re.length() > 0) {
                re.append(".*");
            }
            re.append(Pattern.quote(part));
        }
        return Pattern.matches(re.toString(), value);
    }

    /** Exact address or CIDR ({@code 10.0.0.0/8}, {@code ::1/128}); throws on invalid syntax. */
    static boolean ipMatches(String spec, String ip) {
        try {
            int slash = spec.indexOf('/');
            byte[] net = InetAddress.getByName(slash < 0 ? spec : spec.substring(0, slash)).getAddress();
            byte[] addr = InetAddress.getByName(ip).getAddress();
            if (net.length != addr.length) {
                return false;
            }
            int bits = slash < 0 ? net.length * 8 : Integer.parseInt(spec.substring(slash + 1));
            for (int i = 0; i < net.length && bits > 0; i++, bits -= 8) {
                int mask = bits >= 8 ? 0xff : (0xff << (8 - bits)) & 0xff;
                if ((net[i] & mask) != (addr[i] & mask)) {
                    return false;
                }
            }
            return true;
        } catch (java.net.UnknownHostException | NumberFormatException e) {
            throw new IllegalArgumentException("invalid ip / CIDR: " + spec);
        }
    }

    /** JSON view; contains no secrets (policies hold none), role ARNs are not secret. */
    public JsonObject toJson() {
        JsonObject o = new JsonObject();
        o.addProperty("store", store);
        o.addProperty("mode", mode.name().toLowerCase(Locale.ROOT));
        o.addProperty("target", target);
        o.addProperty("cloudPercent", cloudPercent);
        o.addProperty("stickyBy", stickyBy);
        o.addProperty("seed", seed);
        JsonArray ra = new JsonArray();
        for (Rule r : rules) {
            JsonObject j = new JsonObject();
            j.addProperty("name", r.name);
            j.addProperty("accessKey", r.accessKey);
            j.addProperty("ip", r.ip);
            j.addProperty("header", r.header);
            j.addProperty("headerValue", r.headerValue);
            j.addProperty("route", r.compare ? "compare" : r.route.name().toLowerCase(Locale.ROOT));
            j.addProperty("pinWrites", r.pinWrites);
            ra.add(j);
        }
        o.add("rules", ra);
        JsonObject c = new JsonObject();
        c.addProperty("primary", comparePrimary.name().toLowerCase(Locale.ROOT));
        JsonArray ik = new JsonArray();
        ignoreKeys.stream().sorted().forEach(ik::add);
        c.add("ignoreKeys", ik);
        c.addProperty("recordValues", recordValues);
        c.addProperty("maxBodyBytes", compareMaxBodyBytes);
        c.addProperty("bufferSize", bufferSize);
        o.add("compare", c);
        o.addProperty("writeOwner", writeOwner.name().toLowerCase(Locale.ROOT));
        o.addProperty("dualWrite", dualWrite);
        o.addProperty("dualWriteMaxBytes", dualWriteMaxBytes);
        JsonObject ro = new JsonObject();
        roleOverrides.forEach(ro::addProperty);
        o.add("roleOverrides", ro);
        o.addProperty("trustXForwardedFor", trustXForwardedFor);
        return o;
    }
}
