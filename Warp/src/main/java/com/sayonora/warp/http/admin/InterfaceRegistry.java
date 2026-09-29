package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.StoreType;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The frontends this Warp process is actually serving. {@code Main} registers each one right after
 * its listener has bound (so a frontend that failed to start is never listed), and
 * {@code GET /api/interfaces} renders the registry joined with live data: the backend set and
 * hosts a store-backed frontend is served from, and the request count the metrics collector holds
 * under the frontend's {@code metricsKey} (0 until it has served something; null without a key).
 *
 * <p>{@code kind}: {@code sql} (relational wire drivers), {@code api} (storage/messaging/search APIs),
 * {@code mcp} (Model Context Protocol / agent frontends). {@code mode}: {@code Relay} (native
 * protocol forwarded to a backend of the same engine), {@code Emulate} (client dialect translated to
 * the backend's) or {@code Emulate} (an API implemented on top of Postgres), or {@code null} when
 * none of those applies.
 */
public final class InterfaceRegistry {

    public record Entry(String id, String label, String kind, String protocol, int port, String mode,
            String storeId, String metricsKey) {
    }

    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    /** Interface id to the TlsListeners name of the HTTP-style frontends that serve HTTPS in addition to plaintext. */
    private static final Map<String, String> TLS_NAMES = Map.ofEntries(Map.entry("dynamowire", "DYNAMOWIRE"),
            Map.entry("sqswire", "SQSWIRE"), Map.entry("oswire", "OSWIRE"), Map.entry("influxwire", "INFLUXWIRE"),
            Map.entry("s3wire", "S3WIRE"), Map.entry("gcswire", "GCSWIRE"), Map.entry("awswire", "AWSWIRE"),
            Map.entry("pubsubwire-rest", "PUBSUBWIRE_REST"), Map.entry("cosmoswire", "COSMOSWIRE"),
            Map.entry("azblobwire", "AZBLOBWIRE"), Map.entry("azqueuewire", "AZQUEUEWIRE"), Map.entry("aztablewire", "AZTABLEWIRE"),
            Map.entry("gremlinwire", "GREMLINWIRE"));

    private InterfaceRegistry() {
    }

    public static void register(String id, String label, String kind, String protocol, int port, String mode,
            String storeId, String metricsKey) {
        ENTRIES.put(id, new Entry(id, label, kind, protocol, port, mode, storeId, metricsKey));
    }

    public static void clear() {
        ENTRIES.clear();
    }

    public static List<Entry> entries() {
        return ENTRIES.values().stream()
                .sorted(java.util.Comparator.comparingInt((Entry e) -> "sql".equals(e.kind()) ? 0 : "api".equals(e.kind()) ? 1 : 2).thenComparing(Entry::port).thenComparing(Entry::id))
                .toList();
    }

    /** JSON for {@code GET /api/interfaces}; {@code registry} and {@code protocolCounts} may be null. */
    /** As the 5-arg overload, with no policy config available ({@code policies} is
     * omitted from every row). Kept for callers (and existing tests) that don't have a {@code
     * WarpConfig}/firewall-rule-count on hand. */
    public static JsonObject toJson(BackendRegistry registry, Map<String, Long> protocolCounts, int activeSessions) {
        return toJson(registry, protocolCounts, activeSessions, null, 0);
    }

    /**
     * @param policyConfig the latest {@code WarpConfig} (for router/QoS/ACL rule presence), or
     *     {@code null} to omit {@code policies} from every row entirely
     * @param firewallEnabledRuleCount count of currently-ENABLED SQL firewall rules (see {@link
     *     PolicySummary#compute})
     */
    public static JsonObject toJson(BackendRegistry registry, Map<String, Long> protocolCounts, int activeSessions,
            com.sayonora.warp.config.WarpConfig policyConfig, int firewallEnabledRuleCount) {
        // Joined onto every row below (auth*) rather than left isolated on the Access page -- see
        // AccessSummary#frontendAuth's own javadoc for why this is now factored out that way. A
        // frontend absent from this map (grpc, mcp, a2a, boltwire, dynamowire, sqswire, oswire,
        // mongowire, ...) reports no auth fields at all, which the UI shows as "not reported" --
        // never guessed.
        Map<String, JsonObject> auth = AccessSummary.frontendAuth(System.getenv());
        PolicySummary.Summary policySummary = policyConfig == null ? null
                : PolicySummary.compute(policyConfig, firewallEnabledRuleCount);
        JsonArray arr = new JsonArray();
        for (Entry e : entries()) {
            JsonObject o = new JsonObject();
            o.addProperty("id", e.id());
            o.addProperty("label", e.label());
            o.addProperty("kind", e.kind());
            o.addProperty("protocol", e.protocol());
            o.addProperty("port", e.port());
            o.addProperty("mode", e.mode());
            o.addProperty("store", e.storeId());
            // "listening" only ever meant "the socket is bound" -- it said nothing about whether
            // the frontend has anywhere real to put/read data. A store-backed frontend (s3wire,
            // dynamowire, ...) whose store isn't enabled on ANY backend still shows this as
            // "listening" today, which reads as healthy when it's actually running on the legacy
            // implicit-default-backend fallback, not the store the operator thinks they enabled.
            // Surfaced as its own status value here (rather than folded silently into "listening")
            // so the UI can render it as a real, distinct warning instead of a plain green pill --
            // see docs/WARP_GUIDE.md and InterfaceTable.tsx's own StatusPill rendering.
            boolean storeConfiguredButNotHosted = false;
            if (registry != null && e.storeId() != null) {
                StoreType t = StoreType.valueOf(e.storeId().toUpperCase(Locale.ROOT));
                o.addProperty("set", registry.frontendSet(t));
                JsonArray hosts = new JsonArray();
                registry.storeHosts(t).forEach(hosts::add);
                o.add("hosts", hosts);
                o.addProperty("setEnvVar", t.setEnvVar());
                storeConfiguredButNotHosted = hosts.isEmpty();
            }
            o.addProperty("status", storeConfiguredButNotHosted ? "listening_no_store" : "listening");
            JsonObject authEntry = auth.get(e.id());
            if (authEntry != null) {
                o.addProperty("authMethod", authEntry.get("method").getAsString());
                o.addProperty("authEnforced", authEntry.get("enforced").getAsBoolean());
                o.addProperty("authDetail", authEntry.get("detail").getAsString());
            }
            if (policySummary != null) {
                o.add("policies", PolicySummary.applicablePolicies(policySummary, e.kind()));
            }
            // The collector only has an entry for a protocol once it served something: no entry means 0 so far.
            // No metrics key at all (e.g. A2A) means the count is unknown, reported as null.
            Long count = protocolCounts == null || e.metricsKey() == null ? null : protocolCounts.getOrDefault(e.metricsKey(), 0L);
            com.sayonora.warp.tls.TlsListeners.Info tls = "mcp".equals(e.id()) ? com.sayonora.warp.tls.TlsListeners.info("MCP")
                    : "a2a".equals(e.id()) ? com.sayonora.warp.tls.TlsListeners.info("A2A")
                    : TLS_NAMES.containsKey(e.id()) ? com.sayonora.warp.tls.TlsListeners.info(TLS_NAMES.get(e.id())) : null;
            if (tls != null) {
                o.addProperty("tlsEnabled", tls.tlsEnabled());
                o.addProperty("httpsPort", tls.tlsEnabled() ? tls.httpsPort() : null);
                o.addProperty("selfSigned", tls.selfSigned());
                if (tls.tlsError() != null) {
                    o.addProperty("tlsError", tls.tlsError());
                }
            }
            // Protocol frontends (gRPC / raw TCP): WireTls.Info. Absent = never set up = TLS not enabled.
            if (tls == null) {
                String wid = e.id().endsWith("-native") ? e.id().substring(0, e.id().length() - 7) : e.id();
                com.sayonora.warp.tls.WireTls.Info wt = com.sayonora.warp.tls.WireTls.info(wid);
                o.addProperty("tlsEnabled", wt != null && wt.tlsEnabled());
                o.addProperty("tlsMode", wt == null ? "off" : wt.mode());
                if (wt != null && wt.tlsEnabled()) {
                    o.addProperty("tlsPort", wt.tlsPort());
                    o.addProperty("selfSigned", wt.selfSigned());
                    o.addProperty("tlsClientAuth", wt.clientAuth());
                }
                if (wt != null && wt.error() != null) {
                    o.addProperty("tlsError", wt.error());
                }
            }
            o.addProperty("requests", count);
            o.addProperty("metricsKey", e.metricsKey());
            arr.add(o);
        }
        JsonObject out = new JsonObject();
        out.add("interfaces", arr);
        out.addProperty("activeSessions", activeSessions);
        return out;
    }
}
