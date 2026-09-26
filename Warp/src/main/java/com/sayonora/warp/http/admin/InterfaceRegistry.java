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
 * protocol forwarded to a backend of the same engine), {@code Adapt} (client dialect translated to
 * the backend's) or {@code Emulate} (an API implemented on top of Postgres), or {@code null} when
 * none of those applies.
 */
public final class InterfaceRegistry {

    public record Entry(String id, String label, String kind, String protocol, int port, String mode,
            String storeId, String metricsKey) {
    }

    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

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
    public static JsonObject toJson(BackendRegistry registry, Map<String, Long> protocolCounts, int activeSessions) {
        JsonArray arr = new JsonArray();
        for (Entry e : entries()) {
            JsonObject o = new JsonObject();
            o.addProperty("id", e.id());
            o.addProperty("label", e.label());
            o.addProperty("kind", e.kind());
            o.addProperty("protocol", e.protocol());
            o.addProperty("port", e.port());
            o.addProperty("mode", e.mode());
            o.addProperty("status", "listening");
            o.addProperty("store", e.storeId());
            if (registry != null && e.storeId() != null) {
                StoreType t = StoreType.valueOf(e.storeId().toUpperCase(Locale.ROOT));
                o.addProperty("set", registry.frontendSet(t));
                JsonArray hosts = new JsonArray();
                registry.storeHosts(t).forEach(hosts::add);
                o.add("hosts", hosts);
                o.addProperty("setEnvVar", t.setEnvVar());
            }
            // The collector only has an entry for a protocol once it served something: no entry means 0 so far.
            // No metrics key at all (e.g. A2A) means the count is unknown, reported as null.
            Long count = protocolCounts == null || e.metricsKey() == null ? null : protocolCounts.getOrDefault(e.metricsKey(), 0L);
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
