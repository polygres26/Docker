package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.audit.AuditEvent;
import com.sayonora.warp.audit.AuditLog;
import com.sayonora.warp.cluster.CacheStage;
import com.sayonora.warp.cluster.CacheStats;
import com.sayonora.warp.cluster.RowCache;
import com.sayonora.warp.core.MetricsHistory;
import com.sayonora.warp.core.PolicyDecisionLog;
import com.sayonora.warp.core.TranslationCache;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Read-mostly admin endpoints that expose counters Warp already keeps: {@code GET /api/metrics/history}
 * (sampled cumulative counters), {@code GET /api/cache/stats}, {@code POST /api/cache/invalidate} and
 * {@code GET /api/policy-decisions}. Every field is a real counter or a real recent event; anything Warp
 * does not track is simply not present.
 */
public final class WarpInsightsApi {

    private volatile MetricsHistory history;
    private volatile CacheStage cacheStage;
    private volatile Supplier<TranslationCache> translationCache = () -> null;
    private volatile AuditLog auditLog;
    private volatile Supplier<Integer> clusterSize = () -> 1;

    public void setHistory(MetricsHistory history) {
        this.history = history;
    }

    public void setCacheStage(CacheStage cacheStage) {
        this.cacheStage = cacheStage;
    }

    public void setTranslationCache(Supplier<TranslationCache> translationCache) {
        this.translationCache = translationCache;
    }

    public void setAuditLog(AuditLog auditLog) {
        this.auditLog = auditLog;
    }

    public void setClusterSize(Supplier<Integer> clusterSize) {
        this.clusterSize = clusterSize;
    }

    public static boolean handles(String target) {
        return "/api/metrics/history".equals(target) || "/api/cache/stats".equals(target)
                || "/api/cache/invalidate".equals(target) || "/api/policy-decisions".equals(target);
    }

    public void handle(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json; charset=utf-8");
        String method = request.getMethod();
        String body;
        if ("/api/metrics/history".equals(target) && "GET".equals(method)) {
            body = renderHistory().toString();
        } else if ("/api/cache/stats".equals(target) && "GET".equals(method)) {
            body = renderCacheStats().toString();
        } else if ("/api/cache/invalidate".equals(target) && "POST".equals(method)) {
            body = invalidate(request, response);
        } else if ("/api/policy-decisions".equals(target) && "GET".equals(method)) {
            int limit = 100;
            try {
                String p = request.getParameter("limit");
                if (p != null) {
                    limit = Math.max(1, Math.min(500, Integer.parseInt(p)));
                }
            } catch (NumberFormatException ignored) {
                // keep default
            }
            body = renderPolicyDecisions(limit).toString();
        } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            body = "{\"error\":\"no such route\"}";
        }
        response.getWriter().write(body);
    }

    JsonObject renderHistory() {
        JsonObject out = new JsonObject();
        MetricsHistory h = history;
        out.addProperty("enabled", h != null);
        JsonArray samples = new JsonArray();
        if (h != null) {
            out.addProperty("intervalSeconds", h.intervalMillis() / 1000.0);
            out.addProperty("capacity", h.capacity());
            for (MetricsHistory.Sample s : h.samples()) {
                JsonObject o = new JsonObject();
                o.addProperty("t", s.timestampMillis());
                JsonObject c = new JsonObject();
                s.counters().forEach(c::addProperty);
                o.add("c", c);
                samples.add(o);
            }
        }
        out.add("samples", samples);
        return out;
    }

    JsonObject renderCacheStats() {
        JsonObject out = new JsonObject();
        CacheStage stage = cacheStage;
        out.addProperty("enabled", stage != null);
        out.addProperty("clusterNodes", clusterSize.get());
        JsonObject tiers = new JsonObject();
        if (stage != null) {
            CacheStage.Info info = stage.info();
            CacheStats.Snapshot snap = stage.stats().snapshot();
            JsonArray patterns = new JsonArray();
            info.tablePatterns().forEach(patterns::add);
            out.add("tablePatterns", patterns);
            out.addProperty("ttlMillis", info.ttlMillis());
            tiers.add("result", tier(info.resultEntries(), snap.resultHits(), snap.resultMisses()));
            tiers.add("pk", tier(info.pkEntries(), snap.pkHits(), snap.pkMisses()));
            JsonObject row = tier(info.rowEntries(), RowCache.hitCount(), RowCache.missCount());
            row.addProperty("attached", info.rowCacheAttached());
            row.addProperty("invalidatedKeys", RowCache.invalidatedKeyCount());
            tiers.add("row", row);
            JsonObject inv = new JsonObject();
            inv.addProperty("entriesRemoved", snap.invalidatedEntries());
            inv.addProperty("events", snap.invalidationEvents());
            inv.addProperty("fullClears", snap.fullClears());
            JsonArray recent = new JsonArray();
            for (CacheStats.Invalidation e : snap.recent()) {
                JsonObject o = new JsonObject();
                o.addProperty("at", e.at().toString());
                o.addProperty("kind", e.kind());
                o.addProperty("target", e.target());
                o.addProperty("entries", e.entries());
                o.addProperty("source", e.source());
                recent.add(o);
            }
            inv.add("recent", recent);
            out.add("invalidations", inv);
            JsonArray tables = new JsonArray();
            for (CacheStats.TableStat t : snap.byTable()) {
                JsonObject o = new JsonObject();
                o.addProperty("table", t.table());
                o.addProperty("hits", t.hits());
                o.addProperty("misses", t.misses());
                tables.add(o);
            }
            out.add("byTable", tables);
        }
        TranslationCache tc = translationCache.get();
        if (tc != null) {
            JsonObject t = new JsonObject();
            t.addProperty("entries", tc.size());
            t.addProperty("maxEntries", tc.maxEntries());
            t.addProperty("hits", tc.hits());
            t.addProperty("misses", tc.misses());
            t.addProperty("evictions", tc.evictions());
            tiers.add("translation", t);
        }
        out.add("tiers", tiers);
        return out;
    }

    private static JsonObject tier(long entries, long hits, long misses) {
        JsonObject o = new JsonObject();
        o.addProperty("entries", entries);
        o.addProperty("hits", hits);
        o.addProperty("misses", misses);
        return o;
    }

    private String invalidate(HttpServletRequest request, HttpServletResponse response) throws IOException {
        CacheStage stage = cacheStage;
        if (stage == null) {
            response.setStatus(HttpServletResponse.SC_CONFLICT);
            return "{\"error\":\"the result cache is not running on this node (no cache tables configured at startup)\"}";
        }
        String table = null;
        try (var reader = request.getReader()) {
            var parsed = JsonParser.parseReader(reader);
            if (parsed != null && !parsed.isJsonNull() && !parsed.isJsonObject()) {
                throw new IllegalArgumentException("not an object");
            }
            if (parsed != null && parsed.isJsonObject() && parsed.getAsJsonObject().has("table")
                    && !parsed.getAsJsonObject().get("table").isJsonNull()) {
                table = parsed.getAsJsonObject().get("table").getAsString().trim();
            }
        } catch (RuntimeException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return "{\"error\":\"body must be JSON: {} to clear everything or {\\\"table\\\":\\\"name\\\"}\"}";
        }
        JsonObject out = new JsonObject();
        if (table == null || table.isEmpty()) {
            stage.invalidateEverything("operator request via admin API");
            out.addProperty("scope", "all");
        } else {
            int dot = table.indexOf('.');
            if (dot > 0) {
                stage.invalidateTable(table.substring(0, dot), table.substring(dot + 1), "operator request via admin API");
            } else {
                stage.invalidateTable(null, table, "operator request via admin API");
            }
            out.addProperty("scope", "table");
            out.addProperty("table", table);
        }
        out.addProperty("ok", true);
        return out.toString();
    }

    JsonObject renderPolicyDecisions(int limit) {
        List<Object[]> merged = new ArrayList<>();
        for (PolicyDecisionLog.Decision d : PolicyDecisionLog.get().recent(limit)) {
            merged.add(new Object[] {d.at(), row(d.at(), d.policy(), d.identity(), d.target(), d.decision(), d.reason(),
                    "policy-log")});
        }
        AuditLog log = auditLog;
        if (log != null) {
            for (AuditEvent e : log.recent(Math.max(limit * 3, 300))) {
                JsonObject r = fromAudit(e);
                if (r != null) {
                    merged.add(new Object[] {e.timestamp(), r});
                }
            }
        }
        merged.sort(Comparator.comparing((Object[] m) -> (Instant) m[0]).reversed());
        JsonArray arr = new JsonArray();
        for (Object[] m : merged) {
            if (arr.size() >= limit) {
                break;
            }
            arr.add((JsonObject) m[1]);
        }
        JsonObject out = new JsonObject();
        out.add("decisions", arr);
        out.addProperty("policyLogRecorded", PolicyDecisionLog.get().totalRecorded());
        out.addProperty("auditAvailable", log != null);
        out.addProperty("mcpReadOnly", "true".equalsIgnoreCase(System.getenv("WARP_MCP_READ_ONLY")));
        return out;
    }

    private static JsonObject fromAudit(AuditEvent e) {
        Map<String, String> d = e.details();
        return switch (e.type()) {
            case ACCESS_DENIED -> row(e.timestamp(), "Access control", e.userId(), firstNonNull(d.get("column"), d.get("filterColumn")),
                    "Block", e.summary(), "audit");
            case ACCESS_ALLOWED -> row(e.timestamp(), "Access control", e.userId(), null, "Allow", e.summary(), "audit");
            case ROW_FILTER_APPLIED -> row(e.timestamp(), "Row filter", e.userId(), d.get("filterColumn"), "Allow (filtered)",
                    e.summary(), "audit");
            case COLUMN_MASKED -> row(e.timestamp(), "Column mask", e.userId(), d.get("column"), "Allow (masked)",
                    e.summary(), "audit");
            case DB_LOGIN_FAILED -> row(e.timestamp(), "Authentication", e.userId(), null, "Block", e.summary(), "audit");
            case MCP_TOOL_CALLED -> row(e.timestamp(), "MCP tools", e.userId(),
                    firstNonNull(d.get("tool"), null) + (d.get("endpoint") == null ? "" : " @ " + d.get("endpoint")),
                    "true".equals(d.get("success")) ? "Allow" : "Error",
                    "true".equals(d.get("success")) ? e.summary() : e.summary() + (d.get("error") == null ? "" : ": " + d.get("error")),
                    "audit");
            default -> null;
        };
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }

    private static JsonObject row(Instant at, String policy, String identity, String target, String decision, String reason,
            String source) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("timestamp", at.toString());
        m.put("policy", policy);
        m.put("identity", identity);
        m.put("target", target);
        m.put("decision", decision);
        m.put("reason", reason);
        m.put("source", source);
        JsonObject o = new JsonObject();
        m.forEach(o::addProperty);
        return o;
    }
}
