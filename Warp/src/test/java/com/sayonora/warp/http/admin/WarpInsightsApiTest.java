package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.audit.AuditEvent;
import com.sayonora.warp.audit.AuditLog;
import com.sayonora.warp.core.MetricsHistory;
import com.sayonora.warp.core.PolicyDecisionLog;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WarpInsightsApiTest {

    @Test
    void routesAreRecognised() {
        assertTrue(WarpInsightsApi.handles("/api/metrics/history"));
        assertTrue(WarpInsightsApi.handles("/api/cache/stats"));
        assertTrue(WarpInsightsApi.handles("/api/cache/invalidate"));
        assertTrue(WarpInsightsApi.handles("/api/policy-decisions"));
        assertFalse(WarpInsightsApi.handles("/api/cache"));
    }

    @Test
    void historyIsEmptyUntilConfiguredThenReportsSamples() {
        WarpInsightsApi api = new WarpInsightsApi();
        assertFalse(api.renderHistory().get("enabled").getAsBoolean());
        MetricsHistory h = new MetricsHistory(4, 10_000, () -> Map.of("statements", 5L));
        h.sampleNow(1_000);
        h.sampleNow(11_000);
        api.setHistory(h);
        var json = api.renderHistory();
        assertTrue(json.get("enabled").getAsBoolean());
        assertEquals(2, json.getAsJsonArray("samples").size());
        assertEquals(5, json.getAsJsonArray("samples").get(1).getAsJsonObject().getAsJsonObject("c").get("statements").getAsLong());
    }

    @Test
    void cacheStatsReportDisabledWhenNoStage() {
        var json = new WarpInsightsApi().renderCacheStats();
        assertFalse(json.get("enabled").getAsBoolean());
        assertTrue(json.getAsJsonObject("tiers").entrySet().isEmpty() || json.getAsJsonObject("tiers").has("translation"));
    }

    @Test
    void policyDecisionsMergePolicyLogAndAuditNewestFirst() throws Exception {
        PolicyDecisionLog.get().clearForTest();
        AuditLog audit = new AuditLog();
        audit.record(new AuditEvent(java.time.Instant.parse("2026-01-01T00:00:00Z"), AuditEvent.Type.ACCESS_DENIED, "alice",
                "not entitled to column \"ssn\"", Map.of("column", "ssn")));
        audit.record(AuditEvent.of(AuditEvent.Type.ADMIN_ACTION, "x", "ignored"));
        PolicyDecisionLog.get().record("SQL firewall", "bob", "drop table t", "Block", "Deny rule #1");
        WarpInsightsApi api = new WarpInsightsApi();
        api.setAuditLog(audit);
        var rows = api.renderPolicyDecisions(10).getAsJsonArray("decisions");
        assertEquals(2, rows.size(), "admin actions are not policy decisions");
        assertEquals("SQL firewall", rows.get(0).getAsJsonObject().get("policy").getAsString());
        assertEquals("Access control", rows.get(1).getAsJsonObject().get("policy").getAsString());
        assertEquals("ssn", rows.get(1).getAsJsonObject().get("target").getAsString());
    }
}
