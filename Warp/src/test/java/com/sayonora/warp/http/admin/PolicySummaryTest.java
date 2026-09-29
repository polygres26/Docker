package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.sayonora.warp.config.WarpConfig;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link PolicySummary} -- the "Policy" dimension of the shared readiness
 * evaluator, factored out of what used to be {@code Workloads.tsx}'s own independent, per-page
 * computation (its {@code routerConfigured}/{@code qosConfigured}/{@code aclRules}/{@code
 * fwRules} booleans) so it's computed once, server-side, and joined onto every consumer via
 * {@code GET /api/interfaces} instead.
 */
class PolicySummaryTest {

    /** Only the 3 fields this class reads are ever non-null; every other WarpConfig field is
     * irrelevant here (mirrors BackendSetModelTest's own minimal-cfg helper style). */
    private static WarpConfig cfg(String qosRatePerSec, String routerSchemaRules, String aclRules) {
        return new WarpConfig(qosRatePerSec, null, null, null, null, null, null, null, null, null,
                routerSchemaRules, null, null, null, null, null, aclRules, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    @Test
    void nullConfigMeansNothingIsConfiguredExceptTheFirewallCount() {
        PolicySummary.Summary s = PolicySummary.compute(null, 3);
        assertFalse(s.routerConfigured());
        assertFalse(s.qosConfigured());
        assertEquals(0, s.aclRuleCount());
        assertEquals(3, s.firewallRuleCount());
    }

    @Test
    void detectsRouterAndQosConfiguredFromNonBlankFields() {
        PolicySummary.Summary s = PolicySummary.compute(cfg("100", "public:pg", null), 0);
        assertTrue(s.routerConfigured());
        assertTrue(s.qosConfigured());
    }

    @Test
    void blankFieldsAreNotConfigured() {
        PolicySummary.Summary s = PolicySummary.compute(cfg("", "  ", null), 0);
        assertFalse(s.routerConfigured());
        assertFalse(s.qosConfigured());
    }

    @Test
    void countsAclRuleEntriesSeparatedBySemicolons() {
        PolicySummary.Summary s = PolicySummary.compute(cfg(null, null, "allow:10.0.0.0/8;reject:0.0.0.0/0"), 0);
        assertEquals(2, s.aclRuleCount());
    }

    @Test
    void routerQosAndFirewallOnlyApplyToSqlFrontends() {
        PolicySummary.Summary s = PolicySummary.compute(cfg("100", "public:pg", "allow:10.0.0.0/8"), 2);
        JsonArray sql = PolicySummary.applicablePolicies(s, "sql");
        assertTrue(contains(sql, "router"));
        assertTrue(contains(sql, "qos"));
        assertTrue(contains(sql, "firewall:2"));
        assertTrue(contains(sql, "acl:1"));

        JsonArray api = PolicySummary.applicablePolicies(s, "api");
        assertFalse(contains(api, "router"));
        assertFalse(contains(api, "qos"));
        assertFalse(contains(api, "firewall:2"), "firewall/router/qos never apply outside the SQL pipeline");
        assertTrue(contains(api, "acl:1"), "ACL is a connection-level gate -- applies to every frontend");
    }

    @Test
    void aclAlwaysAppearsEvenWhenNoRulesExist() {
        PolicySummary.Summary s = PolicySummary.compute(null, 0);
        JsonArray arr = PolicySummary.applicablePolicies(s, "mcp");
        assertTrue(contains(arr, "acl:0"), "explicit 'no ACL rules' must be distinguishable from 'not computed'");
    }

    private static boolean contains(JsonArray arr, String value) {
        for (var e : arr) {
            if (e.getAsString().equals(value)) {
                return true;
            }
        }
        return false;
    }
}
