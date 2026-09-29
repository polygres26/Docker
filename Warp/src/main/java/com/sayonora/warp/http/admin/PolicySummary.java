package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.sayonora.warp.config.WarpConfig;

/**
 * Which control-plane policies apply to a given interface, for {@code GET /api/interfaces}'
 * {@code policies} field -- factored out of what used to be an independent, per-page
 * client-side computation in {@code Workloads.tsx} (its own {@code routerConfigured}/{@code
 * qosConfigured}/{@code aclRules}/{@code fwRules} booleans), so it's computed once, server-side,
 * exactly like {@link AccessSummary#frontendAuth} already is for authentication -- one real source
 * of truth every consumer (Workloads, Interfaces, a future Security aggregate) joins onto instead
 * of reimplementing. See the shared-readiness-evaluator scoping notes for the fuller design (this
 * class covers the "Policy" dimension only; maturity and the overall rollup are computed
 * client-side, deliberately -- maturity is editorial, not measured, see {@code maturity.ts}).
 *
 * <p>Router/QoS/Firewall only ever apply to the SQL pipeline (a store-backed API frontend's
 * requests never pass through {@code RouterStage}/{@code QosStage}/{@code FirewallStage} --
 * confirmed by the same {@code i.kind === 'sql'} gating {@code Workloads.tsx} already used before
 * this move). ACL is a connection-level gate that applies to every frontend, SQL or not.
 */
public final class PolicySummary {

    private PolicySummary() {
    }

    /** {@code aclRuleCount}/{@code firewallRuleCount}: real counts (0 = none configured), not
     * booleans, so a caller can show "3 rules" instead of a bare on/off. */
    public record Summary(boolean routerConfigured, boolean qosConfigured, int aclRuleCount, int firewallRuleCount) {
    }

    /** {@code firewallEnabledRuleCount}: pass the count of currently-ENABLED firewall rules (a
     * disabled rule doesn't apply to anything) -- see {@code FirewallRuleStore#listAll} filtered on
     * {@code enabled}, the same source {@code GET /api/firewall-rules} already reads. */
    public static Summary compute(WarpConfig cfg, int firewallEnabledRuleCount) {
        if (cfg == null) {
            return new Summary(false, false, 0, firewallEnabledRuleCount);
        }
        boolean router = anyFilled(cfg.routerSchemaRules(), cfg.routerPredicateRules(), cfg.routerValueShardRules(),
                cfg.routerShardTables(), cfg.routerTableShards());
        boolean qos = filled(cfg.qosRatePerSec()) || filled(cfg.qosClassLimits());
        int aclCount = aclRuleCount(cfg.aclRules());
        return new Summary(router, qos, aclCount, firewallEnabledRuleCount);
    }

    /** The policies that actually apply to an interface of this {@code kind}, as short tags
     * ({@code "router"}, {@code "qos"}, {@code "firewall:N"}, {@code "acl:N"}) -- always includes
     * ACL (every frontend), only includes router/qos/firewall for {@code kind == "sql"}. An
     * unconfigured policy is simply absent from the array (not a {@code false} entry), except ACL,
     * which always appears (as {@code "acl:0"} when no rules exist) so "explicitly no ACL" reads
     * differently from "this dimension wasn't computed at all" -- the same "never silently guess,
     * report an explicit absence" posture {@link AccessSummary} already takes for auth. */
    public static JsonArray applicablePolicies(Summary s, String kind) {
        JsonArray arr = new JsonArray();
        if ("sql".equals(kind)) {
            if (s.routerConfigured()) {
                arr.add("router");
            }
            if (s.qosConfigured()) {
                arr.add("qos");
            }
            if (s.firewallRuleCount() > 0) {
                arr.add("firewall:" + s.firewallRuleCount());
            }
        }
        arr.add("acl:" + s.aclRuleCount());
        return arr;
    }

    private static int aclRuleCount(String aclRules) {
        if (!filled(aclRules)) {
            return 0;
        }
        int count = 0;
        for (String entry : aclRules.split(";")) {
            if (!entry.isBlank()) {
                count++;
            }
        }
        return count;
    }

    private static boolean anyFilled(String... values) {
        for (String v : values) {
            if (filled(v)) {
                return true;
            }
        }
        return false;
    }

    private static boolean filled(String s) {
        return s != null && !s.isBlank();
    }
}
