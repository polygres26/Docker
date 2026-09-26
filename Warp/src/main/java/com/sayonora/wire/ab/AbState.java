package com.sayonora.wire.ab;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One immutable, versioned snapshot of the whole A/B routing configuration: policies by store, cloud
 * targets by name, and the kill switch. Swapped atomically on every change, on every node.
 */
public final class AbState {

    public record Kill(AbSide side, String reason, String by, long atMillis) {
        JsonObject toJson() {
            JsonObject o = new JsonObject();
            o.addProperty("side", side.id());
            o.addProperty("reason", reason);
            o.addProperty("by", by);
            o.addProperty("at", atMillis);
            return o;
        }

        static Kill from(JsonObject o) {
            return new Kill(AbSide.parse(o.get("side").getAsString()),
                    o.has("reason") && !o.get("reason").isJsonNull() ? o.get("reason").getAsString() : null,
                    o.has("by") && !o.get("by").isJsonNull() ? o.get("by").getAsString() : null,
                    o.has("at") ? o.get("at").getAsLong() : 0);
        }
    }

    public static final AbState EMPTY = new AbState(0, Map.of(), Map.of(), null, Map.of());

    public final long version;
    public final Map<String, AbPolicy> policies;
    public final Map<String, AbTarget> targets;
    public final Kill kill;
    public final Map<String, Kill> storeKill;

    AbState(long version, Map<String, AbPolicy> policies, Map<String, AbTarget> targets, Kill kill, Map<String, Kill> storeKill) {
        this.version = version;
        this.policies = policies;
        this.targets = targets;
        this.kill = kill;
        this.storeKill = storeKill;
    }

    /** Build from the persisted document; secrets in it are decrypted here. */
    static AbState fromStored(long version, JsonObject doc, AbState previous) {
        Map<String, AbTarget> targets = new LinkedHashMap<>();
        if (doc.has("targets")) {
            for (var e : doc.getAsJsonObject("targets").entrySet()) {
                try {
                    AbTarget t = new AbTarget(e.getKey(), AbTarget.decrypt(e.getValue().getAsJsonObject()));
                    t.adoptProvider(previous == null ? null : previous.targets.get(e.getKey()));
                    targets.put(e.getKey(), t);
                } catch (RuntimeException ex) {
                    // one broken target must not take the rest of the config down; requests routed to it fail 502
                    org.slf4j.LoggerFactory.getLogger(AbState.class).warn("ab-routing: target '{}' ignored: {}", e.getKey(), ex.getMessage());
                }
            }
        }
        Map<String, AbPolicy> policies = new LinkedHashMap<>();
        if (doc.has("policies")) {
            for (var e : doc.getAsJsonObject("policies").entrySet()) {
                try {
                    policies.put(e.getKey(), AbPolicy.parse(e.getKey(), e.getValue().getAsJsonObject()));
                } catch (RuntimeException ex) {
                    org.slf4j.LoggerFactory.getLogger(AbState.class).warn("ab-routing: policy '{}' ignored: {}", e.getKey(), ex.getMessage());
                }
            }
        }
        Kill kill = doc.has("killSwitch") && doc.get("killSwitch").isJsonObject() ? Kill.from(doc.getAsJsonObject("killSwitch")) : null;
        Map<String, Kill> sk = new LinkedHashMap<>();
        if (doc.has("storeKill")) {
            doc.getAsJsonObject("storeKill").entrySet().forEach(e -> sk.put(e.getKey(), Kill.from(e.getValue().getAsJsonObject())));
        }
        return new AbState(version, Map.copyOf(policies), Map.copyOf(targets), kill, Map.copyOf(sk));
    }

    public Kill killFor(String store) {
        Kill k = storeKill.get(store);
        return k != null ? k : kill;
    }

    /** Cloud target name that serves {@code store}: the policy's, else the only configured target. */
    public AbTarget targetFor(String store) {
        AbPolicy p = policies.get(store);
        if (p != null && p.target != null) {
            return targets.get(p.target);
        }
        return targets.size() == 1 ? targets.values().iterator().next() : null;
    }

    /**
     * Full routing decision for one request of {@code store}. Kill switch first (it wins over every
     * rule, split and dual-write); then the store's policy; no policy = local.
     */
    public AbPolicy.Route decide(String store, AbPolicy.Ctx ctx) {
        Kill k = killFor(store);
        if (k != null) {
            return k.side() == AbSide.LOCAL ? AbPolicy.Route.LOCAL : AbPolicy.Route.CLOUD;
        }
        AbPolicy p = policies.get(store);
        return p == null ? AbPolicy.Route.LOCAL : p.decide(ctx);
    }

    /** True when every request of {@code store} is served locally, with no need to look at the request at all. */
    public boolean alwaysLocal(String store) {
        Kill k = killFor(store);
        if (k != null) {
            return k.side() == AbSide.LOCAL;
        }
        AbPolicy p = policies.get(store);
        return p == null || (p.mode == AbPolicy.Mode.LOCAL && p.rules.isEmpty());
    }

    public JsonObject toPublicJson() {
        JsonObject o = new JsonObject();
        o.addProperty("version", version);
        JsonObject ps = new JsonObject();
        policies.forEach((k, v) -> ps.add(k, v.toJson()));
        o.add("policies", ps);
        JsonArray ts = new JsonArray();
        targets.values().forEach(t -> ts.add(t.publicJson()));
        o.add("targets", ts);
        o.add("killSwitch", kill == null ? null : kill.toJson());
        JsonObject sk = new JsonObject();
        storeKill.forEach((k, v) -> sk.add(k, v.toJson()));
        o.add("storeKill", sk);
        return o;
    }
}
