package com.nexagres.wire.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackendRegistry {

    private static final Logger log = LoggerFactory.getLogger(BackendRegistry.class);

    public static final String DEFAULT_BACKEND_NAME = "default";

    /** A backend's operational state for routing purposes -- see {@link #resolveForRouting}.
     * {@code ACTIVE} is the default for every backend that's never had its state touched.
     * {@code DRAINING} is set explicitly via the admin drain API ahead of planned maintenance;
     * {@code DOWN} is reserved for a future automated health-checker (not built yet) to set on
     * an unplanned failure -- both are treated identically by routing today (prefer the
     * configured fallback, if any), so adding the health-checker later needs no routing change. */
    public enum BackendState {
        ACTIVE, DRAINING, DOWN
    }

    private volatile Map<String, BackendTarget> targets;
    private volatile List<String> shardGroup;

    // Deliberately NOT reset by reload() -- a backend's drain/down state is an operational fact
    // set by an admin action or a health check, independent of whatever WARP_BACKENDS config
    // happens to be current. A name that disappears from a fresh reload just leaves its state
    // entry orphaned (harmless -- resolveForRouting/stateOf only ever look it up by name, and
    // get(name) already returns null for an unknown name regardless of this map).
    private final Map<String, BackendState> states = new ConcurrentHashMap<>();

    private final BackendTarget defaultTarget;

    public BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup) {
        this(targets, shardGroup, null);
    }

    private BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup, BackendTarget defaultTarget) {
        this.targets = Map.copyOf(targets);
        this.shardGroup = List.copyOf(shardGroup);
        this.defaultTarget = defaultTarget;
    }

    public static BackendRegistry fromConfig(String spec, String shardGroupSpec) {
        return fromConfig(spec, shardGroupSpec, null);
    }

    public static BackendRegistry fromConfig(String spec, String shardGroupSpec, BackendTarget defaultTarget) {
        
        TrustedBackendHosts trustedHosts = TrustedBackendHosts.fromEnv();
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        if (spec != null && !spec.isBlank()) {
            for (String entry : spec.split(";")) {
                if (entry.isBlank()) {
                    continue;
                }
                int eq = entry.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String name = entry.substring(0, eq).trim();
                String[] parts = entry.substring(eq + 1).split("\\|", -1);
                String url = parts.length > 0 ? parts[0].replace("%3B", ";").replace("%3b", ";") : "";
                String user = parts.length > 1 ? parts[1] : null;
                String password = parts.length > 2 ? parts[2] : null;
                // Optional 4th field: the name of another backend in THIS SAME spec to prefer for
                // new routing while this one is DRAINING/DOWN (a same-region replica, or another
                // region's backend entirely -- the mechanism doesn't care which). Not validated to
                // exist here (the fallback entry may appear later in the spec, or be added in a
                // later reload) -- resolveForRouting treats an unresolvable fallback name as "no
                // fallback" rather than failing config parsing over it.
                String fallbackName = parts.length > 3 && !parts[3].isBlank() ? parts[3].trim() : null;
                if (!trustedHosts.isTrusted(url)) {
                    log.warn("backend registry: REFUSING to register backend '{}' ({}) -- its host is not in "
                            + "WARP_TRUSTED_BACKEND_HOSTS. This entry is skipped, not fatal; every other "
                            + "configured backend is unaffected.", name, url);
                    continue;
                }
                // License-tier backend cap (see com.nexagres.wire.license.License#maxBackends) --
                // checked against targets.size() BEFORE this entry, not after, so a spec with
                // exactly the cap's worth of backends still registers all of them; the first entry
                // past the cap is what gets skipped, same "skip this one entry, not fatal" pattern
                // as the trusted-host check just above.
                int maxBackends = com.nexagres.wire.license.License.current().maxBackends();
                if (targets.size() >= maxBackends && !targets.containsKey(name)) {
                    log.warn("license: REFUSING to register backend '{}' -- Developer edition is capped at {} "
                            + "Postgres backends (see the Pricing section of the docs for Enterprise, which has "
                            + "no backend limit). This entry is skipped, not fatal; every other configured "
                            + "backend up to the cap is unaffected.", name, maxBackends);
                    continue;
                }
                targets.put(name, new BackendTarget(name, url, user, password, null, fallbackName));
            }
        } else if (defaultTarget != null) {
            targets.put(DEFAULT_BACKEND_NAME, defaultTarget);
            log.info("backend registry: no WARP_BACKENDS configured -- registered the single "
                    + "implicit WARP_* backend as '{}' so routing/translation has a fallback target",
                    DEFAULT_BACKEND_NAME);
        }
        List<String> shardGroup = shardGroupSpec == null || shardGroupSpec.isBlank()
                ? List.of()
                : List.of(shardGroupSpec.split(",")).stream().map(String::trim).toList();
        return new BackendRegistry(targets, shardGroup, defaultTarget);
    }

    public void reload(String spec, String shardGroupSpec) {
        BackendRegistry fresh = fromConfig(spec, shardGroupSpec, this.defaultTarget);
        this.targets = fresh.targets;
        this.shardGroup = fresh.shardGroup;
    }

    /** Exact, unredirected lookup -- returns the literal backend registered under {@code name},
     * regardless of its drain/down state. This is deliberate: {@code XaRecovery} resolves an
     * in-doubt branch's backend by the exact name it was prepared against, and admin routes
     * (test/tables/query) operate on the backend an operator explicitly named -- neither should
     * be silently redirected to a fallback. Statement routing should call {@link
     * #resolveForRouting} instead. */
    public BackendTarget get(String name) {
        return targets.get(name);
    }

    /** As {@link #get}, but for new statement routing: an {@code ACTIVE} backend (the default for
     * every name, until {@link #setState} says otherwise) resolves to itself unchanged. A
     * {@code DRAINING}/{@code DOWN} backend with a configured {@link BackendTarget#fallbackName}
     * resolves to that fallback instead -- one level only, no chained fallback-of-a-fallback, to
     * keep this from ever looping. A {@code DRAINING}/{@code DOWN} backend with NO fallback
     * configured still resolves to itself: better to let the caller's connection attempt fail
     * loudly against a backend that's mid-maintenance than to silently mask a missing fallback by
     * pretending the backend is fine. Existing sessions already bound to a connection on the
     * draining backend are unaffected either way -- this only changes where the NEXT statement
     * that needs a new connection gets routed. */
    public BackendTarget resolveForRouting(String name) {
        BackendTarget target = targets.get(name);
        if (target == null || stateOf(name) == BackendState.ACTIVE || target.fallbackName() == null) {
            return target;
        }
        BackendTarget fallback = targets.get(target.fallbackName());
        return fallback != null ? fallback : target;
    }

    public BackendState stateOf(String name) {
        return states.getOrDefault(name, BackendState.ACTIVE);
    }

    /** Returns false (and changes nothing) if {@code name} isn't a currently-registered backend --
     * an admin caller should treat that as a 404, not a silently-ignored no-op. */
    public boolean setState(String name, BackendState state) {
        if (!targets.containsKey(name)) {
            return false;
        }
        states.put(name, state);
        return true;
    }

    public boolean isEmpty() {
        return targets.isEmpty();
    }

    public List<String> shardGroup() {
        return shardGroup;
    }

    public java.util.Collection<BackendTarget> all() {
        return targets.values();
    }
}
