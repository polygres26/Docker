package com.sayonora.wire.core;

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

    /** Reserved names for each protocol's own single native-backend-mode default target,
     * registered by {@code Main.java} only when that protocol's own native-mode flag
     * (WARP_MYWIRE_BACKEND=mysql / WARP_MSSQLWIRE_BACKEND=sqlserver / WARP_ORACLE_BACKEND_MODE=native)
     * is on. {@link RouterStage}'s no-rule-matched fallback checks these BY NAME, not by "the sole
     * backend of a matching dialect" -- deliberately: an operator can ALSO register other real
     * Oracle/MySQL/SQL Server backends under arbitrary names via WARP_BACKENDS purely for router-
     * rule-driven sharding (see ShardingAcrossBackendEnginesIntegrationTest), reachable via pgwire
     * with dialect translation, while a DIFFERENT protocol (say mywire) still runs in its own
     * default/translating mode -- a same-dialect match would otherwise ambiguously look identical
     * to that protocol's OWN would-be native default and silently hijack its untranslated routing
     * with no rule and no operator intent behind it. Confirmed as a real gap while generalizing
     * native-mode routing away from a hardcoded per-session pin -- see RouterStage's own javadoc
     * on resolveUnambiguousDefault. */
    public static final String MYSQL_NATIVE_DEFAULT_NAME = "mysql-native";
    public static final String MSSQL_NATIVE_DEFAULT_NAME = "mssql-native";
    public static final String ORACLE_NATIVE_DEFAULT_NAME = "oracle-native";

    /** The dual-port native-mode listener's own registered target -- a DIFFERENT name from
     * {@link #MYSQL_NATIVE_DEFAULT_NAME}/{@link #MSSQL_NATIVE_DEFAULT_NAME} deliberately, so a
     * dual-port deployment (BOTH a translated-mode listener AND a native-mode listener running
     * from the same process at once -- see ServerOptions#withMywireNativeListener/
     * withMssqlwireNativeListener) doesn't make RouterStage#resolveUnambiguousDefault's own
     * same-dialect reserved-name fallback ambiguously hijack the TRANSLATED listener's own
     * statements too. The dual-port native listener's own session pins its statements to this
     * name explicitly (see MySqlWireSessionHandler/MssqlWireSessionHandler's own
     * nativeViaDualPort check) rather than relying on that implicit fallback at all -- so, unlike
     * the single-toggle native mode, a dual-port native session's routing isn't overridable by a
     * router or table-shard rule. A real, deliberate narrowing for this first version:
     * the single-toggle native mode (unaffected by any of this) keeps that flexibility. */
    public static final String MYSQL_NATIVE_DUAL_PORT_NAME = "mysql-native-dual-port";
    public static final String MSSQL_NATIVE_DUAL_PORT_NAME = "mssql-native-dual-port";

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

    // Named, reusable sets of backend names -- possibly spanning multiple engines (a Postgres, an
    // Oracle, a MySQL, a SQL Server, and a MongoDB backend all in one set), unlike shardGroup
    // above (a single, unnamed, homogeneous set used for hash-sharding documents/items/queues).
    // A backend set exists purely to be referenced BY NAME wherever a backend list is otherwise
    // typed out by hand -- today, that's RouterStage#fromConfig's table-shard "backends" field for
    // the hash/consistent strategies (see RouterStage.expandBackendSets). Every member must be a
    // real, currently-registered backend name -- checked at construction time, not lazily at
    // lookup time, so a typo in WARP_BACKEND_SETS fails loudly at startup/reload instead of
    // silently shrinking a shard set the first time a rule referencing it actually runs.
    private volatile Map<String, List<String>> backendSets;

    // A SEPARATE concept from backendSets above, deliberately not reusing that name/grammar --
    // WARP_BACKEND_SETS allows (and existing rules rely on) a backend belonging to MULTIPLE named
    // sets at once (see RouterStageBackendSetExpansionTest: "pair-a=pg,ora" and "pair-b=ora,mysql"
    // sharing "ora" is valid, existing behavior), which is fundamentally incompatible with THIS
    // concept's own invariant -- every backend belongs to EXACTLY ONE group, because the whole
    // point is answering one unambiguous question per backend ("is a table-name collision here
    // expected, or a real conflict") that has no meaning if a backend could answer it two
    // different ways depending on which set you asked about. WARP_BACKEND_GROUPS is that separate,
    // mandatory-partition mechanism; WARP_BACKEND_SETS is untouched, same as before this existed.
    public static final String UNGROUPED_GROUP_NAME = "__ungrouped__";
    private volatile Map<String, Boolean> backendGroupSharded;
    private volatile Map<String, String> backendToGroupName;

    /** {@code name}: the real, declared group name, or {@link #UNGROUPED_GROUP_NAME} for a backend
     * not named as a member of any {@code WARP_BACKEND_GROUPS} entry. {@code sharded}: whether
     * this group is a real, partitioned shard set (a table-name collision among its OWN members is
     * expected, not a conflict -- see {@code BackendCatalogDiscovery}'s own javadoc for the full
     * reasoning) or a plain grouping of independent backends (a table-name collision among its
     * members IS a real conflict). Declared via {@code WARP_BACKEND_GROUPS}' {@code
     * name:sharded=member,...} / {@code name:plain=member,...} grammar -- {@code :plain} is also
     * the default when no qualifier is given, matching this concept's own safe default (assume
     * independence unless told otherwise). The synthetic ungrouped group is always {@code
     * sharded=false}. */
    public record BackendGroupInfo(String name, boolean sharded) {
    }

    // Deliberately NOT reset by reload() -- a backend's drain/down state is an operational fact
    // set by an admin action or a health check, independent of whatever WARP_BACKENDS config
    // happens to be current. A name that disappears from a fresh reload just leaves its state
    // entry orphaned (harmless -- resolveForRouting/stateOf only ever look it up by name, and
    // get(name) already returns null for an unknown name regardless of this map).
    private final Map<String, BackendState> states = new ConcurrentHashMap<>();

    private final BackendTarget defaultTarget;

    // Native-backend-mode targets (mysql-native, mssql-native, ...), registered once at startup
    // from each protocol's own WARP_*_BACKEND env var, not from WARP_BACKENDS -- see Main.java.
    // Kept separate from the env-driven spec so a WARP_BACKENDS reload (reload() below) can't
    // silently drop them: reload() re-merges this same fixed map back in every time, exactly like
    // it re-threads defaultTarget through every rebuild.
    private final Map<String, BackendTarget> staticExtraTargets;

    public BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup) {
        this(targets, shardGroup, null);
    }

    private BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup, BackendTarget defaultTarget) {
        this(targets, shardGroup, defaultTarget, Map.of());
    }

    private BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup, BackendTarget defaultTarget,
            Map<String, BackendTarget> staticExtraTargets) {
        this(targets, shardGroup, defaultTarget, staticExtraTargets, Map.of());
    }

    private BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup, BackendTarget defaultTarget,
            Map<String, BackendTarget> staticExtraTargets, Map<String, List<String>> backendSets) {
        this(targets, shardGroup, defaultTarget, staticExtraTargets, backendSets, Map.of(), Map.of());
    }

    private BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup, BackendTarget defaultTarget,
            Map<String, BackendTarget> staticExtraTargets, Map<String, List<String>> backendSets,
            Map<String, Boolean> backendGroupSharded, Map<String, String> backendToGroupName) {
        this.targets = Map.copyOf(targets);
        this.shardGroup = List.copyOf(shardGroup);
        this.defaultTarget = defaultTarget;
        this.staticExtraTargets = Map.copyOf(staticExtraTargets);
        this.backendSets = Map.copyOf(backendSets);
        this.backendGroupSharded = Map.copyOf(backendGroupSharded);
        // Every backend not already assigned to a declared group belongs to the synthetic
        // ungrouped group instead -- computed here (not left to callers) so every construction
        // path, including the legacy 5-arg constructor above (used by tests/callers that never
        // pass this information explicitly), still gives every backend a real, mandatory group.
        Map<String, String> resolved = new LinkedHashMap<>(backendToGroupName);
        for (String backendName : targets.keySet()) {
            resolved.putIfAbsent(backendName, UNGROUPED_GROUP_NAME);
        }
        this.backendToGroupName = Map.copyOf(resolved);
    }

    public static BackendRegistry fromConfig(String spec, String shardGroupSpec) {
        return fromConfig(spec, shardGroupSpec, null);
    }

    public static BackendRegistry fromConfig(String spec, String shardGroupSpec, BackendTarget defaultTarget) {
        return fromConfig(spec, shardGroupSpec, defaultTarget, Map.of());
    }

    /** As the 3-arg overload, plus {@code staticExtraTargets} -- backends registered outside the
     * WARP_BACKENDS spec entirely (today: native-backend-mode targets like {@code mysql-native}/
     * {@code mssql-native}, one per protocol actually running in native mode). These bypass the
     * WARP_TRUSTED_BACKEND_HOSTS check and the Developer-edition backend-count cap above, same as
     * {@code defaultTarget} always has -- both are the operator's own single configured backend
     * for that protocol, not an operator-supplied list of additional Postgres shards, which is
     * what those two checks exist to gate. */
    public static BackendRegistry fromConfig(String spec, String shardGroupSpec, BackendTarget defaultTarget,
            Map<String, BackendTarget> staticExtraTargets) {
        return fromConfig(spec, shardGroupSpec, null, defaultTarget, staticExtraTargets);
    }

    /** As the 4-arg overload, plus {@code backendSetsSpec} -- {@code WARP_BACKEND_SETS}'s
     * grammar: {@code name=backend1,backend2,...} entries, {@code |}-separated (same delimiter
     * convention {@code WARP_TABLE_SHARDS} uses). Every member must already be a real registered
     * backend name (from {@code spec} or {@code staticExtraTargets}) -- a set can mix engines
     * freely (a Postgres, an Oracle, a MySQL, a SQL Server, and a MongoDB backend in one set),
     * but every one of them must actually exist. */
    public static BackendRegistry fromConfig(String spec, String shardGroupSpec, String backendSetsSpec,
            BackendTarget defaultTarget, Map<String, BackendTarget> staticExtraTargets) {
        return fromConfig(spec, shardGroupSpec, backendSetsSpec, null, defaultTarget, staticExtraTargets);
    }

    /** As the 5-arg overload, plus {@code backendGroupsSpec} -- {@code WARP_BACKEND_GROUPS}' own,
     * separate grammar: {@code name[:sharded|:plain]=backend1,backend2,...} entries,
     * {@code |}-separated. See {@link #UNGROUPED_GROUP_NAME}'s own field javadoc for why this is a
     * deliberately different mechanism from {@code backendSetsSpec}, not an extension of it. */
    public static BackendRegistry fromConfig(String spec, String shardGroupSpec, String backendSetsSpec,
            String backendGroupsSpec, BackendTarget defaultTarget, Map<String, BackendTarget> staticExtraTargets) {

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
                // License-tier backend cap (see com.sayonora.wire.license.License#maxBackends) --
                // checked against targets.size() BEFORE this entry, not after, so a spec with
                // exactly the cap's worth of backends still registers all of them; the first entry
                // past the cap is what gets skipped, same "skip this one entry, not fatal" pattern
                // as the trusted-host check just above.
                int maxBackends = com.sayonora.wire.license.License.current().maxBackends();
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
        targets.putAll(staticExtraTargets);
        List<String> shardGroup = shardGroupSpec == null || shardGroupSpec.isBlank()
                ? List.of()
                : List.of(shardGroupSpec.split(",")).stream().map(String::trim).toList();
        Map<String, List<String>> backendSets = parseBackendSets(backendSetsSpec, targets.keySet());
        ParsedBackendGroups parsedGroups = parseBackendGroups(backendGroupsSpec, targets.keySet());
        return new BackendRegistry(targets, shardGroup, defaultTarget, staticExtraTargets,
                backendSets, parsedGroups.sharded(), parsedGroups.backendToGroupName());
    }

    private static Map<String, List<String>> parseBackendSets(String spec, java.util.Set<String> registeredNames) {
        if (spec == null || spec.isBlank()) {
            return Map.of();
        }
        Map<String, List<String>> sets = new LinkedHashMap<>();
        for (String entry : spec.split("\\|")) {
            if (entry.isBlank()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("WARP_BACKEND_SETS entry \"" + entry
                        + "\" is missing \"=\" -- expected setName=backend1,backend2,...");
            }
            String name = entry.substring(0, eq).trim();
            if (name.isEmpty() || name.indexOf(',') >= 0 || name.indexOf('|') >= 0) {
                throw new IllegalArgumentException("WARP_BACKEND_SETS set name \"" + name
                        + "\" must be non-empty and contain neither \",\" nor \"|\"");
            }
            if (registeredNames.contains(name)) {
                throw new IllegalArgumentException("WARP_BACKEND_SETS set name \"" + name
                        + "\" collides with a real backend name -- a rule referencing \"" + name
                        + "\" would be ambiguous between the backend and the set");
            }
            List<String> members = List.of(entry.substring(eq + 1).split(",")).stream()
                    .map(String::trim).filter(m -> !m.isEmpty()).toList();
            if (members.isEmpty()) {
                throw new IllegalArgumentException("WARP_BACKEND_SETS set \"" + name + "\" has no members");
            }
            for (String member : members) {
                if (!registeredNames.contains(member)) {
                    throw new IllegalArgumentException("WARP_BACKEND_SETS set \"" + name
                            + "\" names \"" + member + "\", which is not a registered backend ("
                            + String.join(", ", registeredNames) + ")");
                }
            }
            sets.put(name, members);
        }
        return Map.copyOf(sets);
    }

    /** Holds one {@code WARP_BACKEND_GROUPS} parse's results -- {@code sharded} (group name ->
     * whether it's a real shard set) and {@code backendToGroupName} (backend name -> its one and
     * only group). See {@link #UNGROUPED_GROUP_NAME}'s own field javadoc for why this is a
     * separate mechanism from {@link #parseBackendSets}, not a variant of it. */
    private record ParsedBackendGroups(Map<String, Boolean> sharded, Map<String, String> backendToGroupName) {
    }

    private static ParsedBackendGroups parseBackendGroups(String spec, java.util.Set<String> registeredNames) {
        if (spec == null || spec.isBlank()) {
            return new ParsedBackendGroups(Map.of(), Map.of());
        }
        Map<String, Boolean> sharded = new LinkedHashMap<>();
        Map<String, String> backendToGroupName = new LinkedHashMap<>();
        for (String entry : spec.split("\\|")) {
            if (entry.isBlank()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("WARP_BACKEND_GROUPS entry \"" + entry
                        + "\" is missing \"=\" -- expected groupName[:sharded|:plain]=backend1,backend2,...");
            }
            String rawName = entry.substring(0, eq).trim();
            // Optional ":sharded"/":plain" qualifier -- see BackendGroupInfo's own javadoc for the
            // real semantic difference. ":plain" is also the implicit default (no qualifier at
            // all) -- "assume independence unless told otherwise" is the safer default for a
            // table-name-collision check than "assume it's fine."
            String name = rawName;
            boolean isSharded = false;
            int colon = rawName.indexOf(':');
            if (colon >= 0) {
                name = rawName.substring(0, colon).trim();
                String qualifier = rawName.substring(colon + 1).trim().toLowerCase(java.util.Locale.ROOT);
                if (qualifier.equals("sharded")) {
                    isSharded = true;
                } else if (!qualifier.equals("plain")) {
                    throw new IllegalArgumentException("WARP_BACKEND_GROUPS group \"" + name + "\" has unknown "
                            + "qualifier \"" + qualifier + "\" -- expected \"sharded\" or \"plain\"");
                }
            }
            if (name.isEmpty() || name.indexOf(',') >= 0 || name.indexOf('|') >= 0) {
                throw new IllegalArgumentException("WARP_BACKEND_GROUPS group name \"" + name
                        + "\" must be non-empty and contain neither \",\" nor \"|\"");
            }
            if (name.equals(UNGROUPED_GROUP_NAME)) {
                throw new IllegalArgumentException("WARP_BACKEND_GROUPS group name \"" + name
                        + "\" is reserved for backends not assigned to any declared group");
            }
            List<String> members = List.of(entry.substring(eq + 1).split(",")).stream()
                    .map(String::trim).filter(m -> !m.isEmpty()).toList();
            if (members.isEmpty()) {
                throw new IllegalArgumentException("WARP_BACKEND_GROUPS group \"" + name + "\" has no members");
            }
            for (String member : members) {
                if (!registeredNames.contains(member)) {
                    throw new IllegalArgumentException("WARP_BACKEND_GROUPS group \"" + name
                            + "\" names \"" + member + "\", which is not a registered backend ("
                            + String.join(", ", registeredNames) + ")");
                }
                // Every backend belongs to exactly ONE group -- unlike WARP_BACKEND_SETS, this
                // concept's whole purpose (deciding whether a table-name collision at this backend
                // is expected or a real conflict) requires exactly one unambiguous answer per
                // backend, not silently picking whichever declaration happened to come first.
                String existingGroup = backendToGroupName.get(member);
                if (existingGroup != null) {
                    throw new IllegalArgumentException("WARP_BACKEND_GROUPS backend \"" + member
                            + "\" is a member of both \"" + existingGroup + "\" and \"" + name
                            + "\" -- every backend must belong to exactly one group");
                }
                backendToGroupName.put(member, name);
            }
            sharded.put(name, isSharded);
        }
        return new ParsedBackendGroups(Map.copyOf(sharded), Map.copyOf(backendToGroupName));
    }

    public void reload(String spec, String shardGroupSpec, String backendSetsSpec) {
        reload(spec, shardGroupSpec, backendSetsSpec, null);
    }

    public void reload(String spec, String shardGroupSpec, String backendSetsSpec, String backendGroupsSpec) {
        BackendRegistry fresh = fromConfig(spec, shardGroupSpec, backendSetsSpec, backendGroupsSpec,
                this.defaultTarget, this.staticExtraTargets);
        this.targets = fresh.targets;
        this.shardGroup = fresh.shardGroup;
        this.backendSets = fresh.backendSets;
        this.backendGroupSharded = fresh.backendGroupSharded;
        this.backendToGroupName = fresh.backendToGroupName;
    }

    /** Every backend's mandatory group membership -- see {@link BackendGroupInfo}'s own javadoc.
     * Never {@code null}: a backend not named in any declared {@code WARP_BACKEND_GROUPS} entry
     * still resolves here, to the synthetic {@link #UNGROUPED_GROUP_NAME} group (plain). Returns
     * {@code null} only for a name that isn't a registered backend at all (same "unknown name"
     * contract as {@link #get}). */
    public BackendGroupInfo groupInfoFor(String backendName) {
        if (!targets.containsKey(backendName)) {
            return null;
        }
        String groupName = backendToGroupName.getOrDefault(backendName, UNGROUPED_GROUP_NAME);
        boolean sharded = backendGroupSharded.getOrDefault(groupName, false);
        return new BackendGroupInfo(groupName, sharded);
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

    /** Named backend sets from {@code WARP_BACKEND_SETS} -- see {@link #parseBackendSets}.
     * Empty (not null) when none are configured. */
    public Map<String, List<String>> backendSets() {
        return backendSets;
    }

    public java.util.Collection<BackendTarget> all() {
        return targets.values();
    }
}
