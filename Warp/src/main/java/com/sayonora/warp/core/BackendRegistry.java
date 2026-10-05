package com.sayonora.warp.core;

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
     * (WARP_MYWIRE_BACKEND=mysql / WARP_MSSQLWIRE_BACKEND=sqlserver / WARP_ORACLE_BACKEND_MODE=relay)
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

    /** The MCP gateway's own native-backend-mode target ({@code WARP_MCP_BACKEND=oracle|mysql|
     * sqlserver}), registered by {@code Main.java} only in that mode, under its OWN reserved name
     * (never one of the per-protocol names above -- those are what {@link RouterStage}'s own
     * same-dialect fallback resolves a mywire/mssqlwire/orawire native session to, and MCP must
     * not become an ambiguous second claimant). {@code WarpMcpServer#runSql} pins every MCP
     * statement to this name explicitly (like the dual-port names, not the fallback), with the
     * Statement's {@code sourceDialect} set to the target's real dialect so
     * {@code DialectTranslationStage} no-ops, and {@code RoutingBackendExecutor} treats this name
     * as "the caller-supplied connection" -- which is how the shared pipeline (firewall, QoS,
     * capture, stats, repair) now governs MCP native-mode traffic that used to bypass it entirely.
     * Excluded from catalog auto-discovery like every other reserved name (see
     * {@code BackendCatalogDiscovery}). */
    public static final String MCP_NATIVE_DEFAULT_NAME = "mcp-native";

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

    // Read replicas per primary backend name (see ReplicaSpec). Replaced wholesale by reload().
    private volatile Map<String, List<ReplicaSpec>> replicaSpecs = Map.of();
    private final ReplicaRouter replicaRouter = new ReplicaRouter(this);

    public static final java.util.Set<String> FAILOVER_MODES = java.util.Set.of("follow", "promote", "off");

    // Failover mode per backend (6th WARP_BACKENDS field); absent = default (follow when replicas exist).
    private volatile Map<String, String> failoverModes = Map.of();

    /** Effective failover mode for {@code name}: "off" when it has no replicas or was set to off,
     * otherwise the configured "follow" (default) or "promote". */
    public String failoverModeOf(String name) {
        if (replicaSpecsOf(name).isEmpty()) {
            return "off";
        }
        return failoverModes.getOrDefault(name, "follow");
    }

    /** A promotion this process applied in memory that the shared config does not (yet) reflect --
     * kept so a reload from a config that still names the old primary does not silently revert it. */
    private record FailoverOverride(String expectedOldUrl, String newUrl, List<ReplicaSpec> newReplicas) {
    }

    private final Map<String, FailoverOverride> failoverOverrides = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile FailoverMonitor failoverMonitor;

    public FailoverMonitor failoverMonitor() {
        return failoverMonitor;
    }

    public void setFailoverMonitor(FailoverMonitor monitor) {
        this.failoverMonitor = monitor;
    }

    /**
     * Repoints backend {@code name} at {@code newUrl} (and replaces its replica list) in this
     * process only, compare-and-set on the primary URL it is expected to currently have. Returns
     * false -- changing nothing -- when the backend is unknown or already points elsewhere (someone,
     * possibly this process via a config reload, already moved it). The change is remembered as an
     * override so a later {@link #reload} from a config that has not caught up cannot undo it; the
     * override disappears once the config itself names {@code newUrl}.
     */
    public synchronized boolean applyFailoverLocally(String name, String expectedOldUrl, String newUrl,
            List<ReplicaSpec> newReplicas) {
        BackendTarget t = targets.get(name);
        if (t == null || !t.jdbcUrl().equals(expectedOldUrl)) {
            return false;
        }
        failoverOverrides.put(name, new FailoverOverride(expectedOldUrl, newUrl, List.copyOf(newReplicas)));
        swapPrimary(this, name, newUrl, newReplicas);
        touch();
        return true;
    }

    private static void swapPrimary(BackendRegistry r, String name, String newUrl, List<ReplicaSpec> newReplicas) {
        BackendTarget t = r.targets.get(name);
        Map<String, BackendTarget> copy = new LinkedHashMap<>(r.targets);
        copy.put(name, new BackendTarget(name, newUrl, t.user(), t.password(), t.failoverOptions(),
                t.fallbackName(), t.connectorOperand()));
        r.targets = Map.copyOf(copy);
        Map<String, List<ReplicaSpec>> specs = new LinkedHashMap<>(r.replicaSpecs);
        if (newReplicas.isEmpty()) {
            specs.remove(name);
        } else {
            specs.put(name, List.copyOf(newReplicas));
        }
        r.replicaSpecs = Map.copyOf(specs);
    }

    private synchronized void reapplyFailoverOverrides() {
        for (var it = failoverOverrides.entrySet().iterator(); it.hasNext();) {
            var e = it.next();
            BackendTarget t = targets.get(e.getKey());
            FailoverOverride o = e.getValue();
            if (t == null || t.jdbcUrl().equals(o.newUrl())) {
                it.remove(); // config caught up (or the backend is gone)
            } else if (t.jdbcUrl().equals(o.expectedOldUrl())) {
                swapPrimary(this, e.getKey(), o.newUrl(), o.newReplicas());
            } else {
                it.remove(); // config now names some third node: the config wins
            }
        }
    }

    /** Replicas configured for {@code primaryName}; empty when none. */
    public List<ReplicaSpec> replicaSpecsOf(String primaryName) {
        List<ReplicaSpec> r = replicaSpecs.get(primaryName);
        return r == null ? List.of() : r;
    }

    public Map<String, List<ReplicaSpec>> allReplicaSpecs() {
        return replicaSpecs;
    }

    /** The lag-gated read-replica chooser for this registry's backends. */
    public ReplicaRouter replicaRouter() {
        return replicaRouter;
    }

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
        this.declarationOrder = List.copyOf(targets.keySet());
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
        Map<String, List<ReplicaSpec>> replicaSpecs = new LinkedHashMap<>();
        Map<String, String> failoverModes = new LinkedHashMap<>();
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
                // Optional 5th field: read replicas of this backend (see ReplicaSpec). They share
                // this backend's credentials and are NOT registered backends, so they neither count
                // against the license cap nor become routable targets. A malformed field drops that
                // entry's replicas with a warning rather than failing the whole backend spec.
                List<ReplicaSpec> entryReplicas = List.of();
                if (parts.length > 4 && !parts[4].isBlank()) {
                    try {
                        entryReplicas = ReplicaSpec.parseList(parts[4]);
                    } catch (IllegalArgumentException e) {
                        log.warn("backend registry: ignoring replicas of '{}': {}", name, e.getMessage());
                    }
                }
                // Optional 6th field: failover mode for this backend's replica group -- "follow"
                // (Warp follows a promotion made by the database's own HA tooling) or "off". Blank
                // means follow when replicas exist. Unknown values are ignored with a warning.
                if (parts.length > 5 && !parts[5].isBlank()) {
                    String mode = parts[5].trim().toLowerCase(java.util.Locale.ROOT);
                    if (FAILOVER_MODES.contains(mode)) {
                        failoverModes.put(name, mode);
                    } else {
                        log.warn("backend registry: ignoring unknown failover mode '{}' for '{}' (expected {})",
                                parts[5].trim(), name, FAILOVER_MODES);
                    }
                }
                if (!trustedHosts.isTrusted(url)) {
                    log.warn("backend registry: REFUSING to register backend '{}' ({}) -- its host is not in "
                            + "WARP_TRUSTED_BACKEND_HOSTS. This entry is skipped, not fatal; every other "
                            + "configured backend is unaffected.", name, url);
                    continue;
                }
                // License-tier backend cap (see com.sayonora.warp.license.License#maxBackends) --
                // checked against targets.size() BEFORE this entry, not after, so a spec with
                // exactly the cap's worth of backends still registers all of them; the first entry
                // past the cap is what gets skipped, same "skip this one entry, not fatal" pattern
                // as the trusted-host check just above.
                int maxBackends = com.sayonora.warp.license.License.current().maxBackends();
                if (targets.size() >= maxBackends && !targets.containsKey(name)) {
                    // Counts EVERY engine, DynamoDB/MongoDB connector backends included -- a
                    // federated source is a backend like any other for licensing purposes.
                    log.warn("license: REFUSING to register backend '{}' -- Developer edition is capped at {} "
                            + "backends of any engine (see the Pricing section of the docs for Enterprise, which has "
                            + "no backend limit). This entry is skipped, not fatal; every other configured "
                            + "backend up to the cap is unaffected.", name, maxBackends);
                    continue;
                }
                // DynamoDB/MongoDB connector backends: parse the pseudo-URL's table/field config
                // into the operand map SchemaFederationStage hands the connector at mount time (see
                // ConnectorOperands for the grammar). Credentials stay unresolved references here.
                BackendTarget probe = new BackendTarget(name, url, user, password);
                java.util.Map<String, Object> connectorOperand = probe.isFederationOnlyConnector()
                        ? com.sayonora.warp.core.connector.ConnectorOperands.parse(url, user, password) : null;
                targets.put(name, new BackendTarget(name, url, user, password, null, fallbackName, connectorOperand));
                if (!entryReplicas.isEmpty()) {
                    boolean allTrusted = true;
                    for (ReplicaSpec r : entryReplicas) {
                        if (!trustedHosts.isTrusted(r.url())) {
                            log.warn("backend registry: REFUSING replica of '{}' ({}) -- its host is not in "
                                    + "WARP_TRUSTED_BACKEND_HOSTS.", name, r.url());
                            allTrusted = false;
                        }
                    }
                    if (allTrusted) {
                        replicaSpecs.put(name, entryReplicas);
                    } else {
                        List<ReplicaSpec> kept = new java.util.ArrayList<>();
                        for (ReplicaSpec r : entryReplicas) {
                            if (trustedHosts.isTrusted(r.url())) {
                                kept.add(r);
                            }
                        }
                        if (!kept.isEmpty()) {
                            replicaSpecs.put(name, List.copyOf(kept));
                        }
                    }
                }
            }
        } else if (defaultTarget != null) {
            targets.put(DEFAULT_BACKEND_NAME, defaultTarget);
            // WARP_REPLICAS: read replicas of the implicit single backend, same grammar as the 5th
            // WARP_BACKENDS field (url[~maxLagSeconds][^url...]). Failover is always off for it: the
            // implicit backend is not a WARP_BACKENDS entry, so there is no config to switch.
            String defaultReplicas = System.getenv("WARP_REPLICAS");
            if (defaultReplicas != null && !defaultReplicas.isBlank()) {
                try {
                    List<ReplicaSpec> kept = new java.util.ArrayList<>();
                    for (ReplicaSpec r : ReplicaSpec.parseList(defaultReplicas)) {
                        if (trustedHosts.isTrusted(r.url())) {
                            kept.add(r);
                        } else {
                            log.warn("backend registry: REFUSING WARP_REPLICAS entry {} -- its host is not in "
                                    + "WARP_TRUSTED_BACKEND_HOSTS.", r.url());
                        }
                    }
                    if (!kept.isEmpty()) {
                        replicaSpecs.put(DEFAULT_BACKEND_NAME, List.copyOf(kept));
                        failoverModes.put(DEFAULT_BACKEND_NAME, "off");
                    }
                } catch (IllegalArgumentException e) {
                    log.warn("backend registry: ignoring WARP_REPLICAS: {}", e.getMessage());
                }
            }
            log.info("backend registry: no WARP_BACKENDS configured -- registered the single "
                    + "implicit WARP_* backend as '{}' so routing/translation has a fallback target",
                    DEFAULT_BACKEND_NAME);
        }
        targets.putAll(staticExtraTargets);
        List<String> declarationOrder = List.copyOf(targets.keySet());
        List<String> shardGroup = shardGroupSpec == null || shardGroupSpec.isBlank()
                ? List.of()
                : List.of(shardGroupSpec.split(",")).stream().map(String::trim).toList();
        Map<String, List<String>> backendSets = parseBackendSets(backendSetsSpec, targets.keySet());
        ParsedBackendGroups parsedGroups = parseBackendGroups(backendGroupsSpec, targets.keySet());
        BackendRegistry built = new BackendRegistry(targets, shardGroup, defaultTarget, staticExtraTargets,
                backendSets, parsedGroups.sharded(), parsedGroups.backendToGroupName());
        built.declarationOrder = declarationOrder;
        built.replicaSpecs = Map.copyOf(replicaSpecs);
        built.failoverModes = Map.copyOf(failoverModes);
        return built;
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
        this.declarationOrder = fresh.declarationOrder;
        this.failoverModes = fresh.failoverModes;
        this.replicaSpecs = fresh.replicaSpecs;
        reapplyFailoverOverrides();
        this.hostsCache = new java.util.concurrent.ConcurrentHashMap<>();
        touch();
    }

    // Operator-supplied, human-readable descriptions (WARP_BACKEND_DESCRIPTIONS /
    // WARP_BACKEND_GROUP_DESCRIPTIONS -- a JSON object of name -> text, persisted in warp_config
    // like every other backend setting). They surface in the MCP list_backends/describe_backend
    // tools and the admin GET /api/backends. Deliberately NOT part of the reload() parse: a
    // description never affects routing, so a malformed value must never fail a backend reload.
    private volatile Map<String, String> backendDescriptions = Map.of();
    private volatile Map<String, String> groupDescriptions = Map.of();

    /** Replaces both description maps from their JSON-object config strings (null/blank = none).
     * A malformed JSON value is logged and treated as empty, never thrown. */
    public void applyDescriptions(String backendDescriptionsJson, String groupDescriptionsJson) {
        this.backendDescriptions = parseDescriptionMap("WARP_BACKEND_DESCRIPTIONS", backendDescriptionsJson);
        this.groupDescriptions = parseDescriptionMap("WARP_BACKEND_GROUP_DESCRIPTIONS", groupDescriptionsJson);
    }

    static Map<String, String> parseDescriptionMap(String label, String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            com.google.gson.JsonObject o = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
            Map<String, String> out = new LinkedHashMap<>();
            for (Map.Entry<String, com.google.gson.JsonElement> e : o.entrySet()) {
                if (!e.getValue().isJsonNull()) {
                    out.put(e.getKey(), e.getValue().getAsString());
                }
            }
            return Map.copyOf(out);
        } catch (RuntimeException e) {
            log.warn("{} is not a valid JSON object of name -> description ({}); ignoring it", label, e.toString());
            return Map.of();
        }
    }

    /** The operator's description of backend {@code name}, or {@code null}. */
    public String descriptionOf(String name) {
        return backendDescriptions.get(name);
    }

    /** The operator's description of backend group/set {@code groupName}, or {@code null}. */
    public String groupDescriptionOf(String groupName) {
        return groupDescriptions.get(groupName);
    }

    /** Names of the {@code WARP_BACKEND_SETS} sets that contain {@code backendName}. */
    // ---- Backend sets (the one user-facing grouping concept) and enabled stores ---------------
    //
    // User-facing "backend set" == the mandatory-partition WARP_BACKEND_GROUPS concept: every
    // backend belongs to exactly one set. A backend with no declared group lives in the implicit
    // set named DEFAULT_SET_NAME, so existing configs need no migration. The older multi-membership
    // WARP_BACKEND_SETS is unchanged and remains what router rules can reference by name; it is
    // an advanced router alias and not a "backend set" in the user-facing sense (see docs).

    public static final String DEFAULT_SET_NAME = "default";

    // Bumped after every mutation of the backend/set model (reload, applyStoreConfig) and of the
    // connect-time route table, so ConnectionRouter's immutable lookup snapshot rebuilds lazily.
    private final java.util.concurrent.atomic.AtomicLong generation = new java.util.concurrent.atomic.AtomicLong();
    private final ConnectionRouter connectionRouter = new ConnectionRouter(this, ConnectionRouter.modeFromEnv());

    public long generation() {
        return generation.get();
    }

    void touch() {
        generation.incrementAndGet();
    }

    /** Connect-time routing (database/service name -> backend or set); see {@link ConnectionRouter}. */
    public ConnectionRouter connectionRouter() {
        return connectionRouter;
    }

    private volatile List<String> declarationOrder = List.of();
    private volatile Map<String, List<StoreType>> enabledStores = Map.of();
    private volatile List<String> declaredSetNames = List.of();
    private volatile Map<StoreType, String> storeFrontendSets = Map.of();

    /** Backend names in the order they were declared in WARP_BACKENDS (implicit default first). */
    public List<String> orderedNames() {
        return declarationOrder;
    }

    /**
     * Applies the enabled-store and declared-set configuration ({@code warp_config.backendStores},
     * {@code backendSetNames}). Grammar: stores {@code backend=store1,store2|backend2=store3};
     * set names {@code a,b,c}. Never throws -- a malformed entry is logged and skipped so a bad
     * value cannot break a config reload (the admin API validates strictly before writing).
     */
    public void applyStoreConfig(String storesSpec, String setNamesSpec) {
        this.enabledStores = parseStoreSpec(storesSpec);
        List<String> names = new ArrayList<>();
        if (setNamesSpec != null) {
            for (String n : setNamesSpec.split(",")) {
                if (!n.isBlank() && !names.contains(n.trim())) {
                    names.add(n.trim());
                }
            }
        }
        this.declaredSetNames = List.copyOf(names);
        this.hostsCache = new java.util.concurrent.ConcurrentHashMap<>();
        touch();
    }

    public static Map<String, List<StoreType>> parseStoreSpec(String spec) {
        Map<String, List<StoreType>> out = new LinkedHashMap<>();
        if (spec == null || spec.isBlank()) {
            return out;
        }
        for (String entry : spec.split("\\|")) {
            int eq = entry.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String backend = entry.substring(0, eq).trim();
            List<StoreType> stores = new ArrayList<>();
            for (String s : entry.substring(eq + 1).split(",")) {
                if (s.isBlank()) {
                    continue;
                }
                try {
                    StoreType t = StoreType.parse(s);
                    if (!stores.contains(t)) {
                        stores.add(t);
                    }
                } catch (IllegalArgumentException e) {
                    log.warn("backend registry: ignoring unknown store \"{}\" for backend '{}'", s, backend);
                }
            }
            if (!stores.isEmpty()) {
                out.put(backend, List.copyOf(stores));
            }
        }
        return out;
    }

    /**
     * Applies the admin-settable per-store serving-set assignment ({@code warp_config.
     * storeFrontendSets}) -- the UI/API alternative to a protocol's {@code WARP_<PROTO>WIRE_SET}
     * env var (see {@link #frontendSet}). Grammar: {@code store1=set1|store2=set2}, store ids as
     * {@link StoreType#id()}. Never throws -- a malformed entry is logged and skipped, same
     * contract as {@link #applyStoreConfig}, so a bad value cannot break a config reload.
     */
    public void applyStoreFrontendSets(String spec) {
        this.storeFrontendSets = parseStoreFrontendSets(spec);
        this.hostsCache = new java.util.concurrent.ConcurrentHashMap<>();
        touch();
    }

    public static Map<StoreType, String> parseStoreFrontendSets(String spec) {
        Map<StoreType, String> out = new LinkedHashMap<>();
        if (spec == null || spec.isBlank()) {
            return out;
        }
        for (String entry : spec.split("\\|")) {
            if (entry.isBlank()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq <= 0) {
                log.warn("backend registry: ignoring malformed storeFrontendSets entry \"{}\"", entry);
                continue;
            }
            String storeId = entry.substring(0, eq).trim();
            String setName = entry.substring(eq + 1).trim();
            if (setName.isEmpty()) {
                continue;
            }
            try {
                out.put(StoreType.parse(storeId), setName);
            } catch (IllegalArgumentException e) {
                log.warn("backend registry: ignoring storeFrontendSets entry for unknown store \"{}\"", storeId);
            }
        }
        return Map.copyOf(out);
    }

    public static String renderStoreFrontendSets(Map<StoreType, String> assignments) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<StoreType, String> e : assignments.entrySet()) {
            if (e.getValue() == null || e.getValue().isBlank()) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(e.getKey().id()).append('=').append(e.getValue());
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /** The admin-persisted serving set for {@code store}, or {@code null} if none is assigned. */
    public String storeFrontendSetOverride(StoreType store) {
        return storeFrontendSets.get(store);
    }

    public static String renderStoreSpec(Map<String, List<StoreType>> stores) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<StoreType>> e : stores.entrySet()) {
            if (e.getValue().isEmpty()) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(e.getKey()).append('=');
            for (int i = 0; i < e.getValue().size(); i++) {
                sb.append(i == 0 ? "" : ",").append(e.getValue().get(i).id());
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /** The backend set {@code backendName} belongs to, or {@code null} for an unknown backend. */
    public String setOf(String backendName) {
        BackendGroupInfo info = groupInfoFor(backendName);
        if (info == null) {
            return null;
        }
        return UNGROUPED_GROUP_NAME.equals(info.name()) ? DEFAULT_SET_NAME : info.name();
    }

    /** Member backends of a set, in declaration order, excluding Warp's own reserved native targets. */
    public List<String> membersOfSet(String setName) {
        List<String> out = new ArrayList<>();
        for (String name : declarationOrder) {
            if (targets.containsKey(name) && !BackendCatalogDiscovery.isReservedNativeName(name)
                    && setName.equals(setOf(name))) {
                out.add(name);
            }
        }
        return out;
    }

    /** All set names: the implicit "default" set first (when it has members or is declared), then
     * declared and group-derived sets in order. */
    public List<String> setNames() {
        java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
        boolean defaultUsed = declaredSetNames.contains(DEFAULT_SET_NAME);
        for (String n : declarationOrder) {
            if (targets.containsKey(n) && !BackendCatalogDiscovery.isReservedNativeName(n)
                    && DEFAULT_SET_NAME.equals(setOf(n))) {
                defaultUsed = true;
            }
        }
        if (defaultUsed) {
            names.add(DEFAULT_SET_NAME);
        }
        names.addAll(declaredSetNames);
        names.addAll(backendGroupSharded.keySet());
        return List.copyOf(names);
    }

    public List<StoreType> enabledStores(String backendName) {
        return enabledStores.getOrDefault(backendName, List.of());
    }

    /**
     * The set a protocol frontend serves, in priority order: the admin-persisted assignment
     * ({@code warp_config.storeFrontendSets}, settable in the UI/API -- see
     * {@link #applyStoreFrontendSets}), else its {@code WARP_<PROTO>_SET} env var (kept for
     * existing env-var-only deployments), when either names an existing set; otherwise the set
     * holding the {@code default} backend (or the first set).
     */
    public String frontendSet(StoreType store) {
        List<String> sets = setNames();
        String persisted = storeFrontendSets.get(store);
        if (persisted != null && !persisted.isBlank()) {
            if (sets.contains(persisted.trim())) {
                return persisted.trim();
            }
            log.warn("backend registry: storeFrontendSets assigns {} to '{}', not an existing backend set ({}); "
                    + "falling back", store.id(), persisted, sets);
        }
        String configured = System.getenv(store.setEnvVar());
        if (configured != null && !configured.isBlank()) {
            if (sets.contains(configured.trim())) {
                return configured.trim();
            }
            log.warn("backend registry: {}='{}' is not an existing backend set ({}); using the default set",
                    store.setEnvVar(), configured, sets);
        }
        String defaultBackendSet = setOf(DEFAULT_BACKEND_NAME);
        if (defaultBackendSet != null) {
            return defaultBackendSet;
        }
        return sets.isEmpty() ? null : sets.get(0);
    }

    /**
     * The backends HOSTING {@code store}: Postgres backends of the frontend's set that enable it,
     * in declaration order (the hash order -- stable as long as the host list is unchanged).
     * Empty when the store is enabled nowhere, in which case frontends keep their legacy behavior
     * (WARP_SHARD_BACKENDS group where that existed, otherwise the {@code default} backend).
     */
    public List<String> storeHosts(StoreType store) {
        if (enabledStores.isEmpty()) {
            return List.of();
        }
        // hot path (every DynamoDB/SQS/... call): computed once per config version, dropped on reload
        java.util.concurrent.ConcurrentHashMap<StoreType, List<String>> cache = hostsCache;
        List<String> cached = cache.get(store);
        if (cached != null) {
            return cached;
        }
        List<String> computed = computeStoreHosts(store);
        cache.put(store, computed);
        return computed;
    }

    private volatile java.util.concurrent.ConcurrentHashMap<StoreType, List<String>> hostsCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    private List<String> computeStoreHosts(StoreType store) {
        String set = frontendSet(store);
        if (set == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String name : membersOfSet(set)) {
            BackendTarget t = targets.get(name);
            if (t != null && t.dialect() == SourceDialect.POSTGRES && enabledStores(name).contains(store)) {
                out.add(name);
            }
        }
        return List.copyOf(out);
    }

    /** {@link #storeHosts} when the store is enabled somewhere, else the legacy {@link #shardGroup()}. */
    public List<String> storeShardGroup(StoreType store) {
        List<String> hosts = storeHosts(store);
        return hosts.isEmpty() ? shardGroup : hosts;
    }

    /** First host of an enabled store (home of its catalog), or {@code null} when not enabled. */
    public String storeHome(StoreType store) {
        List<String> hosts = storeHosts(store);
        return hosts.isEmpty() ? null : hosts.get(0);
    }

    /** Every (backend, stores) pair currently enabled on a registered Postgres backend. */
    public Map<String, List<StoreType>> allEnabledStores() {
        Map<String, List<StoreType>> out = new LinkedHashMap<>();
        for (String name : declarationOrder) {
            BackendTarget t = targets.get(name);
            List<StoreType> s = enabledStores.get(name);
            if (t != null && s != null && !s.isEmpty() && t.dialect() == SourceDialect.POSTGRES) {
                out.put(name, s);
            }
        }
        return out;
    }

    public List<String> backendSetsContaining(String backendName) {
        List<String> out = new ArrayList<>();
        backendSets.forEach((set, members) -> {
            if (members.contains(backendName)) {
                out.add(set);
            }
        });
        return out;
    }

    /** Names of every declared {@code WARP_BACKEND_GROUPS} group (excludes the synthetic ungrouped one). */
    public List<String> groupNames() {
        return List.copyOf(backendGroupSharded.keySet());
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

    /** Every registered backend whose {@link #groupInfoFor} group is {@code groupName} -- the
     * same membership walk {@code WarpMcpServer}'s scoped discovery does, exposed so a GROUP
     * {@code McpScope} can be turned into a concrete {@link BackendScope}. Empty (never null)
     * for an unknown group name. Registration order. */
    public List<String> membersOfGroup(String groupName) {
        List<String> members = new ArrayList<>();
        for (BackendTarget target : targets.values()) {
            BackendGroupInfo info = groupInfoFor(target.name());
            if (info != null && info.name().equals(groupName)) {
                members.add(target.name());
            }
        }
        return List.copyOf(members);
    }
}
