package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real schema auto-discovery across every backend {@link BackendRegistry} knows about --
 * {@code query_federated}'s answer to a real gap found live: {@link SchemaFederationStage} (and
 * the wire-protocol frontends' own routing) can only federate across backends an OPERATOR
 * pre-declared via {@code WARP_ROUTER_SCHEMA_RULES}, naming which schema alias lives on which
 * backend ahead of time. That's the right model for a real Postgres/MySQL/SQL Server client, which
 * hands Warp nothing but raw SQL text with no other signal -- a config-declared rule is genuinely
 * the only thing to route on. It's the WRONG model for MCP/NL-to-SQL: an agent (or the LLM drafting
 * SQL for {@code query_natural_language}) has no reason to know an operator's schema-alias naming
 * scheme, and shouldn't have to -- Warp already holds every backend's real connection info in
 * {@link BackendRegistry#all()}, so it can just ask each one what tables it actually has.
 *
 * <p>Scope, deliberately narrow for a first real implementation: introspects each backend's own
 * DEFAULT visible schema (whatever {@link DatabaseMetaData#getTables} returns with a {@code null}
 * schema pattern -- the connecting user's own search_path/default schema, "public" for a typical
 * Postgres backend), ordinary {@code TABLE}s only (no views). A backend whose real data lives
 * outside its default schema, or where discovery needs to span multiple schemas per backend, isn't
 * covered here -- a real, disclosed limitation rather than a silent one, matching {@code
 * WARP_ROUTER_SCHEMA_RULES}' own equally real limitation (an operator has to know and declare the
 * schema name up front, same trade-off in the other direction).
 *
 * <p><b>Conflict detection is {@code WARP_BACKEND_GROUPS}-aware, not a flat "any collision is
 * ambiguous" check</b> -- a real gap raised directly: sharding shouldn't require the same
 * ceremony as federation. A table found on multiple backends within the SAME {@code sharded}
 * group ({@link BackendRegistry.BackendGroupInfo#sharded()}) is never a conflict, regardless of
 * how many members have it -- that's either the normal partitioned-fact-table shape (routing
 * already goes through the shard key, via {@code WARP_TABLE_SHARDS}/{@code ShardJoinExecutor},
 * not this class) or an intentionally-replicated dimension table for local joins; either way, a
 * query against it is never ambiguous about which backend to use because it's never resolved that
 * way at all. A table found on multiple backends within the same PLAIN group, or across two
 * DIFFERENT groups (sharded or not), IS a real conflict -- see {@link #resolveUnambiguous}.
 */
public final class BackendCatalogDiscovery {

    private static final Logger log = LoggerFactory.getLogger(BackendCatalogDiscovery.class);

    public record DiscoveredTable(String tableName, String backendName, String realSchemaName) {
    }

    // Reserved native-backend-mode targets are protocol-specific passthrough connections -- each
    // one is the SOLE backend a given protocol's OWN native-mode session ever resolves to
    // (RouterStage's own same-dialect reserved-name fallback), never a genuine participant in
    // cross-backend federation. Found live, breaking a real test: four native targets each holding
    // their OWN separate, differently-shaped "widgets" table (one per protocol, by design, never
    // meant to be joined or resolved together) got flagged as a false-positive plain-group
    // conflict the moment auto-discovery started running for every statement, not just MCP's own
    // tool calls. Excluded from discovery entirely, not just from the sharded/plain grouping,
    // since they were never meant to be resolved through this mechanism at all.
    private static final java.util.Set<String> RESERVED_NATIVE_BACKEND_NAMES = java.util.Set.of(
            BackendRegistry.MYSQL_NATIVE_DEFAULT_NAME, BackendRegistry.MSSQL_NATIVE_DEFAULT_NAME,
            BackendRegistry.ORACLE_NATIVE_DEFAULT_NAME, BackendRegistry.MYSQL_NATIVE_DUAL_PORT_NAME,
            BackendRegistry.MSSQL_NATIVE_DUAL_PORT_NAME);

    /** One real JDBC metadata query per backend -- not cached here (see this class's own javadoc
     * on why: a fresh discovery keyed to one MCP call, not a background service, is the deliberate
     * scope for this first implementation). A backend that fails to connect or introspect is
     * skipped with a logged warning rather than failing the whole discovery -- one unreachable
     * backend shouldn't block federation across the ones that ARE reachable. */
    public static List<DiscoveredTable> discoverAll(BackendRegistry registry) {
        List<DiscoveredTable> tables = new ArrayList<>();
        for (BackendTarget target : registry.all()) {
            if (RESERVED_NATIVE_BACKEND_NAMES.contains(target.name())) {
                continue;
            }
            try (Connection conn = target.open()) {
                DatabaseMetaData md = conn.getMetaData();
                try (ResultSet rs = md.getTables(null, null, "%", new String[] {"TABLE"})) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        String schemaName = rs.getString("TABLE_SCHEM");
                        tables.add(new DiscoveredTable(tableName, target.name(), schemaName));
                    }
                }
            } catch (SQLException e) {
                log.warn("schema auto-discovery: could not introspect backend \"{}\" -- skipping it "
                        + "for this discovery pass, real query is unaffected if it doesn't need this "
                        + "backend ({})", target.name(), e.toString());
            }
        }
        return tables;
    }

    /** Groups {@link #discoverAll}'s flat list by table name (case-insensitively -- SQL identifiers
     * are case-insensitive unless quoted, and a caller matching a bare table reference from SQL
     * text has no quoting information left to go on). A table found on more than one backend is a
     * real ambiguity: {@link #resolveUnambiguous} is where that gets surfaced as a clear error
     * rather than a silent pick-one. */
    public static Map<String, List<DiscoveredTable>> byTableNameLowercase(List<DiscoveredTable> tables) {
        Map<String, List<DiscoveredTable>> byName = new LinkedHashMap<>();
        for (DiscoveredTable t : tables) {
            byName.computeIfAbsent(t.tableName().toLowerCase(Locale.ROOT), k -> new ArrayList<>()).add(t);
        }
        return byName;
    }

    /** {@code sharded}: true when {@code backendName}'s hit came from a real {@code sharded}
     * group -- a caller (e.g. {@code query_federated}'s own Calcite-mount step) MUST NOT treat
     * this as "the one and only place this table lives" the way a plain-group resolution means;
     * it means "this table's real location depends on the query's shard-key predicate, defer to
     * the existing shard-routing machinery instead of picking a backend yourself." */
    public record ResolvedTable(String backendName, String realSchemaName, boolean sharded, String groupName) {
    }

    /**
     * Resolves every discovered hit for one table name against {@code registry}'s group semantics
     * (see this class's own javadoc for the exact rule). Returns {@code null} for zero hits (the
     * table wasn't found on any backend at all -- not this class's problem to report, the query
     * that references it will fail naturally downstream). Throws {@link IllegalStateException}
     * with a clear, group-aware message for a real conflict -- never silently picks one backend.
     * A successful sharded-group resolution returns an arbitrary member as the representative (its
     * own real schema name, for the rare case a caller still wants SOME concrete connection info)
     * but callers should key off {@link ResolvedTable#sharded()} before using it for routing.
     */
    // The pre-existing, much older WARP_SHARD_BACKENDS/BackendRegistry#shardGroup() scatter-gather
    // mechanism predates WARP_BACKEND_GROUPS entirely -- real, already-shipped deployments (and
    // this codebase's own existing sharding tests) partition a table like "orders" across a
    // shardGroup() without ever declaring WARP_BACKEND_GROUPS at all. Found live, breaking those
    // exact tests: without this, every one of them fell into the synthetic UNGROUPED_GROUP_NAME
    // (plain) bucket and got flagged as a false-positive conflict the instant this class started
    // being consulted from a real statement path (SchemaAutoDiscoveryStage), not just MCP's
    // comparatively rare tool calls. A shardGroup() member is therefore treated as implicitly
    // sharded here too, in its own pseudo-group -- an operator gets the "sharded databases need no
    // extra ceremony" behavior for free from a mechanism they already had configured, without
    // needing to ALSO redeclare the same backends under the newer, separate WARP_BACKEND_GROUPS.
    private static final String LEGACY_SHARD_GROUP_PSEUDO_NAME = "__legacy_shard_group__";

    /** Two DIFFERENTLY-NAMED backend registrations can be the exact same real physical database --
     * a real, common test/deployment pattern: {@code BackendRegistry.DEFAULT_BACKEND_NAME} ("default")
     * is often registered pointing at the same connection info as one real named backend, purely
     * so the session's own initial connection lands on a real shard member. Found live, breaking
     * this codebase's own sharding tests: without collapsing these first, "orders" existing on
     * both "default" and "shard1" (the SAME physical table, seen twice under two names) looked
     * like a real cross-group conflict ("default" isn't itself named in any shard rule) rather
     * than the non-issue it actually is. Compares by {@link BackendTarget#poolKey()} (real
     * connection identity: URL + user), not name; when two hits collapse, the non-"default" name
     * wins as the representative, since that's the name an operator's real routing rules actually
     * reference. */
    private static List<DiscoveredTable> dedupePhysicallyIdenticalBackends(List<DiscoveredTable> hits, BackendRegistry registry) {
        Map<String, DiscoveredTable> byPoolKey = new LinkedHashMap<>();
        for (DiscoveredTable t : hits) {
            BackendTarget target = registry.get(t.backendName());
            String key = target == null ? t.backendName() : target.poolKey();
            DiscoveredTable existing = byPoolKey.get(key);
            if (existing == null || BackendRegistry.DEFAULT_BACKEND_NAME.equals(existing.backendName())) {
                byPoolKey.put(key, t);
            }
        }
        return new ArrayList<>(byPoolKey.values());
    }

    public static ResolvedTable resolveUnambiguous(List<DiscoveredTable> hits, BackendRegistry registry) {
        return resolveUnambiguous(hits, registry, Set.of());
    }

    /** As the 2-arg overload, plus {@code extraShardedBackendNames} -- backend names an operator
     * declared sharded through a mechanism OTHER than {@code shardGroup()} or {@code
     * WARP_BACKEND_GROUPS}, most commonly {@code WARP_TABLE_SHARDS}' own per-table {@code
     * ShardingStrategy} backend lists (a declarative table-shard rule's backends are never
     * required to also be a member of the GLOBAL {@code shardGroup()} -- found live, breaking
     * this codebase's own {@code WARP_TABLE_SHARDS}-only sharding tests, which never configure
     * {@code WARP_SHARD_BACKENDS} at all). Callers (see {@code SchemaAutoDiscoveryStage}/{@code
     * WarpMcpServer}) compute this from {@code RouterStage.tableShardRules()}'s own {@code
     * ShardingStrategy.allBackends}. */
    public static ResolvedTable resolveUnambiguous(List<DiscoveredTable> hits, BackendRegistry registry,
            Set<String> extraShardedBackendNames) {
        if (hits == null || hits.isEmpty()) {
            return null;
        }
        hits = dedupePhysicallyIdenticalBackends(hits, registry);
        Set<String> legacyShardGroupMembers = new java.util.HashSet<>(registry.shardGroup());
        legacyShardGroupMembers.addAll(extraShardedBackendNames);
        Map<String, List<DiscoveredTable>> byGroup = new LinkedHashMap<>();
        for (DiscoveredTable t : hits) {
            String groupName;
            if (legacyShardGroupMembers.contains(t.backendName())) {
                groupName = LEGACY_SHARD_GROUP_PSEUDO_NAME;
            } else {
                BackendRegistry.BackendGroupInfo info = registry.groupInfoFor(t.backendName());
                groupName = info == null ? BackendRegistry.UNGROUPED_GROUP_NAME : info.name();
            }
            byGroup.computeIfAbsent(groupName, k -> new ArrayList<>()).add(t);
        }
        if (byGroup.size() > 1) {
            List<String> backendNames = hits.stream().map(DiscoveredTable::backendName).distinct().toList();
            throw new IllegalStateException("found on backends in DIFFERENT backend groups ("
                    + String.join(", ", backendNames) + ") -- these are unrelated to each other, "
                    + "not sharded copies of the same table");
        }
        String soleGroupName = byGroup.keySet().iterator().next();
        boolean sharded;
        if (soleGroupName.equals(LEGACY_SHARD_GROUP_PSEUDO_NAME)) {
            sharded = true;
        } else {
            BackendRegistry.BackendGroupInfo groupInfo = registry.groupInfoFor(hits.get(0).backendName());
            sharded = groupInfo != null && groupInfo.sharded();
        }
        List<DiscoveredTable> groupHits = byGroup.get(soleGroupName);
        if (!sharded) {
            Set<String> distinctBackends = new LinkedHashSet<>();
            for (DiscoveredTable t : groupHits) {
                distinctBackends.add(t.backendName());
            }
            if (distinctBackends.size() > 1) {
                throw new IllegalStateException("found on more than one backend within the same PLAIN "
                        + "group \"" + soleGroupName + "\" (" + String.join(", ", distinctBackends)
                        + ") -- these are independent backends, not shards of one table; mark the group "
                        + "\":sharded\" if that's wrong, or rename one of the colliding tables");
            }
        }
        DiscoveredTable representative = groupHits.get(0);
        return new ResolvedTable(representative.backendName(), representative.realSchemaName(), sharded, soleGroupName);
    }

    private BackendCatalogDiscovery() {
    }
}
