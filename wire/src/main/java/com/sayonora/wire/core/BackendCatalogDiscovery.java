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

    /** One real JDBC metadata query per backend -- not cached here (see this class's own javadoc
     * on why: a fresh discovery keyed to one MCP call, not a background service, is the deliberate
     * scope for this first implementation). A backend that fails to connect or introspect is
     * skipped with a logged warning rather than failing the whole discovery -- one unreachable
     * backend shouldn't block federation across the ones that ARE reachable. */
    public static List<DiscoveredTable> discoverAll(BackendRegistry registry) {
        List<DiscoveredTable> tables = new ArrayList<>();
        for (BackendTarget target : registry.all()) {
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
    public static ResolvedTable resolveUnambiguous(List<DiscoveredTable> hits, BackendRegistry registry) {
        if (hits == null || hits.isEmpty()) {
            return null;
        }
        Map<String, List<DiscoveredTable>> byGroup = new LinkedHashMap<>();
        for (DiscoveredTable t : hits) {
            BackendRegistry.BackendGroupInfo info = registry.groupInfoFor(t.backendName());
            String groupName = info == null ? BackendRegistry.UNGROUPED_GROUP_NAME : info.name();
            byGroup.computeIfAbsent(groupName, k -> new ArrayList<>()).add(t);
        }
        if (byGroup.size() > 1) {
            List<String> backendNames = hits.stream().map(DiscoveredTable::backendName).distinct().toList();
            throw new IllegalStateException("found on backends in DIFFERENT backend groups ("
                    + String.join(", ", backendNames) + ") -- these are unrelated to each other, "
                    + "not sharded copies of the same table");
        }
        String soleGroupName = byGroup.keySet().iterator().next();
        BackendRegistry.BackendGroupInfo groupInfo = registry.groupInfoFor(hits.get(0).backendName());
        boolean sharded = groupInfo != null && groupInfo.sharded();
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
