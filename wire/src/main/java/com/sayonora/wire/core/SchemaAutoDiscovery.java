package com.sayonora.wire.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Real, protocol-agnostic core of schema auto-discovery federation -- resolves a statement's bare
 * (unqualified) {@code FROM}/{@code JOIN} table references against a discovered backend catalog
 * and, when 2+ distinct PLAIN-group backends are genuinely touched, produces a rewritten SQL text
 * plus the {@link SchemaFederationStage.BackendMount} map needed to run it through {@link
 * SchemaFederationStage#executeWithMounts} -- the SAME Calcite planning core {@code
 * WARP_ROUTER_SCHEMA_RULES}-declared federation already uses.
 *
 * <p>Originally built as a private method on {@code WarpMcpServer} (MCP's own {@code
 * query_federated} tool); extracted here so the SAME resolution logic can ALSO run as a real
 * shared {@link PipelineStage} ({@code SchemaAutoDiscoveryStage}) for wire-protocol clients --
 * a real Postgres/Oracle/MySQL/SQL Server driver sending plain, unqualified table names across
 * backends deserves the same "it just works" federation an MCP agent gets, not a capability that
 * only exists behind one specific tool call.
 */
public final class SchemaAutoDiscovery {

    // Bare (unqualified) FROM/JOIN table references only -- an already-qualified "schema.table"
    // reference (the shape WARP_ROUTER_SCHEMA_RULES federation expects) is left untouched;
    // auto-discovery is specifically for the case a caller has no reason to know any schema-alias
    // naming scheme and just writes plain table names. Regex over raw SQL, not a full parser --
    // same posture as every other "table reference" scan in this codebase (e.g.
    // MySqlBinaryProtocol#countPlaceholders); a CTE name or derived-table alias that happens to
    // collide with a real discovered table name is a real, narrow, disclosed scope limit.
    private static final Pattern BARE_TABLE_REF = Pattern.compile(
            "\\b(FROM|JOIN)\\s+([A-Za-z_][A-Za-z0-9_]*)\\b(?!\\s*\\.)", Pattern.CASE_INSENSITIVE);

    /** {@code mounts} non-null and size &gt;= 2 means federate ({@link #rewrittenSql} is what to
     * actually run); {@code ambiguousMessage} non-null means refuse with a clear error naming
     * {@code ambiguousTable} and why; neither means "nothing for auto-discovery to do here" --
     * the caller should proceed with the ORIGINAL sql on its normal path (a sharded-group table
     * alone still routes correctly there; see this class's own javadoc on why a sharded-group
     * reference is never mount-federated by this class at all). */
    public record Resolution(String rewrittenSql, Map<String, SchemaFederationStage.BackendMount> mounts,
            String ambiguousTable, String ambiguousMessage) {

        public boolean federated() {
            return mounts != null && mounts.size() >= 2;
        }

        public boolean ambiguous() {
            return ambiguousMessage != null;
        }

        static Resolution notApplicable() {
            return new Resolution(null, null, null, null);
        }

        static Resolution ambiguous(String table, String message) {
            return new Resolution(null, null, table, message);
        }

        static Resolution federated(String rewrittenSql, Map<String, SchemaFederationStage.BackendMount> mounts) {
            return new Resolution(rewrittenSql, mounts, null, null);
        }
    }

    public static Resolution resolve(String sql, BackendRegistry registry,
            Map<String, List<BackendCatalogDiscovery.DiscoveredTable>> byTableNameLowercase) {
        return resolve(sql, registry, byTableNameLowercase, java.util.Set.of());
    }

    /** @param byTableNameLowercase a discovered catalog (fresh or cached -- see {@link
     *      BackendCatalogCache}), grouped the same way {@link
     *      BackendCatalogDiscovery#byTableNameLowercase} returns.
     * @param extraShardedBackendNames as {@link BackendCatalogDiscovery#resolveUnambiguous}'s own
     *      3-arg overload -- typically the union of every {@code WARP_TABLE_SHARDS} rule's own
     *      backend list, computed by the caller from {@code RouterStage.tableShardRules()}. */
    public static Resolution resolve(String sql, BackendRegistry registry,
            Map<String, List<BackendCatalogDiscovery.DiscoveredTable>> byTableNameLowercase,
            java.util.Set<String> extraShardedBackendNames) {
        Map<String, SchemaFederationStage.BackendMount> mounts = new LinkedHashMap<>();
        // A table resolved to a SHARDED backend group is real, deliberate scope this method does
        // NOT attempt to Calcite-mount -- its actual location depends on the query's own shard-key
        // predicate (WARP_TABLE_SHARDS/ShardJoinExecutor's job, not this one), so picking any single
        // "representative" backend for it here would silently drop every other shard's rows. Any
        // sharded-group reference is left untouched in the rewrite and the whole statement defers
        // to plain execution instead -- mixing a sharded-group table with plain-group Calcite
        // federation in the SAME statement isn't supported yet (a real, disclosed limitation, not a
        // guess at which execution path should win).
        boolean touchesShardedGroupTable = false;
        StringBuilder rewritten = new StringBuilder();
        Matcher m = BARE_TABLE_REF.matcher(sql);
        while (m.find()) {
            String tableName = m.group(2);
            List<BackendCatalogDiscovery.DiscoveredTable> matches = byTableNameLowercase.get(tableName.toLowerCase(Locale.ROOT));
            BackendCatalogDiscovery.ResolvedTable resolved;
            try {
                resolved = BackendCatalogDiscovery.resolveUnambiguous(matches, registry, extraShardedBackendNames);
            } catch (IllegalStateException ambiguous) {
                return Resolution.ambiguous(tableName, ambiguous.getMessage());
            }
            if (resolved == null) {
                m.appendReplacement(rewritten, Matcher.quoteReplacement(m.group()));
                continue;
            }
            if (resolved.sharded()) {
                touchesShardedGroupTable = true;
                m.appendReplacement(rewritten, Matcher.quoteReplacement(m.group()));
                continue;
            }
            mounts.putIfAbsent(resolved.backendName(),
                    new SchemaFederationStage.BackendMount(resolved.backendName(), resolved.realSchemaName()));
            // The mount alias is double-quoted, not bare -- a real backend name can be a reserved
            // SQL keyword ("default", BackendRegistry.DEFAULT_BACKEND_NAME -- confirmed live, this
            // broke Calcite's parser outright) or contain characters an unquoted identifier can't
            // (a hyphen, e.g. "mysql-native-dual-port"). Calcite's own parser config here already
            // accepts double-quoted identifiers (the Frameworks Planner's default), so this is a
            // real fix, not a workaround.
            m.appendReplacement(rewritten, Matcher.quoteReplacement(
                    m.group(1) + " \"" + resolved.backendName() + "\"." + tableName));
        }
        m.appendTail(rewritten);

        if (touchesShardedGroupTable || mounts.size() < 2) {
            return Resolution.notApplicable();
        }
        return Resolution.federated(rewritten.toString(), mounts);
    }

    private SchemaAutoDiscovery() {
    }
}
