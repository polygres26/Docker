package com.sayonora.wire.core;

import java.sql.SQLException;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real, protocol-agnostic schema auto-discovery federation -- makes {@link SchemaAutoDiscovery}'s
 * resolution logic (previously reachable only from MCP's own {@code query_federated} tool) work
 * for ANY wire-protocol client (pgwire/orawire/mywire/mssqlwire) sending plain, unqualified table
 * names across backends, with no {@code WARP_ROUTER_SCHEMA_RULES} configuration at all. Runs
 * BEFORE {@link RouterStage} in the shared pipeline, same slot as {@link SchemaFederationStage} and
 * for the identical reason: a statement referencing 2+ backends has to be federated before {@link
 * RouterStage#resolveBackend} ever narrows it down to just one of them.
 *
 * <p>Only added to the pipeline when {@link BackendRegistry#all()} has 2+ backends ({@code
 * Main}'s own gate, mirroring {@link SchemaFederationStage#fromConfigOrNull}'s "&lt; 2 means
 * nothing to ever federate" reasoning) -- a single-backend deployment has nothing this stage could
 * ever do, so it's not even constructed there.
 *
 * <p>Uses a {@link BackendCatalogCache}, not a fresh {@link BackendCatalogDiscovery#discoverAll}
 * per statement -- required here in a way it wasn't for MCP's own comparatively rare tool calls: a
 * real driver client can send thousands of statements a second, and live JDBC introspection
 * against every registered backend on every single one would be a real, unacceptable latency
 * regression for ALL protocol traffic, not just federated queries.
 */
public final class SchemaAutoDiscoveryStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(SchemaAutoDiscoveryStage.class);
    private static final Pattern SELECT_PREFIX = Pattern.compile("^\\s*select\\b", Pattern.CASE_INSENSITIVE);

    private final BackendRegistry backendRegistry;
    private final BackendCatalogCache catalogCache;
    private final RouterStage routerStage;

    public SchemaAutoDiscoveryStage(BackendRegistry backendRegistry, BackendCatalogCache catalogCache,
            RouterStage routerStage) {
        this.backendRegistry = backendRegistry;
        this.catalogCache = catalogCache;
        this.routerStage = routerStage;
    }

    /** {@code null} when fewer than 2 real backends are registered -- same "absent means the
     * feature doesn't exist" shape {@link SchemaFederationStage#fromConfigOrNull} uses; {@code
     * Main} skips adding this stage entirely in that case. */
    /** Forces the next statement to pay for a fresh discovery instead of using a possibly-stale
     * cached catalog -- called on {@link BackendRegistry#reload}, so a backend topology change is
     * picked up promptly rather than waiting out the TTL. Like {@link SchemaFederationStage}, a
     * deployment that had FEWER than 2 backends at startup (so this stage was never constructed at
     * all -- see {@link #fromRegistryOrNull}) doesn't gain auto-discovery federation retroactively
     * just because a reload added a second backend; that's a real, disclosed limitation shared
     * with {@code SchemaFederationStage}'s own reload behavior, not new here. */
    public void invalidateCatalogCache() {
        catalogCache.invalidate();
    }

    public static SchemaAutoDiscoveryStage fromRegistryOrNull(BackendRegistry backendRegistry, RouterStage routerStage) {
        if (backendRegistry.all().size() < 2) {
            return null;
        }
        return new SchemaAutoDiscoveryStage(backendRegistry, new BackendCatalogCache(backendRegistry), routerStage);
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String sql = statement.sqlText();
        if (!SELECT_PREFIX.matcher(sql).find()) {
            return next.proceed(statement);
        }
        SchemaAutoDiscovery.Resolution resolution = SchemaAutoDiscovery.resolve(
                sql, backendRegistry, catalogCache.byTableNameLowercase(),
                RouterStage.tableShardBackendNames(routerStage.tableShardRules()));
        if (resolution.ambiguous()) {
            throw new SQLException("table \"" + resolution.ambiguousTable() + "\" is ambiguous: "
                    + resolution.ambiguousMessage(), "42P09");
        }
        if (!resolution.federated()) {
            return next.proceed(statement);
        }
        log.info("schema auto-discovery: statement references {} backend(s) with no schema-rule "
                + "configured -- executing via a federated Calcite connection instead of routing to one: {}",
                resolution.mounts().size(), resolution.mounts().keySet());
        Statement rewritten = new Statement(statement.tenantId(), statement.sourceDialect(),
                resolution.rewrittenSql(), statement.bindParams(), statement.workloadClass(),
                statement.targetBackend(), statement.accessContext());
        return new SchemaFederationStage(java.util.List.of(), backendRegistry).executeWithMounts(resolution.mounts(), rewritten);
    }
}
