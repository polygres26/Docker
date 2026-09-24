package com.sayonora.wire.core;

import java.sql.SQLException;
import java.util.Map;
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
    // Threaded into the throwaway SchemaFederationStage this stage builds per statement (see
    // handle()'s own executeWithMounts call) -- without this, a plain wire-protocol client's
    // unqualified cross-backend query (or MCP's own query_federated auto-discovery) would silently
    // get NO native RLS/VPD enforcement even when WARP_ACCESS_NATIVE_RLS_DIALECTS is configured,
    // since this stage never reused the shared, config-declared SchemaFederationStage instance Main
    // builds -- a real gap found while wiring the config-declared path, not present there.
    private final Map<SourceDialect, com.sayonora.wire.core.access.NativeRlsSessionInitializer> nativeRlsInitializers;

    public SchemaAutoDiscoveryStage(BackendRegistry backendRegistry, BackendCatalogCache catalogCache,
            RouterStage routerStage) {
        this(backendRegistry, catalogCache, routerStage, Map.of());
    }

    public SchemaAutoDiscoveryStage(BackendRegistry backendRegistry, BackendCatalogCache catalogCache,
            RouterStage routerStage,
            Map<SourceDialect, com.sayonora.wire.core.access.NativeRlsSessionInitializer> nativeRlsInitializers) {
        this.backendRegistry = backendRegistry;
        this.catalogCache = catalogCache;
        this.routerStage = routerStage;
        this.nativeRlsInitializers = nativeRlsInitializers == null ? Map.of() : Map.copyOf(nativeRlsInitializers);
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
        return fromRegistryOrNull(backendRegistry, routerStage, Map.of());
    }

    /** As the other {@code fromRegistryOrNull}, plus {@code nativeRlsInitializers} (see this
     * class's own field javadoc) -- {@code Main} wires this from the same
     * {@code WARP_ACCESS_NATIVE_RLS_DIALECTS} config the schema-rule-declared federation path
     * uses, so a client relying on auto-discovery instead of a declared schema rule gets identical
     * RLS/VPD coverage. */
    public static SchemaAutoDiscoveryStage fromRegistryOrNull(BackendRegistry backendRegistry, RouterStage routerStage,
            Map<SourceDialect, com.sayonora.wire.core.access.NativeRlsSessionInitializer> nativeRlsInitializers) {
        if (backendRegistry.all().size() < 2) {
            return null;
        }
        return new SchemaAutoDiscoveryStage(backendRegistry, new BackendCatalogCache(backendRegistry), routerStage,
                nativeRlsInitializers);
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
        // Same per-mount BackendScope check as SchemaFederationStage#handle, same reasoning: a
        // federated execution opens every mounted backend here, never via RouterStage/
        // RoutingBackendExecutor. No-op when the scope is null.
        if (statement.backendScope() != null) {
            for (SchemaFederationStage.BackendMount mount : resolution.mounts().values()) {
                BackendScope.check(statement.backendScope(), mount.backendName());
            }
        }
        log.info("schema auto-discovery: statement references {} backend(s) with no schema-rule "
                + "configured -- executing via a federated Calcite connection instead of routing to one: {}",
                resolution.mounts().size(), resolution.mounts().keySet());
        // withSqlText (not a positional re-construction) so every other component -- including
        // backendScope -- is carried through unchanged.
        Statement rewritten = statement.withSqlText(resolution.rewrittenSql());
        return new SchemaFederationStage(java.util.List.of(), backendRegistry, null, null, nativeRlsInitializers)
                .executeWithMounts(resolution.mounts(), rewritten);
    }
}
