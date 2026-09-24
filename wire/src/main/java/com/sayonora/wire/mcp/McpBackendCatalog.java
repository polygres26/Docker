package com.sayonora.wire.mcp;

import com.sayonora.wire.core.BackendCatalogDiscovery;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.server.ServerOptions.McpBackendMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Resolves which backends an MCP endpoint's {@link McpScope} contains, with each backend's TYPE
 * derived automatically ({@link BackendTypes}). Warp-emulated stores (dynamowire/mongowire/influxwire,
 * whose data lives in Postgres tables) appear as extra logical backends of the gateway's default
 * backend, named {@code default.dynamodb} / {@code default.mongodb} / {@code default.influx}, only
 * when (a) the default backend is inside the scope, (b) the store's wire frontend is actually running
 * and (c) the operator opted the store in ({@code WARP_MCP_EMULATED_STORES}, or the legacy
 * {@code WARP_MCP_KIND} names its kind).
 */
final class McpBackendCatalog {

    private final BackendRegistry registry;
    private final EmulatedStores stores;
    private final Set<BackendKind> emulatedKinds;
    private final McpBackendMode mode;

    McpBackendCatalog(BackendRegistry registry, EmulatedStores stores, Set<BackendKind> emulatedKinds,
            McpBackendMode mode) {
        this.registry = registry;
        this.stores = stores;
        this.emulatedKinds = emulatedKinds;
        this.mode = mode;
    }

    /** Every backend visible under {@code scope}, restricted to the tool families in {@code kinds}. */
    List<McpBackend> inScope(McpScope scope, Set<BackendKind> kinds) {
        List<BackendTarget> targets = new ArrayList<>();
        switch (scope.type()) {
            case ALL -> registry.all().forEach(targets::add);
            case DATABASE -> {
                BackendTarget t = registry.get(scope.name());
                if (t != null) {
                    targets.add(t);
                }
            }
            case GROUP -> registry.membersOfGroup(scope.name()).forEach(n -> targets.add(registry.get(n)));
        }
        // Registry iteration order is unspecified: list "default" first, then by name, so
        // list_backends output is stable.
        targets.removeIf(java.util.Objects::isNull);
        targets.sort(java.util.Comparator.comparing((BackendTarget t) -> !t.name().equals(BackendRegistry.DEFAULT_BACKEND_NAME))
                .thenComparing(BackendTarget::name));
        List<McpBackend> out = new ArrayList<>();
        boolean defaultInScope = false;
        if (mode != McpBackendMode.POSTGRES && scope.type() != McpScope.Type.DATABASE) {
            // Native mode: the endpoint's SQL runs on the gateway's Oracle/MySQL/SQL Server connection.
            BackendTarget nat = registry.get(BackendRegistry.MCP_NATIVE_DEFAULT_NAME);
            if (nat != null && kinds.contains(BackendKind.RELATIONAL)) {
                out.add(new McpBackend(BackendRegistry.DEFAULT_BACKEND_NAME, mode.name().toLowerCase(Locale.ROOT),
                        BackendKind.RELATIONAL, false, null, nat));
            }
            defaultInScope = true;
        } else {
            for (BackendTarget t : targets) {
                if (t == null || BackendCatalogDiscovery.isReservedNativeName(t.name())) {
                    continue;
                }
                if (t.name().equals(BackendRegistry.DEFAULT_BACKEND_NAME)) {
                    defaultInScope = true;
                }
                BackendKind kind = BackendTypes.kindOf(t);
                if (kinds.contains(kind)) {
                    out.add(new McpBackend(t.name(), BackendTypes.typeOf(t), kind, false, null, t));
                }
            }
            if (scope.type() == McpScope.Type.ALL) {
                defaultInScope = true;
            }
        }
        if (defaultInScope) {
            String host = BackendRegistry.DEFAULT_BACKEND_NAME;
            for (BackendKind k : new BackendKind[] {BackendKind.DYNAMODB, BackendKind.MONGODB, BackendKind.INFLUX}) {
                if (emulatedKinds.contains(k) && kinds.contains(k) && storeRunning(k)) {
                    out.add(new McpBackend(host + McpBackend.EMULATED_SEPARATOR + k.id(), k.id(), k, true, host,
                            registry.get(host)));
                }
            }
        }
        return out;
    }

    private boolean storeRunning(BackendKind k) {
        return switch (k) {
            case DYNAMODB -> stores.dynamo() != null;
            case MONGODB -> stores.mongo() != null;
            case INFLUX -> stores.influx() != null;
            default -> false;
        };
    }
}
