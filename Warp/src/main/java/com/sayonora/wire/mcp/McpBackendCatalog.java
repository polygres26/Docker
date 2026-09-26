package com.sayonora.wire.mcp;

import com.sayonora.wire.core.BackendCatalogDiscovery;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.StoreType;
import com.sayonora.wire.server.ServerOptions.McpBackendMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        // Stores ENABLED through backend-set config: each hosting Postgres backend lists them as typed
        // stores named <backend>.<kind>. The tools behind them are the sharded frontends' own logic,
        // so a call sees the whole store whichever host's entry it addresses.
        java.util.Set<String> configured = new java.util.HashSet<>();
        if (mode == McpBackendMode.POSTGRES) {
            Map<String, List<StoreType>> enabled = registry.allEnabledStores();
            for (BackendTarget t : targets) {
                for (StoreType st : enabled.getOrDefault(t.name(), List.of())) {
                    BackendKind k = kindOf(st);
                    if (!kinds.contains(k) || !registry.storeHosts(st).contains(t.name()) || !storeRunning(k)) {
                        continue;
                    }
                    String name = t.name() + McpBackend.EMULATED_SEPARATOR + k.id();
                    configured.add(name);
                    out.add(new McpBackend(name, k.id(), k, true, t.name(), t));
                }
            }
        }
        if (defaultInScope) {
            String host = BackendRegistry.DEFAULT_BACKEND_NAME;
            for (BackendKind k : new BackendKind[] {BackendKind.DYNAMODB, BackendKind.MONGODB, BackendKind.INFLUX}) {
                if (configured.contains(host + McpBackend.EMULATED_SEPARATOR + k.id())) {
                    continue; // already listed because it is enabled through config; env is only the fallback
                }
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
            case SQS, OPENSEARCH, NEO4J, S3STORE, REDIS, AZBLOB, AZQUEUE, AZTABLE, SNS, KINESIS, AWSPARAMS, GCS, BIGTABLE, PUBSUB -> true; // described straight from the hosting Postgres
            default -> false;
        };
    }

    static BackendKind kindOf(StoreType st) {
        return switch (st) {
            case INFLUXDB -> BackendKind.INFLUX;
            case MONGODB -> BackendKind.MONGODB;
            case SQS -> BackendKind.SQS;
            case NEO4J -> BackendKind.NEO4J;
            case OPENSEARCH -> BackendKind.OPENSEARCH;
            case DYNAMODB -> BackendKind.DYNAMODB;
            case S3 -> BackendKind.S3STORE;
            case REDIS -> BackendKind.REDIS;
            case AZBLOB -> BackendKind.AZBLOB;
            case AZQUEUE -> BackendKind.AZQUEUE;
            case AZTABLE -> BackendKind.AZTABLE;
            case SNS -> BackendKind.SNS;
            case KINESIS -> BackendKind.KINESIS;
            case AWSPARAMS -> BackendKind.AWSPARAMS;
            case FIRESTORE -> BackendKind.FIRESTORE;
            case DATASTORE -> BackendKind.DATASTORE;
            case GCS -> BackendKind.GCS;
            case BIGTABLE -> BackendKind.BIGTABLE;
            case PUBSUB -> BackendKind.PUBSUB;
        };
    }
}
