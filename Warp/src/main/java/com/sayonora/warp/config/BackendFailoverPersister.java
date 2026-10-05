package com.sayonora.warp.config;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendSetModel;
import com.sayonora.warp.core.FailoverMonitor;
import com.sayonora.warp.core.ReplicaSpec;
import java.util.List;

/**
 * Records a failover-follow primary change in {@code warp_config}: the promoted node's URL becomes
 * the backend's URL and the replica list is replaced, as one new config version (so every other
 * instance picks it up over LISTEN/NOTIFY and a restart starts from the right primary). Compare-
 * and-set on the old primary URL against the LATEST config: if the config no longer names it, a
 * concurrent decision (another instance, or an operator) already moved the backend and nothing is
 * written. The registry of this process is reloaded immediately rather than waiting for the notify.
 */
public final class BackendFailoverPersister implements FailoverMonitor.Persister {

    private static final Object WRITE_LOCK = new Object();

    private final ConfigStore configStore;
    private final BackendRegistry registry;

    public BackendFailoverPersister(ConfigStore configStore, BackendRegistry registry) {
        this.configStore = configStore;
        this.registry = registry;
    }

    @Override
    public boolean persist(String backend, String expectedOldUrl, String newPrimaryUrl, List<ReplicaSpec> newReplicas)
            throws Exception {
        synchronized (WRITE_LOCK) {
            WarpConfig before = configStore.readLatest().map(ConfigStore.Version::payload)
                    .orElseGet(WarpConfig::fromEnvDefaults);
            BackendSetModel model = BackendSetModel.from(before, null);
            BackendSetModel.Backend current = model.backend(backend);
            if (current == null || !current.url().equals(expectedOldUrl)) {
                return false;
            }
            model.patchBackend(backend, newPrimaryUrl, null, null, false, null, null, ReplicaSpec.format(newReplicas));
            WarpConfig after = model.applyTo(before);
            configStore.write(after);
            registry.reload(after.backends(), after.shardBackends(), after.backendSets(), after.backendGroups());
            registry.applyStoreConfig(after.backendStores(), after.backendSetNames());
            return true;
        }
    }
}
