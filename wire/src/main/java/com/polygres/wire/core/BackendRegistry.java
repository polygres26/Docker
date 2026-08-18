package com.polygres.wire.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackendRegistry {

    private static final Logger log = LoggerFactory.getLogger(BackendRegistry.class);

    public static final String DEFAULT_BACKEND_NAME = "default";

    private volatile Map<String, BackendTarget> targets;
    private volatile List<String> shardGroup;
    
    private final BackendTarget defaultTarget;

    public BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup) {
        this(targets, shardGroup, null);
    }

    private BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup, BackendTarget defaultTarget) {
        this.targets = Map.copyOf(targets);
        this.shardGroup = List.copyOf(shardGroup);
        this.defaultTarget = defaultTarget;
    }

    public static BackendRegistry fromConfig(String spec, String shardGroupSpec) {
        return fromConfig(spec, shardGroupSpec, null);
    }

    public static BackendRegistry fromConfig(String spec, String shardGroupSpec, BackendTarget defaultTarget) {
        
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
                if (!trustedHosts.isTrusted(url)) {
                    log.warn("backend registry: REFUSING to register backend '{}' ({}) -- its host is not in "
                            + "POLYWIRE_TRUSTED_BACKEND_HOSTS. This entry is skipped, not fatal; every other "
                            + "configured backend is unaffected.", name, url);
                    continue;
                }
                targets.put(name, new BackendTarget(name, url, user, password));
            }
        } else if (defaultTarget != null) {
            targets.put(DEFAULT_BACKEND_NAME, defaultTarget);
            log.info("backend registry: no POLYWIRE_BACKENDS configured -- registered the single "
                    + "implicit POLYWIRE_PG_* backend as '{}' so routing/translation has a fallback target",
                    DEFAULT_BACKEND_NAME);
        }
        List<String> shardGroup = shardGroupSpec == null || shardGroupSpec.isBlank()
                ? List.of()
                : List.of(shardGroupSpec.split(",")).stream().map(String::trim).toList();
        return new BackendRegistry(targets, shardGroup, defaultTarget);
    }

    public void reload(String spec, String shardGroupSpec) {
        BackendRegistry fresh = fromConfig(spec, shardGroupSpec, this.defaultTarget);
        this.targets = fresh.targets;
        this.shardGroup = fresh.shardGroup;
    }

    public BackendTarget get(String name) {
        return targets.get(name);
    }

    public boolean isEmpty() {
        return targets.isEmpty();
    }

    public List<String> shardGroup() {
        return shardGroup;
    }

    public java.util.Collection<BackendTarget> all() {
        return targets.values();
    }
}
