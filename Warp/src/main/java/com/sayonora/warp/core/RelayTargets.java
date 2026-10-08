package com.sayonora.warp.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where a raw-byte relay connects. A relay frontend (Oracle, MySQL, SQL Server {@code RELAY}) used to dial one fixed {@code WARP_*_HOST}:port
 * and so could not follow a failover or switchover. With {@code WARP_<ORACLE|MYWIRE|MSSQLWIRE>_RELAY_BACKEND} naming a {@code WARP_BACKENDS}
 * entry, the target is that backend's current primary, looked up at every new connection; the failover monitor changes it when the primary
 * moves. Sessions that are already open are not moved: they end when the old primary goes away or turns read-only, and reconnect to the new one.
 */
public final class RelayTargets {

    private static final Logger log = LoggerFactory.getLogger(RelayTargets.class);

    public record HostPort(String host, int port) {
    }

    private RelayTargets() {
    }

    /**
     * @param envPrefix e.g. {@code WARP_MYWIRE_RELAY}
     * @param registry may be null (no registry: the configured host is used)
     * @param defaultDbPort the engine's usual port, for a registry URL that names none
     */
    public static HostPort primary(String envPrefix, BackendRegistry registry, String configuredHost, int configuredPort, int defaultDbPort) {
        return resolve(System.getenv(envPrefix + "_BACKEND"), envPrefix, registry, configuredHost, configuredPort, defaultDbPort);
    }

    static HostPort resolve(String backend, String envPrefix, BackendRegistry registry, String configuredHost, int configuredPort,
            int defaultDbPort) {
        if (backend == null || backend.isBlank() || registry == null) {
            return new HostPort(configuredHost, configuredPort);
        }
        BackendTarget target = registry.resolveForRouting(backend.trim());
        if (target == null) {
            log.warn("relay: {}_BACKEND='{}' is not a configured backend; using {}:{}", envPrefix, backend, configuredHost, configuredPort);
            return new HostPort(configuredHost, configuredPort);
        }
        try {
            JdbcHostPort hp = JdbcHostPort.parse(target.jdbcUrl(), defaultDbPort);
            return new HostPort(hp.host(), hp.port());
        } catch (IllegalArgumentException e) {
            log.warn("relay: cannot read a host from the URL of backend '{}' ({}); using {}:{}", backend, e.getMessage(), configuredHost, configuredPort);
            return new HostPort(configuredHost, configuredPort);
        }
    }
}
