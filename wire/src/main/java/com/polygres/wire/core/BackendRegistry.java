package com.polygres.wire.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Named Postgres backends a {@link RouterStage} rule can target, beyond the one connection each
 * frontend already opens for itself. Config-only registry — parsed once at startup from
 * {@code POLYWIRE_BACKENDS}, format: {@code name1=jdbcUrl|user|password;name2=jdbcUrl|user|password}.
 * An empty registry (the default) means routing rules can only ever match "no override," which is
 * exactly today's single-backend behavior — this feature is opt-in.
 *
 * <p>Also carries an optional shard group ({@code POLYWIRE_SHARD_BACKENDS}, comma-separated names
 * already present in this registry) that {@link RoutingBackendExecutor} fans a scatter-gather query
 * out to — see that class's javadoc.
 *
 * <p><b>Backend poisoning protection</b> ({@link TrustedBackendHosts}, {@code
 * POLYWIRE_TRUSTED_BACKEND_HOSTS}): {@code spec} here is sourced from {@code
 * polywire_config.backends}/{@code shardBackends}, hot-reloadable straight from a Postgres table
 * (see {@link #reload}) -- so without an allowlist, anyone with write access to that table could
 * register an arbitrary host and have real client traffic routed to it. Opt-in, disabled by
 * default; see that class's javadoc for the full threat model and why the allowlist is env-var
 * only, never itself settable from the same DB-writable surface it gates.
 *
 * <p><b>Postgres-only</b>: unlike Polywire (which also resolved secrets via Vault/CyberArk through
 * {@code com.omnigate.secrets.SecretResolver} and capped backend count per free/commercial edition),
 * PolyWire keeps this deliberately small — every {@code user}/{@code password} field is used as a
 * literal, and there is no edition cap. Cut, not stubbed: PolyWire has no secrets-manager
 * integration of its own, and re-adding one is follow-up work if ever needed, not a dependency worth
 * carrying here just to satisfy one class's import.
 *
 * <p><b>Hot reload</b> ({@link #reload}): {@code targets}/{@code shardGroup} are {@code volatile},
 * not {@code final} — {@link #reload} builds a completely fresh, fully-formed pair (by delegating
 * back to {@link #fromConfig}) and only then swaps both references in one assignment each, so a
 * concurrent reader (routing a query mid-reload) always sees either the old, complete config or the
 * new one, never a torn mix.
 */
public final class BackendRegistry {

    private static final Logger log = LoggerFactory.getLogger(BackendRegistry.class);

    /** Synthetic name used for the implicit single-backend entry — see {@link #fromConfig(String, String, BackendTarget)}. */
    public static final String DEFAULT_BACKEND_NAME = "default";

    private volatile Map<String, BackendTarget> targets;
    private volatile List<String> shardGroup;
    // Not volatile-swapped independently: only ever read back by #reload to re-derive a fresh
    // registry the same way startup did for the same inputs — see #reload's javadoc.
    private final BackendTarget defaultTarget;

    public BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup) {
        this(targets, shardGroup, null);
    }

    private BackendRegistry(Map<String, BackendTarget> targets, List<String> shardGroup, BackendTarget defaultTarget) {
        this.targets = Map.copyOf(targets);
        this.shardGroup = List.copyOf(shardGroup);
        this.defaultTarget = defaultTarget;
    }

    /**
     * {@code shardGroupSpec}: "backend1,backend2,..." — each name must also appear in {@code spec}.
     */
    public static BackendRegistry fromConfig(String spec, String shardGroupSpec) {
        return fromConfig(spec, shardGroupSpec, null);
    }

    /**
     * Same as the two-arg {@link #fromConfig}, plus {@code defaultTarget}: when {@code spec}
     * ({@code POLYWIRE_BACKENDS}) is unset, PolyWire is running in single-backend mode — every
     * frontend already opens its own connection to the one Postgres backend described by the
     * {@code ORAPG_PG_*} env vars, but until now that connection was never itself registered here,
     * so {@link RouterStage} had nothing to fall back to and {@link DialectTranslationStage} never
     * fired for the default deployment path (the gap this parameter closes — see
     * {@code RouterStage#resolveBackend}'s javadoc). Registered under {@link #DEFAULT_BACKEND_NAME}
     * only when {@code spec} is null/blank; a real {@code POLYWIRE_BACKENDS} config is always
     * authoritative and never gets this synthetic entry added alongside it.
     */
    public static BackendRegistry fromConfig(String spec, String shardGroupSpec, BackendTarget defaultTarget) {
        // POLYWIRE_TRUSTED_BACKEND_HOSTS -- see TrustedBackendHosts' class javadoc for why this is
        // checked here (the DB-writable POLYWIRE_BACKENDS/POLYWIRE_SHARD_BACKENDS surface) and not
        // for defaultTarget below (the operator's own env-var-configured connection, already
        // inherently trusted). Disabled (unset) is a zero-cost no-op -- every entry passes.
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
                    + "implicit ORAPG_PG_* backend as '{}' so routing/translation has a fallback target",
                    DEFAULT_BACKEND_NAME);
        }
        List<String> shardGroup = shardGroupSpec == null || shardGroupSpec.isBlank()
                ? List.of()
                : List.of(shardGroupSpec.split(",")).stream().map(String::trim).toList();
        return new BackendRegistry(targets, shardGroup, defaultTarget);
    }

    /**
     * Replaces this registry's targets and shard group in place, reusing {@link #fromConfig}'s
     * parsing logic so a reload can never diverge from what a fresh startup would have produced for
     * the same {@code spec}/{@code shardGroupSpec}. Every existing holder of this instance observes
     * the change immediately — no new indirection layer needed, since object identity never changes,
     * only the two fields inside it. Carries this instance's original {@code defaultTarget} forward
     * (it's {@code ORAPG_PG_*}-derived and fixed for the process lifetime, unlike {@code spec}) so a
     * hot-reload that clears {@code POLYWIRE_BACKENDS} back to unset doesn't lose the fallback entry.
     */
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

    /** For read-only config introspection (the HTTP admin API) — never includes passwords. */
    public java.util.Collection<BackendTarget> all() {
        return targets.values();
    }
}
