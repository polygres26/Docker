package com.sayonora.wire.core;

import java.sql.SQLException;
import java.util.Set;

/**
 * The set of registered backend names one {@link Statement} is ALLOWED to execute against --
 * carried on the Statement itself (not on {@link AccessContext}: {@code explain_sql}'s own MCP
 * path deliberately passes {@link AccessContext#ANONYMOUS}, and the constraint has to survive
 * that) and checked at every point the pipeline turns a statement into a real backend
 * connection: {@link RouterStage#handle} (early, before QoS/translation/caching spend anything),
 * {@link RoutingBackendExecutor#execute} (terminal, including cursor-derived and scatter/shard-
 * join targets), and both federation stages' per-mount checks.
 *
 * <p>{@code null} everywhere means "unconstrained" -- every wire frontend leaves it null, so
 * every check below starts with a null short-circuit and the pgwire/mywire/... paths are
 * byte-for-byte unchanged. Only the MCP gateway sets it today, from its {@code McpScope}.
 *
 * <p>{@code defaultBackend} (nullable) is where a statement that no routing rule claims goes when the
 * pipeline's own fallback ({@code default} / a native-mode default) is outside the scope -- set for
 * connect-time routing to a backend SET (see {@link ConnectionRouter}); see
 * {@link RouterStage#handle}. Never consulted for permission, only for choosing a target.
 *
 * <p>{@code label} is purely for the error message ({@code "db:backend_b"}, {@code
 * "group:team_alpha"}), never compared.
 */
public record BackendScope(Set<String> allowedBackends, String label, String defaultBackend) {

    public BackendScope {
        allowedBackends = Set.copyOf(allowedBackends);
    }

    public BackendScope(Set<String> allowedBackends, String label) {
        this(allowedBackends, label, null);
    }

    public static BackendScope single(String backendName) {
        return new BackendScope(Set.of(backendName), backendName);
    }

    /** A {@code null}/blank name is the pipeline's "no explicit target" spelling of
     * {@link BackendRegistry#DEFAULT_BACKEND_NAME} (see {@code RoutingBackendExecutor#execute}),
     * so it's checked as that name rather than trivially permitted. */
    public boolean permits(String backendName) {
        String effective = backendName == null || backendName.isBlank()
                ? BackendRegistry.DEFAULT_BACKEND_NAME : backendName;
        return allowedBackends.contains(effective);
    }

    /** No-op when {@code scope} is {@code null}; otherwise throws SQLSTATE 42501
     * ({@code ERR_SCOPE_BACKEND_DENIED}) unless {@code scope} permits {@code backendName}. */
    public static void check(BackendScope scope, String backendName) throws SQLException {
        if (scope == null) {
            return;
        }
        if (!scope.permits(backendName)) {
            String effective = backendName == null || backendName.isBlank()
                    ? BackendRegistry.DEFAULT_BACKEND_NAME : backendName;
            throw ErrorCatalog.sqlExceptionWithState("ERR_SCOPE_BACKEND_DENIED", "42501", effective, scope.label());
        }
    }
}
