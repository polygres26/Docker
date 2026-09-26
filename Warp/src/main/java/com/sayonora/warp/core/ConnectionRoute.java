package com.sayonora.warp.core;

/**
 * The outcome of resolving one client connection's database/service name (and login user) through
 * {@link ConnectionRouter}: unrouted (today's behavior), routed to one backend (DATABASE scope),
 * routed to a backend set (GROUP scope), or rejected (strict mode / an explicit route whose target
 * no longer exists). Immutable; a frontend session keeps one and re-resolves it when the client
 * switches database mid-session ({@code USE db}, {@code COM_INIT_DB}, a per-command {@code $db}).
 */
public final class ConnectionRoute {

    public enum Kind {
        UNROUTED, BACKEND, SET, REJECTED
    }

    public static final ConnectionRoute UNROUTED = new ConnectionRoute(Kind.UNROUTED, null, null, null, null);

    private final Kind kind;
    private final BackendScope scope;
    private final String pin;
    private final String name;
    private final String description;
    private String setName;

    private ConnectionRoute(Kind kind, BackendScope scope, String pin, String name, String description) {
        this.kind = kind;
        this.scope = scope;
        this.pin = pin;
        this.name = name;
        this.description = description;
    }

    static ConnectionRoute toBackend(String backend, String requestedName) {
        return new ConnectionRoute(Kind.BACKEND, BackendScope.single(backend), backend, requestedName,
                "backend " + backend);
    }

    static ConnectionRoute toSet(String set, java.util.Collection<String> members, String defaultBackend,
            String requestedName) {
        ConnectionRoute route = new ConnectionRoute(Kind.SET,
                new BackendScope(new java.util.LinkedHashSet<>(members), "group:" + set, defaultBackend), null,
                requestedName, "set " + set);
        route.setName = set;
        return route;
    }

    static ConnectionRoute rejected(String requestedName, String why) {
        return new ConnectionRoute(Kind.REJECTED, null, null, requestedName, why);
    }

    public Kind kind() {
        return kind;
    }

    public boolean isRouted() {
        return scope != null;
    }

    public boolean isRejected() {
        return kind == Kind.REJECTED;
    }

    /** The scope every Statement of this connection carries; {@code null} when unrouted/rejected. */
    public BackendScope scope() {
        return scope;
    }

    /** The single backend a DATABASE route pins to; {@code null} otherwise. */
    public String backend() {
        return pin;
    }

    /** The set a GROUP route targets; {@code null} otherwise. */
    public String setName() {
        return setName;
    }

    /** The database/service name the client asked for. */
    public String requestedName() {
        return name;
    }

    /** Whether a statement of this connection can ever run on the session's own default backend
     * connection. When it cannot (routed to another backend/set), frontends skip borrowing that
     * connection for transaction bookkeeping, so a session that lives on backend X never holds a
     * connection to the default backend. */
    public boolean permitsDefault() {
        return scope == null || scope.permits(BackendRegistry.DEFAULT_BACKEND_NAME);
    }

    /** {@code "backend x"}, {@code "set s"}, or the rejection reason -- for logs and messages. */
    public String description() {
        return description;
    }

    /**
     * Attaches this connection's scope to {@code statement}; a route to ONE backend also pins the
     * statement to it (unless the frontend already pinned a target), so no router rule is
     * consulted and an unqualified statement runs on that backend rather than the default. The
     * unrouted case returns the statement itself -- no allocation on the per-statement hot path.
     */
    public Statement apply(Statement statement) {
        if (scope == null) {
            return statement;
        }
        Statement scoped = statement.withBackendScope(scope);
        if (pin != null && statement.targetBackend() == null) {
            scoped = scoped.withRouting(scoped.workloadClass(), pin);
        }
        return scoped;
    }

    @Override
    public String toString() {
        return "ConnectionRoute[" + kind + (description == null ? "" : " " + description) + "]";
    }
}
