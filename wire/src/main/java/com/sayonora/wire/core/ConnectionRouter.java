package com.sayonora.wire.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CONNECT-TIME BACKEND ROUTING: resolves, once per new client connection (and again when a client
 * switches database mid-session), the database / service name the driver already sent -- plus the
 * login user -- to a {@link ConnectionRoute}, so a client selects a specific backend, or a whole
 * backend set, with no client code change.
 *
 * <p><b>Resolution order</b> (first hit wins):
 * <ol>
 *   <li>explicit routes ({@code warp_config.connectionRoutes}, a JSON array of
 *       {@code {protocol?, database, user?, target, defaultBackend?}}; {@code database}/{@code user}
 *       are exact or {@code *}/{@code ?} globs; the first matching route in array order wins);
 *       {@code target} is a backend name, a set name, or explicitly {@code db:NAME}/{@code set:NAME}
 *       (a backend beats a set of the same name when written bare). A matched route whose target no
 *       longer exists REJECTS the connection (fail closed) instead of falling through to unscoped
 *       access;</li>
 *   <li>implicit: the name equals a backend name -> that backend (DATABASE scope: every statement
 *       pinned to it); equals a backend set name -> that set (GROUP scope);</li>
 *   <li>otherwise unrouted -- exactly today's behavior -- unless {@code WARP_CONNECT_ROUTING=strict},
 *       which rejects the connection with the protocol's own "unknown database" error. A blank name
 *       counts as unknown in strict mode (add a {@code database:"*"} route to allow it).</li>
 * </ol>
 * {@code WARP_CONNECT_ROUTING=off} disables the whole feature (routes included).
 *
 * <p>Names compare case-insensitively on every protocol (documented simplification: backend names
 * that differ only by case are ambiguous; an exact-case match is preferred). The database name is
 * NOT authentication -- non-strict mode is a convenience, strict mode is the boundary -- and existing
 * authentication is unchanged.
 *
 * <p>The per-connection lookup is two hash probes over an immutable snapshot of the registry's
 * backends/sets (rebuilt lazily when {@link BackendRegistry#generation()} moves) plus a scan of the
 * (normally tiny) route list: microseconds even for 100 backends in 10 sets.
 */
public final class ConnectionRouter {

    private static final Logger log = LoggerFactory.getLogger(ConnectionRouter.class);

    public static final String PROTO_POSTGRES = "postgres";
    public static final String PROTO_MYSQL = "mysql";
    public static final String PROTO_SQLSERVER = "sqlserver";
    public static final String PROTO_ORACLE = "oracle";
    public static final String PROTO_MONGODB = "mongodb";
    public static final String PROTO_BOLT = "bolt";
    public static final String PROTO_GRPC = "grpc";
    public static final String PROTO_HTTP = "http";
    private static final java.util.Set<String> PROTOCOLS = java.util.Set.of(PROTO_POSTGRES, PROTO_MYSQL,
            PROTO_SQLSERVER, PROTO_ORACLE, PROTO_MONGODB, PROTO_BOLT, PROTO_GRPC, PROTO_HTTP);

    public enum Mode {
        /** Routes + implicit backend/set names; unknown names fall back to today's behavior. */
        IMPLICIT,
        /** As IMPLICIT, but an unknown (or blank) name is rejected. */
        STRICT,
        /** Connect-time routing disabled entirely. */
        OFF
    }

    /** One explicit route. {@code protocol}/{@code user} null = any. */
    public record Route(String protocol, String database, String user, String target, String defaultBackend) {
        public String key() {
            return (protocol == null ? "*" : protocol) + "|" + database.toLowerCase(Locale.ROOT) + "|"
                    + (user == null ? "*" : user.toLowerCase(Locale.ROOT));
        }

        boolean matches(String proto, String db, String usr) {
            if (protocol != null && !protocol.equals(proto)) {
                return false;
            }
            if (!glob(database, db)) {
                return false;
            }
            return user == null || glob(user, usr == null ? "" : usr);
        }
    }

    private record Snapshot(long generation, Map<String, String> backendsExact, Map<String, String> backendsLower,
            Map<String, String> setsExact, Map<String, String> setsLower, Map<String, List<String>> members) {
    }

    private final BackendRegistry registry;
    private final Mode mode;
    private volatile List<Route> routes = List.of();
    private volatile Snapshot snapshot;

    public ConnectionRouter(BackendRegistry registry, Mode mode) {
        this.registry = registry;
        this.mode = mode;
    }

    public static Mode modeFromEnv() {
        String raw = System.getenv("WARP_CONNECT_ROUTING");
        if (raw == null || raw.isBlank()) {
            return Mode.IMPLICIT;
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "strict" -> Mode.STRICT;
            case "off", "false", "disabled" -> Mode.OFF;
            default -> Mode.IMPLICIT;
        };
    }

    public Mode mode() {
        return mode;
    }

    public List<Route> routes() {
        return routes;
    }

    // ---- configuration -------------------------------------------------------------------------

    /** Replaces the route table from its JSON config string (null/blank = none). A malformed
     * value is logged and the PREVIOUS routes are kept (never silently widened); the admin API
     * validates strictly before anything is written. */
    public void load(String json) {
        try {
            this.routes = parse(json);
        } catch (RuntimeException e) {
            log.warn("connection routes: ignoring malformed connectionRoutes config, keeping the previous {} "
                    + "route(s) ({})", routes.size(), e.getMessage());
        }
        registry.touch();
    }

    /** Strict parse: throws {@link IllegalArgumentException} naming the offending entry. */
    public static List<Route> parse(String json) {
        List<Route> out = new ArrayList<>();
        if (json == null || json.isBlank()) {
            return List.of();
        }
        JsonElement root = JsonParser.parseString(json);
        if (!root.isJsonArray()) {
            throw new IllegalArgumentException("connectionRoutes must be a JSON array");
        }
        JsonArray array = root.getAsJsonArray();
        for (int i = 0; i < array.size(); i++) {
            if (!array.get(i).isJsonObject()) {
                throw new IllegalArgumentException("connectionRoutes[" + i + "] must be an object");
            }
            JsonObject o = array.get(i).getAsJsonObject();
            out.add(validated(str(o, "protocol"), str(o, "database"), str(o, "user"), str(o, "target"),
                    str(o, "defaultBackend"), "connectionRoutes[" + i + "]"));
        }
        return List.copyOf(out);
    }

    public static String render(List<Route> routes) {
        JsonArray array = new JsonArray();
        for (Route r : routes) {
            array.add(toJson(r));
        }
        return array.size() == 0 ? null : array.toString();
    }

    public static JsonObject toJson(Route r) {
        JsonObject o = new JsonObject();
        if (r.protocol() != null) {
            o.addProperty("protocol", r.protocol());
        }
        o.addProperty("database", r.database());
        if (r.user() != null) {
            o.addProperty("user", r.user());
        }
        o.addProperty("target", r.target());
        if (r.defaultBackend() != null) {
            o.addProperty("defaultBackend", r.defaultBackend());
        }
        return o;
    }

    public static Route validated(String protocol, String database, String user, String target,
            String defaultBackend, String where) {
        String proto = protocol == null || protocol.isBlank() || protocol.equals("*") ? null
                : protocol.trim().toLowerCase(Locale.ROOT);
        if (proto != null && !PROTOCOLS.contains(proto)) {
            throw new IllegalArgumentException(where + ": unknown protocol \"" + protocol + "\" (one of "
                    + new java.util.TreeSet<>(PROTOCOLS) + ")");
        }
        if (database == null || database.isBlank()) {
            throw new IllegalArgumentException(where + ": database is required (exact name or glob with * and ?)");
        }
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException(where + ": target is required (a backend or set name)");
        }
        String usr = user == null || user.isBlank() || user.equals("*") ? null : user.trim();
        String dflt = defaultBackend == null || defaultBackend.isBlank() ? null : defaultBackend.trim();
        return new Route(proto, database.trim(), usr, target.trim(), dflt);
    }

    private static String str(JsonObject o, String key) {
        JsonElement e = o.get(key);
        return e == null || e.isJsonNull() ? null : e.getAsString();
    }

    /** Throws {@link IllegalArgumentException} if the routes reference a missing target or contain
     * duplicate matchers. Used by the admin API before persisting. */
    public void validateAgainstRegistry(List<Route> candidate) {
        Snapshot s = snapshot();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Route r : candidate) {
            if (!seen.add(r.key())) {
                throw new IllegalArgumentException("duplicate route for protocol/database/user matcher " + r.key());
            }
            Target t = resolveTarget(r.target(), s);
            if (t == null) {
                throw new IllegalArgumentException("route target \"" + r.target()
                        + "\" is neither a backend nor a backend set");
            }
            if (r.defaultBackend() != null) {
                if (t.set == null) {
                    throw new IllegalArgumentException("defaultBackend only applies to a route whose target is a set");
                }
                if (!s.members.get(t.set).contains(r.defaultBackend())) {
                    throw new IllegalArgumentException("defaultBackend \"" + r.defaultBackend()
                            + "\" is not a member of set \"" + t.set + "\"");
                }
            }
        }
    }

    // ---- resolution ----------------------------------------------------------------------------

    private record Target(String backend, String set) {
    }

    /** Resolves one connection. {@code database}/{@code user} may be null/blank. */
    public ConnectionRoute resolve(String protocol, String database, String user) {
        if (mode == Mode.OFF) {
            return ConnectionRoute.UNROUTED;
        }
        Snapshot s = snapshot();
        String db = database == null ? "" : database.trim();
        List<Route> current = routes;
        for (int i = 0, n = current.size(); i < n; i++) {
            Route r = current.get(i);
            if (r.matches(protocol, db, user)) {
                Target t = resolveTarget(r.target(), s);
                if (t == null) {
                    return ConnectionRoute.rejected(db, "connection route for \"" + db + "\" targets \""
                            + r.target() + "\", which does not exist");
                }
                return build(t, r.defaultBackend(), db, s);
            }
        }
        if (!db.isEmpty()) {
            String backend = lookup(s.backendsExact, s.backendsLower, db);
            if (backend != null) {
                return ConnectionRoute.toBackend(backend, db);
            }
            String set = lookup(s.setsExact, s.setsLower, db);
            if (set != null) {
                return build(new Target(null, set), null, db, s);
            }
        }
        if (mode == Mode.STRICT) {
            return ConnectionRoute.rejected(db, "unknown database \"" + db + "\"");
        }
        return ConnectionRoute.UNROUTED;
    }

    private ConnectionRoute build(Target t, String defaultBackend, String requested, Snapshot s) {
        if (t.backend != null) {
            return ConnectionRoute.toBackend(t.backend, requested);
        }
        List<String> members = s.members.get(t.set);
        if (members == null || members.isEmpty()) {
            return ConnectionRoute.rejected(requested, "backend set \"" + t.set + "\" has no backends");
        }
        String dflt = defaultBackend;
        if (dflt == null && !members.contains(BackendRegistry.DEFAULT_BACKEND_NAME)) {
            dflt = members.get(0);
        }
        return ConnectionRoute.toSet(t.set, members, dflt, requested);
    }

    private Target resolveTarget(String target, Snapshot s) {
        String lower = target.toLowerCase(Locale.ROOT);
        if (lower.startsWith("db:") || lower.startsWith("backend:")) {
            String b = lookup(s.backendsExact, s.backendsLower, target.substring(target.indexOf(':') + 1).trim());
            return b == null ? null : new Target(b, null);
        }
        if (lower.startsWith("set:") || lower.startsWith("group:")) {
            String set = lookup(s.setsExact, s.setsLower, target.substring(target.indexOf(':') + 1).trim());
            return set == null ? null : new Target(null, set);
        }
        String b = lookup(s.backendsExact, s.backendsLower, target);
        if (b != null) {
            return new Target(b, null);
        }
        String set = lookup(s.setsExact, s.setsLower, target);
        return set == null ? null : new Target(null, set);
    }

    private static String lookup(Map<String, String> exact, Map<String, String> lower, String name) {
        String hit = exact.get(name);
        return hit != null ? hit : lower.get(name.toLowerCase(Locale.ROOT));
    }

    /**
     * For the frontends that do NOT run SQL through the shared pipeline (mongowire documents,
     * boltwire graph): the backends a store may use under {@code route}. {@code null} = unrouted (the
     * store's own normal placement); otherwise the Postgres-dialect backends -- the one backend of a
     * DATABASE route, or the members of a set in declaration order. An EMPTY list means the route
     * names something that cannot host such a store (e.g. a MySQL backend): the caller must refuse.
     */
    public List<String> storeBackends(ConnectionRoute route) {
        if (route == null || !route.isRouted()) {
            return null;
        }
        List<String> candidates = route.backend() != null ? List.of(route.backend())
                : registry.membersOfSet(route.setName());
        List<String> out = new ArrayList<>();
        for (String name : candidates) {
            BackendTarget t = registry.get(name);
            if (t != null && t.dialect() == SourceDialect.POSTGRES && !t.isFederationOnlyConnector()) {
                out.add(name);
            }
        }
        return out;
    }

    // ---- discovery for the admin API / UI -------------------------------------------------------

    /** The exact database name that selects this backend (its own name). */
    public String connectAsBackend(String backendName) {
        return backendName;
    }

    /** The database name that selects this set, or {@code null} when a backend of the same name
     * shadows it (a bare name resolves to the backend first; use a route to reach the set). */
    public String connectAsSet(String setName) {
        Snapshot s = snapshot();
        return lookup(s.backendsExact, s.backendsLower, setName) != null ? null : setName;
    }

    // ---- snapshot ------------------------------------------------------------------------------

    private Snapshot snapshot() {
        Snapshot s = snapshot;
        long gen = registry.generation();
        if (s != null && s.generation == gen) {
            return s;
        }
        Map<String, String> bExact = new HashMap<>();
        Map<String, String> bLower = new HashMap<>();
        for (String name : registry.orderedNames()) {
            if (registry.get(name) == null || BackendCatalogDiscovery.isReservedNativeName(name)) {
                continue;
            }
            bExact.put(name, name);
            String previous = bLower.putIfAbsent(name.toLowerCase(Locale.ROOT), name);
            if (previous != null) {
                log.warn("connection routing: backends \"{}\" and \"{}\" differ only by case; the "
                        + "case-insensitive name resolves to \"{}\"", previous, name, previous);
            }
        }
        Map<String, String> sExact = new HashMap<>();
        Map<String, String> sLower = new HashMap<>();
        Map<String, List<String>> members = new LinkedHashMap<>();
        for (String set : registry.setNames()) {
            List<String> m = registry.membersOfSet(set);
            if (m.isEmpty()) {
                continue;
            }
            members.put(set, List.copyOf(m));
            sExact.put(set, set);
            sLower.putIfAbsent(set.toLowerCase(Locale.ROOT), set);
        }
        Snapshot fresh = new Snapshot(gen, Map.copyOf(bExact), Map.copyOf(bLower), Map.copyOf(sExact),
                Map.copyOf(sLower), Map.copyOf(members));
        this.snapshot = fresh;
        return fresh;
    }

    // ---- glob ----------------------------------------------------------------------------------

    /** Case-insensitive {@code *} / {@code ?} match without regex. */
    static boolean glob(String pattern, String text) {
        int p = 0, t = 0, star = -1, mark = 0;
        int pl = pattern.length(), tl = text.length();
        while (t < tl) {
            char pc = p < pl ? pattern.charAt(p) : 0;
            if (p < pl && pc != '*'
                    && (pc == '?' || Character.toLowerCase(pc) == Character.toLowerCase(text.charAt(t)))) {
                p++;
                t++;
            } else if (p < pl && pc == '*') {
                star = p++;
                mark = t;
            } else if (star >= 0) {
                p = star + 1;
                t = ++mark;
            } else {
                return false;
            }
        }
        while (p < pl && pattern.charAt(p) == '*') {
            p++;
        }
        return p == pl;
    }
}
