package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.core.BackendConnectionPools;
import com.sayonora.warp.core.BackendConnectivityTest;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendSetModel;
import com.sayonora.warp.core.BackendSetModel.Backend;
import com.sayonora.warp.core.BackendSetModel.BackendSet;
import com.sayonora.warp.core.BackendSetModel.ModelException;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ConnectionRouter;
import com.sayonora.warp.core.StoreBootstrap;
import com.sayonora.warp.core.StoreType;
import com.sayonora.warp.mcp.BackendTypes;
import com.sayonora.warp.server.ServerOptions;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The backend-set admin API -- the one place backends are added, edited and removed:
 * <pre>
 *   GET    /api/backend-sets[?health=true]              every set with its backends
 *   POST   /api/backend-sets                            {name, description?}
 *   GET    /api/backend-sets/{set}
 *   PATCH  /api/backend-sets/{set}                      {description?, name?}   (name renames the set)
 *   DELETE /api/backend-sets/{set}                      (empty sets only; never the default backend's set)
 *   POST   /api/backend-sets/{set}/backends             {name,url,user?,password?,description?,enabledStores?[]}
 *   GET    /api/backend-sets/{set}/backends/{name}
 *   PATCH  /api/backend-sets/{set}/backends/{name}      {description?,enabledStores?,url?,user?,password?,set?}   (set moves it)
 *   DELETE /api/backend-sets/{set}/backends/{name}
 *   POST   /api/backend-sets/{set}/backends/{name}/test
 *   GET    /api/backend-stores                          the stores a Postgres backend can host
 *   GET    /api/connection-routes                       connect-time routing: mode + explicit routes
 *   POST   /api/connection-routes                       {protocol?, database, user?, target, defaultBackend?, position?}
 *   PATCH  /api/connection-routes/{id}                  {target?, defaultBackend?}   (id = protocol|database|user)
 *   DELETE /api/connection-routes/{id}
 * </pre>
 * {@code POST /api/backends} is accepted as an alias of the add-backend call but REQUIRES {@code set}.
 * Everything is persisted as a new {@code warp_config} version (hot-reloaded on every instance).
 * See docs/WARP_GUIDE.md.
 */
public final class BackendSetsApi {

    private static final Logger log = LoggerFactory.getLogger(BackendSetsApi.class);
    private static final Object WRITE_LOCK = new Object();

    private static final Pattern ROUTES = Pattern.compile("^/api/connection-routes/?$");
    private static final Pattern ROUTE = Pattern.compile("^/api/connection-routes/([^/]+)/?$");
    private static final Pattern SETS = Pattern.compile("^/api/backend-sets/?$");
    private static final Pattern SET = Pattern.compile("^/api/backend-sets/([^/]+)/?$");
    private static final Pattern SET_BACKENDS = Pattern.compile("^/api/backend-sets/([^/]+)/backends/?$");
    private static final Pattern SET_BACKEND = Pattern.compile("^/api/backend-sets/([^/]+)/backends/([^/]+)/?$");
    private static final Pattern SET_BACKEND_TEST = Pattern.compile("^/api/backend-sets/([^/]+)/backends/([^/]+)/test$");

    private BackendSetsApi() {
    }

    public static boolean handles(String target) {
        return target.equals("/api/backend-stores") || target.startsWith("/api/backend-sets")
                || target.startsWith("/api/connection-routes");
    }

    public static void handle(String target, HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore, BackendRegistry registry, ServerOptions options) throws IOException {
        response.setContentType("application/json; charset=utf-8");
        String method = request.getMethod();
        try {
            if (target.equals("/api/backend-stores") && "GET".equals(method)) {
                write(response, 200, storesCatalog());
                return;
            }
            Matcher m;
            if (ROUTES.matcher(target).matches()) {
                switch (method) {
                    case "GET" -> write(response, 200, routesJson(registry));
                    case "POST" -> addRoute(readBody(request), response, configStore, registry);
                    default -> notFound(response);
                }
                return;
            }
            if ((m = ROUTE.matcher(target)).matches()) {
                String id = decode(m.group(1));
                switch (method) {
                    case "GET" -> getRoute(id, response, registry);
                    case "PATCH", "PUT" -> patchRoute(id, readBody(request), response, configStore, registry);
                    case "DELETE" -> deleteRoute(id, response, configStore, registry);
                    default -> notFound(response);
                }
                return;
            }
            if (SETS.matcher(target).matches()) {
                if ("GET".equals(method)) {
                    write(response, 200, listSets(request, configStore, registry, options));
                } else if ("POST".equals(method)) {
                    createSet(request, response, configStore, registry, options);
                } else {
                    notFound(response);
                }
                return;
            }
            if ((m = SET_BACKEND_TEST.matcher(target)).matches() && "POST".equals(method)) {
                testBackend(m.group(1), m.group(2), response, configStore, registry, options);
                return;
            }
            if ((m = SET_BACKEND.matcher(target)).matches()) {
                String set = decode(m.group(1));
                String name = decode(m.group(2));
                switch (method) {
                    case "GET" -> getBackend(set, name, response, configStore, registry, options);
                    case "PATCH", "PUT" -> patchBackend(set, name, request, response, configStore, registry, options);
                    case "DELETE" -> deleteBackend(set, name, response, configStore, registry, options);
                    default -> notFound(response);
                }
                return;
            }
            if ((m = SET_BACKENDS.matcher(target)).matches() && "POST".equals(method)) {
                addBackend(decode(m.group(1)), readBody(request), response, configStore, registry, options);
                return;
            }
            if ((m = SET.matcher(target)).matches()) {
                String set = decode(m.group(1));
                switch (method) {
                    case "GET" -> getSet(set, request, response, configStore, registry, options);
                    case "PATCH", "PUT" -> patchSet(set, request, response, configStore, registry, options);
                    case "DELETE" -> deleteSet(set, response, configStore, registry, options);
                    default -> notFound(response);
                }
                return;
            }
            notFound(response);
        } catch (ModelException e) {
            error(response, e.status(), e.getMessage());
        } catch (JsonParseException | IllegalStateException | ClassCastException e) {
            error(response, 400, "invalid request body: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            error(response, 400, e.getMessage());
        } catch (SQLException e) {
            log.warn("backend-sets admin API: database error", e);
            error(response, 502, e.getMessage());
        }
    }

    /** {@code POST /api/backends}: legacy-path alias of add-backend that REQUIRES a set. */
    public static void addViaLegacyPath(HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore, BackendRegistry registry, ServerOptions options) throws IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            JsonObject body = readBody(request);
            addBackend(str(body, "set"), body, response, configStore, registry, options);
        } catch (ModelException e) {
            error(response, e.status(), e.getMessage());
        } catch (JsonParseException | IllegalStateException | ClassCastException e) {
            error(response, 400, "invalid request body: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            error(response, 400, e.getMessage());
        } catch (SQLException e) {
            error(response, 502, e.getMessage());
        }
    }

    // ---- reads -------------------------------------------------------------------------------

    private static JsonObject storesCatalog() {
        JsonArray arr = new JsonArray();
        for (StoreType t : StoreType.values()) {
            JsonObject o = new JsonObject();
            o.addProperty("id", t.id());
            o.addProperty("label", t.label());
            o.addProperty("description", t.description());
            o.addProperty("shardable", t.shardable());
            o.addProperty("setEnvVar", t.setEnvVar());
            arr.add(o);
        }
        JsonObject out = new JsonObject();
        out.add("stores", arr);
        return out;
    }

    private static WarpConfig latest(ConfigStore configStore) throws SQLException {
        return configStore.readLatest().map(ConfigStore.Version::payload).orElseGet(WarpConfig::fromEnvDefaults);
    }

    private static BackendTarget implicitDefault(ServerOptions o) {
        return new BackendTarget(BackendRegistry.DEFAULT_BACKEND_NAME,
                "jdbc:postgresql://" + o.pgHost() + ":" + o.pgPort() + "/" + o.pgDatabase(),
                o.pgUser(), o.pgPassword());
    }

    private static BackendSetModel model(ConfigStore configStore, ServerOptions options) throws SQLException {
        return BackendSetModel.from(latest(configStore), implicitDefault(options));
    }

    private static JsonObject listSets(HttpServletRequest request, ConfigStore configStore, BackendRegistry registry,
            ServerOptions options) throws SQLException {
        boolean health = "true".equals(request.getParameter("health"));
        BackendSetModel model = model(configStore, options);
        JsonArray sets = new JsonArray();
        for (BackendSet s : model.sets()) {
            sets.add(setJson(model, s, registry, health));
        }
        JsonObject out = new JsonObject();
        out.add("sets", sets);
        out.addProperty("maxBackends", com.sayonora.warp.license.License.current().maxBackends());
        out.addProperty("backendCount", model.allBackends().size());
        out.add("stores", storesCatalog().get("stores"));
        out.add("connectionRouting", routesJson(registry));
        return out;
    }

    private static void getSet(String set, HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore, BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        BackendSetModel model = model(configStore, options);
        BackendSet s = model.set(set);
        if (s == null) {
            error(response, 404, "backend set '" + set + "' does not exist");
            return;
        }
        write(response, 200, setJson(model, s, registry, "true".equals(request.getParameter("health"))));
    }

    private static JsonObject setJson(BackendSetModel model, BackendSet s, BackendRegistry registry, boolean health) {
        JsonObject o = new JsonObject();
        o.addProperty("name", s.name());
        o.addProperty("description", s.description());
        o.addProperty("isDefaultSet", holdsDefault(model, s.name()));
        // the exact database/service name that selects this whole set (null: a backend of the same
        // name shadows it -- reach the set through an explicit route instead)
        o.addProperty("connectAs", registry == null ? s.name() : registry.connectionRouter().connectAsSet(s.name()));
        JsonArray arr = new JsonArray();
        for (Backend b : model.backendsOf(s.name())) {
            arr.add(backendJson(model, b, registry, health));
        }
        o.add("backends", arr);
        o.add("stores", setStoresJson(model, s.name(), registry));
        return o;
    }

    private static boolean holdsDefault(BackendSetModel model, String set) {
        Backend d = model.backend(BackendRegistry.DEFAULT_BACKEND_NAME);
        return BackendRegistry.DEFAULT_SET_NAME.equals(set) || d != null && d.set().equals(set);
    }

    /** Per store: which backends of this set host it and whether that means sharding. */
    private static JsonObject setStoresJson(BackendSetModel model, String set, BackendRegistry registry) {
        JsonObject o = new JsonObject();
        for (StoreType t : StoreType.values()) {
            List<String> hosts = new ArrayList<>();
            for (Backend b : model.backendsOf(set)) {
                if (b.stores().contains(t)) {
                    hosts.add(b.name());
                }
            }
            if (hosts.isEmpty()) {
                continue;
            }
            JsonObject e = new JsonObject();
            e.add("hosts", strings(hosts));
            e.addProperty("sharded", hosts.size() > 1);
            String served = registry == null ? null : registry.frontendSet(t);
            e.addProperty("servedFromThisSet", served == null || served.equals(set));
            e.addProperty("frontendSetEnvVar", t.setEnvVar());
            o.add(t.id(), e);
        }
        return o;
    }

    private static JsonObject backendJson(BackendSetModel model, Backend b, BackendRegistry registry, boolean health) {
        BackendTarget t = new BackendTarget(b.name(), b.url(), b.user(), b.password());
        JsonObject o = new JsonObject();
        o.addProperty("name", b.name());
        o.addProperty("set", b.set());
        o.addProperty("type", BackendTypes.typeOf(t));
        o.addProperty("family", BackendTypes.kindOf(t).id());
        o.addProperty("dialect", t.dialect() == null ? null : t.dialect().name());
        o.addProperty("url", BackendSetModel.maskUrl(b.url()));
        o.addProperty("user", b.user());
        o.addProperty("description", b.description());
        o.addProperty("fallback", b.fallback());
        o.addProperty("isDefault", BackendRegistry.DEFAULT_BACKEND_NAME.equals(b.name()));
        o.addProperty("connectAs", b.name());
        List<String> ids = new ArrayList<>();
        for (StoreType s : b.stores()) {
            ids.add(s.id());
        }
        o.add("enabledStores", strings(ids));
        o.addProperty("canHostStores", b.isPostgres());
        String state = registry != null && registry.get(b.name()) != null ? registry.stateOf(b.name()).name()
                : "PENDING";
        o.addProperty("state", state);
        var pool = BackendConnectionPools.statsForBackend(b.name());
        if (pool != null) {
            JsonObject p = new JsonObject();
            p.addProperty("active", pool.activeConnections());
            p.addProperty("idle", pool.idleConnections());
            p.addProperty("total", pool.totalConnections());
            p.addProperty("max", pool.maxPoolSize());
            p.addProperty("waiting", pool.threadsAwaitingConnection());
            o.add("pool", p);
        }
        if (health) {
            var r = BackendConnectivityTest.test(b.url(), b.user(), b.password());
            JsonObject h = new JsonObject();
            h.addProperty("ok", r.ok());
            h.addProperty("message", r.message());
            h.addProperty("tookMs", r.tookMs());
            h.addProperty("serverVersion", r.serverVersion());
            o.add("health", h);
        }
        return o;
    }

    private static void getBackend(String set, String name, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        BackendSetModel model = model(configStore, options);
        Backend b = model.backend(name);
        if (b == null || !b.set().equals(set)) {
            error(response, 404, "backend '" + name + "' does not exist in backend set '" + set + "'");
            return;
        }
        write(response, 200, backendJson(model, b, registry, true));
    }

    private static void testBackend(String setRaw, String nameRaw, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        String set = decode(setRaw);
        String name = decode(nameRaw);
        BackendSetModel model = model(configStore, options);
        Backend b = model.backend(name);
        if (b == null || !b.set().equals(set)) {
            error(response, 404, "backend '" + name + "' does not exist in backend set '" + set + "'");
            return;
        }
        var r = BackendConnectivityTest.test(b.url(), b.user(), b.password());
        JsonObject o = new JsonObject();
        o.addProperty("ok", r.ok());
        o.addProperty("message", r.message());
        o.addProperty("tookMs", r.tookMs());
        o.addProperty("serverVersion", r.serverVersion());
        write(response, 200, o);
    }

    // ---- writes ------------------------------------------------------------------------------

    private static void createSet(HttpServletRequest request, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        JsonObject body = readBody(request);
        synchronized (WRITE_LOCK) {
            WarpConfig before = latest(configStore);
            BackendSetModel model = BackendSetModel.from(before, implicitDefault(options));
            BackendSet s = model.addSet(str(body, "name"), str(body, "description"));
            JsonObject out = commit(before, model, registry, configStore, options, List.of());
            out.add("set", setJson(model, s, registry, false));
            write(response, 201, out);
        }
    }

    private static void patchSet(String set, HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore, BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        JsonObject body = readBody(request);
        synchronized (WRITE_LOCK) {
            WarpConfig before = latest(configStore);
            BackendSetModel model = BackendSetModel.from(before, implicitDefault(options));
            BackendSetModel oldModel = BackendSetModel.from(before, implicitDefault(options));
            BackendSet s = model.patchSet(set, body.has("description"), str(body, "description"));
            if (body.has("name") && str(body, "name") != null && !str(body, "name").equals(set)) {
                s = model.renameSet(set, str(body, "name"));
            }
            JsonObject out = commit(before, model, registry, configStore, options, List.of(), oldModel);
            out.add("set", setJson(model, s, registry, false));
            write(response, 200, out);
        }
    }

    private static void deleteSet(String set, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        synchronized (WRITE_LOCK) {
            WarpConfig before = latest(configStore);
            BackendSetModel model = BackendSetModel.from(before, implicitDefault(options));
            model.deleteSet(set);
            JsonObject out = commit(before, model, registry, configStore, options, List.of());
            out.addProperty("deleted", set);
            write(response, 200, out);
        }
    }

    private static void addBackend(String set, JsonObject body, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        if (set == null || set.isBlank()) {
            throw new ModelException(400, "a backend must be added to a backend set -- 'set' is required "
                    + "(POST /api/backend-sets/{set}/backends)");
        }
        synchronized (WRITE_LOCK) {
            WarpConfig before = latest(configStore);
            BackendSetModel oldModel = BackendSetModel.from(before, implicitDefault(options));
            BackendSetModel model = BackendSetModel.from(before, implicitDefault(options));
            Backend b = model.addBackend(set, str(body, "name"), firstNonNull(str(body, "url"), str(body, "jdbcUrl")),
                    str(body, "user"), str(body, "password"), str(body, "fallback"), str(body, "description"),
                    stores(body));
            List<String> warnings = new ArrayList<>();
            boolean specBlank = before.backends() == null || before.backends().isBlank();
            String standby = System.getenv("WARP_STANDBY_HOST");
            if (specBlank && standby != null && !standby.isBlank()) {
                warnings.add("WARP_BACKENDS was not configured: the implicit 'default' backend was written out "
                        + "as an explicit entry, which drops its standby/failover settings (WARP_STANDBY_*).");
            }
            JsonObject out = commit(before, model, registry, configStore, options, warnings, oldModel);
            out.add("backend", backendJson(model, b, registry, false));
            write(response, 201, out);
        }
    }

    private static void patchBackend(String set, String name, HttpServletRequest request, HttpServletResponse response,
            ConfigStore configStore, BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        JsonObject body = readBody(request);
        synchronized (WRITE_LOCK) {
            WarpConfig before = latest(configStore);
            BackendSetModel oldModel = BackendSetModel.from(before, implicitDefault(options));
            BackendSetModel model = BackendSetModel.from(before, implicitDefault(options));
            Backend existing = model.backend(name);
            if (existing == null || !existing.set().equals(set)) {
                throw new ModelException(404, "backend '" + name + "' does not exist in backend set '" + set + "'");
            }
            Backend b = model.patchBackend(name, str(body, "url"), str(body, "user"),
                    // blank password keeps the stored one (the API never returns it, so clients cannot resend it)
                    body.has("password") && !str(body, "password").isBlank() ? str(body, "password") : null,
                    body.has("description"), str(body, "description"),
                    body.has("enabledStores") ? stores(body) : null);
            if (body.has("set") && str(body, "set") != null && !set.equals(str(body, "set"))) {
                b = model.moveBackend(name, str(body, "set"));
            }
            JsonObject out = commit(before, model, registry, configStore, options, List.of(), oldModel);
            out.add("backend", backendJson(model, b, registry, false));
            write(response, 200, out);
        }
    }

    private static void deleteBackend(String set, String name, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry, ServerOptions options) throws SQLException, IOException {
        synchronized (WRITE_LOCK) {
            WarpConfig before = latest(configStore);
            BackendSetModel oldModel = BackendSetModel.from(before, implicitDefault(options));
            BackendSetModel model = BackendSetModel.from(before, implicitDefault(options));
            Backend existing = model.backend(name);
            if (existing == null || !existing.set().equals(set)) {
                throw new ModelException(404, "backend '" + name + "' does not exist in backend set '" + set + "'");
            }
            model.deleteBackend(name);
            List<String> warnings = new ArrayList<>();
            if (!existing.stores().isEmpty()) {
                warnings.add("backend '" + name + "' hosted " + BackendSetModelAccess.ids(existing.stores())
                        + ": its data is NOT deleted and stays in that database, but Warp no longer serves it; "
                        + "the remaining hosts are re-hashed, so see rebalanceRequired");
            }
            JsonObject out = commit(before, model, registry, configStore, options, warnings, oldModel);
            out.addProperty("deleted", name);
            write(response, 200, out);
        }
    }

    private static JsonObject commit(WarpConfig before, BackendSetModel model, BackendRegistry registry,
            ConfigStore configStore, ServerOptions options, List<String> warnings) throws SQLException {
        return commit(before, model, registry, configStore, options, warnings,
                BackendSetModel.from(before, implicitDefault(options)));
    }

    /**
     * Validates the resulting config as a whole (the registry parser rejects dangling references),
     * creates schema on newly-enabled (backend, store) pairs BEFORE persisting (a failure there
     * leaves the config untouched), writes one new warp_config version, applies it locally at once
     * (other instances pick it up over LISTEN/NOTIFY) and reports stores whose shard layout changed.
     */
    private static JsonObject commit(WarpConfig before, BackendSetModel model, BackendRegistry registry,
            ConfigStore configStore, ServerOptions options, List<String> warnings, BackendSetModel oldModel)
            throws SQLException {
        model.validateStores();
        WarpConfig after = model.applyTo(before);
        BackendRegistry beforeReg = hypothetical(before, options);
        BackendRegistry afterReg = hypothetical(after, options); // throws IllegalArgumentException on dangling refs
        // a connect-time route may not be left pointing at a backend/set this change removes
        try {
            afterReg.connectionRouter().load(after.connectionRoutes());
            afterReg.connectionRouter().validateAgainstRegistry(afterReg.connectionRouter().routes());
        } catch (IllegalArgumentException e) {
            throw new ModelException(409, e.getMessage() + " -- remove or change the connection route first");
        }

        // schema for newly enabled (backend, store) pairs
        for (Backend b : model.allBackends()) {
            Backend old = oldModel.backend(b.name());
            for (StoreType s : b.stores()) {
                boolean isNew = old == null || !old.stores().contains(s) || !old.url().equals(b.url());
                if (!isNew) {
                    continue;
                }
                BackendTarget target = new BackendTarget(b.name(), b.url(), b.user(), b.password());
                try {
                    StoreBootstrap.ensure(target, s);
                } catch (SQLException | RuntimeException e) {
                    throw new ModelException(502, "could not create the " + s.id() + " schema on backend '" + b.name()
                            + "': " + e.getMessage() + " -- nothing was changed");
                }
            }
        }

        JsonArray rebalance = new JsonArray();
        for (StoreType s : StoreType.values()) {
            List<String> was = effective(beforeReg, s);
            List<String> now = effective(afterReg, s);
            if (!was.equals(now)) {
                JsonObject r = new JsonObject();
                r.addProperty("store", s.id());
                r.add("before", strings(was));
                r.add("after", strings(now));
                r.addProperty("message", "the hosts of " + s.id() + " changed from " + was + " to " + now
                        + ": Warp does NOT move existing data. Keys are re-hashed over the new host list, so data "
                        + "written earlier may no longer be found until it is copied to the host it now hashes to "
                        + "(see 'Rebalancing' in docs/WARP_GUIDE.md).");
                rebalance.add(r);
                log.warn("backend sets: stores: {} hosts changed {} -> {}; existing data is NOT rebalanced",
                        s.id(), was, now);
            }
        }

        long version;
        version = configStore.write(after);
        if (registry != null) {
            try {
                registry.reload(after.backends(), after.shardBackends(), after.backendSets(), after.backendGroups());
                registry.applyDescriptions(after.backendDescriptions(), after.backendGroupDescriptions());
                registry.applyStoreConfig(after.backendStores(), after.backendSetNames());
                registry.connectionRouter().load(after.connectionRoutes());
            } catch (RuntimeException e) {
                log.warn("backend sets: local immediate apply failed (the LISTEN/NOTIFY reload will retry): {}", e.toString());
            }
        }
        JsonObject out = new JsonObject();
        out.addProperty("ok", true);
        out.addProperty("version", version);
        out.add("rebalanceRequired", rebalance);
        out.add("warnings", strings(warnings));
        return out;
    }

    private static BackendRegistry hypothetical(WarpConfig c, ServerOptions options) {
        BackendRegistry r = BackendRegistry.fromConfig(c.backends(), c.shardBackends(), c.backendSets(),
                c.backendGroups(), implicitDefault(options), Map.of());
        r.applyStoreConfig(c.backendStores(), c.backendSetNames());
        return r;
    }

    /** Where a store's data effectively lives: its hosts, else the legacy shard group / default backend. */
    private static List<String> effective(BackendRegistry r, StoreType s) {
        List<String> hosts = r.storeHosts(s);
        if (!hosts.isEmpty()) {
            return hosts;
        }
        boolean legacyShardable = s == StoreType.DYNAMODB || s == StoreType.MONGODB || s == StoreType.SQS
                || s == StoreType.OPENSEARCH;
        if (legacyShardable && !r.shardGroup().isEmpty()) {
            return r.shardGroup();
        }
        return List.of(BackendRegistry.DEFAULT_BACKEND_NAME);
    }

    // ---- connect-time routes -------------------------------------------------------------------

    private static JsonObject routeJson(ConnectionRouter.Route r, BackendRegistry registry) {
        JsonObject o = ConnectionRouter.toJson(r);
        o.addProperty("id", r.key());
        String t = r.target().toLowerCase(java.util.Locale.ROOT);
        String kind = t.startsWith("set:") || t.startsWith("group:") ? "set"
                : t.startsWith("db:") || t.startsWith("backend:") ? "backend"
                : registry != null && registry.get(r.target()) != null ? "backend" : "set";
        o.addProperty("targetKind", kind);
        return o;
    }

    private static JsonObject routesJson(BackendRegistry registry) {
        JsonObject out = new JsonObject();
        ConnectionRouter router = registry.connectionRouter();
        out.addProperty("mode", router.mode().name().toLowerCase(java.util.Locale.ROOT));
        JsonArray arr = new JsonArray();
        for (ConnectionRouter.Route r : router.routes()) {
            arr.add(routeJson(r, registry));
        }
        out.add("routes", arr);
        return out;
    }

    private static ConnectionRouter.Route findRoute(String id, BackendRegistry registry) {
        for (ConnectionRouter.Route r : registry.connectionRouter().routes()) {
            if (r.key().equals(id)) {
                return r;
            }
        }
        return null;
    }

    private static void getRoute(String id, HttpServletResponse response, BackendRegistry registry) throws IOException {
        ConnectionRouter.Route r = findRoute(id, registry);
        if (r == null) {
            error(response, 404, "no connection route '" + id + "'");
            return;
        }
        write(response, 200, routeJson(r, registry));
    }

    /** Persists {@code routes} as a new warp_config version and applies it locally at once. */
    private static long commitRoutes(List<ConnectionRouter.Route> routes, ConfigStore configStore,
            BackendRegistry registry) throws SQLException {
        registry.connectionRouter().validateAgainstRegistry(routes);
        WarpConfig before = latest(configStore);
        long version = configStore.write(before.withConnectionRoutes(ConnectionRouter.render(routes)));
        registry.connectionRouter().load(ConnectionRouter.render(routes));
        return version;
    }

    private static void addRoute(JsonObject body, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry) throws SQLException, IOException {
        ConnectionRouter.Route added = ConnectionRouter.validated(str(body, "protocol"), str(body, "database"),
                str(body, "user"), str(body, "target"), str(body, "defaultBackend"), "route");
        synchronized (WRITE_LOCK) {
            List<ConnectionRouter.Route> routes = new ArrayList<>(reloadedRoutes(configStore, registry));
            for (ConnectionRouter.Route r : routes) {
                if (r.key().equals(added.key())) {
                    error(response, 409, "a route for '" + added.key() + "' already exists");
                    return;
                }
            }
            JsonElement pos = body.get("position");
            int at = pos == null || pos.isJsonNull() ? routes.size() : Math.max(0, Math.min(pos.getAsInt(), routes.size()));
            routes.add(at, added);
            long version = commitRoutes(routes, configStore, registry);
            JsonObject out = routeJson(added, registry);
            out.addProperty("version", version);
            write(response, 201, out);
        }
    }

    private static void patchRoute(String id, JsonObject body, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry) throws SQLException, IOException {
        synchronized (WRITE_LOCK) {
            List<ConnectionRouter.Route> routes = new ArrayList<>(reloadedRoutes(configStore, registry));
            for (int i = 0; i < routes.size(); i++) {
                ConnectionRouter.Route r = routes.get(i);
                if (!r.key().equals(id)) {
                    continue;
                }
                ConnectionRouter.Route changed = ConnectionRouter.validated(r.protocol(), r.database(), r.user(),
                        body.has("target") ? str(body, "target") : r.target(),
                        body.has("defaultBackend") ? str(body, "defaultBackend") : r.defaultBackend(), "route");
                routes.set(i, changed);
                long version = commitRoutes(routes, configStore, registry);
                JsonObject out = routeJson(changed, registry);
                out.addProperty("version", version);
                write(response, 200, out);
                return;
            }
            error(response, 404, "no connection route '" + id + "'");
        }
    }

    private static void deleteRoute(String id, HttpServletResponse response, ConfigStore configStore,
            BackendRegistry registry) throws SQLException, IOException {
        synchronized (WRITE_LOCK) {
            List<ConnectionRouter.Route> routes = new ArrayList<>(reloadedRoutes(configStore, registry));
            if (!routes.removeIf(r -> r.key().equals(id))) {
                error(response, 404, "no connection route '" + id + "'");
                return;
            }
            long version = commitRoutes(routes, configStore, registry);
            JsonObject out = new JsonObject();
            out.addProperty("ok", true);
            out.addProperty("version", version);
            write(response, 200, out);
        }
    }

    /** The persisted route list (the source of truth for a read-modify-write), falling back to the
     * live one when nothing is persisted yet. */
    private static List<ConnectionRouter.Route> reloadedRoutes(ConfigStore configStore, BackendRegistry registry)
            throws SQLException {
        return ConnectionRouter.parse(latest(configStore).connectionRoutes());
    }

    // ---- small helpers -----------------------------------------------------------------------

    private static List<StoreType> stores(JsonObject body) {
        JsonElement e = body.get("enabledStores");
        if (e == null || e.isJsonNull()) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        for (JsonElement x : e.getAsJsonArray()) {
            names.add(x.getAsString());
        }
        return BackendSetModel.parseStores(names);
    }

    private static String str(JsonObject body, String key) {
        JsonElement e = body.get(key);
        return e == null || e.isJsonNull() ? null : e.getAsString();
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }

    private static JsonArray strings(List<String> values) {
        JsonArray a = new JsonArray();
        values.forEach(a::add);
        return a;
    }

    private static JsonObject readBody(HttpServletRequest request) throws IOException {
        JsonElement e = JsonParser.parseReader(request.getReader());
        if (e == null || !e.isJsonObject()) {
            throw new ModelException(400, "a JSON object body is required");
        }
        return e.getAsJsonObject();
    }

    private static String decode(String s) {
        return java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void write(HttpServletResponse response, int status, JsonElement body) throws IOException {
        response.setStatus(status);
        response.getWriter().write(body.toString());
    }

    private static void error(HttpServletResponse response, int status, String message) throws IOException {
        JsonObject o = new JsonObject();
        o.addProperty("error", message);
        response.setStatus(status);
        response.getWriter().write(o.toString());
    }

    private static void notFound(HttpServletResponse response) throws IOException {
        error(response, 404, "no such route");
    }

    /** Tiny indirection so the error text above reads naturally. */
    private static final class BackendSetModelAccess {
        static List<String> ids(List<StoreType> stores) {
            List<String> out = new ArrayList<>();
            stores.forEach(s -> out.add(s.id()));
            return out;
        }
    }
}
