package com.sayonora.warp.core;

import com.google.gson.JsonObject;
import com.sayonora.warp.config.WarpConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The editable, validated view of "backend sets and their backends" that the admin API works on.
 * It is derived from -- and rendered back onto -- the flat {@link WarpConfig} strings
 * ({@code backends}, {@code backendGroups}, {@code backendDescriptions},
 * {@code backendGroupDescriptions}, {@code backendStores}, {@code backendSetNames}), so a backend
 * set is persisted in {@code warp_config} and hot-reloaded across instances like any other config.
 *
 * <p>Set reconciliation: a user-facing backend set is the mandatory-partition
 * {@code WARP_BACKEND_GROUPS} concept (every backend in exactly one set). A backend with no
 * declared group belongs to the implicit set {@value BackendRegistry#DEFAULT_SET_NAME}, which is
 * how an existing config is "migrated" without any rewrite. The legacy multi-membership
 * {@code WARP_BACKEND_SETS} (router aliases) is not touched by this model.
 *
 * <p>All rule violations throw {@link ModelException}, carrying the HTTP status the admin API
 * should answer with.
 */
public final class BackendSetModel {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,62}");

    public static final class ModelException extends IllegalArgumentException {
        private final int status;

        public ModelException(int status, String message) {
            super(message);
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    public record Backend(String name, String url, String user, String password, String fallback,
            String description, List<StoreType> stores, String set) {

        Backend with(String url, String user, String password, String description, List<StoreType> stores) {
            return new Backend(name, url, user, password, fallback, description, stores, set);
        }

        public SourceDialect dialect() {
            return new BackendTarget(name, url, user, password).dialect();
        }

        public boolean isPostgres() {
            return dialect() == SourceDialect.POSTGRES;
        }
    }

    public record BackendSet(String name, String description, boolean sharded) {
    }

    private final LinkedHashMap<String, Backend> backends = new LinkedHashMap<>();
    private final LinkedHashMap<String, BackendSet> sets = new LinkedHashMap<>();
    private final BackendTarget implicitDefault;
    private boolean specWasBlank;

    private BackendSetModel(BackendTarget implicitDefault) {
        this.implicitDefault = implicitDefault;
    }

    // ---- reading -----------------------------------------------------------------------------

    /**
     * @param implicitDefault the Warp-primary Postgres, used as backend {@code default} when the
     *     config has no {@code WARP_BACKENDS} spec at all (may be {@code null} in unit tests)
     */
    public static BackendSetModel from(WarpConfig c, BackendTarget implicitDefault) {
        BackendSetModel m = new BackendSetModel(implicitDefault);
        Map<String, String> descriptions = BackendRegistry.parseDescriptionMap("backendDescriptions", c.backendDescriptions());
        Map<String, String> setDescriptions = BackendRegistry.parseDescriptionMap("backendGroupDescriptions",
                c.backendGroupDescriptions());
        Map<String, List<StoreType>> stores = BackendRegistry.parseStoreSpec(c.backendStores());

        // group membership + sharded flag (tolerant parse: the registry itself validates strictly)
        Map<String, String> groupOf = new LinkedHashMap<>();
        Map<String, Boolean> groupSharded = new LinkedHashMap<>();
        if (c.backendGroups() != null && !c.backendGroups().isBlank()) {
            for (String entry : c.backendGroups().split("\\|")) {
                int eq = entry.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String rawName = entry.substring(0, eq).trim();
                boolean sharded = false;
                int colon = rawName.indexOf(':');
                String name = rawName;
                if (colon >= 0) {
                    name = rawName.substring(0, colon).trim();
                    sharded = rawName.substring(colon + 1).trim().equalsIgnoreCase("sharded");
                }
                groupSharded.put(name, sharded);
                for (String member : entry.substring(eq + 1).split(",")) {
                    if (!member.isBlank()) {
                        groupOf.putIfAbsent(member.trim(), name);
                    }
                }
            }
        }

        String spec = c.backends();
        m.specWasBlank = spec == null || spec.isBlank();
        if (m.specWasBlank) {
            if (implicitDefault != null) {
                m.backends.put(BackendRegistry.DEFAULT_BACKEND_NAME, new Backend(BackendRegistry.DEFAULT_BACKEND_NAME,
                        implicitDefault.jdbcUrl(), implicitDefault.user(), implicitDefault.password(), null,
                        descriptions.get(BackendRegistry.DEFAULT_BACKEND_NAME),
                        stores.getOrDefault(BackendRegistry.DEFAULT_BACKEND_NAME, List.of()),
                        setName(groupOf.get(BackendRegistry.DEFAULT_BACKEND_NAME))));
            }
        } else {
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
                String fallback = parts.length > 3 && !parts[3].isBlank() ? parts[3].trim() : null;
                m.backends.put(name, new Backend(name, url, user, password, fallback, descriptions.get(name),
                        stores.getOrDefault(name, List.of()), setName(groupOf.get(name))));
            }
        }

        // sets: default first, then declared, then group-derived
        boolean defaultUsed = false;
        for (Backend b : m.backends.values()) {
            defaultUsed |= BackendRegistry.DEFAULT_SET_NAME.equals(b.set());
        }
        List<String> declared = new ArrayList<>();
        if (c.backendSetNames() != null) {
            for (String n : c.backendSetNames().split(",")) {
                if (!n.isBlank()) {
                    declared.add(n.trim());
                }
            }
        }
        if (defaultUsed || declared.contains(BackendRegistry.DEFAULT_SET_NAME)) {
            m.sets.put(BackendRegistry.DEFAULT_SET_NAME, new BackendSet(BackendRegistry.DEFAULT_SET_NAME,
                    setDescriptions.get(BackendRegistry.DEFAULT_SET_NAME),
                    groupSharded.getOrDefault(BackendRegistry.DEFAULT_SET_NAME, false)));
        }
        for (String n : declared) {
            m.sets.putIfAbsent(n, new BackendSet(n, setDescriptions.get(n), groupSharded.getOrDefault(n, false)));
        }
        for (String n : groupSharded.keySet()) {
            m.sets.putIfAbsent(n, new BackendSet(n, setDescriptions.get(n), groupSharded.get(n)));
        }
        return m;
    }

    private static String setName(String group) {
        return group == null || group.isBlank() ? BackendRegistry.DEFAULT_SET_NAME : group;
    }

    public List<BackendSet> sets() {
        return List.copyOf(sets.values());
    }

    public BackendSet set(String name) {
        return sets.get(name);
    }

    public List<Backend> backendsOf(String set) {
        List<Backend> out = new ArrayList<>();
        for (Backend b : backends.values()) {
            if (b.set().equals(set)) {
                out.add(b);
            }
        }
        return out;
    }

    public Backend backend(String name) {
        return backends.get(name);
    }

    public List<Backend> allBackends() {
        return List.copyOf(backends.values());
    }

    // ---- mutations (each validates and throws ModelException) --------------------------------

    public BackendSet addSet(String name, String description) {
        checkSetName(name);
        if (sets.containsKey(name) || BackendRegistry.DEFAULT_SET_NAME.equals(name)) {
            throw new ModelException(409, "backend set '" + name + "' already exists");
        }
        BackendSet s = new BackendSet(name, blankToNull(description), false);
        sets.put(name, s);
        return s;
    }

    public BackendSet patchSet(String name, boolean hasDescription, String description) {
        BackendSet s = requireSet(name);
        if (hasDescription) {
            s = new BackendSet(name, blankToNull(description), s.sharded());
            sets.put(name, s);
        }
        return s;
    }

    public void deleteSet(String name) {
        requireSet(name);
        if (BackendRegistry.DEFAULT_SET_NAME.equals(name)) {
            throw new ModelException(409, "the '" + name + "' backend set cannot be deleted");
        }
        List<Backend> members = backendsOf(name);
        for (Backend b : members) {
            if (BackendRegistry.DEFAULT_BACKEND_NAME.equals(b.name())) {
                throw new ModelException(409, "backend set '" + name + "' holds the '"
                        + BackendRegistry.DEFAULT_BACKEND_NAME + "' backend and cannot be deleted");
            }
        }
        if (!members.isEmpty()) {
            throw new ModelException(409, "backend set '" + name + "' is not empty (" + members.size()
                    + " backend(s): " + names(members) + ") -- delete or move its backends first");
        }
        sets.remove(name);
    }

    public Backend addBackend(String set, String name, String url, String user, String password, String fallback,
            String description, List<StoreType> stores) {
        if (set == null || set.isBlank()) {
            throw new ModelException(400, "a backend must be added to a backend set -- 'set' is required");
        }
        if (!sets.containsKey(set)) {
            if (BackendRegistry.DEFAULT_SET_NAME.equals(set)) {
                sets.put(set, new BackendSet(set, null, false));
            } else {
                throw new ModelException(404, "backend set '" + set + "' does not exist -- create it first");
            }
        }
        checkBackendName(name);
        if (backends.containsKey(name)) {
            throw new ModelException(409, "backend '" + name + "' already exists");
        }
        if (url == null || url.isBlank()) {
            throw new ModelException(400, "url is required");
        }
        checkSpecSafe("url", url.replace(";", ""));
        checkSpecSafe("user", user);
        checkSpecSafe("password", password);
        if (fallback != null && !fallback.isBlank() && !backends.containsKey(fallback.trim())) {
            throw new ModelException(400, "fallback backend '" + fallback + "' does not exist");
        }
        TrustedBackendHosts trusted = TrustedBackendHosts.fromEnv();
        if (!trusted.isTrusted(url)) {
            throw new ModelException(400, "the host of this backend is not in WARP_TRUSTED_BACKEND_HOSTS");
        }
        int max = com.sayonora.warp.license.License.current().maxBackends();
        if (backends.size() >= max) {
            throw new ModelException(400, "license: this edition is capped at " + max + " backends of any engine ("
                    + backends.size() + " already configured) -- remove one or use an Enterprise license");
        }
        Backend b = new Backend(name, url.trim(), user, password, fallback == null || fallback.isBlank() ? null
                : fallback.trim(), blankToNull(description), List.copyOf(stores == null ? List.of() : stores), set);
        backends.put(name, b);
        try {
            validateStores();
        } catch (ModelException e) {
            backends.remove(name);
            throw e;
        }
        return b;
    }

    /** Fields passed as {@code null} are left unchanged; pass {@code stores} to replace the list. */
    public Backend patchBackend(String name, String url, String user, String password, boolean hasDescription,
            String description, List<StoreType> stores) {
        Backend b = backends.get(name);
        if (b == null) {
            throw new ModelException(404, "backend '" + name + "' does not exist");
        }
        if (url != null) {
            if (url.isBlank()) {
                throw new ModelException(400, "url must not be blank");
            }
            if (!TrustedBackendHosts.fromEnv().isTrusted(url)) {
                throw new ModelException(400, "the host of this backend is not in WARP_TRUSTED_BACKEND_HOSTS");
            }
            checkSpecSafe("url", url.replace(";", ""));
        }
        checkSpecSafe("user", user);
        checkSpecSafe("password", password);
        Backend updated = b.with(url != null ? url.trim() : b.url(), user != null ? user : b.user(),
                password != null ? password : b.password(),
                hasDescription ? blankToNull(description) : b.description(),
                stores != null ? List.copyOf(stores) : b.stores());
        backends.put(name, updated);
        try {
            validateStores();
        } catch (ModelException e) {
            backends.put(name, b);
            throw e;
        }
        return updated;
    }

    public void deleteBackend(String name) {
        Backend b = backends.get(name);
        if (b == null) {
            throw new ModelException(404, "backend '" + name + "' does not exist");
        }
        if (BackendRegistry.DEFAULT_BACKEND_NAME.equals(name)) {
            throw new ModelException(409, "the '" + name + "' backend cannot be deleted -- Warp's own frontends "
                    + "rely on it");
        }
        backends.remove(name);
    }

    /** Store rules: only Postgres backends host stores; Neo4j on at most one backend per set. */
    public void validateStores() {
        Map<String, String> neo4jHost = new LinkedHashMap<>();
        for (Backend b : backends.values()) {
            if (b.stores().isEmpty()) {
                continue;
            }
            if (!b.isPostgres()) {
                throw new ModelException(400, "only Postgres backends can host stores, but backend '" + b.name()
                        + "' is not Postgres -- remove enabledStores " + storeIds(b.stores()) + " from it");
            }
            if (b.stores().contains(StoreType.NEO4J)) {
                String other = neo4jHost.putIfAbsent(b.set(), b.name());
                if (other != null) {
                    throw new ModelException(400, "Neo4j can be enabled on only ONE backend per backend set (set '"
                            + b.set() + "' already hosts it on '" + other + "', so '" + b.name() + "' cannot): "
                            + "graph traversals cannot be answered correctly when the graph is spread across "
                            + "several databases, so the graph store is not sharded");
                }
            }
        }
    }

    // ---- rendering back onto WarpConfig ------------------------------------------------------

    public WarpConfig applyTo(WarpConfig base) {
        // backends spec
        String spec;
        if (specWasBlank && backends.size() == (implicitDefault != null ? 1 : 0) && backends.containsKey(
                BackendRegistry.DEFAULT_BACKEND_NAME)) {
            spec = null; // still just the implicit default: keep it implicit
        } else {
            StringBuilder sb = new StringBuilder();
            for (Backend b : backends.values()) {
                if (sb.length() > 0) {
                    sb.append(';');
                }
                sb.append(b.name()).append('=').append(b.url().replace(";", "%3B"));
                boolean hasFallback = b.fallback() != null;
                boolean hasPassword = b.password() != null;
                boolean hasUser = b.user() != null;
                if (hasUser || hasPassword || hasFallback) {
                    sb.append('|').append(b.user() == null ? "" : b.user());
                }
                if (hasPassword || hasFallback) {
                    sb.append('|').append(b.password() == null ? "" : b.password());
                }
                if (hasFallback) {
                    sb.append('|').append(b.fallback());
                }
            }
            spec = sb.length() == 0 ? null : sb.toString();
        }

        // groups: every non-default set with members; default only when flagged sharded
        StringBuilder groups = new StringBuilder();
        for (BackendSet s : sets.values()) {
            List<Backend> members = backendsOf(s.name());
            if (members.isEmpty()) {
                continue;
            }
            boolean isDefault = BackendRegistry.DEFAULT_SET_NAME.equals(s.name());
            if (isDefault && !s.sharded()) {
                continue;
            }
            if (groups.length() > 0) {
                groups.append('|');
            }
            groups.append(s.name()).append(s.sharded() ? ":sharded" : "").append('=').append(names(members));
        }

        JsonObject bd = new JsonObject();
        for (Backend b : backends.values()) {
            if (b.description() != null && !b.description().isBlank()) {
                bd.addProperty(b.name(), b.description());
            }
        }
        JsonObject sd = new JsonObject();
        for (BackendSet s : sets.values()) {
            if (s.description() != null && !s.description().isBlank()) {
                sd.addProperty(s.name(), s.description());
            }
        }
        Map<String, List<StoreType>> stores = new LinkedHashMap<>();
        for (Backend b : backends.values()) {
            if (!b.stores().isEmpty()) {
                stores.put(b.name(), b.stores());
            }
        }
        List<String> setNames = new ArrayList<>();
        for (BackendSet s : sets.values()) {
            if (!BackendRegistry.DEFAULT_SET_NAME.equals(s.name())) {
                setNames.add(s.name());
            }
        }
        return base.withBackendModel(spec, groups.length() == 0 ? null : groups.toString(),
                bd.size() == 0 ? null : bd.toString(), sd.size() == 0 ? null : sd.toString(),
                BackendRegistry.renderStoreSpec(stores), setNames.isEmpty() ? null : String.join(",", setNames));
    }

    // ---- helpers -----------------------------------------------------------------------------

    private BackendSet requireSet(String name) {
        BackendSet s = sets.get(name);
        if (s == null) {
            throw new ModelException(404, "backend set '" + name + "' does not exist");
        }
        return s;
    }

    private static void checkSetName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new ModelException(400, "backend set name must be 1-63 characters of letters, digits, '_', '-' "
                    + "or '.', starting with a letter or digit");
        }
        if (name.equals(BackendRegistry.UNGROUPED_GROUP_NAME)) {
            throw new ModelException(400, "'" + name + "' is a reserved name");
        }
    }

    private static void checkBackendName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new ModelException(400, "backend name must be 1-63 characters of letters, digits, '_', '-' "
                    + "or '.', starting with a letter or digit");
        }
        if (BackendCatalogDiscovery.isReservedNativeName(name)) {
            throw new ModelException(400, "'" + name + "' is a reserved backend name");
        }
    }

    private static void checkSpecSafe(String field, String value) {
        if (value != null && (value.indexOf('|') >= 0 || value.indexOf(';') >= 0 || value.indexOf('\n') >= 0)) {
            throw new ModelException(400, field + " must not contain '|', ';' or newlines (the backend spec "
                    + "format cannot represent them)");
        }
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }

    static String names(List<Backend> members) {
        List<String> n = new ArrayList<>();
        for (Backend b : members) {
            n.add(b.name());
        }
        return String.join(",", n);
    }

    static List<String> storeIds(List<StoreType> stores) {
        List<String> out = new ArrayList<>();
        for (StoreType t : stores) {
            out.add(t.id());
        }
        return out;
    }

    /** Parses a JSON list of store names into validated {@link StoreType}s (deduplicated, ordered). */
    public static List<StoreType> parseStores(Iterable<String> names) {
        List<StoreType> out = new ArrayList<>();
        for (String n : names) {
            try {
                StoreType t = StoreType.parse(n);
                if (!out.contains(t)) {
                    out.add(t);
                }
            } catch (IllegalArgumentException e) {
                throw new ModelException(400, e.getMessage());
            }
        }
        return out;
    }

    /** URL with any embedded credential ({@code user:pass@} or {@code password=} parameter) masked. */
    public static String maskUrl(String url) {
        if (url == null) {
            return null;
        }
        String masked = url.replaceAll("(?i)(password|pwd|secret|token)=[^&;]*", "$1=****");
        masked = masked.replaceAll("(://[^/:@]+:)[^@/]*@", "$1****@");
        return masked;
    }
}
