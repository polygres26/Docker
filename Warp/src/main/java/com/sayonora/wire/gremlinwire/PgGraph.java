package com.sayonora.wire.gremlinwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ShardingStrategy;
import com.sayonora.wire.core.StoreBootstrap;
import com.sayonora.wire.core.StoreType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Postgres property-graph store of gremlinwire, sharded over the backends of the set that enable the {@code gremlin} store.
 * A vertex lives on the host owning hash(vertex key); an edge lives WITH ITS OUT VERTEX, so out-edge lookups touch one host and
 * in-edge lookups (and lookups of an edge by id) scatter over every host and merge. Every unit of work borrows a pooled
 * connection for one short statement or transaction and returns it: nothing is held while a client reads results.
 * Writes that span hosts (dropping a vertex with in-edges stored elsewhere) are one transaction per host, not distributed ones.
 */
final class PgGraph implements GraphStore {

    private static final Logger log = LoggerFactory.getLogger(PgGraph.class);
    private static final int PAGE = 500;
    private static final String VCOLS = "vkey, label, props::text";
    private static final String ECOLS = "ekey, label, out_key, in_key, out_label, in_label, props::text";

    private final BackendRegistry registry;

    PgGraph(BackendRegistry registry) {
        this.registry = registry;
    }

    boolean available() {
        return !registry.storeHosts(StoreType.GREMLIN).isEmpty();
    }

    List<String> hosts() {
        List<String> h = registry.storeHosts(StoreType.GREMLIN);
        if (h.isEmpty()) {
            throw new G.GremlinError(500, "No Postgres backend of this set has the gremlin store enabled");
        }
        return h;
    }

    String owner(List<String> hosts, String key) {
        return hosts.size() == 1 ? hosts.get(0) : ShardingStrategy.hash(hosts).resolve(key);
    }

    @FunctionalInterface
    interface SqlFn<T> {
        T apply(Connection c) throws SQLException;
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("gremlinwire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, StoreType.GREMLIN);
        return t;
    }

    <T> T conn(String host, SqlFn<T> fn) {
        try (Connection c = target(host).open()) {
            return fn.apply(c);
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    <T> T tx(String host, SqlFn<T> fn) {
        try (Connection c = target(host).openManualCommit()) {
            try {
                T out = fn.apply(c);
                c.commit();
                return out;
            } catch (SQLException | RuntimeException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // the pool discards the connection
                }
                throw e;
            } finally {
                try {
                    c.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // pool resets on return
                }
            }
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    static G.GremlinError storage(SQLException e) {
        log.error("gremlinwire: Postgres store error", e);
        return new G.GremlinError(500, "The graph store is currently unavailable (" + e.getMessage() + ")");
    }

    // ------------------------------------------------------------------ row <-> element

    private static long synth(String vkey, String key, int idx) {
        return Integer.toUnsignedLong(Objects.hash(vkey, key, idx));
    }

    static G.Vertex vertexOf(String vkey, String label, String propsJson) {
        G.Vertex v = new G.Vertex(GraphStore.idOf(vkey), label);
        JsonObject o = JsonParser.parseString(propsJson).getAsJsonObject();
        for (Map.Entry<String, JsonElement> e : o.entrySet()) {
            List<G.VProp> l = new ArrayList<>();
            int idx = 0;
            for (JsonElement x : e.getValue().getAsJsonArray()) {
                JsonObject po = x.getAsJsonObject();
                G.VProp p = new G.VProp(synth(vkey, e.getKey(), idx++), e.getKey(), GraphSon.V3.read(po.get("v")));
                p.vertex = v;
                if (po.has("m")) {
                    p.meta = new LinkedHashMap<>();
                    for (Map.Entry<String, JsonElement> m : po.getAsJsonObject("m").entrySet()) {
                        p.meta.put(m.getKey(), GraphSon.V3.read(m.getValue()));
                    }
                }
                l.add(p);
            }
            if (!l.isEmpty()) {
                v.props.put(e.getKey(), l);
            }
        }
        return v;
    }

    static String propsJson(G.Vertex v) {
        JsonObject o = new JsonObject();
        for (Map.Entry<String, List<G.VProp>> e : v.props.entrySet()) {
            JsonArray a = new JsonArray();
            for (G.VProp p : e.getValue()) {
                JsonObject po = new JsonObject();
                po.add("v", GraphSon.V3.write(p.value));
                if (p.meta != null && !p.meta.isEmpty()) {
                    JsonObject m = new JsonObject();
                    for (Map.Entry<String, Object> x : p.meta.entrySet()) {
                        m.add(x.getKey(), GraphSon.V3.write(x.getValue()));
                    }
                    po.add("m", m);
                }
                a.add(po);
            }
            if (a.size() > 0) {
                o.add(e.getKey(), a);
            }
        }
        return o.toString();
    }

    static G.Edge edgeOf(ResultSet rs) throws SQLException {
        G.Edge e = new G.Edge(GraphStore.idOf(rs.getString(1)), rs.getString(2), GraphStore.idOf(rs.getString(3)), rs.getString(5),
                GraphStore.idOf(rs.getString(4)), rs.getString(6));
        JsonObject o = JsonParser.parseString(rs.getString(7)).getAsJsonObject();
        for (Map.Entry<String, JsonElement> x : o.entrySet()) {
            e.props.put(x.getKey(), GraphSon.V3.read(x.getValue()));
        }
        return e;
    }

    static String edgePropsJson(G.Edge e) {
        JsonObject o = new JsonObject();
        for (Map.Entry<String, Object> x : e.props.entrySet()) {
            o.add(x.getKey(), GraphSon.V3.write(x.getValue()));
        }
        return o.toString();
    }

    private static Object arr(Connection c, Collection<String> s) throws SQLException {
        return c.createArrayOf("text", s.toArray());
    }

    // ------------------------------------------------------------------ reads

    @Override
    public G.Vertex vertex(Object id) {
        List<G.Vertex> l = vertices(List.of(id));
        return l.isEmpty() ? null : l.get(0);
    }

    @Override
    public List<G.Vertex> vertices(Collection<Object> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        List<String> hosts = hosts();
        Map<String, List<String>> byHost = new LinkedHashMap<>();
        List<String> order = new ArrayList<>();
        for (Object id : ids) {
            String k = GraphStore.key(GraphStore.canonId(id));
            order.add(k);
            byHost.computeIfAbsent(owner(hosts, k), h -> new ArrayList<>()).add(k);
        }
        Map<String, G.Vertex> found = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : byHost.entrySet()) {
            conn(e.getKey(), c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT " + VCOLS + " FROM warp_gremlin_vertices WHERE vkey = ANY(?)")) {
                    ps.setObject(1, arr(c, e.getValue()));
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            found.put(rs.getString(1), vertexOf(rs.getString(1), rs.getString(2), rs.getString(3)));
                        }
                    }
                }
                return null;
            });
        }
        List<G.Vertex> out = new ArrayList<>();
        for (String k : order) {
            G.Vertex v = found.get(k);
            if (v != null) {
                out.add(v);
            }
        }
        return out;
    }

    @Override
    public Iterator<G.Vertex> allVertices(VFilter f) {
        List<Iterator<G.Vertex>> perHost = new ArrayList<>();
        for (String host : hosts()) {
            perHost.add(new PagedIterator<G.Vertex>() {
                @Override
                List<G.Vertex> page(Object[] after) {
                    return conn(host, c -> {
                        StringBuilder sql = new StringBuilder("SELECT " + VCOLS + ", kind, lid FROM warp_gremlin_vertices WHERE true");
                        if (after != null) {
                            sql.append(" AND (kind, lid, vkey) > (?, ?, ?)");
                        }
                        if (f.labels() != null && !f.labels().isEmpty()) {
                            sql.append(" AND label = ANY(?)");
                        }
                        for (int i = 0; i < f.stringProps().size(); i++) {
                            sql.append(" AND props @> ?::jsonb");
                        }
                        sql.append(" ORDER BY kind, lid, vkey LIMIT ").append(PAGE);
                        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                            int n = 1;
                            if (after != null) {
                                ps.setShort(n++, (Short) after[0]);
                                ps.setLong(n++, (Long) after[1]);
                                ps.setString(n++, (String) after[2]);
                            }
                            if (f.labels() != null && !f.labels().isEmpty()) {
                                ps.setObject(n++, arr(c, f.labels()));
                            }
                            for (Map.Entry<String, String> e : f.stringProps().entrySet()) {
                                JsonObject cond = new JsonObject();
                                JsonArray a = new JsonArray();
                                JsonObject v = new JsonObject();
                                v.addProperty("v", e.getValue());
                                a.add(v);
                                cond.add(e.getKey(), a);
                                ps.setString(n++, cond.toString());
                            }
                            List<G.Vertex> out = new ArrayList<>();
                            try (ResultSet rs = ps.executeQuery()) {
                                while (rs.next()) {
                                    out.add(vertexOf(rs.getString(1), rs.getString(2), rs.getString(3)));
                                    cursor = new Object[] {rs.getShort(4), rs.getLong(5), rs.getString(1)};
                                }
                            }
                            return out;
                        }
                    });
                }
            });
        }
        return merge(perHost, (a, b) -> GraphStore.compareIds(a.id, b.id));
    }

    @Override
    public G.Edge edge(Object id) {
        List<G.Edge> l = edges(List.of(id));
        return l.isEmpty() ? null : l.get(0);
    }

    @Override
    public List<G.Edge> edges(Collection<Object> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        List<String> keys = new ArrayList<>();
        for (Object id : ids) {
            keys.add(GraphStore.key(GraphStore.canonId(id)));
        }
        Map<String, G.Edge> found = new LinkedHashMap<>();
        for (String h : hosts()) {
            conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT " + ECOLS + " FROM warp_gremlin_edges WHERE ekey = ANY(?)")) {
                    ps.setObject(1, arr(c, keys));
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            found.put(rs.getString(1), edgeOf(rs));
                        }
                    }
                }
                return null;
            });
        }
        List<G.Edge> out = new ArrayList<>();
        for (String k : keys) {
            G.Edge e = found.get(k);
            if (e != null) {
                out.add(e);
            }
        }
        return out;
    }

    @Override
    public Iterator<G.Edge> allEdges() {
        List<Iterator<G.Edge>> perHost = new ArrayList<>();
        for (String host : hosts()) {
            perHost.add(new PagedIterator<G.Edge>() {
                @Override
                List<G.Edge> page(Object[] after) {
                    return conn(host, c -> {
                        String sql = "SELECT " + ECOLS + ", kind, lid FROM warp_gremlin_edges WHERE true" + (after != null ? " AND (kind, lid, ekey) > (?, ?, ?)" : "")
                                + " ORDER BY kind, lid, ekey LIMIT " + PAGE;
                        try (PreparedStatement ps = c.prepareStatement(sql)) {
                            if (after != null) {
                                ps.setShort(1, (Short) after[0]);
                                ps.setLong(2, (Long) after[1]);
                                ps.setString(3, (String) after[2]);
                            }
                            List<G.Edge> out = new ArrayList<>();
                            try (ResultSet rs = ps.executeQuery()) {
                                while (rs.next()) {
                                    out.add(edgeOf(rs));
                                    cursor = new Object[] {rs.getShort(8), rs.getLong(9), rs.getString(1)};
                                }
                            }
                            return out;
                        }
                    });
                }
            });
        }
        return merge(perHost, (a, b) -> GraphStore.compareIds(a.id, b.id));
    }

    /** Keyset-paged scan of one host. */
    private abstract static class PagedIterator<T> implements Iterator<T> {
        Object[] cursor;
        private Iterator<T> cur = List.<T>of().iterator();
        private boolean done;

        abstract List<T> page(Object[] after);

        @Override
        public boolean hasNext() {
            while (!cur.hasNext()) {
                if (done) {
                    return false;
                }
                List<T> p = page(cursor);
                if (p.size() < PAGE) {
                    done = true;
                }
                cur = p.iterator();
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return cur.next();
        }
    }

    /** k-way merge of per-host sorted streams. */
    private static <T> Iterator<T> merge(List<Iterator<T>> its, java.util.Comparator<T> cmp) {
        if (its.size() == 1) {
            return its.get(0);
        }
        return new Iterator<>() {
            final List<T> heads = new ArrayList<>(java.util.Collections.nCopies(its.size(), null));
            final boolean[] loaded = new boolean[its.size()];

            private void fill() {
                for (int i = 0; i < its.size(); i++) {
                    if (!loaded[i]) {
                        heads.set(i, its.get(i).hasNext() ? its.get(i).next() : null);
                        loaded[i] = true;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                fill();
                return heads.stream().anyMatch(h -> h != null);
            }

            @Override
            public T next() {
                fill();
                int best = -1;
                for (int i = 0; i < heads.size(); i++) {
                    if (heads.get(i) != null && (best < 0 || cmp.compare(heads.get(i), heads.get(best)) < 0)) {
                        best = i;
                    }
                }
                if (best < 0) {
                    throw new NoSuchElementException();
                }
                T r = heads.get(best);
                loaded[best] = false;
                return r;
            }
        };
    }

    @Override
    public Map<Object, List<G.Edge>> adjacent(Collection<Object> vertexIds, G.Direction dir, String[] labels) {
        Map<Object, List<G.Edge>> outMap = new LinkedHashMap<>();
        if (vertexIds.isEmpty()) {
            return outMap;
        }
        List<String> hosts = hosts();
        List<String> keys = new ArrayList<>();
        for (Object id : new java.util.LinkedHashSet<>(vertexIds)) {
            keys.add(GraphStore.key(GraphStore.canonId(id)));
        }
        List<String> lab = labels == null || labels.length == 0 ? null : List.of(labels);
        Map<Object, List<G.Edge>> outs = new LinkedHashMap<>();
        Map<Object, List<G.Edge>> ins = new LinkedHashMap<>();
        if (dir != G.Direction.IN) {
            Map<String, List<String>> byHost = new LinkedHashMap<>();
            for (String k : keys) {
                byHost.computeIfAbsent(owner(hosts, k), h -> new ArrayList<>()).add(k);
            }
            for (Map.Entry<String, List<String>> e : byHost.entrySet()) {
                edgeQuery(e.getKey(), "out_key", e.getValue(), lab, outs, true);
            }
        }
        if (dir != G.Direction.OUT) {
            for (String h : hosts) {
                edgeQuery(h, "in_key", keys, lab, ins, false);
            }
        }
        for (String k : keys) {
            Object id = GraphStore.idOf(k);
            List<G.Edge> l = new ArrayList<>();
            if (outs.containsKey(id)) {
                l.addAll(GraphStore.tinkerOrder(outs.get(id), labels));
            }
            if (ins.containsKey(id)) {
                l.addAll(GraphStore.tinkerOrder(ins.get(id), labels));
            }
            if (!l.isEmpty()) {
                outMap.put(id, l);
            }
        }
        return outMap;
    }

    private void edgeQuery(String host, String col, List<String> keys, List<String> labels, Map<Object, List<G.Edge>> into, boolean out) {
        conn(host, c -> {
            String sql = "SELECT " + ECOLS + " FROM warp_gremlin_edges WHERE " + col + " = ANY(?)" + (labels == null ? "" : " AND label = ANY(?)")
                    + " ORDER BY seq";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setObject(1, arr(c, keys));
                if (labels != null) {
                    ps.setObject(2, arr(c, labels));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        G.Edge e = edgeOf(rs);
                        into.computeIfAbsent(out ? e.outId : e.inId, k -> new ArrayList<>()).add(e);
                    }
                }
            }
            return null;
        });
    }

    // ------------------------------------------------------------------ writes

    @Override
    public long nextId() {
        return conn(hosts().get(0), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT nextval('warp_gremlin_id_seq')"); ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        });
    }

    @Override
    public void addVertex(G.Vertex v) {
        String k = GraphStore.key(v.id);
        boolean ok = conn(owner(hosts(), k), c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_gremlin_vertices (vkey, label, props) VALUES (?, ?, ?::jsonb) ON CONFLICT (vkey) DO NOTHING")) {
                ps.setString(1, k);
                ps.setString(2, v.label);
                ps.setString(3, propsJson(v));
                return ps.executeUpdate() == 1;
            }
        });
        if (!ok) {
            throw G.GremlinError.script("Vertex with id already exists: " + v.id);
        }
    }

    @Override
    public void addEdge(G.Edge e) {
        String ok = GraphStore.key(e.outId);
        boolean done = conn(owner(hosts(), ok), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_gremlin_edges (ekey, label, out_key, in_key, out_label, in_label, props) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb) ON CONFLICT (ekey) DO NOTHING")) {
                ps.setString(1, GraphStore.key(e.id));
                ps.setString(2, e.label);
                ps.setString(3, ok);
                ps.setString(4, GraphStore.key(e.inId));
                ps.setString(5, e.outLabel);
                ps.setString(6, e.inLabel);
                ps.setString(7, edgePropsJson(e));
                return ps.executeUpdate() == 1;
            }
        });
        if (!done) {
            throw G.GremlinError.script("Edge with id already exists: " + e.id);
        }
    }

    @Override
    public void updateVertex(Object id, Consumer<G.Vertex> fn) {
        String k = GraphStore.key(GraphStore.canonId(id));
        tx(owner(hosts(), k), c -> {
            String label;
            String json;
            try (PreparedStatement ps = c.prepareStatement("SELECT label, props::text FROM warp_gremlin_vertices WHERE vkey = ? FOR UPDATE")) {
                ps.setString(1, k);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    label = rs.getString(1);
                    json = rs.getString(2);
                }
            }
            G.Vertex v = vertexOf(k, label, json);
            fn.accept(v);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_gremlin_vertices SET props = ?::jsonb WHERE vkey = ?")) {
                ps.setString(1, propsJson(v));
                ps.setString(2, k);
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public void updateEdge(Object id, Consumer<G.Edge> fn) {
        String k = GraphStore.key(GraphStore.canonId(id));
        for (String h : hosts()) {
            boolean found = tx(h, c -> {
                G.Edge e;
                try (PreparedStatement ps = c.prepareStatement("SELECT " + ECOLS + " FROM warp_gremlin_edges WHERE ekey = ? FOR UPDATE")) {
                    ps.setString(1, k);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            return false;
                        }
                        e = edgeOf(rs);
                    }
                }
                fn.accept(e);
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_gremlin_edges SET props = ?::jsonb WHERE ekey = ?")) {
                    ps.setString(1, edgePropsJson(e));
                    ps.setString(2, k);
                    ps.executeUpdate();
                }
                return true;
            });
            if (found) {
                return;
            }
        }
    }

    @Override
    public void removeVertex(Object id) {
        String k = GraphStore.key(GraphStore.canonId(id));
        List<String> hosts = hosts();
        String own = owner(hosts, k);
        tx(own, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_gremlin_edges WHERE out_key = ? OR in_key = ?")) {
                ps.setString(1, k);
                ps.setString(2, k);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_gremlin_vertices WHERE vkey = ?")) {
                ps.setString(1, k);
                ps.executeUpdate();
            }
            return null;
        });
        for (String h : hosts) {
            if (!h.equals(own)) {
                conn(h, c -> {
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_gremlin_edges WHERE in_key = ?")) {
                        ps.setString(1, k);
                        ps.executeUpdate();
                    }
                    return null;
                });
            }
        }
    }

    @Override
    public void removeEdge(Object id) {
        String k = GraphStore.key(GraphStore.canonId(id));
        for (String h : hosts()) {
            conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_gremlin_edges WHERE ekey = ?")) {
                    ps.setString(1, k);
                    ps.executeUpdate();
                }
                return null;
            });
        }
    }

    @Override
    public long vertexCount() {
        return count("warp_gremlin_vertices");
    }

    @Override
    public long edgeCount() {
        return count("warp_gremlin_edges");
    }

    private long count(String table) {
        long n = 0;
        for (String h : hosts()) {
            n += conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM " + table); ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            });
        }
        return n;
    }

    @Override
    public Map<String, Long> vertexLabels() {
        return labels("warp_gremlin_vertices");
    }

    @Override
    public Map<String, Long> edgeLabels() {
        return labels("warp_gremlin_edges");
    }

    private Map<String, Long> labels(String table) {
        Map<String, Long> m = new TreeMap<>();
        for (String h : hosts()) {
            conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT label, count(*) FROM " + table + " GROUP BY label"); ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        m.merge(rs.getString(1), rs.getLong(2), Long::sum);
                    }
                }
                return null;
            });
        }
        return m;
    }
}
