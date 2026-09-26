package com.sayonora.wire.boltwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sayonora.wire.boltwire.Values.DurationV;
import com.sayonora.wire.boltwire.Values.NodeV;
import com.sayonora.wire.boltwire.Values.PointV;
import com.sayonora.wire.boltwire.Values.RelV;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.DdlTemplates;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Property-graph storage for boltwire, backed by plain Postgres -- same "real SQL underneath, no external service
 * required" shape as every other store in this codebase.
 *
 * <p>Two physical tables shared by every label / relationship type:
 * <pre>
 *   warp_graph_nodes(id bigserial pk, labels text[], properties jsonb)
 *   warp_graph_edges(id bigserial pk, type text, from_id bigint, to_id bigint, properties jsonb)
 * </pre>
 * plus {@code warp_graph_schema} (the catalog of constraints / indexes created through Cypher DDL; the constraints
 * themselves are real unique partial expression indexes, so Postgres enforces them). Properties are typed JSON:
 * integers are JSON integers, floats are JSON numbers with a fraction (exponent-form, NaN, infinities and -0.0 are
 * tagged), temporal values / points / byte arrays are tagged objects ({@code {"$t": ...}}), lists are JSON arrays. A
 * Cypher property can never be a map, so a JSON object in a property is always such a tagged value.
 *
 * <p>Statements execute in {@link Exec}, which owns the identity map of entities and calls the row-level operations
 * here on the session's (possibly transaction-pinned) connection.
 */
final class PgGraphStore {

    private final BackendRegistry backendRegistry;
    private final java.util.concurrent.ConcurrentHashMap<String, Boolean> schemaEnsured =
            new java.util.concurrent.ConcurrentHashMap<>();

    PgGraphStore(BackendRegistry backendRegistry) {
        this.backendRegistry = backendRegistry;
    }

    BackendTarget defaultTarget() {
        // Neo4j enabled on a backend of the frontend's set: the graph lives there (one host only --
        // the admin API rejects a second; graph traversals are not shardable). Otherwise `default`.
        String home = backendRegistry.storeHome(com.sayonora.wire.core.StoreType.NEO4J);
        BackendTarget target = backendRegistry.resolveForRouting(
                home != null ? home : BackendRegistry.DEFAULT_BACKEND_NAME);
        if (target == null) {
            throw new IllegalStateException("boltwire: no default backend configured");
        }
        return target;
    }

    Connection connect() throws SQLException {
        return connect(defaultTarget());
    }

    /** As {@link #connect()} but against an explicit backend (connect-time routing). */
    Connection connect(BackendTarget target) throws SQLException {
        ensureSchema(target);
        return target.open();
    }

    /** The graph host for a connect-time route: the one backend of a DATABASE route; for a set, its
     * member with the Neo4j store enabled, else its first Postgres member. {@code null} when the
     * route names nothing that can host a graph. */
    BackendTarget targetFor(com.sayonora.wire.core.ConnectionRoute route) {
        List<String> hosts = backendRegistry.connectionRouter().storeBackends(route);
        if (hosts == null) {
            return defaultTarget();
        }
        String chosen = null;
        for (String h : hosts) {
            if (backendRegistry.enabledStores(h).contains(com.sayonora.wire.core.StoreType.NEO4J)) {
                chosen = h;
                break;
            }
        }
        if (chosen == null && !hosts.isEmpty()) {
            chosen = hosts.get(0);
        }
        return chosen == null ? null : backendRegistry.resolveForRouting(chosen);
    }

    private void ensureSchema(BackendTarget target) throws SQLException {
        if (schemaEnsured.putIfAbsent(target.jdbcUrl(), Boolean.TRUE) != null) {
            return;
        }
        try (Connection c = target.open(); var st = c.createStatement()) {
            // The DDL file ends with warp_graph_schema: when that exists everything does, and re-running CREATE INDEX IF NOT
            // EXISTS would queue behind other sessions' open transactions (it takes a SHARE lock even when a no-op).
            try (ResultSet rs = st.executeQuery("SELECT to_regclass('warp_graph_schema') IS NOT NULL")) {
                if (rs.next() && rs.getBoolean(1)) {
                    return;
                }
            }
            for (String statement : DdlTemplates.loadStatements("postgres", "boltwire_graph_schema", Map.of())) {
                st.executeUpdate(statement);
            }
        } catch (SQLException | RuntimeException e) {
            schemaEnsured.remove(target.jdbcUrl());
            throw e;
        }
    }

    // ------------------------------------------------------------------------------------------ reads

    NodeV loadNodeRow(ResultSet rs, int col, Exec x) throws SQLException {
        long id = rs.getLong(col);
        NodeV existing = x.cachedNode(id);
        if (existing != null) {
            return existing;
        }
        NodeV n = new NodeV(id);
        java.sql.Array arr = rs.getArray(col + 1);
        if (arr != null) {
            for (String l : (String[]) arr.getArray()) {
                n.labels.add(l);
            }
        }
        n.props.putAll(decodeProps(rs.getString(col + 2)));
        return x.intern(n);
    }

    RelV loadRelRow(ResultSet rs, int col, Exec x) throws SQLException {
        long id = rs.getLong(col);
        RelV existing = x.cachedRel(id);
        if (existing != null) {
            return existing;
        }
        RelV r = new RelV(id, rs.getString(col + 1), rs.getLong(col + 2), rs.getLong(col + 3));
        r.props.putAll(decodeProps(rs.getString(col + 4)));
        return x.intern(r);
    }

    List<NodeV> nodesByIds(Connection c, Collection<Long> ids, Exec x) throws SQLException {
        List<NodeV> out = new ArrayList<>();
        List<Long> missing = new ArrayList<>();
        for (Long id : ids) {
            NodeV n = x.cachedNode(id);
            if (n != null) {
                out.add(n);
            } else {
                missing.add(id);
            }
        }
        if (!missing.isEmpty()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT id, labels, properties FROM warp_graph_nodes WHERE id = ANY(?)")) {
                ps.setArray(1, c.createArrayOf("bigint", missing.toArray()));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(loadNodeRow(rs, 1, x));
                    }
                }
            }
        }
        return out;
    }

    /** All nodes carrying every label in {@code labels} (a superset filter on {@code props}: equality on scalars). */
    List<NodeV> scanNodes(Connection c, List<String> labels, Map<String, Object> eq, Exec x) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT id, labels, properties FROM warp_graph_nodes");
        List<Object> params = new ArrayList<>();
        List<String> where = new ArrayList<>();
        if (!labels.isEmpty()) {
            where.add("labels @> ?::text[]");
            params.add(pgTextArray(labels));
        }
        JsonObject want = pushdownJson(eq);
        if (want != null) {
            where.add("properties @> ?::jsonb");
            params.add(want.toString());
        }
        if (!where.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", where));
        }
        sql.append(" ORDER BY id");
        List<NodeV> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setString(i + 1, (String) params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(loadNodeRow(rs, 1, x));
                }
            }
        }
        return out;
    }

    List<RelV> scanRels(Connection c, Set<String> types, Exec x) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT id, type, from_id, to_id, properties FROM warp_graph_edges");
        if (types != null && !types.isEmpty()) {
            sql.append(" WHERE type = ANY(?)");
        }
        sql.append(" ORDER BY id");
        List<RelV> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            if (types != null && !types.isEmpty()) {
                ps.setArray(1, c.createArrayOf("text", types.toArray()));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(loadRelRow(rs, 1, x));
                }
            }
        }
        return out;
    }

    /** One incident relationship together with the node at its other end. */
    record Adj(RelV rel, NodeV other) {
    }

    /** dir: 0 = outgoing, 1 = incoming, 2 = both. */
    List<Adj> expand(Connection c, long nodeId, int dir, Set<String> types, Exec x) throws SQLException {
        boolean hasTypes = types != null && !types.isEmpty();
        StringBuilder sql = new StringBuilder();
        String cols = "e.id, e.type, e.from_id, e.to_id, e.properties, n.id, n.labels, n.properties";
        if (dir == 0 || dir == 2) {
            sql.append("SELECT ").append(cols).append(" FROM warp_graph_edges e JOIN warp_graph_nodes n ON n.id = e.to_id")
                    .append(" WHERE e.from_id = ?").append(hasTypes ? " AND e.type = ANY(?)" : "");
        }
        if (dir == 2) {
            sql.append(" UNION ALL ");
        }
        if (dir == 1 || dir == 2) {
            sql.append("SELECT ").append(cols).append(" FROM warp_graph_edges e JOIN warp_graph_nodes n ON n.id = e.from_id")
                    .append(" WHERE e.to_id = ?").append(hasTypes ? " AND e.type = ANY(?)" : "")
                    .append(dir == 2 ? " AND e.from_id <> e.to_id" : "");
        }
        List<Adj> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            int i = 1;
            int passes = dir == 2 ? 2 : 1;
            for (int k = 0; k < passes; k++) {
                ps.setLong(i++, nodeId);
                if (hasTypes) {
                    ps.setArray(i++, c.createArrayOf("text", types.toArray()));
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    RelV r = loadRelRow(rs, 1, x);
                    NodeV other = loadNodeRow(rs, 6, x);
                    out.add(new Adj(r, other));
                }
            }
        }
        return out;
    }

    /** Both endpoints of a self-loop appear once under dir 2 (the UNION's second half skips from_id = to_id). */
    RelV relById(Connection c, long id, Exec x) throws SQLException {
        RelV r = x.cachedRel(id);
        if (r != null) {
            return r;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT id, type, from_id, to_id, properties FROM warp_graph_edges WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? loadRelRow(rs, 1, x) : null;
            }
        }
    }

    // ------------------------------------------------------------------------------------------ writes

    long insertNode(Connection c, Collection<String> labels, Map<String, Object> props) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO warp_graph_nodes (labels, properties) VALUES (?::text[], ?::jsonb) RETURNING id")) {
            ps.setString(1, pgTextArray(labels));
            ps.setString(2, encodeProps(props));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    long insertRel(Connection c, String type, long from, long to, Map<String, Object> props) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO warp_graph_edges (type, from_id, to_id, properties) VALUES (?, ?, ?, ?::jsonb) RETURNING id")) {
            ps.setString(1, type);
            ps.setLong(2, from);
            ps.setLong(3, to);
            ps.setString(4, encodeProps(props));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    void updateNode(Connection c, NodeV n) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE warp_graph_nodes SET labels = ?::text[], properties = ?::jsonb WHERE id = ?")) {
            ps.setString(1, pgTextArray(n.labels));
            ps.setString(2, encodeProps(n.props));
            ps.setLong(3, n.id);
            ps.executeUpdate();
        }
    }

    void updateRel(Connection c, RelV r) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_graph_edges SET properties = ?::jsonb WHERE id = ?")) {
            ps.setString(1, encodeProps(r.props));
            ps.setLong(2, r.id);
            ps.executeUpdate();
        }
    }

    void deleteRels(Connection c, Collection<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_graph_edges WHERE id = ANY(?)")) {
            ps.setArray(1, c.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        }
    }

    /** Deletes (and returns the ids of) every relationship touching one of the nodes. */
    List<Long> deleteRelsOf(Connection c, Collection<Long> nodeIds) throws SQLException {
        List<Long> out = new ArrayList<>();
        if (nodeIds.isEmpty()) {
            return out;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM warp_graph_edges WHERE from_id = ANY(?) OR to_id = ANY(?) RETURNING id")) {
            java.sql.Array arr = c.createArrayOf("bigint", nodeIds.toArray());
            ps.setArray(1, arr);
            ps.setArray(2, arr);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getLong(1));
                }
            }
        }
        return out;
    }

    /** Ids among {@code nodeIds} that still have a relationship. */
    List<Long> nodesWithRels(Connection c, Collection<Long> nodeIds) throws SQLException {
        List<Long> out = new ArrayList<>();
        if (nodeIds.isEmpty()) {
            return out;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT DISTINCT x FROM (SELECT from_id AS x FROM warp_graph_edges WHERE from_id = ANY(?) "
                        + "UNION ALL SELECT to_id FROM warp_graph_edges WHERE to_id = ANY(?)) t")) {
            java.sql.Array arr = c.createArrayOf("bigint", nodeIds.toArray());
            ps.setArray(1, arr);
            ps.setArray(2, arr);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getLong(1));
                }
            }
        }
        return out;
    }

    void deleteNodes(Connection c, Collection<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_graph_nodes WHERE id = ANY(?)")) {
            ps.setArray(1, c.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        }
    }

    String describeUnique(BackendTarget target, String pgIndex, String detail) throws SQLException {
        return Schema.describeUnique(target, pgIndex, detail);
    }

    // ------------------------------------------------------------------------------------------ encoding

    static String pgTextArray(Collection<String> items) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (String s : items) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
        }
        return sb.append('}').toString();
    }

    /** JSON object of the scalar equality constraints that can be pushed to Postgres safely (a superset filter). */
    static JsonObject pushdownJson(Map<String, Object> eq) {
        if (eq == null || eq.isEmpty()) {
            return null;
        }
        JsonObject o = new JsonObject();
        for (Map.Entry<String, Object> e : eq.entrySet()) {
            Object v = e.getValue();
            if (v instanceof String s) {
                o.addProperty(e.getKey(), s);
            } else if (v instanceof Boolean b) {
                o.addProperty(e.getKey(), b);
            } else if (v instanceof Long l && Math.abs(l) < 10_000_000L) {
                o.addProperty(e.getKey(), l);
            }
        }
        return o.size() == 0 ? null : o;
    }

    static String encodeProps(Map<String, Object> props) {
        JsonObject o = new JsonObject();
        for (Map.Entry<String, Object> e : props.entrySet()) {
            if (e.getValue() != null) {
                o.add(e.getKey(), encodeValue(e.getValue()));
            }
        }
        return o.toString();
    }

    static JsonElement encodeValue(Object v) {
        if (v == null) {
            return JsonNull.INSTANCE;
        }
        if (v instanceof String s) {
            return new JsonPrimitive(s);
        }
        if (v instanceof Boolean b) {
            return new JsonPrimitive(b);
        }
        if (v instanceof Long l) {
            return new JsonPrimitive(l);
        }
        if (v instanceof Double d) {
            String t = Double.toString(d);
            if (Double.isNaN(d) || Double.isInfinite(d) || t.contains("E") || (d == 0.0 && 1 / d < 0)) {
                JsonObject o = new JsonObject();
                o.addProperty("$t", "f");
                o.addProperty("v", t);
                return o;
            }
            return new JsonPrimitive(new java.math.BigDecimal(t));
        }
        if (v instanceof List<?> l) {
            JsonArray a = new JsonArray();
            for (Object o : l) {
                a.add(encodeValue(o));
            }
            return a;
        }
        JsonObject o = new JsonObject();
        if (v instanceof LocalDate d) {
            tag(o, "date", d.toString());
        } else if (v instanceof LocalTime t) {
            tag(o, "ltime", t.toString());
        } else if (v instanceof OffsetTime t) {
            tag(o, "time", t.toString());
        } else if (v instanceof LocalDateTime t) {
            tag(o, "ldt", t.toString());
        } else if (v instanceof ZonedDateTime t) {
            tag(o, "dt", t.toString());
        } else if (v instanceof DurationV d) {
            o.addProperty("$t", "dur");
            o.addProperty("m", d.months());
            o.addProperty("d", d.days());
            o.addProperty("s", d.seconds());
            o.addProperty("n", d.nanos());
        } else if (v instanceof PointV p) {
            o.addProperty("$t", "pt");
            o.addProperty("srid", p.srid());
            o.addProperty("x", Double.toString(p.x()));
            o.addProperty("y", Double.toString(p.y()));
            if (p.z() != null) {
                o.addProperty("z", Double.toString(p.z()));
            }
        } else if (v instanceof byte[] b) {
            tag(o, "b", java.util.Base64.getEncoder().encodeToString(b));
        } else {
            throw CypherException.type("Property values can only be of primitive types or arrays thereof. Encountered: "
                    + Values.typeName(v));
        }
        return o;
    }

    private static void tag(JsonObject o, String t, String v) {
        o.addProperty("$t", t);
        o.addProperty("v", v);
    }

    static Map<String, Object> decodeProps(String json) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (json == null) {
            return out;
        }
        JsonObject o = JsonParser.parseString(json).getAsJsonObject();
        for (Map.Entry<String, JsonElement> e : o.entrySet()) {
            out.put(e.getKey(), decodeValue(e.getValue()));
        }
        return out;
    }

    static Object decodeValue(JsonElement e) {
        if (e == null || e.isJsonNull()) {
            return null;
        }
        if (e.isJsonPrimitive()) {
            JsonPrimitive p = e.getAsJsonPrimitive();
            if (p.isBoolean()) {
                return p.getAsBoolean();
            }
            if (p.isNumber()) {
                String t = p.getAsString();
                if (t.indexOf('.') >= 0 || t.indexOf('e') >= 0 || t.indexOf('E') >= 0) {
                    return Double.parseDouble(t);
                }
                try {
                    return Long.parseLong(t);
                } catch (NumberFormatException ex) {
                    return Double.parseDouble(t);
                }
            }
            return p.getAsString();
        }
        if (e.isJsonArray()) {
            List<Object> l = new ArrayList<>();
            for (JsonElement x : e.getAsJsonArray()) {
                l.add(decodeValue(x));
            }
            return l;
        }
        JsonObject o = e.getAsJsonObject();
        String t = o.get("$t").getAsString();
        switch (t) {
            case "f" -> {
                String v = o.get("v").getAsString();
                return switch (v) {
                    case "NaN" -> Double.NaN;
                    case "Infinity" -> Double.POSITIVE_INFINITY;
                    case "-Infinity" -> Double.NEGATIVE_INFINITY;
                    default -> Double.parseDouble(v);
                };
            }
            case "date" -> {
                return LocalDate.parse(o.get("v").getAsString());
            }
            case "ltime" -> {
                return LocalTime.parse(o.get("v").getAsString());
            }
            case "time" -> {
                return OffsetTime.parse(o.get("v").getAsString());
            }
            case "ldt" -> {
                return LocalDateTime.parse(o.get("v").getAsString());
            }
            case "dt" -> {
                return ZonedDateTime.parse(o.get("v").getAsString());
            }
            case "dur" -> {
                return new DurationV(o.get("m").getAsLong(), o.get("d").getAsLong(), o.get("s").getAsLong(),
                        o.get("n").getAsInt());
            }
            case "pt" -> {
                return new PointV(o.get("srid").getAsInt(), Double.parseDouble(o.get("x").getAsString()),
                        Double.parseDouble(o.get("y").getAsString()),
                        o.has("z") ? Double.parseDouble(o.get("z").getAsString()) : null);
            }
            case "b" -> {
                return java.util.Base64.getDecoder().decode(o.get("v").getAsString());
            }
            default -> throw new IllegalStateException("boltwire: unknown stored value tag " + t);
        }
    }
}
