package com.nexagres.wire.boltwire;

import com.google.gson.JsonObject;
import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.core.BackendTarget;
import com.nexagres.wire.core.DdlTemplates;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Property-graph storage for boltwire, backed by plain Postgres -- same "real SQL underneath, no
 * external service required" shape as every other store in this codebase
 * ({@code oswire.PostgresSearchStore}, {@code influxwire.PgTimeSeriesStore}).
 *
 * <p>Two physical tables, shared across every label/relationship-type (not one table per label the
 * way oswire/influxwire do one table per index/measurement) -- a property graph's whole point is
 * that any node can relate to any other regardless of label, so a single {@code nodes} table with a
 * {@code labels} array column (matching real Cypher's own multi-label-per-node model) and a single
 * {@code edges} table are the natural relational shape, not N per-label tables that would need
 * cross-table joins for the common case of an untyped/mixed-label traversal:
 * <pre>
 *   polywire_graph_nodes(id bigserial pk, labels text[] not null default '{}',
 *                         properties jsonb not null default '{}')
 *   polywire_graph_edges(id bigserial pk, type text not null,
 *                         from_id bigint not null references polywire_graph_nodes(id),
 *                         to_id bigint not null references polywire_graph_nodes(id),
 *                         properties jsonb not null default '{}')
 * </pre>
 * GIN indexes on {@code labels} and both {@code properties} columns, btree on {@code from_id}/
 * {@code to_id} for traversal -- all created once, idempotently, on first use (see
 * {@link #ensureSchema}), the same "lazy, cached per backend" convention as
 * {@code PgTimeSeriesStore#ensureMeasurement}.
 */
final class PgGraphStore {

    private final BackendRegistry backendRegistry;
    private final AtomicBoolean schemaEnsured = new AtomicBoolean(false);

    PgGraphStore(BackendRegistry backendRegistry) {
        this.backendRegistry = backendRegistry;
    }

    BackendTarget defaultTarget() {
        BackendTarget target = backendRegistry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
        if (target == null) {
            throw new IllegalStateException("boltwire: no default backend configured");
        }
        return target;
    }

    /** Opens one pooled connection for a whole Bolt session to reuse across every RUN it sends,
     * with the schema already ensured -- see {@link BoltWireSessionHandler}'s own "one connection
     * per session" javadoc for why this replaced opening (and immediately returning) a fresh
     * pooled connection on every single query. */
    Connection connect() throws SQLException {
        BackendTarget target = defaultTarget();
        ensureSchema(target);
        return target.open();
    }

    private void ensureSchema(BackendTarget target) throws SQLException {
        if (!schemaEnsured.compareAndSet(false, true)) {
            return;
        }
        // Real DDL, loaded from ddl/postgres/boltwire_graph_schema.sql -- see that file's own
        // comment for why this store is still Postgres-only (the "labels" array column has no
        // cross-engine equivalent at all, unlike dynamowire/influxwire's own now-portable DDL).
        try (Connection c = target.open(); var st = c.createStatement()) {
            for (String statement : DdlTemplates.loadStatements("postgres", "boltwire_graph_schema", Map.of())) {
                st.executeUpdate(statement);
            }
        }
    }

    GraphNode createNode(Connection c, List<String> labels, Map<String, Object> properties) throws SQLException {
        String labelsArray = "{" + String.join(",", labels.stream().map(PgGraphStore::pgArrayEscape).toList()) + "}";
        try (PreparedStatement ps = c.prepareStatement(
                // ?::text[] -- real bug, found live: pgjdbc doesn't implicitly cast a plain String
                // bind parameter to a text[] column just because the string looks like a Postgres
                // array literal ("{a,b}") -- "column \"labels\" is of type text[] but expression
                // is of type character varying". An explicit cast on the parameter itself (not
                // just relying on the column's own declared type) is required.
                "INSERT INTO polywire_graph_nodes (labels, properties) VALUES (?::text[], ?::jsonb) RETURNING id")) {
            ps.setString(1, labelsArray);
            ps.setString(2, toJson(properties));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long id = rs.getLong(1);
                return new GraphNode(id, labels, properties, GraphNode.elementId(id));
            }
        }
    }

    void createEdge(Connection c, long fromId, long toId, String type, Map<String, Object> properties)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO polywire_graph_edges (type, from_id, to_id, properties) VALUES (?, ?, ?, ?::jsonb)")) {
            ps.setString(1, type == null ? "RELATED_TO" : type);
            ps.setLong(2, fromId);
            ps.setLong(3, toId);
            ps.setString(4, toJson(properties));
            ps.executeUpdate();
        }
    }

    /** Runs {@code action} as one transaction against the caller's own already-open connection
     * (see {@link #connect()}) -- every write in one Cypher statement (a node, or a node+edge+node)
     * shares one transaction, so a partial failure (e.g. the edge insert failing after the first
     * node succeeded) rolls back cleanly instead of leaving an orphaned node. Toggles
     * {@code autoCommit} off for the transaction and back on afterward rather than closing the
     * connection -- unlike the old per-call {@code target.open()} this replaced, {@code c} is a
     * whole Bolt session's own connection, reused by later RUNs too. */
    <T> T withConnection(Connection c, SqlAction<T> action) throws SQLException {
        c.setAutoCommit(false);
        try {
            T result = action.run(c);
            c.commit();
            return result;
        } catch (SQLException | RuntimeException e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
    }

    interface SqlAction<T> {
        T run(Connection c) throws SQLException;
    }

    private static String pgArrayEscape(String label) {
        return "\"" + label.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String toJson(Map<String, Object> properties) {
        JsonObject obj = new JsonObject();
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            Object v = e.getValue();
            if (v == null) {
                obj.add(e.getKey(), com.google.gson.JsonNull.INSTANCE);
            } else if (v instanceof String s) {
                obj.addProperty(e.getKey(), s);
            } else if (v instanceof Boolean b) {
                obj.addProperty(e.getKey(), b);
            } else if (v instanceof Number n) {
                obj.addProperty(e.getKey(), n);
            } else {
                obj.addProperty(e.getKey(), String.valueOf(v));
            }
        }
        return obj.toString();
    }
}
