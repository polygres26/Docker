package com.sayonora.wire.boltwire;

import com.sayonora.wire.boltwire.Values.NodeV;
import com.sayonora.wire.boltwire.Values.RelV;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The state of one Cypher statement while it runs: parameters, the connection (transaction-pinned or auto-commit), the
 * identity map of nodes / relationships it has touched, pending writes and the update statistics. Reads flush pending
 * writes first, so a statement sees its own writes; node deletions are applied at {@link #finish()} (after checking that
 * no relationship still references them, as Neo4j does at commit).
 */
final class Exec {

    /** Update counters of one statement (Neo4j's {@code stats}). */
    static final class Stats {
        long nodesCreated, nodesDeleted, relsCreated, relsDeleted, propertiesSet, labelsAdded, labelsRemoved;
        long indexesAdded, indexesRemoved, constraintsAdded, constraintsRemoved;
        boolean systemUpdate;

        boolean containsUpdates() {
            return nodesCreated + nodesDeleted + relsCreated + relsDeleted + propertiesSet + labelsAdded + labelsRemoved > 0;
        }

        boolean containsSystemUpdates() {
            return indexesAdded + indexesRemoved + constraintsAdded + constraintsRemoved > 0;
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            put(m, "nodes-created", nodesCreated);
            put(m, "nodes-deleted", nodesDeleted);
            put(m, "relationships-created", relsCreated);
            put(m, "relationships-deleted", relsDeleted);
            put(m, "properties-set", propertiesSet);
            put(m, "labels-added", labelsAdded);
            put(m, "labels-removed", labelsRemoved);
            put(m, "indexes-added", indexesAdded);
            put(m, "indexes-removed", indexesRemoved);
            put(m, "constraints-added", constraintsAdded);
            put(m, "constraints-removed", constraintsRemoved);
            if (containsSystemUpdates()) {
                m.put("system-updates", indexesAdded + indexesRemoved + constraintsAdded + constraintsRemoved);
            }
            if (containsUpdates()) {
                m.put("contains-updates", true);
            }
            if (containsSystemUpdates()) {
                m.put("contains-system-updates", true);
            }
            return m;
        }

        private static void put(Map<String, Object> m, String k, long v) {
            if (v != 0) {
                m.put(k, v);
            }
        }
    }

    final PgGraphStore store;
    final com.sayonora.wire.core.BackendTarget target;
    final Connection conn;
    final Map<String, Object> params;
    final Stats stats = new Stats();
    long deadlineNanos;
    final long startedNanos = System.nanoTime();
    /** Instant the statement began (temporal functions like date() are fixed per statement). */
    final java.time.Instant startedAt = java.time.Instant.now();
    boolean readOnlyTx;

    private final Map<Long, NodeV> nodes = new HashMap<>();
    private final Map<Long, RelV> rels = new HashMap<>();
    private final Set<NodeV> dirtyNodes = new LinkedHashSet<>();
    private final Set<RelV> dirtyRels = new LinkedHashSet<>();
    private final Set<Long> pendingRelDeletes = new LinkedHashSet<>();
    private final Set<Long> detachNodes = new LinkedHashSet<>();
    private final Map<Long, NodeV> pendingNodeDeletes = new HashMap<>();
    private final Set<Long> createdInThisStatement = new HashSet<>();

    Exec(PgGraphStore store, com.sayonora.wire.core.BackendTarget target, Connection conn, Map<String, Object> params) {
        this.store = store;
        this.target = target;
        this.conn = conn;
        this.params = params == null ? Map.of() : params;
    }

    void checkDeadline() {
        if (deadlineNanos != 0 && System.nanoTime() > deadlineNanos) {
            throw new CypherException("Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
                    "The transaction has been terminated. Retry your operation in a new transaction, and you should see a "
                            + "successful result. The transaction has not completed within the specified timeout.");
        }
    }

    // ------------------------------------------------------------------------------------------ identity map

    NodeV cachedNode(long id) {
        return nodes.get(id);
    }

    RelV cachedRel(long id) {
        return rels.get(id);
    }

    NodeV intern(NodeV n) {
        NodeV e = nodes.putIfAbsent(n.id, n);
        return e != null ? e : n;
    }

    RelV intern(RelV r) {
        RelV e = rels.putIfAbsent(r.id, r);
        return e != null ? e : r;
    }

    NodeV node(long id) throws SQLException {
        NodeV n = nodes.get(id);
        if (n != null) {
            return n;
        }
        flush();
        List<NodeV> l = store.nodesByIds(conn, List.of(id), this);
        return l.isEmpty() ? null : l.get(0);
    }

    // ------------------------------------------------------------------------------------------ reads

    List<NodeV> scan(List<String> labels, Map<String, Object> eq) {
        try {
            flush();
            List<NodeV> raw = store.scanNodes(conn, labels, eq, this);
            List<NodeV> out = new ArrayList<>(raw.size());
            for (NodeV n : raw) {
                if (!n.deleted) {
                    out.add(n);
                }
            }
            return out;
        } catch (SQLException e) {
            throw sql(e);
        }
    }

    List<RelV> scanRels(Set<String> types) {
        try {
            flush();
            List<RelV> raw = store.scanRels(conn, types, this);
            List<RelV> out = new ArrayList<>(raw.size());
            for (RelV r : raw) {
                if (!r.deleted) {
                    out.add(r);
                }
            }
            return out;
        } catch (SQLException e) {
            throw sql(e);
        }
    }

    List<PgGraphStore.Adj> expand(NodeV n, int dir, Set<String> types) {
        try {
            flush();
            List<PgGraphStore.Adj> raw = store.expand(conn, n.id, dir, types, this);
            List<PgGraphStore.Adj> out = new ArrayList<>(raw.size());
            for (PgGraphStore.Adj a : raw) {
                if (!a.rel().deleted && !a.other().deleted) {
                    out.add(a);
                }
            }
            return out;
        } catch (SQLException e) {
            throw sql(e);
        }
    }

    // ------------------------------------------------------------------------------------------ writes

    NodeV createNode(Collection<String> labels, Map<String, Object> props) {
        Map<String, Object> clean = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Object> e : props.entrySet()) {
            Object v = storable(e.getValue());
            if (v != null) {
                clean.put(e.getKey(), v);
            }
        }
        try {
            long id = store.insertNode(conn, new LinkedHashSet<>(labels), clean);
            NodeV n = new NodeV(id);
            n.labels.addAll(labels);
            n.props.putAll(clean);
            stats.nodesCreated++;
            stats.labelsAdded += n.labels.size();
            stats.propertiesSet += clean.size();
            createdInThisStatement.add(id);
            return intern(n);
        } catch (SQLException e) {
            throw sql(e);
        }
    }

    RelV createRel(String type, NodeV start, NodeV end, Map<String, Object> props) {
        Map<String, Object> clean = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Object> e : props.entrySet()) {
            Object v = storable(e.getValue());
            if (v != null) {
                clean.put(e.getKey(), v);
            }
        }
        try {
            flush();
            long id = store.insertRel(conn, type, start.id, end.id, clean);
            RelV r = new RelV(id, type, start.id, end.id);
            r.props.putAll(clean);
            stats.relsCreated++;
            stats.propertiesSet += clean.size();
            return intern(r);
        } catch (SQLException e) {
            throw sql(e);
        }
    }

    void setProp(Object entity, String key, Object value) {
        Object v = storable(value);
        if (entity instanceof NodeV n) {
            checkLive(n);
            if (v == null) {
                if (n.props.remove(key) != null) {
                    stats.propertiesSet++;
                    markDirty(n);
                }
            } else {
                n.props.put(key, v);
                stats.propertiesSet++;
                markDirty(n);
            }
        } else if (entity instanceof RelV r) {
            checkLive(r);
            if (v == null) {
                if (r.props.remove(key) != null) {
                    stats.propertiesSet++;
                    markDirty(r);
                }
            } else {
                r.props.put(key, v);
                stats.propertiesSet++;
                markDirty(r);
            }
        }
    }

    void replaceProps(Object entity, Map<String, Object> newProps) {
        Map<String, Object> clean = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Object> e : newProps.entrySet()) {
            Object v = storable(e.getValue());
            if (v != null) {
                clean.put(e.getKey(), v);
            }
        }
        Map<String, Object> target = entity instanceof NodeV n ? n.props : ((RelV) entity).props;
        if (entity instanceof NodeV n) {
            checkLive(n);
        } else {
            checkLive((RelV) entity);
        }
        int removed = 0;
        for (String k : target.keySet()) {
            if (!clean.containsKey(k)) {
                removed++;
            }
        }
        target.clear();
        target.putAll(clean);
        stats.propertiesSet += clean.size() + removed;
        if (entity instanceof NodeV n) {
            markDirty(n);
        } else {
            markDirty((RelV) entity);
        }
    }

    void mergeProps(Object entity, Map<String, Object> more) {
        for (Map.Entry<String, Object> e : more.entrySet()) {
            setProp(entity, e.getKey(), e.getValue());
        }
    }

    void addLabels(NodeV n, Collection<String> labels) {
        checkLive(n);
        boolean changed = false;
        for (String l : labels) {
            if (n.labels.add(l)) {
                stats.labelsAdded++;
                changed = true;
            }
        }
        if (changed) {
            markDirty(n);
        }
    }

    void removeLabels(NodeV n, Collection<String> labels) {
        checkLive(n);
        boolean changed = false;
        for (String l : labels) {
            if (n.labels.remove(l)) {
                stats.labelsRemoved++;
                changed = true;
            }
        }
        if (changed) {
            markDirty(n);
        }
    }

    void deleteNode(NodeV n, boolean detach) {
        if (n.deleted) {
            return;
        }
        n.deleted = true;
        stats.nodesDeleted++;
        pendingNodeDeletes.put(n.id, n);
        dirtyNodes.remove(n);
        if (detach) {
            detachNodes.add(n.id);
        }
    }

    void deleteRel(RelV r) {
        if (r.deleted) {
            return;
        }
        r.deleted = true;
        stats.relsDeleted++;
        pendingRelDeletes.add(r.id);
        dirtyRels.remove(r);
    }

    private void markDirty(NodeV n) {
        n.dirty = true;
        dirtyNodes.add(n);
    }

    private void markDirty(RelV r) {
        r.dirty = true;
        dirtyRels.add(r);
    }

    static void checkLive(NodeV n) {
        if (n.deleted) {
            throw new CypherException(CypherException.ENTITY_NOT_FOUND,
                    "Node with id " + n.id + " has been deleted in this transaction");
        }
    }

    static void checkLive(RelV r) {
        if (r.deleted) {
            throw new CypherException(CypherException.ENTITY_NOT_FOUND,
                    "Relationship with id " + r.id + " has been deleted in this transaction");
        }
    }

    /** Pushes pending property / label updates and relationship deletions to the backend. */
    void flush() {
        if (dirtyNodes.isEmpty() && dirtyRels.isEmpty() && pendingRelDeletes.isEmpty() && detachNodes.isEmpty()) {
            return;
        }
        try {
            for (NodeV n : dirtyNodes) {
                if (!n.deleted) {
                    store.updateNode(conn, n);
                }
                n.dirty = false;
            }
            dirtyNodes.clear();
            for (RelV r : dirtyRels) {
                if (!r.deleted) {
                    store.updateRel(conn, r);
                }
                r.dirty = false;
            }
            dirtyRels.clear();
            if (!pendingRelDeletes.isEmpty()) {
                store.deleteRels(conn, pendingRelDeletes);
                pendingRelDeletes.clear();
            }
            if (!detachNodes.isEmpty()) {
                List<Long> gone = store.deleteRelsOf(conn, detachNodes);
                for (Long id : gone) {
                    RelV r = rels.get(id);
                    if (r == null || !r.deleted) {
                        stats.relsDeleted++;
                        if (r != null) {
                            r.deleted = true;
                        }
                    }
                }
                detachNodes.clear();
            }
        } catch (SQLException e) {
            throw sql(e);
        }
    }

    /** End of statement: flush, check that no deleted node still has relationships, delete the nodes. */
    void finish() {
        flush();
        if (pendingNodeDeletes.isEmpty()) {
            return;
        }
        try {
            List<Long> stuck = store.nodesWithRels(conn, pendingNodeDeletes.keySet());
            if (!stuck.isEmpty()) {
                throw new CypherException(CypherException.CONSTRAINT,
                        "Cannot delete node<" + stuck.get(0) + ">, because it still has relationships. "
                                + "To delete this node, you must first delete its relationships.");
            }
            store.deleteNodes(conn, pendingNodeDeletes.keySet());
            pendingNodeDeletes.clear();
        } catch (SQLException e) {
            throw sql(e);
        }
    }

    // ------------------------------------------------------------------------------------------ values

    /** Validates a property value: returns it as storable (null = absent) or throws Neo4j's TypeError. */
    static Object storable(Object v) {
        if (v == null || v instanceof String || v instanceof Boolean || v instanceof Long || v instanceof Double) {
            return v;
        }
        if (v instanceof List<?> l) {
            Class<?> kind = null;
            for (Object o : l) {
                if (o == null) {
                    throw CypherException.type("Collections containing null values can not be stored in properties.");
                }
                if (o instanceof List || o instanceof Map) {
                    throw CypherException.type("Collections containing collections can not be stored in properties.");
                }
                Object so = storable(o);
                Class<?> k = so.getClass();
                if (kind == null) {
                    kind = k;
                } else if (kind != k && !(so instanceof Number && (kind == Long.class || kind == Double.class))) {
                    throw CypherException.type("Collections containing mixed types can not be stored in properties.");
                }
            }
            return v;
        }
        if (v instanceof Map) {
            throw CypherException.type("Property values can only be of primitive types or arrays thereof. Encountered: "
                    + "Map{...}.");
        }
        if (v instanceof NodeV || v instanceof RelV || v instanceof Values.PathV) {
            throw CypherException.type("Property values can only be of primitive types or arrays thereof. Encountered: "
                    + Values.typeName(v) + ".");
        }
        return v; // temporal values, points, byte arrays
    }

    CypherException sql(SQLException e) {
        String state = e.getSQLState();
        if ("23505".equals(state)) {
            return new CypherException(CypherException.CONSTRAINT, uniqueMessage(e));
        }
        if ("23503".equals(state)) {
            return new CypherException(CypherException.CONSTRAINT,
                    "Cannot delete node, because it still has relationships. To delete this node, you must first delete "
                            + "its relationships.");
        }
        if ("57014".equals(state) && deadlineNanos != 0) {
            return new CypherException("Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
                    "The transaction has been terminated: the timeout was reached.");
        }
        return new CypherException("Neo.DatabaseError.General.UnknownError", "boltwire: " + e.getMessage());
    }

    private String uniqueMessage(SQLException e) {
        String detail = e.getMessage();
        try {
            org.postgresql.util.ServerErrorMessage m = e instanceof org.postgresql.util.PSQLException p
                    ? p.getServerErrorMessage() : null;
            if (m != null && m.getConstraint() != null) {
                String msg = store.describeUnique(target, m.getConstraint(), m.getDetail());
                if (msg != null) {
                    return msg;
                }
            }
        } catch (RuntimeException | SQLException ignored) {
            // fall through to the generic message
        }
        return "Node already exists with the given label and property: " + detail;
    }
}
