package com.sayonora.warp.datastorewire;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ConnectionRouter;
import com.sayonora.warp.core.ShardingStrategy;
import com.sayonora.warp.core.StoreBootstrap;
import com.sayonora.warp.core.StoreType;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres storage of datastorewire (see {@code ddl/postgres/datastorewire_store.sql}). An entity is placed by hash of its partition
 * and ROOT ancestor key, so an entity group (and every ancestor query) lives on one host; kind and kindless queries scan all hosts and
 * merge by key. Pooled connections are borrowed per short statement/transaction only.
 */
final class DsStore {

    private static final Logger log = LoggerFactory.getLogger(DsStore.class);
    static final int PAGE = 1000;

    private final BackendRegistry registry;
    private final StoreType type = StoreType.DATASTORE;

    DsStore(BackendRegistry registry) {
        this.registry = registry;
    }

    // ------------------------------------------------------------------ hosts

    List<String> hosts() {
        List<String> h = registry.storeHosts(type);
        if (h.isEmpty()) {
            throw DsException.unavailable("No Postgres backend of this set has the " + type.id() + " store enabled");
        }
        return h;
    }

    List<String> hosts(DsKeys.Part part) {
        try {
            var router = registry.connectionRouter();
            if (router != null) {
                String name = part.database().isEmpty() ? part.project() : part.database();
                List<String> r = router.storeBackends(router.resolve(ConnectionRouter.PROTO_HTTP, name, null));
                if (r != null) {
                    if (r.isEmpty()) {
                        throw DsException.invalid("The backend or set \"" + name + "\" cannot host the datastore store");
                    }
                    return r;
                }
            }
        } catch (DsException e) {
            throw e;
        } catch (RuntimeException e) {
            // no router: default placement
        }
        return hosts();
    }

    String owner(DsKeys.Part part, byte[] rootKey) {
        List<String> h = hosts(part);
        return h.size() == 1 ? h.get(0) : ShardingStrategy.hash(h).resolve(part.scope() + "/" + Base64.getEncoder().encodeToString(rootKey));
    }

    BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("datastorewire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, type);
        return t;
    }

    @FunctionalInterface
    interface SqlFn<T> {
        T apply(Connection c) throws SQLException;
    }

    <T> T conn(String host, SqlFn<T> fn) {
        try (Connection c = target(host).open()) {
            return fn.apply(c);
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    static DsException storage(SQLException e) {
        log.error("datastorewire: Postgres store error", e);
        return DsException.internal("Internal error (" + e.getMessage() + ")");
    }

    // ------------------------------------------------------------------ rows

    /** A stored entity. */
    static final class Row {
        final DsKeys.Part part;
        final Key key;
        final byte[] keyBytes;
        final Entity entity;
        final long version;
        final long createUs;
        final long updateUs;

        Row(DsKeys.Part part, Key key, byte[] keyBytes, Entity entity, long version, long createUs, long updateUs) {
            this.part = part;
            this.key = key;
            this.keyBytes = keyBytes;
            this.entity = entity;
            this.version = version;
            this.createUs = createUs;
            this.updateUs = updateUs;
        }

        Entity resultEntity() {
            return entity.toBuilder().setKey(DsKeys.withPartition(key, part)).build();
        }
    }

    static ByteBuffer bb(byte[] b) {
        return ByteBuffer.wrap(b);
    }

    private Row row(DsKeys.Part part, ResultSet rs) throws SQLException {
        try {
            Entity e = Entity.parseFrom(rs.getBytes("data"));
            return new Row(part, e.getKey(), rs.getBytes("key_bytes"), e, rs.getLong("version"), rs.getLong("create_us"), rs.getLong("update_us"));
        } catch (InvalidProtocolBufferException ex) {
            throw new SQLException("corrupt entity payload", ex);
        }
    }

    private static final String COLS = "key_bytes, data, version, create_us, update_us";

    /** Point reads; absent entities are missing from the result (keyed by {@link #bb}). */
    Map<ByteBuffer, Row> getMany(DsKeys.Part part, Collection<byte[]> keys) {
        Map<String, List<byte[]>> byHost = new LinkedHashMap<>();
        for (byte[] k : keys) {
            byte[] root = rootOf(k);
            byHost.computeIfAbsent(owner(part, root), x -> new ArrayList<>()).add(k);
        }
        Map<ByteBuffer, Row> out = new HashMap<>();
        for (var e : byHost.entrySet()) {
            out.putAll(conn(e.getKey(), c -> {
                Map<ByteBuffer, Row> m = new HashMap<>();
                Array arr = c.createArrayOf("bytea", e.getValue().toArray(new byte[0][]));
                try (PreparedStatement ps = c.prepareStatement("SELECT " + COLS + " FROM warp_datastore_entities WHERE ns = ? AND key_bytes = ANY(?)")) {
                    ps.setString(1, part.scope());
                    ps.setArray(2, arr);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Row r = row(part, rs);
                            m.put(bb(r.keyBytes), r);
                        }
                    }
                }
                return m;
            }));
        }
        return out;
    }

    /** The root element's bytes of an encoded key (elements are self-delimiting). */
    static byte[] rootOf(byte[] key) {
        int i = 0;
        while (key[i] != 0) {
            i++;
        }
        i++;
        if (key[i] == 1) {
            i += 9;
        } else {
            i++;
            while (key[i] != 0) {
                i++;
            }
            i++;
        }
        return java.util.Arrays.copyOf(key, i);
    }

    // ------------------------------------------------------------------ scans

    /** What a scan covers: one kind (or every non-metadata kind), optionally below an ancestor's encoded key. */
    record Scope(String kind, byte[] ancestor) {
    }

    private static void where(DsKeys.Part part, Scope s, byte[] lo, boolean loIncl, byte[] hi, boolean hiIncl, StringBuilder w, List<Object> args) {
        w.append("ns = ?");
        args.add(part.scope());
        if (s.kind() != null) {
            w.append(" AND kind = ?");
            args.add(s.kind());
        } else {
            w.append(" AND kind NOT LIKE '\\_\\_%'");
        }
        if (s.ancestor() != null) {
            w.append(" AND key_bytes >= ? AND key_bytes < ?");
            args.add(s.ancestor());
            byte[] up = java.util.Arrays.copyOf(s.ancestor(), s.ancestor().length + 1);
            up[up.length - 1] = (byte) 0xFF;
            args.add(up);
        }
        if (lo != null) {
            w.append(" AND key_bytes ").append(loIncl ? ">=" : ">").append(" ?");
            args.add(lo);
        }
        if (hi != null) {
            w.append(" AND key_bytes ").append(hiIncl ? "<=" : "<").append(" ?");
            args.add(hi);
        }
    }

    /** Lazy, host-paged keyset scan of one host in key order. */
    final class HostScan implements Iterator<Row> {
        private final String host;
        private final DsKeys.Part part;
        private final Scope scope;
        private final boolean desc;
        private final byte[] lo;
        private final byte[] hi;
        private final boolean loIncl;
        private final boolean hiIncl;
        private byte[] last;
        private List<Row> buf = List.of();
        private int pos;
        private boolean exhausted;

        HostScan(String host, DsKeys.Part part, Scope scope, boolean desc, byte[] lo, boolean loIncl, byte[] hi, boolean hiIncl) {
            this.host = host;
            this.part = part;
            this.scope = scope;
            this.desc = desc;
            this.lo = lo;
            this.loIncl = loIncl;
            this.hi = hi;
            this.hiIncl = hiIncl;
        }

        private void fill() {
            buf = conn(host, c -> {
                StringBuilder w = new StringBuilder();
                List<Object> args = new ArrayList<>();
                byte[] l = desc ? lo : (last != null ? last : lo);
                boolean li = desc ? loIncl : (last == null && loIncl);
                byte[] h = desc ? (last != null ? last : hi) : hi;
                boolean hin = desc ? (last == null && hiIncl) : hiIncl;
                where(part, scope, l, li, h, hin, w, args);
                List<Row> out = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement("SELECT " + COLS + " FROM warp_datastore_entities WHERE " + w
                        + " ORDER BY key_bytes" + (desc ? " DESC" : "") + " LIMIT " + PAGE)) {
                    for (int i = 0; i < args.size(); i++) {
                        ps.setObject(i + 1, args.get(i));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(row(part, rs));
                        }
                    }
                }
                return out;
            });
            pos = 0;
            if (buf.size() < PAGE) {
                exhausted = true;
            }
            if (!buf.isEmpty()) {
                last = buf.get(buf.size() - 1).keyBytes;
            }
        }

        @Override
        public boolean hasNext() {
            while (pos >= buf.size()) {
                if (exhausted) {
                    return false;
                }
                fill();
                if (buf.isEmpty()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buf.get(pos++);
        }
    }

    /**
     * K-way merge by key of every host's scan (an ancestor scan reads only the owner of that root).
     */
    Iterator<Row> scan(DsKeys.Part part, Scope scope, boolean desc, byte[] lo, boolean loIncl, byte[] hi, boolean hiIncl) {
        List<String> hs = scope.ancestor() != null ? List.of(owner(part, rootOf(scope.ancestor()))) : hosts(part);
        List<HostScan> scans = new ArrayList<>();
        for (String h : hs) {
            scans.add(new HostScan(h, part, scope, desc, lo, loIncl, hi, hiIncl));
        }
        if (scans.size() == 1) {
            return scans.get(0);
        }
        return new Iterator<>() {
            private final Row[] head = new Row[scans.size()];
            private boolean init;

            private void prime() {
                if (!init) {
                    init = true;
                    for (int i = 0; i < head.length; i++) {
                        head[i] = scans.get(i).hasNext() ? scans.get(i).next() : null;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                prime();
                for (Row r : head) {
                    if (r != null) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Row next() {
                prime();
                int best = -1;
                for (int i = 0; i < head.length; i++) {
                    if (head[i] == null) {
                        continue;
                    }
                    if (best < 0) {
                        best = i;
                    } else {
                        int c = java.util.Arrays.compareUnsigned(head[i].keyBytes, head[best].keyBytes);
                        if (desc ? c > 0 : c < 0) {
                            best = i;
                        }
                    }
                }
                if (best < 0) {
                    throw new NoSuchElementException();
                }
                Row r = head[best];
                head[best] = scans.get(best).hasNext() ? scans.get(best).next() : null;
                return r;
            }
        };
    }

    /** Distinct non-metadata kinds of a partition (all hosts). */
    List<String> kinds(DsKeys.Part part) {
        TreeSet<String> out = new TreeSet<>(DsValues::compareStrings);
        for (String h : hosts(part)) {
            conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT DISTINCT kind FROM warp_datastore_entities WHERE ns = ? AND kind NOT LIKE '\\_\\_%'")) {
                    ps.setString(1, part.scope());
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(rs.getString(1));
                        }
                    }
                }
                return null;
            });
        }
        return new ArrayList<>(out);
    }

    /** Distinct namespaces of a project/database (all hosts); the default namespace is the empty string. */
    List<String> namespaces(DsKeys.Part part) {
        TreeSet<String> out = new TreeSet<>(DsValues::compareStrings);
        String prefix = part.project() + "/" + part.database() + "/";
        for (String h : hosts(part)) {
            conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT DISTINCT ns FROM warp_datastore_entities WHERE ns LIKE ?")) {
                    ps.setString(1, prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%");
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(rs.getString(1).substring(prefix.length()));
                        }
                    }
                }
                return null;
            });
        }
        return new ArrayList<>(out);
    }

    /** Exact entity count of a scope without materialising rows. */
    long count(DsKeys.Part part, Scope scope) {
        long n = 0;
        List<String> hs = scope.ancestor() != null ? List.of(owner(part, rootOf(scope.ancestor()))) : hosts(part);
        for (String h : hs) {
            n += conn(h, c -> {
                StringBuilder w = new StringBuilder();
                List<Object> args = new ArrayList<>();
                where(part, scope, null, true, null, true, w, args);
                try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_datastore_entities WHERE " + w)) {
                    for (int i = 0; i < args.size(); i++) {
                        ps.setObject(i + 1, args.get(i));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        return rs.getLong(1);
                    }
                }
            });
        }
        return n;
    }

    // ------------------------------------------------------------------ ids

    /** Allocates {@code n} consecutive ids in a partition (on the first host of the set); returns the first. */
    long allocate(DsKeys.Part part, int n) {
        return conn(hosts(part).get(0), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_datastore_ids (scope, next_id) VALUES (?, ?) "
                    + "ON CONFLICT (scope) DO UPDATE SET next_id = warp_datastore_ids.next_id + ? RETURNING next_id - ?")) {
                ps.setString(1, part.scope());
                ps.setLong(2, 1L + n);
                ps.setLong(3, n);
                ps.setLong(4, n);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    /** Makes sure future allocations start above {@code id}. */
    void reserve(DsKeys.Part part, long id) {
        conn(hosts(part).get(0), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_datastore_ids (scope, next_id) VALUES (?, ?) "
                    + "ON CONFLICT (scope) DO UPDATE SET next_id = GREATEST(warp_datastore_ids.next_id, EXCLUDED.next_id)")) {
                ps.setString(1, part.scope());
                ps.setLong(2, id + 1);
                ps.executeUpdate();
            }
            return null;
        });
    }

    // ------------------------------------------------------------------ writes

    /** A key taking part in a write transaction. */
    record KeyRef(DsKeys.Part part, Key key, byte[] bytes) {
        String id() {
            return part.scope() + "|" + Base64.getEncoder().encodeToString(bytes);
        }
    }

    /** A final state: {@code row == null} deletes the entity. */
    record Change(KeyRef ref, Entity entity, long version, long createUs, long updateUs) {
    }

    /** One write transaction over the hosts owning the given keys: locked and loaded, then applied and committed together. */
    final class WriteTx implements AutoCloseable {
        private final Map<String, Connection> conns = new LinkedHashMap<>();
        private final Map<String, String> ownerOf = new HashMap<>();
        final Map<String, Row> current = new HashMap<>();
        private boolean done;

        WriteTx(Collection<KeyRef> refs) {
            try {
                Map<String, List<KeyRef>> byHost = new LinkedHashMap<>();
                java.util.List<KeyRef> sorted = new ArrayList<>(refs);
                sorted.sort((a, b) -> a.id().compareTo(b.id()));
                for (KeyRef r : sorted) {
                    String h = owner(r.part(), rootOf(r.bytes()));
                    ownerOf.put(r.id(), h);
                    byHost.computeIfAbsent(h, k -> new ArrayList<>()).add(r);
                }
                List<String> all = new ArrayList<>(byHost.keySet());
                java.util.Collections.sort(all);
                for (String h : all) {
                    List<KeyRef> mine = byHost.get(h);
                    Connection c = target(h).openManualCommit();
                    conns.put(h, c);
                    try (Statement st = c.createStatement()) {
                        StringBuilder sb = new StringBuilder();
                        for (KeyRef r : mine) {
                            sb.append("SELECT pg_advisory_xact_lock(hashtextextended('").append(r.id().replace("'", "''")).append("', 2));");
                        }
                        st.execute(sb.toString());
                    }
                    Map<String, List<KeyRef>> byScope = new LinkedHashMap<>();
                    for (KeyRef r : mine) {
                        byScope.computeIfAbsent(r.part().scope(), k -> new ArrayList<>()).add(r);
                    }
                    for (var e : byScope.entrySet()) {
                        byte[][] keys = e.getValue().stream().map(KeyRef::bytes).toArray(byte[][]::new);
                        DsKeys.Part p = e.getValue().get(0).part();
                        try (PreparedStatement ps = c.prepareStatement("SELECT " + COLS + " FROM warp_datastore_entities WHERE ns = ? AND key_bytes = ANY(?)")) {
                            ps.setString(1, e.getKey());
                            ps.setArray(2, c.createArrayOf("bytea", keys));
                            try (ResultSet rs = ps.executeQuery()) {
                                while (rs.next()) {
                                    Row r = row(p, rs);
                                    current.put(p.scope() + "|" + Base64.getEncoder().encodeToString(r.keyBytes), r);
                                }
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                closeQuietly();
                throw storage(e);
            } catch (RuntimeException e) {
                closeQuietly();
                throw e;
            }
        }

        void applyAndCommit(List<Change> changes) {
            try {
                Map<String, List<Change>> byHost = new LinkedHashMap<>();
                for (Change ch : changes) {
                    byHost.computeIfAbsent(ownerOf.get(ch.ref().id()), k -> new ArrayList<>()).add(ch);
                }
                for (var e : byHost.entrySet()) {
                    Connection c = conns.get(e.getKey());
                    try (PreparedStatement up = c.prepareStatement("INSERT INTO warp_datastore_entities (ns, key_bytes, root_key, kind, data, version, "
                            + "create_us, update_us) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (ns, key_bytes) DO UPDATE SET data = EXCLUDED.data, "
                            + "version = EXCLUDED.version, create_us = EXCLUDED.create_us, update_us = EXCLUDED.update_us");
                            PreparedStatement del = c.prepareStatement("DELETE FROM warp_datastore_entities WHERE ns = ? AND key_bytes = ?")) {
                        for (Change ch : e.getValue()) {
                            if (ch.entity() == null) {
                                del.setString(1, ch.ref().part().scope());
                                del.setBytes(2, ch.ref().bytes());
                                del.addBatch();
                            } else {
                                Key k = ch.ref().key();
                                up.setString(1, ch.ref().part().scope());
                                up.setBytes(2, ch.ref().bytes());
                                up.setBytes(3, rootOf(ch.ref().bytes()));
                                up.setString(4, k.getPath(k.getPathCount() - 1).getKind());
                                up.setBytes(5, ch.entity().toByteArray());
                                up.setLong(6, ch.version());
                                up.setLong(7, ch.createUs());
                                up.setLong(8, ch.updateUs());
                                up.addBatch();
                            }
                        }
                        up.executeBatch();
                        del.executeBatch();
                    }
                }
                for (Connection c : conns.values()) {
                    c.commit();
                }
                done = true;
            } catch (SQLException e) {
                throw storage(e);
            }
        }

        @Override
        public void close() {
            closeQuietly();
        }

        private void closeQuietly() {
            for (Connection c : conns.values()) {
                try {
                    if (!done) {
                        c.rollback();
                    }
                } catch (SQLException ignored) {
                    // pool discards
                }
                try {
                    c.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // reset by pool
                }
                try {
                    c.close();
                } catch (SQLException ignored) {
                    // gone
                }
            }
            conns.clear();
        }
    }

    WriteTx begin(Collection<KeyRef> refs) {
        return new WriteTx(refs);
    }

    /** Deletes every entity and id counter (the emulator's /reset convenience). */
    void clearAll() {
        for (String h : hosts()) {
            conn(h, c -> {
                try (Statement st = c.createStatement()) {
                    st.execute("DELETE FROM warp_datastore_entities");
                    st.execute("DELETE FROM warp_datastore_ids");
                }
                return null;
            });
        }
    }

    /** Deletes every entity and id counter of a project's partitions (a test convenience mirroring the emulator's reset). */
    void clear(String project) {
        for (String h : hosts()) {
            conn(h, c -> {
                try (PreparedStatement a = c.prepareStatement("DELETE FROM warp_datastore_entities WHERE ns LIKE ?");
                        PreparedStatement b = c.prepareStatement("DELETE FROM warp_datastore_ids WHERE scope LIKE ?")) {
                    String like = project.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "/%";
                    a.setString(1, like);
                    a.executeUpdate();
                    b.setString(1, like);
                    b.executeUpdate();
                }
                return null;
            });
        }
    }
}
