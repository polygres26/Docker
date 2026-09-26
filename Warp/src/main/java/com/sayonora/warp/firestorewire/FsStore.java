package com.sayonora.warp.firestorewire;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ShardingStrategy;
import com.sayonora.warp.core.StoreBootstrap;
import com.sayonora.warp.core.StoreType;
import com.google.protobuf.InvalidProtocolBufferException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres storage of firestorewire (see {@code ddl/postgres/firestorewire_store.sql}). Shards by hash of database + document
 * path; every operation borrows a pooled connection for one short statement or transaction; nothing is held across client I/O
 * or between the pages of a streamed scan.
 */
final class FsStore {

    private static final Logger log = LoggerFactory.getLogger(FsStore.class);
    static final String CHANNEL = "warp_firestore";
    static final int PAGE = 1000;

    private final BackendRegistry registry;
    private final StoreType type = StoreType.FIRESTORE;
    private static final ThreadLocal<List<String>> ROUTED = new ThreadLocal<>();
    final long historyMicros;

    FsStore(BackendRegistry registry, long historySeconds) {
        this.registry = registry;
        this.historyMicros = historySeconds * 1_000_000L;
    }

    static void route(List<String> hosts) {
        if (hosts == null) {
            ROUTED.remove();
        } else {
            ROUTED.set(hosts);
        }
    }

    BackendRegistry registry() {
        return registry;
    }

    List<String> hosts() {
        List<String> r = ROUTED.get();
        List<String> h = r != null ? r : registry.storeHosts(type);
        if (h.isEmpty()) {
            throw FsException.unavailable("No Postgres backend of this set has the " + type.id() + " store enabled");
        }
        return h;
    }

    /** The hosts serving a database: a connect-time route (project or database id naming a backend or set) or the store's hosts. */
    List<String> hosts(FsNames.Db db) {
        try {
            var router = registry.connectionRouter();
            if (router != null) {
                String name = db.database().equals("(default)") ? db.project() : db.database();
                List<String> r = router.storeBackends(router.resolve(com.sayonora.warp.core.ConnectionRouter.PROTO_HTTP, name, null));
                if (r != null) {
                    if (r.isEmpty()) {
                        throw FsException.precondition("The backend or set \"" + name + "\" cannot host the firestore store");
                    }
                    return r;
                }
            }
        } catch (FsException e) {
            throw e;
        } catch (RuntimeException e) {
            // no router: default placement
        }
        return hosts();
    }

    boolean available() {
        return !registry.storeHosts(type).isEmpty();
    }

    String owner(FsNames.Db db, String rel) {
        List<String> h = hosts(db);
        return h.size() == 1 ? h.get(0) : ShardingStrategy.hash(h).resolve(db.name() + "/" + rel);
    }

    BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("firestorewire: backend '" + host + "' is not registered");
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

    static FsException storage(SQLException e) {
        log.error("firestorewire: Postgres store error", e);
        return FsException.internal("Internal error (" + e.getMessage() + ")");
    }

    // ------------------------------------------------------------------ documents

    /** A stored document version. */
    static final class Doc {
        final String rel;
        final Map<String, Value> fields;
        final long createUs;
        final long updateUs;

        Doc(String rel, Map<String, Value> fields, long createUs, long updateUs) {
            this.rel = rel;
            this.fields = fields;
            this.createUs = createUs;
            this.updateUs = updateUs;
        }

        Document toProto(FsNames.Db db) {
            return Document.newBuilder().setName(db.docsRoot() + "/" + rel).putAllFields(fields)
                    .setCreateTime(FsClock.ts(createUs)).setUpdateTime(FsClock.ts(updateUs)).build();
        }

        byte[] nameKey() {
            return FsNames.nameKey(rel);
        }
    }

    static byte[] encode(Map<String, Value> fields) {
        return Document.newBuilder().putAllFields(fields).build().toByteArray();
    }

    static Map<String, Value> decode(byte[] data) {
        try {
            return Document.parseFrom(data).getFieldsMap();
        } catch (InvalidProtocolBufferException e) {
            throw FsException.internal("corrupt document payload");
        }
    }

    private static Doc row(ResultSet rs) throws SQLException {
        return new Doc(rs.getString("path"), decode(rs.getBytes("data")), rs.getLong("create_us"), rs.getLong("update_us"));
    }

    private static final String COLS = "name_key, path, data, create_us, update_us";

    /** Point reads (current state or as of {@code atUs}); absent documents are missing from the result. */
    Map<String, Doc> getMany(FsNames.Db db, Collection<String> rels, Long atUs) {
        Map<String, List<String>> byHost = new LinkedHashMap<>();
        for (String r : rels) {
            byHost.computeIfAbsent(owner(db, r), k -> new ArrayList<>()).add(r);
        }
        Map<String, Doc> out = new HashMap<>();
        for (var e : byHost.entrySet()) {
            out.putAll(conn(e.getKey(), c -> {
                Map<String, Doc> m = new HashMap<>();
                byte[][] keys = e.getValue().stream().map(FsNames::nameKey).toArray(byte[][]::new);
                Array arr = c.createArrayOf("bytea", keys);
                String sql = atUs == null
                        ? "SELECT " + COLS + " FROM warp_firestore_docs WHERE db = ? AND name_key = ANY(?)"
                        : "SELECT * FROM (SELECT DISTINCT ON (name_key) name_key, path, data, create_us, commit_us AS update_us, deleted "
                        + "FROM warp_firestore_log WHERE db = ? AND name_key = ANY(?) AND commit_us <= ? "
                        + "ORDER BY name_key, commit_us DESC) t WHERE NOT deleted";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setString(1, db.name());
                    ps.setArray(2, arr);
                    if (atUs != null) {
                        ps.setLong(3, atUs);
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Doc d = row(rs);
                            m.put(d.rel, d);
                        }
                    }
                }
                return m;
            }));
        }
        return out;
    }

    Doc get(FsNames.Db db, String rel, Long atUs) {
        return getMany(db, List.of(rel), atUs).get(rel);
    }

    // ------------------------------------------------------------------ scans

    /** Which documents a scan covers: direct children of a collection, or descendants of a parent with a collection id. */
    record Scope(String collPath, String collId, String descendantsOf, Integer directDepth) {
        /** Direct children of {@code collPath} (a full collection path such as "users" or "users/u1/posts"). */
        static Scope collection(String collPath) {
            return new Scope(collPath, null, null, null);
        }

        /** Collection group: every document whose parent collection has this id, below {@code parent} ("" = whole database). */
        static Scope group(String parentDocRel, String collId) {
            return new Scope(null, collId, parentDocRel, null);
        }

        /** Documents of every collection that is a direct child of {@code parent} (a query with an empty FROM). */
        static Scope children(String parentDocRel) {
            return new Scope(null, null, parentDocRel, FsNames.depth(parentDocRel) + 2 - 0);
        }

        /** Every document below {@code parent}, at any depth. */
        static Scope all(String parentDocRel) {
            return new Scope(null, null, parentDocRel, null);
        }
    }

    /** SQL predicate (and its parameters) selecting a scope. */
    static String scopeSql(Scope scope, StringBuilder w, List<Object> args) {
        if (scope.collPath() != null) {
            w.append(" AND coll_path = ?");
            args.add(scope.collPath());
        }
        if (scope.collId() != null) {
            w.append(" AND coll_id = ?");
            args.add(scope.collId());
        }
        if (scope.descendantsOf() != null && !scope.descendantsOf().isEmpty()) {
            w.append(" AND path >= ? AND path < ?");
            args.add(scope.descendantsOf() + "/");
            args.add(scope.descendantsOf() + "0");
        }
        if (scope.directDepth() != null) {
            w.append(" AND (length(path) - length(replace(path, '/', ''))) = ?");
            args.add(scope.directDepth() - 1);
        }
        return w.toString();
    }

    /** Lazy, host-paged, keyset scan of one host ordered by name key. */
    final class HostScan implements Iterator<Doc> {
        private final String host;
        private final FsNames.Db db;
        private final Scope scope;
        private final boolean desc;
        private final Long atUs;
        private final byte[] lo;
        private final byte[] hi;
        private final boolean loIncl;
        private final boolean hiIncl;
        private byte[] last;
        private List<Doc> buf = List.of();
        private int pos;
        private boolean exhausted;

        HostScan(String host, FsNames.Db db, Scope scope, boolean desc, Long atUs, byte[] lo, boolean loIncl, byte[] hi, boolean hiIncl) {
            this.host = host;
            this.db = db;
            this.scope = scope;
            this.desc = desc;
            this.atUs = atUs;
            this.lo = lo;
            this.loIncl = loIncl;
            this.hi = hi;
            this.hiIncl = hiIncl;
        }

        private void fill() {
            List<Doc> page = conn(host, c -> {
                StringBuilder w = new StringBuilder("db = ?");
                List<Object> args = new ArrayList<>();
                args.add(db.name());
                scopeSql(scope, w, args);
                byte[] lower = desc ? lo : (last != null ? last : lo);
                boolean lowerIncl = desc ? loIncl : (last != null ? false : loIncl);
                byte[] upper = desc ? (last != null ? last : hi) : hi;
                boolean upperIncl = desc ? (last != null ? false : hiIncl) : hiIncl;
                if (lower != null) {
                    w.append(" AND name_key ").append(lowerIncl ? ">=" : ">").append(" ?");
                    args.add(lower);
                }
                if (upper != null) {
                    w.append(" AND name_key ").append(upperIncl ? "<=" : "<").append(" ?");
                    args.add(upper);
                }
                String order = desc ? "name_key DESC" : "name_key";
                String sql;
                if (atUs == null) {
                    sql = "SELECT " + COLS + " FROM warp_firestore_docs WHERE " + w + " ORDER BY " + order + " LIMIT " + PAGE;
                } else {
                    sql = "SELECT * FROM (SELECT DISTINCT ON (name_key) name_key, path, data, create_us, commit_us AS update_us, deleted "
                            + "FROM warp_firestore_log WHERE " + w + " AND commit_us <= ? ORDER BY name_key, commit_us DESC) t "
                            + "WHERE NOT deleted ORDER BY " + order + " LIMIT " + PAGE;
                    args.add(atUs);
                }
                List<Doc> out = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    for (int i = 0; i < args.size(); i++) {
                        ps.setObject(i + 1, args.get(i));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(row(rs));
                        }
                    }
                }
                return out;
            });
            buf = page;
            pos = 0;
            if (page.size() < PAGE) {
                exhausted = true;
            }
            if (!page.isEmpty()) {
                last = page.get(page.size() - 1).nameKey();
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
        public Doc next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buf.get(pos++);
        }
    }

    /** K-way merge of the per-host scans by name key: the exact global document order. */
    Iterator<Doc> scan(FsNames.Db db, Scope scope, boolean desc, Long atUs, byte[] lo, boolean loIncl, byte[] hi, boolean hiIncl) {
        List<String> hs = hosts(db);
        List<HostScan> scans = new ArrayList<>();
        for (String h : hs) {
            scans.add(new HostScan(h, db, scope, desc, atUs, lo, loIncl, hi, hiIncl));
        }
        if (scans.size() == 1) {
            return scans.get(0);
        }
        return new Iterator<>() {
            private final Doc[] head = new Doc[scans.size()];
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
                for (Doc d : head) {
                    if (d != null) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Doc next() {
                prime();
                int best = -1;
                for (int i = 0; i < head.length; i++) {
                    if (head[i] == null) {
                        continue;
                    }
                    if (best < 0) {
                        best = i;
                    } else {
                        int c = java.util.Arrays.compareUnsigned(head[i].nameKey(), head[best].nameKey());
                        if (desc ? c > 0 : c < 0) {
                            best = i;
                        }
                    }
                }
                if (best < 0) {
                    throw new NoSuchElementException();
                }
                Doc d = head[best];
                head[best] = scans.get(best).hasNext() ? scans.get(best).next() : null;
                return d;
            }
        };
    }

    /** Distinct child collection ids of a parent (a document path, or "" for the root), from the current state. */
    List<String> collectionIds(FsNames.Db db, String parentRel, Long atUs) {
        TreeMap<String, Boolean> ids = new TreeMap<>(FsValues::compareStrings);
        int prefixDepth = FsNames.depth(parentRel);
        for (String h : hosts(db)) {
            conn(h, c -> {
                String where = "db = ?" + (parentRel.isEmpty() ? "" : " AND path >= ? AND path < ?");
                String sql = atUs == null
                        ? "SELECT DISTINCT coll_path FROM warp_firestore_docs WHERE " + where
                        : "SELECT DISTINCT coll_path FROM (SELECT DISTINCT ON (name_key) name_key, coll_path, deleted FROM warp_firestore_log "
                        + "WHERE " + where + " AND commit_us <= ? ORDER BY name_key, commit_us DESC) t WHERE NOT deleted";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    int i = 1;
                    ps.setString(i++, db.name());
                    if (!parentRel.isEmpty()) {
                        ps.setString(i++, parentRel + "/");
                        ps.setString(i++, parentRel + "0");
                    }
                    if (atUs != null) {
                        ps.setLong(i, atUs);
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            List<String> segs = FsNames.segments(rs.getString(1));
                            if (segs.size() > prefixDepth) {
                                ids.put(segs.get(prefixDepth), true);
                            }
                        }
                    }
                }
                return null;
            });
        }
        return new ArrayList<>(ids.keySet());
    }

    /** Exact document count of a scope on all hosts without materialising documents (used for unfiltered count()). */
    long count(FsNames.Db db, Scope scope) {
        long n = 0;
        for (String h : hosts(db)) {
            n += conn(h, c -> {
                StringBuilder w = new StringBuilder("db = ?");
                List<Object> args = new ArrayList<>();
                args.add(db.name());
                scopeSql(scope, w, args);
                try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_firestore_docs WHERE " + w)) {
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

    // ------------------------------------------------------------------ writes

    /** A final state for one document: {@code doc == null} deletes it. */
    record Change(String rel, Doc doc) {
    }

    /** One write transaction over the hosts owning the given documents: locked and loaded, then applied and committed together. */
    final class WriteTx implements AutoCloseable {
        private final FsNames.Db db;
        private final Map<String, Connection> conns = new LinkedHashMap<>();
        private final Map<String, String> ownerOf = new HashMap<>();
        final Map<String, Doc> current = new HashMap<>();
        /** Newest commit time already in the database log (read under the per-database lock): the next commit must be later. */
        long lastCommitUs;
        private boolean done;

        WriteTx(FsNames.Db db, Collection<String> rels) {
            this.db = db;
            try {
                Map<String, List<String>> byHost = new HashMap<>();
                for (String r : new java.util.TreeSet<>(rels)) {
                    String h = owner(db, r);
                    ownerOf.put(r, h);
                    byHost.computeIfAbsent(h, k -> new ArrayList<>()).add(r);
                }
                for (String h : hosts(db)) {
                    List<String> mine = byHost.get(h);
                    if (mine == null) {
                        continue;
                    }
                    Connection c = target(h).openManualCommit();
                    conns.put(h, c);
                    try (Statement st = c.createStatement()) {
                        // one lock per database serialises log order == commit order (the Listen change feed relies on it)
                        StringBuilder sb = new StringBuilder("SELECT pg_advisory_xact_lock(hashtextextended('fsdb:" + db.name().replace("'", "''") + "', 0));");
                        for (String r : mine) {
                            sb.append("SELECT pg_advisory_xact_lock(hashtextextended('")
                                    .append((db.name() + "/" + r).replace("'", "''")).append("', 1));");
                        }
                        st.execute(sb.toString());
                    }
                    try (PreparedStatement ps = c.prepareStatement("SELECT coalesce(max(commit_us), 0) FROM warp_firestore_log WHERE db = ?")) {
                        ps.setString(1, db.name());
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                            lastCommitUs = Math.max(lastCommitUs, rs.getLong(1));
                        }
                    }
                    byte[][] keys = mine.stream().map(FsNames::nameKey).toArray(byte[][]::new);
                    try (PreparedStatement ps = c.prepareStatement("SELECT " + COLS + " FROM warp_firestore_docs WHERE db = ? AND name_key = ANY(?)")) {
                        ps.setString(1, db.name());
                        ps.setArray(2, c.createArrayOf("bytea", keys));
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                Doc d = row(rs);
                                current.put(d.rel, d);
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

        /** Applies the final states (with the commit time) and commits every host. */
        void applyAndCommit(List<Change> changes, long commitUs) {
            try {
                Map<String, List<Change>> byHost = new LinkedHashMap<>();
                for (Change ch : changes) {
                    byHost.computeIfAbsent(ownerOf.get(ch.rel()), k -> new ArrayList<>()).add(ch);
                }
                for (var e : byHost.entrySet()) {
                    Connection c = conns.get(e.getKey());
                    try (PreparedStatement up = c.prepareStatement("INSERT INTO warp_firestore_docs (db, name_key, path, coll_path, coll_id, data, "
                            + "create_us, update_us) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (db, name_key) DO UPDATE SET data = EXCLUDED.data, "
                            + "create_us = EXCLUDED.create_us, update_us = EXCLUDED.update_us");
                            PreparedStatement del = c.prepareStatement("DELETE FROM warp_firestore_docs WHERE db = ? AND name_key = ?");
                            PreparedStatement lg = c.prepareStatement("INSERT INTO warp_firestore_log (db, name_key, path, coll_path, coll_id, "
                                    + "commit_us, deleted, data, create_us) VALUES (?,?,?,?,?,?,?,?,?)")) {
                        for (Change ch : e.getValue()) {
                            String rel = ch.rel();
                            byte[] key = FsNames.nameKey(rel);
                            String cp = FsNames.collPath(rel);
                            String cid = FsNames.collId(rel);
                            lg.setString(1, db.name());
                            lg.setBytes(2, key);
                            lg.setString(3, rel);
                            lg.setString(4, cp);
                            lg.setString(5, cid);
                            lg.setLong(6, commitUs);
                            if (ch.doc() == null) {
                                del.setString(1, db.name());
                                del.setBytes(2, key);
                                del.addBatch();
                                lg.setBoolean(7, true);
                                lg.setNull(8, java.sql.Types.BINARY);
                                Doc prev = current.get(rel);
                                lg.setLong(9, prev == null ? commitUs : prev.createUs);
                            } else {
                                byte[] data = encode(ch.doc().fields);
                                up.setString(1, db.name());
                                up.setBytes(2, key);
                                up.setString(3, rel);
                                up.setString(4, cp);
                                up.setString(5, cid);
                                up.setBytes(6, data);
                                up.setLong(7, ch.doc().createUs);
                                up.setLong(8, ch.doc().updateUs);
                                up.addBatch();
                                lg.setBoolean(7, false);
                                lg.setBytes(8, data);
                                lg.setLong(9, ch.doc().createUs);
                            }
                            lg.addBatch();
                        }
                        up.executeBatch();
                        del.executeBatch();
                        lg.executeBatch();
                    }
                    try (PreparedStatement n = c.prepareStatement("SELECT pg_notify('" + CHANNEL + "', ?)")) {
                        n.setString(1, db.name());
                        n.execute();
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

    WriteTx begin(FsNames.Db db, Collection<String> rels) {
        return new WriteTx(db, rels);
    }

    /** Deletes every document (and its history) of a database on all its hosts (the emulator's "clear all data" convenience). */
    void clear(FsNames.Db db) {
        for (String h : hosts(db)) {
            conn(h, c -> {
                try (PreparedStatement a = c.prepareStatement("DELETE FROM warp_firestore_docs WHERE db = ?");
                        PreparedStatement b = c.prepareStatement("DELETE FROM warp_firestore_log WHERE db = ?")) {
                    a.setString(1, db.name());
                    a.executeUpdate();
                    b.setString(1, db.name());
                    b.executeUpdate();
                }
                return null;
            });
        }
    }

    // ------------------------------------------------------------------ change feed & maintenance

    /** One version-log row. */
    record LogRow(long seq, String rel, long commitUs, boolean deleted, Doc doc) {
    }

    /** Newest commit time of a database on one host (0 if none). */
    long maxCommitUs(String host, FsNames.Db db) {
        return conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT coalesce(max(commit_us), 0) FROM warp_firestore_log WHERE db = ?")) {
                ps.setString(1, db.name());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    /** Log rows of a database on one host with commit time greater than {@code afterUs}, oldest first. */
    List<LogRow> logSince(String host, FsNames.Db db, long afterUs, int limit) {
        return conn(host, c -> {
            List<LogRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT seq, path, commit_us, deleted, data, create_us FROM warp_firestore_log "
                    + "WHERE db = ? AND commit_us > ? ORDER BY commit_us, seq LIMIT " + limit)) {
                ps.setString(1, db.name());
                ps.setLong(2, afterUs);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        boolean del = rs.getBoolean("deleted");
                        String rel = rs.getString("path");
                        Doc d = del ? null : new Doc(rel, decode(rs.getBytes("data")), rs.getLong("create_us"), rs.getLong("commit_us"));
                        out.add(new LogRow(rs.getLong("seq"), rel, rs.getLong("commit_us"), del, d));
                    }
                }
            }
            return out;
        });
    }

    /** Oldest time reads may target (now - retention). */
    long horizonUs() {
        return FsClock.wallMicros() - historyMicros;
    }

    /** Drops log rows older than the retention window that a newer row at or before the horizon supersedes. */
    void prune() {
        long horizon = horizonUs();
        for (String h : registry.storeHosts(type)) {
            try {
                conn(h, c -> {
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_firestore_log l WHERE l.commit_us < ? AND EXISTS ("
                            + "SELECT 1 FROM warp_firestore_log n WHERE n.db = l.db AND n.name_key = l.name_key AND n.commit_us <= ? "
                            + "AND (n.commit_us > l.commit_us OR (n.commit_us = l.commit_us AND n.seq > l.seq)))")) {
                        ps.setLong(1, horizon);
                        ps.setLong(2, horizon);
                        ps.executeUpdate();
                    }
                    return null;
                });
            } catch (RuntimeException e) {
                log.debug("firestorewire log prune failed on {}: {}", h, e.getMessage());
            }
        }
    }
}
