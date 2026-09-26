package com.sayonora.warp.azurewire;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sayonora.warp.azurewire.BlobModel.Blob;
import com.sayonora.warp.azurewire.BlobModel.Cont;
import com.sayonora.warp.azurewire.BlobModel.Lease;
import com.sayonora.warp.azurewire.BlobModel.Seg;
import com.sayonora.warp.core.StoreType;
import com.sayonora.warp.s3wire.ChunkMath;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Blob storage: containers (catalog on the home host), blobs / snapshots / data chunks (on the shard owning
 * hash(account/container/blob)). A blob's content is an ordered list of segments, each a chunked data blob
 * ({@code warp_azblob_data}, chunks of {@link #CHUNK} bytes) owned by exactly one blob row through
 * {@code warp_azblob_data_owner}, so deleting or replacing a blob drops exactly its own data; snapshots and copies
 * duplicate the data server-side (INSERT ... SELECT) instead of sharing it. Bodies are streamed chunk by chunk, each
 * chunk its own short borrow of a pooled connection, so a slow client never pins one.
 */
final class BlobStore {

    static final int CHUNK = 4 * 1024 * 1024;
    private static final Gson GSON = new Gson();
    private static final TypeToken<Map<String, String>> MAP_T = new TypeToken<>() { };
    private static final TypeToken<List<Seg>> SEGS_T = new TypeToken<>() { };
    private static final TypeToken<List<Map<String, String>>> ACL_T = new TypeToken<>() { };

    final AzShards shards;

    BlobStore(AzShards shards) {
        this.shards = shards;
    }

    static String shardKey(String account, String container, String blob) {
        return account + "/" + container + "/" + blob;
    }

    String ownerHost(String account, String container, String blob) {
        return shards.owner(shardKey(account, container, blob));
    }

    // ------------------------------------------------------------------------------------------ containers

    private static final String CONT_COLS = "account, name, created_at, last_modified, etag, metadata, public_access, "
            + "acl, lease_state, lease_id, lease_duration, lease_expiry, lease_break";

    private static Instant inst(Timestamp t) {
        return t == null ? null : t.toInstant();
    }

    private static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }

    private static Cont readCont(ResultSet rs) throws SQLException {
        Cont c = new Cont();
        c.account = rs.getString(1);
        c.name = rs.getString(2);
        c.createdAt = inst(rs.getTimestamp(3));
        c.lastModified = inst(rs.getTimestamp(4));
        c.etag = rs.getString(5);
        c.metadata = orEmpty(GSON.fromJson(rs.getString(6), MAP_T));
        c.publicAccess = rs.getString(7);
        List<Map<String, String>> acl = GSON.fromJson(rs.getString(8), ACL_T);
        c.acl = new ArrayList<>();
        if (acl != null) {
            for (Map<String, String> m : acl) {
                c.acl.add(new AzureAuth.Policy(m.get("id"), m.get("start"), m.get("expiry"), m.get("permission")));
            }
        }
        c.lease.state = rs.getString(9);
        c.lease.id = rs.getString(10);
        c.lease.duration = rs.getInt(11);
        c.lease.expiry = inst(rs.getTimestamp(12));
        c.lease.breakAt = inst(rs.getTimestamp(13));
        return c;
    }

    private static Map<String, String> orEmpty(Map<String, String> m) {
        return m == null ? new LinkedHashMap<>() : new LinkedHashMap<>(m);
    }

    static String aclJson(List<AzureAuth.Policy> acl) {
        List<Map<String, String>> l = new ArrayList<>();
        for (AzureAuth.Policy p : acl) {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("id", p.id());
            if (p.start() != null) {
                m.put("start", p.start());
            }
            if (p.expiry() != null) {
                m.put("expiry", p.expiry());
            }
            if (p.permission() != null) {
                m.put("permission", p.permission());
            }
            l.add(m);
        }
        return GSON.toJson(l);
    }

    Cont getContainer(String account, String name) {
        return shards.conn(shards.home(), c -> selectContainer(c, account, name, false));
    }

    static Cont selectContainer(Connection c, String account, String name, boolean forUpdate) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + CONT_COLS + " FROM warp_azblob_containers "
                + "WHERE account=? AND name=?" + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, account);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readCont(rs) : null;
            }
        }
    }

    static void insertContainer(Connection c, Cont k) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_azblob_containers (account, name, created_at, "
                + "last_modified, etag, metadata, public_access, acl) VALUES (?,?,?,?,?,?::jsonb,?,?::jsonb)")) {
            ps.setString(1, k.account);
            ps.setString(2, k.name);
            ps.setTimestamp(3, ts(k.createdAt));
            ps.setTimestamp(4, ts(k.lastModified));
            ps.setString(5, k.etag);
            ps.setString(6, GSON.toJson(k.metadata));
            ps.setString(7, k.publicAccess);
            ps.setString(8, aclJson(k.acl));
            ps.executeUpdate();
        }
    }

    static void updateContainer(Connection c, Cont k) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_azblob_containers SET last_modified=?, etag=?, "
                + "metadata=?::jsonb, public_access=?, acl=?::jsonb, lease_state=?, lease_id=?, lease_duration=?, "
                + "lease_expiry=?, lease_break=? WHERE account=? AND name=?")) {
            ps.setTimestamp(1, ts(k.lastModified));
            ps.setString(2, k.etag);
            ps.setString(3, GSON.toJson(k.metadata));
            ps.setString(4, k.publicAccess);
            ps.setString(5, aclJson(k.acl));
            ps.setString(6, k.lease.state);
            ps.setString(7, k.lease.id);
            ps.setInt(8, k.lease.duration);
            ps.setTimestamp(9, ts(k.lease.expiry));
            ps.setTimestamp(10, ts(k.lease.breakAt));
            ps.setString(11, k.account);
            ps.setString(12, k.name);
            ps.executeUpdate();
        }
    }

    /** Runs {@code fn} on the locked container row inside one transaction on the home host; {@code fn} mutates and returns it. */
    Cont mutateContainer(String account, String name, java.util.function.Function<Cont, Cont> fn) {
        return shards.tx(shards.home(), c -> {
            Cont k = selectContainer(c, account, name, true);
            if (k == null) {
                throw AzErrors.containerNotFound();
            }
            Cont out = fn.apply(k);
            if (out != null) {
                updateContainer(c, out);
            }
            return out == null ? k : out;
        });
    }

    void createContainer(Cont k) {
        shards.tx(shards.home(), c -> {
            Cont ex = selectContainer(c, k.account, k.name, true);
            if (ex != null) {
                throw new AzureException(409, "ContainerAlreadyExists", "The specified container already exists.");
            }
            insertContainer(c, k);
            return null;
        });
    }

    List<Cont> listContainers(String account, String prefix, String marker, int limit) {
        return shards.conn(shards.home(), c -> {
            StringBuilder sql = new StringBuilder("SELECT " + CONT_COLS + " FROM warp_azblob_containers WHERE account=?");
            List<Object> args = new ArrayList<>();
            args.add(account);
            if (prefix != null && !prefix.isEmpty()) {
                sql.append(" AND name >= ? AND name < ?");
                args.add(prefix);
                args.add(prefixEnd(prefix));
            }
            if (marker != null && !marker.isEmpty()) {
                sql.append(" AND name >= ?");
                args.add(marker);
            }
            sql.append(" ORDER BY name LIMIT ").append(limit);
            List<Cont> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                for (int i = 0; i < args.size(); i++) {
                    ps.setObject(i + 1, args.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(readCont(rs));
                    }
                }
            }
            return out;
        });
    }

    static String sk(String snapshot) {
        return snapshot == null || snapshot.isEmpty() ? "~" : snapshot;
    }

    static String prefixEnd(String prefix) {
        if (prefix.isEmpty()) {
            return "￿";
        }
        char last = prefix.charAt(prefix.length() - 1);
        return prefix.substring(0, prefix.length() - 1) + (char) (last + 1);
    }

    /** Deletes the container row on the home host, then every blob and data row of it on every shard. */
    void deleteContainer(String account, String name) {
        shards.tx(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_azblob_containers WHERE account=? AND name=?")) {
                ps.setString(1, account);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
        for (String h : shards.hosts()) {
            shards.tx(h, c -> {
                exec(c, "DELETE FROM warp_azblob_data WHERE data_id IN (SELECT data_id FROM warp_azblob_data_owner "
                        + "WHERE account=? AND container=?)", account, name);
                exec(c, "DELETE FROM warp_azblob_data_owner WHERE account=? AND container=?", account, name);
                exec(c, "DELETE FROM warp_azblob_blobs WHERE account=? AND container=?", account, name);
                return null;
            });
        }
    }

    static int exec(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            return ps.executeUpdate();
        }
    }

    // ------------------------------------------------------------------------------------------ service properties

    String serviceProperties(String account) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT properties::text FROM warp_azblob_service WHERE account=?")) {
                ps.setString(1, account);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : "{}";
                }
            }
        });
    }

    void setServiceProperties(String account, String json) {
        shards.conn(shards.home(), c -> exec(c, "INSERT INTO warp_azblob_service (account, properties) VALUES (?, ?::jsonb) "
                + "ON CONFLICT (account) DO UPDATE SET properties = EXCLUDED.properties", account, json));
    }

    // ------------------------------------------------------------------------------------------ blob rows

    private static final String BLOB_COLS = "account, container, name, snapshot, blob_type, size, etag, created_at, "
            + "last_modified, content_type, content_encoding, content_language, content_md5, cache_control, "
            + "content_disposition, metadata, tags, tier, tier_inferred, tier_changed, segments, sealed, seq, "
            + "lease_state, lease_id, lease_duration, lease_expiry, lease_break, copy_id, copy_source, copy_status, "
            + "copy_completion";

    static Blob readBlob(ResultSet rs) throws SQLException {
        Blob b = new Blob();
        b.account = rs.getString(1);
        b.container = rs.getString(2);
        b.name = rs.getString(3);
        b.snapshot = rs.getString(4);
        b.type = rs.getString(5);
        b.size = rs.getLong(6);
        b.etag = rs.getString(7);
        b.createdAt = inst(rs.getTimestamp(8));
        b.lastModified = inst(rs.getTimestamp(9));
        b.contentType = rs.getString(10);
        b.contentEncoding = rs.getString(11);
        b.contentLanguage = rs.getString(12);
        b.contentMd5 = rs.getString(13);
        b.cacheControl = rs.getString(14);
        b.contentDisposition = rs.getString(15);
        b.metadata = orEmpty(GSON.fromJson(rs.getString(16), MAP_T));
        b.tags = orEmpty(GSON.fromJson(rs.getString(17), MAP_T));
        b.tier = rs.getString(18);
        b.tierInferred = rs.getBoolean(19);
        b.tierChanged = inst(rs.getTimestamp(20));
        List<Seg> segs = GSON.fromJson(rs.getString(21), SEGS_T);
        b.segments = segs == null ? new ArrayList<>() : segs;
        b.sealed = rs.getBoolean(22);
        b.seq = rs.getLong(23);
        b.lease.state = rs.getString(24);
        b.lease.id = rs.getString(25);
        b.lease.duration = rs.getInt(26);
        b.lease.expiry = inst(rs.getTimestamp(27));
        b.lease.breakAt = inst(rs.getTimestamp(28));
        b.copyId = rs.getString(29);
        b.copySource = rs.getString(30);
        b.copyStatus = rs.getString(31);
        b.copyCompletion = inst(rs.getTimestamp(32));
        return b;
    }

    static Blob selectBlob(Connection c, String account, String container, String name, String snapshot, boolean forUpdate)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + BLOB_COLS + " FROM warp_azblob_blobs WHERE account=? "
                + "AND container=? AND name=? AND snapshot=?" + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, account);
            ps.setString(2, container);
            ps.setString(3, name);
            ps.setString(4, snapshot == null ? "" : snapshot);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readBlob(rs) : null;
            }
        }
    }

    Blob getBlob(String account, String container, String name, String snapshot) {
        return shards.conn(ownerHost(account, container, name), c -> selectBlob(c, account, container, name, snapshot, false));
    }

    static void upsertBlob(Connection c, Blob b) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_azblob_blobs (" + BLOB_COLS + ") VALUES "
                + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb,?::jsonb,?,?,?,?::jsonb,?,?,?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (account, container, name, snapshot) DO UPDATE SET blob_type=EXCLUDED.blob_type, "
                + "size=EXCLUDED.size, etag=EXCLUDED.etag, created_at=EXCLUDED.created_at, last_modified=EXCLUDED.last_modified, "
                + "content_type=EXCLUDED.content_type, content_encoding=EXCLUDED.content_encoding, "
                + "content_language=EXCLUDED.content_language, content_md5=EXCLUDED.content_md5, "
                + "cache_control=EXCLUDED.cache_control, content_disposition=EXCLUDED.content_disposition, "
                + "metadata=EXCLUDED.metadata, tags=EXCLUDED.tags, tier=EXCLUDED.tier, tier_inferred=EXCLUDED.tier_inferred, "
                + "tier_changed=EXCLUDED.tier_changed, segments=EXCLUDED.segments, sealed=EXCLUDED.sealed, seq=EXCLUDED.seq, "
                + "lease_state=EXCLUDED.lease_state, lease_id=EXCLUDED.lease_id, lease_duration=EXCLUDED.lease_duration, "
                + "lease_expiry=EXCLUDED.lease_expiry, lease_break=EXCLUDED.lease_break, copy_id=EXCLUDED.copy_id, "
                + "copy_source=EXCLUDED.copy_source, copy_status=EXCLUDED.copy_status, copy_completion=EXCLUDED.copy_completion")) {
            int i = 1;
            ps.setString(i++, b.account);
            ps.setString(i++, b.container);
            ps.setString(i++, b.name);
            ps.setString(i++, b.snapshot == null ? "" : b.snapshot);
            ps.setString(i++, b.type);
            ps.setLong(i++, b.size);
            ps.setString(i++, b.etag);
            ps.setTimestamp(i++, ts(b.createdAt));
            ps.setTimestamp(i++, ts(b.lastModified));
            ps.setString(i++, b.contentType);
            ps.setString(i++, b.contentEncoding);
            ps.setString(i++, b.contentLanguage);
            ps.setString(i++, b.contentMd5);
            ps.setString(i++, b.cacheControl);
            ps.setString(i++, b.contentDisposition);
            ps.setString(i++, GSON.toJson(b.metadata));
            ps.setString(i++, GSON.toJson(b.tags));
            ps.setString(i++, b.tier);
            ps.setBoolean(i++, b.tierInferred);
            ps.setTimestamp(i++, ts(b.tierChanged));
            ps.setString(i++, GSON.toJson(b.segments));
            ps.setBoolean(i++, b.sealed);
            ps.setLong(i++, b.seq);
            ps.setString(i++, b.lease.state);
            ps.setString(i++, b.lease.id);
            ps.setInt(i++, b.lease.duration);
            ps.setTimestamp(i++, ts(b.lease.expiry));
            ps.setTimestamp(i++, ts(b.lease.breakAt));
            ps.setString(i++, b.copyId);
            ps.setString(i++, b.copySource);
            ps.setString(i++, b.copyStatus);
            ps.setTimestamp(i, ts(b.copyCompletion));
            ps.executeUpdate();
        }
    }

    // ------------------------------------------------------------------------------------------ data

    record Ingested(String dataId, long size, String md5) {
    }

    /**
     * Streams {@code in} into chunk rows of a new data blob owned by (account, container, name, snapshot) as
     * {@code kind} ('T' temporary until adopted, 'U' uncommitted block, 'C' committed). At most {@code max} bytes.
     */
    Ingested ingest(String host, InputStream in, long max, String account, String container, String name,
            String snapshot, String kind, String blockId) throws IOException {
        String dataId = UUID.randomUUID().toString();
        shards.conn(host, c -> exec(c, "INSERT INTO warp_azblob_data_owner (data_id, account, container, name, snapshot, "
                + "kind, block_id) VALUES (?::uuid,?,?,?,?,?,?)", dataId, account, container, name,
                snapshot == null ? "" : snapshot, "T", blockId));
        MessageDigest md5 = md5();
        long total = 0;
        int seq = 0;
        byte[] buf = new byte[(int) Math.min(CHUNK, Math.max(1, max < 0 ? CHUNK : Math.min(max + 1, CHUNK)))];
        try {
            while (true) {
                int n = 0;
                while (n < buf.length) {
                    int r = in.read(buf, n, buf.length - n);
                    if (r < 0) {
                        break;
                    }
                    n += r;
                }
                if (n == 0) {
                    break;
                }
                total += n;
                if (max >= 0 && total > max) {
                    throw new AzureException(413, "RequestBodyTooLarge",
                            "The request body is too large and exceeds the maximum permissible limit.");
                }
                md5.update(buf, 0, n);
                final int fseq = seq++;
                final int fn = n;
                final byte[] chunk = n == buf.length ? buf : java.util.Arrays.copyOf(buf, n);
                shards.conn(host, c -> {
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_azblob_data (data_id, seq, data) "
                            + "VALUES (?::uuid,?,?)")) {
                        ps.setString(1, dataId);
                        ps.setInt(2, fseq);
                        ps.setBytes(3, chunk);
                        return ps.executeUpdate();
                    }
                });
                if (n < buf.length) {
                    break;
                }
            }
        } catch (IOException | RuntimeException e) {
            dropData(host, List.of(dataId));
            throw e;
        }
        final long size = total;
        shards.conn(host, c -> exec(c, "UPDATE warp_azblob_data_owner SET size=?, kind=? WHERE data_id=?::uuid", size, kind,
                dataId));
        return new Ingested(dataId, size, Base64.getEncoder().encodeToString(md5.digest()));
    }

    /** Ingests already-in-memory bytes (small bodies: page writes, batch parts). */
    Ingested ingestBytes(String host, byte[] data, String account, String container, String name, String snapshot,
            String kind, String blockId) {
        try {
            return ingest(host, new java.io.ByteArrayInputStream(data), -1, account, container, name, snapshot, kind, blockId);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    void dropData(String host, Collection<String> dataIds) {
        if (dataIds.isEmpty()) {
            return;
        }
        shards.tx(host, c -> {
            dropData(c, dataIds);
            return null;
        });
    }

    static void dropData(Connection c, Collection<String> dataIds) throws SQLException {
        if (dataIds.isEmpty()) {
            return;
        }
        String[] ids = dataIds.toArray(new String[0]);
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_azblob_data WHERE data_id = ANY (?::uuid[])")) {
            ps.setArray(1, c.createArrayOf("uuid", ids));
            ps.executeUpdate();
        }
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_azblob_data_owner WHERE data_id = ANY (?::uuid[])")) {
            ps.setArray(1, c.createArrayOf("uuid", ids));
            ps.executeUpdate();
        }
    }

    /** Drops every data blob owned by a blob's snapshot (all kinds) except {@code keep}. */
    static void dropOwned(Connection c, String account, String container, String name, String snapshot, Collection<String> keep)
            throws SQLException {
        List<String> ids = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT data_id::text FROM warp_azblob_data_owner WHERE account=? "
                + "AND container=? AND name=? AND snapshot=?")) {
            ps.setString(1, account);
            ps.setString(2, container);
            ps.setString(3, name);
            ps.setString(4, snapshot == null ? "" : snapshot);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String id = rs.getString(1);
                    if (keep == null || !keep.contains(id)) {
                        ids.add(id);
                    }
                }
            }
        }
        dropData(c, ids);
    }

    /** Drops every data blob owned by any snapshot of a blob name. */
    static void dropAllOwned(Connection c, String account, String container, String name) throws SQLException {
        exec(c, "DELETE FROM warp_azblob_data WHERE data_id IN (SELECT data_id FROM warp_azblob_data_owner "
                + "WHERE account=? AND container=? AND name=?)", account, container, name);
        exec(c, "DELETE FROM warp_azblob_data_owner WHERE account=? AND container=? AND name=?", account, container, name);
    }

    static void adopt(Connection c, String dataId, String kind, String snapshot) throws SQLException {
        exec(c, "UPDATE warp_azblob_data_owner SET kind=?, snapshot=? WHERE data_id=?::uuid", kind,
                snapshot == null ? "" : snapshot, dataId);
    }

    /** Copies the data blob {@code from} into a new data blob owned by the target; returns the new id. */
    static String duplicateData(Connection c, String from, String account, String container, String name, String snapshot,
            String kind, String blockId, long size) throws SQLException {
        String id = UUID.randomUUID().toString();
        exec(c, "INSERT INTO warp_azblob_data_owner (data_id, account, container, name, snapshot, kind, block_id, size) "
                + "VALUES (?::uuid,?,?,?,?,?,?,?)", id, account, container, name, snapshot == null ? "" : snapshot, kind,
                blockId, size);
        exec(c, "INSERT INTO warp_azblob_data (data_id, seq, data) SELECT ?::uuid, seq, data FROM warp_azblob_data "
                + "WHERE data_id=?::uuid", id, from);
        return id;
    }

    /** Duplicates every segment of {@code src} for a new owner; returns the new segment list. */
    static List<Seg> duplicateSegments(Connection c, Blob src, String account, String container, String name,
            String snapshot) throws SQLException {
        List<Seg> out = new ArrayList<>();
        Map<String, String> remap = new LinkedHashMap<>();
        for (Seg s : src.segments) {
            String nd = remap.get(s.d);
            if (nd == null) {
                long dsz = 0;
                try (PreparedStatement ps = c.prepareStatement("SELECT size FROM warp_azblob_data_owner WHERE data_id=?::uuid")) {
                    ps.setString(1, s.d);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            dsz = rs.getLong(1);
                        }
                    }
                }
                nd = duplicateData(c, s.d, account, container, name, snapshot, "C", s.id, dsz);
                remap.put(s.d, nd);
            }
            Seg n = new Seg();
            n.id = s.id;
            n.d = nd;
            n.n = s.n;
            n.o = s.o;
            n.s = s.s;
            out.add(n);
        }
        return out;
    }

    static long dataSize(Connection c, String dataId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT size FROM warp_azblob_data_owner WHERE data_id=?::uuid")) {
            ps.setString(1, dataId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    /**
     * Streams bytes [start, end] (inclusive) of a blob to {@code out}. Page blobs are sparse: holes read as zeros.
     * Memory is bounded by one chunk slice; every slice is one short borrow of a connection.
     */
    void stream(Blob b, long start, long end, OutputStream out) throws IOException {
        if (b.size == 0 || end < start) {
            return;
        }
        String host = ownerHost(b.account, b.container, b.name);
        if ("PageBlob".equals(b.type)) {
            long pos = start;
            List<Seg> segs = new ArrayList<>(b.segments);
            segs.sort(java.util.Comparator.comparingLong(s -> s.o));
            for (Seg s : segs) {
                if (s.o + s.n <= pos) {
                    continue;
                }
                if (s.o > end) {
                    break;
                }
                if (s.o > pos) {
                    writeZeros(out, Math.min(s.o - 1, end) - pos + 1);
                    pos = s.o;
                }
                long from = pos;
                long to = Math.min(end, s.o + s.n - 1);
                streamData(host, s.d, s.s + (from - s.o), s.s + (to - s.o), out);
                pos = to + 1;
                if (pos > end) {
                    break;
                }
            }
            if (pos <= end) {
                writeZeros(out, end - pos + 1);
            }
            return;
        }
        List<ChunkMath.Segment> segs = new ArrayList<>();
        for (Seg s : b.segments) {
            segs.add(new ChunkMath.Segment(s.d, s.n, CHUNK));
        }
        for (ChunkMath.Slice sl : ChunkMath.slices(segs, start, end)) {
            out.write(readSlice(host, sl.blobId(), sl.seq(), sl.offset(), sl.length()));
        }
    }

    private void streamData(String host, String dataId, long from, long to, OutputStream out) throws IOException {
        for (ChunkMath.Slice sl : ChunkMath.slices(List.of(new ChunkMath.Segment(dataId, to + 1, CHUNK)), from, to)) {
            out.write(readSlice(host, sl.blobId(), sl.seq(), sl.offset(), sl.length()));
        }
    }

    private static void writeZeros(OutputStream out, long n) throws IOException {
        byte[] z = new byte[(int) Math.min(n, 65536)];
        while (n > 0) {
            int k = (int) Math.min(n, z.length);
            out.write(z, 0, k);
            n -= k;
        }
    }

    private byte[] readSlice(String host, String dataId, int seq, int offset, int length) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT substring(data FROM ? FOR ?) FROM warp_azblob_data "
                    + "WHERE data_id=?::uuid AND seq=?")) {
                ps.setInt(1, offset + 1);
                ps.setInt(2, length);
                ps.setString(3, dataId);
                ps.setInt(4, seq);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new AzureException(500, "InternalError", "blob data chunk is missing");
                    }
                    return rs.getBytes(1);
                }
            }
        });
    }

    byte[] readAll(Blob b, long start, long end) {
        java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
        try {
            stream(b, start, end, o);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return o.toByteArray();
    }

    // ------------------------------------------------------------------------------------------ listing

    /** A cursor over one shard's blob rows of a container in (name, snapshot) order, starting at (fromName, fromSnap) inclusive. */
    final class ShardCursor {
        private final String host;
        private final String account;
        private final String container;
        private final String prefix;
        private final boolean snapshots;
        private String fromName;
        private String fromSnap;
        private String lastName;
        private String lastSnap;
        private boolean afterLast;
        private List<Blob> buf = List.of();
        private int i;
        private boolean done;

        ShardCursor(String host, String account, String container, String prefix, boolean snapshots, String fromName,
                String fromSnap) {
            this.host = host;
            this.account = account;
            this.container = container;
            this.prefix = prefix == null ? "" : prefix;
            this.snapshots = snapshots;
            this.fromName = fromName;
            this.fromSnap = fromSnap == null ? "" : fromSnap;
        }

        /** Continue at the first row with name >= {@code name}. */
        void seek(String name) {
            fromName = name;
            fromSnap = "";
            afterLast = false;
            buf = List.of();
            i = 0;
            done = false;
        }

        Blob peek() {
            while (i >= buf.size()) {
                if (done) {
                    return null;
                }
                fill();
            }
            return buf.get(i);
        }

        void next() {
            i++;
        }

        private void fill() {
            final boolean after = afterLast;
            List<Blob> rows = shards.conn(host, c -> {
                StringBuilder sql = new StringBuilder("SELECT " + BLOB_COLS + " FROM warp_azblob_blobs WHERE account=? "
                        + "AND container=?");
                List<Object> args = new ArrayList<>(List.of(account, container));
                if (!prefix.isEmpty()) {
                    sql.append(" AND name >= ? AND name < ?");
                    args.add(prefix);
                    args.add(prefixEnd(prefix));
                }
                if (after) {
                    sql.append(" AND (name, (CASE WHEN snapshot = '' THEN '~' ELSE snapshot END)) > (?, ?)");
                    args.add(lastName);
                    args.add(sk(lastSnap));
                } else if (fromName != null) {
                    sql.append(" AND (name, (CASE WHEN snapshot = '' THEN '~' ELSE snapshot END)) >= (?, ?)");
                    args.add(fromName);
                    args.add(sk(fromSnap));
                }
                if (!snapshots) {
                    sql.append(" AND snapshot = ''");
                }
                sql.append(" ORDER BY name, (CASE WHEN snapshot = '' THEN '~' ELSE snapshot END) LIMIT 500");
                List<Blob> out = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                    for (int k = 0; k < args.size(); k++) {
                        ps.setObject(k + 1, args.get(k));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(readBlob(rs));
                        }
                    }
                }
                return out;
            });
            buf = rows;
            i = 0;
            if (rows.size() < 500) {
                done = true;
            }
            if (!rows.isEmpty()) {
                Blob last = rows.get(rows.size() - 1);
                lastName = last.name;
                lastSnap = last.snapshot;
                afterLast = true;
            }
        }
    }

    /** k-way merge over the shard cursors of a container listing. */
    final class MergedCursor {
        private final List<ShardCursor> cursors = new ArrayList<>();

        MergedCursor(String account, String container, String prefix, boolean snapshots, String afterName, String afterSnap) {
            for (String h : shards.hosts()) {
                cursors.add(new ShardCursor(h, account, container, prefix, snapshots, afterName, afterSnap));
            }
        }

        Blob peek() {
            Blob best = null;
            for (ShardCursor c : cursors) {
                Blob b = c.peek();
                if (b != null && (best == null || cmp(b, best) < 0)) {
                    best = b;
                }
            }
            return best;
        }

        Blob next() {
            ShardCursor bestC = null;
            Blob best = null;
            for (ShardCursor c : cursors) {
                Blob b = c.peek();
                if (b != null && (best == null || cmp(b, best) < 0)) {
                    best = b;
                    bestC = c;
                }
            }
            if (bestC != null) {
                bestC.next();
            }
            return best;
        }

        /** Skips every remaining row whose name starts with {@code prefix}. */
        void skipPrefix(String prefix) {
            String end = prefixEnd(prefix);
            for (ShardCursor c : cursors) {
                Blob b = c.peek();
                if (b != null && b.name.startsWith(prefix)) {
                    c.seek(end);
                }
            }
        }

        private int cmp(Blob a, Blob b) {
            int n = a.name.compareTo(b.name);
            return n != 0 ? n : sk(a.snapshot).compareTo(sk(b.snapshot));
        }
    }

    /** Blobs of the account carrying tags (all shards), optionally limited to a container; evaluated by the caller. */
    List<Blob> taggedBlobs(String account, String container) {
        List<Blob> out = new ArrayList<>();
        for (List<Blob> part : shards.<List<Blob>>onAll(c -> {
            List<Blob> l = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + BLOB_COLS + " FROM warp_azblob_blobs WHERE account=? "
                    + (container == null ? "" : "AND container=? ") + "AND snapshot='' AND tags <> '{}'::jsonb ORDER BY container, name")) {
                ps.setString(1, account);
                if (container != null) {
                    ps.setString(2, container);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        l.add(readBlob(rs));
                    }
                }
            }
            return l;
        })) {
            out.addAll(part);
        }
        out.sort(java.util.Comparator.comparing((Blob b) -> b.container).thenComparing(b -> b.name));
        return out;
    }

    /** Uncommitted (staged) blocks of one blob: (block id, data id, size, kind). */
    record BlockRow(String blockId, String dataId, long size, String kind) {
    }

    static List<BlockRow> blocks(Connection c, String account, String container, String name, String kindsCsv)
            throws SQLException {
        List<BlockRow> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT block_id, data_id::text, size, kind FROM warp_azblob_data_owner "
                + "WHERE account=? AND container=? AND name=? AND snapshot='' AND kind = ANY (string_to_array(?, ',')) "
                + "ORDER BY created_at")) {
            ps.setString(1, account);
            ps.setString(2, container);
            ps.setString(3, name);
            ps.setString(4, kindsCsv);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new BlockRow(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getString(4)));
                }
            }
        }
        return out;
    }

    StoreType type() {
        return StoreType.AZBLOB;
    }

    /** Removes temporary data blobs (interrupted uploads) older than {@code olderThanSeconds} on every shard. */
    long sweepTemporary(long olderThanSeconds) {
        long n = 0;
        for (String h : shards.hosts()) {
            n += shards.tx(h, c -> {
                exec(c, "DELETE FROM warp_azblob_data WHERE data_id IN (SELECT data_id FROM warp_azblob_data_owner "
                        + "WHERE kind='T' AND created_at < now() - make_interval(secs => ?))", olderThanSeconds);
                return (long) exec(c, "DELETE FROM warp_azblob_data_owner WHERE kind='T' AND created_at < now() - "
                        + "make_interval(secs => ?)", olderThanSeconds);
            });
        }
        return n;
    }

    /** Counts for the MCP/admin description: blobs and bytes per container on every shard. */
    Map<String, long[]> containerStats(String account) {
        Map<String, long[]> out = new LinkedHashMap<>();
        for (String h : shards.hosts()) {
            shards.conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT container, count(*), coalesce(sum(size),0) FROM "
                        + "warp_azblob_blobs WHERE account=? AND snapshot='' GROUP BY container")) {
                    ps.setString(1, account);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            long[] a = out.computeIfAbsent(rs.getString(1), k -> new long[2]);
                            a[0] += rs.getLong(2);
                            a[1] += rs.getLong(3);
                        }
                    }
                }
                return null;
            });
        }
        return out;
    }
}
