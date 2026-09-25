package com.sayonora.wire.s3wire;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ShardingStrategy;
import com.sayonora.wire.core.StoreBootstrap;
import com.sayonora.wire.core.StoreType;
import com.sayonora.wire.s3wire.ObjectStore.Attrs;
import com.sayonora.wire.s3wire.S3Model.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres mode: objects live IN the Postgres backends of a backend set that enabled the {@code s3} store
 * ({@link StoreType#S3}), sharded by {@code hash(bucket + "/" + key)} over those backends in declaration
 * order (the same {@link ShardingStrategy#hash} and host order dynamowire/sqswire/mongowire use).
 *
 * <h3>Storage model (per hosting backend, table names {@code warp_s3_*}, see ddl/postgres/s3wire_store.sql)</h3>
 * <ul>
 *   <li>{@code objects (bucket, key)} -- the CURRENT version of every key (the fast path: a plain GET/PUT/list
 *       touches only this table): the committed name -> content pointer plus attributes, checksums, tags,
 *       ACL, SSE... (json {@code extra}). A plain object points at one <em>blob</em>; a multipart object lists
 *       its parts' blobs in {@code segments}, so CompleteMultipartUpload assembles by reference.</li>
 *   <li>{@code versions (bucket, key, version_id)} -- the NON-current history of a key in a versioned bucket
 *       (older versions and delete markers), in insertion order ({@code seq}). The latest version of a key is
 *       its {@code objects} row, or, when the key was deleted, the newest {@code versions} row (a delete marker).
 *       An overwrite/delete in a versioned bucket MOVES the current row into {@code versions} in the same
 *       transaction; deleting the current version by id promotes the newest older version back. A key and all of
 *       its versions always live on the same shard, so version order is a per-shard {@code seq}.</li>
 *   <li>{@code blobs (object_id, state)} -- lifecycle of a chunk set: {@code uploading} (invisible) ->
 *       {@code committed} -> {@code garbage}. A half-written upload is never referenced by an object row.</li>
 *   <li>{@code chunks (object_id, seq, data bytea)} -- fixed-size pieces (default 4 MiB, per-object).</li>
 *   <li>{@code multipart_uploads}, {@code parts} -- in-flight multipart state; all of an upload lives on the
 *       shard owning (bucket, key), and the upload id encodes that host.</li>
 *   <li>{@code buckets}, {@code bucket_config}, {@code annotations} -- the bucket catalog and its subresource
 *       documents (cors, policy, lifecycle... stored verbatim), written on the FIRST host of the set only;
 *       object annotations live on the shard owning the key.</li>
 * </ul>
 *
 * <h3>Connection discipline</h3>
 * Every chunk insert / chunk read is its own short statement on a connection borrowed from the backend pool
 * and returned immediately; nothing holds a pooled connection while waiting for the client.
 *
 * <h3>Isolation</h3>
 * An overwrite or delete atomically re-points/removes the object row in one transaction and only marks the old
 * blob {@code garbage}; its chunks are removed by the background collector after
 * {@code WARP_S3WIRE_GC_GRACE_SECONDS}. Concurrent writes to one key serialize on the row lock (last commit
 * wins). There is no cross-shard atomicity (DeleteBucket, DeleteObjects across shards, cross-shard copy are
 * best-effort sequences).
 */
final class PostgresObjectStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresObjectStore.class);
    private static final Gson GSON = new Gson();
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int MIN_PART = 5 * 1024 * 1024;
    private static final String DEFAULT_TYPE = "binary/octet-stream";
    static final String DEFAULT_CRC = "CRC64NVME";

    private final BackendRegistry registry;
    private final S3StoreOptions opt;
    private record Cached(BucketInfo info, long untilNanos) {
    }

    private final Map<String, Cached> bucketCache = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService gc;

    PostgresObjectStore(BackendRegistry registry, S3StoreOptions options) {
        this.registry = registry;
        this.opt = options;
        if (options.gcIntervalSeconds() > 0) {
            gc = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "s3wire-gc");
                t.setDaemon(true);
                return t;
            });
            gc.scheduleWithFixedDelay(this::gcAll, options.gcIntervalSeconds(), options.gcIntervalSeconds(),
                    TimeUnit.SECONDS);
        }
    }

    /** Hosts of the s3 store right now (hot-reloadable), in hash order. */
    List<String> hosts() {
        return registry.storeHosts(StoreType.S3);
    }

    boolean available() {
        return !hosts().isEmpty();
    }

    String label() {
        return "s3pg:" + String.join(",", hosts());
    }

    void close() {
        ScheduledExecutorService g = gc;
        if (g != null) {
            g.shutdownNow();
        }
    }

    // ---- shard / connection plumbing -----------------------------------------------------------------

    private List<String> requireHosts() {
        List<String> h = hosts();
        if (h.isEmpty()) {
            throw new S3WireException(503, "ServiceUnavailable",
                    "No Postgres backend of this set has the s3 store enabled");
        }
        return h;
    }

    static String ownerOf(List<String> hosts, String bucket, String key) {
        return hosts.size() == 1 ? hosts.get(0) : ShardingStrategy.hash(hosts).resolve(S3Keys.shardKey(bucket, key));
    }

    private String owner(String bucket, String key) {
        return ownerOf(requireHosts(), bucket, key);
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("s3wire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, StoreType.S3);
        return t;
    }

    @FunctionalInterface
    private interface SqlFn<T> {
        T apply(Connection c) throws SQLException;
    }

    /** One short auto-commit unit of work on a borrowed connection, returned right after. */
    private <T> T withConn(String host, SqlFn<T> fn) {
        try (Connection c = target(host).open()) {
            return fn.apply(c);
        } catch (SQLException e) {
            throw storageError(e);
        }
    }

    /** One transaction. */
    private <T> T withTx(String host, SqlFn<T> fn) {
        try (Connection c = target(host).openManualCommit()) {
            try {
                T out = fn.apply(c);
                c.commit();
                return out;
            } catch (SQLException | RuntimeException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // connection is being discarded anyway
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
            throw storageError(e);
        }
    }

    private static S3WireException storageError(SQLException e) {
        log.error("s3wire: Postgres store error", e);
        return new S3WireException(503, "ServiceUnavailable", "The Postgres object store failed: " + e.getMessage());
    }

    static String newVersionId() {
        byte[] b = new byte[24];
        RANDOM.nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    static String ext(String dbVersion) {
        return dbVersion == null ? "null" : dbVersion;
    }

    /** @throws S3WireException 400 InvalidArgument for a version id that is not one of ours */
    static void checkVersionId(String v) {
        if (v == null || v.equals("null") || v.matches("[A-Za-z0-9_-]{32}")) {
            return;
        }
        throw new S3WireException(400, "InvalidArgument", "Invalid version id specified");
    }

    // ---- buckets ---------------------------------------------------------------------------------

    private String home() {
        return requireHosts().get(0);
    }

    /** @return the bucket row (cached for {@code WARP_S3WIRE_BUCKET_CACHE_MILLIS}), or null when it does not exist */
    BucketInfo bucketInfo(String bucket) {
        long now = System.nanoTime();
        Cached c = bucketCache.get(bucket);
        if (c != null && c.untilNanos() - now > 0) {
            return c.info();
        }
        BucketInfo info = withConn(home(), conn -> {
            try (PreparedStatement ps = conn.prepareStatement("SELECT b.created_at, b.region, b.versioning, b.object_lock, "
                    + "b.ownership, e.body FROM warp_s3_buckets b LEFT JOIN warp_s3_bucket_config e "
                    + "ON e.bucket = b.name AND e.kind = 'encryption' WHERE b.name = ?")) {
                ps.setString(1, bucket);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    String[] enc = S3Cfg.encryptionDefaults(rs.getString(6));
                    return new BucketInfo(bucket, rs.getTimestamp(1).toInstant(), rs.getString(2), rs.getString(3),
                            rs.getBoolean(4), rs.getString(5), enc[0], enc[1]);
                }
            }
        });
        if (info != null && opt.bucketCacheMillis() > 0) {
            bucketCache.put(bucket, new Cached(info, now + opt.bucketCacheMillis() * 1_000_000L));
        }
        return info;
    }

    boolean bucketExists(String bucket) {
        return bucketInfo(bucket) != null;
    }

    BucketInfo requireBucket(String bucket) {
        BucketInfo b = bucketInfo(bucket);
        if (b == null) {
            throw new S3WireException(404, "NoSuchBucket", "The specified bucket does not exist", null,
                    Map.of("BucketName", bucket));
        }
        return b;
    }

    /** @param aclXml canned/explicit ACL document to store, or null */
    void createBucket(String bucket, String region, boolean objectLock, String ownership, String aclXml) {
        if (!S3WireServer.BUCKET_NAME.matcher(bucket).matches() || bucket.contains("..") || bucket.contains(".-")
                || bucket.contains("-.") || bucket.matches("\\d+\\.\\d+\\.\\d+\\.\\d+") || bucket.startsWith("xn--")
                || bucket.startsWith("sthree-") || bucket.endsWith("-s3alias") || bucket.endsWith("--ol-s3")) {
            throw new S3WireException(400, "InvalidBucketName", "The specified bucket is not valid.", null,
                    Map.of("BucketName", bucket));
        }
        int n = withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_s3_buckets (name, region, versioning, "
                    + "object_lock, ownership) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, bucket);
                ps.setString(2, region);
                ps.setString(3, objectLock ? "Enabled" : null);
                ps.setBoolean(4, objectLock);
                ps.setString(5, ownership);
                return ps.executeUpdate();
            }
        });
        if (n == 0) {
            throw new S3WireException(409, "BucketAlreadyOwnedByYou",
                    "Your previous request to create the named bucket succeeded and you already own it.", null,
                    Map.of("BucketName", bucket));
        }
        if (aclXml != null) {
            putConfig(bucket, "acl", aclXml);
        }
    }

    void deleteBucket(String bucket) {
        List<String> hosts = requireHosts();
        bucketCache.remove(bucket);
        requireBucket(bucket);
        bucketCache.remove(bucket);
        for (String h : hosts) {
            boolean nonEmpty = withConn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT 1 WHERE EXISTS (SELECT 1 FROM warp_s3_objects "
                        + "WHERE bucket = ?) OR EXISTS (SELECT 1 FROM warp_s3_versions WHERE bucket = ?)")) {
                    ps.setString(1, bucket);
                    ps.setString(2, bucket);
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next();
                    }
                }
            });
            if (nonEmpty) {
                throw new S3WireException(409, "BucketNotEmpty", "The bucket you tried to delete is not empty", null,
                        Map.of("BucketName", bucket));
            }
        }
        withTx(hosts.get(0), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_bucket_config WHERE bucket = ?")) {
                ps.setString(1, bucket);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_buckets WHERE name = ?")) {
                ps.setString(1, bucket);
                return ps.executeUpdate();
            }
        });
        bucketCache.remove(bucket);
        for (String h : hosts) { // in-flight multipart uploads of the bucket die with it
            withTx(h, c -> {
                List<UUID> ids = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM warp_s3_multipart_uploads WHERE bucket = ? RETURNING upload_id")) {
                    ps.setString(1, bucket);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getObject(1, UUID.class));
                        }
                    }
                }
                for (UUID id : ids) {
                    discardParts(c, id);
                }
                return null;
            });
        }
    }

    List<BucketInfo> listBuckets() {
        return withConn(home(), c -> {
            List<BucketInfo> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT name, created_at, region FROM warp_s3_buckets ORDER BY name");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new BucketInfo(rs.getString(1), rs.getTimestamp(2).toInstant(), rs.getString(3), null, false,
                            null, "AES256", null));
                }
            }
            return out;
        });
    }

    /** Versioning status change (Enabled / Suspended). A bucket that has been versioned can never go back to null. */
    void setVersioning(String bucket, String status) {
        BucketInfo b = requireBucket(bucket);
        if (b.objectLock() && "Suspended".equals(status)) {
            throw new S3WireException(409, "InvalidBucketState",
                    "An Object Lock configuration is present on this bucket, so the versioning state cannot be changed.");
        }
        withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_s3_buckets SET versioning = ? WHERE name = ?")) {
                ps.setString(1, status);
                ps.setString(2, bucket);
                return ps.executeUpdate();
            }
        });
        bucketCache.remove(bucket);
    }

    void enableObjectLock(String bucket, String ownershipUnused) {
        requireBucket(bucket);
        withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_s3_buckets SET object_lock = true, "
                    + "versioning = 'Enabled' WHERE name = ?")) {
                ps.setString(1, bucket);
                return ps.executeUpdate();
            }
        });
        bucketCache.remove(bucket);
    }

    void setOwnership(String bucket, String ownership) {
        requireBucket(bucket);
        withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_s3_buckets SET ownership = ? WHERE name = ?")) {
                ps.setString(1, ownership);
                ps.setString(2, bucket);
                return ps.executeUpdate();
            }
        });
        bucketCache.remove(bucket);
    }

    // ---- bucket subresource documents ------------------------------------------------------------

    /** @return the stored document for {@code kind}, or null when none */
    String config(String bucket, String kind) {
        requireBucket(bucket);
        return withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT body FROM warp_s3_bucket_config WHERE bucket = ? AND kind = ?")) {
                ps.setString(1, bucket);
                ps.setString(2, kind);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        });
    }

    /** CORS lookup for browsers: no bucket-exists check, null when the bucket or its CORS config is missing. */
    String corsConfig(String bucket) {
        return withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT body FROM warp_s3_bucket_config WHERE bucket = ? AND kind = 'cors'")) {
                ps.setString(1, bucket);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        });
    }

    void putConfig(String bucket, String kind, String body) {
        requireBucket(bucket);
        withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_s3_bucket_config (bucket, kind, body) "
                    + "VALUES (?, ?, ?) ON CONFLICT (bucket, kind) DO UPDATE SET body = EXCLUDED.body, updated_at = now()")) {
                ps.setString(1, bucket);
                ps.setString(2, kind);
                ps.setString(3, body);
                return ps.executeUpdate();
            }
        });
        bucketCache.remove(bucket);
    }

    /** @return true when a document was removed */
    boolean deleteConfig(String bucket, String kind) {
        requireBucket(bucket);
        int n = withConn(home(), c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "DELETE FROM warp_s3_bucket_config WHERE bucket = ? AND kind = ?")) {
                ps.setString(1, bucket);
                ps.setString(2, kind);
                return ps.executeUpdate();
            }
        });
        bucketCache.remove(bucket);
        return n > 0;
    }

    /** Ids and bodies of an indexed configuration family ({@code analytics}, {@code inventory}, ...). */
    Map<String, String> configFamily(String bucket, String family) {
        requireBucket(bucket);
        return withConn(home(), c -> {
            Map<String, String> out = new LinkedHashMap<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT kind, body FROM warp_s3_bucket_config WHERE bucket = ? AND kind LIKE ? ORDER BY kind")) {
                ps.setString(1, bucket);
                ps.setString(2, family + ":%");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.put(rs.getString(1).substring(family.length() + 1), rs.getString(2));
                    }
                }
            }
            return out;
        });
    }

    // ---- object rows -----------------------------------------------------------------------------

    /** A stored object version (row of objects or versions). {@code dbVersion}: null = the null version (objects only). */
    private record Row(String host, String bucket, String key, String dbVersion, boolean deleteMarker, boolean current,
            UUID objectId, long size, String eTag, int chunkSize, List<ChunkMath.Segment> segments,
            List<PartMeta> parts, Attrs attrs, Instant lastModified, Map<String, String> checksums,
            String checksumType, JsonObject extra) {
        Meta meta(boolean latest) {
            return new Meta(bucket, key, ext(dbVersion), deleteMarker, latest, size, eTag, lastModified, attrs, checksums,
                    checksumType, extra, parts);
        }
    }

    private static final String COLS = "object_id, size, etag, chunk_size, segments::text, content_type, "
            + "cache_control, content_disposition, content_encoding, content_language, expires, "
            + "user_metadata::text, last_modified, checksums::text, checksum_type, extra::text";
    /** Same columns, unqualified, for INSERT ... SELECT between objects and versions. */
    private static final String MOVE_COLS = "object_id, size, etag, chunk_size, segments, content_type, cache_control, "
            + "content_disposition, content_encoding, content_language, expires, user_metadata, last_modified, "
            + "checksums, checksum_type, extra";
    private static final java.lang.reflect.Type STR_MAP = new TypeToken<LinkedHashMap<String, String>>() { }.getType();

    private static JsonObject obj(String json) {
        return json == null ? new JsonObject() : GSON.fromJson(json, JsonObject.class);
    }

    /** Reads {@link #COLS} (columns 1-16) plus version_id (17) and delete_marker (18). */
    private static Row readRow(String host, String bucket, String key, ResultSet rs, boolean current) throws SQLException {
        UUID id = rs.getObject(1, UUID.class);
        long size = rs.getLong(2);
        int chunkSize = rs.getInt(4);
        String segJson = rs.getString(5);
        List<ChunkMath.Segment> segments = new ArrayList<>();
        List<PartMeta> parts = new ArrayList<>();
        if (segJson == null) {
            segments.add(new ChunkMath.Segment(id.toString(), size, chunkSize));
        } else {
            int i = 0;
            for (JsonElement e : GSON.fromJson(segJson, JsonArray.class)) {
                JsonObject o = e.getAsJsonObject();
                segments.add(new ChunkMath.Segment(o.get("id").getAsString(), o.get("size").getAsLong(),
                        o.get("cs").getAsInt()));
                Map<String, String> ck = o.has("ck") ? GSON.fromJson(o.get("ck"), STR_MAP) : Map.of();
                parts.add(new PartMeta(o.has("n") ? o.get("n").getAsInt() : ++i, o.get("size").getAsLong(), ck));
            }
        }
        Map<String, String> meta = rs.getString(12) == null ? Map.of() : GSON.fromJson(rs.getString(12), STR_MAP);
        Attrs attrs = new Attrs(rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10),
                rs.getString(11), meta);
        Map<String, String> checksums = rs.getString(14) == null ? Map.of() : GSON.fromJson(rs.getString(14), STR_MAP);
        return new Row(host, bucket, key, rs.getString(17), rs.getBoolean(18), current, id, size, rs.getString(3),
                chunkSize, segments, parts, attrs, rs.getTimestamp(13).toInstant(), checksums, rs.getString(15),
                obj(rs.getString(16)));
    }

    private Row currentOn(String host, String bucket, String key) {
        return withConn(host, c -> currentIn(c, host, bucket, key, false));
    }

    private static Row currentIn(Connection c, String host, String bucket, String key, boolean forUpdate)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + COLS + ", version_id, false FROM warp_s3_objects "
                + "WHERE bucket = ? AND key = ?" + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readRow(host, bucket, key, rs, true) : null;
            }
        }
    }

    /**
     * The commit path of an unversioned bucket only needs enough of the current row to garbage-collect it
     * (blob ids, annotation flag): a narrow locking select instead of every column.
     */
    private static Row currentLite(Connection c, String bucket, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT object_id, segments::text, extra::text FROM warp_s3_objects "
                + "WHERE bucket = ? AND key = ? FOR UPDATE")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                UUID id = rs.getObject(1, UUID.class);
                List<ChunkMath.Segment> segs = new ArrayList<>();
                for (UUID blob : blobIdsOf(id, rs.getString(2))) {
                    segs.add(new ChunkMath.Segment(blob.toString(), 0, 0));
                }
                return new Row("", bucket, key, null, false, true, id, 0, "", 0, segs, List.of(), new Attrs(null, null,
                        null, null, null, null, Map.of()), Instant.EPOCH, Map.of(), null, obj(rs.getString(3)));
            }
        }
    }

    /** A history row by version id ("null" = the null version). */
    private static Row historyIn(Connection c, String host, String bucket, String key, String version,
            boolean forUpdate) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + COLS + ", version_id, delete_marker "
                + "FROM warp_s3_versions WHERE bucket = ? AND key = ? AND version_id = ?" + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            ps.setString(3, version);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readRow(host, bucket, key, rs, false) : null;
            }
        }
    }

    /** Newest history row of a key (the latest version when the key has no current row). */
    private static Row newestHistoryIn(Connection c, String host, String bucket, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + COLS + ", version_id, delete_marker "
                + "FROM warp_s3_versions WHERE bucket = ? AND key = ? ORDER BY seq DESC LIMIT 1")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readRow(host, bucket, key, rs, false) : null;
            }
        }
    }

    /**
     * Finds a version. {@code version == null}: the latest (current row, or a delete marker when the key was
     * deleted in a versioned bucket). Otherwise that exact version, "null" being the null version.
     */
    private Row locate(String bucket, String key, String version, boolean versioned) {
        List<String> hosts = requireHosts();
        String owner = ownerOf(hosts, bucket, key);
        Row row = locateOn(owner, bucket, key, version, versioned);
        if (row == null && opt.probeOtherShards() && hosts.size() > 1) {
            for (String h : hosts) {
                if (!h.equals(owner) && (row = locateOn(h, bucket, key, version, versioned)) != null) {
                    break;
                }
            }
        }
        return row;
    }

    private Row locateOn(String host, String bucket, String key, String version, boolean versioned) {
        return withConn(host, c -> {
            if (version == null) {
                Row cur = currentIn(c, host, bucket, key, false);
                if (cur != null || !versioned) {
                    return cur;
                }
                return newestHistoryIn(c, host, bucket, key);
            }
            Row cur = currentIn(c, host, bucket, key, false);
            if (cur != null && cur.dbVersion() == null ? version.equals("null") : cur != null && version.equals(cur.dbVersion())) {
                return cur;
            }
            return historyIn(c, host, bucket, key, version, false);
        });
    }

    /** The row the request addresses, or the proper 404/405 (NoSuchKey / NoSuchVersion / delete-marker) error. */
    private Row requireRow(BucketInfo b, String key, String version, boolean forRead) {
        S3Keys.validate(key);
        checkVersionId(version);
        Row row = locate(b.name(), key, version, b.versioned());
        if (row == null) {
            if (version != null) {
                throw new S3WireException(404, "NoSuchVersion", "The specified version does not exist.", null,
                        Map.of("Key", key, "VersionId", version));
            }
            throw new S3WireException(404, "NoSuchKey", "The specified key does not exist.", null, Map.of("Key", key));
        }
        if (row.deleteMarker() && forRead) {
            Map<String, String> h = new LinkedHashMap<>();
            h.put("x-amz-delete-marker", "true");
            h.put("x-amz-version-id", ext(row.dbVersion()));
            if (version == null) {
                throw new S3WireException(404, "NoSuchKey", "The specified key does not exist.", h, Map.of("Key", key));
            }
            h.put("Allow", "DELETE");
            throw new S3WireException(405, "MethodNotAllowed",
                    "The specified method is not allowed against this resource.", h,
                    Map.of("Method", "GET", "ResourceType", "DeleteMarker"));
        }
        return row;
    }

    private static void checkConditions(ObjectStore.Conditions c, Row row) {
        int status = S3Http.evaluate(c, row.eTag(), row.lastModified());
        if (status == 304) {
            throw new S3WireException(304, "NotModified", "Not Modified");
        }
        if (status == 412) {
            throw new S3WireException(412, "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold", null,
                    Map.of("Condition", "If-Match"));
        }
    }

    private static Attrs withDefaultType(Attrs a) {
        return a.contentType() != null ? a : new Attrs(DEFAULT_TYPE, a.cacheControl(), a.contentDisposition(),
                a.contentEncoding(), a.contentLanguage(), a.expires(), a.metadata());
    }

    Meta head(String bucket, GetIn r) {
        BucketInfo b = requireBucketFor(bucket);
        Row row = requireRow(b, r.key(), r.versionId(), true);
        checkConditions(r.conditions(), row);
        return row.meta(row.current());
    }

    private BucketInfo requireBucketFor(String bucket) {
        return requireBucket(bucket);
    }

    Read get(GetIn r) {
        BucketInfo b = requireBucket(r.bucket());
        Row row = requireRow(b, r.key(), r.versionId(), true);
        checkConditions(r.conditions(), row);
        long start = 0;
        long end = row.size() - 1;
        String contentRange = null;
        long length = row.size();
        if (r.partNumber() > 0) {
            if (row.parts().isEmpty() || row.eTag().indexOf('-') < 0) {
                if (r.partNumber() != 1) {
                    throw new S3WireException(416, "InvalidPartNumber", "The requested partnumber is not satisfiable");
                }
            } else {
                if (r.partNumber() > row.parts().size()) {
                    throw new S3WireException(416, "InvalidPartNumber", "The requested partnumber is not satisfiable");
                }
                for (int i = 0; i < r.partNumber() - 1; i++) {
                    start += row.parts().get(i).size();
                }
                end = start + row.parts().get(r.partNumber() - 1).size() - 1;
                length = row.parts().get(r.partNumber() - 1).size();
                contentRange = "bytes " + start + "-" + end + "/" + row.size();
            }
        } else {
            S3Http.ByteRange br = S3Http.parseRange(r.range(), row.size());
            if (br != null) {
                start = br.start();
                end = br.end();
                length = row.size() == 0 ? 0 : end - start + 1;
                contentRange = "bytes " + start + "-" + end + "/" + row.size();
            }
        }
        final long s = start;
        final long e = end;
        final long len = length;
        final String cr = contentRange;
        final Meta meta = row.meta(row.current());
        return new Read() {
            @Override
            public Meta meta() {
                return meta;
            }

            @Override
            public long contentLength() {
                return len;
            }

            @Override
            public String contentRange() {
                return cr;
            }

            @Override
            public void transferTo(OutputStream out) throws IOException {
                if (len > 0) {
                    new ChunkInputStream(row, s, e).transferTo(out);
                }
            }

            @Override
            public void close() {
                // nothing is held between chunks
            }
        };
    }

    /** Streams an object range chunk by chunk; a connection is borrowed for one chunk read only. */
    private final class ChunkInputStream extends InputStream {
        private final String host;
        private final List<ChunkMath.Slice> slices;
        private int next;
        private byte[] cur = new byte[0];
        private int pos;

        ChunkInputStream(Row row, long start, long end) {
            this.host = row.host();
            this.slices = ChunkMath.slices(row.segments(), start, end);
        }

        private boolean fill() throws IOException {
            while (pos >= cur.length) {
                if (next >= slices.size()) {
                    return false;
                }
                ChunkMath.Slice s = slices.get(next++);
                try {
                    cur = withConn(host, c -> {
                        try (PreparedStatement ps = c.prepareStatement("SELECT substring(data FROM ? FOR ?) "
                                + "FROM warp_s3_chunks WHERE object_id = ? AND seq = ?")) {
                            ps.setInt(1, s.offset() + 1);
                            ps.setInt(2, s.length());
                            ps.setObject(3, UUID.fromString(s.blobId()));
                            ps.setInt(4, s.seq());
                            try (ResultSet rs = ps.executeQuery()) {
                                if (!rs.next()) {
                                    throw new SQLException("chunk " + s.seq() + " of " + s.blobId() + " is missing");
                                }
                                return rs.getBytes(1);
                            }
                        }
                    });
                } catch (S3WireException e) {
                    throw new IOException(e.getMessage(), e);
                }
                if (cur.length != s.length()) {
                    throw new IOException("chunk " + s.seq() + " of " + s.blobId() + " is truncated");
                }
                pos = 0;
            }
            return true;
        }

        @Override
        public int read() throws IOException {
            if (!fill()) {
                return -1;
            }
            return cur[pos++] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (!fill()) {
                return -1;
            }
            int n = Math.min(len, cur.length - pos);
            System.arraycopy(cur, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public long transferTo(OutputStream out) throws IOException {
            long total = 0;
            while (fill()) {
                out.write(cur, pos, cur.length - pos);
                total += cur.length - pos;
                pos = cur.length;
            }
            return total;
        }
    }

    // ---- writing blobs -----------------------------------------------------------------------------

    /**
     * A chunk set written (or, when {@code pending != null}, fully buffered but not yet stored: the small
     * object fast path stores it inside the commit transaction to save round trips).
     */
    private record Blob(UUID id, long size, byte[] md5, int chunkSize, byte[] pending, Map<String, String> checksums) {
    }

    private static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int readFully(InputStream in, byte[] buf, int len) throws IOException {
        int got = 0;
        while (got < len) {
            int n = in.read(buf, got, len - got);
            if (n < 0) {
                break;
            }
            got += n;
        }
        return got;
    }

    /** Streams {@code length} bytes from {@code in} into chunks of {@code host}; never buffers more than one chunk. */
    private Blob writeBlob(String host, InputStream in, long length, Set<String> algos) throws IOException {
        int cs = opt.chunkBytes();
        MessageDigest md = md5();
        Checksums.Acc acc = new Checksums.Acc(algos);
        UUID id = UUID.randomUUID();
        int firstLen = (int) Math.min(cs, length);
        byte[] buf = new byte[firstLen];
        int got = readFully(in, buf, firstLen);
        if (got < firstLen) {
            throw incomplete();
        }
        md.update(buf, 0, got);
        acc.update(buf, 0, got);
        if (length <= cs) {
            requireEnd(in);
            return new Blob(id, length, md.digest(), cs, buf, acc.finish());
        }
        withConn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_s3_blobs (object_id, state, size) VALUES (?, 'uploading', 0)")) {
                ps.setObject(1, id);
                return ps.executeUpdate();
            }
        });
        try {
            long written = 0;
            int seq = 0;
            while (true) {
                putChunk(host, id, seq++, buf, got);
                written += got;
                if (written >= length) {
                    break;
                }
                got = readFully(in, buf, (int) Math.min(cs, length - written));
                if (got < Math.min(cs, length - written)) {
                    throw incomplete();
                }
                md.update(buf, 0, got);
                acc.update(buf, 0, got);
            }
            requireEnd(in);
            return new Blob(id, length, md.digest(), cs, null, acc.finish());
        } catch (IOException | RuntimeException e) {
            discardBlob(host, id);
            throw e;
        }
    }

    private static S3WireException incomplete() {
        return new S3WireException(400, "IncompleteBody",
                "You did not provide the number of bytes specified by the Content-Length HTTP header");
    }

    private static void requireEnd(InputStream in) throws IOException {
        if (in.read() != -1) {
            throw new S3WireException(400, "IncompleteBody", "The request body is longer than the declared length");
        }
    }

    /** One chunk = one short statement that also refreshes the blob's idle clock; it only inserts while the blob is still 'uploading'. */
    private void putChunk(String host, UUID blob, int seq, byte[] buf, int len) {
        int n = withConn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "WITH u AS (UPDATE warp_s3_blobs SET state_at = now() WHERE object_id = ? AND state = 'uploading' "
                            + "RETURNING object_id) INSERT INTO warp_s3_chunks (object_id, seq, data) "
                            + "SELECT ?, ?, ? FROM u")) {
                ps.setObject(1, blob);
                ps.setObject(2, blob);
                ps.setInt(3, seq);
                ps.setBinaryStream(4, new ByteArrayInputStream(buf, 0, len), len);
                return ps.executeUpdate();
            }
        });
        if (n == 0) {
            throw new S3WireException(400, "RequestTimeout", "The upload was abandoned and collected before it finished");
        }
    }

    private void discardBlob(String host, UUID id) {
        try {
            withConn(host, c -> {
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE warp_s3_blobs SET state = 'garbage', state_at = now() WHERE object_id = ? AND state = 'uploading'")) {
                    ps.setObject(1, id);
                    return ps.executeUpdate();
                }
            });
        } catch (RuntimeException e) {
            log.debug("s3wire: could not discard blob {} (the collector will)", id, e);
        }
    }

    /** Makes a written blob permanent inside {@code c}'s transaction. */
    private void finalizeBlob(Connection c, Blob b) throws SQLException {
        if (b.pending() != null) {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_s3_blobs (object_id, state, size) VALUES (?, 'committed', ?)")) {
                ps.setObject(1, b.id());
                ps.setLong(2, b.size());
                ps.executeUpdate();
            }
            if (b.size() > 0) {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO warp_s3_chunks (object_id, seq, data) VALUES (?, 0, ?)")) {
                    ps.setObject(1, b.id());
                    ps.setBinaryStream(2, new ByteArrayInputStream(b.pending(), 0, (int) b.size()), (int) b.size());
                    ps.executeUpdate();
                }
            }
            return;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE warp_s3_blobs SET state = 'committed', size = ?, state_at = now() "
                        + "WHERE object_id = ? AND state = 'uploading'")) {
            ps.setLong(1, b.size());
            ps.setObject(2, b.id());
            if (ps.executeUpdate() == 0) {
                throw new S3WireException(400, "RequestTimeout", "The upload was abandoned and collected before it finished");
            }
        }
    }

    /** Content-MD5, SigV4 payload hash and client-claimed additional checksums; discards the blob and throws on a mismatch. */
    private void verify(Blob blob, String host, String contentMd5, java.util.function.BooleanSupplier payloadOk,
            java.util.function.Supplier<Map<String, String>> claimed) {
        S3WireException problem = null;
        if (contentMd5 != null) {
            byte[] expected;
            try {
                expected = Base64.getDecoder().decode(contentMd5.trim());
            } catch (IllegalArgumentException e) {
                expected = null;
            }
            if (expected == null || expected.length != 16) {
                problem = new S3WireException(400, "InvalidDigest", "The Content-MD5 you specified was invalid.");
            } else if (!java.util.Arrays.equals(expected, blob.md5())) {
                problem = new S3WireException(400, "BadDigest", "The Content-MD5 you specified did not match what we received.");
            }
        }
        if (problem == null && !payloadOk.getAsBoolean()) {
            problem = new S3WireException(400, "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.");
        }
        if (problem == null && claimed != null) {
            problem = checkClaimed(blob.checksums(), claimed.get());
        }
        if (problem != null) {
            if (blob.pending() == null) {
                discardBlob(host, blob.id());
            }
            throw problem;
        }
    }

    static S3WireException checkClaimed(Map<String, String> computed, Map<String, String> claimed) {
        for (Map.Entry<String, String> e : claimed.entrySet()) {
            if (!Checksums.wellFormed(e.getKey(), e.getValue())) {
                return new S3WireException(400, "InvalidRequest",
                        "Value for x-amz-checksum-" + e.getKey().toLowerCase() + " header is invalid.");
            }
            String actual = computed.get(e.getKey());
            if (actual != null && !actual.equals(e.getValue())) {
                return new S3WireException(400, "BadDigest",
                        "The " + e.getKey() + " you specified did not match the calculated checksum.");
            }
        }
        return null;
    }

    private static List<UUID> blobIdsOf(UUID objectId, String segmentsJson) {
        List<UUID> ids = new ArrayList<>();
        if (segmentsJson == null) {
            ids.add(objectId);
        } else {
            for (JsonElement e : GSON.fromJson(segmentsJson, JsonArray.class)) {
                ids.add(UUID.fromString(e.getAsJsonObject().get("id").getAsString()));
            }
        }
        return ids;
    }

    private static void markGarbage(Connection c, List<UUID> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE warp_s3_blobs SET state = 'garbage', state_at = now() WHERE object_id = ANY(?)")) {
            ps.setArray(1, c.createArrayOf("uuid", ids.toArray()));
            ps.executeUpdate();
        }
    }

    private static List<UUID> blobIdsOfRow(Row r) {
        List<UUID> ids = new ArrayList<>();
        for (ChunkMath.Segment s : r.segments()) {
            ids.add(UUID.fromString(s.blobId()));
        }
        return ids;
    }

    // ---- commit / delete of versions ---------------------------------------------------------------

    /** The content a commit points a key at. {@code segments == null}: a plain object (its blob is {@code objectId}). */
    private record NewObj(UUID objectId, List<ChunkMath.Segment> segments, List<PartMeta> parts, long size, String eTag,
            int chunkSize, Attrs attrs, Map<String, String> checksums, String checksumType, JsonObject extra) {
    }

    private record Committed(String versionId, Instant modified) {
    }

    private static String segJson(NewObj n) {
        if (n.segments() == null) {
            return null;
        }
        JsonArray arr = new JsonArray();
        for (int i = 0; i < n.segments().size(); i++) {
            ChunkMath.Segment s = n.segments().get(i);
            JsonObject o = new JsonObject();
            o.addProperty("id", s.blobId());
            o.addProperty("size", s.size());
            o.addProperty("cs", s.chunkSize());
            if (n.parts() != null && i < n.parts().size()) {
                o.addProperty("n", n.parts().get(i).number());
                if (!n.parts().get(i).checksums().isEmpty()) {
                    o.add("ck", GSON.toJsonTree(n.parts().get(i).checksums()));
                }
            }
            arr.add(o);
        }
        return arr.toString();
    }

    private static void moveCurrentToHistory(Connection c, String bucket, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("WITH d AS (DELETE FROM warp_s3_objects WHERE bucket = ? AND key = ? "
                + "RETURNING bucket, key, version_id, " + MOVE_COLS + ") INSERT INTO warp_s3_versions (bucket, key, "
                + "version_id, delete_marker, " + MOVE_COLS + ") SELECT bucket, key, COALESCE(version_id, 'null'), false, "
                + MOVE_COLS + " FROM d")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            ps.executeUpdate();
        }
    }

    /** Drops the history row of the null version (a suspended-versioning write replaces it); returns its blobs as garbage. */
    private static void dropHistoryNull(Connection c, String bucket, String key) throws SQLException {
        List<UUID> garbage = new ArrayList<>();
        boolean ann = false;
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_versions WHERE bucket = ? AND key = ? "
                + "AND version_id = 'null' RETURNING object_id, segments::text, delete_marker, extra::text")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (!rs.getBoolean(3)) {
                        garbage.addAll(blobIdsOf(rs.getObject(1, UUID.class), rs.getString(2)));
                    }
                    ann |= obj(rs.getString(4)).has("ann");
                }
            }
        }
        markGarbage(c, garbage);
        if (ann) {
            dropAnnotations(c, bucket, key, "null");
        }
    }

    private static void dropAnnotations(Connection c, String bucket, String key, String version) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM warp_s3_annotations WHERE bucket = ? AND key = ? AND version_id = ?")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            ps.setString(3, version);
            ps.executeUpdate();
        }
    }

    /**
     * The single commit point: locks the current row, points (bucket, key) at the new content and hands the
     * previous content to the collector (unversioned) or to the version history (versioned), all in one
     * transaction the caller already opened.
     */
    private Committed commitObject(Connection c, BucketInfo b, String bucket, String key, NewObj n, String ifNoneMatch,
            String ifMatch) throws SQLException {
        Row cur = b.versioning() == null && ifNoneMatch == null && ifMatch == null ? currentLite(c, bucket, key)
                : currentIn(c, "", bucket, key, true);
        if (ifNoneMatch != null && cur != null && (ifNoneMatch.trim().equals("*")
                || S3Http.etagMatches(ifNoneMatch, cur.eTag()))) {
            throw new S3WireException(412, "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold", null,
                    Map.of("Condition", "If-None-Match"));
        }
        if (ifMatch != null) {
            if (cur == null) {
                throw new S3WireException(404, "NoSuchKey", "The specified key does not exist.", null, Map.of("Key", key));
            }
            if (!S3Http.etagMatches(ifMatch, cur.eTag())) {
                throw new S3WireException(412, "PreconditionFailed",
                        "At least one of the pre-conditions you specified did not hold", null,
                        Map.of("Condition", "If-Match"));
            }
        }
        String newDb = null;
        String mode = b.versioning();
        if ("Enabled".equals(mode)) {
            newDb = newVersionId();
            if (cur != null) {
                moveCurrentToHistory(c, bucket, key);
            }
        } else if ("Suspended".equals(mode)) {
            if (cur != null && cur.dbVersion() != null) {
                moveCurrentToHistory(c, bucket, key);
                cur = null;
            }
            dropHistoryNull(c, bucket, key);
        }
        String segJson = segJson(n);
        Instant modified;
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO warp_s3_objects (bucket, key, object_id, size, etag, chunk_size, segments, content_type, "
                        + "cache_control, content_disposition, content_encoding, content_language, expires, user_metadata, "
                        + "last_modified, version_id, checksums, checksum_type, extra) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), "
                        + "?, ?, ?, ?, ?, ?, CAST(? AS jsonb), now(), ?, CAST(? AS jsonb), ?, CAST(? AS jsonb)) "
                        + "ON CONFLICT (bucket, key) DO UPDATE SET object_id = EXCLUDED.object_id, size = EXCLUDED.size, "
                        + "etag = EXCLUDED.etag, chunk_size = EXCLUDED.chunk_size, segments = EXCLUDED.segments, "
                        + "content_type = EXCLUDED.content_type, cache_control = EXCLUDED.cache_control, "
                        + "content_disposition = EXCLUDED.content_disposition, content_encoding = EXCLUDED.content_encoding, "
                        + "content_language = EXCLUDED.content_language, expires = EXCLUDED.expires, "
                        + "user_metadata = EXCLUDED.user_metadata, last_modified = now(), version_id = EXCLUDED.version_id, "
                        + "checksums = EXCLUDED.checksums, checksum_type = EXCLUDED.checksum_type, extra = EXCLUDED.extra "
                        + "RETURNING last_modified")) {
            int i = 1;
            Attrs a = n.attrs();
            ps.setString(i++, bucket);
            ps.setString(i++, key);
            ps.setObject(i++, n.objectId());
            ps.setLong(i++, n.size());
            ps.setString(i++, n.eTag());
            ps.setInt(i++, n.chunkSize());
            ps.setString(i++, segJson);
            ps.setString(i++, a.contentType());
            ps.setString(i++, a.cacheControl());
            ps.setString(i++, a.contentDisposition());
            ps.setString(i++, a.contentEncoding());
            ps.setString(i++, a.contentLanguage());
            ps.setString(i++, a.expires());
            ps.setString(i++, a.metadata() == null || a.metadata().isEmpty() ? null : GSON.toJson(a.metadata()));
            ps.setString(i++, newDb);
            ps.setString(i++, n.checksums() == null || n.checksums().isEmpty() ? null : GSON.toJson(n.checksums()));
            ps.setString(i++, n.checksumType());
            ps.setString(i, n.extra() == null || n.extra().size() == 0 ? null : n.extra().toString());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                Timestamp ts = rs.getTimestamp(1);
                modified = ts.toInstant();
            }
        }
        if (cur != null && !"Enabled".equals(mode)) { // replaced in place: its content is garbage now
            markGarbage(c, blobIdsOfRow(cur));
            if (cur.extra().has("ann")) {
                dropAnnotations(c, bucket, key, ext(cur.dbVersion()));
            }
        }
        return new Committed("Enabled".equals(mode) ? newDb : mode == null ? null : "null", modified);
    }

    // ---- put ---------------------------------------------------------------------------------------

    /** Attributes an object gets from its bucket's default encryption unless the request chose its own. */
    private static void defaultSse(JsonObject extra, BucketInfo b) {
        // plain SSE-S3 (AES256) is the implicit default and is not stored; only a non-default bucket rule is recorded
        if (!extra.has("sse") && b.encryptionAlgo() != null && !"AES256".equals(b.encryptionAlgo())) {
            extra.addProperty("sse", b.encryptionAlgo());
            if (b.encryptionKmsKey() != null && !extra.has("kms")) {
                extra.addProperty("kms", b.encryptionKmsKey());
            }
        }
    }

    /** Algorithms to compute for a write that declared {@code declared} ones (none declared = CRC64NVME by default). */
    static Set<String> effectiveAlgos(Set<String> declared, String uploadAlgo) {
        Set<String> out = new java.util.LinkedHashSet<>(declared);
        if (uploadAlgo != null) {
            out.add(uploadAlgo);
        }
        if (out.isEmpty()) {
            out.add(DEFAULT_CRC);
        }
        return out;
    }

    Meta put(PutIn r) throws IOException {
        S3Keys.validate(r.key());
        BucketInfo b = r.bucket0() != null ? r.bucket0() : requireBucket(r.bucket());
        if (r.length() > opt.maxObjectBytes()) {
            throw new S3WireException(400, "EntityTooLarge",
                    "Your proposed upload exceeds the maximum allowed object size (" + opt.maxObjectBytes() + " bytes)");
        }
        String host = owner(r.bucket(), r.key());
        Set<String> algos = effectiveAlgos(r.algos(), null);
        Blob blob = writeBlob(host, r.body(), r.length(), algos);
        verify(blob, host, r.contentMd5(), r.payloadOk(), r.claimed());
        Attrs attrs = withDefaultType(r.attrs());
        JsonObject extra = r.extra();
        defaultSse(extra, b);
        String eTag = S3Http.quote(blob.md5());
        try {
            Committed cm = withTx(host, c -> {
                finalizeBlob(c, blob);
                return commitObject(c, b, r.bucket(), r.key(), new NewObj(blob.id(), null, null, blob.size(), eTag,
                        blob.chunkSize(), attrs, blob.checksums(), "FULL_OBJECT", extra), r.ifNoneMatch(), r.ifMatch());
            });
            return new Meta(r.bucket(), r.key(), cm.versionId(), false, true, blob.size(), eTag, cm.modified(), attrs,
                    blob.checksums(), "FULL_OBJECT", extra, List.of());
        } catch (RuntimeException e) {
            if (blob.pending() == null) {
                discardBlob(host, blob.id());
            }
            throw e;
        }
    }

    // ---- delete ------------------------------------------------------------------------------------

    private static void checkLock(Row r, boolean bypassGovernance) {
        JsonObject x = r.extra();
        boolean denied = false;
        if (x.has("lh") && "ON".equals(x.get("lh").getAsString())) {
            denied = true;
        }
        if (x.has("lm") && x.has("lu")) {
            Instant until = Instant.parse(x.get("lu").getAsString());
            if (until.isAfter(Instant.now())) {
                String mode = x.get("lm").getAsString();
                if ("COMPLIANCE".equals(mode) || !bypassGovernance) {
                    denied = true;
                }
            }
        }
        if (denied) {
            throw new S3WireException(403, "AccessDenied", "Access Denied because object protected by object lock.");
        }
    }

    private static void insertMarker(Connection c, String bucket, String key, String versionId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_s3_versions (bucket, key, version_id, "
                + "delete_marker, object_id, size, etag, chunk_size) VALUES (?, ?, ?, true, ?, 0, '', 0)")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            ps.setString(3, versionId);
            ps.setObject(4, UUID.randomUUID());
            ps.executeUpdate();
        }
    }

    /** After the current row went away: the newest older version becomes current again (unless it is a delete marker). */
    private static void promote(Connection c, String host, String bucket, String key) throws SQLException {
        Row newest = newestHistoryIn(c, host, bucket, key);
        if (newest == null || newest.deleteMarker()) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement("WITH d AS (DELETE FROM warp_s3_versions WHERE bucket = ? AND key = ? "
                + "AND version_id = ? RETURNING bucket, key, version_id, " + MOVE_COLS + ") INSERT INTO warp_s3_objects "
                + "(bucket, key, version_id, " + MOVE_COLS + ") SELECT bucket, key, CASE WHEN version_id = 'null' THEN NULL "
                + "ELSE version_id END, " + MOVE_COLS + " FROM d")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            ps.setString(3, ext(newest.dbVersion()));
            ps.executeUpdate();
        }
    }

    private DeleteOut deleteIn(Connection c, String host, BucketInfo b, String key, String version, boolean bypass)
            throws SQLException {
        String bucket = b.name();
        Row cur = currentIn(c, host, bucket, key, true);
        if (version == null) {
            String mode = b.versioning();
            if (mode == null) {
                if (cur != null) {
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_objects WHERE bucket = ? AND key = ?")) {
                        ps.setString(1, bucket);
                        ps.setString(2, key);
                        ps.executeUpdate();
                    }
                    markGarbage(c, blobIdsOfRow(cur));
                    if (cur.extra().has("ann")) {
                        dropAnnotations(c, bucket, key, "null");
                    }
                }
                return new DeleteOut(null, false, cur != null);
            }
            String markerId;
            if ("Enabled".equals(mode)) {
                markerId = newVersionId();
                if (cur != null) {
                    moveCurrentToHistory(c, bucket, key);
                }
            } else {
                markerId = "null";
                if (cur != null) {
                    if (cur.dbVersion() == null) {
                        try (PreparedStatement ps = c.prepareStatement(
                                "DELETE FROM warp_s3_objects WHERE bucket = ? AND key = ?")) {
                            ps.setString(1, bucket);
                            ps.setString(2, key);
                            ps.executeUpdate();
                        }
                        markGarbage(c, blobIdsOfRow(cur));
                        if (cur.extra().has("ann")) {
                            dropAnnotations(c, bucket, key, "null");
                        }
                    } else {
                        moveCurrentToHistory(c, bucket, key);
                    }
                }
                dropHistoryNull(c, bucket, key);
            }
            insertMarker(c, bucket, key, markerId);
            return new DeleteOut(markerId, true, cur != null);
        }
        checkVersionId(version);
        boolean isCur = cur != null && (cur.dbVersion() == null ? version.equals("null") : version.equals(cur.dbVersion()));
        if (isCur) {
            checkLock(cur, bypass);
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_objects WHERE bucket = ? AND key = ?")) {
                ps.setString(1, bucket);
                ps.setString(2, key);
                ps.executeUpdate();
            }
            markGarbage(c, blobIdsOfRow(cur));
            if (cur.extra().has("ann")) {
                dropAnnotations(c, bucket, key, version);
            }
            promote(c, host, bucket, key);
            return new DeleteOut(version, false, true);
        }
        Row h = historyIn(c, host, bucket, key, version, true);
        if (h == null) {
            return new DeleteOut(version, false, false);
        }
        if (!h.deleteMarker()) {
            checkLock(h, bypass);
        }
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM warp_s3_versions WHERE bucket = ? AND key = ? AND version_id = ?")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            ps.setString(3, version);
            ps.executeUpdate();
        }
        if (!h.deleteMarker()) {
            markGarbage(c, blobIdsOfRow(h));
            if (h.extra().has("ann")) {
                dropAnnotations(c, bucket, key, version);
            }
        }
        if (cur == null) {
            promote(c, host, bucket, key);
        }
        return new DeleteOut(version, h.deleteMarker(), true);
    }

    DeleteOut delete(String bucket, String key, String version, boolean bypass) {
        S3Keys.validate(key);
        BucketInfo b = requireBucket(bucket);
        String host = owner(bucket, key);
        return withTx(host, c -> deleteIn(c, host, b, key, version, bypass));
    }

    private static void deleteKeys(Connection c, String bucket, List<String> keys) throws SQLException {
        List<UUID> garbage = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_objects WHERE bucket = ? AND key = ANY(?) "
                + "RETURNING object_id, segments::text")) {
            ps.setString(1, bucket);
            ps.setArray(2, c.createArrayOf("text", keys.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    garbage.addAll(blobIdsOf(rs.getObject(1, UUID.class), rs.getString(2)));
                }
            }
        }
        markGarbage(c, garbage);
    }

    DeleteManyOut deleteMany(String bucket, List<ObjId> ids, boolean bypass) {
        BucketInfo b = requireBucket(bucket);
        List<String> hosts = requireHosts();
        DeleteManyOut.Item[] out = new DeleteManyOut.Item[ids.size()];
        Map<String, List<Integer>> byHost = new LinkedHashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            ObjId id = ids.get(i);
            try {
                S3Keys.validate(id.key());
                checkVersionId(id.versionId());
            } catch (S3WireException e) {
                out[i] = new DeleteManyOut.Item(id.key(), id.versionId(), null, false, e.code, e.getMessage());
                continue;
            }
            byHost.computeIfAbsent(ownerOf(hosts, bucket, id.key()), h -> new ArrayList<>()).add(i);
        }
        for (Map.Entry<String, List<Integer>> e : byHost.entrySet()) {
            String host = e.getKey();
            boolean bulk = !b.versioned() && e.getValue().stream().allMatch(i -> ids.get(i).versionId() == null);
            if (bulk) {
                try {
                    List<String> keys = new ArrayList<>();
                    e.getValue().forEach(i -> keys.add(ids.get(i).key()));
                    withTx(host, c -> {
                        deleteKeys(c, bucket, keys);
                        return null;
                    });
                    for (int i : e.getValue()) {
                        out[i] = new DeleteManyOut.Item(ids.get(i).key(), null, null, false, null, null);
                    }
                } catch (S3WireException ex) {
                    for (int i : e.getValue()) {
                        out[i] = new DeleteManyOut.Item(ids.get(i).key(), null, null, false, "InternalError", ex.getMessage());
                    }
                }
                continue;
            }
            for (int i : e.getValue()) {
                ObjId id = ids.get(i);
                try {
                    DeleteOut d = withTx(host, c -> deleteIn(c, host, b, id.key(), id.versionId(), bypass));
                    out[i] = new DeleteManyOut.Item(id.key(), id.versionId(), d.versionId(), d.deleteMarker(), null, null);
                } catch (S3WireException ex) {
                    out[i] = new DeleteManyOut.Item(id.key(), id.versionId(), null, false, ex.code, ex.getMessage());
                }
            }
        }
        return new DeleteManyOut(java.util.Arrays.asList(out));
    }

    // ---- copy --------------------------------------------------------------------------------------

    private static void checkCopyConditions(ObjectStore.Conditions c, Row src) {
        if (c == null) {
            return;
        }
        int st = S3Http.evaluate(c, src.eTag(), src.lastModified());
        boolean unmodifiedFail = c.ifUnmodifiedSince() != null && c.ifMatch() == null && st == 412;
        if (st == 412 || st == 304) {
            // copy-source conditions never answer 304: every failed condition is 412 PreconditionFailed
            throw new S3WireException(412, "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold", null,
                    Map.of("Condition", unmodifiedFail ? "x-amz-copy-source-If-Unmodified-Since"
                            : "x-amz-copy-source-If-Match"));
        }
    }

    private Row requireCopySource(BucketInfo b, String key, String version) {
        S3Keys.validate(key);
        Row row = requireRow(b, key, version, false);
        if (row.deleteMarker()) {
            if (version == null) {
                throw new S3WireException(404, "NoSuchKey", "The specified key does not exist.", null, Map.of("Key", key));
            }
            throw new S3WireException(400, "InvalidRequest",
                    "The source of a copy request may not specifically refer to a delete marker by version id.");
        }
        return row;
    }

    CopyOut copy(CopyIn r) throws IOException {
        BucketInfo src = requireBucket(r.srcBucket());
        BucketInfo dst = r.dstBucketInfo() != null ? r.dstBucketInfo() : requireBucket(r.dstBucket());
        S3Keys.validate(r.dstKey());
        Row srcRow = requireCopySource(src, r.srcKey(), r.srcVersionId());
        checkCopyConditions(r.srcConditions(), srcRow);
        List<String> hosts = requireHosts();
        String dstHost = ownerOf(hosts, r.dstBucket(), r.dstKey());
        Attrs attrs = r.replaceAttrs() != null ? withDefaultType(r.replaceAttrs()) : srcRow.attrs();
        JsonObject extra = r.extra() == null ? new JsonObject() : r.extra().deepCopy();
        if (!r.replaceTags() && srcRow.extra().has("tags")) {
            extra.add("tags", srcRow.extra().get("tags").deepCopy());
        }
        defaultSse(extra, dst);
        boolean self = r.srcBucket().equals(r.dstBucket()) && r.srcKey().equals(r.dstKey());
        String srcVer = srcRow.deleteMarker() ? null : ext(srcRow.dbVersion());
        boolean srcVersioned = src.versioned();
        if (self && !dst.versioned()) {
            if (r.replaceAttrs() == null && !r.replaceTags() && r.algos().isEmpty()) {
                throw new S3WireException(400, "InvalidRequest", "This copy request is illegal because it is trying to "
                        + "copy an object to itself without changing the object's metadata, storage class, website "
                        + "redirect location or encryption attributes.");
            }
            JsonObject keep = srcRow.extra().deepCopy();
            if (r.replaceTags()) {
                keep.remove("tags");
                if (extra.has("tags")) {
                    keep.add("tags", extra.get("tags"));
                }
            }
            for (String k : List.of("sse", "kms", "sc", "wr")) {
                if (extra.has(k) && r.extra() != null && r.extra().has(k)) {
                    keep.add(k, extra.get(k));
                }
            }
            Instant modified = withTx(srcRow.host(), c -> {
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_s3_objects SET content_type = ?, "
                        + "cache_control = ?, content_disposition = ?, content_encoding = ?, content_language = ?, "
                        + "expires = ?, user_metadata = CAST(? AS jsonb), extra = CAST(? AS jsonb), last_modified = now() "
                        + "WHERE bucket = ? AND key = ? RETURNING last_modified")) {
                    int i = 1;
                    ps.setString(i++, attrs.contentType());
                    ps.setString(i++, attrs.cacheControl());
                    ps.setString(i++, attrs.contentDisposition());
                    ps.setString(i++, attrs.contentEncoding());
                    ps.setString(i++, attrs.contentLanguage());
                    ps.setString(i++, attrs.expires());
                    ps.setString(i++, attrs.metadata().isEmpty() ? null : GSON.toJson(attrs.metadata()));
                    ps.setString(i++, keep.size() == 0 ? null : keep.toString());
                    ps.setString(i++, r.dstBucket());
                    ps.setString(i, r.dstKey());
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        return rs.getTimestamp(1).toInstant();
                    }
                }
            });
            return new CopyOut(new Meta(r.dstBucket(), r.dstKey(), null, false, true, srcRow.size(), srcRow.eTag(),
                    modified, attrs, srcRow.checksums(), srcRow.checksumType(), keep, srcRow.parts()), srcVer);
        }
        Set<String> algos = r.algos();
        boolean plainSingle = srcRow.parts().isEmpty();
        boolean satisfied = srcRow.checksums().keySet().containsAll(algos);
        Blob blob;
        String eTag;
        Map<String, String> checksums;
        if (srcRow.host().equals(dstHost) && srcRow.segments().size() == 1 && plainSingle && satisfied) {
            blob = copyChunksInSql(dstHost, srcRow);
            eTag = srcRow.eTag();
            checksums = srcRow.checksums();
        } else {
            Set<String> want = !algos.isEmpty() ? algos : !srcRow.checksums().isEmpty()
                    ? srcRow.checksums().keySet() : Set.of(DEFAULT_CRC);
            InputStream in = srcRow.size() == 0 ? InputStream.nullInputStream()
                    : new ChunkInputStream(srcRow, 0, srcRow.size() - 1);
            blob = writeBlob(dstHost, in, srcRow.size(), want);
            eTag = S3Http.quote(blob.md5());
            checksums = blob.checksums();
        }
        String etagFinal = eTag;
        try {
            Committed cm = withTx(dstHost, c -> {
                finalizeBlob(c, blob);
                return commitObject(c, dst, r.dstBucket(), r.dstKey(), new NewObj(blob.id(), null, null, blob.size(),
                        etagFinal, blob.chunkSize(), attrs, checksums, "FULL_OBJECT", extra), null, null);
            });
            Meta m = new Meta(r.dstBucket(), r.dstKey(), cm.versionId(), false, true, blob.size(), etagFinal,
                    cm.modified(), attrs, checksums, "FULL_OBJECT", extra, List.of());
            if (srcRow.extra().has("ann") && !r.excludeAnnotations()) {
                copyAnnotations(srcRow, dstHost, r.dstBucket(), r.dstKey(), cm.versionId() == null ? "null" : cm.versionId());
            }
            return new CopyOut(m, srcVersioned ? srcVer : null);
        } catch (RuntimeException e) {
            if (blob.pending() == null) {
                discardBlob(dstHost, blob.id());
            }
            throw e;
        }
    }

    /** Same-shard copy of a plain object: chunk rows are duplicated inside Postgres, no bytes reach Warp. */
    private Blob copyChunksInSql(String host, Row src) {
        UUID id = UUID.randomUUID();
        withConn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_s3_blobs (object_id, state, size) VALUES (?, 'uploading', 0)")) {
                ps.setObject(1, id);
                return ps.executeUpdate();
            }
        });
        try {
            int chunks = ChunkMath.chunkCount(src.size(), src.chunkSize());
            UUID srcBlob = UUID.fromString(src.segments().get(0).blobId());
            for (int seq = 0; seq < chunks; seq++) {
                int s = seq;
                int n = withConn(host, c -> {
                    try (PreparedStatement ps = c.prepareStatement(
                            "WITH u AS (UPDATE warp_s3_blobs SET state_at = now() WHERE object_id = ? AND state = 'uploading' "
                                    + "RETURNING object_id) INSERT INTO warp_s3_chunks (object_id, seq, data) "
                                    + "SELECT ?, seq, data FROM warp_s3_chunks, u WHERE warp_s3_chunks.object_id = ? AND seq = ?")) {
                        ps.setObject(1, id);
                        ps.setObject(2, id);
                        ps.setObject(3, srcBlob);
                        ps.setInt(4, s);
                        return ps.executeUpdate();
                    }
                });
                if (n == 0) {
                    throw new S3WireException(404, "NoSuchKey", "The source object was replaced while it was being copied");
                }
            }
        } catch (RuntimeException e) {
            discardBlob(host, id);
            throw e;
        }
        return new Blob(id, src.size(), null, src.chunkSize(), null, Map.of());
    }

    // ---- listing -----------------------------------------------------------------------------------

    ObjectStore.ListResult list(ObjectStore.ListRequest r) {
        requireBucket(r.bucket());
        List<String> hosts = requireHosts();
        String prefix = r.prefix() == null ? "" : r.prefix();
        String delimiter = r.delimiter() == null || r.delimiter().isEmpty() ? null : r.delimiter();
        ListMerge.Marker marker = null;
        if (r.token() != null && !r.token().isEmpty()) {
            marker = ListMerge.decodeToken(r.token());
            if (marker == null) {
                return new ObjectStore.ListResult(List.of(), List.of(), false, null, null);
            }
        } else if (r.startAfter() != null && !r.startAfter().isEmpty()) {
            if (!r.v2() && ListMerge.isRolledUpPrefix(r.startAfter(), prefix, delimiter)) {
                String ub = S3Keys.prefixUpperBound(r.startAfter()); // a previous NextMarker that was a common prefix
                if (ub == null) {
                    return new ObjectStore.ListResult(List.of(), List.of(), false, null, null);
                }
                marker = new ListMerge.Marker(ub, true);
            } else {
                marker = new ListMerge.Marker(r.startAfter(), false);
            }
        }
        final ListMerge.Marker m = marker;
        List<List<ListMerge.Entry>> pages = new ArrayList<>();
        for (String h : hosts) {
            pages.add(listShard(h, r.bucket(), prefix, delimiter, m, r.maxKeys() + 1));
        }
        ListMerge.Merged merged = ListMerge.merge(pages, r.maxKeys());
        List<S3Xml.ObjectEntry> objects = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        for (ListMerge.Entry e : merged.entries()) {
            if (e.isPrefix()) {
                prefixes.add(e.sortKey());
            } else {
                objects.add(e.obj());
            }
        }
        ListMerge.Entry last = merged.entries().isEmpty() ? null : merged.entries().get(merged.entries().size() - 1);
        boolean truncated = merged.truncated() && last != null;
        return new ObjectStore.ListResult(objects, prefixes, truncated, truncated ? ListMerge.encodeToken(last) : null,
                truncated ? last.sortKey() : null);
    }

    private static List<String> algosOf(String json) {
        if (json == null) {
            return List.of();
        }
        Map<String, String> m = GSON.fromJson(json, STR_MAP);
        return Checksums.sortedAlgos(m);
    }

    /** Up to {@code limit} entries of one shard in key order, common prefixes already rolled up (skip-scan). */
    private List<ListMerge.Entry> listShard(String host, String bucket, String prefix, String delimiter,
            ListMerge.Marker marker, int limit) {
        String ub = S3Keys.prefixUpperBound(prefix);
        String lower = prefix;
        boolean inclusive = true;
        if (marker != null) {
            int cmp = S3Keys.compare(marker.value(), prefix);
            if (cmp > 0 || cmp == 0 && !marker.inclusive()) {
                lower = marker.value();
                inclusive = marker.inclusive();
            }
        }
        List<ListMerge.Entry> out = new ArrayList<>();
        String lastCp = null;
        while (out.size() < limit) {
            if (ub != null && S3Keys.compare(lower, ub) >= 0) {
                break;
            }
            int need = limit - out.size();
            String lo = lower;
            boolean incl = inclusive;
            List<Object[]> rows = withConn(host, c -> {
                StringBuilder sql = new StringBuilder("SELECT key, size, etag, last_modified, extra->>'sc', "
                        + "checksums::text, checksum_type FROM warp_s3_objects WHERE bucket = ? AND key ")
                        .append(incl ? ">=" : ">").append(" ?");
                if (ub != null) {
                    sql.append(" AND key < ?");
                }
                sql.append(" ORDER BY key LIMIT ?");
                try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                    int i = 1;
                    ps.setString(i++, bucket);
                    ps.setString(i++, lo);
                    if (ub != null) {
                        ps.setString(i++, ub);
                    }
                    ps.setInt(i, need);
                    List<Object[]> list = new ArrayList<>();
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            list.add(new Object[] {rs.getString(1), rs.getLong(2), rs.getString(3),
                                    rs.getTimestamp(4).toInstant(), rs.getString(5), rs.getString(6), rs.getString(7)});
                        }
                    }
                    return list;
                }
            });
            if (rows.isEmpty()) {
                break;
            }
            String tailCp = null;
            String tailKey = null;
            for (Object[] row : rows) {
                String key = (String) row[0];
                tailKey = key;
                int idx = delimiter == null ? -1 : key.indexOf(delimiter, prefix.length());
                if (idx >= 0) {
                    String cp = key.substring(0, idx + delimiter.length());
                    tailCp = cp;
                    if (!cp.equals(lastCp)) {
                        out.add(new ListMerge.Entry(cp, null));
                        lastCp = cp;
                    }
                } else {
                    tailCp = null;
                    out.add(new ListMerge.Entry(key, new S3Xml.ObjectEntry(key, (Instant) row[3], (String) row[2],
                            (Long) row[1], (String) row[4], algosOf((String) row[5]), (String) row[6])));
                }
            }
            if (rows.size() < need) {
                break;
            }
            if (tailCp != null) {
                String skip = S3Keys.prefixUpperBound(tailCp);
                if (skip == null) {
                    break;
                }
                lower = skip;
                inclusive = true;
            } else {
                lower = tailKey;
                inclusive = false;
            }
        }
        return out;
    }

    // ---- version listing -----------------------------------------------------------------------------

    /**
     * ListObjectVersions: every version of every key in (key asc, newest first) order. Each shard returns its
     * own sorted page of entries (the current row first, then the history newest first) and pages are merged like
     * plain listings. The marker is (key, versionId): everything of a key up to and including that version is skipped.
     */
    VersionsListing listVersions(String bucket, String prefix, String delimiter, String keyMarker,
            String versionMarker, int maxKeys) {
        requireBucket(bucket);
        List<String> hosts = requireHosts();
        String pfx = prefix == null ? "" : prefix;
        String dlm = delimiter == null || delimiter.isEmpty() ? null : delimiter;
        List<List<VersionEntry>> pages = new ArrayList<>();
        for (String h : hosts) {
            pages.add(listVersionsShard(h, bucket, pfx, dlm, keyMarker, versionMarker, maxKeys + 1));
        }
        // k-way merge by key (entries of one key come from a single shard and keep their order)
        int k = pages.size();
        int[] idx = new int[k];
        List<VersionEntry> out = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        boolean truncated = false;
        String lastSort = null;
        boolean lastWasPrefix = false;
        VersionEntry lastEntry = null;
        while (true) {
            int best = -1;
            for (int i = 0; i < k; i++) {
                if (idx[i] < pages.get(i).size() && (best < 0 || S3Keys.compare(pages.get(i).get(idx[i]).key(),
                        pages.get(best).get(idx[best]).key()) < 0)) {
                    best = i;
                }
            }
            if (best < 0) {
                break;
            }
            VersionEntry e = pages.get(best).get(idx[best]++);
            boolean isPrefix = e.versionId() == null;
            if (isPrefix && lastWasPrefix && e.key().equals(lastSort)) {
                continue;
            }
            if (out.size() + prefixes.size() == maxKeys) {
                truncated = true;
                break;
            }
            if (isPrefix) {
                prefixes.add(e.key());
            } else {
                out.add(e);
                lastEntry = e;
            }
            lastSort = e.key();
            lastWasPrefix = isPrefix;
        }
        String nextKey = null;
        String nextVer = null;
        if (truncated) {
            nextKey = lastSort;
            nextVer = lastWasPrefix || lastEntry == null ? null : lastEntry.versionId();
        }
        return new VersionsListing(out, prefixes, truncated, nextKey, nextVer);
    }

    private List<VersionEntry> listVersionsShard(String host, String bucket, String prefix, String delimiter,
            String keyMarker, String versionMarker, int limit) {
        String ub = S3Keys.prefixUpperBound(prefix);
        String lower = prefix;
        boolean inclusive = true;
        String skipVersionsOf = null; // rows of this key up to versionMarker are skipped
        if (keyMarker != null && !keyMarker.isEmpty()) {
            int cmp = S3Keys.compare(keyMarker, prefix);
            if (cmp >= 0) {
                boolean rolled = delimiter != null && ListMerge.isRolledUpPrefix(keyMarker, prefix, delimiter);
                if (rolled) {
                    String skip = S3Keys.prefixUpperBound(keyMarker);
                    lower = skip == null ? keyMarker : skip;
                    if (skip == null) {
                        return List.of();
                    }
                } else if (versionMarker != null && !versionMarker.isEmpty()) {
                    lower = keyMarker;
                    skipVersionsOf = keyMarker;
                } else {
                    lower = keyMarker;
                    inclusive = false;
                }
            }
        }
        List<VersionEntry> out = new ArrayList<>();
        String lastCp = null;
        boolean skipping = skipVersionsOf != null;
        while (out.size() < limit) {
            if (ub != null && S3Keys.compare(lower, ub) >= 0) {
                break;
            }
            int need = Math.max(limit - out.size(), 1) + 64;
            String lo = lower;
            boolean incl = inclusive;
            List<Object[]> rows = withConn(host, c -> {
                String sql = "SELECT key, version_id, dm, last_modified, etag, size, sc, cks, ckt, latest FROM ("
                        + "SELECT o.key AS key, COALESCE(o.version_id, 'null') AS version_id, false AS dm, o.last_modified, "
                        + "o.etag, o.size, o.extra->>'sc' AS sc, o.checksums::text AS cks, o.checksum_type AS ckt, "
                        + "true AS latest, 1 AS src, 0::bigint AS seq FROM warp_s3_objects o WHERE o.bucket = ? "
                        + "AND o.key " + (incl ? ">=" : ">") + " ?" + (ub != null ? " AND o.key < ?" : "")
                        + " UNION ALL SELECT v.key, v.version_id, v.delete_marker, v.last_modified, v.etag, v.size, "
                        + "v.extra->>'sc', v.checksums::text, v.checksum_type, (NOT EXISTS (SELECT 1 FROM warp_s3_objects x "
                        + "WHERE x.bucket = v.bucket AND x.key = v.key) AND v.seq = (SELECT max(seq) FROM warp_s3_versions y "
                        + "WHERE y.bucket = v.bucket AND y.key = v.key)), 0, v.seq FROM warp_s3_versions v WHERE v.bucket = ? "
                        + "AND v.key " + (incl ? ">=" : ">") + " ?" + (ub != null ? " AND v.key < ?" : "")
                        + ") u ORDER BY key COLLATE \"C\", src DESC, seq DESC LIMIT ?";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    int i = 1;
                    ps.setString(i++, bucket);
                    ps.setString(i++, lo);
                    if (ub != null) {
                        ps.setString(i++, ub);
                    }
                    ps.setString(i++, bucket);
                    ps.setString(i++, lo);
                    if (ub != null) {
                        ps.setString(i++, ub);
                    }
                    ps.setInt(i, need);
                    List<Object[]> list = new ArrayList<>();
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            list.add(new Object[] {rs.getString(1), rs.getString(2), rs.getBoolean(3),
                                    rs.getTimestamp(4).toInstant(), rs.getString(5), rs.getLong(6), rs.getString(7),
                                    rs.getString(8), rs.getString(9), rs.getBoolean(10)});
                        }
                    }
                    return list;
                }
            });
            if (rows.isEmpty()) {
                break;
            }
            boolean full = rows.size() == need;
            String tailKey = (String) rows.get(rows.size() - 1)[0];
            if (full) { // drop the trailing key group: it may continue in the next batch
                int cut = rows.size();
                while (cut > 0 && rows.get(cut - 1)[0].equals(tailKey)) {
                    cut--;
                }
                if (cut == 0) {
                    // one key with more versions than a batch: take them all in one go by re-querying unbounded per key
                    final String tk = tailKey;
                    rows = withConn(host, c -> allVersionsOf(c, bucket, tk));
                    full = false;
                } else {
                    rows = rows.subList(0, cut);
                    tailKey = (String) rows.get(rows.size() - 1)[0];
                }
            }
            String tailCp = null;
            for (Object[] row : rows) {
                String key = (String) row[0];
                if (skipping) {
                    if (key.equals(skipVersionsOf)) {
                        if (row[1].equals(versionMarker)) {
                            skipping = false;
                        }
                        continue;
                    }
                    skipping = false;
                }
                int idx = delimiter == null ? -1 : key.indexOf(delimiter, prefix.length());
                if (idx >= 0) {
                    String cp = key.substring(0, idx + delimiter.length());
                    tailCp = cp;
                    if (!cp.equals(lastCp)) {
                        out.add(new VersionEntry(cp, null, false, false, null, null, 0, null, List.of(), null));
                        lastCp = cp;
                    }
                } else {
                    tailCp = null;
                    out.add(new VersionEntry(key, (String) row[1], (Boolean) row[9], (Boolean) row[2], (Instant) row[3],
                            (String) row[4], (Long) row[5], (String) row[6], algosOf((String) row[7]), (String) row[8]));
                }
            }
            if (!full) {
                break;
            }
            if (tailCp != null) {
                String skip = S3Keys.prefixUpperBound(tailCp);
                if (skip == null) {
                    break;
                }
                lower = skip;
                inclusive = true;
            } else {
                lower = tailKey;
                inclusive = false;
            }
            skipping = false;
        }
        return out;
    }

    private static List<Object[]> allVersionsOf(Connection c, String bucket, String key) throws SQLException {
        List<Object[]> list = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT o.key, COALESCE(o.version_id, 'null'), false, o.last_modified, "
                + "o.etag, o.size, o.extra->>'sc', o.checksums::text, o.checksum_type, true, 1, 0::bigint "
                + "FROM warp_s3_objects o WHERE o.bucket = ? AND o.key = ? UNION ALL SELECT v.key, v.version_id, "
                + "v.delete_marker, v.last_modified, v.etag, v.size, v.extra->>'sc', v.checksums::text, v.checksum_type, "
                + "(NOT EXISTS (SELECT 1 FROM warp_s3_objects x WHERE x.bucket = v.bucket AND x.key = v.key) AND v.seq = "
                + "(SELECT max(seq) FROM warp_s3_versions y WHERE y.bucket = v.bucket AND y.key = v.key)), 0, v.seq "
                + "FROM warp_s3_versions v WHERE v.bucket = ? AND v.key = ? ORDER BY 11 DESC, 12 DESC")) {
            ps.setString(1, bucket);
            ps.setString(2, key);
            ps.setString(3, bucket);
            ps.setString(4, key);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(new Object[] {rs.getString(1), rs.getString(2), rs.getBoolean(3),
                            rs.getTimestamp(4).toInstant(), rs.getString(5), rs.getLong(6), rs.getString(7),
                            rs.getString(8), rs.getString(9), rs.getBoolean(10)});
                }
            }
        }
        return list;
    }

    // ---- in-place updates of a version's metadata (tags, acl, lock, ...) -----------------------------

    private record Target(Row row, boolean current) {
    }

    private Target resolveTarget(Connection c, String host, BucketInfo b, String key, String version)
            throws SQLException {
        String bucket = b.name();
        Row cur = currentIn(c, host, bucket, key, true);
        if (version == null) {
            if (cur != null) {
                return new Target(cur, true);
            }
            Row nh = b.versioned() ? newestHistoryIn(c, host, bucket, key) : null;
            if (nh != null && nh.deleteMarker()) {
                throw new S3WireException(404, "NoSuchKey", "The specified key does not exist.",
                        Map.of("x-amz-delete-marker", "true", "x-amz-version-id", ext(nh.dbVersion())), Map.of("Key", key));
            }
            throw new S3WireException(404, "NoSuchKey", "The specified key does not exist.", null, Map.of("Key", key));
        }
        boolean isCur = cur != null && (cur.dbVersion() == null ? version.equals("null") : version.equals(cur.dbVersion()));
        if (isCur) {
            return new Target(cur, true);
        }
        Row h = historyIn(c, host, bucket, key, version, true);
        if (h == null) {
            throw new S3WireException(404, "NoSuchVersion", "The specified version does not exist.", null,
                    Map.of("Key", key, "VersionId", version));
        }
        if (h.deleteMarker()) {
            throw new S3WireException(405, "MethodNotAllowed", "The specified method is not allowed against this resource.",
                    Map.of("x-amz-delete-marker", "true", "x-amz-version-id", version, "Allow", "DELETE"), null);
        }
        return new Target(h, false);
    }

    private static void writeExtra(Connection c, Target t, JsonObject x) throws SQLException {
        Row r = t.row();
        String table = t.current() ? "warp_s3_objects" : "warp_s3_versions";
        try (PreparedStatement ps = c.prepareStatement("UPDATE " + table + " SET extra = CAST(? AS jsonb) WHERE bucket = ? "
                + "AND key = ?" + (t.current() ? "" : " AND version_id = ?"))) {
            ps.setString(1, x.size() == 0 ? null : x.toString());
            ps.setString(2, r.bucket());
            ps.setString(3, r.key());
            if (!t.current()) {
                ps.setString(4, r.dbVersion());
            }
            ps.executeUpdate();
        }
    }

    /** Applies {@code fn} to the addressed version's {@code extra} document (tags, ACL, retention...) and returns it. */
    Meta mutateExtra(String bucket, String key, String version, java.util.function.Consumer<JsonObject> fn) {
        S3Keys.validate(key);
        checkVersionId(version);
        BucketInfo b = requireBucket(bucket);
        String host = owner(bucket, key);
        return withTx(host, c -> {
            Target t = resolveTarget(c, host, b, key, version);
            JsonObject x = t.row().extra().deepCopy();
            fn.accept(x);
            writeExtra(c, t, x);
            Row r = t.row();
            return new Meta(bucket, key, ext(r.dbVersion()), false, t.current(), r.size(), r.eTag(), r.lastModified(),
                    r.attrs(), r.checksums(), r.checksumType(), x, r.parts());
        });
    }

    // ---- annotations -------------------------------------------------------------------------------

    Annotation putAnnotation(String bucket, String key, String version, String name, byte[] payload,
            String contentType, Set<String> algos, Map<String, String> claimed) {
        S3Keys.validate(key);
        checkVersionId(version);
        BucketInfo b = requireBucket(bucket);
        String host = owner(bucket, key);
        Checksums.Acc acc = new Checksums.Acc(effectiveAlgos(algos, null));
        acc.update(payload, 0, payload.length);
        Map<String, String> computed = acc.finish();
        S3WireException bad = checkClaimed(computed, claimed);
        if (bad != null) {
            throw bad;
        }
        String etag = S3Http.quote(md5().digest(payload));
        return withTx(host, c -> {
            Target t = resolveTarget(c, host, b, key, version);
            Row r = t.row();
            if (!r.extra().has("ann")) {
                JsonObject x = r.extra().deepCopy();
                x.addProperty("ann", true);
                writeExtra(c, t, x);
            }
            String vid = ext(r.dbVersion());
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_s3_annotations (bucket, key, version_id, name, "
                    + "payload, etag, checksums, checksum_type, content_type) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), "
                    + "'FULL_OBJECT', ?) ON CONFLICT (bucket, key, version_id, name) DO UPDATE SET payload = EXCLUDED.payload, "
                    + "etag = EXCLUDED.etag, checksums = EXCLUDED.checksums, checksum_type = EXCLUDED.checksum_type, "
                    + "content_type = EXCLUDED.content_type, modified = now()")) {
                ps.setString(1, bucket);
                ps.setString(2, key);
                ps.setString(3, vid);
                ps.setString(4, name);
                ps.setBytes(5, payload);
                ps.setString(6, etag);
                ps.setString(7, GSON.toJson(computed));
                ps.setString(8, contentType);
                ps.executeUpdate();
            }
            return new Annotation(name, payload, payload.length, etag, Instant.now(), computed, "FULL_OBJECT",
                    contentType, b.versioned() ? vid : null);
        });
    }

    private Annotation readAnnotation(ResultSet rs, boolean withPayload, String vid) throws SQLException {
        byte[] p = withPayload ? rs.getBytes("payload") : null;
        Map<String, String> ck = rs.getString("cks") == null ? Map.of() : GSON.fromJson(rs.getString("cks"), STR_MAP);
        return new Annotation(rs.getString("name"), p, rs.getLong("sz"), rs.getString("etag"),
                rs.getTimestamp("modified").toInstant(), ck, rs.getString("checksum_type"), rs.getString("content_type"), vid);
    }

    /** @return the annotation (payload included) or null; throws for a missing object/version */
    Annotation getAnnotation(String bucket, String key, String version, String name) {
        BucketInfo b = requireBucket(bucket);
        Row row = requireRow(b, key, version, true);
        return withConn(row.host(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT name, payload, octet_length(payload) AS sz, etag, "
                    + "modified, checksums::text AS cks, checksum_type, content_type FROM warp_s3_annotations "
                    + "WHERE bucket = ? AND key = ? AND version_id = ? AND name = ?")) {
                ps.setString(1, bucket);
                ps.setString(2, key);
                ps.setString(3, ext(row.dbVersion()));
                ps.setString(4, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? readAnnotation(rs, true, b.versioned() ? ext(row.dbVersion()) : null) : null;
                }
            }
        });
    }

    List<Annotation> listAnnotations(String bucket, String key, String version) {
        BucketInfo b = requireBucket(bucket);
        Row row = requireRow(b, key, version, true);
        return withConn(row.host(), c -> {
            List<Annotation> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT name, NULL::bytea AS payload, octet_length(payload) AS sz, "
                    + "etag, modified, checksums::text AS cks, checksum_type, content_type FROM warp_s3_annotations "
                    + "WHERE bucket = ? AND key = ? AND version_id = ? ORDER BY name")) {
                ps.setString(1, bucket);
                ps.setString(2, key);
                ps.setString(3, ext(row.dbVersion()));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(readAnnotation(rs, false, b.versioned() ? ext(row.dbVersion()) : null));
                    }
                }
            }
            return out;
        });
    }

    /** @return the version id the annotation lived on (null when the bucket is unversioned); a missing annotation is fine */
    String deleteAnnotation(String bucket, String key, String version, String name) {
        S3Keys.validate(key);
        BucketInfo b = requireBucket(bucket);
        String host = owner(bucket, key);
        return withTx(host, c -> {
            Target t = resolveTarget(c, host, b, key, version);
            String vid = ext(t.row().dbVersion());
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_annotations WHERE bucket = ? AND key = ? "
                    + "AND version_id = ? AND name = ?")) {
                ps.setString(1, bucket);
                ps.setString(2, key);
                ps.setString(3, vid);
                ps.setString(4, name);
                ps.executeUpdate();
            }
            return b.versioned() ? vid : null;
        });
    }

    private void copyAnnotations(Row src, String dstHost, String dstBucket, String dstKey, String dstVersion) {
        List<Object[]> rows = withConn(src.host(), c -> {
            List<Object[]> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT name, payload, etag, checksums::text, checksum_type, "
                    + "content_type FROM warp_s3_annotations WHERE bucket = ? AND key = ? AND version_id = ?")) {
                ps.setString(1, src.bucket());
                ps.setString(2, src.key());
                ps.setString(3, ext(src.dbVersion()));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Object[] {rs.getString(1), rs.getBytes(2), rs.getString(3), rs.getString(4),
                                rs.getString(5), rs.getString(6)});
                    }
                }
            }
            return out;
        });
        if (rows.isEmpty()) {
            return;
        }
        withTx(dstHost, c -> {
            for (Object[] a : rows) {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_s3_annotations (bucket, key, version_id, "
                        + "name, payload, etag, checksums, checksum_type, content_type) VALUES (?, ?, ?, ?, ?, ?, "
                        + "CAST(? AS jsonb), ?, ?) ON CONFLICT (bucket, key, version_id, name) DO NOTHING")) {
                    ps.setString(1, dstBucket);
                    ps.setString(2, dstKey);
                    ps.setString(3, dstVersion);
                    ps.setString(4, (String) a[0]);
                    ps.setBytes(5, (byte[]) a[1]);
                    ps.setString(6, (String) a[2]);
                    ps.setString(7, (String) a[3]);
                    ps.setString(8, (String) a[4]);
                    ps.setString(9, (String) a[5]);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_s3_objects SET extra = COALESCE(extra, '{}'::jsonb) "
                    + "|| '{\"ann\": true}'::jsonb WHERE bucket = ? AND key = ?")) {
                ps.setString(1, dstBucket);
                ps.setString(2, dstKey);
                ps.executeUpdate();
            }
            return null;
        });
    }

    // ---- multipart ---------------------------------------------------------------------------------

    /**
     * @param algo    declared checksum algorithm (null = none declared)
     * @param type    COMPOSITE / FULL_OBJECT (null = default for the algorithm)
     */
    String createMultipart(String bucket, String key, Attrs attrs, JsonObject extra, String algo, String type) {
        S3Keys.validate(key);
        BucketInfo b = requireBucket(bucket);
        if (type != null && !"COMPOSITE".equals(type) && !"FULL_OBJECT".equals(type)) {
            throw new S3WireException(400, "InvalidRequest", "Checksum type " + type + " is not valid");
        }
        if ("FULL_OBJECT".equals(type) && algo != null && !Checksums.isCrc(algo)) {
            throw new S3WireException(400, "InvalidRequest", "The FULL_OBJECT checksum type is not supported with the "
                    + algo + " algorithm.");
        }
        if ("COMPOSITE".equals(type) && "CRC64NVME".equals(algo)) {
            throw new S3WireException(400, "InvalidRequest",
                    "The COMPOSITE checksum type is not supported with the CRC64NVME algorithm.");
        }
        String host = owner(bucket, key);
        UUID id = UUID.randomUUID();
        Attrs a = withDefaultType(attrs);
        JsonObject ex = extra == null ? new JsonObject() : extra;
        defaultSse(ex, b);
        withConn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_s3_multipart_uploads (upload_id, bucket, key, attrs) VALUES (?, ?, ?, CAST(? AS jsonb))")) {
                ps.setObject(1, id);
                ps.setString(2, bucket);
                ps.setString(3, key);
                JsonObject o = new JsonObject();
                addIfSet(o, "ct", a.contentType());
                addIfSet(o, "cc", a.cacheControl());
                addIfSet(o, "cd", a.contentDisposition());
                addIfSet(o, "ce", a.contentEncoding());
                addIfSet(o, "cl", a.contentLanguage());
                addIfSet(o, "ex", a.expires());
                o.add("meta", GSON.toJsonTree(a.metadata()));
                addIfSet(o, "cka", algo);
                addIfSet(o, "ckt", type != null ? type : algo == null ? null
                        : "CRC64NVME".equals(algo) ? "FULL_OBJECT" : "COMPOSITE");
                if (ex.size() > 0) {
                    o.add("extra", ex);
                }
                ps.setString(4, o.toString());
                return ps.executeUpdate();
            }
        });
        return S3Http.encodeUploadId(host, id);
    }

    private static void addIfSet(JsonObject o, String k, String v) {
        if (v != null) {
            o.addProperty(k, v);
        }
    }

    private static Attrs attrsFromJson(JsonObject o) {
        Map<String, String> meta = o.has("meta") ? GSON.fromJson(o.get("meta"), STR_MAP) : Map.of();
        return new Attrs(str(o, "ct"), str(o, "cc"), str(o, "cd"), str(o, "ce"), str(o, "cl"), str(o, "ex"), meta);
    }

    private static String str(JsonObject o, String k) {
        return o.has(k) ? o.get(k).getAsString() : null;
    }

    /** Resolves an upload id to its host, validating it is ours, on a live host, and still where (bucket, key) hashes. */
    private S3Http.UploadRef resolveUpload(String bucket, String key, String uploadId) {
        S3Http.UploadRef ref = S3Http.decodeUploadId(uploadId);
        List<String> hosts = requireHosts();
        if (ref == null || !hosts.contains(ref.host()) || !ref.host().equals(ownerOf(hosts, bucket, key))) {
            throw noSuchUpload(uploadId);
        }
        return ref;
    }

    private static S3WireException noSuchUpload(String uploadId) {
        return new S3WireException(404, "NoSuchUpload", "The specified upload does not exist. The upload ID may be "
                + "invalid, or the upload may have been aborted or completed.", null,
                uploadId == null ? null : Map.of("UploadId", uploadId));
    }

    private JsonObject uploadAttrs(String host, UUID id, String bucket, String key, String uploadId) {
        JsonObject o = withConn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT attrs::text FROM warp_s3_multipart_uploads WHERE upload_id = ? AND bucket = ? AND key = ?")) {
                ps.setObject(1, id);
                ps.setString(2, bucket);
                ps.setString(3, key);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? obj(rs.getString(1)) : null;
                }
            }
        });
        if (o == null) {
            throw noSuchUpload(uploadId);
        }
        return o;
    }

    PartOut uploadPart(UploadPartIn r, Set<String> declared) throws IOException {
        S3Keys.validate(r.key());
        requireBucket(r.bucket());
        S3Http.UploadRef ref = resolveUpload(r.bucket(), r.key(), r.uploadId());
        if (r.length() > opt.maxObjectBytes()) {
            throw new S3WireException(400, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size");
        }
        String host = ref.host();
        JsonObject up = uploadAttrs(host, ref.id(), r.bucket(), r.key(), r.uploadId());
        Blob blob = writeBlob(host, r.body(), r.length(), effectiveAlgos(declared, str(up, "cka")));
        verify(blob, host, r.contentMd5(), r.payloadOk(), r.claimed());
        return storePart(host, ref.id(), r.partNumber(), blob);
    }

    private PartOut storePart(String host, UUID uploadId, int partNumber, Blob blob) {
        String eTag = S3Http.quote(blob.md5());
        try {
            withTx(host, c -> {
                UUID old = null;
                try (PreparedStatement ps = c.prepareStatement("SELECT object_id FROM warp_s3_parts "
                        + "WHERE upload_id = ? AND part_number = ? FOR UPDATE")) {
                    ps.setObject(1, uploadId);
                    ps.setInt(2, partNumber);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            old = rs.getObject(1, UUID.class);
                        }
                    }
                }
                finalizeBlob(c, blob);
                int n;
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_s3_parts (upload_id, part_number, "
                        + "object_id, size, etag, chunk_size, checksums, modified) SELECT ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), now() "
                        + "WHERE EXISTS (SELECT 1 FROM warp_s3_multipart_uploads WHERE upload_id = ?) "
                        + "ON CONFLICT (upload_id, part_number) DO UPDATE SET object_id = EXCLUDED.object_id, "
                        + "size = EXCLUDED.size, etag = EXCLUDED.etag, chunk_size = EXCLUDED.chunk_size, "
                        + "checksums = EXCLUDED.checksums, modified = now()")) {
                    ps.setObject(1, uploadId);
                    ps.setInt(2, partNumber);
                    ps.setObject(3, blob.id());
                    ps.setLong(4, blob.size());
                    ps.setString(5, eTag);
                    ps.setInt(6, blob.chunkSize());
                    ps.setString(7, blob.checksums().isEmpty() ? null : GSON.toJson(blob.checksums()));
                    ps.setObject(8, uploadId);
                    n = ps.executeUpdate();
                }
                if (n == 0) {
                    throw noSuchUpload(null);
                }
                if (old != null) {
                    markGarbage(c, List.of(old));
                }
                return null;
            });
        } catch (RuntimeException e) {
            if (blob.pending() == null) {
                discardBlob(host, blob.id());
            }
            throw e;
        }
        return new PartOut(eTag, blob.checksums());
    }

    /** UploadPartCopy: the part's bytes come from (a byte range of) an existing object. */
    PartCopyOut uploadPartCopy(UploadPartCopyIn r) throws IOException {
        S3Keys.validate(r.key());
        requireBucket(r.bucket());
        BucketInfo src = requireBucket(r.srcBucket());
        S3Http.UploadRef ref = resolveUpload(r.bucket(), r.key(), r.uploadId());
        String host = ref.host();
        JsonObject up = uploadAttrs(host, ref.id(), r.bucket(), r.key(), r.uploadId());
        Row srcRow = requireCopySource(src, r.srcKey(), r.srcVersionId());
        checkCopyConditions(r.srcConditions(), srcRow);
        long start = 0;
        long end = srcRow.size() - 1;
        if (r.range() != null) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("^bytes=(\\d+)-(\\d+)$").matcher(r.range().trim());
            if (!m.matches()) {
                throw new S3WireException(400, "InvalidArgument", "The x-amz-copy-source-range value must be of the form "
                        + "bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy");
            }
            start = Long.parseLong(m.group(1));
            end = Long.parseLong(m.group(2));
            if (start > end || start >= srcRow.size()) {
                throw new S3WireException(416, "InvalidRange", "Range specified is not valid for source object of size: "
                        + srcRow.size());
            }
            end = Math.min(end, srcRow.size() - 1);
        }
        long length = srcRow.size() == 0 ? 0 : end - start + 1;
        InputStream in = length == 0 ? InputStream.nullInputStream() : new ChunkInputStream(srcRow, start, end);
        Blob blob = writeBlob(host, in, length, effectiveAlgos(Set.of(), str(up, "cka")));
        PartOut p = storePart(host, ref.id(), r.partNumber(), blob);
        return new PartCopyOut(p.eTag(), Instant.now(), p.checksums(), src.versioned() ? ext(srcRow.dbVersion()) : null);
    }

    private record PartRowDb(UUID blob, long size, String eTag, int chunkSize, Map<String, String> checksums,
            Instant modified) {
    }

    CompleteOut completeMultipart(String bucket, String key, String uploadId, List<CompletePart> parts,
            Map<String, String> claimedFull, String requestedType, String ifNoneMatch) {
        S3Keys.validate(key);
        BucketInfo b = requireBucket(bucket);
        S3Http.UploadRef ref = resolveUpload(bucket, key, uploadId);
        if (parts.isEmpty()) {
            throw new S3WireException(400, "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema");
        }
        String host = ref.host();
        return withTx(host, c -> {
            JsonObject up;
            try (PreparedStatement ps = c.prepareStatement("SELECT attrs::text FROM warp_s3_multipart_uploads "
                    + "WHERE upload_id = ? AND bucket = ? AND key = ? FOR UPDATE")) {
                ps.setObject(1, ref.id());
                ps.setString(2, bucket);
                ps.setString(3, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw noSuchUpload(uploadId);
                    }
                    up = obj(rs.getString(1));
                }
            }
            Attrs attrs = attrsFromJson(up);
            Map<Integer, PartRowDb> stored = new LinkedHashMap<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT part_number, object_id, size, etag, chunk_size, "
                    + "checksums::text, modified FROM warp_s3_parts WHERE upload_id = ?")) {
                ps.setObject(1, ref.id());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        stored.put(rs.getInt(1), new PartRowDb(rs.getObject(2, UUID.class), rs.getLong(3), rs.getString(4),
                                rs.getInt(5), rs.getString(6) == null ? Map.of() : GSON.fromJson(rs.getString(6), STR_MAP),
                                rs.getTimestamp(7).toInstant()));
                    }
                }
            }
            int prev = 0;
            for (CompletePart p : parts) { // order is validated for the whole list before any part is looked at
                if (p.number() <= prev) {
                    throw new S3WireException(400, "InvalidPartOrder", "The list of parts was not in ascending order. "
                            + "The parts list must be specified in order by part number.");
                }
                prev = p.number();
            }
            List<PartRowDb> chosen = new ArrayList<>();
            for (int i = 0; i < parts.size(); i++) {
                CompletePart p = parts.get(i);
                PartRowDb row = stored.get(p.number());
                if (row == null || p.eTag() != null && !S3Http.etagMatches(p.eTag(), row.eTag())) {
                    throw new S3WireException(400, "InvalidPart", "One or more of the specified parts could not be found.  "
                            + "The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
                            null, Map.of("UploadId", uploadId, "PartNumber", String.valueOf(p.number()),
                                    "ETag", p.eTag() == null ? "" : p.eTag()));
                }
                if (row.size() < MIN_PART && i < parts.size() - 1) {
                    throw new S3WireException(400, "EntityTooSmall",
                            "Your proposed upload is smaller than the minimum allowed object size.", null,
                            Map.of("ETag", row.eTag(), "MinSizeAllowed", String.valueOf(MIN_PART),
                                    "PartNumber", String.valueOf(p.number()), "ProposedSize", String.valueOf(row.size())));
                }
                chosen.add(row);
            }
            // ---- checksum type and value
            String declaredAlgo = str(up, "cka");
            String algo = declaredAlgo;
            if (algo == null) {
                for (String cand : Checksums.ALL) {
                    if (!DEFAULT_CRC.equals(cand) && chosen.stream().allMatch(pr -> pr.checksums().containsKey(cand))) {
                        algo = cand;
                        break;
                    }
                }
                if (algo == null) {
                    algo = DEFAULT_CRC;
                }
            }
            String type = str(up, "ckt");
            if (type == null) {
                type = requestedType != null ? requestedType : DEFAULT_CRC.equals(algo) ? "FULL_OBJECT" : "COMPOSITE";
            }
            if (requestedType != null && !requestedType.equals(type)) {
                throw new S3WireException(400, "InvalidRequest", "The upload was created with the " + type
                        + " checksum type, but the complete request asked for " + requestedType + ".");
            }
            List<String> values = new ArrayList<>();
            List<Long> sizes = new ArrayList<>();
            for (int i = 0; i < parts.size(); i++) {
                CompletePart p = parts.get(i);
                PartRowDb row = chosen.get(i);
                String have = row.checksums().get(algo);
                String said = p.checksums().get(algo);
                if ("COMPOSITE".equals(type) && declaredAlgo != null && said == null) {
                    throw new S3WireException(400, "InvalidRequest", "The upload was created using a "
                            + algo.toLowerCase() + " checksum. The complete request must include the checksum for each part. "
                            + "It was missing for part " + p.number() + " in the request.");
                }
                if (said != null && have != null && !said.equals(have)) {
                    throw new S3WireException(400, "InvalidPart", "One or more of the specified parts could not be found.  "
                            + "The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.");
                }
                if (have == null) {
                    throw new S3WireException(400, "InvalidRequest", "Part " + p.number() + " has no " + algo
                            + " checksum.");
                }
                values.add(have);
                sizes.add(row.size());
            }
            String finalValue = "COMPOSITE".equals(type) ? Checksums.composite(algo, values)
                    : Checksums.fullObject(algo, values, sizes);
            String claim = claimedFull.get(algo);
            if (claim != null && !claim.equals(finalValue)) {
                throw new S3WireException(400, "BadDigest",
                        "The " + algo + " you specified did not match the calculated checksum.");
            }
            Map<String, String> checksums = new LinkedHashMap<>();
            checksums.put(algo, finalValue);
            // ---- assemble
            List<ChunkMath.Segment> segments = new ArrayList<>();
            List<PartMeta> metas = new ArrayList<>();
            List<byte[]> md5s = new ArrayList<>();
            java.util.Set<UUID> used = new java.util.HashSet<>();
            long total = 0;
            for (int i = 0; i < parts.size(); i++) {
                PartRowDb row = chosen.get(i);
                total += row.size();
                if (total > opt.maxMultipartBytes()) {
                    throw new S3WireException(400, "EntityTooLarge", "The completed object would exceed the maximum allowed size ("
                            + opt.maxMultipartBytes() + " bytes)");
                }
                segments.add(new ChunkMath.Segment(row.blob().toString(), row.size(), row.chunkSize()));
                metas.add(new PartMeta(parts.get(i).number(), row.size(), row.checksums()));
                md5s.add(S3Http.unhex(row.eTag().replace("\"", "")));
                used.add(row.blob());
            }
            List<UUID> unused = new ArrayList<>();
            stored.values().forEach(pr -> {
                if (!used.contains(pr.blob())) {
                    unused.add(pr.blob());
                }
            });
            markGarbage(c, unused);
            String eTag = S3Http.multipartEtag(md5s);
            JsonObject extra = up.has("extra") ? up.getAsJsonObject("extra") : new JsonObject();
            Committed cm = commitObject(c, b, bucket, key, new NewObj(UUID.randomUUID(), segments, metas, total, eTag,
                    opt.chunkBytes(), attrs, checksums, type, extra), ifNoneMatch, null);
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_parts WHERE upload_id = ?")) {
                ps.setObject(1, ref.id());
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_multipart_uploads WHERE upload_id = ?")) {
                ps.setObject(1, ref.id());
                ps.executeUpdate();
            }
            return new CompleteOut(eTag, cm.versionId(), checksums, type);
        });
    }

    void abortMultipart(String bucket, String key, String uploadId) {
        S3Keys.validate(key);
        S3Http.UploadRef ref = resolveUpload(bucket, key, uploadId);
        withTx(ref.host(), c -> {
            int n;
            try (PreparedStatement ps = c.prepareStatement(
                    "DELETE FROM warp_s3_multipart_uploads WHERE upload_id = ? AND bucket = ? AND key = ?")) {
                ps.setObject(1, ref.id());
                ps.setString(2, bucket);
                ps.setString(3, key);
                n = ps.executeUpdate();
            }
            if (n == 0) {
                throw noSuchUpload(uploadId);
            }
            discardParts(c, ref.id());
            return null;
        });
    }

    private static UploadInfo uploadInfo(String uploadId, String key, Instant initiated, String attrsJson) {
        JsonObject o = obj(attrsJson);
        return new UploadInfo(uploadId, key, initiated, attrsFromJson(o), str(o, "cka"), str(o, "ckt"),
                o.has("extra") ? o.getAsJsonObject("extra") : new JsonObject());
    }

    PartListing listParts(String bucket, String key, String uploadId, int marker, int maxParts) {
        S3Keys.validate(key);
        requireBucket(bucket);
        S3Http.UploadRef ref = resolveUpload(bucket, key, uploadId);
        return withConn(ref.host(), c -> {
            UploadInfo info;
            try (PreparedStatement ps = c.prepareStatement("SELECT initiated, attrs::text FROM warp_s3_multipart_uploads "
                    + "WHERE upload_id = ? AND bucket = ? AND key = ?")) {
                ps.setObject(1, ref.id());
                ps.setString(2, bucket);
                ps.setString(3, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw noSuchUpload(uploadId);
                    }
                    info = uploadInfo(uploadId, key, rs.getTimestamp(1).toInstant(), rs.getString(2));
                }
            }
            List<PartRow> rows = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT part_number, modified, etag, size, checksums::text "
                    + "FROM warp_s3_parts WHERE upload_id = ? AND part_number > ? ORDER BY part_number LIMIT ?")) {
                ps.setObject(1, ref.id());
                ps.setInt(2, marker);
                ps.setInt(3, maxParts + 1);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new PartRow(rs.getInt(1), rs.getTimestamp(2).toInstant(), rs.getString(3), rs.getLong(4),
                                rs.getString(5) == null ? Map.of() : GSON.fromJson(rs.getString(5), STR_MAP)));
                    }
                }
            }
            boolean truncated = rows.size() > maxParts;
            if (truncated) {
                rows = rows.subList(0, maxParts);
            }
            return new PartListing(info, rows, truncated, rows.isEmpty() ? 0 : rows.get(rows.size() - 1).number());
        });
    }

    UploadsListing listUploads(String bucket, String prefix, String delimiter, String keyMarker, String uploadIdMarker,
            int maxUploads) {
        requireBucket(bucket);
        String pfx = prefix == null ? "" : prefix;
        String dlm = delimiter == null || delimiter.isEmpty() ? null : delimiter;
        String ub = S3Keys.prefixUpperBound(pfx);
        List<UploadInfo> all = new ArrayList<>();
        for (String h : requireHosts()) {
            withConn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT upload_id, key, initiated, attrs::text FROM "
                        + "warp_s3_multipart_uploads WHERE bucket = ? AND key >= ?" + (ub != null ? " AND key < ?" : "")
                        + " ORDER BY key, initiated, upload_id LIMIT 10000")) {
                    int i = 1;
                    ps.setString(i++, bucket);
                    ps.setString(i++, pfx);
                    if (ub != null) {
                        ps.setString(i, ub);
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            all.add(uploadInfo(S3Http.encodeUploadId(h, rs.getObject(1, UUID.class)), rs.getString(2),
                                    rs.getTimestamp(3).toInstant(), rs.getString(4)));
                        }
                    }
                }
                return null;
            });
        }
        all.sort((x, y) -> {
            int k = S3Keys.compare(x.key(), y.key());
            if (k != 0) {
                return k;
            }
            int t = x.initiated().compareTo(y.initiated());
            return t != 0 ? t : x.uploadId().compareTo(y.uploadId());
        });
        List<UploadInfo> out = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        boolean truncated = false;
        String lastKey = null;
        String lastUpload = null;
        String lastCp = null;
        boolean skipping = keyMarker != null && !keyMarker.isEmpty() && uploadIdMarker != null && !uploadIdMarker.isEmpty();
        for (UploadInfo u : all) {
            if (keyMarker != null && !keyMarker.isEmpty()) {
                int k = S3Keys.compare(u.key(), keyMarker);
                if (k < 0) {
                    continue;
                }
                if (k == 0) {
                    if (skipping) {
                        if (u.uploadId().equals(uploadIdMarker)) {
                            skipping = false;
                        }
                        continue;
                    }
                    if (uploadIdMarker == null || uploadIdMarker.isEmpty()) {
                        continue; // key-marker alone: strictly after that key
                    }
                }
            }
            int idx = dlm == null ? -1 : u.key().indexOf(dlm, pfx.length());
            if (idx >= 0) {
                String cp = u.key().substring(0, idx + dlm.length());
                if (cp.equals(lastCp)) {
                    continue;
                }
                if (out.size() + prefixes.size() == maxUploads) {
                    truncated = true;
                    break;
                }
                prefixes.add(cp);
                lastCp = cp;
                lastKey = cp;
                lastUpload = null;
            } else {
                if (out.size() + prefixes.size() == maxUploads) {
                    truncated = true;
                    break;
                }
                out.add(u);
                lastKey = u.key();
                lastUpload = u.uploadId();
            }
        }
        return new UploadsListing(out, prefixes, truncated, truncated ? lastKey : null, truncated ? lastUpload : null);
    }

    private static void discardParts(Connection c, UUID uploadId) throws SQLException {
        List<UUID> blobs = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_parts WHERE upload_id = ? RETURNING object_id")) {
            ps.setObject(1, uploadId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    blobs.add(rs.getObject(1, UUID.class));
                }
            }
        }
        markGarbage(c, blobs);
    }

    // ---- garbage collection ------------------------------------------------------------------------

    private void gcAll() {
        List<String> hosts = hosts();
        for (String h : hosts) {
            try {
                gcHost(h);
            } catch (RuntimeException e) {
                log.warn("s3wire: garbage collection on backend '{}' failed: {}", h, e.toString());
            }
        }
    }

    /** One collection pass on {@code host}; returns the number of blobs removed. Visible for tests. */
    int gcHost(String host) {
        int removed = 0;
        // 1. multipart uploads nobody completed within the configured age are aborted
        List<UUID> stale = withConn(host, c -> {
            List<UUID> ids = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT upload_id FROM warp_s3_multipart_uploads "
                    + "WHERE initiated < now() - (? * interval '1 second') LIMIT 100")) {
                ps.setLong(1, opt.gcMultipartAgeSeconds());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ids.add(rs.getObject(1, UUID.class));
                    }
                }
            }
            return ids;
        });
        for (UUID id : stale) {
            withTx(host, c -> {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_multipart_uploads WHERE upload_id = ?")) {
                    ps.setObject(1, id);
                    ps.executeUpdate();
                }
                discardParts(c, id);
                return null;
            });
        }
        // 2. blobs: abandoned uploads and blobs past their post-delete grace
        while (true) {
            List<UUID> batch = withConn(host, c -> {
                List<UUID> ids = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement("SELECT object_id FROM warp_s3_blobs WHERE "
                        + "(state = 'uploading' AND state_at < now() - (? * interval '1 second')) OR "
                        + "(state = 'garbage' AND state_at < now() - (? * interval '1 second')) LIMIT 50")) {
                    ps.setLong(1, opt.gcUploadingAgeSeconds());
                    ps.setLong(2, opt.gcGraceSeconds());
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getObject(1, UUID.class));
                        }
                    }
                }
                return ids;
            });
            if (batch.isEmpty()) {
                break;
            }
            for (UUID id : batch) {
                withTx(host, c -> {
                    // chunks first, blob row last: the row is the handle, so a crash between leaves it collectable
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_chunks WHERE object_id = ?")) {
                        ps.setObject(1, id);
                        ps.executeUpdate();
                    }
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_s3_blobs WHERE object_id = ?")) {
                        ps.setObject(1, id);
                        ps.executeUpdate();
                    }
                    return null;
                });
                removed++;
            }
        }
        if (removed > 0 || !stale.isEmpty()) {
            log.info("s3wire: collected {} blob(s) and aborted {} stale multipart upload(s) on backend '{}'", removed,
                    stale.size(), host);
        }
        return removed;
    }
}
