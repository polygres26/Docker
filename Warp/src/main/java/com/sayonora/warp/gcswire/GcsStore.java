package com.sayonora.warp.gcswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.gcswire.GcsModel.Bucket;
import com.sayonora.warp.gcswire.GcsModel.HmacKey;
import com.sayonora.warp.gcswire.GcsModel.Obj;
import com.sayonora.warp.gcswire.GcsModel.Pre;
import com.sayonora.warp.gcswire.GcsModel.Seg;
import com.sayonora.warp.gcswire.GcsModel.Session;
import com.sayonora.warp.s3wire.ChunkMath;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.zip.CRC32C;

/**
 * GCS storage on Postgres. Buckets and HMAC keys live on the home host; every object generation, its data chunks and the
 * upload sessions live on the shard owning hash(bucket + "/" + name). An object's bytes are an ordered list of segments (each
 * a chunked data blob owned by exactly one object generation), so a resumable upload or an XML multipart upload becomes an
 * object without copying a byte, compose/copy duplicate blobs server-side, and every chunk read or write is its own short
 * borrow of a pooled connection: a slow client never pins one.
 */
final class GcsStore {

    private static final int GRACE_MARK = 0;

    final GcsShards shards;
    final GcsConfig cfg;

    GcsStore(GcsShards shards, GcsConfig cfg) {
        this.shards = shards;
        this.cfg = cfg;
    }

    static String shardKey(String bucket, String name) {
        return bucket + "/" + name;
    }

    String ownerHost(String bucket, String name) {
        return shards.owner(shardKey(bucket, name));
    }

    private static Instant inst(Timestamp t) {
        return t == null ? null : t.toInstant();
    }

    private static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }

    static int exec(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            return ps.executeUpdate();
        }
    }

    // ------------------------------------------------------------------------------------------ buckets

    private static Bucket readBucket(ResultSet rs) throws SQLException {
        Bucket b = new Bucket();
        b.name = rs.getString(1);
        b.project = rs.getString(2);
        b.created = inst(rs.getTimestamp(3));
        b.updated = inst(rs.getTimestamp(4));
        b.metageneration = rs.getLong(5);
        b.versioning = rs.getBoolean(6);
        b.doc = JsonParser.parseString(rs.getString(7)).getAsJsonObject();
        return b;
    }

    private static final String BUCKET_COLS = "name, project, created_at, updated_at, metageneration, versioning, doc::text";

    Bucket getBucket(String name) {
        return shards.conn(shards.home(), c -> selectBucket(c, name, false));
    }

    private static Bucket selectBucket(Connection c, String name, boolean forUpdate) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + BUCKET_COLS + " FROM warp_gcs_buckets WHERE name=?"
                + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readBucket(rs) : null;
            }
        }
    }

    /** @return false when the bucket already exists */
    boolean createBucket(Bucket b) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_gcs_buckets (name, project, created_at, "
                    + "updated_at, metageneration, versioning, doc) VALUES (?,?,?,?,?,?,?::jsonb) ON CONFLICT (name) DO NOTHING")) {
                ps.setString(1, b.name);
                ps.setString(2, b.project);
                ps.setTimestamp(3, ts(b.created));
                ps.setTimestamp(4, ts(b.updated));
                ps.setLong(5, b.metageneration);
                ps.setBoolean(6, b.versioning);
                ps.setString(7, b.doc.toString());
                return ps.executeUpdate() == 1;
            }
        });
    }

    /** Read-modify-write of a bucket row under a row lock; {@code fn} may throw a GcsException. */
    Bucket mutateBucket(String name, Function<Bucket, Bucket> fn) {
        return shards.tx(shards.home(), c -> {
            Bucket b = selectBucket(c, name, true);
            if (b == null) {
                throw GcsException.noSuchBucket();
            }
            Bucket n = fn.apply(b);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_gcs_buckets SET updated_at=?, metageneration=?, "
                    + "versioning=?, doc=?::jsonb WHERE name=?")) {
                ps.setTimestamp(1, ts(n.updated));
                ps.setLong(2, n.metageneration);
                ps.setBoolean(3, n.versioning);
                ps.setString(4, n.doc.toString());
                ps.setString(5, name);
                ps.executeUpdate();
            }
            return n;
        });
    }

    List<Bucket> listBuckets(String project, String prefix, String after, int limit) {
        return shards.conn(shards.home(), c -> {
            List<Bucket> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + BUCKET_COLS + " FROM warp_gcs_buckets WHERE "
                    + "(?::text IS NULL OR project=?) AND name LIKE ? AND name > ? ORDER BY name LIMIT ?")) {
                ps.setString(1, project);
                ps.setString(2, project);
                ps.setString(3, (prefix == null ? "" : prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")) + "%");
                ps.setString(4, after == null ? "" : after);
                ps.setInt(5, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(readBucket(rs));
                    }
                }
            }
            return out;
        });
    }

    /** {@code true} when any host holds an object (any generation) or open upload of the bucket. */
    boolean bucketHasObjects(String bucket) {
        for (String h : shards.hosts()) {
            boolean any = shards.conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM warp_gcs_objects WHERE bucket=? LIMIT 1")) {
                    ps.setString(1, bucket);
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next();
                    }
                }
            });
            if (any) {
                return true;
            }
        }
        return false;
    }

    /** Deletes a bucket row (after the emptiness check) and any leftover sessions / data of it on every host. */
    void deleteBucket(String bucket) {
        shards.conn(shards.home(), c -> exec(c, "DELETE FROM warp_gcs_buckets WHERE name=?", bucket));
        for (String h : shards.hosts()) {
            shards.tx(h, c -> {
                exec(c, "DELETE FROM warp_gcs_data WHERE data_id IN (SELECT data_id FROM warp_gcs_data_owner WHERE bucket=?)", bucket);
                exec(c, "DELETE FROM warp_gcs_data_owner WHERE bucket=?", bucket);
                exec(c, "DELETE FROM warp_gcs_sessions WHERE bucket=?", bucket);
                return null;
            });
        }
    }

    // ------------------------------------------------------------------------------------------ objects

    private static final String OBJ_COLS = "bucket, name, generation, metageneration, size, md5, crc32c, component_count, "
            + "content_type, content_encoding, content_disposition, cache_control, content_language, storage_class, custom_time, "
            + "created_at, updated_at, class_updated_at, deleted_at, metadata::text, doc::text, segments::text";

    private static Obj readObj(ResultSet rs) throws SQLException {
        Obj o = new Obj();
        o.bucket = rs.getString(1);
        o.name = rs.getString(2);
        o.generation = rs.getLong(3);
        o.metageneration = rs.getLong(4);
        o.size = rs.getLong(5);
        o.md5 = rs.getString(6);
        o.crc32c = rs.getString(7);
        int cc = rs.getInt(8);
        o.componentCount = rs.wasNull() ? null : cc;
        o.contentType = rs.getString(9);
        o.contentEncoding = rs.getString(10);
        o.contentDisposition = rs.getString(11);
        o.cacheControl = rs.getString(12);
        o.contentLanguage = rs.getString(13);
        o.storageClass = rs.getString(14);
        o.customTime = rs.getString(15);
        o.created = inst(rs.getTimestamp(16));
        o.updated = inst(rs.getTimestamp(17));
        o.classUpdated = inst(rs.getTimestamp(18));
        o.deleted = inst(rs.getTimestamp(19));
        o.metadata = new LinkedHashMap<>();
        JsonParser.parseString(rs.getString(20)).getAsJsonObject().entrySet()
                .forEach(e -> o.metadata.put(e.getKey(), e.getValue().getAsString()));
        o.doc = JsonParser.parseString(rs.getString(21)).getAsJsonObject();
        o.segments = GcsModel.segsFrom(rs.getString(22));
        return o;
    }

    private static Obj selectOne(Connection c, String bucket, String name, Long generation, boolean forUpdate) throws SQLException {
        String sql = "SELECT " + OBJ_COLS + " FROM warp_gcs_objects WHERE bucket=? AND name=? AND "
                + (generation == null ? "deleted_at IS NULL" : "generation=?") + (forUpdate ? " FOR UPDATE" : "");
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, bucket);
            ps.setString(2, name);
            if (generation != null) {
                ps.setLong(3, generation);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readObj(rs) : null;
            }
        }
    }

    /** The live object ({@code generation} null) or one specific generation (live or noncurrent); null when absent. */
    Obj find(String bucket, String name, Long generation) {
        String host = ownerHost(bucket, name);
        Obj o = shards.conn(host, c -> selectOne(c, bucket, name, generation, false));
        if (o == null && cfg.probeOtherShards && shards.hosts().size() > 1) {
            for (String h : shards.hosts()) {
                if (!h.equals(host)) {
                    o = shards.conn(h, c -> selectOne(c, bucket, name, generation, false));
                    if (o != null) {
                        break;
                    }
                }
            }
        }
        return o;
    }

    private static void insertObj(Connection c, Obj o) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_gcs_objects (" + OBJ_COLS.replace("metadata::text", "metadata")
                .replace("doc::text", "doc").replace("segments::text", "segments") + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                + "?::jsonb,?::jsonb,?::jsonb)")) {
            ps.setString(1, o.bucket);
            ps.setString(2, o.name);
            ps.setLong(3, o.generation);
            ps.setLong(4, o.metageneration);
            ps.setLong(5, o.size);
            ps.setString(6, o.md5);
            ps.setString(7, o.crc32c);
            if (o.componentCount == null) {
                ps.setNull(8, java.sql.Types.INTEGER);
            } else {
                ps.setInt(8, o.componentCount);
            }
            ps.setString(9, o.contentType);
            ps.setString(10, o.contentEncoding);
            ps.setString(11, o.contentDisposition);
            ps.setString(12, o.cacheControl);
            ps.setString(13, o.contentLanguage);
            ps.setString(14, o.storageClass);
            ps.setString(15, o.customTime);
            ps.setTimestamp(16, ts(o.created));
            ps.setTimestamp(17, ts(o.updated));
            ps.setTimestamp(18, ts(o.classUpdated));
            ps.setTimestamp(19, ts(o.deleted));
            JsonObject md = new JsonObject();
            o.metadata.forEach(md::addProperty);
            ps.setString(20, md.toString());
            ps.setString(21, o.doc.toString());
            ps.setString(22, GcsModel.segsJson(o.segments).toString());
            ps.executeUpdate();
        }
    }

    private static void updateObjMeta(Connection c, Obj o) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_gcs_objects SET metageneration=?, content_type=?, "
                + "content_encoding=?, content_disposition=?, cache_control=?, content_language=?, storage_class=?, custom_time=?, "
                + "updated_at=?, class_updated_at=?, metadata=?::jsonb, doc=?::jsonb WHERE bucket=? AND name=? AND generation=?")) {
            ps.setLong(1, o.metageneration);
            ps.setString(2, o.contentType);
            ps.setString(3, o.contentEncoding);
            ps.setString(4, o.contentDisposition);
            ps.setString(5, o.cacheControl);
            ps.setString(6, o.contentLanguage);
            ps.setString(7, o.storageClass);
            ps.setString(8, o.customTime);
            ps.setTimestamp(9, ts(o.updated));
            ps.setTimestamp(10, ts(o.classUpdated));
            JsonObject md = new JsonObject();
            o.metadata.forEach(md::addProperty);
            ps.setString(11, md.toString());
            ps.setString(12, o.doc.toString());
            ps.setString(13, o.bucket);
            ps.setString(14, o.name);
            ps.setLong(15, o.generation);
            ps.executeUpdate();
        }
    }

    private static long maxGeneration(Connection c, String bucket, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT coalesce(max(generation),0) FROM warp_gcs_objects WHERE bucket=? AND name=?")) {
            ps.setString(1, bucket);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /** Retention policy / hold enforcement for deleting or overwriting a live generation. */
    static void checkProtected(Bucket b, Obj live) {
        if (live == null) {
            return;
        }
        if (live.bool("temporaryHold") || live.bool("eventBasedHold")) {
            throw new GcsException(403, "forbidden", "AccessDenied", "Object is under active " + (live.bool("temporaryHold")
                    ? "Temporary" : "Event-Based") + " hold and cannot be deleted, overwritten or archived until hold is removed.");
        }
        JsonObject rp = b == null || !b.doc.has("retentionPolicy") ? null : b.doc.getAsJsonObject("retentionPolicy");
        if (rp != null && rp.has("retentionPeriod")) {
            Instant until = live.created.plusSeconds(rp.get("retentionPeriod").getAsLong());
            if (until.isAfter(Instant.now())) {
                throw new GcsException(403, "forbidden", "AccessDenied", "Object '" + live.bucket + "/" + live.name
                        + "' is subject to bucket's retention policy and cannot be deleted, overwritten or archived until "
                        + GcsJson.ts(until));
            }
        }
    }

    /**
     * Publishes a new generation: under a lock on the live row checks the preconditions, retires the old live generation
     * (noncurrent in a versioned bucket, gone otherwise), assigns the generation number and adopts the segments' data blobs.
     */
    Obj commit(Bucket b, Obj n, Pre pre) {
        String host = ownerHost(n.bucket, n.name);
        GcsException last = null;
        for (int attempt = 0; attempt < 4; attempt++) {
            try {
                return shards.tx(host, c -> {
                    Obj live = selectOne(c, n.bucket, n.name, null, true);
                    pre.check(live);
                    if (live != null) {
                        checkProtected(b, live);
                    }
                    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
                    long gen = Math.max(now.getEpochSecond() * 1_000_000L + now.getNano() / 1000, maxGeneration(c, n.bucket, n.name) + 1);
                    n.generation = gen;
                    n.metageneration = 1;
                    n.created = now;
                    n.updated = now;
                    n.classUpdated = now;
                    n.deleted = null;
                    if (live != null) {
                        retire(c, live, b != null && b.versioning, now);
                    }
                    insertObj(c, n);
                    adopt(c, n);
                    return n;
                });
            } catch (GcsException e) {
                if (!GcsShards.isUniqueViolation(e)) {
                    throw e;
                }
                last = e;
            }
        }
        throw last;
    }

    private static void retire(Connection c, Obj live, boolean versioned, Instant now) throws SQLException {
        if (versioned) {
            exec(c, "UPDATE warp_gcs_objects SET deleted_at=? WHERE bucket=? AND name=? AND generation=?", ts(now), live.bucket,
                    live.name, live.generation);
        } else {
            exec(c, "DELETE FROM warp_gcs_objects WHERE bucket=? AND name=? AND generation=?", live.bucket, live.name, live.generation);
            markGarbage(c, live.segments);
        }
    }

    private static void adopt(Connection c, Obj o) throws SQLException {
        for (Seg s : o.segments) {
            exec(c, "UPDATE warp_gcs_data_owner SET kind='C', generation=?, bucket=?, name=?, ref='' WHERE data_id=?::uuid",
                    o.generation, o.bucket, o.name, s.d());
        }
    }

    private static void markGarbage(Connection c, List<Seg> segs) throws SQLException {
        for (Seg s : segs) {
            exec(c, "UPDATE warp_gcs_data_owner SET kind='G', created_at=now() WHERE data_id=?::uuid", s.d());
        }
    }

    /** Deletes the live object (generation null) or one generation. @return the removed object */
    Obj delete(Bucket b, String bucket, String name, Long generation, Pre pre) {
        String host = ownerHost(bucket, name);
        return shards.tx(host, c -> {
            Obj live = selectOne(c, bucket, name, null, true);
            Obj target = generation == null ? live : selectOne(c, bucket, name, generation, true);
            if (target == null) {
                throw GcsException.noSuchObject(bucket, name);
            }
            pre.check(target);
            if (target.live()) {
                checkProtected(b, target);
            }
            if (generation == null && b != null && b.versioning) {
                exec(c, "UPDATE warp_gcs_objects SET deleted_at=now() WHERE bucket=? AND name=? AND generation=?", bucket, name,
                        target.generation);
            } else {
                exec(c, "DELETE FROM warp_gcs_objects WHERE bucket=? AND name=? AND generation=?", bucket, name, target.generation);
                markGarbage(c, target.segments);
            }
            return target;
        });
    }

    /** Read-modify-write of object metadata under a row lock; bumps metageneration. */
    Obj mutate(String bucket, String name, Long generation, Pre pre, Function<Obj, Obj> fn) {
        String host = ownerHost(bucket, name);
        return shards.tx(host, c -> {
            Obj o = selectOne(c, bucket, name, generation, true);
            if (o == null) {
                throw GcsException.noSuchObject(bucket, name);
            }
            pre.check(o);
            Obj n = fn.apply(o);
            n.metageneration = o.metageneration + 1;
            n.updated = Instant.now().truncatedTo(ChronoUnit.MICROS);
            updateObjMeta(c, n);
            return n;
        });
    }

    // ------------------------------------------------------------------------------------------ data

    record Ingested(String dataId, long size, String md5, long crc) {
    }

    /**
     * Streams up to {@code max} bytes of {@code in} into chunk rows of a new data blob ({@code kind} 'T' temporary, 'S' session
     * chunk, 'P' multipart part), owned by (bucket, name, ref). A body longer than {@code max} is an error unless {@code lenient}
     * (the excess is drained and dropped: the unaligned tail of a resumable chunk).
     */
    Ingested ingest(String host, InputStream in, long max, boolean lenient, String bucket, String name, String kind, String ref,
            int part) throws IOException {
        String dataId = UUID.randomUUID().toString();
        shards.conn(host, c -> exec(c, "INSERT INTO warp_gcs_data_owner (data_id, bucket, name, kind, ref, part) "
                + "VALUES (?::uuid,?,?,?,?,?)", dataId, bucket, name, "T", ref, part));
        MessageDigest md5 = GcsHash.md5();
        CRC32C crc = new CRC32C();
        long total = 0;
        int seq = 0;
        int chunk = cfg.chunkBytes;
        byte[] buf = new byte[(int) Math.max(1, max < 0 ? chunk : Math.min(max + 1, chunk))];
        try {
            boolean eof = false;
            while (!eof) {
                int n = 0;
                while (n < buf.length) {
                    int r = in.read(buf, n, buf.length - n);
                    if (r < 0) {
                        eof = true;
                        break;
                    }
                    n += r;
                }
                if (n == 0) {
                    break;
                }
                long take = n;
                if (max >= 0 && total + n > max) {
                    if (!lenient) {
                        throw GcsException.invalid("Upload exceeds the declared size.");
                    }
                    take = max - total;
                }
                if (take > 0) {
                    md5.update(buf, 0, (int) take);
                    crc.update(buf, 0, (int) take);
                    total += take;
                    final int fseq = seq++;
                    final byte[] data = take == buf.length ? buf : java.util.Arrays.copyOf(buf, (int) take);
                    shards.conn(host, c -> {
                        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_gcs_data (data_id, seq, data) VALUES (?::uuid,?,?)")) {
                            ps.setString(1, dataId);
                            ps.setInt(2, fseq);
                            ps.setBytes(3, data);
                            return ps.executeUpdate();
                        }
                    });
                }
                if (max >= 0 && total >= max && lenient) {
                    drain(in);
                    break;
                }
            }
        } catch (IOException | RuntimeException e) {
            dropData(host, List.of(dataId));
            throw e;
        }
        final long size = total;
        shards.conn(host, c -> exec(c, "UPDATE warp_gcs_data_owner SET size=?, kind=? WHERE data_id=?::uuid", size, kind, dataId));
        return new Ingested(dataId, size, Base64.getEncoder().encodeToString(md5.digest()), crc.getValue());
    }

    private static void drain(InputStream in) throws IOException {
        byte[] b = new byte[65536];
        while (in.read(b) >= 0) {
            // discard
        }
    }

    void dropData(String host, Collection<String> ids) {
        if (ids.isEmpty()) {
            return;
        }
        shards.tx(host, c -> {
            String[] a = ids.toArray(new String[0]);
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_gcs_data WHERE data_id = ANY (?::uuid[])")) {
                ps.setArray(1, c.createArrayOf("uuid", a));
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_gcs_data_owner WHERE data_id = ANY (?::uuid[])")) {
                ps.setArray(1, c.createArrayOf("uuid", a));
                ps.executeUpdate();
            }
            return null;
        });
    }

    byte[] readSlice(String host, String dataId, int seq, int offset, int length) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT substring(data FROM ? FOR ?) FROM warp_gcs_data WHERE data_id=?::uuid AND seq=?")) {
                ps.setInt(1, offset + 1);
                ps.setInt(2, length);
                ps.setString(3, dataId);
                ps.setInt(4, seq);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw GcsException.internal("object data chunk is missing");
                    }
                    return rs.getBytes(1);
                }
            }
        });
    }

    /** Streams bytes [start, end] (inclusive) of the segments stored on {@code host}. */
    void stream(String host, List<Seg> segments, long start, long end, OutputStream out) throws IOException {
        if (end < start) {
            return;
        }
        List<ChunkMath.Segment> segs = new ArrayList<>();
        for (Seg s : segments) {
            segs.add(new ChunkMath.Segment(s.d(), s.n(), s.c()));
        }
        for (ChunkMath.Slice sl : ChunkMath.slices(segs, start, end)) {
            out.write(readSlice(host, sl.blobId(), sl.seq(), sl.offset(), sl.length()));
        }
    }

    /** crc32c (as unsigned long) of the segments' bytes, computed by reading them back. */
    long crcOf(String host, List<Seg> segments) {
        CRC32C crc = new CRC32C();
        long total = segments.stream().mapToLong(Seg::n).sum();
        try {
            stream(host, segments, 0, total - 1, new OutputStream() {
                @Override
                public void write(int b) {
                    crc.update(b);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    crc.update(b, off, len);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return crc.getValue();
    }

    /** md5 (base64) and crc32c of the segments' bytes, computed by reading them back (finalising a resumable upload). */
    String[] hashesOf(String host, List<Seg> segments) {
        MessageDigest md5 = GcsHash.md5();
        CRC32C crc = new CRC32C();
        long total = segments.stream().mapToLong(Seg::n).sum();
        try {
            stream(host, segments, 0, total - 1, new OutputStream() {
                @Override
                public void write(int b) {
                    md5.update((byte) b);
                    crc.update(b);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    md5.update(b, off, len);
                    crc.update(b, off, len);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return new String[] {Base64.getEncoder().encodeToString(md5.digest()), GcsHash.crc32cB64(crc.getValue())};
    }

    /**
     * Copies segments (living on {@code srcHost}) into new data blobs on {@code dstHost}, owned (temporarily) by the target.
     * Same host: one INSERT ... SELECT per blob; other host: chunk rows are moved through this process, one at a time.
     */
    List<Seg> copySegments(String srcHost, List<Seg> segs, String dstHost, String bucket, String name) {
        List<Seg> out = new ArrayList<>();
        try {
            for (Seg s : segs) {
                String id = UUID.randomUUID().toString();
                shards.conn(dstHost, c -> exec(c, "INSERT INTO warp_gcs_data_owner (data_id, bucket, name, kind, size) "
                        + "VALUES (?::uuid,?,?,'T',?)", id, bucket, name, s.n()));
                if (srcHost.equals(dstHost)) {
                    shards.conn(dstHost, c -> exec(c, "INSERT INTO warp_gcs_data (data_id, seq, data) SELECT ?::uuid, seq, data "
                            + "FROM warp_gcs_data WHERE data_id=?::uuid", id, s.d()));
                } else {
                    int chunks = ChunkMath.chunkCount(s.n(), s.c());
                    for (int seq = 0; seq < chunks; seq++) {
                        int len = (int) Math.min(s.c(), s.n() - (long) seq * s.c());
                        byte[] data = readSlice(srcHost, s.d(), seq, 0, len);
                        final int fseq = seq;
                        shards.conn(dstHost, c -> {
                            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_gcs_data (data_id, seq, data) VALUES (?::uuid,?,?)")) {
                                ps.setString(1, id);
                                ps.setInt(2, fseq);
                                ps.setBytes(3, data);
                                return ps.executeUpdate();
                            }
                        });
                    }
                }
                out.add(new Seg(id, s.n(), s.c()));
            }
            return out;
        } catch (RuntimeException e) {
            dropData(dstHost, out.stream().map(Seg::d).toList());
            throw e;
        }
    }

    // ------------------------------------------------------------------------------------------ listing

    /** One listing entry: an object (generation) or a common prefix. */
    record Entry(boolean prefix, String key, Obj obj) {
        int compareTo(Entry o) {
            int c = key.compareTo(o.key);
            if (c != 0) {
                return c;
            }
            if (prefix != o.prefix) {
                return prefix ? 1 : -1;
            }
            return prefix ? 0 : Long.compare(obj.generation, o.obj.generation);
        }
    }

    record ListSpec(String bucket, String prefix, String delimiter, String startOffset, String endOffset, boolean versions,
            boolean includeTrailingDelimiter, java.util.regex.Pattern glob, String afterName, long afterGen) {
    }

    /** A lazily-advancing cursor over one shard's rows in (name, generation) order with delimiter roll-up. */
    final class Cursor {
        private final String host;
        private final ListSpec spec;
        private String posName;
        private long posGen;
        private final java.util.ArrayDeque<Entry> buf = new java.util.ArrayDeque<>();
        private boolean done;
        private String lastPrefix;

        Cursor(String host, ListSpec spec) {
            this.host = host;
            this.spec = spec;
            String start = spec.startOffset() == null ? "" : spec.startOffset();
            if (spec.prefix().compareTo(start) > 0) {
                start = spec.prefix();
            }
            posName = start;
            posGen = -1;
            if (spec.afterName() != null && (spec.afterName().compareTo(start) > 0
                    || spec.afterName().equals(start) && spec.afterGen() > -1)) {
                posName = spec.afterName();
                posGen = spec.afterGen();
            }
        }

        Entry peek() {
            while (buf.isEmpty() && !done) {
                fill();
            }
            return buf.peekFirst();
        }

        Entry next() {
            peek();
            return buf.pollFirst();
        }

        private void fill() {
            String end = spec.prefix().isEmpty() ? null : GcsMatch.prefixEnd(spec.prefix());
            if (spec.endOffset() != null && (end == null || spec.endOffset().compareTo(end) < 0)) {
                end = spec.endOffset();
            }
            final String fend = end;
            final String fromName = posName;
            final long fromGen = posGen;
            List<Obj> rows = shards.conn(host, c -> {
                StringBuilder sql = new StringBuilder("SELECT " + OBJ_COLS + " FROM warp_gcs_objects WHERE bucket=? "
                        + "AND (name, generation) > (?, ?)");
                if (fend != null) {
                    sql.append(" AND name < ?");
                }
                if (!spec.versions()) {
                    sql.append(" AND deleted_at IS NULL");
                }
                sql.append(" ORDER BY name, generation LIMIT 500");
                List<Obj> out = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                    ps.setString(1, spec.bucket());
                    ps.setString(2, fromName);
                    ps.setLong(3, fromGen);
                    if (fend != null) {
                        ps.setString(4, fend);
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(readObj(rs));
                        }
                    }
                }
                return out;
            });
            if (rows.size() < 500) {
                done = true;
            }
            for (Obj o : rows) {
                posName = o.name;
                posGen = o.generation;
                if (spec.glob() != null && !spec.glob().matcher(o.name).matches()) {
                    continue;
                }
                String roll = GcsMatch.rollup(o.name, spec.prefix(), spec.delimiter());
                if (roll == null) {
                    buf.add(new Entry(false, o.name, o));
                    continue;
                }
                if (spec.includeTrailingDelimiter() && o.name.equals(roll)) {
                    buf.add(new Entry(false, o.name, o));
                }
                if (!roll.equals(lastPrefix)) {
                    lastPrefix = roll;
                    buf.add(new Entry(true, roll, null));
                }
                // everything else under this prefix is skipped: jump past it (a later fill continues there)
                String jump = GcsMatch.prefixEnd(roll);
                posName = jump;
                posGen = -1;
                if (rows.get(rows.size() - 1) != o) {
                    // rows after o in this batch may still belong to the prefix or beyond; re-fetch from the jump
                    done = false;
                    return;
                }
                done = false;
                return;
            }
        }
    }

    record Page(List<Obj> objects, List<String> prefixes, String nextToken) {
    }

    /** Merged, paged listing across all shards. {@code token} = opaque continuation. */
    Page list(ListSpec spec, int max) {
        List<Cursor> cursors = new ArrayList<>();
        for (String h : shards.hosts()) {
            cursors.add(new Cursor(h, spec));
        }
        List<Obj> objs = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        Entry lastEmitted = null;
        int count = 0;
        while (true) {
            Cursor best = null;
            Entry be = null;
            for (Cursor c : cursors) {
                Entry e = c.peek();
                if (e != null && (be == null || e.compareTo(be) < 0)) {
                    be = e;
                    best = c;
                }
            }
            if (best == null) {
                return new Page(objs, prefixes, null);
            }
            if (be.prefix() && lastEmitted != null && lastEmitted.prefix() && lastEmitted.key().equals(be.key())) {
                best.next();
                continue;
            }
            if (count >= max) {
                return new Page(objs, prefixes, encodeToken(lastEmitted));
            }
            best.next();
            if (be.prefix()) {
                prefixes.add(be.key());
            } else {
                objs.add(be.obj());
            }
            lastEmitted = be;
            count++;
        }
    }

    static String encodeToken(Entry e) {
        if (e == null) {
            return null;
        }
        String s = e.prefix() ? "p\n" + e.key() : "o\n" + e.key() + "\n" + e.obj().generation;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** @return {name, generation} exclusive resume position, or throws invalid */
    static Object[] decodeToken(String token) {
        try {
            String s = new String(Base64.getUrlDecoder().decode(token), java.nio.charset.StandardCharsets.UTF_8);
            String[] p = s.split("\n", 3);
            if (p[0].equals("p")) {
                return new Object[] {GcsMatch.prefixEnd(p[1]), -1L};
            }
            return new Object[] {p[1], Long.parseLong(p[2])};
        } catch (RuntimeException e) {
            throw GcsException.invalid("Invalid page token.").at("parameter", "pageToken");
        }
    }

    // ------------------------------------------------------------------------------------------ sessions

    private static Session readSession(ResultSet rs) throws SQLException {
        Session s = new Session();
        s.id = rs.getString(1);
        s.kind = rs.getString(2);
        s.bucket = rs.getString(3);
        s.name = rs.getString(4);
        s.request = JsonParser.parseString(rs.getString(5)).getAsJsonObject();
        s.segments = GcsModel.segsFrom(rs.getString(6));
        s.persisted = rs.getLong(7);
        s.state = rs.getString(8);
        String r = rs.getString(9);
        s.result = r == null ? null : JsonParser.parseString(r).getAsJsonObject();
        return s;
    }

    private static final String SESS_COLS = "upload_id, kind, bucket, name, request::text, segments::text, persisted, state, result::text";

    Session createSession(String kind, String bucket, String name, JsonObject request) {
        Session s = new Session();
        s.id = UUID.randomUUID().toString().replace("-", "") + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        s.kind = kind;
        s.bucket = bucket;
        s.name = name;
        s.request = request;
        s.state = "open";
        shards.conn(ownerHost(bucket, name), c -> exec(c, "INSERT INTO warp_gcs_sessions (upload_id, kind, bucket, name, request, "
                + "state) VALUES (?,?,?,?,?::jsonb,'open')", s.id, kind, bucket, name, request.toString()));
        return s;
    }

    /** Looks a session up on the owner shard of (bucket, name), else (when the name is unknown) on every host. */
    Session findSession(String id, String bucket, String name) {
        if (bucket != null && name != null) {
            return shards.conn(ownerHost(bucket, name), c -> selectSession(c, id));
        }
        for (String h : shards.hosts()) {
            Session s = shards.conn(h, c -> selectSession(c, id));
            if (s != null) {
                return s;
            }
        }
        return null;
    }

    private static Session selectSession(Connection c, String id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + SESS_COLS + " FROM warp_gcs_sessions WHERE upload_id=?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readSession(rs) : null;
            }
        }
    }

    /** Appends a segment when the session still has {@code expectedPersisted} bytes. @return false on a lost race */
    boolean appendSegment(Session s, Seg seg, long expectedPersisted) {
        List<Seg> next = new ArrayList<>(s.segments);
        next.add(seg);
        long persisted = expectedPersisted + seg.n();
        return shards.tx(ownerHost(s.bucket, s.name), c -> {
            int n = exec(c, "UPDATE warp_gcs_sessions SET segments=?::jsonb, persisted=?, touched_at=now() WHERE upload_id=? AND "
                    + "persisted=? AND state='open'", GcsModel.segsJson(next).toString(), persisted, s.id, expectedPersisted);
            if (n == 1) {
                exec(c, "UPDATE warp_gcs_data_owner SET kind='S', ref=? WHERE data_id=?::uuid", s.id, seg.d());
            }
            return n == 1;
        });
    }

    void finishSession(Session s, JsonObject result) {
        shards.conn(ownerHost(s.bucket, s.name), c -> exec(c, "UPDATE warp_gcs_sessions SET state='done', result=?::jsonb, "
                + "segments='[]', touched_at=now() WHERE upload_id=?", result.toString(), s.id));
    }

    /** Cancels (drops) a session and every data blob it owns. */
    void cancelSession(Session s) {
        String host = ownerHost(s.bucket, s.name);
        shards.tx(host, c -> {
            exec(c, "DELETE FROM warp_gcs_data WHERE data_id IN (SELECT data_id FROM warp_gcs_data_owner WHERE ref=? AND kind IN ('S','P','T'))", s.id);
            exec(c, "DELETE FROM warp_gcs_data_owner WHERE ref=? AND kind IN ('S','P','T')", s.id);
            exec(c, "UPDATE warp_gcs_sessions SET state='cancelled', segments='[]', persisted=0 WHERE upload_id=?", s.id);
            return null;
        });
    }

    /** XML multipart parts of an upload: number -> (data blob, size, etag). */
    record Part(int number, String dataId, long size, String etag) {
    }

    /** Replaces (or adds) part {@code number} of a multipart session; returns nothing, the old blob is garbage. */
    void putPart(Session s, int number, Ingested ing, String etagHex) {
        String host = ownerHost(s.bucket, s.name);
        shards.tx(host, c -> {
            exec(c, "UPDATE warp_gcs_data_owner SET kind='G', created_at=now(), ref='' WHERE ref=? AND kind='P' AND part=?", s.id, number);
            exec(c, "UPDATE warp_gcs_data_owner SET kind='P', ref=?, part=?, etag=? WHERE data_id=?::uuid", s.id, number, etagHex, ing.dataId());
            return null;
        });
    }

    List<Part> parts(Session s) {
        return shards.conn(ownerHost(s.bucket, s.name), c -> {
            List<Part> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT part, data_id::text, size, etag FROM warp_gcs_data_owner "
                    + "WHERE ref=? AND kind='P' ORDER BY part")) {
                ps.setString(1, s.id);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Part(rs.getInt(1), rs.getString(2), rs.getLong(3), rs.getString(4)));
                    }
                }
            }
            return out;
        });
    }

    /** Open multipart sessions of a bucket (XML "?uploads"). */
    List<Session> listSessions(String bucket, String kind) {
        List<Session> out = new ArrayList<>();
        for (String h : shards.hosts()) {
            out.addAll(shards.conn(h, c -> {
                List<Session> l = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement("SELECT " + SESS_COLS + " FROM warp_gcs_sessions WHERE bucket=? "
                        + "AND kind=? AND state='open' ORDER BY name, created_at")) {
                    ps.setString(1, bucket);
                    ps.setString(2, kind);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            l.add(readSession(rs));
                        }
                    }
                }
                return l;
            }));
        }
        out.sort((a, b) -> a.name.compareTo(b.name));
        return out;
    }

    // ------------------------------------------------------------------------------------------ HMAC keys

    void putHmac(HmacKey k) {
        shards.conn(shards.home(), c -> exec(c, "INSERT INTO warp_gcs_hmac (access_id, secret, project, sa_email, state, created_at, "
                + "updated_at) VALUES (?,?,?,?,?,?,?)", k.accessId(), k.secret(), k.project(), k.saEmail(), k.state(), ts(k.created()),
                ts(k.updated())));
    }

    private static HmacKey readHmac(ResultSet rs) throws SQLException {
        return new HmacKey(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                inst(rs.getTimestamp(6)), inst(rs.getTimestamp(7)));
    }

    HmacKey getHmac(String accessId) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT access_id, secret, project, sa_email, state, created_at, "
                    + "updated_at FROM warp_gcs_hmac WHERE access_id=?")) {
                ps.setString(1, accessId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? readHmac(rs) : null;
                }
            }
        });
    }

    List<HmacKey> listHmac(String project, String saEmail) {
        return shards.conn(shards.home(), c -> {
            List<HmacKey> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT access_id, secret, project, sa_email, state, created_at, "
                    + "updated_at FROM warp_gcs_hmac WHERE project=? AND (?::text IS NULL OR sa_email=?) ORDER BY access_id")) {
                ps.setString(1, project);
                ps.setString(2, saEmail);
                ps.setString(3, saEmail);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(readHmac(rs));
                    }
                }
            }
            return out;
        });
    }

    boolean setHmacState(String accessId, String state) {
        return shards.conn(shards.home(), c -> exec(c, "UPDATE warp_gcs_hmac SET state=?, updated_at=now() WHERE access_id=?", state,
                accessId) == 1);
    }

    boolean deleteHmac(String accessId) {
        return shards.conn(shards.home(), c -> exec(c, "DELETE FROM warp_gcs_hmac WHERE access_id=?", accessId) == 1);
    }

    // ------------------------------------------------------------------------------------------ housekeeping

    /** Drops temporary blobs and garbage past the grace period, and abandoned sessions past their TTL, on every shard. */
    long sweep() {
        long n = 0;
        for (String h : shards.hosts()) {
            n += shards.tx(h, c -> {
                exec(c, "DELETE FROM warp_gcs_data WHERE data_id IN (SELECT data_id FROM warp_gcs_data_owner WHERE (kind='G' AND "
                        + "created_at < now() - make_interval(secs => ?)) OR (kind='T' AND created_at < now() - make_interval(secs => 3600)))",
                        (double) cfg.gcGraceSeconds);
                long r = exec(c, "DELETE FROM warp_gcs_data_owner WHERE (kind='G' AND created_at < now() - make_interval(secs => ?)) "
                        + "OR (kind='T' AND created_at < now() - make_interval(secs => 3600))", (double) cfg.gcGraceSeconds);
                // abandoned sessions and their blobs
                exec(c, "DELETE FROM warp_gcs_data WHERE data_id IN (SELECT data_id FROM warp_gcs_data_owner WHERE kind IN ('S','P') "
                        + "AND ref IN (SELECT upload_id FROM warp_gcs_sessions WHERE touched_at < now() - make_interval(secs => ?)))",
                        (double) cfg.sessionTtlSeconds);
                exec(c, "DELETE FROM warp_gcs_data_owner WHERE kind IN ('S','P') AND ref IN (SELECT upload_id FROM warp_gcs_sessions "
                        + "WHERE touched_at < now() - make_interval(secs => ?))", (double) cfg.sessionTtlSeconds);
                exec(c, "DELETE FROM warp_gcs_sessions WHERE touched_at < now() - make_interval(secs => ?)", (double) cfg.sessionTtlSeconds);
                return r;
            });
        }
        return n;
    }

    /** All object rows of a bucket on every host (tests / stats). */
    long countObjects(String bucket) {
        long n = 0;
        for (String h : shards.hosts()) {
            n += shards.conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_gcs_objects WHERE bucket=?")) {
                    ps.setString(1, bucket);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        return rs.getLong(1);
                    }
                }
            });
        }
        return n;
    }

    static JsonArray unused() {
        return new JsonArray();
    }
}
