package com.sayonora.wire.s3wire;

/**
 * Tunables of the Postgres-backed object store, read from the environment.
 *
 * <ul>
 *   <li>{@code WARP_S3WIRE_CHUNK_BYTES} -- chunk (bytea row) size, default 4 MiB, clamped to 64 KiB..64 MiB.
 *       Each object records the chunk size it was written with, so changing it never breaks old objects.</li>
 *   <li>{@code WARP_S3WIRE_MAX_OBJECT_BYTES} -- largest single PUT / part, default 5 GiB (S3's limit).</li>
 *   <li>{@code WARP_S3WIRE_MAX_MULTIPART_BYTES} -- largest completed multipart object, default 50 GiB.</li>
 *   <li>{@code WARP_S3WIRE_GC_INTERVAL_SECONDS} -- background collection period, default 60 (0 = off).</li>
 *   <li>{@code WARP_S3WIRE_GC_UPLOADING_AGE_SECONDS} -- an unfinished upload (blob still 'uploading') that has
 *       written nothing for this long is discarded, default 3600.</li>
 *   <li>{@code WARP_S3WIRE_GC_GRACE_SECONDS} -- overwritten/deleted objects keep their chunks this long so
 *       readers already streaming the old version finish, default 600.</li>
 *   <li>{@code WARP_S3WIRE_GC_MULTIPART_AGE_SECONDS} -- an initiated multipart upload not completed within
 *       this age is aborted, default 604800 (7 days).</li>
 *   <li>{@code WARP_S3WIRE_BUCKET_CACHE_MILLIS} -- how long a positive bucket-exists answer is cached per
 *       Warp process, default 2000 (0 = never cache).</li>
 *   <li>{@code WARP_S3WIRE_PROBE_OTHER_SHARDS} -- on a GET/HEAD/copy-source miss also look on the other hosts
 *       (finds data written before a topology change), default false.</li>
 * </ul>
 */
public record S3StoreOptions(int chunkBytes, long maxObjectBytes, long maxMultipartBytes, long gcIntervalSeconds,
        long gcUploadingAgeSeconds, long gcGraceSeconds, long gcMultipartAgeSeconds, long bucketCacheMillis,
        boolean probeOtherShards) {

    public static final int MIN_CHUNK = 64 * 1024;
    public static final int MAX_CHUNK = 64 * 1024 * 1024;

    public static S3StoreOptions defaults() {
        return new S3StoreOptions(4 * 1024 * 1024, 5L << 30, 50L << 30, 60, 3600, 600, 7 * 86400L, 2000, false);
    }

    public static S3StoreOptions fromEnv() {
        S3StoreOptions d = defaults();
        long chunk = num("WARP_S3WIRE_CHUNK_BYTES", d.chunkBytes());
        return new S3StoreOptions((int) Math.max(MIN_CHUNK, Math.min(MAX_CHUNK, chunk)),
                num("WARP_S3WIRE_MAX_OBJECT_BYTES", d.maxObjectBytes()),
                num("WARP_S3WIRE_MAX_MULTIPART_BYTES", d.maxMultipartBytes()),
                num("WARP_S3WIRE_GC_INTERVAL_SECONDS", d.gcIntervalSeconds()),
                num("WARP_S3WIRE_GC_UPLOADING_AGE_SECONDS", d.gcUploadingAgeSeconds()),
                num("WARP_S3WIRE_GC_GRACE_SECONDS", d.gcGraceSeconds()),
                num("WARP_S3WIRE_GC_MULTIPART_AGE_SECONDS", d.gcMultipartAgeSeconds()),
                num("WARP_S3WIRE_BUCKET_CACHE_MILLIS", d.bucketCacheMillis()),
                "true".equalsIgnoreCase(System.getenv("WARP_S3WIRE_PROBE_OTHER_SHARDS")));
    }

    private static long num(String name, long dflt) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            return dflt;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be an integer, got \"" + v + "\"");
        }
    }
}
