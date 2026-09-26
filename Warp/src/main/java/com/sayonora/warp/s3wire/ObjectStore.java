package com.sayonora.warp.s3wire;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Storage SPI behind {@link S3WireServer}: exactly the operations s3wire serves, with all HTTP
 * details (routing, SigV4, headers, XML) kept in the server. Two implementations:
 * {@link ProxyObjectStore} (one external S3-compatible bucket through the AWS SDK) and
 * {@link PostgresObjectStore} (objects stored as chunked bytea rows, sharded across the Postgres
 * backends of a backend set that enabled the {@code s3} store).
 *
 * <p>Failures are reported as {@link S3WireException} (an S3 XML error). The proxy implementation
 * additionally lets AWS SDK exceptions propagate, which the server maps as before.
 */
interface ObjectStore {

    /** Backend label used for metrics. */
    String label();

    /** {@code true} when the key {@link S3WireServer#MARKER} is an internal bucket marker (proxy mode). */
    boolean reservesMarkerKey();

    List<BucketEntry> listBuckets();

    boolean bucketExists(String bucket);

    /** @throws S3WireException 409 BucketAlreadyOwnedByYou */
    void createBucket(String bucket);

    /** @throws S3WireException 409 BucketNotEmpty */
    void deleteBucket(String bucket);

    ObjectInfo put(PutRequest request) throws IOException;

    /** @param range raw {@code Range} header or null */
    ObjectRead get(String bucket, String key, Conditions conditions, String range) throws IOException;

    ObjectInfo head(String bucket, String key, Conditions conditions);

    void delete(String bucket, String key);

    DeleteManyResult deleteMany(String bucket, List<String> keys);

    CopyResult copy(CopyRequest request) throws IOException;

    ListResult list(ListRequest request);

    String createMultipart(String bucket, String key, Attrs attrs);

    /** @return the part's ETag (quoted) */
    String uploadPart(UploadPartRequest request) throws IOException;

    /** @return the final object's ETag (quoted) */
    String completeMultipart(String bucket, String key, String uploadId, List<S3Xml.Part> parts);

    void abortMultipart(String bucket, String key, String uploadId);

    default void close() {
    }

    // ---- value types ----------------------------------------------------------------------------

    record BucketEntry(String name, Instant created) {
    }

    /** Object properties that are stored and returned verbatim. */
    record Attrs(String contentType, String cacheControl, String contentDisposition, String contentEncoding,
            String contentLanguage, String expires, Map<String, String> metadata) {
        static final Attrs EMPTY = new Attrs(null, null, null, null, null, null, Map.of());
    }

    record ObjectInfo(long size, String eTag, Instant lastModified, Attrs attrs) {
    }

    /** Conditional request headers (raw values, null when absent). */
    record Conditions(String ifMatch, String ifNoneMatch, String ifModifiedSince, String ifUnmodifiedSince) {
        static final Conditions NONE = new Conditions(null, null, null, null);
    }

    /** A readable object (or byte range of one). {@link #transferTo} streams it; close releases resources. */
    interface ObjectRead extends Closeable {
        ObjectInfo info();

        /** Bytes that {@link #transferTo} will write. */
        long contentLength();

        /** {@code bytes a-b/total} for a partial read, else null. */
        String contentRange();

        void transferTo(OutputStream out) throws IOException;
    }

    /**
     * @param payloadOk called after the whole body was consumed and before the object becomes visible;
     *                  {@code false} = the SigV4 payload hash did not match
     * @param contentMd5 base64 Content-MD5 header or null
     */
    record PutRequest(String bucket, String key, Attrs attrs, InputStream body, long length, String contentMd5,
            BooleanSupplier payloadOk) {
    }

    record UploadPartRequest(String bucket, String key, String uploadId, int partNumber, InputStream body, long length,
            String contentMd5, BooleanSupplier payloadOk) {
    }

    /** @param replaceAttrs non-null = {@code x-amz-metadata-directive: REPLACE} with these attributes */
    record CopyRequest(String srcBucket, String srcKey, String dstBucket, String dstKey, Attrs replaceAttrs) {
    }

    record CopyResult(String eTag, Instant lastModified) {
    }

    record DeleteManyResult(List<String> deleted, List<S3Xml.DeleteOutcome> errors) {
    }

    /**
     * @param token   v2 continuation token
     * @param startAfter v2 start-after or v1 marker
     */
    record ListRequest(String bucket, String prefix, String delimiter, int maxKeys, String token, String startAfter,
            boolean v2) {
    }

    /** @param lastKey last key/common prefix returned (v1 NextMarker) */
    record ListResult(List<S3Xml.ObjectEntry> objects, List<String> commonPrefixes, boolean truncated,
            String nextToken, String lastKey) {
    }
}
