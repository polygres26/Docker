package com.sayonora.wire.s3wire;

import com.google.gson.JsonObject;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Value types of the rich (Postgres-mode) S3 API: everything {@link PostgresObjectStore} accepts and
 * returns. Kept apart from {@link ObjectStore} (the small SPI proxy mode implements) so proxy mode is untouched.
 */
final class S3Model {

    private S3Model() {
    }

    /** Bucket row + cached defaults. versioning is null (never enabled), "Enabled" or "Suspended". */
    record BucketInfo(String name, Instant created, String region, String versioning, boolean objectLock,
            String ownership, String encryptionAlgo, String encryptionKmsKey) {
        boolean versioned() {
            return versioning != null;
        }
    }

    /** One stored part of a multipart object (for GetObjectAttributes / ?partNumber=). */
    record PartMeta(int number, long size, Map<String, String> checksums) {
    }

    /**
     * An object (or delete marker) version as stored.
     *
     * @param versionId null = bucket never versioned; "null" = the null version; else a generated id
     * @param extra     tags / acl / sse / storage class / lock JSON, never null
     */
    record Meta(String bucket, String key, String versionId, boolean deleteMarker, boolean latest, long size,
            String eTag, Instant lastModified, ObjectStore.Attrs attrs, Map<String, String> checksums,
            String checksumType, JsonObject extra, List<PartMeta> parts) {
        String tagHeaderCount() {
            JsonObject t = extra.has("tags") ? extra.getAsJsonObject("tags") : null;
            return t == null || t.size() == 0 ? null : String.valueOf(t.size());
        }

        String str(String k) {
            return extra.has(k) && !extra.get(k).isJsonNull() ? extra.get(k).getAsString() : null;
        }

        boolean multipart() {
            return eTag != null && eTag.contains("-") && !parts.isEmpty();
        }
    }

    /** {@code claimed} is evaluated after the body was fully read (trailing checksums arrive after it). */
    record PutIn(String bucket, String key, ObjectStore.Attrs attrs, JsonObject extra, InputStream body, long length,
            String contentMd5, BooleanSupplier payloadOk, java.util.Set<String> algos,
            Supplier<Map<String, String>> claimed, String ifNoneMatch, String ifMatch, BucketInfo bucket0) {
    }

    /** Read request. partNumber 0 = none. */
    record GetIn(String bucket, String key, String versionId, int partNumber, ObjectStore.Conditions conditions,
            String range) {
    }

    interface Read extends Closeable {
        Meta meta();

        long contentLength();

        String contentRange();

        void transferTo(OutputStream out) throws IOException;
    }

    record ObjId(String key, String versionId) {
    }

    /** Result of a single delete. deleteMarker = a marker was created (or the deleted version was a marker). */
    record DeleteOut(String versionId, boolean deleteMarker, boolean existed) {
    }

    record DeleteManyOut(List<Item> results) {
        record Item(String key, String requestedVersion, String versionId, boolean deleteMarker, String errCode,
                String errMessage) {
        }
    }

    /** algorithm null = default for the source */
    record CopyIn(String srcBucket, String srcKey, String srcVersionId, String dstBucket, String dstKey,
            ObjectStore.Attrs replaceAttrs, JsonObject extra, boolean replaceTags, boolean excludeAnnotations,
            ObjectStore.Conditions srcConditions,
            java.util.Set<String> algos, BucketInfo dstBucketInfo) {
    }

    record CopyOut(Meta meta, String srcVersionId) {
    }

    record UploadPartIn(String bucket, String key, String uploadId, int partNumber, InputStream body, long length,
            String contentMd5, BooleanSupplier payloadOk, Supplier<Map<String, String>> claimed) {
    }

    record PartOut(String eTag, Map<String, String> checksums) {
    }

    record UploadPartCopyIn(String bucket, String key, String uploadId, int partNumber, String srcBucket,
            String srcKey, String srcVersionId, String range, ObjectStore.Conditions srcConditions) {
    }

    record PartCopyOut(String eTag, Instant lastModified, Map<String, String> checksums, String srcVersionId) {
    }

    /** Completed part as sent by the client, with the checksums it claims. */
    record CompletePart(int number, String eTag, Map<String, String> checksums) {
    }

    record CompleteOut(String eTag, String versionId, Map<String, String> checksums, String checksumType) {
    }

    record UploadInfo(String uploadId, String key, Instant initiated, ObjectStore.Attrs attrs, String checksumAlgo,
            String checksumType, JsonObject extra) {
    }

    record PartListing(UploadInfo upload, List<PartRow> parts, boolean truncated, int nextMarker) {
    }

    record PartRow(int number, Instant modified, String eTag, long size, Map<String, String> checksums) {
    }

    record UploadsListing(List<UploadInfo> uploads, List<String> commonPrefixes, boolean truncated, String nextKeyMarker,
            String nextUploadIdMarker) {
    }

    record VersionEntry(String key, String versionId, boolean latest, boolean deleteMarker, Instant lastModified,
            String eTag, long size, String storageClass, java.util.List<String> checksumAlgos, String checksumType) {
    }

    record VersionsListing(List<VersionEntry> entries, List<String> commonPrefixes, boolean truncated,
            String nextKeyMarker, String nextVersionMarker) {
    }

    record Annotation(String name, byte[] payload, long size, String eTag, Instant modified,
            Map<String, String> checksums, String checksumType, String contentType, String versionId) {
    }
}
