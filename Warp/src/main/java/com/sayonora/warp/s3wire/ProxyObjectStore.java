package com.sayonora.warp.s3wire;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Proxy mode: every client bucket is a key prefix inside ONE backend S3-compatible bucket (client
 * bucket {@code b}, key {@code k} lives at backend key {@code b/k}; CreateBucket writes the reserved
 * zero-byte marker {@code b/.s3wire-bucket}). This is the original s3wire behaviour, moved verbatim
 * behind the {@link ObjectStore} SPI. AWS SDK exceptions propagate to {@link S3WireServer}, which maps
 * them to S3 error responses.
 */
final class ProxyObjectStore implements ObjectStore {

    private static final Logger log = LoggerFactory.getLogger(ProxyObjectStore.class);
    private static final String MARKER = S3WireServer.MARKER;
    private static final java.util.regex.Pattern BUCKET_NAME = S3WireServer.BUCKET_NAME;

    private final S3Client s3;
    private final S3WireConfig config;
    private final Set<String> knownBuckets = ConcurrentHashMap.newKeySet();

    ProxyObjectStore(S3WireConfig config) {
        this.config = config;
        this.s3 = buildClient(config);
    }

    private static S3Client buildClient(S3WireConfig c) {
        AwsCredentialsProvider creds = c.accessKey() != null && c.secretKey() != null
                ? StaticCredentialsProvider.create(AwsBasicCredentials.create(c.accessKey(), c.secretKey()))
                : DefaultCredentialsProvider.create();
        S3ClientBuilder b = S3Client.builder().region(Region.of(c.region())).credentialsProvider(creds)
                // Only send checksums the API requires: keeps streamed uploads on plain
                // aws-chunked framing without an extra trailing-checksum pass.
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        if (c.endpoint() != null) {
            b.endpointOverride(URI.create(c.endpoint()));
        }
        if (c.pathStyle()) {
            b.forcePathStyle(true);
        }
        return b.build();
    }

    @Override
    public String label() {
        return "s3:" + config.backendBucket();
    }

    @Override
    public boolean reservesMarkerKey() {
        return true;
    }

    @Override
    public void close() {
        s3.close();
    }

    private static String backendKey(String bucket, String key) {
        return bucket + "/" + key;
    }

    // ---- buckets ---------------------------------------------------------------------------------

    @Override
    public boolean bucketExists(String bucket) {
        if (knownBuckets.contains(bucket)) {
            return true;
        }
        ListObjectsV2Response res = s3.listObjectsV2(b -> b.bucket(config.backendBucket()).prefix(bucket + "/").maxKeys(1));
        boolean exists = res.keyCount() != null && res.keyCount() > 0;
        if (exists) {
            knownBuckets.add(bucket);
        }
        return exists;
    }

    private void requireBucket(String bucket) {
        if (!bucketExists(bucket)) {
            throw new S3WireException(404, "NoSuchBucket", "The specified bucket does not exist");
        }
    }

    @Override
    public void createBucket(String bucket) {
        if (!BUCKET_NAME.matcher(bucket).matches()) {
            throw new S3WireException(400, "InvalidBucketName", "The specified bucket is not valid.");
        }
        if (bucketExists(bucket)) {
            throw new S3WireException(409, "BucketAlreadyOwnedByYou",
                    "Your previous request to create the named bucket succeeded and you already own it.");
        }
        s3.putObject(b -> b.bucket(config.backendBucket()).key(backendKey(bucket, MARKER)).contentLength(0L),
                RequestBody.empty());
        knownBuckets.add(bucket);
    }

    @Override
    public void deleteBucket(String bucket) {
        requireBucket(bucket);
        ListObjectsV2Response res = s3.listObjectsV2(b -> b.bucket(config.backendBucket()).prefix(bucket + "/").maxKeys(2));
        for (S3Object o : res.contents()) {
            if (!o.key().equals(backendKey(bucket, MARKER))) {
                throw new S3WireException(409, "BucketNotEmpty", "The bucket you tried to delete is not empty");
            }
        }
        s3.deleteObject(b -> b.bucket(config.backendBucket()).key(backendKey(bucket, MARKER)));
        knownBuckets.remove(bucket);
    }

    @Override
    public List<BucketEntry> listBuckets() {
        List<BucketEntry> buckets = new ArrayList<>();
        String token = null;
        do {
            String t = token;
            ListObjectsV2Response res = s3.listObjectsV2(b -> {
                b.bucket(config.backendBucket()).delimiter("/");
                if (t != null) {
                    b.continuationToken(t);
                }
            });
            for (CommonPrefix cp : res.commonPrefixes()) {
                String name = cp.prefix().substring(0, cp.prefix().length() - 1);
                Instant created = Instant.EPOCH;
                try {
                    created = s3.headObject(b -> b.bucket(config.backendBucket()).key(backendKey(name, MARKER))).lastModified();
                } catch (S3Exception ignored) {
                    // prefix without a marker (objects written another way): still listed
                }
                buckets.add(new BucketEntry(name, created));
            }
            token = Boolean.TRUE.equals(res.isTruncated()) ? res.nextContinuationToken() : null;
        } while (token != null);
        return buckets;
    }

    // ---- listing ---------------------------------------------------------------------------------

    @Override
    public ListResult list(ListRequest r) {
        String bucketPrefix = r.bucket() + "/";
        String prefix = r.prefix();
        String delimiter = r.delimiter();
        String token = r.token();
        String startAfter = r.startAfter();
        ListObjectsV2Response res = s3.listObjectsV2(b -> {
            b.bucket(config.backendBucket()).prefix(bucketPrefix + prefix).maxKeys(r.maxKeys());
            if (delimiter != null && !delimiter.isEmpty()) {
                b.delimiter(delimiter);
            }
            if (token != null) {
                b.continuationToken(token);
            }
            if (startAfter != null && !startAfter.isEmpty()) {
                b.startAfter(bucketPrefix + startAfter);
            }
        });
        if (res.contents().isEmpty() && res.commonPrefixes().isEmpty() && token == null && !bucketExists(r.bucket())) {
            throw new S3WireException(404, "NoSuchBucket", "The specified bucket does not exist");
        }
        List<S3Xml.ObjectEntry> objects = new ArrayList<>();
        String lastKey = null;
        for (S3Object o : res.contents()) {
            String k = o.key().substring(bucketPrefix.length());
            lastKey = k;
            if (!k.equals(MARKER)) {
                objects.add(new S3Xml.ObjectEntry(k, o.lastModified(), o.eTag(), o.size() == null ? 0 : o.size()));
            }
        }
        List<String> prefixes = new ArrayList<>();
        for (CommonPrefix cp : res.commonPrefixes()) {
            String p = cp.prefix().substring(bucketPrefix.length());
            prefixes.add(p);
            if (lastKey == null || p.compareTo(lastKey) > 0) {
                lastKey = p;
            }
        }
        boolean truncated = Boolean.TRUE.equals(res.isTruncated());
        return new ListResult(objects, prefixes, truncated, truncated ? res.nextContinuationToken() : null,
                truncated ? lastKey : null);
    }

    // ---- objects ---------------------------------------------------------------------------------

    @Override
    public ObjectInfo put(PutRequest r) {
        PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key())).contentLength(r.length());
        Attrs a = r.attrs();
        ifSet(a.contentType(), b::contentType);
        ifSet(a.cacheControl(), b::cacheControl);
        ifSet(a.contentDisposition(), b::contentDisposition);
        ifSet(a.contentEncoding(), b::contentEncoding);
        ifSet(a.contentLanguage(), b::contentLanguage);
        if (!a.metadata().isEmpty()) {
            b.metadata(a.metadata());
        }
        if (r.contentMd5() != null) {
            b.contentMD5(r.contentMd5());
        }
        PutObjectResponse res = s3.putObject(b.build(), RequestBody.fromInputStream(r.body(), r.length()));
        if (!r.payloadOk().getAsBoolean()) {
            try {
                s3.deleteObject(d -> d.bucket(config.backendBucket()).key(backendKey(r.bucket(), r.key())));
            } catch (RuntimeException e) {
                log.warn("s3wire: could not roll back object after payload-hash mismatch", e);
            }
            throw new S3WireException(400, "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.");
        }
        return new ObjectInfo(r.length(), res.eTag(), null, a);
    }

    private static void ifSet(String v, java.util.function.Consumer<String> c) {
        if (v != null) {
            c.accept(v);
        }
    }

    @Override
    public ObjectRead get(String bucket, String key, Conditions c, String range) {
        GetObjectRequest.Builder b = GetObjectRequest.builder().bucket(config.backendBucket())
                .key(backendKey(bucket, key));
        if (range != null) {
            b.range(range);
        }
        if (c.ifMatch() != null) {
            b.ifMatch(c.ifMatch());
        }
        if (c.ifNoneMatch() != null) {
            b.ifNoneMatch(c.ifNoneMatch());
        }
        ResponseInputStream<GetObjectResponse> in = s3.getObject(b.build());
        GetObjectResponse g = in.response();
        ObjectInfo info = new ObjectInfo(g.contentLength() == null ? 0 : g.contentLength(), g.eTag(), g.lastModified(),
                new Attrs(g.contentType(), g.cacheControl(), g.contentDisposition(), g.contentEncoding(),
                        g.contentLanguage(), null, g.metadata()));
        return new ObjectRead() {
            @Override
            public ObjectInfo info() {
                return info;
            }

            @Override
            public long contentLength() {
                return g.contentLength() == null ? -1 : g.contentLength();
            }

            @Override
            public String contentRange() {
                return g.contentRange();
            }

            @Override
            public void transferTo(OutputStream out) throws IOException {
                in.transferTo(out);
            }

            @Override
            public void close() throws IOException {
                in.close();
            }
        };
    }

    @Override
    public ObjectInfo head(String bucket, String key, Conditions c) {
        HeadObjectRequest.Builder b = HeadObjectRequest.builder().bucket(config.backendBucket())
                .key(backendKey(bucket, key));
        if (c.ifMatch() != null) {
            b.ifMatch(c.ifMatch());
        }
        if (c.ifNoneMatch() != null) {
            b.ifNoneMatch(c.ifNoneMatch());
        }
        HeadObjectResponse h = s3.headObject(b.build());
        return new ObjectInfo(h.contentLength() == null ? 0 : h.contentLength(), h.eTag(), h.lastModified(),
                new Attrs(h.contentType(), h.cacheControl(), h.contentDisposition(), h.contentEncoding(),
                        h.contentLanguage(), null, h.metadata()));
    }

    @Override
    public void delete(String bucket, String key) {
        s3.deleteObject(b -> b.bucket(config.backendBucket()).key(backendKey(bucket, key)));
    }

    @Override
    public DeleteManyResult deleteMany(String bucket, List<String> keys) {
        List<ObjectIdentifier> ids = new ArrayList<>();
        List<S3Xml.DeleteOutcome> errors = new ArrayList<>();
        for (String k : keys) {
            if (k.equals(MARKER)) {
                errors.add(new S3Xml.DeleteOutcome(k, "InvalidArgument", "Key is reserved by s3wire"));
            } else {
                ids.add(ObjectIdentifier.builder().key(backendKey(bucket, k)).build());
            }
        }
        List<String> deleted = new ArrayList<>();
        if (!ids.isEmpty()) {
            DeleteObjectsResponse res = s3.deleteObjects(DeleteObjectsRequest.builder().bucket(config.backendBucket())
                    .delete(Delete.builder().objects(ids).quiet(false).build()).build());
            int strip = bucket.length() + 1;
            res.deleted().forEach(x -> deleted.add(x.key().substring(strip)));
            res.errors().forEach(x -> errors.add(new S3Xml.DeleteOutcome(x.key().substring(strip), x.code(), x.message())));
        }
        return new DeleteManyResult(deleted, errors);
    }

    @Override
    public CopyResult copy(CopyRequest r) {
        requireBucket(r.srcBucket());
        CopyObjectRequest.Builder b = CopyObjectRequest.builder().sourceBucket(config.backendBucket())
                .sourceKey(backendKey(r.srcBucket(), r.srcKey())).destinationBucket(config.backendBucket())
                .destinationKey(backendKey(r.dstBucket(), r.dstKey()));
        if (r.replaceAttrs() != null) {
            Attrs a = r.replaceAttrs();
            b.metadataDirective(MetadataDirective.REPLACE);
            ifSet(a.contentType(), b::contentType);
            ifSet(a.cacheControl(), b::cacheControl);
            ifSet(a.contentDisposition(), b::contentDisposition);
            ifSet(a.contentEncoding(), b::contentEncoding);
            ifSet(a.contentLanguage(), b::contentLanguage);
            if (!a.metadata().isEmpty()) {
                b.metadata(a.metadata());
            }
        }
        CopyObjectResponse res = s3.copyObject(b.build());
        return new CopyResult(res.copyObjectResult().eTag(), res.copyObjectResult().lastModified());
    }

    // ---- multipart -------------------------------------------------------------------------------

    @Override
    public String createMultipart(String bucket, String key, Attrs a) {
        CreateMultipartUploadRequest.Builder b = CreateMultipartUploadRequest.builder().bucket(config.backendBucket())
                .key(backendKey(bucket, key));
        ifSet(a.contentType(), b::contentType);
        ifSet(a.cacheControl(), b::cacheControl);
        ifSet(a.contentDisposition(), b::contentDisposition);
        ifSet(a.contentEncoding(), b::contentEncoding);
        ifSet(a.contentLanguage(), b::contentLanguage);
        if (!a.metadata().isEmpty()) {
            b.metadata(a.metadata());
        }
        return s3.createMultipartUpload(b.build()).uploadId();
    }

    @Override
    public String uploadPart(UploadPartRequest r) {
        software.amazon.awssdk.services.s3.model.UploadPartRequest.Builder b =
                software.amazon.awssdk.services.s3.model.UploadPartRequest.builder().bucket(config.backendBucket())
                .key(backendKey(r.bucket(), r.key())).uploadId(r.uploadId()).partNumber(r.partNumber())
                .contentLength(r.length());
        if (r.contentMd5() != null) {
            b.contentMD5(r.contentMd5());
        }
        UploadPartResponse res = s3.uploadPart(b.build(), RequestBody.fromInputStream(r.body(), r.length()));
        if (!r.payloadOk().getAsBoolean()) {
            throw new S3WireException(400, "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.");
        }
        return res.eTag();
    }

    @Override
    public String completeMultipart(String bucket, String key, String uploadId, List<S3Xml.Part> parts) {
        List<CompletedPart> cps = new ArrayList<>();
        for (S3Xml.Part p : parts) {
            cps.add(CompletedPart.builder().partNumber(p.number()).eTag(p.eTag()).build());
        }
        var res = s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(config.backendBucket())
                .key(backendKey(bucket, key)).uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(cps).build()).build());
        knownBuckets.add(bucket);
        return res.eTag();
    }

    @Override
    public void abortMultipart(String bucket, String key, String uploadId) {
        s3.abortMultipartUpload(b -> b.bucket(config.backendBucket()).key(backendKey(bucket, key)).uploadId(uploadId));
    }
}
