# Floci java / s3 vs warp-baseline (2026-09-25 14:39)

endpoint `http://localhost:55602`, runner rc=1, 7.6s

| status | count |
|---|---|
| pass | 30 |
| fail | 23 |
| error | 22 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.S3Test | 21 | 6 | 7 | 0 |
| com.floci.test.S3AnnotationsTest | 2 | 6 | 3 | 0 |
| com.floci.test.S3MultipartChecksumTest | 0 | 4 | 2 | 0 |
| com.floci.test.S3VirtualHostStyleTest | 2 | 5 | 3 | 0 |
| com.floci.test.S3FeaturesTest | 5 | 2 | 7 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `com.floci.test.S3Test::getBucketLocationEuCentral1` -- expected: eu-central-1
- [a] `com.floci.test.S3Test::putObjectTagging` -- s3wire does not implement this operation (PUT [tagging]) (Service: S3, Status Code: 501, Request ID: 7DC866E937B648DE) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3Test::getObjectTagging` -- s3wire does not implement this operation (GET [tagging]) (Service: S3, Status Code: 501, Request ID: EF3003A950B349CF) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3Test::deleteObjectTagging` -- s3wire does not implement this operation (DELETE [tagging]) (Service: S3, Status Code: 501, Request ID: 8A1EEEED5B4544F4) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3Test::putBucketTagging` -- s3wire does not implement this operation (PUT [tagging]) (Service: S3, Status Code: 501, Request ID: 6FDA3AD704F04549) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3Test::getBucketTagging` -- s3wire does not implement this operation (GET [tagging]) (Service: S3, Status Code: 501, Request ID: CAF24D53A8C14E4F) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3Test::deleteBucketTagging` -- s3wire does not implement this operation (DELETE [tagging]) (Service: S3, Status Code: 501, Request ID: 3C26077B914247B5) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3Test::putObjectPersistsInlineTaggingHeader` -- s3wire does not implement this operation (GET [tagging]) (Service: S3, Status Code: 501, Request ID: 0B1CE3FA9419456B) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3Test::putObjectInlineTaggingRejectsMalformedPair` -- Expecting code to raise a throwable.
- [a] `com.floci.test.S3Test::putObjectInlineTaggingRejectsEmptyKey` -- Expecting code to raise a throwable.
- [a] `com.floci.test.S3Test::putObjectInlineTaggingRejectsDuplicateKey` -- Expecting code to raise a throwable.
- [a] `com.floci.test.S3Test::putObjectInlineTaggingRejectsTooManyTags` -- Expecting code to raise a throwable.
- [a] `com.floci.test.S3Test::putObjectInlineTaggingRejectsOversizedHeader` -- Multiple Failures (1 failure)
- [b] `com.floci.test.S3AnnotationsTest::putObjectAnnotation` -- expected: "docs/annotated.txt"
- [b] `com.floci.test.S3AnnotationsTest::getObjectAnnotationWithChecksumMode` -- Expecting not blank but was: null
- [a] `com.floci.test.S3AnnotationsTest::listObjectAnnotations` -- Could not parse XML response. (SDK Attempt Count: 1)
- [b] `com.floci.test.S3AnnotationsTest::putObjectAnnotationWithChecksum` -- Expecting not blank but was: null
- [b] `com.floci.test.S3AnnotationsTest::getMissingAnnotationThrows` -- Expecting code to raise a throwable.
- [b] `com.floci.test.S3AnnotationsTest::deleteObjectAnnotationIsIdempotent` -- Expecting actual throwable to be an instance of:
- [b] `com.floci.test.S3AnnotationsTest::copyObjectExcludeDirectiveSkipsAnnotations` -- The specified key does not exist. (Service: S3, Status Code: 404, Request ID: 8ADD1D9A5C5045B5) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3AnnotationsTest::enableVersioning` -- s3wire does not implement this operation (PUT [versioning]) (Service: S3, Status Code: 501, Request ID: C1032D298D9F4C06) (SDK Attempt Count: 1)
- [b] `com.floci.test.S3AnnotationsTest::annotationAttachesToSpecificVersion` -- Expecting not blank but was: null
- [a] `com.floci.test.S3MultipartChecksumTest::sha256MultipartUploadReturnsCompositeChecksum` -- expected: SHA256
- [b] `com.floci.test.S3MultipartChecksumTest::completeWithoutPartChecksumsIsRejected` -- Expecting code to raise a throwable.
- [b] `com.floci.test.S3MultipartChecksumTest::crc32FullObjectUpload` -- expected: FULL_OBJECT
- [a] `com.floci.test.S3MultipartChecksumTest::copyOfMultipartObjectIsASingleFullObject` -- (Service: S3, Status Code: 404, Request ID: B4AB55B34F9C4483) (SDK Attempt Count: 1) (Service: S3, Status Code: 404, Request ID: B4AB55B34F9C4483)
- [a] `com.floci.test.S3MultipartChecksumTest::transferManagerSha256UploadReportsCompositeChecksum` -- s3wire does not implement this operation (GET [attributes]) (Service: S3, Status Code: 501, Request ID: F81FA8BB6E63467C) (SDK Attempt Count: 1)
- [b] `com.floci.test.S3MultipartChecksumTest::transferManagerDefaultChecksum` -- expected: FULL_OBJECT
- [b] `com.floci.test.S3VirtualHostStyleTest::createBucket` -- The specified method is not allowed against this resource. (Service: S3, Status Code: 405, Request ID: DF0371F0430D4A40) (SDK Attempt Count: 1)
- [b] `com.floci.test.S3VirtualHostStyleTest::headBucket` -- Method Not Allowed (Service: S3, Status Code: 405, Request ID: E5FBCFA0A0E04B18)
- [b] `com.floci.test.S3VirtualHostStyleTest::headBucketNonExistent` -- Multiple Failures (2 failures)
- [b] `com.floci.test.S3VirtualHostStyleTest::headObject` -- expected: 32L
- [b] `com.floci.test.S3VirtualHostStyleTest::getObject` -- expected:
- [b] `com.floci.test.S3VirtualHostStyleTest::listObjectsV2` -- Expecting any elements of:
- [b] `com.floci.test.S3VirtualHostStyleTest::copyObject` -- Expecting actual not to be null
- [b] `com.floci.test.S3VirtualHostStyleTest::deleteBucket` -- The specified method is not allowed against this resource. (Service: S3, Status Code: 405, Request ID: 7A9502AF51C24AD5) (SDK Attempt Count: 1)
- [b] `com.floci.test.S3FeaturesTest::listObjectVersionsPaginatorNonVersionedBucketDoesNotNpe` -- Expecting code not to raise a throwable but caught
- [a] `com.floci.test.S3FeaturesTest::listObjectVersionsPaginatorVersionedBucketReturnsTruncatedFlag` -- s3wire does not implement this operation (PUT [versioning]) (Service: S3, Status Code: 501, Request ID: 02DF757664EE4E7D) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3FeaturesTest::listObjectVersionsPaginatorPaginates` -- s3wire does not implement this operation (GET [versions, max-keys]) (Service: S3, Status Code: 501, Request ID: 9CC26A4C0C0D4657) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3FeaturesTest::listObjectVersionsNonVersionedBucketReturnsObjects` -- s3wire does not implement this operation (GET [versions]) (Service: S3, Status Code: 501, Request ID: D5F1D12BD2E24A01) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3FeaturesTest::listObjectVersionsPreVersioningObjectsAppearsWithNullVersionId` -- s3wire does not implement this operation (GET [versions]) (Service: S3, Status Code: 501, Request ID: 17EF0DB457E446AC) (SDK Attempt Count: 1)
- [b] `com.floci.test.S3FeaturesTest::putPublicAccessBlockSucceeds` -- Expecting code not to raise a throwable but caught
- [a] `com.floci.test.S3FeaturesTest::getPublicAccessBlockReturnsConfig` -- s3wire does not implement this operation (GET [publicAccessBlock]) (Service: S3, Status Code: 501, Request ID: 189C5F4626D0421C) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3FeaturesTest::putPublicAccessBlockCanBeUpdated` -- s3wire does not implement this operation (PUT [publicAccessBlock]) (Service: S3, Status Code: 501, Request ID: 1CEFD1E1449849E9) (SDK Attempt Count: 1)
- [a] `com.floci.test.S3FeaturesTest::deletePublicAccessBlockRemovesConfig` -- s3wire does not implement this operation (DELETE [publicAccessBlock]) (Service: S3, Status Code: 501, Request ID: 8A6532C80B5D4133) (SDK Attempt Count: 1)
