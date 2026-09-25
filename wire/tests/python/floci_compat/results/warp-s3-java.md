# Floci java / s3 vs warp (2026-09-25 16:16)

endpoint `http://localhost:53160`, runner rc=1, 7.1s

| status | count |
|---|---|
| pass | 74 |
| fail | 0 |
| error | 1 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.S3Test | 33 | 0 | 1 | 0 |
| com.floci.test.S3AnnotationsTest | 11 | 0 | 0 | 0 |
| com.floci.test.S3MultipartChecksumTest | 6 | 0 | 0 | 0 |
| com.floci.test.S3VirtualHostStyleTest | 10 | 0 | 0 | 0 |
| com.floci.test.S3FeaturesTest | 14 | 0 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `com.floci.test.S3Test::deleteBucketTagging` -- The TagSet does not exist (Service: S3, Status Code: 404, Request ID: E7A3614073864077, Extended Request ID: b4fbHqHKhMxC6XAS=b4fbHqHKhMxC6XAS) (SDK Attempt Count: 1)
