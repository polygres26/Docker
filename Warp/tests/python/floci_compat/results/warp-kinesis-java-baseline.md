# Floci java / kinesis vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 3.8s

| status | count |
|---|---|
| pass | 0 |
| fail | 3 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.KinesisTest | 0 | 2 | 0 | 0 |
| com.floci.test.KinesisEfoTest | 0 | 1 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `com.floci.test.KinesisTest::listShardsPaginatesThroughEveryPage` -- Unexpected exception thrown: software.amazon.awssdk.services.kinesis.model.KinesisException: Service returned HTTP status code 400 (Service: Kinesis, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KinesisTest::awsSdkV2UsesRootCborRoute` -- Unexpected exception thrown: software.amazon.awssdk.services.kinesis.model.KinesisException: Service returned HTTP status code 400 (Service: Kinesis, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KinesisEfoTest::` -- Unexpected exception thrown: software.amazon.awssdk.services.kinesis.model.KinesisException: Service returned HTTP status code 400 (Service: Kinesis, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
