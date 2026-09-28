# Floci AWS-SDK compatibility baseline for Warp (dynamowire / sqswire / s3wire, and SNS / Kinesis / Secrets Manager / SSM / KMS / STS on the unified AWS endpoint)

Runs the SDK tests from [floci-io/floci](https://github.com/floci-io/floci) `compatibility-tests/` (MIT) against
Warp's AWS frontends. Floci code is NOT in this repo; point `--floci-dir` at a checkout
(`git clone --depth 1 https://github.com/floci-io/floci.git`). Baseline date: 2026-09-25, Warp jar built from the
working tree (dynamowire/sqswire), native Postgres 17.

## Files
- `launch_warp.py` starts a throwaway Warp (local Postgres, dynamowire+sqswire, optional s3wire) on free ports, cluster
  static discovery, prints endpoints as JSON, runs until SIGTERM.
- `run_floci_compat.py` runs one service x suite, writes `results/warp-<service>-<suite>.{json,md}`.
- `results/` per-run summaries with every failure and a heuristic class (a/b/c/d).

## Re-run
```
export WARP_TEST_PG_LOCAL=1 WARP_TEST_PG_BIN=/opt/homebrew/opt/postgresql@17/bin WARP_TEST_JAR=<path>/sayonora-warp.jar
ulimit -n 8192
# Warp with dynamodb+sqs (+ s3wire backed by MinIO; needs Docker disk):
python3 launch_warp.py --state /tmp/warp_state.json [--minio | --s3-backend-endpoint URL --s3-backend-bucket B --s3-backend-key K --s3-backend-secret S] &
# S3 in Postgres mode (no MinIO): python3 launch_warp.py --state /tmp/warp_state.json --s3-postgres &   (enables the s3 store on `default`
#   through the admin API, WARP_S3WIRE_ENABLED=true, credentials test=test); then point --endpoint at the `s3` entry of the state file:
F=/path/to/floci
python3 run_floci_compat.py --service dynamodb --suite python --endpoint $(jq -r .dynamodb /tmp/warp_state.json) --floci-dir $F
python3 run_floci_compat.py --service sqs      --suite node   --endpoint $(jq -r .sqs /tmp/warp_state.json)      --floci-dir $F
python3 run_floci_compat.py --service s3       --suite java   --endpoint http://localhost:<s3wire-port>          --floci-dir $F
```
Suites: `python` (pytest+pytest-timeout: `pip install --user pytest-timeout boto3`), `node` (`npm install` once in
`$F/compatibility-tests/sdk-test-node`), `java` (mvn; run one at a time). Credentials test/test, us-east-1 (Warp:
dynamowire and sqswire accept them with no WARP_AWS_IAM_CREDENTIALS set; s3wire needs `WARP_S3WIRE_CREDENTIALS=test=test`).
Not wired: `awscli` (needs aws-cli v2 + bats-core; neither installed), `go` (no Go toolchain), terraform/cdk/opentofu.

## Results (pass / fail+error) -- note the Floci suites are smaller than the "2,576" headline
| service | python | node | java |
|---|---|---|---|
| DynamoDB (baseline) | 13 / 9 | 23 / 36 | 38 / 82 |
| **DynamoDB (after the dynamowire conformance work, 2026-09-25)** | **22 / 0** | **59 / 0** | **118 / 2** (both class c, below) |
| SQS (baseline) | 7 / 9 | 6 / 2 | 8 / 19 |
| **SQS (after the sqswire conformance work, 2026-09-25)** | **16 / 0** | **8 / 0** | **27 / 0** |
| S3 (baseline, Postgres mode before the conformance work, 2026-09-25) | 17 / 25 | 20 / 15 | 30 / 45 |
| **S3 (after the conformance work, 2026-09-25)** | **42 / 0** | **35 / 0** | **74 / 1** |

S3 (Postgres mode, `launch_warp.py --s3-postgres`; test_s3.py + test_s3_cors.py, s3 / s3-cors / s3-multipart-checksum, S3Test + S3FeaturesTest +
S3VirtualHostStyleTest + S3MultipartChecksumTest + S3AnnotationsTest). Baseline (`results/warp-baseline-s3-*.md`): failures were tagging, versioning, ListObjectVersions,
GetObjectAttributes, checksum types, CORS, public access block, UploadPartCopy, LocationConstraint, virtual-hosted addressing and object annotations. After
(`results/warp-s3-*.md`) everything passes except `S3Test::deleteBucketTagging`, which is class **c** (Floci-specific): it calls GetBucketTagging after
DeleteBucketTagging and expects an empty tag set, while real S3 answers `404 NoSuchTagSet` (the python suite accepts either, and s3wire answers like S3).
Extra Floci classes run against the same Warp (not part of the three suites above): S3LifecycleTest 2/2, S3PresignTest 1/1, S3BlockPublicAccessTest 6/6, S3SelectTest 19/19,
S3PresignedUrlSigV4VerificationTest 11 of 13 (class c: `presignedUrlSignedWithIamAccessKeyIsAccepted` needs Floci's IAM service; `presignedUrlWithMalformedCredentialIsRejected`
expects 403 InvalidAccessKeyId where S3 answers 400 AuthorizationQueryParametersError). Not runnable against s3wire (class c): S3AuthEnforcementCompatibilityTest (Floci IAM),
S3NotificationsTest and the python/node notification suites (need Floci's SNS/Lambda/CloudWatch Logs), S3ControlTest / S3Tables / S3Vectors (other AWS APIs).
Re-run: `python3 launch_warp.py --state s.json --s3-postgres &` then `run_floci_compat.py --service s3 --suite {python,node,java} --endpoint $(jq -r .s3 s.json) --floci-dir $F`
(fresh Warp per run; `--vhost-domain d1,d2` sets `WARP_S3WIRE_VHOST_DOMAIN`, not needed for `*.localhost*` names).

Failure classes (heuristic, from `classify()` on test name/message; a=not implemented, b=wrong behaviour/shape,
c=Floci-specific, d=environment): dynamodb java a48/b30/c4, node a21/b13/c2, python a9; sqs java a10/b9, node a1/b1,
python a7/b2; d=0 in every run. The heuristic over-counts (a); e.g. `dlq`/`batch` name hits and the sqs `QueueArn`
KeyError are really (b). Treat as +/-15%. Some DynamoDB java/node failures (ListTables Limit sizes, ScanFilter counts)
are contaminated by tables left by earlier tests in the same Warp; use a fresh Warp per run.

## SQS -- what failed at baseline (all of it is fixed; sqswire now speaks the JSON and the Query/XML protocols)

After the conformance work every Floci SQS test passes (python 16, node 8, java 27; the baseline summaries are kept as
`results/warp-sqs-*-baseline.md`, the current ones are `results/warp-sqs-*.md`). Nothing needed a class-c (Floci-specific)
exemption. The list below is the baseline diagnosis, kept for history; the SQS behaviour and its differences from real SQS
are documented in `docs/WARP_GUIDE.md` (section *The SQS store*).

Not implemented (a): SendMessageBatch, DeleteMessageBatch, ChangeMessageVisibilityBatch, TagQueue/UntagQueue/ListQueueTags,
ListDeadLetterSourceQueues, StartMessageMoveTask. Wrong behaviour (b): MessageAttributes not stored/returned and
MD5OfMessageAttributes wrong (breaks SDK MD5 validation, ~8 java tests, 1 python); GetQueueAttributes lacks QueueArn
(breaks DLQ/RedrivePolicy tests); long polling (WaitTimeSeconds) returns immediately; DLQ routing after maxReceiveCount
does not move messages; FIFO receive returns one message per call rather than all from one group.
Top 15 (port target = `floci/src/main/java/io/github/hectorvent/floci/services/sqs/`):
1 message attributes + MD5 (SqsService.java md5OfMessageAttributes; SqsJsonHandler.java) 2 SendMessageBatch 3 DeleteMessageBatch
4 ChangeMessageVisibilityBatch (all SqsJsonHandler + SqsService) 5 QueueArn/all attribute names in GetQueueAttributes
6 long polling 7 RedrivePolicy/DLQ routing 8 ListDeadLetterSourceQueues 9 Tag/Untag/ListQueueTags 10 FIFO batch receive per group
11 dedup replay returning original ids 12 StartMessageMoveTask family 13 Query/XML protocol (SqsQueryHandler.java; older SDKs/aws-cli v1)
14 PurgeQueue/attribute edge cases 15 inspection endpoint (SqsInspectionController, Floci-specific, low priority).

## DynamoDB -- what failed at baseline, and what still fails

After the conformance work (fresh Warp on native Postgres 17 per suite; baseline summaries kept as
`results/warp-dynamodb-*-baseline.md`, current ones are `results/warp-dynamodb-*.md`) python passes 22/22, node 59/59 and java
118/120. The two remaining java failures are Floci-specific (class c), not DynamoDB behaviour:
`DynamoDbTest::updateTableReplicaLifecycle` (adds a replica region to a table that has no stream through UpdateTable
`ReplicaUpdates`; real DynamoDB global tables need DynamoDB Streams, and Warp answers a clear ValidationException: global
tables are documented as unsupported) and `DynamoDbTest::searchVectors` (`SearchVectors` is a Floci extension, not a DynamoDB
API). Behaviour, the differences from real DynamoDB and the tests are in `docs/WARP_GUIDE.md` (section *The DynamoDB store*) and
`tests/python/test_dynamowire_conformance.py`. The list below is the baseline diagnosis, kept for history.

Not implemented (a): UpdateTable, DescribeTimeToLive/UpdateTimeToLive, DescribeContinuousBackups, Tag/Untag/ListTagsOfResource,
GSI/LSI (CreateTable accepts them but DescribeTable reports none; Query on IndexName fails with "non-key attribute"),
`attribute_type()`, legacy API (AttributesToGet, AttributeUpdates, QueryFilter, ScanFilter, KeyConditions, Expected),
parallel Scan (Segment/TotalSegments), ReturnItemCollectionMetrics/ConsumedCapacity, ExecuteTransaction and non-key-equality
PartiQL (SELECT with placeholders/params, UPDATE REMOVE, index-qualified selects). Wrong (b): input validation
(ListTables Limit 0/>100, table-name rules, 25/100 batch limits, empty-string key, duplicate transact keys, idempotency token,
reserved words in expressions, enum validation before table lookup all return success or ResourceNotFound instead of
ValidationException), Select=COUNT still returns items, `Type BOOL is not comparable` on `=`/`<>` filters, parenthesised /
BETWEEN key conditions rejected, atomicity under concurrency (attribute_not_exists races, UpdateItem arithmetic).
Top 15 (Floci `services/dynamodb/`): 1 GSI/LSI + DynamoDbAccessPath(Validator).java, DynamoDbService.java 2 DynamoDbKeyConditionParser.java
(paren/BETWEEN/begins_with) 3 ExpressionEvaluator.java (BOOL compare, attribute_type, size, reserved words: DynamoDbReservedWords.java)
4 validation layer: DynamoDbTableNames.java, DynamoDbAttributeValueValidator.java, DynamoDbItemSize/ExpressionSize 5 UpdateTable
6 Tags 7 TTL (DynamoDbTtlService.java) + ContinuousBackups 8 PartiQL: DynamoDbPartiQLParser/Handler/KeyPlan.java 9 ExecuteTransaction
(TransactionCanceledException.java; DynamoDbTransactCapacity.java) 10 legacy params (DynamoDbService.java) 11 Select=COUNT + parallel Scan
12 ConsumedCapacity/ItemCollectionMetrics (DynamoDbWriteCapacity.java, DynamoDbResponses.java) 13 concurrency semantics of conditional writes
14 Streams (DynamoDbStreamService.java; not exercised by the python suite but by java) 15 Export/Import, vector search (Floci extras; low priority).

Floci vs dynamowire (structure): Floci has a backend seam (`backend/DynamoDbOperations|TableAccess|ItemAccess` + selector, native
in-memory/file backend) with the expression engine (ExpressionEvaluator ~1.1k lines, DynamoDbService ~5k lines) in the service layer,
so all protocol semantics sit above storage. dynamowire (~3k lines) has OperationHandlers + a Postgres PgItemStore with a small
expression set (ConditionExpressionEvaluator, UpdateExpressionParser, KeyConditionParser, PartiQlParser). The Floci layer already
solves: indexes, full key-condition grammar, validation errors, legacy params, TTL/tags/UpdateTable, streams, richer PartiQL. These
are portable as expression/validation code on top of PgItemStore; indexes need a PgItemStore design (materialised index tables or
expression indexes).

## S3 port targets (from tree; not measured)
`services/s3/`: S3Service.java (buckets/objects/multipart/versioning/ListParts), S3Controller.java (REST routing),
S3VirtualHostFilter.java (virtual-host style), S3HeaderSignatureFilter.java + S3RequestAuthorizationParser.java (SigV4 header
auth), PreSignedUrlFilter/Generator, S3CorsFilter, S3AclPolicy, S3SelectService.


## SNS, Kinesis, Secrets Manager, SSM, KMS, STS (awswire, 2026-09-26)

These six run against the **unified AWS endpoint** (Floci's tests use one endpoint for every service, and the SNS tests create SQS queues): `python3 launch_warp.py --state s.json --aws-unified &` starts a throwaway Warp with the
endpoint on a free port, the `sns`, `kinesis` and `awsparams` stores enabled on the default backend (through the admin API) and `WARP_KMS_INSECURE_DEV_KEY=true`, and writes it as `aws` in the state file; then
`python3 run_floci_compat.py --service {sns,kinesis,secretsmanager,ssm,kms,sts} --suite {python,node,java} --endpoint $(jq -r .aws s.json) --floci-dir $F`. `--aws-unified --s3-postgres` also serves S3 there. Baseline = a Warp
without the AWS frontends (every call is answered by sqswire with `UnknownOperation`), summaries kept as `results/warp-<service>-<suite>-baseline.md`; current summaries are `results/warp-<service>-<suite>.md`.
On Python 3.9 Floci's `test_kms.py` cannot even be collected (a class-scoped fixture stacked on `@staticmethod` needs Python 3.10's `staticmethod.__name__`); the runner then runs a generated copy with that one fixture hoisted to module level
(`tests/_py39_test_kms.py` in the Floci checkout), the tests themselves are unchanged.

| service (files run) | python pass / fail: baseline -> now | node | java |
|---|---|---|---|
| SNS (`test_sns.py`, `sns.test.ts`, `SnsTest`) | 0 / 10 -> **10 / 0** | 0 / 10 -> **10 / 0** | 0 / 16 -> **16 / 0** |
| Kinesis (`test_kinesis.py`, `kinesis.test.ts`, `KinesisTest` + `KinesisEfoTest`) | 0 / 9 -> **9 / 0** | 0 / 6 -> **6 / 0** | 0 / 3 -> **5 / 0** |
| Secrets Manager | 1 / 12 -> **13 / 0** | 1 / 5 -> **6 / 0** | 1 / 21 -> **21 / 1** |
| SSM | 0 / 12 -> **12 / 0** | 1 / 6 -> **7 / 0** | 0 / 16 -> **16 / 0** |
| KMS (`test_kms.py`, `kms.test.ts` + `kms-features.test.ts`, `KmsTest` + `KmsFeaturesTest` + `KmsGrantLifecycleTest` + `KmsSm2Test`) | 0 / 39 -> **39 / 0** | 0 / 13 -> **13 / 0** | 0 / 57 -> **57 / 0** |
| STS (`StsTest` needs Floci's IAM: SAML providers and roles) | 1 / 8 -> **9 / 0** | 0 / 2 -> **2 / 0** | 0 / 1 -> **20 / 0** |

The single remaining failure, `SecretsManagerTest::rotateSecretStub`, is **class c** (Floci-specific): it creates a Lambda function through Floci's Lambda service before calling RotateSecret, and Warp does not emulate Lambda. Everything else that failed at baseline was
class a (not implemented). Notes on what the suites needed beyond the services themselves: the JavaScript SDK v3 speaks **cleartext HTTP/2 with prior knowledge** to Kinesis (Warp's h2c support, `H2cConnectionFactory`); the Java SDK v2 speaks **CBOR** to Kinesis; the Java async client's SubscribeToShard is an HTTP/1.1
event stream here (`Protocol.HTTP1_1` in Floci's fixture); `StsTest` needs IAM `CreateSAMLProvider` / `CreateRole` and a real XML-DSig validation of the SAML assertion (Warp implements both, see WARP_GUIDE section 4.7); the KMS suites need Ed25519 / Ed25519ph, secp256k1 (BouncyCastle), ML-DSA (Java 24+) and SM2.
Re-run with a fresh Warp per suite run.
