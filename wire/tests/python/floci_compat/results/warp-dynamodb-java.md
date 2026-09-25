# Floci java / dynamodb vs warp (2026-09-25 15:17)

endpoint `http://localhost:53904`, runner rc=1, 2.9s

| status | count |
|---|---|
| pass | 118 |
| fail | 0 |
| error | 2 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.DynamoDbAccessPathValidationTest | 12 | 0 | 0 | 0 |
| com.floci.test.DynamoDbExpressionTests | 15 | 0 | 0 | 0 |
| com.floci.test.DynamoDbEnhancedClientTest | 2 | 0 | 0 | 0 |
| com.floci.test.DynamoDbPartiQLTest | 20 | 0 | 0 | 0 |
| com.floci.test.DynamoDbTest | 18 | 0 | 2 | 0 |
| com.floci.test.DynamoDbConformanceChangesTest | 41 | 0 | 0 | 0 |
| com.floci.test.DynamoDbScanConditionTests | 6 | 0 | 0 | 0 |
| com.floci.test.DynamoDbConcurrencyTest | 4 | 0 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [c] `com.floci.test.DynamoDbTest::updateTableReplicaLifecycle` -- Global tables / replicas (ReplicaUpdates) are not supported by Warp's dynamowire (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [c] `com.floci.test.DynamoDbTest::searchVectors` -- Index: 0
