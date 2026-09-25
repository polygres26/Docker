# Floci java / dynamodb vs warp (2026-09-25 13:38)

endpoint `http://localhost:50817`, runner rc=1, 33.9s

| status | count |
|---|---|
| pass | 38 |
| fail | 51 |
| error | 31 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.DynamoDbAccessPathValidationTest | 3 | 8 | 1 | 0 |
| com.floci.test.DynamoDbExpressionTests | 7 | 3 | 5 | 0 |
| com.floci.test.DynamoDbEnhancedClientTest | 2 | 0 | 0 | 0 |
| com.floci.test.DynamoDbPartiQLTest | 2 | 6 | 12 | 0 |
| com.floci.test.DynamoDbTest | 13 | 0 | 7 | 0 |
| com.floci.test.DynamoDbConformanceChangesTest | 8 | 27 | 6 | 0 |
| com.floci.test.DynamoDbScanConditionTests | 1 | 5 | 0 | 0 |
| com.floci.test.DynamoDbConcurrencyTest | 2 | 2 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `com.floci.test.DynamoDbAccessPathValidationTest::lsiRejectsBaseTableSortKeyCondition` -- Expecting code to raise a throwable.
- [a] `com.floci.test.DynamoDbAccessPathValidationTest::gsiStillRejectsConsistentReads` -- Expecting code to raise a throwable.
- [a] `com.floci.test.DynamoDbAccessPathValidationTest::queryRejectsInvalidCompositeSortKeyConditions` -- Multiple Failures (1 failure)
- [a] `com.floci.test.DynamoDbAccessPathValidationTest::legacyKeyConditionsUseSelectedGsiSchema` -- Multiple Failures (1 failure)
- [a] `com.floci.test.DynamoDbAccessPathValidationTest::acceptsProjectedGsiAttributesAndLsiTableFetch` -- KeyConditionExpression references non-key attribute: status (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbAccessPathValidationTest::queryAndScanRejectNonProjectedGsiAttribute` -- Expecting code to raise a throwable.
- [a] `com.floci.test.DynamoDbAccessPathValidationTest::queryAndScanRejectUnknownIndexOnEmptyTable` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbAccessPathValidationTest::queryRejectsKeyConditionValuesWithWrongSchemaTypes` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbAccessPathValidationTest::queryAndScanRejectWrongExclusiveStartKeyTypes` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbExpressionTests::filterBoolNotEqual` -- Type BOOL is not comparable (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbExpressionTests::filterBoolEqual` -- Type BOOL is not comparable (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbExpressionTests::filterNot` -- Type BOOL is not comparable (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbExpressionTests::consumedCapacityTotal` -- Expecting actual not to be null
- [a] `com.floci.test.DynamoDbExpressionTests::consumedCapacityGetItem` -- Expecting actual not to be null
- [a] `com.floci.test.DynamoDbExpressionTests::consumedCapacityPutItem` -- Expecting actual not to be null
- [a] `com.floci.test.DynamoDbExpressionTests::queryParenthesizedBetween` -- KeyConditionExpression references non-key attribute:  (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbExpressionTests::queryCompactBetween` -- KeyConditionExpression references non-key attribute:  (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbPartiQLTest::selectByPkOnly` -- Expected a value placeholder, got 'pk1' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbPartiQLTest::selectWithProjection` -- Expected a value placeholder, got 'pk1' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbPartiQLTest::insertDuplicateThrows` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbPartiQLTest::updateSetAttribute` -- Referenced path does not exist in the item: 'Charlie' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbPartiQLTest::updateRemoveAttribute` -- dynamowire's PartiQL support covers single-table SELECT/INSERT/UPDATE/DELETE with a plain key-equality WHERE clause -- could not parse: UPDATE "partiql-test-table" REMOVE age WHERE pk = 'pk1' AND sk = 'sk1' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbPartiQLTest::insertMultipleItemsAndQueryRange` -- Expected a value placeholder, got 'pk2' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbPartiQLTest::selectWithBeginsWith` -- KeyConditionExpression references non-key attribute: begins_with (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbPartiQLTest::deleteItem` -- Expected a value placeholder, got 'pk1' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbPartiQLTest::executeTransactionInsertAndDelete` -- dynamowire's PartiQL support covers ExecuteStatement and BatchExecuteStatement today, not ExecuteTransaction -- a disclosed gap, not a silent one. (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbPartiQLTest::executeTransactionDuplicateInsertThrows` -- Expecting actual throwable to be an instance of:
- [a] `com.floci.test.DynamoDbPartiQLTest::batchExecuteStatementMixedResults` -- Expecting actual:
- [a] `com.floci.test.DynamoDbPartiQLTest::executeStatementOnGsiRejectsConsistentRead` -- Expecting throwable message:
- [a] `com.floci.test.DynamoDbPartiQLTest::executeStatementReadsThroughGsi` -- dynamowire's PartiQL support covers single-table SELECT/INSERT/UPDATE/DELETE with a plain key-equality WHERE clause -- could not parse: SELECT * FROM "partiql-index-table"."status-index" WHERE status = 'active' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbPartiQLTest::executeStatementOnLsiAcceptsConsistentRead` -- dynamowire's PartiQL support covers single-table SELECT/INSERT/UPDATE/DELETE with a plain key-equality WHERE clause -- could not parse: SELECT * FROM "partiql-index-table"."alternate-index" WHERE pk = 'idxpk' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbPartiQLTest::executeStatementOnUnknownIndexFailsWithoutIndexName` -- expected: "The table does not have the specified index"
- [a] `com.floci.test.DynamoDbPartiQLTest::batchExecuteStatementRejectsIndexQualifiedSelectPerSlot` -- expected: "ValidationError"
- [b] `com.floci.test.DynamoDbPartiQLTest::executeStatementRejectsForeignNextToken` -- Expected a value placeholder, got 'idxpk' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbPartiQLTest::executeStatementReadsEveryAttributeThroughAllProjectionIndex` -- dynamowire's PartiQL support covers single-table SELECT/INSERT/UPDATE/DELETE with a plain key-equality WHERE clause -- could not parse: SELECT * FROM "partiql-index-table"."status-index" WHERE status = 'active' (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbTest::updateTable` -- Operation not implemented by dynamowire: UpdateTable (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [c] `com.floci.test.DynamoDbTest::updateTableReplicaLifecycle` -- Operation not implemented by dynamowire: UpdateTable (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbTest::describeTimeToLive` -- Operation not implemented by dynamowire: DescribeTimeToLive (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbTest::tagResource` -- Operation not implemented by dynamowire: TagResource (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbTest::listTagsOfResource` -- Operation not implemented by dynamowire: ListTagsOfResource (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbTest::untagResource` -- Operation not implemented by dynamowire: UntagResource (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [c] `com.floci.test.DynamoDbTest::searchVectors` -- Index: 0
- [b] `com.floci.test.DynamoDbConformanceChangesTest::listTablesWithLimit` -- Expected size: 1 but was: 4 in:
- [b] `com.floci.test.DynamoDbConformanceChangesTest::listTablesLimitZeroThrowsValidationException` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbConformanceChangesTest::listTablesLimitOver100ThrowsValidationException` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbConformanceChangesTest::putItemInvalidTableNameThrowsValidationException` -- Multiple Failures (1 failure)
- [a] `com.floci.test.DynamoDbConformanceChangesTest::batchWriteItemRejectsMoreThan25Items` -- Expecting code to raise a throwable.
- [a] `com.floci.test.DynamoDbConformanceChangesTest::batchGetItemRejectsMoreThan100Keys` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbConformanceChangesTest::putItemRejectsEmptyStringPrimaryKey` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbConformanceChangesTest::transactWriteItemsRejectsDuplicateKeys` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbConformanceChangesTest::transactWriteItemsIdempotencyTokenRejectsConflict` -- Expecting code to raise a throwable.
- [a] `com.floci.test.DynamoDbConformanceChangesTest::returnItemCollectionMetricsOnLsiTable` -- Expecting actual not to be null
- [b] `com.floci.test.DynamoDbConformanceChangesTest::querySelectCountReturnsCountNotItems` -- Expecting empty but was: [{"count"=AttributeValue(N=0), "name"=AttributeValue(S=Item-0), "pk"=AttributeValue(S=p1), "sk"=AttributeValue(S=s0), "status"=AttributeValue(S=active)},
- [b] `com.floci.test.DynamoDbConformanceChangesTest::scanSelectCountReturnsCountNotItems` -- Expecting empty but was: [{"count"=AttributeValue(N=0), "name"=AttributeValue(S=Item-0), "pk"=AttributeValue(S=p1), "sk"=AttributeValue(S=s0), "status"=AttributeValue(S=active)},
- [a] `com.floci.test.DynamoDbConformanceChangesTest::parallelScanPartitionsResultsCorrectly` -- Expecting
- [a] `com.floci.test.DynamoDbConformanceChangesTest::parallelScanRequiresBothSegmentAndTotalSegments` -- Expecting code to raise a throwable.
- [a] `com.floci.test.DynamoDbConformanceChangesTest::queryWithAttributeTypeFunction` -- Unsupported function: attribute_type (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.DynamoDbConformanceChangesTest::queryWithParenthesizedKeyCondition` -- KeyConditionExpression references non-key attribute:  (Service: DynamoDb, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.DynamoDbConformanceChangesTest::queryRequiresKeyCondition` -- Multiple Failures (1 failure)
- [c] `com.floci.test.DynamoDbConformanceChangesTest::tagResourceWithInvalidArnThrowsValidationException` -- Multiple Failures (1 failure)
- [c] `com.floci.test.DynamoDbConformanceChangesTest::listTagsOfResourceWithValidFormatNonExistentArnThrowsAccessDeniedException` -- Multiple Failures (1 failure)
- [b] `com.floci.test.DynamoDbConformanceChangesTest::getItemAttributesToGet` -- Expecting actual:
- [b] `com.floci.test.DynamoDbConformanceChangesTest::attributesToGetAndProjectionExpressionAreMutuallyExclusive` -- Expecting code to raise a throwable.
- [a] `com.floci.test.DynamoDbConformanceChangesTest::updateItemWithAttributeUpdates` -- Cannot invoke "com.google.gson.JsonElement.getAsString()" because the return value of "com.google.gson.JsonObject.get(String)" is null (Service: DynamoDb, Status Code: 500, Request ID: null) (SDK Attempt Count: 9)
- [a] `com.floci.test.DynamoDbConformanceChangesTest::queryWithQueryFilter` -- Cannot invoke "com.google.gson.JsonElement.getAsString()" because the return value of "com.google.gson.JsonObject.get(String)" is null (Service: DynamoDb, Status Code: 500, Request ID: null) (SDK Attempt Count: 9)
- [a] `com.floci.test.DynamoDbConformanceChangesTest::scanWithScanFilter` -- Expected size: 1 but was: 38 in:
- [a] `com.floci.test.DynamoDbConformanceChangesTest::updateItemLegacyExpectedAttributeMissing` -- Expecting actual throwable to be an instance of:
- [a] `com.floci.test.DynamoDbConformanceChangesTest::updateItemLegacyExpectedValueMismatch` -- Expecting actual throwable to be an instance of:
- [a] `com.floci.test.DynamoDbConformanceChangesTest::updateItemLegacyExpectedMatchSucceeds` -- Cannot invoke "com.google.gson.JsonElement.getAsString()" because the return value of "com.google.gson.JsonObject.get(String)" is null (Service: DynamoDb, Status Code: 500, Request ID: null) (SDK Attempt Count: 9)
- [a] `com.floci.test.DynamoDbConformanceChangesTest::legacyExpectedExistsTrueComparesValue` -- Cannot invoke "com.google.gson.JsonElement.getAsString()" because the return value of "com.google.gson.JsonObject.get(String)" is null (Service: DynamoDb, Status Code: 500, Request ID: null) (SDK Attempt Count: 9)
- [a] `com.floci.test.DynamoDbConformanceChangesTest::legacyExpectedRejectsInvalidExistsCombinations` -- expected: "ValidationException"
- [b] `com.floci.test.DynamoDbConformanceChangesTest::enumValidationFiresBeforeTableLookupOnPutItem` -- expected: "ValidationException"
- [b] `com.floci.test.DynamoDbConformanceChangesTest::enumValidationFiresBeforeTableLookupOnDeleteItem` -- expected: "ValidationException"
- [b] `com.floci.test.DynamoDbConformanceChangesTest::reservedWordAsAttributeNameInConditionThrowsValidationException` -- Expecting code to raise a throwable.
- [b] `com.floci.test.DynamoDbConformanceChangesTest::transactWriteCancellationReasonNullMessageForNonFailedItems` -- Expected size: 2 but was: 0 in:
- [a] `com.floci.test.DynamoDbScanConditionTests::scanFilterEq` -- expected: 1
- [a] `com.floci.test.DynamoDbScanConditionTests::scanFilterGt` -- expected: 2
- [a] `com.floci.test.DynamoDbScanConditionTests::scanFilterLe` -- expected: 3
- [a] `com.floci.test.DynamoDbScanConditionTests::scanFilterBetween` -- expected: 3
- [a] `com.floci.test.DynamoDbScanConditionTests::scanFilterMultipleConditions` -- expected: 1
- [b] `com.floci.test.DynamoDbConcurrencyTest::concurrentPutItemAttributeNotExists` -- [exactly one concurrent PutItem(attribute_not_exists) must succeed]
- [b] `com.floci.test.DynamoDbConcurrencyTest::concurrentUpdateItemArithmetic` -- [each UpdateItem should return a distinct cnt under contention]
