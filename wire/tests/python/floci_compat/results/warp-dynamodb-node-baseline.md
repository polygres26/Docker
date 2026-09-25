# Floci node / dynamodb vs warp (2026-09-25 13:34)

endpoint `http://localhost:50817`, runner rc=1, 1.8s

| status | count |
|---|---|
| pass | 23 |
| fail | 36 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/dynamodb-conformance.test.ts | 9 | 33 | 0 | 0 |
| tests/dynamodb.test.ts | 14 | 3 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 1: ListTables > respects Limit and returns LastEvaluatedTableName` -- expected [ 'lt-aaa-test-f66ba078', …(4) ] to have a length of 1 but got 5
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 1: ListTables > rejects Limit=0 with ValidationException` -- expected ValidationException to be thrown: expected undefined to be defined
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 1: ListTables > rejects Limit=101 with ValidationException` -- expected ValidationException to be thrown: expected undefined to be defined
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 1: ValidationException rename > rejects a 2-char table name with ValidationException` -- expected 'ResourceNotFoundException' to be 'ValidationException' // Object.is equality
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 1: ValidationException rename > rejects empty table name with ValidationException` -- expected 'ResourceNotFoundException' to be 'ValidationException' // Object.is equality
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 2: ProjectionExpression > rejects AttributesToGet combined with ProjectionExpression` -- expected ValidationException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 3: Batch limits & key validation > BatchWriteItem rejects more than 25 items` -- expected ValidationException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 3: Batch limits & key validation > BatchGetItem rejects more than 100 keys` -- expected ValidationException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 3: Batch limits & key validation > PutItem rejects empty string as PK` -- expected ValidationException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 3: Batch limits & key validation > TransactWriteItems rejects duplicate keys` -- expected ValidationException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 3: Batch limits & key validation > TransactWriteItems idempotency token conflict throws IdempotentParameterMismatchException` -- expected IdempotentParameterMismatchException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 3: Batch limits & key validation > PutItem with ReturnItemCollectionMetrics=SIZE returns metrics on LSI table` -- expected undefined to be defined
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 4: Query/Scan behaviour > Query Select=COUNT returns Count without Items` -- expected [ { pk: { S: 'p1' }, …(3) }, …(4) ] to be undefined
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 4: Query/Scan behaviour > Scan Select=COUNT returns Count without Items` -- expected [ { pk: { S: 'p1' }, …(3) }, …(4) ] to be undefined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 4: Query/Scan behaviour > parallel scan partitions with no overlap and full coverage` -- expected true to be false // Object.is equality
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 4: Query/Scan behaviour > parallel scan requires both Segment and TotalSegments` -- expected ValidationException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 4: Query/Scan behaviour > attribute_type() function in FilterExpression` -- Unsupported function: attribute_type
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 4: Query/Scan behaviour > parenthesized key condition expression works` -- KeyConditionExpression references non-key attribute:
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 4: Query/Scan behaviour > Query without KeyConditionExpression throws ValidationException` -- expected 'InternalFailure' to be 'ValidationException' // Object.is equality
- [c] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 7: Tags API errors > TagResource with invalid ARN throws ValidationException` -- expected 'UnknownOperationException' to be 'ValidationException' // Object.is equality
- [c] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 7: Tags API errors > ListTagsOfResource with valid-format non-existent ARN throws AccessDeniedException` -- expected 'UnknownOperationException' to be 'AccessDeniedException' // Object.is equality
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 9: Reserved words > bare reserved word in ConditionExpression throws ValidationException` -- expected ValidationException to be thrown: expected undefined to be defined
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 9: Reserved words > bare reserved word in FilterExpression throws ValidationException` -- expected ValidationException to be thrown: expected undefined to be defined
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 10: Legacy API > AttributesToGet returns only listed attributes (no auto-key-include)` -- expected { pk: { S: 'p1' }, …(3) } to not have property "pk"
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 10: Legacy API > AttributeUpdates (legacy) applies PUT action` -- Cannot invoke "com.google.gson.JsonElement.getAsString()" because the return value of "com.google.gson.JsonObject.get(String)" is null
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 10: Legacy API > AttributeUpdates (legacy) applies DELETE action to remove attribute` -- Cannot invoke "com.google.gson.JsonElement.getAsString()" because the return value of "com.google.gson.JsonObject.get(String)" is null
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 10: Legacy API > QueryFilter (legacy) filters query results` -- Cannot invoke "com.google.gson.JsonElement.getAsString()" because the return value of "com.google.gson.JsonObject.get(String)" is null
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 10: Legacy API > QueryFilter and KeyConditionExpression are mutually exclusive` -- expected undefined to be 'ValidationException' // Object.is equality
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 10: Legacy API > ScanFilter (legacy) filters scan results` -- expected 3 to be 1 // Object.is equality
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 10: Legacy API > KeyConditions and KeyConditionExpression are mutually exclusive` -- expected ValidationException to be thrown: expected undefined to be defined
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 11: Enum validation before table lookup > invalid ReturnValues on PutItem throws ValidationException even for non-existent table` -- expected 'ResourceNotFoundException' to be 'ValidationException' // Object.is equality
- [b] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 11: Enum validation before table lookup > invalid ReturnValues on DeleteItem throws ValidationException even for non-existent table` -- expected 'ResourceNotFoundException' to be 'ValidationException' // Object.is equality
- [a] `tests/dynamodb-conformance.test.ts::DynamoDB — Phase 11: Enum validation before table lookup > invalid ReturnConsumedCapacity throws ValidationException even for non-existent table` -- expected 'ResourceNotFoundException' to be 'ValidationException' // Object.is equality
- [a] `tests/dynamodb.test.ts::DynamoDB GSI/LSI > should verify indexes via DescribeTable` -- expected +0 to be 1 // Object.is equality
- [a] `tests/dynamodb.test.ts::DynamoDB GSI/LSI > should query GSI` -- KeyConditionExpression references non-key attribute: gsiPk
- [a] `tests/dynamodb.test.ts::DynamoDB GSI/LSI > should query LSI` -- KeyConditionExpression references non-key attribute: lsiSk
