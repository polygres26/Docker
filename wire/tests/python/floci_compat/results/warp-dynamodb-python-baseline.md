# Floci python / dynamodb vs warp (2026-09-25 13:34)

endpoint `http://localhost:50817`, runner rc=1, 0.7s

| status | count |
|---|---|
| pass | 13 |
| fail | 9 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_dynamodb.py | 13 | 9 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `test_dynamodb.py::test_update_table` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the UpdateTable operation: Operation not implemented by dynamowire: UpdateTable
- [a] `test_dynamodb.py::test_describe_time_to_live` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the DescribeTimeToLive operation: Operation not implemented by dynamowire: DescribeTimeToLive
- [a] `test_dynamodb.py::test_update_and_describe_continuous_backups` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the DescribeContinuousBackups operation: Operation not implemented by dynamowire: DescribeContinuousBackups
- [a] `test_dynamodb.py::test_tag_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the TagResource operation: Operation not implemented by dynamowire: TagResource
- [a] `test_dynamodb.py::test_list_tags_of_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the TagResource operation: Operation not implemented by dynamowire: TagResource
- [a] `test_dynamodb.py::test_untag_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the TagResource operation: Operation not implemented by dynamowire: TagResource
- [a] `test_dynamodb.py::test_create_table_with_gsi_and_lsi` -- assert 0 == 1
- [a] `test_dynamodb.py::test_query_gsi_sparse_index` -- botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the Query operation: KeyConditionExpression references non-key attribute: gsiPk
- [a] `test_dynamodb.py::test_query_lsi` -- botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the Query operation: KeyConditionExpression references non-key attribute: lsiSk
