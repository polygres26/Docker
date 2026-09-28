# Floci python / kinesis vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.2s

| status | count |
|---|---|
| pass | 0 |
| fail | 9 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_kinesis.py | 0 | 9 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `test_kinesis.py::test_create_stream` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the DeleteStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.DeleteStream
- [a] `test_kinesis.py::test_list_streams` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
- [a] `test_kinesis.py::test_describe_stream` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
- [a] `test_kinesis.py::test_describe_stream_summary` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
- [a] `test_kinesis.py::test_delete_stream` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
- [a] `test_kinesis.py::test_put_record` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
- [a] `test_kinesis.py::test_get_records` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
- [a] `test_kinesis.py::test_put_records_batch` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
- [a] `test_kinesis.py::test_add_tags_to_stream` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateStream operation: Missing or unrecognized X-Amz-Target header: Kinesis_20131202.CreateStream
