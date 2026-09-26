# Floci python / secretsmanager vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.3s

| status | count |
|---|---|
| pass | 1 |
| fail | 12 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_secretsmanager.py | 1 | 12 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `test_secretsmanager.py::test_create_secret` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the DeleteSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.DeleteSecret
- [a] `test_secretsmanager.py::test_get_secret_value_by_name` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [c] `test_secretsmanager.py::test_get_secret_value_by_arn` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_put_secret_value` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_describe_secret` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_update_secret` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_list_secrets` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_delete_secret` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_tag_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_untag_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_list_secret_version_ids` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [a] `test_secretsmanager.py::test_create_secret_duplicate` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateSecret operation: Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
