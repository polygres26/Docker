# Floci python / ssm vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.3s

| status | count |
|---|---|
| pass | 0 |
| fail | 12 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_ssm.py | 0 | 12 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `test_ssm.py::test_put_parameter` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the DeleteParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.DeleteParameter
- [a] `test_ssm.py::test_get_parameter` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_label_parameter_version` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_get_parameter_history` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_get_parameters` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_describe_parameters` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_get_parameters_by_path` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_add_tags_to_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_list_tags_for_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_remove_tags_from_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_delete_parameter` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [a] `test_ssm.py::test_delete_parameters` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the PutParameter operation: Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
