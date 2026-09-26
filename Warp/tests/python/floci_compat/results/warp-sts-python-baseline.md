# Floci python / sts vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.3s

| status | count |
|---|---|
| pass | 1 |
| fail | 8 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_sts.py | 1 | 8 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `test_sts.py::test_get_caller_identity` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the GetCallerIdentity operation: sqswire does not implement GetCallerIdentity
- [a] `test_sts.py::test_get_caller_identity_account_id` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the GetCallerIdentity operation: sqswire does not implement GetCallerIdentity
- [a] `test_sts.py::test_assume_role` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the AssumeRole operation: sqswire does not implement AssumeRole
- [a] `test_sts.py::test_assume_role_assumed_role_user` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the AssumeRole operation: sqswire does not implement AssumeRole
- [a] `test_sts.py::test_assume_role_with_web_identity` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the AssumeRoleWithWebIdentity operation: sqswire does not implement AssumeRoleWithWebIdentity
- [a] `test_sts.py::test_get_session_token` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the GetSessionToken operation: sqswire does not implement GetSessionToken
- [a] `test_sts.py::test_get_federation_token` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the GetFederationToken operation: sqswire does not implement GetFederationToken
- [a] `test_sts.py::test_decode_authorization_message` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the DecodeAuthorizationMessage operation: sqswire does not implement DecodeAuthorizationMessage
