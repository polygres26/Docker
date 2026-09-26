# Floci python / kms vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.4s

| status | count |
|---|---|
| pass | 0 |
| fail | 39 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| _py39_test_kms.py | 0 | 39 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `_py39_test_kms.py::test_create_key` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_describe_key` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_schedule_key_deletion` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_create_key_rejects_incompatible_key_usage` -- AssertionError: assert 'UnknownOperationException' == 'ValidationException'
- [a] `_py39_test_kms.py::test_list_grants` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_list_grants_paginator` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_create_alias` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_list_aliases` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_delete_alias` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_encrypt_decrypt` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_encrypt_using_alias` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_generate_data_key` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_generate_data_key_without_plaintext` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_re_encrypt` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_2048-RSASSA_PSS_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_2048-RSASSA_PSS_SHA_384-SHA384]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_2048-RSASSA_PSS_SHA_512-SHA512]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_2048-RSASSA_PKCS1_V1_5_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_2048-RSASSA_PKCS1_V1_5_SHA_384-SHA384]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_2048-RSASSA_PKCS1_V1_5_SHA_512-SHA512]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_3072-RSASSA_PSS_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_3072-RSASSA_PSS_SHA_384-SHA384]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_3072-RSASSA_PSS_SHA_512-SHA512]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_3072-RSASSA_PKCS1_V1_5_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_3072-RSASSA_PKCS1_V1_5_SHA_384-SHA384]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_3072-RSASSA_PKCS1_V1_5_SHA_512-SHA512]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_4096-RSASSA_PSS_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_4096-RSASSA_PSS_SHA_384-SHA384]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_4096-RSASSA_PSS_SHA_512-SHA512]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_4096-RSASSA_PKCS1_V1_5_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_4096-RSASSA_PKCS1_V1_5_SHA_384-SHA384]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[RSA_4096-RSASSA_PKCS1_V1_5_SHA_512-SHA512]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[ECC_NIST_P256-ECDSA_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[ECC_NIST_P384-ECDSA_SHA_384-SHA384]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[ECC_NIST_P521-ECDSA_SHA_512-SHA512]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_sign_and_verify[ECC_SECG_P256K1-ECDSA_SHA_256-SHA256]` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_ed25519_sign_and_verify` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_tag_resource` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateKey operation: Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `_py39_test_kms.py::test_generate_random` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the GenerateRandom operation: Missing or unrecognized X-Amz-Target header: TrentService.GenerateRandom
