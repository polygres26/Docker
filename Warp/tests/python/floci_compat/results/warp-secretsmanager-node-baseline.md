# Floci node / secretsmanager vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.4s

| status | count |
|---|---|
| pass | 1 |
| fail | 5 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/secretsmanager.test.ts | 1 | 5 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `tests/secretsmanager.test.ts::Secrets Manager > should create secret` -- Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret
- [b] `tests/secretsmanager.test.ts::Secrets Manager > should get secret value` -- Missing or unrecognized X-Amz-Target header: secretsmanager.GetSecretValue
- [b] `tests/secretsmanager.test.ts::Secrets Manager > should update secret` -- Missing or unrecognized X-Amz-Target header: secretsmanager.UpdateSecret
- [b] `tests/secretsmanager.test.ts::Secrets Manager > should list secrets` -- Missing or unrecognized X-Amz-Target header: secretsmanager.ListSecrets
- [b] `tests/secretsmanager.test.ts::Secrets Manager > should delete secret` -- Missing or unrecognized X-Amz-Target header: secretsmanager.DeleteSecret
