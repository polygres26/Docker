# Floci java / secretsmanager vs warp (2026-09-25 21:45)

endpoint `http://localhost:60641`, runner rc=1, 1.8s

| status | count |
|---|---|
| pass | 21 |
| fail | 0 |
| error | 1 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.SecretsManagerTest | 21 | 0 | 1 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `com.floci.test.SecretsManagerTest::rotateSecretStub` -- Warp's unified AWS endpoint does not emulate the service lambda; supported services: dynamodb, sqs, s3, sns, kinesis, secretsmanager, ssm, kms, sts (Service: Lambda, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
