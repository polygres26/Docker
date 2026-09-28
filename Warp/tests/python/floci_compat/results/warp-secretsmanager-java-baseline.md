# Floci java / secretsmanager vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 1.8s

| status | count |
|---|---|
| pass | 1 |
| fail | 1 |
| error | 17 |
| skip | 3 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.SecretsManagerTest | 1 | 1 | 17 | 3 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `com.floci.test.SecretsManagerTest::createSecret` -- Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::getSecretValueByName` -- Missing or unrecognized X-Amz-Target header: secretsmanager.GetSecretValue (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::putSecretValue` -- Missing or unrecognized X-Amz-Target header: secretsmanager.PutSecretValue (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::getSecretValueAfterPut` -- Missing or unrecognized X-Amz-Target header: secretsmanager.GetSecretValue (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::describeSecret` -- Missing or unrecognized X-Amz-Target header: secretsmanager.DescribeSecret (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::updateSecretDescription` -- Missing or unrecognized X-Amz-Target header: secretsmanager.UpdateSecret (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::listSecrets` -- Missing or unrecognized X-Amz-Target header: secretsmanager.ListSecrets (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SecretsManagerTest::tagResource` -- Missing or unrecognized X-Amz-Target header: secretsmanager.TagResource (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SecretsManagerTest::untagResource` -- Missing or unrecognized X-Amz-Target header: secretsmanager.UntagResource (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::listSecretVersionIds` -- Missing or unrecognized X-Amz-Target header: secretsmanager.ListSecretVersionIds (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::rotateSecretStub` -- Service returned HTTP status code 400 (Service: Lambda, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::kmsKeyIdPreservation` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::createSecretWithAnUnknownKmsKeyIsRejected` -- Expecting actual throwable to be an instance of:
- [b] `com.floci.test.SecretsManagerTest::createSecretDuplicateThrows400` -- Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SecretsManagerTest::getRandomPassword` -- Missing or unrecognized X-Amz-Target header: secretsmanager.GetRandomPassword (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [c] `com.floci.test.SecretsManagerTest::getSecretValueByPartialArnWithSlashesInName` -- Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SecretsManagerTest::batchGetSecretValue` -- Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SecretsManagerTest::batchGetSecretValuePartialErrors` -- Missing or unrecognized X-Amz-Target header: secretsmanager.CreateSecret (Service: SecretsManager, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
