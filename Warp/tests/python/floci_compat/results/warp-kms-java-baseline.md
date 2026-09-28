# Floci java / kms vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 1.8s

| status | count |
|---|---|
| pass | 0 |
| fail | 0 |
| error | 55 |
| skip | 2 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.KmsFeaturesTest | 0 | 0 | 24 | 0 |
| com.floci.test.KmsSm2Test | 0 | 0 | 6 | 0 |
| com.floci.test.KmsTest | 0 | 0 | 22 | 2 |
| com.floci.test.KmsGrantLifecycleTest | 0 | 0 | 3 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `com.floci.test.KmsFeaturesTest::createKeyWithTagsStoresTags` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.KmsFeaturesTest::createKeyWithoutTagsHasEmptyTagList` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::createKeyWithoutPolicyReturnsDefaultPolicy` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::createKeyWithPolicyStoresAndReturnsPolicy` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::putKeyPolicyUpdatesPolicy` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::putKeyPolicyRoundTrip` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::describeSymmetricKeyReturnsNoEncryptionAlgorithms` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::describeAsymmetricRsaSignKeyReturnsSigningAlgorithms` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::describeHmacKeyReturnsMacAlgorithms` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::mlDsaCreateGetPublicKeySignAndVerifyThroughSdk(KeySpec)[1]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::mlDsaCreateGetPublicKeySignAndVerifyThroughSdk(KeySpec)[2]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::mlDsaCreateGetPublicKeySignAndVerifyThroughSdk(KeySpec)[3]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::decryptWithMatchingKeyIdReturnsPlaintext` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::decryptWithMismatchedKeyIdRaisesIncorrectKeyException` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::reEncryptWithMatchingSourceKeyIdReturnsNewCiphertext` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::reEncryptWithMismatchedSourceKeyIdRaisesIncorrectKeyException` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::encryptWithDisabledKeyRaisesDisabledException` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::decryptWithDisabledKeyRaisesDisabledException` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::encryptWithPendingDeletionKeyRaisesKmsInvalidStateException` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::decryptWithPendingDeletionKeyRaisesKmsInvalidStateException` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::rsaOaepEncryptDecryptRoundTrip` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::decryptAcceptsRsaOaepCiphertextMadeWithGetPublicKey` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsFeaturesTest::rsaEncryptWithDefaultAlgorithmRaisesInvalidKeyUsage` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [c] `com.floci.test.KmsFeaturesTest::importedKeyMaterialMakesAnExternalKeyUsable` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsSm2Test::signAndVerifyUseSm2dsaWireAlgorithm(String)[1]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsSm2Test::signAndVerifyUseSm2dsaWireAlgorithm(String)[2]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsSm2Test::signAndVerifyRejectInvalidSm2Parameters(String, String, MessageType, String)[1]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsSm2Test::signAndVerifyRejectInvalidSm2Parameters(String, String, MessageType, String)[2]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsSm2Test::signAndVerifyRejectInvalidSm2Parameters(String, String, MessageType, String)[3]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsSm2Test::signAndVerifyRejectInvalidSm2Parameters(String, String, MessageType, String)[4]` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::createKey` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::describeKey` -- Missing or unrecognized X-Amz-Target header: TrentService.DescribeKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::createAlias` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateAlias (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::listAliases` -- Missing or unrecognized X-Amz-Target header: TrentService.ListAliases (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::encrypt` -- Missing or unrecognized X-Amz-Target header: TrentService.Encrypt (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::encryptUsingAlias` -- Missing or unrecognized X-Amz-Target header: TrentService.Encrypt (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::generateDataKey` -- Missing or unrecognized X-Amz-Target header: TrentService.GenerateDataKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.KmsTest::tagging` -- Missing or unrecognized X-Amz-Target header: TrentService.TagResource (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::generateDataKeyWithoutPlaintext` -- Missing or unrecognized X-Amz-Target header: TrentService.GenerateDataKeyWithoutPlaintext (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::signAndVerify` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::signAndVerifyRSA` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::signWithDigest` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::getPublicKey` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::scheduleKeyDeletion` -- Missing or unrecognized X-Amz-Target header: TrentService.ScheduleKeyDeletion (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::deleteAlias` -- Missing or unrecognized X-Amz-Target header: TrentService.DeleteAlias (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::signAndVerifySecp256k1` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::describeSignVerifyRsaKey` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::generateRandom` -- Missing or unrecognized X-Amz-Target header: TrentService.GenerateRandom (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::describeSignVerifyEcKey` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::describeSignVerifyOtherKey` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::describeHmacKey` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsTest::generateAndVerifyMac` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsGrantLifecycleTest::createListGrantRoundTrip` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsGrantLifecycleTest::createListRetireGrantRoundTrip` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.KmsGrantLifecycleTest::createListRevokeGrantRoundTrip` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey (Service: Kms, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
