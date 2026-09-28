# Floci node / kms vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.5s

| status | count |
|---|---|
| pass | 0 |
| fail | 13 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/kms-features.test.ts | 0 | 6 | 0 | 0 |
| tests/kms.test.ts | 0 | 7 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `tests/kms-features.test.ts::KMS features (#258 #259 #269) > #269: CreateKey with Tags stores tags immediately` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [a] `tests/kms-features.test.ts::KMS features (#258 #259 #269) > #269: CreateKey without Tags has empty tag list` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [b] `tests/kms-features.test.ts::KMS features (#258 #259 #269) > #258: CreateKey without Policy returns a non-empty default policy` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [b] `tests/kms-features.test.ts::KMS features (#258 #259 #269) > #258: CreateKey with Policy stores and returns that policy` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [b] `tests/kms-features.test.ts::KMS features (#258 #259 #269) > #259: PutKeyPolicy updates the key policy` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [b] `tests/kms-features.test.ts::KMS features (#258 #259 #269) > #259: PutKeyPolicy round-trip — get, change, verify` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [b] `tests/kms.test.ts::KMS > should create key` -- Missing or unrecognized X-Amz-Target header: TrentService.CreateKey
- [b] `tests/kms.test.ts::KMS > should describe key` -- Missing or unrecognized X-Amz-Target header: TrentService.DescribeKey
- [b] `tests/kms.test.ts::KMS > should list keys` -- Missing or unrecognized X-Amz-Target header: TrentService.ListKeys
- [b] `tests/kms.test.ts::KMS > should encrypt data` -- Missing or unrecognized X-Amz-Target header: TrentService.Encrypt
- [b] `tests/kms.test.ts::KMS > should decrypt data` -- Missing or unrecognized X-Amz-Target header: TrentService.Decrypt
- [b] `tests/kms.test.ts::KMS > should generate data key` -- Missing or unrecognized X-Amz-Target header: TrentService.GenerateDataKey
- [b] `tests/kms.test.ts::KMS > should generate random bytes` -- Missing or unrecognized X-Amz-Target header: TrentService.GenerateRandom
