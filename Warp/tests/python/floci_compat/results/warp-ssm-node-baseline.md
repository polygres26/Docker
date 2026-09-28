# Floci node / ssm vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.5s

| status | count |
|---|---|
| pass | 1 |
| fail | 6 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/ssm.test.ts | 1 | 6 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `tests/ssm.test.ts::SSM > should put and get a String parameter` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [b] `tests/ssm.test.ts::SSM > should put and get a SecureString parameter` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [b] `tests/ssm.test.ts::SSM > should get parameters by path` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.GetParametersByPath
- [b] `tests/ssm.test.ts::SSM > should describe parameters` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.DescribeParameters
- [b] `tests/ssm.test.ts::SSM > should overwrite parameter` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter
- [b] `tests/ssm.test.ts::SSM > should delete parameter` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.DeleteParameter
