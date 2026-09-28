# Floci node / sts vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.4s

| status | count |
|---|---|
| pass | 0 |
| fail | 2 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/sts.test.ts | 0 | 2 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `tests/sts.test.ts::STS > should get caller identity` -- sqswire does not implement GetCallerIdentity
- [a] `tests/sts.test.ts::STS > should assume role` -- sqswire does not implement AssumeRole
