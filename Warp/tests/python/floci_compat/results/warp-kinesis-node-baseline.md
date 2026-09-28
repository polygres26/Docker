# Floci node / kinesis vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.4s

| status | count |
|---|---|
| pass | 0 |
| fail | 6 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/kinesis.test.ts | 0 | 6 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [c] `tests/kinesis.test.ts::Kinesis > should create stream` -- Protocol error
- [c] `tests/kinesis.test.ts::Kinesis > should describe stream` -- Protocol error
- [c] `tests/kinesis.test.ts::Kinesis > should list streams` -- Protocol error
- [c] `tests/kinesis.test.ts::Kinesis > should put record` -- Protocol error
- [c] `tests/kinesis.test.ts::Kinesis > should get records` -- Protocol error
- [c] `tests/kinesis.test.ts::Kinesis > should delete stream` -- Protocol error
