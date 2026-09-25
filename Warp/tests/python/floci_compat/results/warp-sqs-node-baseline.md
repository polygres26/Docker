# Floci node / sqs vs warp (2026-09-25 13:34)

endpoint `http://localhost:50816`, runner rc=1, 0.5s

| status | count |
|---|---|
| pass | 6 |
| fail | 2 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/sqs.test.ts | 6 | 2 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `tests/sqs.test.ts::SQS > should get queue attributes` -- expected undefined to be truthy
- [a] `tests/sqs.test.ts::SQS > should send batch messages` -- sqswire does not implement SendMessageBatch
