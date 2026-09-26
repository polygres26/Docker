# Floci java / sts vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 2.9s

| status | count |
|---|---|
| pass | 0 |
| fail | 0 |
| error | 1 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.StsTest | 0 | 0 | 1 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `com.floci.test.StsTest::` -- sqswire does not implement CreateSAMLProvider (Service: Iam, Status Code: 400, Request ID: 13cdd2ab-22e7-4695-8397-433436360db4) (SDK Attempt Count: 1)
