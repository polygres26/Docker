# Floci node / sns vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.7s

| status | count |
|---|---|
| pass | 0 |
| fail | 10 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/sns.test.ts | 0 | 10 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `tests/sns.test.ts::SNS > should create a topic` -- sqswire does not implement CreateTopic
- [a] `tests/sns.test.ts::SNS > should list topics` -- sqswire does not implement ListTopics
- [a] `tests/sns.test.ts::SNS > should subscribe SQS to topic` -- sqswire does not implement Subscribe
- [a] `tests/sns.test.ts::SNS > should list subscriptions by topic` -- sqswire does not implement ListSubscriptionsByTopic
- [a] `tests/sns.test.ts::SNS > should get subscription attributes` -- sqswire does not implement GetSubscriptionAttributes
- [a] `tests/sns.test.ts::SNS > should set subscription attributes` -- sqswire does not implement SetSubscriptionAttributes
- [a] `tests/sns.test.ts::SNS > should publish message` -- sqswire does not implement Publish
- [a] `tests/sns.test.ts::SNS > should publish batch messages` -- sqswire does not implement PublishBatch
- [a] `tests/sns.test.ts::SNS > should unsubscribe` -- sqswire does not implement Unsubscribe
- [a] `tests/sns.test.ts::SNS > should delete topic` -- sqswire does not implement DeleteTopic
