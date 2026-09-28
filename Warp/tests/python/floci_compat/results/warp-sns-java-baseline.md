# Floci java / sns vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 2.3s

| status | count |
|---|---|
| pass | 0 |
| fail | 1 |
| error | 15 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.SnsTest | 0 | 1 | 15 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `com.floci.test.SnsTest::createTopic` -- sqswire does not implement CreateTopic (Service: Sns, Status Code: 400, Request ID: 89f71346-07ea-4d03-bbd0-3e36a85d0c88) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::listTopics` -- sqswire does not implement ListTopics (Service: Sns, Status Code: 400, Request ID: 34478c88-2f59-460e-97e6-6f810eda3fbb) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::getTopicAttributes` -- sqswire does not implement GetTopicAttributes (Service: Sns, Status Code: 400, Request ID: d91f3a2e-7ef6-4885-85bd-bb5c901716d3) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::subscribeSqs` -- sqswire does not implement Subscribe (Service: Sns, Status Code: 400, Request ID: bcaf7d5c-743b-4f67-b638-b13580d22692) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::listSubscriptionsByTopic` -- sqswire does not implement ListSubscriptionsByTopic (Service: Sns, Status Code: 400, Request ID: fb767022-a510-43ab-aac4-d0867b4b0e64) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::publish` -- sqswire does not implement Publish (Service: Sns, Status Code: 400, Request ID: 2e749baf-bcdf-4548-937e-09f239f4981f) (SDK Attempt Count: 1)
- [b] `com.floci.test.SnsTest::verifySqsDelivery` -- Expecting actual not to be empty
- [a] `com.floci.test.SnsTest::publishWithMessageAttributes` -- sqswire does not implement Publish (Service: Sns, Status Code: 400, Request ID: d5dcb9ca-56f5-4e0d-8d06-f3a36437fd85) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::rawMessageDelivery` -- sqswire does not implement Subscribe (Service: Sns, Status Code: 400, Request ID: f392a801-e001-4602-9792-c131cde724ac) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::unsubscribe` -- sqswire does not implement Unsubscribe (Service: Sns, Status Code: 400, Request ID: 21a00d02-6a1e-4ff4-afde-bcdc178f41c3) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::deleteTopic` -- sqswire does not implement DeleteTopic (Service: Sns, Status Code: 400, Request ID: 2ec312a8-4b33-491f-ab00-90492b0a604a) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::fifoExplicitDedup` -- sqswire does not implement CreateTopic (Service: Sns, Status Code: 400, Request ID: a7c0980f-610f-4640-bf8d-9775ba8fc8db) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::fifoContentBasedDedup` -- sqswire does not implement CreateTopic (Service: Sns, Status Code: 400, Request ID: 31743b93-26cd-4af8-becf-1d0d6b5e3b1e) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::filterPolicyScope_messageBody_topLevelMatch` -- sqswire does not implement CreateTopic (Service: Sns, Status Code: 400, Request ID: 57d0a139-7535-41d4-8e8b-105415d68ed0) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::filterPolicyScope_messageBody_nestedKeyDescent` -- sqswire does not implement CreateTopic (Service: Sns, Status Code: 400, Request ID: 76a8292c-af12-42ad-9725-4767e8d9df25) (SDK Attempt Count: 1)
- [a] `com.floci.test.SnsTest::publishBatchPreservesPerEntryMessageAttributes` -- sqswire does not implement CreateTopic (Service: Sns, Status Code: 400, Request ID: d80114ee-4011-4980-b13e-861420d1e424) (SDK Attempt Count: 1)
