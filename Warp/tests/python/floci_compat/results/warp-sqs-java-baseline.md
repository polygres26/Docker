# Floci java / sqs vs warp (2026-09-25 13:38)

endpoint `http://localhost:50816`, runner rc=1, 1.9s

| status | count |
|---|---|
| pass | 8 |
| fail | 3 |
| error | 16 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.SqsTest | 8 | 3 | 10 | 0 |
| com.floci.test.SqsMd5Test | 0 | 0 | 6 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `com.floci.test.SqsTest::sendMessageBatch` -- sqswire does not implement SendMessageBatch (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SqsTest::deleteMessageBatch` -- sqswire does not implement DeleteMessageBatch (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SqsTest::tagQueue` -- sqswire does not implement TagQueue (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SqsTest::listQueueTags` -- sqswire does not implement ListQueueTags (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SqsTest::untagQueue` -- sqswire does not implement UntagQueue (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SqsTest::changeMessageVisibilityBatch` -- sqswire does not implement ChangeMessageVisibilityBatch (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SqsTest::messageAttributesString` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "0d2b66022a021b692df4bd9c923ff80d", MD5 checksum returned: "null")
- [b] `com.floci.test.SqsTest::messageAttributesBinary` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "eb949ffb0fde53eb7625b04de10758ed", MD5 checksum returned: "null")
- [a] `com.floci.test.SqsTest::longPolling` -- Expecting actual:
- [a] `com.floci.test.SqsTest::dlqRouting` -- Expecting empty but was: [Message(MessageId=6, ReceiptHandle=f276a892-e818-46d8-bc60-264f5fdf6c8c, MD5OfBody=b0b465d463b138fae9035901814a5e6e, Body=dlq-test, Attributes={ApproximateReceiveCount=1})]
- [a] `com.floci.test.SqsTest::listDeadLetterSourceQueues` -- sqswire does not implement ListDeadLetterSourceQueues (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SqsTest::startMessageMoveTask` -- sqswire does not implement StartMessageMoveTask (Service: Sqs, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SqsTest::fifoReceiveReturnsMultipleMessagesFromSameGroupInOneCall` -- [FIFO ReceiveMessage must return all 3 messages from group g1 in one call]
- [b] `com.floci.test.SqsMd5Test::fifoQueueBinaryAttributePassesSdkMd5Validation` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "bd4e38e0800871ea2e997d6ad3236a62", MD5 checksum returned: "null")
- [b] `com.floci.test.SqsMd5Test::fifoQueueSendMessageWithAttributesPassesSdkMd5Validation` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "cfd821c7862ee66f6dbf2e411c55ff08", MD5 checksum returned: "null")
- [b] `com.floci.test.SqsMd5Test::fifoDedupReplayReturnsOriginalIdsAndRecomputedMd5` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "bf45014b66c69bac3163e48f7552da72", MD5 checksum returned: "null")
- [b] `com.floci.test.SqsMd5Test::highThroughputFifoQueueSendMessageWithAttributesPassesSdkMd5Validation` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "80176cdee1a774a6892d24cec267ac2f", MD5 checksum returned: "null")
- [b] `com.floci.test.SqsMd5Test::standardQueueSendMessageWithAttributesPassesSdkMd5Validation` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "cfd821c7862ee66f6dbf2e411c55ff08", MD5 checksum returned: "null")
- [b] `com.floci.test.SqsMd5Test::fifoQueueCustomTypeNamePassesSdkMd5Validation` -- MD5 returned by SQS does not match the calculation on the original request. (MD5 calculated by the message attributes: "7818dd9025affc7e1aa978a97e9e38fe", MD5 checksum returned: "null")
