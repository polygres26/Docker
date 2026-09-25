# Floci python / sqs vs warp (2026-09-25 13:34)

endpoint `http://localhost:50816`, runner rc=1, 0.4s

| status | count |
|---|---|
| pass | 7 |
| fail | 9 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_sqs.py | 7 | 9 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `test_sqs.py::test_send_message_batch` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the SendMessageBatch operation: sqswire does not implement SendMessageBatch
- [a] `test_sqs.py::test_delete_message_batch` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the SendMessageBatch operation: sqswire does not implement SendMessageBatch
- [b] `test_sqs.py::test_message_attributes` -- AssertionError: assert None == 'myval'
- [a] `test_sqs.py::test_tag_queue` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the TagQueue operation: sqswire does not implement TagQueue
- [a] `test_sqs.py::test_list_queue_tags` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the TagQueue operation: sqswire does not implement TagQueue
- [a] `test_sqs.py::test_untag_queue` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the TagQueue operation: sqswire does not implement TagQueue
- [a] `test_sqs.py::test_long_polling` -- assert 0.0013699531555175781 >= 1.8
- [a] `test_sqs.py::test_dlq_routing` -- KeyError: 'QueueArn'
- [b] `test_sqs.py::test_list_dead_letter_source_queues` -- KeyError: 'QueueArn'
