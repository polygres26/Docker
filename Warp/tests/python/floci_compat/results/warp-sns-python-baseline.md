# Floci python / sns vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 0.3s

| status | count |
|---|---|
| pass | 0 |
| fail | 10 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_sns.py | 0 | 10 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `test_sns.py::test_create_topic` -- UnboundLocalError: local variable 'response' referenced before assignment
- [a] `test_sns.py::test_list_topics` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_get_topic_attributes` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_delete_topic` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_subscribe_sqs` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_list_subscriptions_by_topic` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_unsubscribe` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_publish` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_publish_sqs_delivery` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
- [a] `test_sns.py::test_publish_with_message_attributes` -- botocore.exceptions.ClientError: An error occurred (UnknownOperationException) when calling the CreateTopic operation: sqswire does not implement CreateTopic
