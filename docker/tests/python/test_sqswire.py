"""sqswire: real Amazon SQS HTTP/JSON API, real boto3 client, translated to Postgres underneath
(pgmq-style storage -- no `pgmq` extension needed). Dummy static credentials are enough since
AWS SigV4 request verification is opt-in.
"""
import uuid

import boto3

from conftest import HOST, SQSWIRE_PORT


def client():
    return boto3.client(
        "sqs",
        endpoint_url=f"http://{HOST}:{SQSWIRE_PORT}",
        region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
    )


def test_create_send_receive_delete():
    c = client()
    queue_name = f"smoke-{uuid.uuid4().hex[:8]}"
    queue_url = c.create_queue(QueueName=queue_name)["QueueUrl"]
    try:
        c.send_message(QueueUrl=queue_url, MessageBody="hello from warp")
        messages = c.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2).get("Messages", [])
        assert len(messages) == 1
        assert messages[0]["Body"] == "hello from warp"
        c.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]["ReceiptHandle"])
    finally:
        c.delete_queue(QueueUrl=queue_url)
