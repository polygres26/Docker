"""dynamowire: real DynamoDB HTTP/JSON API, real boto3 client, translated to Postgres
underneath. Dummy static credentials are enough since AWS SigV4 request verification is opt-in
(WARP_AWS_IAM_CREDENTIALS, unset in the test compose file).
"""
import uuid

import boto3

from conftest import HOST, DYNAMOWIRE_PORT


def client():
    return boto3.client(
        "dynamodb",
        endpoint_url=f"http://{HOST}:{DYNAMOWIRE_PORT}",
        region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
    )


def test_create_table_put_get_item():
    c = client()
    table = f"smoke_{uuid.uuid4().hex[:8]}"
    c.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        c.put_item(TableName=table, Item={"id": {"S": "1"}, "name": {"S": "warp"}})
        item = c.get_item(TableName=table, Key={"id": {"S": "1"}})["Item"]
        assert item["name"]["S"] == "warp"
    finally:
        c.delete_table(TableName=table)
