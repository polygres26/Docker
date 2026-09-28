"""SNS, Kinesis, Secrets Manager, SSM Parameter Store, KMS, STS and the unified AWS endpoint (awswire), with boto3 against a real
Warp and native Postgres (WARP_TEST_PG_LOCAL=1), on ONE backend and on TWO sharded backends.

Covers what Floci's SDK suites (run separately, see floci_compat/) do not: SNS fan-out into SQS queues end to end (envelope and raw
delivery, filter policies, FIFO dedup, DLQ redrive) and to real HTTP endpoints (confirmation flow, headers), Kinesis strict per-shard
sequence ordering under concurrent producers, resharding and the retention sweeper, that secrets/SecureString values and KMS key
material are never in Postgres in clear, that KMS fails closed without a master key, SigV4 validation with STS temporary credentials,
that data lands on both Postgres hosts, and WARP_POOL_MAX_SIZE=4 starvation with slow HTTP subscribers.
"""
import base64
import concurrent.futures
import http.server
import json
import threading
import time
import uuid

import boto3
import pytest
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from awsextras_warp_support import sql, start_aws_warp
from warp_test_support import RealPostgres


def client(service, warp, key="test", secret="test", token=None, region="us-east-1"):
    kw = dict(endpoint_url=warp.endpoint, region_name=region, aws_access_key_id=key, aws_secret_access_key=secret,
              config=Config(retries={"max_attempts": 0}))
    if token:
        kw["aws_session_token"] = token
    return boto3.client(service, **kw)


def uniq(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module", params=["one", "two"])
def topo(request):
    pgs = [RealPostgres()] + ([RealPostgres()] if request.param == "two" else [])
    warp = start_aws_warp(pgs[0], pgs[1] if len(pgs) == 2 else None)
    yield warp, pgs
    warp.close()
    for p in pgs:
        p.close()


@pytest.fixture
def warp(topo):
    return topo[0]


@pytest.fixture
def pgs(topo):
    return topo[1]


def total(pgs, table, where="true"):
    return sum(sql(p, f"SELECT count(*) FROM {table} WHERE {where}")[0][0] for p in pgs)


# ------------------------------------------------------------------------------------------- unified endpoint

def test_unified_endpoint_serves_every_service_and_health(warp):
    h = requests.get(warp.endpoint + "/_warp/health", timeout=10).json()
    assert {"dynamodb", "sqs", "sns", "kinesis", "secretsmanager", "ssm", "kms", "sts", "iam"} <= set(h["services"])
    assert h["services"]["sns"] == "running" and h["services"]["kinesis"] == "running"
    ddb = client("dynamodb", warp)
    name = uniq("t")
    ddb.create_table(TableName=name, KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                     AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    ddb.put_item(TableName=name, Item={"id": {"S": "a"}})
    assert ddb.get_item(TableName=name, Key={"id": {"S": "a"}})["Item"]["id"]["S"] == "a"
    sqs = client("sqs", warp)
    q = sqs.create_queue(QueueName=uniq("q"))["QueueUrl"]
    assert q.startswith(warp.endpoint), "queue URLs point at the unified endpoint"
    sqs.send_message(QueueUrl=q, MessageBody="x")
    assert sqs.receive_message(QueueUrl=q, WaitTimeSeconds=1)["Messages"][0]["Body"] == "x"
    assert client("sts", warp).get_caller_identity()["Account"] == "000000000000"


def test_unknown_service_is_refused_not_sent_to_s3(warp):
    with pytest.raises(ClientError) as e:
        client("lambda", warp).list_functions()
    assert "does not emulate" in str(e.value) or e.value.response["Error"]["Code"] == "UnknownServiceException"


# ------------------------------------------------------------------------------------------------------- SNS

def mkqueue(warp, name=None, **attrs):
    sqs = client("sqs", warp)
    url = sqs.create_queue(QueueName=name or uniq("sq"), Attributes=attrs)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def drain(warp, url, want=1, wait=3):
    sqs = client("sqs", warp)
    out = []
    end = time.time() + wait
    while len(out) < want and time.time() < end:
        for m in sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                     MessageAttributeNames=["All"], AttributeNames=["All"]).get("Messages", []):
            out.append(m)
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    return out


def test_sns_topics_lifecycle_tags_attributes_and_pagination(warp):
    sns = client("sns", warp)
    names = [uniq("topic") for _ in range(6)]
    arns = [sns.create_topic(Name=n, Tags=[{"Key": "env", "Value": "t"}])["TopicArn"] for n in names]
    assert sns.create_topic(Name=names[0])["TopicArn"] == arns[0], "CreateTopic is idempotent"
    with pytest.raises(ClientError) as e:
        sns.create_topic(Name=names[0], Attributes={"DisplayName": "different"})
    assert e.value.response["Error"]["Code"] == "InvalidParameter"
    listed = [t["TopicArn"] for t in sns.list_topics()["Topics"]]
    assert set(arns) <= set(listed) and listed == sorted(listed)
    sns.set_topic_attributes(TopicArn=arns[0], AttributeName="DisplayName", AttributeValue="shown")
    a = sns.get_topic_attributes(TopicArn=arns[0])["Attributes"]
    assert a["DisplayName"] == "shown" and a["SubscriptionsConfirmed"] == "0" and "Policy" in a
    assert sns.list_tags_for_resource(ResourceArn=arns[1])["Tags"] == [{"Key": "env", "Value": "t"}]
    sns.untag_resource(ResourceArn=arns[1], TagKeys=["env"])
    assert sns.list_tags_for_resource(ResourceArn=arns[1])["Tags"] == []
    with pytest.raises(ClientError) as e:
        sns.publish(TopicArn=arns[0].replace(names[0], "missing"), Message="x")
    assert e.value.response["Error"]["Code"] == "NotFound"
    for a in arns:
        sns.delete_topic(TopicArn=a)
    assert not set(arns) & {t["TopicArn"] for t in sns.list_topics()["Topics"]}


def test_sns_topics_are_spread_over_both_hosts_and_each_lives_on_one(warp, pgs):
    sns = client("sns", warp)
    made = [sns.create_topic(Name=uniq("spread"))["TopicArn"] for _ in range(24)]
    rows = [sql(p, "SELECT count(*) FROM warp_sns_topics WHERE name LIKE 'spread-%'")[0][0] for p in pgs]
    assert sum(rows) >= 24
    if len(pgs) == 2:
        assert min(rows) >= 4, rows
    listed = {t["TopicArn"] for t in sns.list_topics()["Topics"]}
    assert set(made) <= listed
    for arn in made:
        sns.delete_topic(TopicArn=arn)


def test_sns_fanout_into_sqs_envelope_raw_and_attributes(warp):
    sns = client("sns", warp)
    topic = sns.create_topic(Name=uniq("fan"))["TopicArn"]
    (q1, a1), (q2, a2) = mkqueue(warp), mkqueue(warp)
    sub1 = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=a1)["SubscriptionArn"]
    sub2 = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=a2)["SubscriptionArn"]
    assert sub1.startswith(topic + ":")
    assert sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=a1)["SubscriptionArn"] == sub1, "Subscribe is idempotent"
    sns.set_subscription_attributes(SubscriptionArn=sub2, AttributeName="RawMessageDelivery", AttributeValue="true")
    mid = sns.publish(TopicArn=topic, Message="hello fan", Subject="subj",
                      MessageAttributes={"color": {"DataType": "String", "StringValue": "blue"},
                                         "n": {"DataType": "Number", "StringValue": "42"}})["MessageId"]
    env = json.loads(drain(warp, q1)[0]["Body"])
    assert env["Type"] == "Notification" and env["MessageId"] == mid and env["TopicArn"] == topic
    assert env["Message"] == "hello fan" and env["Subject"] == "subj"
    assert env["MessageAttributes"]["color"] == {"Type": "String", "Value": "blue"}
    assert env["MessageAttributes"]["n"] == {"Type": "Number", "Value": "42"}
    assert env["SignatureVersion"] == "1" and len(base64.b64decode(env["Signature"])) >= 256
    assert env["SigningCertURL"].startswith("https://sns.") and "Timestamp" in env and "UnsubscribeURL" in env
    raw = drain(warp, q2)[0]
    assert raw["Body"] == "hello fan"
    assert raw["MessageAttributes"]["color"]["StringValue"] == "blue"
    assert raw["MessageAttributes"]["n"]["DataType"] == "Number"
    attrs = sns.get_subscription_attributes(SubscriptionArn=sub2)["Attributes"]
    assert attrs["RawMessageDelivery"] == "true" and attrs["Protocol"] == "sqs" and attrs["TopicArn"] == topic
    assert len(sns.list_subscriptions_by_topic(TopicArn=topic)["Subscriptions"]) == 2
    sns.unsubscribe(SubscriptionArn=sub1)
    sns.publish(TopicArn=topic, Message="after unsubscribe")
    assert drain(warp, q1, wait=1) == []
    assert drain(warp, q2)[0]["Body"] == "after unsubscribe"


def test_sns_filter_policies_attribute_and_body_scope(warp):
    sns = client("sns", warp)
    topic = sns.create_topic(Name=uniq("flt"))["TopicArn"]
    qa, aa = mkqueue(warp)
    qb, ab = mkqueue(warp)
    sa = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=aa,
                       Attributes={"FilterPolicy": json.dumps({"kind": ["order"], "n": [{"numeric": [">", 5]}]})})["SubscriptionArn"]
    sb = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=ab,
                       Attributes={"FilterPolicyScope": "MessageBody", "FilterPolicy": json.dumps({"store": {"city": ["seattle"]}})})["SubscriptionArn"]
    with pytest.raises(ClientError) as e:
        sns.set_subscription_attributes(SubscriptionArn=sa, AttributeName="FilterPolicy", AttributeValue='{"a":[{"bogus":1}]}')
    assert e.value.response["Error"]["Code"] == "InvalidParameter"
    sns.publish(TopicArn=topic, Message=json.dumps({"store": {"city": "seattle"}}),
                MessageAttributes={"kind": {"DataType": "String", "StringValue": "order"}, "n": {"DataType": "Number", "StringValue": "9"}})
    sns.publish(TopicArn=topic, Message=json.dumps({"store": {"city": "boston"}}),
                MessageAttributes={"kind": {"DataType": "String", "StringValue": "order"}, "n": {"DataType": "Number", "StringValue": "1"}})
    got_a, got_b = drain(warp, qa, want=2, wait=2), drain(warp, qb, want=2, wait=2)
    assert len(got_a) == 1 and "seattle" in got_a[0]["Body"]
    assert len(got_b) == 1 and "seattle" in got_b[0]["Body"]


def test_sns_message_structure_json_and_batch_and_validation(warp):
    sns = client("sns", warp)
    topic = sns.create_topic(Name=uniq("ms"))["TopicArn"]
    q, a = mkqueue(warp)
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=a, Attributes={"RawMessageDelivery": "true"})
    sns.publish(TopicArn=topic, MessageStructure="json", Message=json.dumps({"default": "dflt", "sqs": "for-sqs"}))
    assert drain(warp, q)[0]["Body"] == "for-sqs"
    with pytest.raises(ClientError) as e:
        sns.publish(TopicArn=topic, MessageStructure="json", Message=json.dumps({"sqs": "no default"}))
    assert e.value.response["Error"]["Code"] == "InvalidParameterValue"
    r = sns.publish_batch(TopicArn=topic, PublishBatchRequestEntries=[{"Id": str(i), "Message": f"b{i}"} for i in range(10)])
    assert len(r["Successful"]) == 10 and not r["Failed"]
    assert sorted(m["Body"] for m in drain(warp, q, want=10)) == sorted(f"b{i}" for i in range(10))
    with pytest.raises(ClientError) as e:
        sns.publish_batch(TopicArn=topic, PublishBatchRequestEntries=[{"Id": "1", "Message": "a"}, {"Id": "1", "Message": "b"}])
    assert e.value.response["Error"]["Code"] == "BatchEntryIdsNotDistinct"
    with pytest.raises(ClientError) as e:
        sns.publish(TopicArn=topic, Message="x" * 262145)
    assert e.value.response["Error"]["Code"] == "InvalidParameter"


def test_sns_fifo_topic_dedup_window_and_group_ordering(warp):
    sns = client("sns", warp)
    sqs = client("sqs", warp)
    qurl = sqs.create_queue(QueueName=uniq("f") + ".fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    qarn = sqs.get_queue_attributes(QueueUrl=qurl, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    topic = sns.create_topic(Name=uniq("t") + ".fifo", Attributes={"FifoTopic": "true"})["TopicArn"]
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)
    with pytest.raises(ClientError) as e:
        sns.publish(TopicArn=topic, Message="no group")
    assert e.value.response["Error"]["Code"] == "InvalidParameter"
    r1 = sns.publish(TopicArn=topic, Message="one", MessageGroupId="g", MessageDeduplicationId="d1")
    r2 = sns.publish(TopicArn=topic, Message="one again", MessageGroupId="g", MessageDeduplicationId="d1")
    assert r1["MessageId"] == r2["MessageId"] and r1["SequenceNumber"] == r2["SequenceNumber"], "duplicate returns the original"
    for i in range(2, 6):
        sns.publish(TopicArn=topic, Message=f"m{i}", MessageGroupId="g", MessageDeduplicationId=f"d{i}")
    bodies = []
    for _ in range(10):
        for m in sqs.receive_message(QueueUrl=qurl, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", []):
            bodies.append(json.loads(m["Body"])["Message"])
            sqs.delete_message(QueueUrl=qurl, ReceiptHandle=m["ReceiptHandle"])
        if len(bodies) >= 5:
            break
    assert bodies == ["one", "m2", "m3", "m4", "m5"], bodies
    with pytest.raises(ClientError):
        sns.create_topic(Name=uniq("t"), Attributes={"FifoTopic": "true"})


def test_sns_delivery_to_a_missing_queue_goes_to_the_subscription_dlq(warp):
    sns = client("sns", warp)
    sqs = client("sqs", warp)
    topic = sns.create_topic(Name=uniq("dlq"))["TopicArn"]
    dq, da = mkqueue(warp)
    gone_url, gone_arn = mkqueue(warp)
    sub = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=gone_arn,
                        Attributes={"RedrivePolicy": json.dumps({"deadLetterTargetArn": da})})["SubscriptionArn"]
    sqs.delete_queue(QueueUrl=gone_url)
    sns.publish(TopicArn=topic, Message="lost")
    got = drain(warp, dq)
    assert got and json.loads(got[0]["Body"])["Message"] == "lost"
    with pytest.raises(ClientError):
        sns.set_subscription_attributes(SubscriptionArn=sub, AttributeName="RedrivePolicy", AttributeValue="{not json")


class Hook(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, delay=0.0, status=200):
        self.requests = []
        self.delay = delay
        self.status = status
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
                outer.requests.append((dict((k.lower(), v) for k, v in self.headers.items()), body))
                if outer.delay:
                    time.sleep(outer.delay)
                self.send_response(outer.status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *a):
                pass

        super().__init__(("127.0.0.1", 0), H)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_address[1]}/hook"

    def wait(self, n, timeout=10):
        end = time.time() + timeout
        while len(self.requests) < n and time.time() < end:
            time.sleep(0.05)
        return self.requests


def test_sns_http_endpoint_confirmation_flow_headers_and_raw(warp):
    sns = client("sns", warp)
    hook = Hook()
    try:
        topic = sns.create_topic(Name=uniq("http"))["TopicArn"]
        res = sns.subscribe(TopicArn=topic, Protocol="http", Endpoint=hook.url)["SubscriptionArn"]
        assert res == "pending confirmation"
        conf = hook.wait(1)
        assert conf, "SubscriptionConfirmation was POSTed"
        headers, body = conf[0]
        doc = json.loads(body)
        assert headers["x-amz-sns-message-type"] == "SubscriptionConfirmation" and headers["x-amz-sns-topic-arn"] == topic
        assert doc["Type"] == "SubscriptionConfirmation" and doc["Token"] and doc["TopicArn"] == topic
        assert sns.get_topic_attributes(TopicArn=topic)["Attributes"]["SubscriptionsPending"] == "1"
        sns.publish(TopicArn=topic, Message="before confirm")
        time.sleep(0.5)
        assert len(hook.requests) == 1, "a pending subscription receives nothing"
        # the subscriber confirms by GETting the SubscribeURL (a Query request on the same listener)
        assert requests.get(doc["SubscribeURL"], timeout=10).status_code == 200
        subs = sns.list_subscriptions_by_topic(TopicArn=topic)["Subscriptions"]
        assert subs[0]["SubscriptionArn"].startswith(topic + ":")
        sns.publish(TopicArn=topic, Message="hello http", Subject="s")
        h2, b2 = hook.wait(2)[1]
        assert h2["x-amz-sns-message-type"] == "Notification" and h2["x-amz-sns-topic-arn"] == topic
        assert h2["x-amz-sns-subscription-arn"].startswith(topic + ":") and h2["x-amz-sns-message-id"]
        assert json.loads(b2)["Message"] == "hello http" and json.loads(b2)["Signature"]
        sns.set_subscription_attributes(SubscriptionArn=subs[0]["SubscriptionArn"], AttributeName="RawMessageDelivery",
                                        AttributeValue="true")
        sns.publish(TopicArn=topic, Message="raw body")
        h3, b3 = hook.wait(3)[2]
        assert b3 == "raw body" and h3.get("x-amz-sns-rawdelivery") == "true"
    finally:
        hook.shutdown()


def test_sns_http_delivery_retries_then_gives_up_without_blocking_publish(warp):
    sns = client("sns", warp)
    hook = Hook(status=500)
    try:
        topic = sns.create_topic(Name=uniq("retry"))["TopicArn"]
        sns.subscribe(TopicArn=topic, Protocol="http", Endpoint=hook.url)
        conf = json.loads(hook.wait(1)[0][1])
        sns.confirm_subscription(TopicArn=topic, Token=conf["Token"])
        n0 = len(hook.requests)
        t0 = time.time()
        sns.publish(TopicArn=topic, Message="x")
        assert time.time() - t0 < 1.5, "Publish does not wait for HTTP delivery"
        assert len(hook.wait(n0 + 3, timeout=15)) >= n0 + 3, "three delivery attempts"
    finally:
        hook.shutdown()


def test_sns_record_only_protocols_and_platform_applications(warp, pgs):
    sns = client("sns", warp)
    topic = sns.create_topic(Name=uniq("rec"))["TopicArn"]
    lam = sns.subscribe(TopicArn=topic, Protocol="lambda", Endpoint="arn:aws:lambda:us-east-1:000000000000:function:f")["SubscriptionArn"]
    mail = sns.subscribe(TopicArn=topic, Protocol="email", Endpoint="a@example.com")["SubscriptionArn"]
    sms = sns.subscribe(TopicArn=topic, Protocol="sms", Endpoint="+15555550100")["SubscriptionArn"]
    assert lam.startswith(topic) and mail.startswith(topic) and sms.startswith(topic)
    sns.publish(TopicArn=topic, Message="recorded")
    assert total(pgs, "warp_sns_deliveries", "message_id IS NOT NULL AND payload LIKE '%recorded%'") == 3
    app = sns.create_platform_application(Name=uniq("app"), Platform="GCM", Attributes={"PlatformCredential": "k"})["PlatformApplicationArn"]
    ep = sns.create_platform_endpoint(PlatformApplicationArn=app, Token="tok1")["EndpointArn"]
    assert sns.create_platform_endpoint(PlatformApplicationArn=app, Token="tok1")["EndpointArn"] == ep
    assert sns.get_endpoint_attributes(EndpointArn=ep)["Attributes"]["Enabled"] == "true"
    assert sns.publish(TargetArn=ep, Message="push")["MessageId"]
    sns.set_endpoint_attributes(EndpointArn=ep, Attributes={"Enabled": "false"})
    with pytest.raises(ClientError) as e:
        sns.publish(TargetArn=ep, Message="push2")
    assert e.value.response["Error"]["Code"] == "EndpointDisabled"
    assert [x["EndpointArn"] for x in sns.list_endpoints_by_platform_application(PlatformApplicationArn=app)["Endpoints"]] == [ep]
    sns.delete_platform_application(PlatformApplicationArn=app)
    assert sns.publish(PhoneNumber="+15555550101", Message="sms")["MessageId"]


# ---------------------------------------------------------------------------------------------------- Kinesis

def test_kinesis_stream_lifecycle_shards_and_partition_key_routing(warp, pgs):
    k = client("kinesis", warp)
    name = uniq("stream")
    k.create_stream(StreamName=name, ShardCount=3)
    with pytest.raises(ClientError) as e:
        k.create_stream(StreamName=name, ShardCount=1)
    assert e.value.response["Error"]["Code"] == "ResourceInUseException"
    d = k.describe_stream(StreamName=name)["StreamDescription"]
    ranges = [(int(s["HashKeyRange"]["StartingHashKey"]), int(s["HashKeyRange"]["EndingHashKey"])) for s in d["Shards"]]
    assert ranges[0][0] == 0 and ranges[-1][1] == 2 ** 128 - 1
    assert all(ranges[i][1] + 1 == ranges[i + 1][0] for i in range(2)), "contiguous ranges"
    import hashlib
    for pk in ["a", "b", "some-key", "é"]:
        h = int.from_bytes(hashlib.md5(pk.encode()).digest(), "big")
        want = next(s["ShardId"] for s, r in zip(d["Shards"], ranges) if r[0] <= h <= r[1])
        assert k.put_record(StreamName=name, Data=b"x", PartitionKey=pk)["ShardId"] == want
    eh = k.put_record(StreamName=name, Data=b"y", PartitionKey="ignored", ExplicitHashKey=str(ranges[2][0]))
    assert eh["ShardId"] == d["Shards"][2]["ShardId"]
    summary = k.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]
    assert summary["OpenShardCount"] == 3 and summary["RetentionPeriodHours"] == 24
    # the whole stream is on exactly one host
    assert sorted(sql(p, "SELECT count(*) FROM warp_kinesis_shards WHERE stream = %s", (name,))[0][0] for p in pgs)[-1] == 3
    assert sum(sql(p, "SELECT count(*) FROM warp_kinesis_shards WHERE stream = %s", (name,))[0][0] for p in pgs) == 3
    k.increase_stream_retention_period(StreamName=name, RetentionPeriodHours=48)
    with pytest.raises(ClientError) as e:
        k.increase_stream_retention_period(StreamName=name, RetentionPeriodHours=24)
    assert e.value.response["Error"]["Code"] == "InvalidArgumentException"
    k.add_tags_to_stream(StreamName=name, Tags={"a": "1"})
    assert k.list_tags_for_stream(StreamName=name)["Tags"] == [{"Key": "a", "Value": "1"}]
    k.enable_enhanced_monitoring(StreamName=name, ShardLevelMetrics=["IncomingBytes"])
    assert k.describe_stream(StreamName=name)["StreamDescription"]["EnhancedMonitoring"][0]["ShardLevelMetrics"] == ["IncomingBytes"]
    k.delete_stream(StreamName=name)
    assert name not in k.list_streams()["StreamNames"]
    assert total(pgs, "warp_kinesis_records", f"stream = '{name}'") == 0


def read_all(k, name, shard_id, itype="TRIM_HORIZON", **kw):
    it = k.get_shard_iterator(StreamName=name, ShardId=shard_id, ShardIteratorType=itype, **kw)["ShardIterator"]
    out = []
    while True:
        r = k.get_records(ShardIterator=it, Limit=100)
        out += r["Records"]
        it = r["NextShardIterator"]
        if not r["Records"] and r["MillisBehindLatest"] == 0:
            return out


def test_kinesis_iterators_all_types_limits_and_sequence_numbers(warp):
    k = client("kinesis", warp)
    name = uniq("iters")
    k.create_stream(StreamName=name, ShardCount=1)
    shard = k.describe_stream(StreamName=name)["StreamDescription"]["Shards"][0]["ShardId"]
    seqs = [k.put_record(StreamName=name, Data=f"r{i}".encode(), PartitionKey="pk")["SequenceNumber"] for i in range(10)]
    assert seqs == sorted(seqs) and len(set(seqs)) == 10 and all(len(s) == 56 for s in seqs)
    # Kinesis timestamps are whole epoch seconds on the wire (botocore truncates): cross a second boundary between the batches
    t_mid = int(time.time()) + 1
    time.sleep(t_mid - time.time() + 0.1)
    seqs += [k.put_record(StreamName=name, Data=f"r{i}".encode(), PartitionKey="pk")["SequenceNumber"] for i in range(10, 15)]
    assert [r["Data"] for r in read_all(k, name, shard)] == [f"r{i}".encode() for i in range(15)]
    assert [r["Data"] for r in read_all(k, name, shard, "AT_SEQUENCE_NUMBER", StartingSequenceNumber=seqs[3])][:2] == [b"r3", b"r4"]
    assert [r["Data"] for r in read_all(k, name, shard, "AFTER_SEQUENCE_NUMBER", StartingSequenceNumber=seqs[3])][:1] == [b"r4"]
    assert [r["Data"] for r in read_all(k, name, shard, "AT_TIMESTAMP", Timestamp=t_mid)] == [f"r{i}".encode() for i in range(10, 15)]
    assert read_all(k, name, shard, "LATEST") == []
    it = k.get_shard_iterator(StreamName=name, ShardId=shard, ShardIteratorType="LATEST")["ShardIterator"]
    k.put_record(StreamName=name, Data=b"new", PartitionKey="pk")
    assert [r["Data"] for r in k.get_records(ShardIterator=it)["Records"]] == [b"new"]
    it = k.get_shard_iterator(StreamName=name, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    page = k.get_records(ShardIterator=it, Limit=4)
    assert len(page["Records"]) == 4 and page["MillisBehindLatest"] >= 0
    assert [r["Data"] for r in k.get_records(ShardIterator=page["NextShardIterator"], Limit=2)["Records"]] == [b"r4", b"r5"]
    with pytest.raises(ClientError) as e:
        k.get_records(ShardIterator=it, Limit=10001)
    assert e.value.response["Error"]["Code"] == "InvalidArgumentException"
    with pytest.raises(ClientError) as e:
        k.get_records(ShardIterator="garbage")
    assert e.value.response["Error"]["Code"] == "InvalidArgumentException"
    with pytest.raises(ClientError) as e:
        k.put_record(StreamName="nope-" + name, Data=b"x", PartitionKey="a")
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError):
        k.put_record(StreamName=name, Data=b"x" * (1024 * 1024 + 1), PartitionKey="a")


def test_kinesis_concurrent_producers_get_strictly_increasing_sequence_numbers(warp):
    k = client("kinesis", warp)
    name = uniq("conc")
    k.create_stream(StreamName=name, ShardCount=1)
    shard = k.describe_stream(StreamName=name)["StreamDescription"]["Shards"][0]["ShardId"]
    producers, per = 16, 40

    def produce(p):
        c = client("kinesis", warp)
        mine, n = [], 0
        for i in range(per):
            if i % 2 == 0:  # a batch of 5
                r = c.put_records(StreamName=name, Records=[{"Data": f"{p}:{n + j}".encode(), "PartitionKey": f"k{p}"} for j in range(5)])
                mine += [x["SequenceNumber"] for x in r["Records"]]
                n += 5
            else:  # five single puts
                for _ in range(5):
                    mine.append(c.put_record(StreamName=name, Data=f"{p}:{n}".encode(), PartitionKey=f"k{p}")["SequenceNumber"])
                    n += 1
        return p, mine

    with concurrent.futures.ThreadPoolExecutor(producers) as ex:
        results = list(ex.map(produce, range(producers)))
    records = read_all(k, name, shard)
    seqs = [r["SequenceNumber"] for r in records]
    assert len(seqs) == producers * per * 5
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs), "strictly increasing, no duplicates, in read order"
    assert all(int(b) > int(a) for a, b in zip(seqs, seqs[1:]))
    per_producer = {}
    for r in records:
        p, i = r["Data"].decode().split(":")
        per_producer.setdefault(p, []).append(int(i))
    for p, got in per_producer.items():
        assert got == sorted(got), f"producer {p}'s records are in the order it sent them"
    # the numbers each producer was handed match what a reader sees
    assigned = sorted(s for _, mine in results for s in mine)
    assert assigned == seqs


def test_kinesis_split_merge_update_shard_count_and_child_shards(warp):
    k = client("kinesis", warp)
    name = uniq("reshard")
    k.create_stream(StreamName=name, ShardCount=1)
    parent = k.describe_stream(StreamName=name)["StreamDescription"]["Shards"][0]
    k.put_record(StreamName=name, Data=b"before", PartitionKey="a")
    mid = str(2 ** 127)
    k.split_shard(StreamName=name, ShardToSplit=parent["ShardId"], NewStartingHashKey=mid)
    shards = k.describe_stream(StreamName=name)["StreamDescription"]["Shards"]
    kids = [s for s in shards if s.get("ParentShardId") == parent["ShardId"]]
    assert len(shards) == 3 and len(kids) == 2
    closed = next(s for s in shards if s["ShardId"] == parent["ShardId"])
    assert "EndingSequenceNumber" in closed["SequenceNumberRange"]
    assert k.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["OpenShardCount"] == 2
    it = k.get_shard_iterator(StreamName=name, ShardId=parent["ShardId"], ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    r = k.get_records(ShardIterator=it)
    assert [x["Data"] for x in r["Records"]] == [b"before"]
    # the end of a closed shard: no next iterator, and the child shards to continue with
    assert "NextShardIterator" not in r and {c["ShardId"] for c in r["ChildShards"]} == {s["ShardId"] for s in kids}
    with pytest.raises(ClientError) as e:  # the parent is closed now
        k.split_shard(StreamName=name, ShardToSplit=parent["ShardId"], NewStartingHashKey=mid)
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    k.merge_shards(StreamName=name, ShardToMerge=kids[0]["ShardId"], AdjacentShardToMerge=kids[1]["ShardId"])
    assert k.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["OpenShardCount"] == 1
    r = k.update_shard_count(StreamName=name, TargetShardCount=4, ScalingType="UNIFORM_SCALING")
    assert r["TargetShardCount"] == 4
    assert k.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["OpenShardCount"] == 4
    assert k.put_record(StreamName=name, Data=b"after", PartitionKey="zz")["ShardId"]


def test_kinesis_retention_sweeper_removes_old_records(warp, pgs):
    k = client("kinesis", warp)
    name = uniq("sweep")
    k.create_stream(StreamName=name, ShardCount=1)
    for i in range(5):
        k.put_record(StreamName=name, Data=b"x", PartitionKey="p")
    for p in pgs:
        sql(p, "UPDATE warp_kinesis_records SET arrived = now() - interval '25 hours' WHERE stream = %s", (name,))
    end = time.time() + 15
    while time.time() < end and total(pgs, "warp_kinesis_records", f"stream = '{name}'") > 0:
        time.sleep(0.5)
    assert total(pgs, "warp_kinesis_records", f"stream = '{name}'") == 0


def test_kinesis_consumers_and_subscribe_to_shard_over_http1_event_stream(warp):
    k = client("kinesis", warp)
    name = uniq("efo")
    k.create_stream(StreamName=name, ShardCount=1)
    arn = k.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]
    shard = k.describe_stream(StreamName=name)["StreamDescription"]["Shards"][0]["ShardId"]
    for i in range(3):
        k.put_record(StreamName=name, Data=f"e{i}".encode(), PartitionKey="a")
    c = k.register_stream_consumer(StreamARN=arn, ConsumerName="c1")["Consumer"]
    assert k.describe_stream_consumer(ConsumerARN=c["ConsumerARN"])["ConsumerDescription"]["ConsumerStatus"] == "ACTIVE"
    assert [x["ConsumerName"] for x in k.list_stream_consumers(StreamARN=arn)["Consumers"]] == ["c1"]
    body = json.dumps({"ConsumerARN": c["ConsumerARN"], "ShardId": shard, "StartingPosition": {"Type": "TRIM_HORIZON"}})
    r = requests.post(warp.endpoint + "/", data=body, timeout=30, headers={
        "X-Amz-Target": "Kinesis_20131202.SubscribeToShard", "Content-Type": "application/x-amz-json-1.1",
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/kinesis/aws4_request, SignedHeaders=host, Signature=00"})
    assert r.status_code == 200 and r.headers["Content-Type"].startswith("application/vnd.amazon.eventstream")
    raw = r.content
    assert b"initial-response" in raw and b"SubscribeToShardEvent" in raw
    for i in range(3):
        assert base64.b64encode(f"e{i}".encode()) in raw
    k.deregister_stream_consumer(ConsumerARN=c["ConsumerARN"])


# ------------------------------------------------------------------- KMS, Secrets Manager, SSM, IAM/STS

def test_kms_crypto_grants_aliases_and_key_lifecycle(warp, pgs):
    kms = client("kms", warp)
    keys = [kms.create_key(Description=f"k{i}")["KeyMetadata"]["KeyId"] for i in range(8)]
    hosts = [sql(p, "SELECT count(*) FROM warp_awsparams_kms_keys WHERE description ~ '^k[0-9]$'")[0][0] for p in pgs]
    assert sum(hosts) >= 8
    if len(pgs) == 2:
        assert min(hosts) >= 1, hosts
    kid = keys[0]
    alias = "alias/" + uniq("a")
    kms.create_alias(AliasName=alias, TargetKeyId=kid)
    with pytest.raises(ClientError) as e:
        kms.create_alias(AliasName=alias, TargetKeyId=keys[1])
    assert e.value.response["Error"]["Code"] == "AlreadyExistsException"
    ct = kms.encrypt(KeyId=alias, Plaintext=b"data", EncryptionContext={"a": "b"})["CiphertextBlob"]
    assert kms.decrypt(CiphertextBlob=ct, EncryptionContext={"a": "b"})["Plaintext"] == b"data"
    with pytest.raises(ClientError) as e:
        kms.decrypt(CiphertextBlob=ct, EncryptionContext={"a": "other"})
    assert e.value.response["Error"]["Code"] == "InvalidCiphertextException"
    dk = kms.generate_data_key(KeyId=kid, KeySpec="AES_256")
    assert len(dk["Plaintext"]) == 32 and kms.decrypt(CiphertextBlob=dk["CiphertextBlob"])["Plaintext"] == dk["Plaintext"]
    kms.disable_key(KeyId=kid)
    with pytest.raises(ClientError) as e:
        kms.encrypt(KeyId=kid, Plaintext=b"x")
    assert e.value.response["Error"]["Code"] == "DisabledException"
    kms.enable_key(KeyId=kid)
    g = kms.create_grant(KeyId=kid, GranteePrincipal="arn:aws:iam::000000000000:role/g", Operations=["Decrypt"])
    assert kms.list_grants(KeyId=kid)["Grants"][0]["GrantId"] == g["GrantId"]
    kms.retire_grant(GrantToken=g["GrantToken"])
    assert kms.list_grants(KeyId=kid)["Grants"] == []
    kms.enable_key_rotation(KeyId=kid)
    assert kms.get_key_rotation_status(KeyId=kid)["KeyRotationEnabled"] is True
    for p in pgs:
        r = sql(p, "SELECT material FROM warp_awsparams_kms_keys WHERE key_id = %s", (kid,))
        if r:
            assert bytes(r[0][0])[:12] != b"", "key material is sealed (nonce + GCM), never the raw key"
            assert len(bytes(r[0][0])) == 12 + 32 + 16
    kms.schedule_key_deletion(KeyId=kid, PendingWindowInDays=7)
    assert kms.describe_key(KeyId=kid)["KeyMetadata"]["KeyState"] == "PendingDeletion"
    kms.cancel_key_deletion(KeyId=kid)
    assert kms.describe_key(KeyId=kid)["KeyMetadata"]["KeyState"] == "Disabled"
    listed = [x["KeyId"] for x in kms.list_keys()["Keys"]]
    assert set(keys) <= set(listed)


def test_secrets_and_ssm_values_are_sealed_by_kms_and_never_stored_in_clear(warp, pgs):
    sm, ssm, kms = client("secretsmanager", warp), client("ssm", warp), client("kms", warp)
    name, secret = uniq("s"), "correct-horse-battery-staple"
    arn = sm.create_secret(Name=name, SecretString=secret)["ARN"]
    param, pval = "/" + uniq("p") + "/db", "hunter2-hunter2"
    ssm.put_parameter(Name=param, Value=pval, Type="SecureString")
    ssm.put_parameter(Name=param + "-plain", Value="visible", Type="String")
    for p in pgs:
        for row in sql(p, "SELECT secret_string FROM warp_awsparams_secret_versions"):
            assert secret.encode() not in bytes(row[0])
        for row in sql(p, "SELECT name, value FROM warp_awsparams_ssm_params WHERE type = 'SecureString'"):
            assert pval.encode() not in bytes(row[1])
    assert sm.get_secret_value(SecretId=name)["SecretString"] == secret
    assert ssm.get_parameter(Name=param, WithDecryption=True)["Parameter"]["Value"] == pval
    assert ssm.get_parameter(Name=param, WithDecryption=False)["Parameter"]["Value"] != pval
    aliases = {a["AliasName"] for a in kms.list_aliases()["Aliases"]}
    assert {"alias/aws/secretsmanager", "alias/aws/ssm"} <= aliases
    # a customer key round trip; the ciphertext of a value under key A cannot be opened after A is disabled
    key = kms.create_key()["KeyMetadata"]
    n2 = uniq("s2")
    sm.create_secret(Name=n2, SecretString="v", KmsKeyId=key["Arn"])
    assert sm.describe_secret(SecretId=n2)["KmsKeyId"] == key["Arn"]
    kms.disable_key(KeyId=key["KeyId"])
    with pytest.raises(ClientError) as e:
        sm.get_secret_value(SecretId=n2)
    assert e.value.response["Error"]["Code"] == "DisabledException"
    with pytest.raises(ClientError) as e:
        sm.create_secret(Name=uniq("s3"), SecretString="v", KmsKeyId="arn:aws:kms:us-east-1:000000000000:key/nope")
    assert "You can't access the KMS key" in e.value.response["Error"]["Message"]
    sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


def test_secrets_versions_stages_recovery_and_ssm_history_labels_paths(warp):
    sm, ssm = client("secretsmanager", warp), client("ssm", warp)
    name = uniq("ver")
    v1 = sm.create_secret(Name=name, SecretString="one")["VersionId"]
    v2 = sm.put_secret_value(SecretId=name, SecretString="two")["VersionId"]
    v3 = sm.put_secret_value(SecretId=name, SecretString="three", VersionStages=["custom"])["VersionId"]
    stages = {v["VersionId"]: sorted(v["VersionStages"]) for v in sm.list_secret_version_ids(SecretId=name)["Versions"]}
    assert stages == {v1: ["AWSPREVIOUS"], v2: ["AWSCURRENT"], v3: ["custom"]}
    assert sm.get_secret_value(SecretId=name)["SecretString"] == "two"
    assert sm.get_secret_value(SecretId=name, VersionStage="AWSPREVIOUS")["SecretString"] == "one"
    assert sm.get_secret_value(SecretId=name, VersionId=v3)["SecretString"] == "three"
    sm.update_secret_version_stage(SecretId=name, VersionStage="AWSCURRENT", MoveToVersionId=v3, RemoveFromVersionId=v2)
    assert sm.get_secret_value(SecretId=name)["SecretString"] == "three"
    sm.delete_secret(SecretId=name, RecoveryWindowInDays=7)
    with pytest.raises(ClientError) as e:
        sm.get_secret_value(SecretId=name)
    assert e.value.response["Error"]["Code"] == "InvalidRequestException"
    with pytest.raises(ClientError) as e:
        sm.create_secret(Name=name, SecretString="again")
    assert e.value.response["Error"]["Code"] == "InvalidRequestException"
    sm.restore_secret(SecretId=name)
    assert sm.get_secret_value(SecretId=name)["SecretString"] == "three"
    listed = [s["Name"] for s in sm.list_secrets(Filters=[{"Key": "name", "Values": [name[:8]]}])["SecretList"]]
    assert name in listed
    pw = sm.get_random_password(PasswordLength=40, ExcludePunctuation=True)["RandomPassword"]
    assert len(pw) == 40 and pw.isalnum()
    sm.tag_resource(SecretId=name, Tags=[{"Key": "k", "Value": "v"}])
    assert sm.describe_secret(SecretId=name)["Tags"] == [{"Key": "k", "Value": "v"}]
    sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    base = "/" + uniq("app")
    for i, (p, v) in enumerate([("/a", "1"), ("/a/b", "2"), ("/c", "3")]):
        ssm.put_parameter(Name=base + p, Value=v, Type="String")
    ssm.put_parameter(Name=base + "/a", Value="1b", Type="String", Overwrite=True)
    with pytest.raises(ClientError) as e:
        ssm.put_parameter(Name=base + "/a", Value="x", Type="String")
    assert e.value.response["Error"]["Code"] == "ParameterAlreadyExists"
    ssm.label_parameter_version(Name=base + "/a", ParameterVersion=1, Labels=["stable"])
    assert ssm.get_parameter(Name=base + "/a:stable")["Parameter"]["Value"] == "1"
    assert ssm.get_parameter(Name=base + "/a:2")["Parameter"]["Value"] == "1b"
    hist = ssm.get_parameter_history(Name=base + "/a")["Parameters"]
    assert [h["Value"] for h in hist] == ["1", "1b"] and hist[0]["Labels"] == ["stable"]
    assert sorted(p["Name"] for p in ssm.get_parameters_by_path(Path=base)["Parameters"]) == [base + "/a", base + "/c"]
    assert sorted(p["Name"] for p in ssm.get_parameters_by_path(Path=base, Recursive=True)["Parameters"]) == \
        [base + "/a", base + "/a/b", base + "/c"]
    d = ssm.describe_parameters(ParameterFilters=[{"Key": "Name", "Option": "BeginsWith", "Values": [base]}])["Parameters"]
    assert len(d) == 3 and {p["Version"] for p in d} == {1, 2}
    got = ssm.get_parameters(Names=[base + "/a", base + "/nope"])
    assert [p["Name"] for p in got["Parameters"]] == [base + "/a"] and got["InvalidParameters"] == [base + "/nope"]


def test_kms_fails_closed_without_a_master_key():
    pg = RealPostgres()
    warp = start_aws_warp(pg, extra={"WARP_KMS_INSECURE_DEV_KEY": "false"})
    try:
        kms, sm, ssm = client("kms", warp), client("secretsmanager", warp), client("ssm", warp)
        for call in (lambda: kms.create_key(), lambda: sm.create_secret(Name="x", SecretString="v"),
                     lambda: ssm.put_parameter(Name="/x", Value="v", Type="SecureString")):
            with pytest.raises(ClientError) as e:
                call()
            assert e.value.response["Error"]["Code"] == "KMSInternalException"
            assert "WARP_KMS_MASTER_KEY" in e.value.response["Error"]["Message"]
        ssm.put_parameter(Name="/plain", Value="ok", Type="String")  # plain parameters need no KMS
        assert ssm.get_parameter(Name="/plain")["Parameter"]["Value"] == "ok"
    finally:
        warp.close()
        pg.close()


def test_master_key_change_makes_existing_key_material_unreadable_instead_of_wrong():
    pg = RealPostgres()
    warp = start_aws_warp(pg, extra={"WARP_KMS_INSECURE_DEV_KEY": "false", "WARP_KMS_MASTER_KEY": "first master key"})
    try:
        kid = client("kms", warp).create_key()["KeyMetadata"]["KeyId"]
        ct = client("kms", warp).encrypt(KeyId=kid, Plaintext=b"x")["CiphertextBlob"]
    finally:
        warp.close()
    warp = start_aws_warp(pg, extra={"WARP_KMS_INSECURE_DEV_KEY": "false", "WARP_KMS_MASTER_KEY": "another master key"})
    try:
        with pytest.raises(ClientError) as e:
            client("kms", warp).decrypt(CiphertextBlob=ct)
        assert e.value.response["Error"]["Code"] in ("InternalFailure", "KMSInternalException")
    finally:
        warp.close()
        pg.close()


def test_sigv4_validation_and_sts_temporary_credentials_are_accepted():
    pg = RealPostgres()
    warp = start_aws_warp(pg, extra={"WARP_AWS_IAM_CREDENTIALS": "AKIAWARPTEST=warp-secret-key"})
    try:
        good = client("sns", warp, "AKIAWARPTEST", "warp-secret-key")
        good.create_topic(Name="signed")
        with pytest.raises(ClientError) as e:
            client("sns", warp, "AKIAWARPTEST", "wrong-secret").list_topics()
        assert e.value.response["Error"]["Code"] == "SignatureDoesNotMatch"
        with pytest.raises(ClientError) as e:
            client("sns", warp, "AKIAUNKNOWN", "x").list_topics()
        assert e.value.response["Error"]["Code"] == "UnrecognizedClientException"
        assert requests.post(warp.endpoint + "/", data="Action=ListTopics&Version=2010-03-31", timeout=10, headers={
            "Content-Type": "application/x-www-form-urlencoded"}).status_code == 403
        sts = client("sts", warp, "AKIAWARPTEST", "warp-secret-key")
        c = sts.assume_role(RoleArn="arn:aws:iam::000000000000:role/r1", RoleSessionName="sess1")["Credentials"]
        assert c["AccessKeyId"].startswith("ASIA")
        temp = client("sns", warp, c["AccessKeyId"], c["SecretAccessKey"], c["SessionToken"])
        assert any(t["TopicArn"].endswith(":signed") for t in temp.list_topics()["Topics"])
        who = client("sts", warp, c["AccessKeyId"], c["SecretAccessKey"], c["SessionToken"]).get_caller_identity()
        assert who["Arn"] == "arn:aws:sts::000000000000:assumed-role/r1/sess1"
        with pytest.raises(ClientError) as e:
            client("sns", warp, c["AccessKeyId"], c["SecretAccessKey"], "wrong-token").list_topics()
        assert e.value.response["Error"]["Code"] in ("InvalidClientTokenId", "UnrecognizedClientException")
        with pytest.raises(ClientError):
            client("sns", warp, c["AccessKeyId"], "not-the-secret", c["SessionToken"]).list_topics()
        # a Kinesis, Secrets, SSM and KMS call with temporary credentials too
        for svc, call in (("kinesis", lambda x: x.list_streams()), ("secretsmanager", lambda x: x.list_secrets()),
                          ("ssm", lambda x: x.describe_parameters()), ("kms", lambda x: x.list_keys())):
            call(client(svc, warp, c["AccessKeyId"], c["SecretAccessKey"], c["SessionToken"]))
        # expired sessions stop working
        sql(pg, "UPDATE warp_awsparams_sts_sessions SET expires_at = now() - interval '1 minute'")
        time.sleep(5.5)  # the per-process session cache is 5 s
        with pytest.raises(ClientError):
            temp.list_topics()
    finally:
        warp.close()
        pg.close()


# ------------------------------------------------------------------------------------------ pool starvation

def test_pool_of_four_connections_mixed_load_and_slow_subscribers_do_not_starve():
    pg = RealPostgres()
    warp = start_aws_warp(pg, extra={"WARP_POOL_MAX_SIZE": "4"})
    slow = Hook(delay=3.0)
    try:
        sns, sm, k, ssm = client("sns", warp), client("secretsmanager", warp), client("kinesis", warp), client("ssm", warp)
        topic = sns.create_topic(Name="pool")["TopicArn"]
        q, a = mkqueue(warp, "pool-q")
        sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=a)
        sns.subscribe(TopicArn=topic, Protocol="http", Endpoint=slow.url)
        sns.confirm_subscription(TopicArn=topic, Token=json.loads(slow.wait(1)[0][1])["Token"])
        k.create_stream(StreamName="pool-s", ShardCount=2)
        # 3 s slow HTTP deliveries are in flight while everything else must keep flowing
        for i in range(6):
            sns.publish(TopicArn=topic, Message=f"slow{i}")

        def work(i):
            c_sns, c_sm, c_k, c_ssm = client("sns", warp), client("secretsmanager", warp), client("kinesis", warp), client("ssm", warp)
            t0 = time.time()
            for j in range(6):
                c_sm.create_secret(Name=f"pool-{i}-{j}", SecretString=f"v{i}{j}")
                assert c_sm.get_secret_value(SecretId=f"pool-{i}-{j}")["SecretString"] == f"v{i}{j}"
                c_k.put_record(StreamName="pool-s", Data=b"x", PartitionKey=f"p{i}{j}")
                c_ssm.put_parameter(Name=f"/pool/{i}/{j}", Value="s", Type="SecureString")
                c_sns.publish(TopicArn=topic, Message=f"m{i}{j}")
            return time.time() - t0

        with concurrent.futures.ThreadPoolExecutor(24) as ex:
            times = list(ex.map(work, range(24)))
        assert max(times) < 60, times
        assert len(drain(warp, q, want=6 + 24 * 6, wait=15)) == 6 + 24 * 6
    finally:
        slow.shutdown()
        warp.close()
        pg.close()


def test_ssm_run_command_records_and_sqs_sns_use_one_endpoint_end_to_end(warp):
    ssm = client("ssm", warp)
    cmd = ssm.send_command(DocumentName="AWS-RunShellScript", InstanceIds=["i-1"], TimeoutSeconds=60,
                           Parameters={"commands": ["echo"]})["Command"]
    assert ssm.get_command_invocation(CommandId=cmd["CommandId"], InstanceId="i-1")["Status"] == "Pending"
    ssm.cancel_command(CommandId=cmd["CommandId"], InstanceIds=["i-1"])
    assert ssm.get_command_invocation(CommandId=cmd["CommandId"], InstanceId="i-1")["Status"] == "Cancelled"


def test_iam_saml_and_sts_role_trust(warp):
    iam, sts = client("iam", warp), client("sts", warp)
    role = iam.create_role(RoleName=uniq("role"), AssumeRolePolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "x"}, "Action": "sts:AssumeRole"}]}))["Role"]
    assert iam.get_role(RoleName=role["RoleName"])["Role"]["Arn"] == role["Arn"]
    with pytest.raises(ClientError) as e:
        iam.create_role(RoleName=role["RoleName"], AssumeRolePolicyDocument="{}")
    assert e.value.response["Error"]["Code"] == "EntityAlreadyExists"
    with pytest.raises(ClientError) as e:
        sts.assume_role_with_saml(RoleArn=role["Arn"], PrincipalArn="arn:aws:iam::000000000000:saml-provider/none", SAMLAssertion="AAAA")
    assert e.value.response["Error"]["Code"] == "InvalidIdentityToken"
    iam.delete_role(RoleName=role["RoleName"])


def test_each_service_on_its_own_port_and_sns_json_protocol():
    from warp_test_support import free_port
    ports = {n: free_port() for n in ("SNS", "KINESIS", "SECRETS", "SSM", "KMS", "STS")}
    pg = RealPostgres()
    warp = start_aws_warp(pg, extra={f"WARP_{n}WIRE_PORT": str(p) for n, p in ports.items()})
    try:
        def one(service, name):
            return boto3.client(service, endpoint_url=f"http://localhost:{ports[name]}", region_name="us-east-1",
                                aws_access_key_id="t", aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}))
        sns = one("sns", "SNS")
        arn = sns.create_topic(Name="own-port")["TopicArn"]
        assert arn in [t["TopicArn"] for t in sns.list_topics()["Topics"]]
        # the JSON protocol of SNS (X-Amz-Target) on the same listener
        r = requests.post(f"http://localhost:{ports['SNS']}/", data=json.dumps({"Name": "json-proto"}), timeout=10, headers={
            "X-Amz-Target": "SNS_20100331.CreateTopic", "Content-Type": "application/x-amz-json-1.0"})
        assert r.status_code == 200 and r.json()["TopicArn"].endswith(":json-proto")
        r = requests.post(f"http://localhost:{ports['SNS']}/", data=json.dumps({"TopicArn": "arn:aws:sns:us-east-1:000000000000:nope", "Message": "x"}),
                          timeout=10, headers={"X-Amz-Target": "SNS_20100331.Publish", "Content-Type": "application/x-amz-json-1.0"})
        assert r.status_code == 404 and r.json()["__type"] == "NotFoundException" and r.headers["x-amzn-query-error"].startswith("NotFound")
        k = one("kinesis", "KINESIS")
        k.create_stream(StreamName="own", ShardCount=1)
        assert k.put_record(StreamName="own", Data=b"x", PartitionKey="a")["SequenceNumber"]
        assert one("secretsmanager", "SECRETS").create_secret(Name="own-s", SecretString="v")["Name"] == "own-s"
        assert one("ssm", "SSM").put_parameter(Name="/own", Value="v", Type="String")["Version"] == 1
        assert len(one("kms", "KMS").generate_random(NumberOfBytes=16)["Plaintext"]) == 16
        assert one("sts", "STS").get_caller_identity()["Account"] == "000000000000"
        # a service is not served by another service's port
        with pytest.raises(ClientError):
            boto3.client("kms", endpoint_url=f"http://localhost:{ports['SNS']}", region_name="us-east-1", aws_access_key_id="t",
                         aws_secret_access_key="t", config=Config(retries={"max_attempts": 0})).list_keys()
    finally:
        warp.close()
        pg.close()


def test_kinesis_over_cleartext_http2_with_the_javascript_sdk(warp):
    """h2c prior knowledge: 2.7 MB uploaded in 900 KB requests (needs HTTP/2 flow control both ways), 40 concurrent 2.7 MB responses."""
    import os
    import shutil
    import subprocess
    root = os.environ.get("FLOCI_DIR")
    modules = os.path.join(root, "compatibility-tests", "sdk-test-node", "node_modules") if root else None
    if not shutil.which("node") or not modules or not os.path.isdir(os.path.join(modules, "@aws-sdk", "client-kinesis")):
        pytest.skip("needs node and @aws-sdk/client-kinesis (set FLOCI_DIR to a Floci checkout after `npm install` in compatibility-tests/sdk-test-node)")
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "awsextras_h2c_check.cjs")
    r = subprocess.run(["node", script, warp.endpoint], capture_output=True, text=True, timeout=90, env={**os.environ, "NODE_PATH": modules})
    assert r.returncode == 0 and r.stdout.startswith("OK"), (r.stdout, r.stderr)
