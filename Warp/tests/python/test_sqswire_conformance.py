"""Amazon SQS conformance for sqswire against a real Warp + real Postgres, with BOTH wire protocols.

Every test runs twice: `single` (one Postgres backend) and `sharded` (two Postgres backends of one backend set, queues
hash by name across them). boto3 exercises the JSON protocol; a small raw client (requests + botocore SigV4 signing)
exercises the older AWS Query protocol (form-encoded request, XML response) that current botocore no longer speaks.
Cross-shard behaviour (dead-letter queue and message-move destination on a different backend than the source queue,
ListQueues / ListDeadLetterSourceQueues across shards) is proved by looking straight into each Postgres with psycopg2.

Run with a local Postgres (`WARP_TEST_PG_LOCAL=1`, `WARP_TEST_PG_BIN=/opt/homebrew/opt/postgresql@17/bin`) or Docker.
"""
import base64
import hashlib
import json
import os
import struct
import threading
import time
import uuid
import xml.etree.ElementTree as ET

import boto3
import psycopg2
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from mcp_support import ADMIN_TOKEN
from warp_test_support import RealPostgres, WarpProcess, isolated_ports

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

NS = "{http://queue.amazonaws.com/doc/2012-11-05/}"


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


class Env:
    """One running Warp with its Postgres backend(s) and helpers."""

    def __init__(self, name, pgs, warp, ports):
        self.name, self.pgs, self.warp, self.ports = name, pgs, warp, ports
        self.port = int(ports["WARP_SQSWIRE_PORT"])
        self.endpoint = f"http://localhost:{self.port}"
        self.sqs = boto3.client("sqs", endpoint_url=self.endpoint, region_name="us-east-1", aws_access_key_id="t",
                                aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}))

    def table(self, queue):
        return "sqs_queue_" + "".join(c if c.isalnum() or c == "_" else "_" for c in queue).lower()

    def shard_of(self, queue):
        where = [i for i, pg in enumerate(self.pgs) if sql(pg, "SELECT 1 FROM information_schema.tables WHERE table_name=%s",
                                                           (self.table(queue),))]
        assert len(where) == 1, (queue, where)
        return where[0]

    def rows(self, queue, where="true"):
        return sql(self.pgs[self.shard_of(queue)], f"SELECT count(*) FROM {self.table(queue)} WHERE {where}")[0][0]

    def query(self, action, path="/", **params):
        """Raw AWS Query protocol call (signed with SigV4 like a real client). Returns (status, ElementTree root)."""
        body = {"Action": action, "Version": "2012-11-05", **{k: str(v) for k, v in params.items()}}
        url = self.endpoint + path
        req = AWSRequest(method="POST", url=url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
        SigV4Auth(Credentials("test", "test"), "sqs", "us-east-1").add_auth(req)
        prepared = req.prepare()
        resp = requests.post(url, data=prepared.body, headers=dict(prepared.headers), timeout=60)
        return resp.status_code, ET.fromstring(resp.text)

    def new_queue(self, prefix="q", **attrs):
        name = f"{prefix}-{uuid.uuid4().hex[:8]}" + (".fifo" if attrs.get("FifoQueue") == "true" else "")
        kw = {"QueueName": name}
        if attrs:
            kw["Attributes"] = attrs
        return name, self.sqs.create_queue(**kw)["QueueUrl"]

    def queue_pair_on_different_shards(self, prefix):
        """Names for (source, dlq) that hash to different backends (single env: just two queues)."""
        for i in range(200):
            src = f"{prefix}-src-{i}-{uuid.uuid4().hex[:4]}"
            dlq = f"{prefix}-dlq-{i}-{uuid.uuid4().hex[:4]}"
            d1 = self.sqs.create_queue(QueueName=src)["QueueUrl"]
            d2 = self.sqs.create_queue(QueueName=dlq)["QueueUrl"]
            if self.name == "single" or self.shard_of(src) != self.shard_of(dlq):
                return src, d1, dlq, d2
            self.sqs.delete_queue(QueueUrl=d1)
            self.sqs.delete_queue(QueueUrl=d2)
        raise AssertionError("could not find queue names on different shards")

    def arn(self, url):
        return self.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


def make_warp(pgs, sharded):
    ports = isolated_ports("WARP_DYNAMOWIRE_PORT")
    extra = {**ports, "WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_SQSWIRE_SWEEP_SECONDS": "1"}
    proc = WarpProcess(pgs[0], "WARP_DYNAMOWIRE_PORT", frontend_name="dynamowire", extra_env=extra)

    def api(method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["sqs"]})
    if sharded:
        api("POST", "/api/backend-sets/default/backends", {
            "name": "pg2", "url": pg_url(pgs[1]), "user": "postgres", "password": "postgres",
            "enabledStores": ["sqs"]}, expect=201)
    return proc, ports


@pytest.fixture(scope="module", params=["single", "sharded"])
def env(request):
    sharded = request.param == "sharded"
    pgs = [RealPostgres() for _ in range(2 if sharded else 1)]
    proc = ports = None
    try:
        proc, ports = make_warp(pgs, sharded)
        yield Env(request.param, pgs, proc, ports)
    finally:
        if proc:
            proc.close()
        for pg in pgs:
            pg.close()


def code(e):
    return e.value.response["Error"]["Code"]


# ---------------------------------------------------------------------------------------------
# md5 reference implementation (independent of the Java one) -- AWS "MD5 of message attributes"
# ---------------------------------------------------------------------------------------------

def md5_attrs(attrs):
    buf = b""
    for name in sorted(attrs):
        v = attrs[name]
        for s in (name.encode(), v["DataType"].encode()):
            buf += struct.pack(">I", len(s)) + s
        if "BinaryValue" in v:
            data = v["BinaryValue"] if isinstance(v["BinaryValue"], bytes) else base64.b64decode(v["BinaryValue"])
            buf += b"\x02" + struct.pack(">I", len(data)) + data
        else:
            sv = v["StringValue"].encode()
            buf += b"\x01" + struct.pack(">I", len(sv)) + sv
    return hashlib.md5(buf).hexdigest()


# ---------------------------------------------------------------------------------------------
# queue lifecycle, names, errors
# ---------------------------------------------------------------------------------------------

def test_queue_url_format_and_get_queue_url(env):
    name, url = env.new_queue("url")
    assert url == f"{env.endpoint}/000000000000/{name}"
    assert env.sqs.get_queue_url(QueueName=name)["QueueUrl"] == url


def test_queue_name_validation(env):
    for bad in ["a b", "x" * 81, "dots.in.name", "semi;colon", "q.fifo"]:
        with pytest.raises(ClientError) as e:
            env.sqs.create_queue(QueueName=bad)
        assert code(e) == "InvalidParameterValue", bad
    with pytest.raises(ClientError) as e:
        env.sqs.create_queue(QueueName="no-suffix", Attributes={"FifoQueue": "true"})
    assert code(e) == "InvalidParameterValue"
    long_url = env.sqs.create_queue(QueueName="x" * 80)["QueueUrl"]
    env.sqs.send_message(QueueUrl=long_url, MessageBody="long")
    assert env.sqs.receive_message(QueueUrl=long_url)["Messages"][0]["Body"] == "long"
    long_fifo = env.sqs.create_queue(QueueName="y" * 75 + ".fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    env.sqs.send_message(QueueUrl=long_fifo, MessageBody="long", MessageGroupId="g", MessageDeduplicationId="d")
    assert env.sqs.receive_message(QueueUrl=long_fifo)["Messages"][0]["Body"] == "long"
    # names that differ only in case / punctuation are different queues with their own messages
    urls = [env.sqs.create_queue(QueueName=n)["QueueUrl"] for n in ("Case-Q", "case-q", "case_q")]
    for i, u in enumerate(urls):
        env.sqs.send_message(QueueUrl=u, MessageBody=f"m{i}")
    assert [env.sqs.receive_message(QueueUrl=u)["Messages"][0]["Body"] for u in urls] == ["m0", "m1", "m2"]


def test_nonexistent_queue_errors(env):
    ghost = f"{env.endpoint}/000000000000/ghost-{uuid.uuid4().hex[:6]}"
    for call in (lambda: env.sqs.get_queue_url(QueueName="ghost-none"),
                 lambda: env.sqs.send_message(QueueUrl=ghost, MessageBody="x"),
                 lambda: env.sqs.receive_message(QueueUrl=ghost),
                 lambda: env.sqs.delete_queue(QueueUrl=ghost),
                 lambda: env.sqs.get_queue_attributes(QueueUrl=ghost, AttributeNames=["All"])):
        with pytest.raises(ClientError) as e:
            call()
        assert code(e) == "AWS.SimpleQueueService.NonExistentQueue"
        assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert env.sqs.exceptions.QueueDoesNotExist


def test_create_queue_is_idempotent_but_conflicting_attributes_are_refused(env):
    name, url = env.new_queue("idem", VisibilityTimeout="45")
    assert env.sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "45"})["QueueUrl"] == url
    assert env.sqs.create_queue(QueueName=name)["QueueUrl"] == url
    with pytest.raises(ClientError) as e:
        env.sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "46"})
    assert code(e) == "QueueAlreadyExists"


def test_list_queues_prefix_pagination_and_merge_across_shards(env):
    prefix = f"lq{uuid.uuid4().hex[:6]}-"
    names = sorted(f"{prefix}{i:02d}" for i in range(7))
    for n in names:
        env.sqs.create_queue(QueueName=n)
    got = [u.rsplit("/", 1)[1] for u in env.sqs.list_queues(QueueNamePrefix=prefix)["QueueUrls"]]
    assert got == names
    pages, token = [], None
    while True:
        kw = {"QueueNamePrefix": prefix, "MaxResults": 3}
        if token:
            kw["NextToken"] = token
        page = env.sqs.list_queues(**kw)
        pages.append([u.rsplit("/", 1)[1] for u in page.get("QueueUrls", [])])
        token = page.get("NextToken")
        if not token:
            break
    assert pages == [names[0:3], names[3:6], names[6:7]]
    if env.name == "sharded":
        assert {env.shard_of(n) for n in names} == {0, 1}, "queues must spread over both backends"
    assert env.sqs.list_queues(QueueNamePrefix="zzz-nothing-").get("QueueUrls", []) == []
    with pytest.raises(ClientError) as e:
        env.sqs.list_queues(MaxResults=0)
    assert code(e) == "InvalidParameterValue"


def test_get_queue_attributes_has_all_standard_attributes(env):
    name, url = env.new_queue("attrs")
    a = env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    for k in ["QueueArn", "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
              "ApproximateNumberOfMessagesDelayed", "CreatedTimestamp", "LastModifiedTimestamp", "VisibilityTimeout",
              "MaximumMessageSize", "MessageRetentionPeriod", "DelaySeconds", "ReceiveMessageWaitTimeSeconds",
              "SqsManagedSseEnabled"]:
        assert k in a, k
    assert a["QueueArn"] == f"arn:aws:sqs:us-east-1:000000000000:{name}"
    assert (a["VisibilityTimeout"], a["MaximumMessageSize"], a["MessageRetentionPeriod"], a["DelaySeconds"],
            a["ReceiveMessageWaitTimeSeconds"]) == ("30", "262144", "345600", "0", "0")
    assert "FifoQueue" not in a and "RedrivePolicy" not in a
    assert abs(int(a["CreatedTimestamp"]) - time.time()) < 60
    # no AttributeNames -> no attributes (like real SQS); a subset returns exactly that subset
    assert "Attributes" not in env.sqs.get_queue_attributes(QueueUrl=url)
    assert set(env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn", "DelaySeconds"])["Attributes"]) \
        == {"QueueArn", "DelaySeconds"}
    with pytest.raises(ClientError) as e:
        env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["Bogus"])
    assert code(e) == "InvalidAttributeName"
    fname, furl = env.new_queue("attrs", FifoQueue="true", ContentBasedDeduplication="true")
    fa = env.sqs.get_queue_attributes(QueueUrl=furl, AttributeNames=["All"])["Attributes"]
    assert (fa["FifoQueue"], fa["ContentBasedDeduplication"], fa["DeduplicationScope"], fa["FifoThroughputLimit"]) \
        == ("true", "true", "queue", "perQueue")


def test_set_queue_attributes_validates_ranges_and_updates_modified_time(env):
    name, url = env.new_queue("setattr")
    ok = {"VisibilityTimeout": "120", "DelaySeconds": "3", "MaximumMessageSize": "2048", "MessageRetentionPeriod": "600",
          "ReceiveMessageWaitTimeSeconds": "5"}
    env.sqs.set_queue_attributes(QueueUrl=url, Attributes=ok)
    a = env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    assert {k: a[k] for k in ok} == ok
    bad = [("VisibilityTimeout", "43201"), ("VisibilityTimeout", "-1"), ("DelaySeconds", "901"), ("MaximumMessageSize", "1023"),
           ("MaximumMessageSize", "262145"), ("MessageRetentionPeriod", "59"), ("MessageRetentionPeriod", "1209601"),
           ("ReceiveMessageWaitTimeSeconds", "21"), ("VisibilityTimeout", "abc"), ("SqsManagedSseEnabled", "maybe"),
           ("RedrivePolicy", "not json"), ("Policy", "{{")]
    for k, v in bad:
        with pytest.raises(ClientError) as e:
            env.sqs.set_queue_attributes(QueueUrl=url, Attributes={k: v})
        assert code(e) in ("InvalidAttributeValue", "InvalidParameterValue"), (k, v)
    for k in ["QueueArn", "CreatedTimestamp", "NoSuchThing", "ContentBasedDeduplication", "FifoQueue"]:
        with pytest.raises(ClientError) as e:
            env.sqs.set_queue_attributes(QueueUrl=url, Attributes={k: "true"})
        assert code(e) == "InvalidAttributeName", k
    # Policy round-trips
    policy = json.dumps({"Version": "2012-10-17", "Statement": []})
    env.sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": policy})
    assert json.loads(env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["Policy"])["Attributes"]["Policy"]) \
        == json.loads(policy)


def test_tags_and_permissions(env):
    name, url = env.new_queue("tags")
    env.sqs.tag_queue(QueueUrl=url, Tags={"env": "test", "team": "backend"})
    assert env.sqs.list_queue_tags(QueueUrl=url)["Tags"] == {"env": "test", "team": "backend"}
    env.sqs.untag_queue(QueueUrl=url, TagKeys=["team"])
    assert env.sqs.list_queue_tags(QueueUrl=url)["Tags"] == {"env": "test"}
    n2 = f"tagged-{uuid.uuid4().hex[:6]}"
    u2 = env.sqs.create_queue(QueueName=n2, tags={"a": "b"})["QueueUrl"]
    assert env.sqs.list_queue_tags(QueueUrl=u2)["Tags"] == {"a": "b"}
    with pytest.raises(ClientError) as e:
        env.sqs.tag_queue(QueueUrl=url, Tags={"aws:reserved": "x"})
    assert code(e) == "InvalidParameterValue"
    env.sqs.add_permission(QueueUrl=url, Label="p1", AWSAccountIds=["123456789012"], Actions=["SendMessage", "ReceiveMessage"])
    pol = json.loads(env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["Policy"])["Attributes"]["Policy"])
    assert pol["Statement"][0]["Sid"] == "p1"
    with pytest.raises(ClientError) as e:
        env.sqs.add_permission(QueueUrl=url, Label="p1", AWSAccountIds=["123456789012"], Actions=["SendMessage"])
    assert code(e) == "InvalidParameterValue"
    env.sqs.remove_permission(QueueUrl=url, Label="p1")
    assert "Policy" not in env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    with pytest.raises(ClientError) as e:
        env.sqs.remove_permission(QueueUrl=url, Label="p1")
    assert code(e) == "InvalidParameterValue"


# ---------------------------------------------------------------------------------------------
# messages: attributes + MD5, receive filters, visibility, delay, batches
# ---------------------------------------------------------------------------------------------

def test_message_attributes_md5_and_receive_filters(env):
    name, url = env.new_queue("md5")
    attrs = {"trace-id": {"DataType": "String", "StringValue": "abc-123"},
             "priority": {"DataType": "Number.int", "StringValue": "42"},
             "blob": {"DataType": "Binary", "BinaryValue": bytes([1, 2, 3, 4, 5])},
             "bar.x": {"DataType": "String", "StringValue": "x"}, "bar.y": {"DataType": "String", "StringValue": "y"}}
    r = env.sqs.send_message(QueueUrl=url, MessageBody="hello world", MessageAttributes=attrs,
                             MessageSystemAttributes={"AWSTraceHeader": {"DataType": "String", "StringValue": "Root=1-5759e988-bd862e3fe1be46a994272793"}})
    assert r["MD5OfMessageBody"] == hashlib.md5(b"hello world").hexdigest()
    assert r["MD5OfMessageAttributes"] == md5_attrs(attrs)
    assert r["MD5OfMessageSystemAttributes"] == md5_attrs({"AWSTraceHeader": {"DataType": "String",
                                                            "StringValue": "Root=1-5759e988-bd862e3fe1be46a994272793"}})
    m = env.sqs.receive_message(QueueUrl=url, MessageAttributeNames=["All"], MessageSystemAttributeNames=["All"],
                                VisibilityTimeout=0)["Messages"][0]
    assert m["MD5OfBody"] == r["MD5OfMessageBody"] and m["MD5OfMessageAttributes"] == r["MD5OfMessageAttributes"]
    assert m["MessageAttributes"]["blob"]["BinaryValue"] == bytes([1, 2, 3, 4, 5])
    assert m["MessageAttributes"]["priority"]["DataType"] == "Number.int"
    sysattrs = m["Attributes"]
    assert sysattrs["AWSTraceHeader"].startswith("Root=1-") and sysattrs["ApproximateReceiveCount"] == "1"
    assert abs(int(sysattrs["SentTimestamp"]) - time.time() * 1000) < 60000
    assert "ApproximateFirstReceiveTimestamp" in sysattrs and "SenderId" in sysattrs
    # filters: exact name, prefix wildcard, none
    def recv(names):
        got = env.sqs.receive_message(QueueUrl=url, MessageAttributeNames=names, VisibilityTimeout=0)["Messages"][0]
        return got
    one = recv(["priority"])
    assert set(one["MessageAttributes"]) == {"priority"}
    assert one["MD5OfMessageAttributes"] == md5_attrs({"priority": attrs["priority"]})
    wild = recv(["bar.*"])
    assert set(wild["MessageAttributes"]) == {"bar.x", "bar.y"}
    assert wild["MD5OfMessageAttributes"] == md5_attrs({k: attrs[k] for k in ("bar.x", "bar.y")})
    dotstar = recv([".*"])
    assert set(dotstar["MessageAttributes"]) == set(attrs)
    bare = recv([])
    assert "MessageAttributes" not in bare and "MD5OfMessageAttributes" not in bare and "Attributes" not in bare
    # system attribute filter
    only = env.sqs.receive_message(QueueUrl=url, MessageSystemAttributeNames=["SentTimestamp"], VisibilityTimeout=0)["Messages"][0]
    assert set(only["Attributes"]) == {"SentTimestamp"}


def test_message_attribute_and_body_validation(env):
    name, url = env.new_queue("val")
    bad_attrs = [{"a": {"DataType": "Blob", "StringValue": "x"}}, {"aws.x": {"DataType": "String", "StringValue": "x"}},
                 {"a": {"DataType": "Number", "StringValue": "NaNx"}}, {"a..b": {"DataType": "String", "StringValue": "x"}},
                 {f"n{i}": {"DataType": "String", "StringValue": "x"} for i in range(11)}]
    for attrs in bad_attrs:
        with pytest.raises(ClientError) as e:
            env.sqs.send_message(QueueUrl=url, MessageBody="x", MessageAttributes=attrs)
        assert code(e) == "InvalidParameterValue", attrs
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="x" * 262145)
    assert code(e) == "InvalidParameterValue"
    env.sqs.send_message(QueueUrl=url, MessageBody="x" * 262144)
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="bad\x01char")
    assert code(e) == "InvalidMessageContents"
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="x", DelaySeconds=901)
    assert code(e) == "InvalidParameterValue"
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="x", MessageDeduplicationId="d")
    assert code(e) == "InvalidParameterValue"
    env.sqs.set_queue_attributes(QueueUrl=url, Attributes={"MaximumMessageSize": "1024"})
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="x" * 1025)
    assert code(e) == "InvalidParameterValue"
    for kw in ({"MaxNumberOfMessages": 0}, {"MaxNumberOfMessages": 11}, {"WaitTimeSeconds": 21}, {"VisibilityTimeout": 43201}):
        with pytest.raises(ClientError) as e:
            env.sqs.receive_message(QueueUrl=url, **kw)
        assert code(e) == "InvalidParameterValue", kw


def test_visibility_timeout_change_and_receipt_handles(env):
    name, url = env.new_queue("vis", VisibilityTimeout="2")
    env.sqs.send_message(QueueUrl=url, MessageBody="one")
    m = env.sqs.receive_message(QueueUrl=url)["Messages"][0]
    assert not env.sqs.receive_message(QueueUrl=url).get("Messages")
    c = env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    assert (c["ApproximateNumberOfMessages"], c["ApproximateNumberOfMessagesNotVisible"]) == ("0", "1")
    env.sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=6)
    time.sleep(2.6)
    assert not env.sqs.receive_message(QueueUrl=url).get("Messages"), "visibility was extended to 6s"
    env.sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    again = env.sqs.receive_message(QueueUrl=url)["Messages"][0]
    assert again["MessageId"] == m["MessageId"] and again["ReceiptHandle"] != m["ReceiptHandle"]
    with pytest.raises(ClientError) as e:
        env.sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=again["ReceiptHandle"], VisibilityTimeout=43201)
    assert code(e) == "InvalidParameterValue"
    with pytest.raises(ClientError) as e:
        env.sqs.delete_message(QueueUrl=url, ReceiptHandle="garbage")
    assert code(e) == "ReceiptHandleIsInvalid"
    with pytest.raises(ClientError) as e:
        env.sqs.change_message_visibility(QueueUrl=url, ReceiptHandle="garbage", VisibilityTimeout=5)
    assert code(e) == "ReceiptHandleIsInvalid"
    env.sqs.delete_message(QueueUrl=url, ReceiptHandle=again["ReceiptHandle"])
    env.sqs.delete_message(QueueUrl=url, ReceiptHandle=again["ReceiptHandle"])  # deleting twice is fine, like real SQS
    # a message that is not in flight cannot have its visibility changed
    env.sqs.send_message(QueueUrl=url, MessageBody="two")
    m2 = env.sqs.receive_message(QueueUrl=url)["Messages"][0]
    env.sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m2["ReceiptHandle"], VisibilityTimeout=0)
    with pytest.raises(ClientError) as e:
        env.sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m2["ReceiptHandle"], VisibilityTimeout=10)
    assert code(e) == "AWS.SimpleQueueService.MessageNotInflight"
    # receive-time VisibilityTimeout overrides the queue default
    env.sqs.receive_message(QueueUrl=url, VisibilityTimeout=1)
    time.sleep(1.4)
    assert env.sqs.receive_message(QueueUrl=url).get("Messages")


def test_delay_queue_and_per_message_delay(env):
    name, url = env.new_queue("delay", DelaySeconds="2")
    env.sqs.send_message(QueueUrl=url, MessageBody="delayed")
    assert not env.sqs.receive_message(QueueUrl=url).get("Messages")
    a = env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    assert (a["ApproximateNumberOfMessages"], a["ApproximateNumberOfMessagesDelayed"]) == ("0", "1")
    time.sleep(2.2)
    assert env.sqs.receive_message(QueueUrl=url)["Messages"][0]["Body"] == "delayed"
    n2, u2 = env.new_queue("delay2")
    env.sqs.send_message(QueueUrl=u2, MessageBody="now")
    env.sqs.send_message(QueueUrl=u2, MessageBody="later", DelaySeconds=2)
    assert [m["Body"] for m in env.sqs.receive_message(QueueUrl=u2, MaxNumberOfMessages=10)["Messages"]] == ["now"]
    time.sleep(2.2)
    assert [m["Body"] for m in env.sqs.receive_message(QueueUrl=u2, MaxNumberOfMessages=10)["Messages"]] == ["later"]
    # per-message delay is not allowed on FIFO queues
    fn, fu = env.new_queue("delayf", FifoQueue="true")
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=fu, MessageBody="x", MessageGroupId="g", MessageDeduplicationId="d", DelaySeconds=5)
    assert code(e) == "InvalidParameterValue"


def test_batch_operations(env):
    name, url = env.new_queue("batch")
    entries = [{"Id": f"m{i}", "MessageBody": f"body-{i}",
                "MessageAttributes": {"k": {"DataType": "String", "StringValue": str(i)}}} for i in range(10)]
    r = env.sqs.send_message_batch(QueueUrl=url, Entries=entries)
    assert [s["Id"] for s in r["Successful"]] == [e["Id"] for e in entries] and not r.get("Failed")
    for s, e in zip(r["Successful"], entries):
        assert s["MD5OfMessageBody"] == hashlib.md5(e["MessageBody"].encode()).hexdigest()
        assert s["MD5OfMessageAttributes"] == md5_attrs(e["MessageAttributes"])
    # a partially bad batch: valid entries succeed, the bad one is reported per entry
    r = env.sqs.send_message_batch(QueueUrl=url, Entries=[
        {"Id": "ok", "MessageBody": "fine"}, {"Id": "bad", "MessageBody": "x", "DelaySeconds": 5000}])
    assert [s["Id"] for s in r["Successful"]] == ["ok"]
    assert r["Failed"][0]["Id"] == "bad" and r["Failed"][0]["SenderFault"] is True and r["Failed"][0]["Code"] == "InvalidParameterValue"
    # structural errors fail the whole request
    for entries, err in [([], "AWS.SimpleQueueService.EmptyBatchRequest"),
                         ([{"Id": f"e{i}", "MessageBody": "x"} for i in range(11)], "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"),
                         ([{"Id": "d", "MessageBody": "x"}, {"Id": "d", "MessageBody": "y"}], "AWS.SimpleQueueService.BatchEntryIdsNotDistinct"),
                         ([{"Id": "bad id!", "MessageBody": "x"}], "AWS.SimpleQueueService.InvalidBatchEntryId"),
                         ([{"Id": "a", "MessageBody": "x" * 200000}, {"Id": "b", "MessageBody": "y" * 100000}],
                          "AWS.SimpleQueueService.BatchRequestTooLong")]:
        with pytest.raises(ClientError) as e:
            env.sqs.send_message_batch(QueueUrl=url, Entries=entries)
        assert code(e) == err, (err, e.value.response)
    got = []
    while True:
        msgs = env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=9).get("Messages", [])
        if not msgs:
            break
        got += msgs
        # change visibility + delete in batches with per-entry failures
        cv = env.sqs.change_message_visibility_batch(QueueUrl=url, Entries=[
            {"Id": f"c{i}", "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 30} for i, m in enumerate(msgs)] + [
            {"Id": "cbad", "ReceiptHandle": "not-a-handle", "VisibilityTimeout": 30}])
        assert len(cv["Successful"]) == len(msgs) and cv["Failed"][0]["Code"] == "ReceiptHandleIsInvalid"
        dl = env.sqs.delete_message_batch(QueueUrl=url, Entries=[
            {"Id": f"d{i}", "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)] + [
            {"Id": "dbad", "ReceiptHandle": "not-a-handle"}])
        assert len(dl["Successful"]) == len(msgs) and dl["Failed"][0]["Id"] == "dbad"
    assert len(got) == 11
    assert env.rows(name) == 0


# ---------------------------------------------------------------------------------------------
# long polling
# ---------------------------------------------------------------------------------------------

def test_long_poll_waits_then_returns_immediately_on_arrival(env):
    name, url = env.new_queue("poll")
    t0 = time.time()
    assert not env.sqs.receive_message(QueueUrl=url, WaitTimeSeconds=2).get("Messages")
    assert 1.9 <= time.time() - t0 < 3.5
    threading.Timer(0.7, lambda: env.sqs.send_message(QueueUrl=url, MessageBody="late")).start()
    t0 = time.time()
    got = env.sqs.receive_message(QueueUrl=url, WaitTimeSeconds=10)["Messages"]
    took = time.time() - t0
    assert got[0]["Body"] == "late" and 0.6 <= took < 2.0, took
    # queue-level ReceiveMessageWaitTimeSeconds applies when the request does not set WaitTimeSeconds
    n2, u2 = env.new_queue("poll2", ReceiveMessageWaitTimeSeconds="2")
    t0 = time.time()
    assert not env.sqs.receive_message(QueueUrl=u2).get("Messages")
    assert time.time() - t0 >= 1.9
    t0 = time.time()
    env.sqs.receive_message(QueueUrl=u2, WaitTimeSeconds=0)
    assert time.time() - t0 < 1.0, "an explicit WaitTimeSeconds=0 overrides the queue default"


def test_long_polls_do_not_pin_backend_connections(env):
    name, url = env.new_queue("nopin")
    other_n, other = env.new_queue("nopin-other")
    results = []

    def poll():
        c = boto3.client("sqs", endpoint_url=env.endpoint, region_name="us-east-1", aws_access_key_id="t",
                         aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}, max_pool_connections=2))
        results.append(c.receive_message(QueueUrl=url, WaitTimeSeconds=4).get("Messages", []))

    pollers = [threading.Thread(target=poll) for _ in range(60)]  # far more than any backend pool holds
    for t in pollers:
        t.start()
    time.sleep(1.0)
    t0 = time.time()
    env.sqs.send_message(QueueUrl=other, MessageBody="x")
    assert env.sqs.receive_message(QueueUrl=other)["Messages"][0]["Body"] == "x"
    assert time.time() - t0 < 2.0, "other operations must not starve behind parked long polls"
    env.sqs.send_message(QueueUrl=url, MessageBody="one")
    for t in pollers:
        t.join(timeout=15)
    assert len(results) == 60 and sum(len(r) for r in results) == 1


# ---------------------------------------------------------------------------------------------
# dead-letter queues, redrive, message move tasks (also across shards)
# ---------------------------------------------------------------------------------------------

def test_dead_letter_routing_across_shards_and_list_sources(env):
    src, src_url, dlq, dlq_url = env.queue_pair_on_different_shards("dlq")
    dlq_arn = env.arn(dlq_url)
    env.sqs.set_queue_attributes(QueueUrl=src_url, Attributes={
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 2})})
    a = env.sqs.get_queue_attributes(QueueUrl=src_url, AttributeNames=["RedrivePolicy"])["Attributes"]
    assert json.loads(a["RedrivePolicy"])["deadLetterTargetArn"] == dlq_arn
    if env.name == "sharded":
        assert env.shard_of(src) != env.shard_of(dlq)
    attrs = {"k": {"DataType": "String", "StringValue": "v"}}
    sent = env.sqs.send_message(QueueUrl=src_url, MessageBody="poison", MessageAttributes=attrs)
    env.sqs.send_message(QueueUrl=src_url, MessageBody="fine")
    seen = []
    for _ in range(2):  # receives 1 and 2 return the messages; the poison one is never deleted
        msgs = env.sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=10, VisibilityTimeout=0,
                                       MessageSystemAttributeNames=["ApproximateReceiveCount"])["Messages"]
        seen.append({m["Body"]: m["Attributes"]["ApproximateReceiveCount"] for m in msgs})
    assert seen == [{"poison": "1", "fine": "1"}, {"poison": "2", "fine": "2"}]
    third = env.sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=10)  # receive count would exceed 2 -> DLQ
    assert not third.get("Messages")
    moved = env.sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10, MessageAttributeNames=["All"],
                                    MessageSystemAttributeNames=["All"])["Messages"]
    assert sorted(m["Body"] for m in moved) == ["fine", "poison"]
    poison = [m for m in moved if m["Body"] == "poison"][0]
    assert poison["MessageId"] == sent["MessageId"], "MessageId is preserved by redrive"
    assert poison["Attributes"]["DeadLetterQueueSourceArn"] == env.arn(src_url)
    assert poison["Attributes"]["ApproximateReceiveCount"] == "1", "receive count restarts in the DLQ"
    assert poison["MessageAttributes"]["k"]["StringValue"] == "v"
    assert env.rows(src) == 0
    assert env.sqs.list_dead_letter_source_queues(QueueUrl=dlq_url)["queueUrls"] == [src_url]
    # a queue that is not a DLQ has no sources; invalid redrive policies are refused
    assert env.sqs.list_dead_letter_source_queues(QueueUrl=src_url)["queueUrls"] == []
    for bad in [{"deadLetterTargetArn": "arn:aws:sqs:us-east-1:000000000000:does-not-exist", "maxReceiveCount": 2},
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 0}, {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 1001},
                {"deadLetterTargetArn": "nonsense", "maxReceiveCount": 3}]:
        with pytest.raises(ClientError) as e:
            env.sqs.set_queue_attributes(QueueUrl=src_url, Attributes={"RedrivePolicy": json.dumps(bad)})
        assert code(e) == "InvalidParameterValue", bad
    # removing the policy stops redrive
    env.sqs.set_queue_attributes(QueueUrl=src_url, Attributes={"RedrivePolicy": ""})
    assert "RedrivePolicy" not in env.sqs.get_queue_attributes(QueueUrl=src_url, AttributeNames=["All"])["Attributes"]
    assert env.sqs.list_dead_letter_source_queues(QueueUrl=dlq_url).get("queueUrls") == []


def test_redrive_allow_policy_is_enforced(env):
    n, dlq_url = env.new_queue("allow-dlq", RedriveAllowPolicy=json.dumps({"redrivePermission": "denyAll"}))
    _, src_url = env.new_queue("allow-src")
    with pytest.raises(ClientError) as e:
        env.sqs.set_queue_attributes(QueueUrl=src_url, Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": env.arn(dlq_url), "maxReceiveCount": 2})})
    assert code(e) == "InvalidParameterValue"
    env.sqs.set_queue_attributes(QueueUrl=dlq_url, Attributes={"RedriveAllowPolicy": json.dumps({
        "redrivePermission": "byQueue", "sourceQueueArns": [env.arn(src_url)]})})
    env.sqs.set_queue_attributes(QueueUrl=src_url, Attributes={
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": env.arn(dlq_url), "maxReceiveCount": 2})})


def test_message_move_task_redrives_dlq_back_to_source_across_shards(env):
    src, src_url, dlq, dlq_url = env.queue_pair_on_different_shards("move")
    env.sqs.set_queue_attributes(QueueUrl=src_url, Attributes={
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": env.arn(dlq_url), "maxReceiveCount": 1})})
    ids = {}
    for i in range(25):
        ids[f"m{i}"] = env.sqs.send_message(QueueUrl=src_url, MessageBody=f"m{i}",
                                            MessageAttributes={"i": {"DataType": "Number", "StringValue": str(i)}})["MessageId"]
    env.sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=10, VisibilityTimeout=0)  # count 1
    deadline = time.time() + 15
    while env.rows(dlq) < 25 and time.time() < deadline:  # count 2 exceeds max 1 -> moved to the DLQ
        env.sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=10, VisibilityTimeout=0)
    assert env.rows(dlq) == 25 and env.rows(src) == 0
    src_arn, dlq_arn = env.arn(src_url), env.arn(dlq_url)
    handle = env.sqs.start_message_move_task(SourceArn=dlq_arn)["TaskHandle"]
    with pytest.raises(ClientError):
        env.sqs.start_message_move_task(SourceArn=src_arn)  # not a DLQ
    deadline = time.time() + 20
    task = None
    while time.time() < deadline:
        task = env.sqs.list_message_move_tasks(SourceArn=dlq_arn)["Results"][0]
        if task["Status"] != "RUNNING":
            break
        time.sleep(0.3)
    assert task["Status"] == "COMPLETED", task
    assert task["ApproximateNumberOfMessagesMoved"] == 25 and task["ApproximateNumberOfMessagesToMove"] == 25
    assert task["SourceArn"] == dlq_arn
    assert env.rows(dlq) == 0 and env.rows(src) == 25
    if env.name == "sharded":
        assert env.shard_of(src) != env.shard_of(dlq), "the move copied messages between backends"
    got = {}
    while len(got) < 25:
        msgs = env.sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=10, MessageAttributeNames=["All"])["Messages"]
        assert msgs
        for m in msgs:
            got[m["Body"]] = m["MessageId"]
            assert m["MessageAttributes"]["i"]["StringValue"] == m["Body"][1:]
    assert got == ids, "MessageIds survive DLQ redrive and move task"
    # an explicit destination + rate limit + cancel
    n3, other_url = env.new_queue("move-dest")
    for i in range(6):
        env.sqs.send_message(QueueUrl=dlq_url, MessageBody=f"d{i}")
    t = env.sqs.start_message_move_task(SourceArn=dlq_arn, DestinationArn=env.arn(other_url), MaxNumberOfMessagesPerSecond=1)
    running = env.sqs.list_message_move_tasks(SourceArn=dlq_arn)["Results"][0]
    assert running["Status"] == "RUNNING" and running["DestinationArn"] == env.arn(other_url)
    with pytest.raises(ClientError):
        env.sqs.start_message_move_task(SourceArn=dlq_arn)  # only one running task per source
    time.sleep(1.5)
    env.sqs.cancel_message_move_task(TaskHandle=t["TaskHandle"])
    for _ in range(40):
        st = env.sqs.list_message_move_tasks(SourceArn=dlq_arn)["Results"][0]
        if st["Status"] == "CANCELLED":
            break
        time.sleep(0.25)
    assert st["Status"] == "CANCELLED" and 0 < st["ApproximateNumberOfMessagesMoved"] < 6, st
    assert env.rows(other_url.rsplit("/", 1)[1]) == st["ApproximateNumberOfMessagesMoved"]
    with pytest.raises(ClientError):
        env.sqs.cancel_message_move_task(TaskHandle=t["TaskHandle"])  # no longer running
    with pytest.raises(ClientError):
        env.sqs.cancel_message_move_task(TaskHandle="00000000-0000-0000-0000-000000000000")


# ---------------------------------------------------------------------------------------------
# FIFO
# ---------------------------------------------------------------------------------------------

def test_fifo_groups_ordering_and_locking(env):
    name, url = env.new_queue("fifo", FifoQueue="true")
    for g in ("A", "B"):
        for i in range(3):
            env.sqs.send_message(QueueUrl=url, MessageBody=f"{g}{i}", MessageGroupId=g, MessageDeduplicationId=f"{g}{i}")
    first = env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=2, VisibilityTimeout=2,
                                    MessageSystemAttributeNames=["All"])["Messages"]
    assert [m["Body"] for m in first] == ["A0", "A1"], "one receive returns messages of one group, in order"
    assert first[0]["Attributes"]["MessageGroupId"] == "A" and first[0]["Attributes"]["MessageDeduplicationId"] == "A0"
    seqs = [int(m["Attributes"]["SequenceNumber"]) for m in first]
    assert seqs[0] < seqs[1] and len(first[0]["Attributes"]["SequenceNumber"]) == 18
    # group A is locked while its messages are in flight: the next receive serves group B only
    second = env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, VisibilityTimeout=30)["Messages"]
    assert [m["Body"] for m in second] == ["B0", "B1", "B2"]
    assert not env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10).get("Messages")
    # A0/A1 become visible again after the visibility timeout, then A2 is only delivered after they are gone
    time.sleep(2.3)
    third = env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, VisibilityTimeout=30)["Messages"]
    assert [m["Body"] for m in third] == ["A0", "A1", "A2"]
    for m in third:
        env.sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    assert not env.sqs.receive_message(QueueUrl=url).get("Messages")  # B still in flight, A drained


def test_fifo_deduplication_window_content_based_and_scope(env):
    name, url = env.new_queue("fifodedup", FifoQueue="true")
    a = env.sqs.send_message(QueueUrl=url, MessageBody="one", MessageGroupId="g", MessageDeduplicationId="same")
    b = env.sqs.send_message(QueueUrl=url, MessageBody="one-again", MessageGroupId="g", MessageDeduplicationId="same")
    assert a["MessageId"] == b["MessageId"] and a["SequenceNumber"] == b["SequenceNumber"]
    assert b["MD5OfMessageBody"] == hashlib.md5(b"one-again").hexdigest(), "MD5 reflects the resend, not the original"
    assert env.rows(name) == 1
    m = env.sqs.receive_message(QueueUrl=url)["Messages"][0]
    env.sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    c = env.sqs.send_message(QueueUrl=url, MessageBody="one", MessageGroupId="g", MessageDeduplicationId="same")
    assert c["MessageId"] == a["MessageId"], "dedup holds for 5 minutes even after the message was deleted"
    assert not env.sqs.receive_message(QueueUrl=url).get("Messages")
    # missing dedup id without content-based deduplication is an error; group id is required
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="x", MessageGroupId="g")
    assert code(e) == "InvalidParameterValue"
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="x", MessageDeduplicationId="d")
    assert code(e) == "MissingParameter"
    # sequence numbers increase
    s1 = env.sqs.send_message(QueueUrl=url, MessageBody="s1", MessageGroupId="g", MessageDeduplicationId="s1")["SequenceNumber"]
    s2 = env.sqs.send_message(QueueUrl=url, MessageBody="s2", MessageGroupId="g", MessageDeduplicationId="s2")["SequenceNumber"]
    assert int(s2) > int(s1)
    # content-based dedup
    n2, u2 = env.new_queue("fifocb", FifoQueue="true", ContentBasedDeduplication="true")
    x = env.sqs.send_message(QueueUrl=u2, MessageBody="same body", MessageGroupId="g")
    y = env.sqs.send_message(QueueUrl=u2, MessageBody="same body", MessageGroupId="g")
    z = env.sqs.send_message(QueueUrl=u2, MessageBody="different", MessageGroupId="g")
    assert x["MessageId"] == y["MessageId"] != z["MessageId"]
    # dedup scope per message group: same dedup id in different groups are different messages
    n3, u3 = env.new_queue("fifoscope", FifoQueue="true", DeduplicationScope="messageGroup", FifoThroughputLimit="perMessageGroupId")
    p = env.sqs.send_message(QueueUrl=u3, MessageBody="1", MessageGroupId="g1", MessageDeduplicationId="d")
    q = env.sqs.send_message(QueueUrl=u3, MessageBody="2", MessageGroupId="g2", MessageDeduplicationId="d")
    r = env.sqs.send_message(QueueUrl=u3, MessageBody="3", MessageGroupId="g1", MessageDeduplicationId="d")
    assert p["MessageId"] != q["MessageId"] and p["MessageId"] == r["MessageId"]


def test_fifo_receive_request_attempt_id_is_idempotent(env):
    name, url = env.new_queue("fifoattempt", FifoQueue="true")
    for i in range(3):
        env.sqs.send_message(QueueUrl=url, MessageBody=f"m{i}", MessageGroupId="g", MessageDeduplicationId=f"d{i}")
    a = env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, ReceiveRequestAttemptId="attempt-1")["Messages"]
    b = env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, ReceiveRequestAttemptId="attempt-1")["Messages"]
    assert [m["ReceiptHandle"] for m in a] == [m["ReceiptHandle"] for m in b] and len(a) == 3
    assert not env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, ReceiveRequestAttemptId="attempt-2").get("Messages")


def test_fifo_dead_letter_queue_must_be_fifo_and_moves_groups(env):
    _, std_dlq = env.new_queue("stddlq")
    _, fifo_src = env.new_queue("fifosrc", FifoQueue="true")
    with pytest.raises(ClientError) as e:
        env.sqs.set_queue_attributes(QueueUrl=fifo_src, Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": env.arn(std_dlq), "maxReceiveCount": 2})})
    assert code(e) == "InvalidParameterValue"
    fn, fifo_dlq = env.new_queue("fifodlq", FifoQueue="true")
    env.sqs.set_queue_attributes(QueueUrl=fifo_src, Attributes={
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": env.arn(fifo_dlq), "maxReceiveCount": 1})})
    env.sqs.send_message(QueueUrl=fifo_src, MessageBody="p", MessageGroupId="grp", MessageDeduplicationId="p1")
    env.sqs.receive_message(QueueUrl=fifo_src, VisibilityTimeout=0)
    assert not env.sqs.receive_message(QueueUrl=fifo_src).get("Messages")
    m = env.sqs.receive_message(QueueUrl=fifo_dlq, MessageSystemAttributeNames=["MessageGroupId"])["Messages"][0]
    assert m["Body"] == "p" and m["Attributes"]["MessageGroupId"] == "grp"


def test_concurrent_consumers_get_each_standard_message_exactly_once(env):
    name, url = env.new_queue("conc", VisibilityTimeout="60")
    total = 120
    for i in range(0, total, 10):
        env.sqs.send_message_batch(QueueUrl=url, Entries=[{"Id": f"e{j}", "MessageBody": f"m{i + j}"} for j in range(10)])
    seen, lock = [], threading.Lock()

    def consume():
        c = boto3.client("sqs", endpoint_url=env.endpoint, region_name="us-east-1", aws_access_key_id="t", aws_secret_access_key="t")
        while True:
            msgs = c.receive_message(QueueUrl=url, MaxNumberOfMessages=5).get("Messages", [])
            if not msgs:
                return
            with lock:
                seen.extend(m["Body"] for m in msgs)
            c.delete_message_batch(QueueUrl=url, Entries=[{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)])

    threads = [threading.Thread(target=consume) for _ in range(8)]
    [t.start() for t in threads]
    [t.join(timeout=60) for t in threads]
    assert sorted(seen) == sorted(f"m{i}" for i in range(total))


def test_concurrent_fifo_consumers_never_hold_two_batches_of_one_group(env):
    name, url = env.new_queue("fifoconc", FifoQueue="true")
    groups, per = 4, 15
    for g in range(groups):
        for i in range(per):
            env.sqs.send_message(QueueUrl=url, MessageBody=f"{g}:{i}", MessageGroupId=f"g{g}", MessageDeduplicationId=f"{g}-{i}")
    in_flight, order, violations, lock = set(), {}, [], threading.Lock()

    def consume():
        c = boto3.client("sqs", endpoint_url=env.endpoint, region_name="us-east-1", aws_access_key_id="t", aws_secret_access_key="t")
        idle = 0
        while idle < 15:
            msgs = c.receive_message(QueueUrl=url, MaxNumberOfMessages=3, VisibilityTimeout=30).get("Messages", [])
            if not msgs:
                idle += 1
                time.sleep(0.05)
                continue
            idle = 0
            with lock:
                gs = {m["Body"].split(":")[0] for m in msgs}
                for g in gs:
                    if g in in_flight:
                        violations.append(g)
                    in_flight.add(g)
                for m in msgs:
                    g, i = m["Body"].split(":")
                    order.setdefault(g, []).append(int(i))
            time.sleep(0.01)
            with lock:
                in_flight.difference_update(gs)
            c.delete_message_batch(QueueUrl=url, Entries=[{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)])

    threads = [threading.Thread(target=consume) for _ in range(6)]
    [t.start() for t in threads]
    [t.join(timeout=90) for t in threads]
    assert not violations, violations
    for g, seq in order.items():
        assert seq == sorted(seq) and len(seq) == per, (g, seq)
    assert len(order) == groups


# ---------------------------------------------------------------------------------------------
# purge, delete, retention
# ---------------------------------------------------------------------------------------------

def test_purge_delete_and_recreate_queue(env):
    name, url = env.new_queue("purge")
    for i in range(5):
        env.sqs.send_message(QueueUrl=url, MessageBody=str(i))
    env.sqs.purge_queue(QueueUrl=url)
    assert not env.sqs.receive_message(QueueUrl=url).get("Messages")
    env.sqs.send_message(QueueUrl=url, MessageBody="after")
    env.sqs.delete_queue(QueueUrl=url)
    with pytest.raises(ClientError) as e:
        env.sqs.send_message(QueueUrl=url, MessageBody="x")
    assert code(e) == "AWS.SimpleQueueService.NonExistentQueue"
    assert url not in env.sqs.list_queues(QueueNamePrefix=name).get("QueueUrls", [])
    env.sqs.create_queue(QueueName=name)
    assert not env.sqs.receive_message(QueueUrl=url).get("Messages"), "a recreated queue starts empty"


def test_retention_sweeper_removes_expired_messages(env):
    name, url = env.new_queue("retain", MessageRetentionPeriod="60")
    env.sqs.send_message(QueueUrl=url, MessageBody="old")
    env.sqs.send_message(QueueUrl=url, MessageBody="new")
    sql(env.pgs[env.shard_of(name)], f"UPDATE {env.table(name)} SET enqueued_at = now() - interval '2 minutes' WHERE body = 'old'")
    assert [m["Body"] for m in env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, VisibilityTimeout=0)["Messages"]] == ["new"], \
        "expired messages are never delivered"
    deadline = time.time() + 15
    while env.rows(name, "body = 'old'") and time.time() < deadline:
        time.sleep(0.5)
    assert env.rows(name, "body = 'old'") == 0 and env.rows(name, "body = 'new'") == 1


# ---------------------------------------------------------------------------------------------
# AWS Query protocol (XML)
# ---------------------------------------------------------------------------------------------

def q_text(root, path):
    node = root.find("./" + "/".join(NS + p for p in path.split("/")))
    return None if node is None else node.text


def test_query_protocol_queue_and_message_round_trip(env):
    name = f"query-{uuid.uuid4().hex[:8]}"
    status, root = env.query("CreateQueue", QueueName=name, **{"Attribute.1.Name": "VisibilityTimeout", "Attribute.1.Value": "45",
                                                              "Tag.1.Key": "k", "Tag.1.Value": "v"})
    assert status == 200 and root.tag == NS + "CreateQueueResponse"
    url = q_text(root, "CreateQueueResult/QueueUrl")
    assert url == f"{env.endpoint}/000000000000/{name}" and q_text(root, "ResponseMetadata/RequestId")
    assert q_text(env.query("GetQueueUrl", QueueName=name)[1], "GetQueueUrlResult/QueueUrl") == url
    listed = [n.text for n in env.query("ListQueues", QueueNamePrefix=name)[1].iter(NS + "QueueUrl")]
    assert listed == [url]
    # attributes
    root = env.query("GetQueueAttributes", QueueUrl=url, **{"AttributeName.1": "VisibilityTimeout", "AttributeName.2": "QueueArn"})[1]
    attrs = {a.find(NS + "Name").text: a.find(NS + "Value").text for a in root.iter(NS + "Attribute")}
    assert attrs == {"VisibilityTimeout": "45", "QueueArn": f"arn:aws:sqs:us-east-1:000000000000:{name}"}
    # send with message attributes (also addressing the queue by URL path only, like Query-protocol SDKs do)
    path = "/" + url.split("/", 3)[3]
    status, root = env.query("SendMessage", path=path, MessageBody="x < y & \"z\"",
                             **{"MessageAttribute.1.Name": "a", "MessageAttribute.1.Value.DataType": "String",
                                "MessageAttribute.1.Value.StringValue": "1",
                                "MessageAttribute.2.Name": "b", "MessageAttribute.2.Value.DataType": "Binary",
                                "MessageAttribute.2.Value.BinaryValue": base64.b64encode(b"\x00\x01").decode()})
    assert status == 200
    expected = md5_attrs({"a": {"DataType": "String", "StringValue": "1"}, "b": {"DataType": "Binary", "BinaryValue": b"\x00\x01"}})
    assert q_text(root, "SendMessageResult/MD5OfMessageAttributes") == expected
    assert q_text(root, "SendMessageResult/MD5OfMessageBody") == hashlib.md5(b"x < y & \"z\"").hexdigest()
    root = env.query("ReceiveMessage", QueueUrl=url, MaxNumberOfMessages=5, **{"AttributeName.1": "All", "MessageAttributeName.1": "All"})[1]
    msg = root.find(f"./{NS}ReceiveMessageResult/{NS}Message")
    assert msg.find(NS + "Body").text == "x < y & \"z\""
    assert msg.find(NS + "MD5OfMessageAttributes").text == expected
    ma = {m.find(NS + "Name").text: m.find(NS + "Value") for m in msg.iter(NS + "MessageAttribute")}
    assert ma["a"].find(NS + "StringValue").text == "1" and base64.b64decode(ma["b"].find(NS + "BinaryValue").text) == b"\x00\x01"
    sysattrs = {a.find(NS + "Name").text: a.find(NS + "Value").text for a in msg.findall(NS + "Attribute")}
    assert sysattrs["ApproximateReceiveCount"] == "1"
    handle = msg.find(NS + "ReceiptHandle").text
    status, root = env.query("DeleteMessage", QueueUrl=url, ReceiptHandle=handle)
    assert status == 200 and root.tag == NS + "DeleteMessageResponse" and root.find(NS + "DeleteMessageResult") is None
    assert env.query("ReceiveMessage", QueueUrl=url)[1].find(f".//{NS}Message") is None
    # tags and queue deletion
    tags = {t.find(NS + "Key").text: t.find(NS + "Value").text for t in env.query("ListQueueTags", QueueUrl=url)[1].iter(NS + "Tag")}
    assert tags == {"k": "v"}
    assert env.query("TagQueue", QueueUrl=url, **{"Tag.1.Key": "n", "Tag.1.Value": "m"})[0] == 200
    assert env.query("UntagQueue", QueueUrl=url, **{"TagKey.1": "k"})[0] == 200
    assert {t.find(NS + "Key").text for t in env.query("ListQueueTags", QueueUrl=url)[1].iter(NS + "Tag")} == {"n"}
    assert env.query("SetQueueAttributes", QueueUrl=url, **{"Attribute.1.Name": "DelaySeconds", "Attribute.1.Value": "1"})[0] == 200
    assert env.query("DeleteQueue", QueueUrl=url)[0] == 200


def test_query_protocol_errors_use_the_xml_error_shape(env):
    status, root = env.query("GetQueueUrl", QueueName="query-ghost-queue")
    assert status == 400 and root.tag == NS + "ErrorResponse"
    assert root.find(f"./{NS}Error/{NS}Type").text == "Sender"
    assert root.find(f"./{NS}Error/{NS}Code").text == "AWS.SimpleQueueService.NonExistentQueue"
    assert root.find(f"./{NS}Error/{NS}Message").text and root.find(NS + "RequestId").text
    status, root = env.query("CreateQueue", QueueName="bad name!")
    assert status == 400 and root.find(f"./{NS}Error/{NS}Code").text == "InvalidParameterValue"
    status, root = env.query("NoSuchAction")
    assert status == 400 and root.tag == NS + "ErrorResponse"
    n, u = env.new_queue("qerr")
    status, root = env.query("SendMessageBatch", QueueUrl=u, **{"SendMessageBatchRequestEntry.1.Id": "a"})
    assert status == 200  # an entry without a body is a per-entry failure, not a request error
    assert root.find(f".//{NS}BatchResultErrorEntry/{NS}Code").text == "MissingParameter"


def test_query_protocol_batches_fifo_and_move_tasks(env):
    n, u = env.new_queue("qbatch")
    status, root = env.query("SendMessageBatch", QueueUrl=u, **{
        "SendMessageBatchRequestEntry.1.Id": "a", "SendMessageBatchRequestEntry.1.MessageBody": "one",
        "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name": "k",
        "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType": "String",
        "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue": "v",
        "SendMessageBatchRequestEntry.2.Id": "b", "SendMessageBatchRequestEntry.2.MessageBody": "two",
        "SendMessageBatchRequestEntry.3.Id": "c", "SendMessageBatchRequestEntry.3.MessageBody": "three",
        "SendMessageBatchRequestEntry.3.DelaySeconds": "99999"})
    assert status == 200
    ok = {e.find(NS + "Id").text: e for e in root.iter(NS + "SendMessageBatchResultEntry")}
    assert set(ok) == {"a", "b"}
    assert ok["a"].find(NS + "MD5OfMessageAttributes").text == md5_attrs({"k": {"DataType": "String", "StringValue": "v"}})
    err = root.find(f".//{NS}BatchResultErrorEntry")
    assert err.find(NS + "Id").text == "c" and err.find(NS + "SenderFault").text == "true"
    assert err.find(NS + "Code").text == "InvalidParameterValue"
    msgs = env.query("ReceiveMessage", QueueUrl=u, MaxNumberOfMessages=10)[1].findall(f".//{NS}Message")
    assert sorted(m.find(NS + "Body").text for m in msgs) == ["one", "two"]
    params = {}
    for i, m in enumerate(msgs, 1):
        params[f"ChangeMessageVisibilityBatchRequestEntry.{i}.Id"] = f"c{i}"
        params[f"ChangeMessageVisibilityBatchRequestEntry.{i}.ReceiptHandle"] = m.find(NS + "ReceiptHandle").text
        params[f"ChangeMessageVisibilityBatchRequestEntry.{i}.VisibilityTimeout"] = "60"
    assert len(list(env.query("ChangeMessageVisibilityBatch", QueueUrl=u, **params)[1].iter(NS + "ChangeMessageVisibilityBatchResultEntry"))) == 2
    params = {}
    for i, m in enumerate(msgs, 1):
        params[f"DeleteMessageBatchRequestEntry.{i}.Id"] = f"d{i}"
        params[f"DeleteMessageBatchRequestEntry.{i}.ReceiptHandle"] = m.find(NS + "ReceiptHandle").text
    params["DeleteMessageBatchRequestEntry.3.Id"] = "dbad"
    params["DeleteMessageBatchRequestEntry.3.ReceiptHandle"] = "junk"
    root = env.query("DeleteMessageBatch", QueueUrl=u, **params)[1]
    assert len(list(root.iter(NS + "DeleteMessageBatchResultEntry"))) == 2
    assert root.find(f".//{NS}BatchResultErrorEntry/{NS}Code").text == "ReceiptHandleIsInvalid"
    # FIFO through the Query protocol
    fname = f"qfifo-{uuid.uuid4().hex[:6]}.fifo"
    furl = q_text(env.query("CreateQueue", QueueName=fname, **{"Attribute.1.Name": "FifoQueue", "Attribute.1.Value": "true"})[1],
                  "CreateQueueResult/QueueUrl")
    r1 = env.query("SendMessage", QueueUrl=furl, MessageBody="f1", MessageGroupId="g", MessageDeduplicationId="d1")[1]
    r2 = env.query("SendMessage", QueueUrl=furl, MessageBody="f1", MessageGroupId="g", MessageDeduplicationId="d1")[1]
    assert q_text(r1, "SendMessageResult/MessageId") == q_text(r2, "SendMessageResult/MessageId")
    assert len(q_text(r1, "SendMessageResult/SequenceNumber")) == 18
    # DLQ + move task + list sources through Query
    dn, durl = env.new_queue("qdlq")
    darn = env.arn(durl)
    assert env.query("SetQueueAttributes", QueueUrl=u, **{"Attribute.1.Name": "RedrivePolicy", "Attribute.1.Value":
                     json.dumps({"deadLetterTargetArn": darn, "maxReceiveCount": 5})})[0] == 200
    srcs = [x.text for x in env.query("ListDeadLetterSourceQueues", QueueUrl=durl)[1].iter(NS + "QueueUrl")]
    assert srcs == [u]
    handle = q_text(env.query("StartMessageMoveTask", SourceArn=darn)[1], "StartMessageMoveTaskResult/TaskHandle")
    assert handle
    tasks = env.query("ListMessageMoveTasks", SourceArn=darn)[1].findall(f".//{NS}ListMessageMoveTasksResultEntry")
    assert tasks and tasks[0].find(NS + "SourceArn").text == darn
    time.sleep(1.5)  # let the (empty) task complete so it is not left running


# ---------------------------------------------------------------------------------------------
# the JSON protocol wire format itself
# ---------------------------------------------------------------------------------------------

def test_json_protocol_error_shape_and_headers(env):
    r = requests.post(env.endpoint, headers={"X-Amz-Target": "AmazonSQS.GetQueueUrl", "Content-Type": "application/x-amz-json-1.0"},
                      data=json.dumps({"QueueName": "ghost-json"}), timeout=10)
    assert r.status_code == 400 and r.headers["Content-Type"].startswith("application/x-amz-json-1.0")
    assert r.headers["x-amzn-query-error"] == "AWS.SimpleQueueService.NonExistentQueue;Sender"
    assert r.headers["x-amzn-RequestId"]
    body = r.json()
    assert body["__type"] == "com.amazonaws.sqs#QueueDoesNotExist" and "queue" in body["message"].lower()
    r = requests.post(env.endpoint, headers={"X-Amz-Target": "AmazonSQS.NoSuchThing"}, data="{}", timeout=10)
    assert r.status_code == 400
    r = requests.post(env.endpoint, headers={"X-Amz-Target": "AmazonSQS.ListQueues"}, data="{not json", timeout=10)
    assert r.status_code == 400


def test_queues_created_before_this_version_keep_working(env):
    """A queue whose table/catalog row predate the newer columns (no message_id, attrs, dedup table ...) is upgraded in place."""
    name = f"legacy-{uuid.uuid4().hex[:6]}"
    pg = env.pgs[0]
    table = env.table(name)
    home = env.pgs[0]
    for target in env.pgs:
        sql(target, f"CREATE TABLE {table} (msg_id BIGSERIAL PRIMARY KEY, receipt_handle TEXT, vt TIMESTAMPTZ NOT NULL DEFAULT now(), "
                    "enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(), read_ct INT NOT NULL DEFAULT 0, body TEXT NOT NULL, "
                    "message_group_id TEXT, dedup_id TEXT)")
    for target in env.pgs:
        sql(target, f"INSERT INTO {table} (body) VALUES ('from the old schema')")
    sql(home, "INSERT INTO sqs_queues_catalog (queue_name, visibility_timeout, is_fifo) VALUES (%s, 30, false)", (name,))
    url = env.sqs.get_queue_url(QueueName=name)["QueueUrl"]
    got = []
    for _ in range(4):
        got += [m["Body"] for m in env.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
                .get("Messages", [])]
    assert "from the old schema" in got
    a = env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    assert a["VisibilityTimeout"] == "30" and a["QueueArn"].endswith(name)
    env.sqs.send_message(QueueUrl=url, MessageBody="new", MessageAttributes={"a": {"DataType": "String", "StringValue": "b"}})
    env.sqs.set_queue_attributes(QueueUrl=url, Attributes={"DelaySeconds": "0"})
