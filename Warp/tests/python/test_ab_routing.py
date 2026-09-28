"""A/B routing between a "cloud" and Warp's local Postgres-backed emulation (com.sayonora.warp.ab), for the
AWS-family frontends s3wire, dynamowire, sqswire and the unified awswire endpoint.

The real cloud is NOT reachable from the test machine, so the stand-in "cloud" is a SECOND real Warp process
on its own Postgres (Floci's Docker image is not pulled here), with its own credentials, so a request that
reaches it was provably re-signed by the local Warp with the cloud target's identity. The Warp under test
("local") is the one clients point at. Fake STS / fake cloud HTTP servers cover the credential flows.
NOTHING here talks to real AWS, Azure or Google: assume-role, web identity and the default chain are verified
only against the fake STS below (and the AWS SigV4 published test vector in the Java unit tests).

Set WARP_TEST_PG_LOCAL=1 (native Postgres) and WARP_TEST_JAR=<jar>. Cluster env is set on every Warp.
"""
import hashlib
import http.server
import json
import os
import random
import re
import socketserver
import threading
import time
import urllib.parse

import boto3
import psycopg2
import pytest
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from redis_warp_support import first_free_ignite_port
from warp_test_support import RealPostgres, WarpProcess, free_port, isolated_ports

ABR_ADMIN_TOKEN = "warp-test-admin-token"
ABR_SECRETS_KEY = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="  # base64 of 32 ASCII bytes
ABR_CLOUD_KEY, ABR_CLOUD_SECRET = "cloudkey", "cloud-secret-value-XYZ"
ABR_LOCAL_KEYS = {f"lk{i}": f"local-secret-{i}" for i in range(6)}
ABR_STATIC_SECRET = "SUPERSECRETVALUE123"


# ----------------------------------------------------------------------------------------------- process helpers

def abr_start_warp(pg, creds, ignite_seed=None, extra=None):
    """One Warp with s3wire + dynamowire + sqswire + awswire, the three stores enabled on `default`."""
    seed = ignite_seed or first_free_ignite_port()
    ports = {"WARP_DYNAMOWIRE_PORT": free_port(), "WARP_SQSWIRE_PORT": free_port(), "WARP_AWSWIRE_PORT": free_port()}
    cred_spec = ";".join(f"{k}={v}" for k, v in creds.items())
    env = {**isolated_ports("WARP_S3WIRE_PORT"), **ports,
           "WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_S3WIRE_ENABLED": "true",
           "WARP_S3WIRE_CREDENTIALS": cred_spec, "WARP_AWS_IAM_CREDENTIALS": cred_spec,
           "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
           "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{seed}", "WARP_ADMIN_TOKEN": ABR_ADMIN_TOKEN,
           "SAYONORA_ENCRYPTION_KEY": ABR_SECRETS_KEY}
    env.update(extra or {})
    proc = WarpProcess(pg, "WARP_S3WIRE_PORT", frontend_name="s3wire", extra_env=env)
    proc.seed = seed
    proc.s3 = f"http://localhost:{proc.frontend_port}"
    proc.dynamo = f"http://localhost:{ports['WARP_DYNAMOWIRE_PORT']}"
    proc.sqs = f"http://localhost:{ports['WARP_SQSWIRE_PORT']}"
    proc.aws = f"http://localhost:{ports['WARP_AWSWIRE_PORT']}"
    abr_api(proc, "PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": ["s3", "dynamodb", "sqs"]})
    abr_wait_port(ports["WARP_DYNAMOWIRE_PORT"])
    abr_wait_port(ports["WARP_SQSWIRE_PORT"])
    abr_wait_port(ports["WARP_AWSWIRE_PORT"])
    return proc


def abr_wait_port(port, timeout=30):
    import socket
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise TimeoutError(f"port {port} never opened")


def abr_api(warp, method, path, body=None, expect=200):
    r = requests.request(method, f"http://localhost:{warp.metrics_port}{path}", json=body, timeout=60,
                         headers={"Authorization": f"Bearer {ABR_ADMIN_TOKEN}"})
    assert r.status_code == expect, (method, path, r.status_code, r.text)
    return r.json() if r.text else None


def abr_until(cond, timeout=20, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(step)
    raise AssertionError("condition not met in time")


def abr_s3(endpoint, key, secret):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret,
                        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}))


def abr_ddb(endpoint, key, secret):
    return boto3.client("dynamodb", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret,
                        region_name="us-east-1", config=Config(retries={"max_attempts": 1}))


def abr_sqs(endpoint, key="x", secret="x"):
    return boto3.client("sqs", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret,
                        region_name="us-east-1", config=Config(retries={"max_attempts": 1}))


def abr_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def abr_body(s3, bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def abr_cloud_target(cloud, name="cloud", **over):
    t = {"region": "us-east-1",
         "endpoints": {"s3": cloud.s3, "dynamodb": cloud.dynamo, "sqs": cloud.sqs},
         "auth": {"type": "static", "accessKeyId": ABR_CLOUD_KEY, "secretAccessKey": ABR_CLOUD_SECRET}}
    t.update(over)
    return t


def abr_policy(warp, store, **p):
    p.setdefault("target", "cloud")
    return abr_api(warp, "PUT", f"/api/ab-routing/policies/{store}", p)


def abr_reset(warp):
    """Everything local again (delete policies + kill switch), targets kept; clears the compare ring."""
    abr_api(warp, "DELETE", "/api/ab-routing/kill-switch")
    for s in ("s3", "dynamodb", "sqs"):
        abr_api(warp, "DELETE", f"/api/ab-routing/policies/{s}")
    abr_api(warp, "DELETE", "/api/ab-routing/compare")


# --------------------------------------------------------------------------------------------------- fixtures

@pytest.fixture(scope="module")
def abr_pgs():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def abr_cloud(abr_pgs):
    w = abr_start_warp(abr_pgs[1], {ABR_CLOUD_KEY: ABR_CLOUD_SECRET})
    yield w
    w.close()


@pytest.fixture(scope="module")
def abr_local(abr_pgs, abr_cloud):
    # -Xmx200m: any whole-object buffering of the 96 MiB streaming test would fail loudly
    w = abr_start_warp(abr_pgs[0], ABR_LOCAL_KEYS, extra={"JAVA_TOOL_OPTIONS": "-Xmx200m"})
    abr_api(w, "PUT", "/api/ab-routing/targets/cloud", abr_cloud_target(abr_cloud))
    yield w
    w.close()


@pytest.fixture()
def abr_clean(abr_local, abr_cloud):
    abr_reset(abr_local)
    yield


@pytest.fixture(scope="module")
def abr_clients(abr_local, abr_cloud):
    k0, s0 = "lk0", ABR_LOCAL_KEYS["lk0"]
    c = {"l3": abr_s3(abr_local.s3, k0, s0), "c3": abr_s3(abr_cloud.s3, ABR_CLOUD_KEY, ABR_CLOUD_SECRET),
         "ld": abr_ddb(abr_local.dynamo, k0, s0), "cd": abr_ddb(abr_cloud.dynamo, ABR_CLOUD_KEY, ABR_CLOUD_SECRET),
         "lq": abr_sqs(abr_local.sqs), "cq": abr_sqs(abr_cloud.sqs)}
    for side in ("l3", "c3"):
        c[side].create_bucket(Bucket="abbkt")
    return c


# ------------------------------------------------------------------------------------------------- the tests

def test_abr_default_is_local_and_api_shape(abr_local, abr_clients, abr_clean):
    """No policy = everything local; the config document is served without secrets."""
    c = abr_clients
    c["l3"].put_object(Bucket="abbkt", Key="plain", Body=b"hello")
    assert abr_body(c["l3"], "abbkt", "plain") == b"hello"
    assert not abr_exists(c["c3"], "abbkt", "plain")
    st = abr_api(abr_local, "GET", "/api/ab-routing")
    assert st["policies"] == {} and st["killSwitch"] is None and st["persistent"] is True
    assert st["secretsEncryptedAtRest"] is True
    t = [x for x in st["targets"] if x["name"] == "cloud"][0]
    assert t["auth"]["secretAccessKeySet"] is True and "secretAccessKey" not in t["auth"]


def test_abr_policy_validation(abr_local, abr_clean):
    abr_api(abr_local, "PUT", "/api/ab-routing/policies/s3", {"mode": "split", "cloudPercent": 150, "target": "cloud"}, expect=400)
    abr_api(abr_local, "PUT", "/api/ab-routing/policies/s3", {"mode": "cloud"}, expect=400)  # cloud needs a target
    abr_api(abr_local, "PUT", "/api/ab-routing/policies/s3", {"mode": "cloud", "target": "nope"}, expect=400)
    abr_api(abr_local, "PUT", "/api/ab-routing/policies/redis", {"mode": "local"}, expect=400)
    abr_api(abr_local, "PUT", "/api/ab-routing/targets/az", {"auth": {"type": "azure-service-principal"}}, expect=400)
    err = abr_api(abr_local, "PUT", "/api/ab-routing/targets/gg", {"auth": {"type": "google-service-account"}}, expect=400)
    assert "UNIMPLEMENTED" in err["error"]
    abr_api(abr_local, "DELETE", "/api/ab-routing/targets/cloud", expect=200)  # unused right now
    abr_api(abr_local, "PUT", "/api/ab-routing/targets/cloud", {"auth": {"type": "static"}}, expect=400)  # needs keys


@pytest.fixture()
def abr_target_back(abr_local, abr_cloud):
    abr_api(abr_local, "PUT", "/api/ab-routing/targets/cloud", abr_cloud_target(abr_cloud))


def test_abr_cloud_mode_reroutes_s3_dynamodb_sqs(abr_local, abr_cloud, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    schema = dict(TableName="abt", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                  AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    c["cd"].create_table(**schema)
    c["ld"].create_table(**schema)
    for s in ("s3", "dynamodb", "sqs"):
        abr_policy(abr_local, s, mode="cloud")
    # S3: client talks to Warp with LOCAL credentials, the object lands in the cloud side only
    c["l3"].put_object(Bucket="abbkt", Key="via-cloud", Body=b"cloud-bytes")
    assert abr_body(c["c3"], "abbkt", "via-cloud") == b"cloud-bytes"
    assert abr_body(c["l3"], "abbkt", "via-cloud") == b"cloud-bytes"  # read through Warp comes from the cloud too
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    assert not abr_exists(c["l3"], "abbkt", "via-cloud"), "local emulation never got it"
    # DynamoDB (the cloud side REQUIRES SigV4 of cloudkey: proves re-signing with the target's identity)
    c["ld"].put_item(TableName="abt", Item={"id": {"S": "1"}, "v": {"S": "sent-via-warp"}})
    got = c["cd"].get_item(TableName="abt", Key={"id": {"S": "1"}})
    assert got["Item"]["v"]["S"] == "sent-via-warp"
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/dynamodb")
    assert "Item" not in c["ld"].get_item(TableName="abt", Key={"id": {"S": "1"}})
    # SQS: the client's queue URL is Warp's; it is translated for the cloud and the answer translated back
    abr_policy(abr_local, "sqs", mode="cloud")
    q = c["lq"].create_queue(QueueName="abq")["QueueUrl"]
    assert q.startswith(abr_local.sqs), q  # cloud URL rewritten back to the Warp address
    c["lq"].send_message(QueueUrl=q, MessageBody="to-the-cloud")
    cq = c["cq"].get_queue_url(QueueName="abq")["QueueUrl"]
    m = c["cq"].receive_message(QueueUrl=cq, MaxNumberOfMessages=1)["Messages"]
    assert m[0]["Body"] == "to-the-cloud"
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/sqs")
    with pytest.raises(ClientError):
        c["lq"].get_queue_url(QueueName="abq")
    stats = abr_api(abr_local, "GET", "/api/ab-routing/stats")
    assert stats["sides"]["s3|cloud"]["requests"] >= 2 and stats["sides"]["dynamodb|cloud"]["requests"] >= 1


def test_abr_unified_awswire_endpoint_is_covered(abr_local, abr_cloud, abr_clients, abr_target_back, abr_clean):
    """The unified endpoint delegates in process to the same handlers, so routing applies to it too."""
    for s in ("s3", "dynamodb", "sqs"):
        abr_policy(abr_local, s, mode="cloud")
    k, s = "lk1", ABR_LOCAL_KEYS["lk1"]
    s3 = abr_s3(abr_local.aws, k, s)
    s3.put_object(Bucket="abbkt", Key="via-awswire", Body=b"unified")
    assert abr_body(abr_clients["c3"], "abbkt", "via-awswire") == b"unified"
    ddb = abr_ddb(abr_local.aws, k, s)
    names = ddb.list_tables()["TableNames"]
    assert set(names) == set(abr_clients["cd"].list_tables()["TableNames"])
    q = abr_sqs(abr_local.aws, k, s)
    urls = q.list_queues().get("QueueUrls", [])
    assert all(u.startswith(abr_local.aws) for u in urls), urls


def test_abr_sticky_split_ratio_and_stability(abr_local, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    c["l3"].put_object(Bucket="abbkt", Key="marker", Body=b"LOCAL")
    c["c3"].put_object(Bucket="abbkt", Key="marker", Body=b"CLOUD")
    abr_policy(abr_local, "s3", mode="split", cloudPercent=30, stickyBy="header:X-Client")
    s3 = abr_s3(abr_local.s3, "lk0", ABR_LOCAL_KEYS["lk0"])
    who = {"v": None}
    s3.meta.events.register("before-sign.s3.*", lambda request, **kw: request.headers.add_header("X-Client", who["v"]))
    seen = {}
    n = 500
    for i in range(n):
        who["v"] = f"tenant-{i}"
        seen[who["v"]] = abr_body(s3, "abbkt", "marker")
    cloud = sum(1 for v in seen.values() if v == b"CLOUD")
    assert abs(100.0 * cloud / n - 30) < 7, cloud
    # sticky: asking again gives every client the same answer
    for i in random.sample(range(n), 60):
        who["v"] = f"tenant-{i}"
        assert abr_body(s3, "abbkt", "marker") == seen[who["v"]]


def test_abr_rules_by_access_key_header_and_ip(abr_local, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    c["l3"].put_object(Bucket="abbkt", Key="rk", Body=b"LOCAL")
    c["c3"].put_object(Bucket="abbkt", Key="rk", Body=b"CLOUD")
    s_a = abr_s3(abr_local.s3, "lk2", ABR_LOCAL_KEYS["lk2"])
    s_b = abr_s3(abr_local.s3, "lk3", ABR_LOCAL_KEYS["lk3"])
    hdr = {"v": None}
    s_b.meta.events.register("before-sign.s3.*", lambda request, **kw: hdr["v"] and request.headers.add_header("X-Team", hdr["v"]))
    abr_policy(abr_local, "s3", mode="local", rules=[{"name": "beta-key", "accessKey": "lk2", "route": "cloud"}])
    assert abr_body(s_a, "abbkt", "rk") == b"CLOUD"
    assert abr_body(s_b, "abbkt", "rk") == b"LOCAL"
    abr_policy(abr_local, "s3", mode="local", rules=[{"header": "X-Team", "headerValue": "beta*", "route": "cloud"}])
    assert abr_body(s_b, "abbkt", "rk") == b"LOCAL"
    hdr["v"] = "beta-7"
    assert abr_body(s_b, "abbkt", "rk") == b"CLOUD"
    hdr["v"] = "alpha"
    assert abr_body(s_b, "abbkt", "rk") == b"LOCAL"
    abr_policy(abr_local, "s3", mode="local", rules=[{"ip": "10.0.0.0/8", "route": "cloud"}])
    assert abr_body(s_b, "abbkt", "rk") == b"LOCAL", "client is 127.0.0.1, not in 10/8"
    abr_policy(abr_local, "s3", mode="local", rules=[{"ip": "127.0.0.0/8", "route": "cloud"}, {"ip": "::1/128", "route": "cloud"}])
    assert abr_body(s_b, "abbkt", "rk") == b"CLOUD"
    # first match wins
    abr_policy(abr_local, "s3", mode="cloud", rules=[{"accessKey": "lk3", "route": "local"}])
    assert abr_body(s_b, "abbkt", "rk") == b"LOCAL"
    assert abr_body(s_a, "abbkt", "rk") == b"CLOUD"


def test_abr_kill_switch_is_immediate_and_cluster_wide(abr_pgs, abr_local, abr_cloud, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    c["l3"].put_object(Bucket="abbkt", Key="ks", Body=b"LOCAL")
    c["c3"].put_object(Bucket="abbkt", Key="ks", Body=b"CLOUD")
    abr_policy(abr_local, "s3", mode="cloud")
    assert abr_body(c["l3"], "abbkt", "ks") == b"CLOUD"
    # a second local node on the same control plane must follow within moments, no restart
    node_b = abr_start_warp(abr_pgs[0], ABR_LOCAL_KEYS, ignite_seed=abr_local.seed)
    try:
        s3b = abr_s3(node_b.s3, "lk0", ABR_LOCAL_KEYS["lk0"])
        abr_until(lambda: abr_body(s3b, "abbkt", "ks") == b"CLOUD")
        t0 = time.time()
        abr_api(abr_local, "POST", "/api/ab-routing/kill-switch", {"side": "local", "reason": "test"})
        assert abr_body(c["l3"], "abbkt", "ks") == b"LOCAL", "effective on the same node immediately"
        abr_until(lambda: abr_body(s3b, "abbkt", "ks") == b"LOCAL", timeout=10)
        assert time.time() - t0 < 10
        st = abr_api(node_b, "GET", "/api/ab-routing")
        assert st["killSwitch"]["side"] == "local" and st["killSwitch"]["reason"] == "test"
        # kill switch beats even a policy that says cloud; writes too
        c["l3"].put_object(Bucket="abbkt", Key="ks-write", Body=b"w")
        assert not abr_exists(c["c3"], "abbkt", "ks-write")
        # switch everything to the cloud, then lift
        abr_api(abr_local, "POST", "/api/ab-routing/kill-switch", {"side": "cloud"})
        assert abr_body(c["l3"], "abbkt", "ks") == b"CLOUD"
        abr_until(lambda: abr_body(s3b, "abbkt", "ks") == b"CLOUD")
        abr_api(node_b, "DELETE", "/api/ab-routing/kill-switch")  # lifted through the OTHER node
        abr_until(lambda: abr_api(abr_local, "GET", "/api/ab-routing")["killSwitch"] is None)
        assert abr_body(c["l3"], "abbkt", "ks") == b"CLOUD", "back to the policy (cloud)"
    finally:
        node_b.close()


def test_abr_write_owner_and_dual_write(abr_local, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    abr_policy(abr_local, "s3", mode="split", cloudPercent=100, writeOwner="local", stickyBy="accessKey")
    keys = [f"wo-local-{i}" for i in range(6)]
    for k in keys:
        c["l3"].put_object(Bucket="abbkt", Key=k, Body=k.encode())
    for k in keys:
        assert abr_exists(c["c3"], "abbkt", k) is False, "a split writes to ONE side (the owner); the cloud never got " + k
    # the read of those (local-only) objects goes to the cloud (client is a 100% cloud client) -> 404 there: DRIFT by design
    with pytest.raises(ClientError):
        c["l3"].get_object(Bucket="abbkt", Key=keys[0])
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    assert all(abr_exists(c["l3"], "abbkt", k) for k in keys), "the owner (local) has every write"
    abr_policy(abr_local, "s3", mode="split", cloudPercent=0, writeOwner="cloud")
    for i in range(4):
        c["l3"].put_object(Bucket="abbkt", Key=f"wo-cloud-{i}", Body=b"c")
    assert all(abr_exists(c["c3"], "abbkt", f"wo-cloud-{i}") for i in range(4))
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    assert not any(abr_exists(c["l3"], "abbkt", f"wo-cloud-{i}") for i in range(4))
    # dual write only when explicitly enabled
    abr_policy(abr_local, "s3", mode="split", cloudPercent=50, writeOwner="local", dualWrite=True)
    for i in range(4):
        c["l3"].put_object(Bucket="abbkt", Key=f"dw-{i}", Body=b"dual")
    abr_until(lambda: all(abr_exists(c["c3"], "abbkt", f"dw-{i}") for i in range(4)), timeout=15)
    assert all(abr_exists(c["l3"], "abbkt", f"dw-{i}") for i in range(4))
    stats = abr_api(abr_local, "GET", "/api/ab-routing/stats")
    assert stats["compare"]["s3"]["dualWriteOk"] >= 4 and stats["compare"]["s3"]["dualWriteFailed"] == 0
    # write owner cloud + dual write: cloud answers, local gets the copy
    abr_policy(abr_local, "s3", mode="split", cloudPercent=0, writeOwner="cloud", dualWrite=True)
    c["l3"].put_object(Bucket="abbkt", Key="dw-cloud-owner", Body=b"x")
    assert abr_exists(c["c3"], "abbkt", "dw-cloud-owner")
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    assert abr_exists(c["l3"], "abbkt", "dw-cloud-owner")


def test_abr_compare_mode_records_differences(abr_local, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    c["l3"].put_object(Bucket="abbkt", Key="cmp-same", Body=b"identical")
    c["c3"].put_object(Bucket="abbkt", Key="cmp-same", Body=b"identical")
    c["l3"].put_object(Bucket="abbkt", Key="cmp-diff", Body=b"local-version")
    c["c3"].put_object(Bucket="abbkt", Key="cmp-diff", Body=b"the-cloud-version")
    c["l3"].put_object(Bucket="abbkt", Key="cmp-local-only", Body=b"only here")
    abr_policy(abr_local, "s3", mode="compare", compare={"primary": "local"})
    assert abr_body(c["l3"], "abbkt", "cmp-same") == b"identical"
    assert abr_body(c["l3"], "abbkt", "cmp-diff") == b"local-version", "the primary side's answer is what the client gets"
    with pytest.raises(ClientError):
        c["l3"].head_object(Bucket="abbkt", Key="cmp-cloud-only-missing")
    assert abr_body(c["l3"], "abbkt", "cmp-local-only") == b"only here"
    entries = abr_until(lambda: (lambda e: e if len(e["entries"]) >= 4 else None)(
        abr_api(abr_local, "GET", "/api/ab-routing/compare?store=s3")))["entries"]
    by = {}
    for e in entries:
        by.setdefault(e["op"], []).append(e)
    gets = by["GetObject"]
    assert any(e["equal"] for e in gets) and any(not e["equal"] for e in gets)
    d = [e for e in gets if not e["equal"] and e["cloudStatus"] == 200][0]
    assert d["localStatus"] == 200 and d["primary"] == "local" and d["store"] == "s3"
    assert any("body differs" in x for x in d["diffs"]) and d["localMs"] >= 0 and d["cloudMs"] >= 0
    lo = [e for e in gets if e["localStatus"] == 200 and e["cloudStatus"] == 404]
    assert lo and any(x.startswith("status: local=200 cloud=404") for x in lo[0]["diffs"])
    heads = [e for e in by.get("HeadObject", [])]
    assert heads and heads[0]["localStatus"] == 404
    only = abr_api(abr_local, "GET", "/api/ab-routing/compare?store=s3&onlyDiff=true")["entries"]
    assert only and all(not e["equal"] for e in only)
    # counters + Prometheus text
    stats = abr_api(abr_local, "GET", "/api/ab-routing/stats")
    assert stats["compare"]["s3"]["compared"] >= 4 and stats["compare"]["s3"]["differ"] >= 2
    assert stats["sides"]["s3|cloud"]["requests"] >= 4 and stats["sides"]["s3|cloud"]["errors"] == 0
    assert stats["sides"]["s3|cloud"]["histogram"][0]["leMs"] == 5
    prom = requests.get(f"http://localhost:{abr_local.metrics_port}/metrics", timeout=20).text
    assert 'warp_ab_requests_total{store="s3",side="cloud"}' in prom and 'warp_ab_compare_total{store="s3",outcome="differ"}' in prom
    # primary = cloud: the CLIENT now receives the cloud's bytes
    abr_policy(abr_local, "s3", mode="compare", compare={"primary": "cloud"})
    assert abr_body(c["l3"], "abbkt", "cmp-diff") == b"the-cloud-version"
    abr_until(lambda: any(e["primary"] == "cloud" for e in abr_api(abr_local, "GET", "/api/ab-routing/compare")["entries"]))
    # writes in compare mode go to the owner only (default local)
    c["l3"].put_object(Bucket="abbkt", Key="cmp-write", Body=b"w")
    assert not abr_exists(c["c3"], "abbkt", "cmp-write")


def test_abr_compare_dynamodb_seeded_with_different_data_and_ring_bound(abr_local, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    for cl in (c["ld"], c["cd"]):
        try:
            cl.create_table(TableName="cmpt", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
        except ClientError as e:
            assert "ResourceInUse" in str(e)
    c["ld"].put_item(TableName="cmpt", Item={"id": {"S": "a"}, "n": {"N": "1"}, "who": {"S": "local"}})
    c["cd"].put_item(TableName="cmpt", Item={"id": {"S": "a"}, "n": {"N": "1"}, "who": {"S": "cloud"}})
    c["ld"].put_item(TableName="cmpt", Item={"id": {"S": "same"}, "n": {"N": "5"}})
    c["cd"].put_item(TableName="cmpt", Item={"id": {"S": "same"}, "n": {"N": "5"}})
    abr_policy(abr_local, "dynamodb", mode="compare", compare={"bufferSize": 10, "recordValues": True})
    assert c["ld"].get_item(TableName="cmpt", Key={"id": {"S": "a"}})["Item"]["who"]["S"] == "local"
    c["ld"].get_item(TableName="cmpt", Key={"id": {"S": "same"}})
    e = abr_until(lambda: [x for x in abr_api(abr_local, "GET", "/api/ab-routing/compare?store=dynamodb&onlyDiff=true")["entries"]
                           if x["op"] == "GetItem"])[0]
    assert any("/Item/who/S: value differs" in d and "local=local" in d and "cloud=cloud" in d for d in e["diffs"]), e["diffs"]
    ok = [x for x in abr_api(abr_local, "GET", "/api/ab-routing/compare?store=dynamodb")["entries"] if x["equal"]]
    assert ok, "identical items compare equal (ConsumedCapacity / RequestId are ignored keys)"
    # the ring buffer is bounded by the policy's bufferSize
    for _ in range(25):
        c["ld"].get_item(TableName="cmpt", Key={"id": {"S": "a"}})
    time.sleep(2)
    assert len(abr_api(abr_local, "GET", "/api/ab-routing/compare?store=dynamodb&limit=500")["entries"]) == 10


def test_abr_compare_sqs_translates_queue_urls(abr_local, abr_clients, abr_target_back, abr_clean):
    c = abr_clients
    for cl in (c["lq"], c["cq"]):
        cl.create_queue(QueueName="cmpq")
    abr_policy(abr_local, "sqs", mode="compare")
    urls = c["lq"].list_queues(QueueNamePrefix="cmpq")["QueueUrls"]
    assert urls and urls[0].startswith(abr_local.sqs)
    e = abr_until(lambda: [x for x in abr_api(abr_local, "GET", "/api/ab-routing/compare?store=sqs")["entries"] if x["op"] == "ListQueues"])
    assert e[0]["equal"], e[0]["diffs"]


def test_abr_streams_multi_mb_object_through_passthrough(abr_local, abr_clients, abr_target_back, abr_clean):
    """96 MiB through a Warp whose heap is 200 MiB: the body must be streamed, never buffered whole (upload and download)."""
    c = abr_clients
    abr_policy(abr_local, "s3", mode="cloud")
    size = 96 * 1024 * 1024
    rnd = random.Random(7)
    chunk = bytes(rnd.getrandbits(8) for _ in range(1024 * 1024))
    import io

    class Gen(io.RawIOBase):
        def __init__(self):
            self.left = size
            self.md = hashlib.md5()

        def readable(self):
            return True

        def readinto(self, b):
            n = min(len(b), self.left, len(chunk))
            b[:n] = chunk[:n]
            self.left -= n
            self.md.update(chunk[:n])
            return n

    g = Gen()
    r = io.BufferedReader(g, buffer_size=1024 * 1024)
    t0 = time.time()
    c["l3"].upload_fileobj(r, "abbkt", "big-single", Config=boto3.s3.transfer.TransferConfig(multipart_threshold=1 << 40))
    up = time.time() - t0
    want = g.md.hexdigest()
    head = c["c3"].head_object(Bucket="abbkt", Key="big-single")
    assert head["ContentLength"] == size, head
    h = hashlib.md5()
    for part in c["c3"].get_object(Bucket="abbkt", Key="big-single")["Body"].iter_chunks(1024 * 1024):
        h.update(part)
    assert h.hexdigest() == want, "bytes stored in the cloud are exactly the bytes sent"
    # download through Warp
    h2 = hashlib.md5()
    total = 0
    for part in c["l3"].get_object(Bucket="abbkt", Key="big-single")["Body"].iter_chunks(1024 * 1024):
        h2.update(part)
        total += len(part)
    assert total == size and h2.hexdigest() == want
    # multipart through the passthrough too
    src = bytes(rnd.getrandbits(8) for _ in range(1024)) * (20 * 1024)  # 20 MiB
    c["l3"].upload_fileobj(io.BytesIO(src), "abbkt", "big-multipart",
                           Config=boto3.s3.transfer.TransferConfig(multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024))
    assert abr_body(c["c3"], "abbkt", "big-multipart") == src
    assert up < 300


# ------------------------------------------------------------------------------ fake STS / fake cloud, secrets

class AbrFakeCloud:
    """One HTTP server that is both a fake STS (form POST Action=AssumeRole...) and a fake S3 that records which
    access key id signed each request; issued keys are ASIA<role-or-default>N and expire after `ttl` seconds."""

    def __init__(self, ttl=5):
        self.ttl = ttl
        self.sts_calls = []
        self.s3_keys = []
        self.raw = []
        self.fail = False
        self.n = 0
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _go(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length).decode() if length else ""
                auth = self.headers.get("Authorization") or ""
                outer.raw.append(body + "|" + auth)
                form = urllib.parse.parse_qs(body)
                if self.command == "POST" and "Action" in form:
                    outer.sts_calls.append({"action": form["Action"][0], "role": form.get("RoleArn", [""])[0],
                                            "signed_by": (re.search(r"Credential=([^/]+)/", auth) or [None, None])[1],
                                            "token": form.get("WebIdentityToken", [None])[0]})
                    if outer.fail:
                        self.send_response(403)
                        self.end_headers()
                        self.wfile.write(b"<ErrorResponse><Error><Code>AccessDenied</Code></Error></ErrorResponse>")
                        return
                    outer.n += 1
                    role = form.get("RoleArn", [""])[0].rsplit("/", 1)[-1]
                    exp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + outer.ttl))
                    x = (f"<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>ASIA{role}{outer.n}</AccessKeyId>"
                         f"<SecretAccessKey>issuedsecret{outer.n}</SecretAccessKey><SessionToken>tok{outer.n}</SessionToken>"
                         f"<Expiration>{exp}</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>").encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(x)))
                    self.end_headers()
                    self.wfile.write(x)
                    return
                m = re.search(r"Credential=([^/]+)/", auth)
                outer.s3_keys.append((m.group(1) if m else None, self.headers.get("x-amz-security-token")))
                x = b"<ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>"
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(x)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(x)

            do_GET = do_POST = do_PUT = do_HEAD = do_DELETE = _go

        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = S(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


@pytest.fixture()
def abr_fake():
    f = AbrFakeCloud(ttl=5)
    yield f
    f.close()


def test_abr_sts_assume_role_refresh_and_role_override(abr_local, abr_fake, abr_clients, abr_clean):
    tgt = {"region": "us-east-1", "endpoints": {"s3": abr_fake.url},
           "auth": {"type": "assume-role", "roleArn": "arn:aws:iam::111111111111:role/warp-ab", "stsEndpoint": abr_fake.url,
                    "refreshSkewSeconds": 2, "durationSeconds": 900,
                    "source": {"type": "static", "accessKeyId": "AKIASOURCE", "secretAccessKey": "source-secret-000"}}}
    abr_api(abr_local, "PUT", "/api/ab-routing/targets/fake", tgt)
    abr_policy(abr_local, "s3", mode="cloud", target="fake", roleOverrides={"lk2": "arn:aws:iam::111111111111:role/narrow"})
    s3 = abr_s3(abr_local.s3, "lk0", ABR_LOCAL_KEYS["lk0"])
    s3.list_buckets()
    assert abr_fake.s3_keys[-1][0] == "ASIAwarp-ab1" and abr_fake.s3_keys[-1][1] == "tok1", abr_fake.s3_keys
    s3.list_buckets()
    assert abr_fake.s3_keys[-1][0] == "ASIAwarp-ab1" and len(abr_fake.sts_calls) == 1, "cached until near expiry"
    assert abr_fake.sts_calls[0]["action"] == "AssumeRole" and abr_fake.sts_calls[0]["signed_by"] == "AKIASOURCE"
    time.sleep(3.5)  # ttl 5s, skew 2s -> refresh
    s3.list_buckets()
    assert abr_fake.s3_keys[-1][0] == "ASIAwarp-ab2" and len(abr_fake.sts_calls) == 2, (abr_fake.s3_keys, abr_fake.sts_calls)
    # the per-policy role override narrows one Warp client to another role
    narrow = abr_s3(abr_local.s3, "lk2", ABR_LOCAL_KEYS["lk2"])
    narrow.list_buckets()
    assert abr_fake.s3_keys[-1][0].startswith("ASIAnarrow"), abr_fake.s3_keys[-1]
    assert abr_fake.sts_calls[-1]["role"].endswith("role/narrow")
    s3.list_buckets()
    assert abr_fake.s3_keys[-1][0].startswith("ASIAwarp-ab"), "other clients keep the default identity"
    # the client's own credentials never reach the cloud; the source secret is never sent
    blob = "\n".join(abr_fake.raw)
    assert ABR_LOCAL_KEYS["lk0"] not in blob and "source-secret-000" not in blob and "AKIASOURCE" in blob
    assert not any(k == "lk0" for k, _ in abr_fake.s3_keys)
    # target test endpoint resolves creds without returning them
    t = abr_api(abr_local, "POST", "/api/ab-routing/targets/fake/test")
    assert t["ok"] is True and "issuedsecret" not in json.dumps(t)
    # STS failure: no valid credentials -> a clean 502 in S3 XML, no secret in the message
    time.sleep(6)
    abr_fake.fail = True
    with pytest.raises(ClientError) as ei:
        s3.list_buckets()
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 502
    assert "WarpCloudAuthFailure" in str(ei.value) and "source-secret" not in str(ei.value)
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    abr_api(abr_local, "DELETE", "/api/ab-routing/targets/fake")


def test_abr_web_identity_against_fake_sts(abr_local, abr_fake, abr_clean, tmp_path):
    f = tmp_path / "token.jwt"
    f.write_text("aaa.bbb.ccc\n")
    abr_api(abr_local, "PUT", "/api/ab-routing/targets/irsa", {"region": "us-east-1", "endpoints": {"s3": abr_fake.url},
            "auth": {"type": "web-identity", "roleArn": "arn:aws:iam::111111111111:role/irsa", "tokenFile": str(f),
                     "stsEndpoint": abr_fake.url}})
    abr_policy(abr_local, "s3", mode="cloud", target="irsa")
    abr_s3(abr_local.s3, "lk0", ABR_LOCAL_KEYS["lk0"]).list_buckets()
    assert abr_fake.sts_calls[0]["action"] == "AssumeRoleWithWebIdentity" and abr_fake.sts_calls[0]["token"] == "aaa.bbb.ccc"
    assert abr_fake.s3_keys[-1][0] == "ASIAirsa1"
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    abr_api(abr_local, "DELETE", "/api/ab-routing/targets/irsa")


def test_abr_default_chain_reads_the_process_environment(abr_pgs, abr_fake, abr_clean):
    """default-chain: a Warp started with AWS_ACCESS_KEY_ID/SECRET in its environment signs with those (AWS SDK v2 chain).
    Instance-role / IMDS / IRSA against real infrastructure is not verifiable here."""
    w = abr_start_warp(abr_pgs[0], ABR_LOCAL_KEYS, extra={"AWS_ACCESS_KEY_ID": "AKIAFROMENV", "AWS_SECRET_ACCESS_KEY": "envsecret",
                                                        "AWS_REGION": "us-east-1", "AWS_EC2_METADATA_DISABLED": "true"})
    try:
        abr_api(w, "PUT", "/api/ab-routing/targets/chain", {"region": "us-east-1", "endpoints": {"s3": abr_fake.url},
                "auth": {"type": "default-chain"}})
        abr_api(w, "PUT", "/api/ab-routing/policies/s3", {"mode": "cloud", "target": "chain"})
        abr_s3(w.s3, "lk0", ABR_LOCAL_KEYS["lk0"]).list_buckets()
        assert abr_fake.s3_keys[-1][0] == "AKIAFROMENV"
    finally:
        abr_api(w, "DELETE", "/api/ab-routing/policies/s3")
        w.close()


def test_abr_secrets_are_encrypted_and_never_leak(abr_pgs, abr_local, abr_fake, abr_clean):
    abr_api(abr_local, "PUT", "/api/ab-routing/targets/leaky", {"region": "us-east-1", "endpoints": {"s3": abr_fake.url},
            "auth": {"type": "static", "accessKeyId": "AKIALEAKY", "secretAccessKey": ABR_STATIC_SECRET, "sessionToken": "SESSION-TOKEN-ABC"}})
    abr_policy(abr_local, "s3", mode="cloud", target="leaky")
    abr_s3(abr_local.s3, "lk0", ABR_LOCAL_KEYS["lk0"]).list_buckets()
    assert abr_fake.s3_keys[-1] == ("AKIALEAKY", "SESSION-TOKEN-ABC")
    # every admin/metrics surface
    bodies = [json.dumps(abr_api(abr_local, "GET", p)) for p in
              ("/api/ab-routing", "/api/ab-routing/targets/leaky", "/api/ab-routing/stats", "/api/ab-routing/compare")]
    bodies.append(json.dumps(abr_api(abr_local, "POST", "/api/ab-routing/targets/leaky/test")))
    bodies.append(requests.get(f"http://localhost:{abr_local.metrics_port}/metrics", timeout=20).text)
    bodies.append(json.dumps(abr_api(abr_local, "GET", "/api/backend-sets")))
    # an update that omits the secret keeps it, still not visible
    bodies.append(json.dumps(abr_api(abr_local, "PUT", "/api/ab-routing/targets/leaky", {"region": "us-east-2", "endpoints": {"s3": abr_fake.url},
                  "auth": {"type": "static", "accessKeyId": "AKIALEAKY"}})))
    abr_s3(abr_local.s3, "lk0", ABR_LOCAL_KEYS["lk0"]).list_buckets()
    assert abr_fake.s3_keys[-1] == ("AKIALEAKY", "SESSION-TOKEN-ABC"), "omitted secret is kept"
    for b in bodies:
        assert ABR_STATIC_SECRET not in b and "SESSION-TOKEN-ABC" not in b
    assert "secretAccessKeySet" in bodies[0]
    # logs of the Warp process
    log = "".join(abr_local._output_lines)
    assert ABR_STATIC_SECRET not in log and "SESSION-TOKEN-ABC" not in log
    # at rest: encrypted with the repo's FieldCipher (encv1: prefix)
    with psycopg2.connect(host="localhost", port=abr_pgs[0].port, user="postgres", password="postgres", dbname="postgres") as cn:
        cur = cn.cursor()
        cur.execute("SELECT payload::text FROM warp_ab_routing ORDER BY version DESC LIMIT 1")
        stored = cur.fetchone()[0]
    assert ABR_STATIC_SECRET not in stored and "SESSION-TOKEN-ABC" not in stored and "encv1:" in stored
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    abr_api(abr_local, "DELETE", "/api/ab-routing/targets/leaky")


def test_abr_admin_api_requires_credentials(abr_local):
    r = requests.get(f"http://localhost:{abr_local.metrics_port}/api/ab-routing", timeout=20)
    assert r.status_code in (401, 403)
    r = requests.post(f"http://localhost:{abr_local.metrics_port}/api/ab-routing/kill-switch", json={"side": "cloud"}, timeout=20)
    assert r.status_code in (401, 403)


def test_abr_unauthenticated_clients_never_reach_the_cloud(abr_local, abr_cloud, abr_clients, abr_target_back, abr_clean):
    abr_policy(abr_local, "s3", mode="cloud")
    bad = abr_s3(abr_local.s3, "lk0", "wrong-secret")
    with pytest.raises(ClientError) as ei:
        bad.put_object(Bucket="abbkt", Key="intruder", Body=b"x")
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] in (401, 403)
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/s3")
    assert not abr_exists(abr_clients["c3"], "abbkt", "intruder")
    ddb = abr_ddb(abr_local.dynamo, "lk0", "wrong-secret")
    abr_policy(abr_local, "dynamodb", mode="cloud")
    with pytest.raises(ClientError):
        ddb.list_tables()
    abr_api(abr_local, "DELETE", "/api/ab-routing/policies/dynamodb")
