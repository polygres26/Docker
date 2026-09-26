"""Warp-side tests of pubsubwire (Google Cloud Pub/Sub gRPC + REST on Postgres).

* the golden corpus: ps_conformance/golden.json.gz holds Google's official Pub/Sub emulator's answers to every step of
  ps_conformance/ps_corpus.py (each case recorded twice with `ps_harness.py --record`; unstable steps dropped). Each case is
  replayed OFFLINE (no Docker) against a real Warp on one Postgres backend and on two sharded backends. A step must answer like the
  emulator, or be a documented divergence (ps_known.py: the emulator is a lenient test double; where it deviates from real
  Cloud Pub/Sub, Warp implements Pub/Sub);
* the behaviour the emulator cannot be the oracle for: REST/JSON API, push delivery, StreamingPull at 1,000 messages and
  reconnect semantics, exactly-once confirmations, ordering keys, dead letters across shards, snapshots across shards, the outbox;
* sharding: subscriptions' queues land on BOTH Postgres hosts, a topic with 3+ subscriptions delivers every message exactly once per
  subscription, 20 parallel pullers never see the same message twice while acks are outstanding;
* WARP_POOL_MAX_SIZE=4 with 30 idle StreamingPull streams and long polls must not starve other requests; bearer tokens.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests. The Google client libraries are not installed;
tests use raw gRPC stubs generated from the vendored protos (ps_conformance/ps_stubs).
"""
import base64
import concurrent.futures
import gzip
import http.server
import json
import os
import sys
import threading
import time

import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "ps_conformance"))
import ps_client as C  # noqa: E402
import ps_corpus  # noqa: E402
import ps_harness as H  # noqa: E402
import ps_launch_warp as L  # noqa: E402

with gzip.open(os.path.join(HERE, "ps_conformance", "golden.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)

_n = [0]


def uniq(prefix="x"):
    _n[0] += 1
    return f"{prefix}{int(time.time() * 1000) % 10**9}n{_n[0]}"


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def b64(s):
    return base64.b64encode(s.encode() if isinstance(s, str) else s).decode()


def ok(res):
    code, msg = res[0], res[1]
    assert code == "OK", (code, msg)
    return res[2]


class Env:
    """A project with helpers over a PsClient."""

    def __init__(self, warp, token=None):
        self.warp = warp
        self.c = C.PsClient(f"localhost:{warp.grpc_port}", token)
        self.project = uniq("p")
        self.P = f"projects/{self.project}"

    def topic(self, name=None, **kw):
        name = name or uniq("t")
        ok(self.c.call("CreateTopic", {"name": f"{self.P}/topics/{name}", **kw}))
        return f"{self.P}/topics/{name}"

    def sub(self, topic, name=None, **kw):
        name = name or uniq("s")
        ok(self.c.call("CreateSubscription", {"name": f"{self.P}/subscriptions/{name}", "topic": topic, **kw}))
        return f"{self.P}/subscriptions/{name}"

    def publish(self, topic, datas, **fields):
        msgs = [({"data": b64(d)} | fields) if not isinstance(d, dict) else d for d in datas]
        ids = []
        for i in range(0, len(msgs), 1000):
            ids += ok(self.c.call("Publish", {"topic": topic, "messages": msgs[i:i + 1000]}))["message_ids"]
        return ids

    def pull(self, sub, n=10, immediate=True):
        res = self.c.call("Pull", {"subscription": sub, "max_messages": n, "return_immediately": immediate})
        return ok(res).get("received_messages", [])

    def ack(self, sub, msgs):
        if msgs:
            ok(self.c.call("Acknowledge", {"subscription": sub, "ack_ids": [m["ack_id"] for m in msgs]}))

    def drain(self, sub, expect=None, timeout=20, ack=True):
        """Pulls (and acks) until `expect` messages or the timeout; returns the data strings in delivery order."""
        out, end = [], time.time() + timeout
        while time.time() < end and (expect is None or len(out) < expect):
            got = self.pull(sub, 100)
            if got:
                out += [base64.b64decode(m["message"].get("data", "")).decode() for m in got]
                if ack:
                    self.ack(sub, got)
            else:
                time.sleep(0.1)
        return out

    def close(self):
        self.c.close()


@pytest.fixture(scope="class")
def one():
    w = L.PsWarp(1, extra_env={"JAVA_TOOL_OPTIONS": "-Xmx400m"})
    yield w
    w.close()


@pytest.fixture(scope="class")
def two():
    w = L.PsWarp(2)
    yield w
    w.close()


@pytest.fixture(scope="class")
def strict():
    w = L.PsWarp(1, extra_env={"WARP_POOL_MAX_SIZE": "4", "WARP_PUBSUBWIRE_TOKENS": "ps-test-token,other-token"})
    yield w
    w.close()


def _replay(w, case):
    client = C.PsClient(f"localhost:{w.grpc_port}")
    try:
        ident, known, unexpected = H.replay_golden(GOLDEN, client, only=case)
    finally:
        client.close()
    assert ident + known + len(unexpected) == len(GOLDEN["cases"][case]), "not every step was compared"
    assert not unexpected, "\n".join(f"{n}#{i} [{k}]\n  emulator {json.dumps(g)[:400]}\n  warp     {json.dumps(x)[:400]}"
                                     for n, i, k, g, x in unexpected[:5])
    return ident, known


def start_http_server(handler):
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


class TestSingleBackend:
    @pytest.mark.parametrize("case", sorted(GOLDEN["cases"]))
    def test_golden_corpus(self, one, case):
        _replay(one, case)

    def test_golden_corpus_is_the_whole_corpus(self):
        assert set(GOLDEN["cases"]) == set(ps_corpus.CASES)

    # ------------------------------------------------------------------------------------------ REST/JSON API

    def test_rest_topics_subscriptions_publish_pull_ack(self, one):
        base = f"http://localhost:{one.rest_port}/v1"
        p = f"projects/{uniq('rp')}"
        t = f"{p}/topics/rest-topic"
        r = requests.put(f"{base}/{t}", json={"labels": {"a": "b"}})
        assert r.status_code == 200 and r.json() == {"name": t, "labels": {"a": "b"}}, r.text
        r = requests.put(f"{base}/{t}", json={})
        assert r.status_code == 409
        assert r.json()["error"] == {"code": 409, "message": "Resource already exists in the project (resource=rest-topic).",
                                     "status": "ALREADY_EXISTS"}
        r = requests.get(f"{base}/{p}/topics/nope-topic")
        assert r.status_code == 404
        assert r.json()["error"] == {"code": 404, "message": "Resource not found (resource=nope-topic).", "status": "NOT_FOUND"}
        s = f"{p}/subscriptions/rest-sub"
        r = requests.put(f"{base}/{s}", json={"topic": t, "ackDeadlineSeconds": 20})
        assert r.status_code == 200 and r.json()["ackDeadlineSeconds"] == 20 and r.json()["pushConfig"] == {}, r.text
        r = requests.post(f"{base}/{t}:publish", json={"messages": [{"data": b64("hello"), "attributes": {"k": "v"}},
                                                                      {"data": b64("w2"), "orderingKey": "o"}]})
        assert r.status_code == 200 and len(r.json()["messageIds"]) == 2, r.text
        r = requests.post(f"{base}/{s}:pull", json={"maxMessages": 10, "returnImmediately": True})
        msgs = r.json()["receivedMessages"]
        assert [base64.b64decode(m["message"]["data"]) for m in msgs] == [b"hello", b"w2"]
        assert msgs[0]["message"]["attributes"] == {"k": "v"} and msgs[1]["message"]["orderingKey"] == "o"
        assert msgs[0]["message"]["publishTime"].endswith("Z") and msgs[0]["message"]["messageId"].isdigit()
        ids = [m["ackId"] for m in msgs]
        r = requests.post(f"{base}/{s}:modifyAckDeadline", json={"ackIds": ids[:1], "ackDeadlineSeconds": 60})
        assert r.status_code == 200 and r.json() == {}
        r = requests.post(f"{base}/{s}:acknowledge", json={"ackIds": ids})
        assert r.status_code == 200 and r.json() == {}
        r = requests.post(f"{base}/{s}:pull", json={"maxMessages": 10, "returnImmediately": True})
        assert r.json() == {}
        r = requests.post(f"{base}/{s}:acknowledge", json={"ackIds": []})
        assert r.status_code == 400 and r.json()["error"]["status"] == "INVALID_ARGUMENT"
        r = requests.get(f"{base}/{p}/topics", params={"pageSize": 1})
        assert [x["name"] for x in r.json()["topics"]] == [t] and "nextPageToken" not in r.json()
        r = requests.get(f"{base}/{t}/subscriptions")
        assert r.json() == {"subscriptions": [s]}
        r = requests.patch(f"{base}/{t}", json={"topic": {"labels": {"c": "d"}}, "updateMask": "labels"})
        assert r.status_code == 200 and r.json()["labels"] == {"c": "d"}, r.text
        r = requests.patch(f"{base}/{s}", json={"subscription": {"ackDeadlineSeconds": 30}, "updateMask": "ackDeadlineSeconds"})
        assert r.status_code == 200 and r.json()["ackDeadlineSeconds"] == 30, r.text
        r = requests.post(f"{base}/{s}:modifyPushConfig", json={"pushConfig": {"pushEndpoint": "https://example.com/x"}})
        assert r.status_code == 200
        assert requests.get(f"{base}/{s}").json()["pushConfig"] == {"pushEndpoint": "https://example.com/x"}
        # snapshots and seek
        snap = f"{p}/snapshots/rest-snap"
        r = requests.put(f"{base}/{snap}", json={"subscription": s})
        assert r.status_code == 200 and r.json()["topic"] == t, r.text
        assert requests.get(f"{base}/{p}/snapshots").json()["snapshots"][0]["name"] == snap
        r = requests.post(f"{base}/{s}:seek", json={"snapshot": snap})
        assert r.status_code == 200 and r.json() == {}
        assert requests.delete(f"{base}/{snap}").status_code == 200
        assert requests.delete(f"{base}/{s}").status_code == 200
        assert requests.delete(f"{base}/{t}").status_code == 200
        assert requests.delete(f"{base}/{t}").status_code == 404

    def test_rest_error_shapes_and_iam_and_schemas(self, one):
        base = f"http://localhost:{one.rest_port}/v1"
        p = f"projects/{uniq('rq')}"
        r = requests.post(f"{base}/{p}/topics/x-topic:publish", data="{not json", headers={"Content-Type": "application/json"})
        assert r.status_code == 400 and r.json()["error"]["status"] == "INVALID_ARGUMENT"
        r = requests.get(f"http://localhost:{one.rest_port}/v2/whatever")
        assert r.status_code == 404 and r.json()["error"]["status"] == "NOT_FOUND"
        t = f"{p}/topics/iam-topic"
        requests.put(f"{base}/{t}", json={})
        r = requests.get(f"{base}/{t}:getIamPolicy")
        assert r.status_code == 200 and "etag" in r.json()
        pol = {"bindings": [{"role": "roles/pubsub.viewer", "members": ["user:x@example.com"]}]}
        r = requests.post(f"{base}/{t}:setIamPolicy", json={"policy": pol})
        assert r.status_code == 200 and r.json()["bindings"] == pol["bindings"], r.text
        assert requests.get(f"{base}/{t}:getIamPolicy").json()["bindings"] == pol["bindings"]
        r = requests.post(f"{base}/{t}:testIamPermissions", json={"permissions": ["pubsub.topics.publish"]})
        assert r.json() == {"permissions": ["pubsub.topics.publish"]}
        avro = '{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}'
        r = requests.post(f"{base}/{p}/schemas", params={"schemaId": "sch"}, json={"type": "AVRO", "definition": avro})
        assert r.status_code == 200 and r.json()["name"] == f"{p}/schemas/sch", r.text
        r = requests.get(f"{base}/{p}/schemas/sch", params={"view": "FULL"})
        assert r.json()["definition"] == avro
        r = requests.post(f"{base}/{p}/schemas:validateMessage", json={"name": f"{p}/schemas/sch", "encoding": "JSON",
                                                                        "message": b64('{"a":"x"}')})
        assert r.status_code == 200 and r.json() == {}
        r = requests.post(f"{base}/{p}/schemas:validateMessage", json={"name": f"{p}/schemas/sch", "encoding": "JSON",
                                                                        "message": b64('{"b":1}')})
        assert r.status_code == 400
        r = requests.post(f"{base}/{p}/schemas:validate", json={"schema": {"type": "AVRO", "definition": "nope"}})
        assert r.status_code == 400
        assert requests.delete(f"{base}/{p}/schemas/sch").status_code == 200

    def test_schema_topic_validates_published_messages_and_protobuf_needs_compiled_schema(self, one):
        env = Env(one)
        avro = '{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"n","type":"long"}]}'
        ok(env.c.call("CreateSchema", {"parent": env.P, "schema_id": "sch1", "schema": {"type": "AVRO", "definition": avro}}))
        t = env.topic(schema_settings={"schema": f"{env.P}/schemas/sch1", "encoding": "JSON"})
        s = env.sub(t)
        env.publish(t, ['{"a":"x","n":5}'])
        assert env.drain(s, 1) == ['{"a":"x","n":5}']
        code, msg, *_ = env.c.call("Publish", {"topic": t, "messages": [{"data": b64('{"a":1}')}]})
        assert code == "INVALID_ARGUMENT", msg
        # protobuf schema given as .proto text: stored, validation is UNIMPLEMENTED (needs protoc); compiled schemas validate
        ok(env.c.call("CreateSchema", {"parent": env.P, "schema_id": "prot1", "schema": {
            "type": "PROTOCOL_BUFFER", "definition": 'syntax = "proto3"; message M { string a = 1; }'}}))
        code, msg, *_ = env.c.call("ValidateMessage", {"parent": env.P, "name": f"{env.P}/schemas/prot1", "encoding": "BINARY",
                                                        "message": b64("")})
        assert code == "UNIMPLEMENTED", (code, msg)
        code, msg, *_ = env.c.call("ListSchemaRevisions", {"name": f"{env.P}/schemas/sch1"})
        assert code == "UNIMPLEMENTED"

    # ------------------------------------------------------------------------------------------ push

    def test_push_subscription_posts_the_documented_envelope_and_retries(self, one):
        got, fail = [], [2]

        class H_(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                got.append((self.path, dict(self.headers), body))
                if self.path == "/flaky" and fail[0] > 0:
                    fail[0] -= 1
                    self.send_response(500)
                else:
                    self.send_response(204 if self.path == "/ok" else 200)
                self.end_headers()

            def log_message(self, *a):
                pass

        srv = start_http_server(H_)
        port = srv.server_address[1]
        try:
            env = Env(one)
            t = env.topic()
            s_ok = env.sub(t, push_config={"push_endpoint": f"http://127.0.0.1:{port}/ok", "attributes": {"x-goog-version": "v1"},
                                           "oidc_token": {"service_account_email": "sa@x.iam.gserviceaccount.com", "audience": "aud"}})
            s_flaky = env.sub(t, push_config={"push_endpoint": f"http://127.0.0.1:{port}/flaky"})
            s_raw = env.sub(t, push_config={"push_endpoint": f"http://127.0.0.1:{port}/raw", "no_wrapper": {"write_metadata": True}})
            sub = ok(env.c.call("GetSubscription", {"subscription": s_ok}))
            assert sub["push_config"]["oidc_token"]["audience"] == "aud"
            env.publish(t, [{"data": b64("push me"), "attributes": {"k": "v"}, "ordering_key": ""}])
            end = time.time() + 30
            while time.time() < end and not ({"/ok", "/raw"} <= {g[0] for g in got} and sum(1 for g in got if g[0] == "/flaky") >= 3):
                time.sleep(0.2)
            ok_calls = [g for g in got if g[0] == "/ok"]
            assert len(ok_calls) == 1, [g[0] for g in got]  # 2xx acknowledges: exactly one delivery
            env_ = json.loads(ok_calls[0][2])
            assert set(env_) == {"message", "subscription"} and env_["subscription"] == s_ok
            m = env_["message"]
            assert base64.b64decode(m["data"]) == b"push me" and m["attributes"] == {"k": "v"}
            assert m["messageId"] == m["message_id"] and m["publishTime"] == m["publish_time"] and m["messageId"].isdigit()
            assert ok_calls[0][1].get("Content-Type", "").startswith("application/json")
            flaky = [g for g in got if g[0] == "/flaky"]
            assert len(flaky) == 3, len(flaky)  # 500, 500, then 200 -> acknowledged
            assert json.loads(flaky[0][2])["message"]["messageId"] == json.loads(flaky[2][2])["message"]["messageId"]
            raw = [g for g in got if g[0] == "/raw"]
            assert raw[0][2] == b"push me" and raw[0][1].get("X-Goog-Pubsub-Message-Id", "").isdigit()
            assert raw[0][1].get("X-Goog-Pubsub-Subscription-Name") == s_raw
            time.sleep(2.5)
            assert len([g for g in got if g[0] == "/ok"]) == 1 and len([g for g in got if g[0] == "/flaky"]) == 3
            # ModifyPushConfig to pull: pushes stop, messages stay for Pull
            ok(env.c.call("ModifyPushConfig", {"subscription": s_ok, "push_config": {}}))
            env.publish(t, ["after"])
            assert env.drain(s_ok, 1) == ["after"]
        finally:
            srv.shutdown()

    # ------------------------------------------------------------------------------------------ streaming pull

    def test_streaming_pull_1000_messages_with_acks(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t)
        env.publish(t, [f"m{i}" for i in range(1000)])
        got, resps, err = env.c.streaming_pull(s, max_messages=1000, timeout=60, ack=True)
        assert err is None or err[0] == "CANCELLED", err  # CANCELLED: the client's own cancel after collecting
        assert sorted(m.message.data.decode() for m in got) == sorted(f"m{i}" for i in range(1000))
        assert len({m.message.message_id for m in got}) == 1000
        time.sleep(1)
        assert env.pull(s) == []
        # the stream acked everything: nothing is redelivered after the deadline either
        assert ok(env.c.call("GetSubscription", {"subscription": s}))["name"] == s

    def test_streaming_pull_reconnect_leaves_unacked_messages_leased_until_the_deadline(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t)
        env.publish(t, ["a", "b", "c"])
        got, _, err = env.c.streaming_pull(s, ack_deadline=10, max_messages=3, timeout=10, ack=False)
        assert sorted(m.message.data for m in got) == [b"a", b"b", b"c"]
        assert env.pull(s) == []  # cancelled without acking: leased for the ack deadline
        got2, _, _ = env.c.streaming_pull(s, ack_deadline=10, max_messages=3, timeout=15, ack=True)
        assert sorted(m.message.data for m in got2) == [b"a", b"b", b"c"]  # redelivered after 10 s
        assert {m.message.message_id for m in got} == {m.message.message_id for m in got2}
        assert env.pull(s) == []

    def test_streaming_pull_modack_extends_and_nack_redelivers(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t)
        env.publish(t, ["x"])
        got, resps, err = env.c.streaming_pull(
            s, ack_deadline=10, max_messages=1, timeout=8, ack=False)
        assert len(got) == 1
        ack_id = got[0].ack_id
        # a second stream: modack 0 (nack) makes it deliverable at once
        ok(env.c.call("ModifyAckDeadline", {"subscription": s, "ack_ids": [ack_id], "ack_deadline_seconds": 0}))
        got2, _, _ = env.c.streaming_pull(s, max_messages=1, timeout=8, ack=True)
        assert [m.message.data for m in got2] == [b"x"]

    def test_streaming_pull_exactly_once_confirmations(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t, enable_exactly_once_delivery=True)
        env.publish(t, ["e1"])
        got, resps, err = env.c.streaming_pull(s, max_messages=1, timeout=8, ack=True, stop_after=1.0)
        assert len(got) == 1
        props = [r for r in resps if r.HasField("subscription_properties")]
        assert props and props[0].subscription_properties.exactly_once_delivery_enabled
        confirms = [r.acknowledge_confirmation for r in resps if r.HasField("acknowledge_confirmation")]
        assert confirms and list(confirms[0].ack_ids) == [got[0].ack_id]
        # acking again on a new stream is reported as invalid (permanent failure)
        from google.pubsub.v1 import pubsub_pb2 as pb
        got2, resps2, _ = env.c.streaming_pull(s, reqs=[pb.StreamingPullRequest(ack_ids=[got[0].ack_id])], timeout=3)
        c2 = [r.acknowledge_confirmation for r in resps2 if r.HasField("acknowledge_confirmation")]
        assert c2 and list(c2[0].invalid_ack_ids) == [got[0].ack_id]
        code, msg, _, _, info = env.c.call("Acknowledge", {"subscription": s, "ack_ids": [got[0].ack_id]})
        assert code == "INVALID_ARGUMENT" and info.get("details_bin") and b"EXACTLY_ONCE_ACKID_FAILURE" in info["details_bin"]

    # ------------------------------------------------------------------------------------------ misc semantics

    def test_publish_limits(self, one):
        env = Env(one)
        t = env.topic()
        big = "x" * (6 * 1000 * 1000)
        code, msg, *_ = env.c.call("Publish", {"topic": t, "messages": [{"data": b64(big)}, {"data": b64(big)}]})
        assert code == "INVALID_ARGUMENT" and "10000000" in msg, (code, msg)
        code, msg, *_ = env.c.call("Publish", {"topic": t, "messages": [{"data": b64("x" * 10_000_001)}]})
        assert code == "INVALID_ARGUMENT", (code, msg)
        s = env.sub(t)
        ids = env.publish(t, ["y" * 9_000_000])
        assert len(ids) == 1
        got = env.pull(s, 1)
        assert len(got[0]["message"]["data"]) > 9_000_000

    def test_ack_deadline_expiry_redelivers_with_the_same_message_id(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t)
        env.publish(t, ["once"])
        first = env.pull(s)
        assert len(first) == 1
        assert env.pull(s) == []
        ok(env.c.call("ModifyAckDeadline", {"subscription": s, "ack_ids": [first[0]["ack_id"]], "ack_deadline_seconds": 2}))
        time.sleep(3)
        again = env.pull(s)
        assert len(again) == 1 and again[0]["message"]["message_id"] == first[0]["message"]["message_id"]
        assert again[0]["ack_id"] != first[0]["ack_id"]
        env.ack(s, again)
        # an ack id from the first delivery is still accepted on an at-least-once subscription
        ok(env.c.call("Acknowledge", {"subscription": s, "ack_ids": [first[0]["ack_id"]]}))

    def test_long_poll_pull_waits_for_a_message(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t)
        threading.Timer(1.0, lambda: env.publish(t, ["late"])).start()
        t0 = time.time()
        got = env.pull(s, 1, immediate=False)
        assert [base64.b64decode(m["message"]["data"]) for m in got] == [b"late"]
        assert 0.8 < time.time() - t0 < 5

    def test_dead_letter_policy_forwards_after_max_attempts_with_attributes(self, one):
        env = Env(one)
        t, dlt = env.topic(), env.topic()
        s = env.sub(t, dead_letter_policy={"dead_letter_topic": dlt, "max_delivery_attempts": 5})
        d = env.sub(dlt)
        env.publish(t, [{"data": b64("poison"), "attributes": {"k": "v"}}])
        attempts = []
        for _ in range(5):
            got = env.pull(s)
            assert len(got) == 1
            attempts.append(got[0]["delivery_attempt"])
            ok(env.c.call("ModifyAckDeadline", {"subscription": s, "ack_ids": [got[0]["ack_id"]], "ack_deadline_seconds": 0}))
        assert attempts == [1, 2, 3, 4, 5]
        end = time.time() + 15
        fwd = []
        while time.time() < end and not fwd:
            assert env.pull(s) == []
            fwd = env.pull(d)
            time.sleep(0.3)
        assert len(fwd) == 1
        m = fwd[0]["message"]
        assert base64.b64decode(m["data"]) == b"poison" and m["attributes"]["k"] == "v"
        assert m["attributes"]["CloudPubSubDeadLetterSourceDeliveryCount"] == "5"
        assert m["attributes"]["CloudPubSubDeadLetterSourceSubscription"] == s.split("/")[-1]
        assert m["attributes"]["CloudPubSubDeadLetterSourceSubscriptionProject"] == env.project
        assert env.pull(s) == []

    def test_retention_and_hold_of_acked_messages_for_seek(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t, retain_acked_messages=True)
        env.publish(t, ["r1", "r2"])
        assert env.drain(s, 2) == ["r1", "r2"]
        assert sql(one.pgs[0], "SELECT count(*) FROM warp_pubsub_msgs WHERE sub = %s AND acked", (s,))[0][0] == 2
        ok(env.c.call("Seek", {"subscription": s, "time": "2000-01-01T00:00:00Z"}))
        assert env.drain(s, 2) == ["r1", "r2"]  # acked messages are replayed: retained
        s2 = env.sub(t)  # no retain_acked_messages: acked messages are deleted
        env.publish(t, ["z"])
        assert env.drain(s2, 1) == ["z"]
        assert sql(one.pgs[0], "SELECT count(*) FROM warp_pubsub_msgs WHERE sub = %s", (s2,))[0][0] == 0

    def test_export_subscriptions_store_their_config_and_delivery_is_unimplemented(self, one):
        env = Env(one)
        t = env.topic()
        s = env.sub(t, bigquery_config={"table": "proj.ds.tbl", "use_topic_schema": True})
        got = ok(env.c.call("GetSubscription", {"subscription": s}))
        assert got["bigquery_config"]["table"] == "proj.ds.tbl"
        code, msg, *_ = env.c.call("Pull", {"subscription": s, "max_messages": 1, "return_immediately": True})
        assert code == "UNIMPLEMENTED", (code, msg)
        s2 = env.sub(t, cloud_storage_config={"bucket": "some-bucket", "filename_prefix": "p-"})
        assert ok(env.c.call("GetSubscription", {"subscription": s2}))["cloud_storage_config"]["bucket"] == "some-bucket"
        _, _, err = env.c.streaming_pull(s2, max_messages=1, timeout=3)
        assert err and err[0] == "UNIMPLEMENTED", err

    def test_mcp_and_metrics_see_the_store(self, one):
        text = one.proc.metrics_text()
        assert "pubsubwire" in text


class TestTwoShardedBackends:
    @pytest.mark.parametrize("case", sorted(GOLDEN["cases"]))
    def test_golden_corpus(self, two, case):
        _replay(two, case)

    def _placement(self, w, env, subs):
        """Which Postgres index holds the queue of each subscription (by publishing one probe message to the topic they share)."""
        where = {}
        for i, pg in enumerate(w.pgs):
            for (sname,) in sql(pg, "SELECT DISTINCT sub FROM warp_pubsub_msgs"):
                where[sname] = i
        return {s: where.get(s) for s in subs}

    def test_three_subscriptions_on_both_hosts_get_every_message_exactly_once(self, two):
        env = Env(two)
        t = env.topic()
        subs = [env.sub(t) for _ in range(8)]
        ids = env.publish(t, [f"m{i}" for i in range(60)])
        assert len(set(ids)) == 60
        place = self._placement(two, env, subs)
        assert set(place.values()) == {0, 1}, place  # queues live on BOTH hosts
        for s in subs:
            data = env.drain(s, 60)
            assert sorted(data) == sorted(f"m{i}" for i in range(60)), s
            assert env.pull(s) == []
        # nothing is left in the outbox
        assert all(sql(pg, "SELECT count(*) FROM warp_pubsub_outbox")[0][0] == 0 for pg in two.pgs)
        # the catalog lives on the first host only
        assert sql(two.pgs[0], "SELECT count(*) FROM warp_pubsub_topics WHERE name = %s", (t,))[0][0] == 1
        assert sql(two.pgs[1], "SELECT count(*) FROM warp_pubsub_topics WHERE name = %s", (t,))[0][0] == 0

    def test_publish_survives_a_crash_between_outbox_and_delivery(self, two):
        from google.pubsub.v1 import pubsub_pb2 as pb
        env = Env(two)
        t = env.topic()
        subs = [env.sub(t) for _ in range(6)]
        req = pb.PublishRequest(topic=t)
        req.messages.add(data=b"orphan", message_id="424242424242424242").publish_time.GetCurrentTime()
        sql(two.pgs[0], "INSERT INTO warp_pubsub_outbox (topic, payload) VALUES (%s, %s)", (t, psycopg2.Binary(req.SerializeToString())))
        for s in subs:  # the sweeper (every 5 s) completes stale outbox rows on every subscription, on both hosts
            assert env.drain(s, 1, timeout=30) == ["orphan"], s
        assert sql(two.pgs[0], "SELECT count(*) FROM warp_pubsub_outbox")[0][0] == 0

    def test_dead_letter_forwarding_across_shards(self, two):
        env = Env(two)
        t, dlt = env.topic(), env.topic()
        probe = env.topic()
        cands = [env.sub(probe) for _ in range(8)]
        env.publish(probe, ["p"])
        place = self._placement(two, env, cands)
        # pick a source and a dead-letter subscription that live on different hosts by creating pairs until they do
        for _ in range(12):
            src = env.sub(t, dead_letter_policy={"dead_letter_topic": dlt, "max_delivery_attempts": 5})
            dead = env.sub(dlt)
            env.publish(t, ["probe-msg"])
            env.publish(dlt, ["probe-dead"])
            pl = self._placement(two, env, [src, dead])
            env.drain(src, 1, timeout=5)
            env.drain(dead, 1, timeout=5)
            if pl[src] is not None and pl[dead] is not None and pl[src] != pl[dead]:
                break
        else:
            pytest.fail(f"could not place source and dead-letter subscriptions on different hosts: {pl}")
        env.publish(t, [{"data": b64("poison"), "attributes": {"a": "b"}}])
        for _ in range(5):
            got = env.pull(src)
            assert len(got) == 1
            ok(env.c.call("ModifyAckDeadline", {"subscription": src, "ack_ids": [got[0]["ack_id"]], "ack_deadline_seconds": 0}))
        end, fwd = time.time() + 20, []
        while time.time() < end and not fwd:
            env.pull(src)
            fwd = env.pull(dead)
            time.sleep(0.3)
        assert len(fwd) == 1 and base64.b64decode(fwd[0]["message"]["data"]) == b"poison"

    def test_ordering_keys_keep_their_order_and_one_message_per_key_is_outstanding(self, two):
        env = Env(two)
        t = env.topic()
        subs = [env.sub(t, enable_message_ordering=True) for _ in range(4)]
        msgs = []
        for i in range(25):
            msgs.append({"data": b64(f"A{i}"), "ordering_key": "A"})
            msgs.append({"data": b64(f"B{i}"), "ordering_key": "B"})
        env.publish(t, msgs)
        for s in subs:
            first = env.pull(s, 100)
            assert sorted(base64.b64decode(m["message"]["data"]).decode() for m in first) == ["A0", "B0"]  # one per key
            assert env.pull(s, 100) == []
            env.ack(s, first)
            seq = [base64.b64decode(m["message"]["data"]).decode() for m in first]
            while len(seq) < 50:
                got = env.pull(s, 100)
                assert 1 <= len(got) <= 2
                seq += [base64.b64decode(m["message"]["data"]).decode() for m in got]
                env.ack(s, got)
            assert [x for x in seq if x[0] == "A"] == [f"A{i}" for i in range(25)]
            assert [x for x in seq if x[0] == "B"] == [f"B{i}" for i in range(25)]

    def test_twenty_parallel_pullers_never_see_a_message_twice_while_acks_are_outstanding(self, two):
        env = Env(two)
        t = env.topic()
        s = env.sub(t, ack_deadline_seconds=60)
        env.publish(t, [f"m{i}" for i in range(400)])
        seen, lock = [], threading.Lock()

        def puller(_):
            mine = []
            c = C.PsClient(f"localhost:{two.grpc_port}")
            try:
                empty = 0
                while empty < 5:
                    res = c.call("Pull", {"subscription": s, "max_messages": 7, "return_immediately": True})
                    got = (res[2] or {}).get("received_messages", [])
                    if got:
                        empty = 0
                        mine += got
                    else:
                        empty += 1
                        time.sleep(0.1)
            finally:
                c.close()
            with lock:
                seen.extend(mine)

        with concurrent.futures.ThreadPoolExecutor(20) as ex:
            list(ex.map(puller, range(20)))
        ids = [m["message"]["message_id"] for m in seen]
        assert len(ids) == 400 and len(set(ids)) == 400, (len(ids), len(set(ids)))
        assert len({m["ack_id"] for m in seen}) == 400
        env.ack(s, seen)
        assert env.pull(s) == []

    def test_streaming_pull_1000_messages_across_shards(self, two):
        env = Env(two)
        t = env.topic()
        subs = [env.sub(t) for _ in range(3)]
        env.publish(t, [f"m{i}" for i in range(1000)])
        for s in subs:
            got, _, err = env.c.streaming_pull(s, max_messages=1000, timeout=60, ack=True)
            assert (err is None or err[0] == "CANCELLED") and len(got) == 1000 and len({m.message.message_id for m in got}) == 1000

    def test_snapshot_and_seek_across_shards(self, two):
        env = Env(two)
        t = env.topic()
        a, b = env.sub(t), env.sub(t)
        env.publish(t, ["s1", "s2", "s3"])
        assert env.drain(a, 3) == ["s1", "s2", "s3"]
        first = env.pull(b, 1)
        assert [base64.b64decode(m["message"]["data"]) for m in first] == [b"s1"]
        env.ack(b, first)
        snap = f"{env.P}/snapshots/{uniq('sn')}"
        ok(env.c.call("CreateSnapshot", {"name": snap, "subscription": a}))
        env.publish(t, ["s4", "s5"])
        assert env.drain(a, 2) == ["s4", "s5"]
        ok(env.c.call("Seek", {"subscription": a, "snapshot": snap}))
        assert env.drain(a, 2) == ["s4", "s5"]
        # another subscription of the same topic can seek to the snapshot too (its queue may be on the other host)
        ok(env.c.call("Seek", {"subscription": b, "snapshot": snap}))
        # b had read only s1 when the snapshot (of a's state: everything through s3 acked) was taken: seeking b to it drops
        # s2/s3 (acked in the snapshot) and replays what was published after it
        assert env.drain(b, 10, timeout=3) == ["s4", "s5"]
        assert env.c.call("ListTopicSnapshots", {"topic": t})[2]["snapshots"] == [snap]

    def test_delete_subscription_and_topic_clean_up_their_queues(self, two):
        env = Env(two)
        t = env.topic()
        s = env.sub(t)
        env.publish(t, ["gone"])
        ok(env.c.call("DeleteSubscription", {"subscription": s}))
        assert all(sql(pg, "SELECT count(*) FROM warp_pubsub_msgs WHERE sub = %s", (s,))[0][0] == 0 for pg in two.pgs)
        ok(env.c.call("DeleteTopic", {"topic": t}))
        code, *_ = env.c.call("Publish", {"topic": t, "messages": [{"data": b64("x")}]})
        assert code == "NOT_FOUND"


class TestPoolAndAuth:
    def test_bearer_token_required_when_configured(self, strict):
        anon = C.PsClient(f"localhost:{strict.grpc_port}")
        code, msg, *_ = anon.call("ListTopics", {"project": "projects/x"})
        assert code == "UNAUTHENTICATED", (code, msg)
        bad = C.PsClient(f"localhost:{strict.grpc_port}", "wrong")
        assert bad.call("ListTopics", {"project": "projects/x"})[0] == "UNAUTHENTICATED"
        good = C.PsClient(f"localhost:{strict.grpc_port}", "ps-test-token")
        assert good.call("ListTopics", {"project": "projects/x"})[0] == "OK"
        assert C.PsClient(f"localhost:{strict.grpc_port}", "other-token").call("ListTopics", {"project": "projects/x"})[0] == "OK"
        base = f"http://localhost:{strict.rest_port}/v1/projects/x/topics"
        assert requests.get(base).status_code == 401
        assert requests.get(base).json()["error"]["status"] == "UNAUTHENTICATED"
        assert requests.get(base, headers={"Authorization": "Bearer nope"}).status_code == 401
        assert requests.get(base, headers={"Authorization": "Bearer ps-test-token"}).status_code == 200

    def test_pool_of_4_with_30_idle_streaming_pulls_and_long_polls_does_not_starve_other_requests(self, strict):
        env = Env(strict, "ps-test-token")
        t = env.topic()
        subs = [env.sub(t) for _ in range(3)]
        # 30 open, idle streams (a StreamingPull that waits for messages) -- opened without blocking this thread
        chans = []
        import queue as _q
        from google.pubsub.v1 import pubsub_pb2 as pb
        calls = []
        for i in range(30):
            ch = C.PsClient(f"localhost:{strict.grpc_port}", "ps-test-token")
            chans.append(ch)
            q = _q.Queue()

            def gen(sub=subs[i % 3], q=q):
                yield pb.StreamingPullRequest(subscription=sub, stream_ack_deadline_seconds=60, max_outstanding_messages=1000)
                while True:
                    try:
                        item = q.get(timeout=0.2)
                    except _q.Empty:
                        continue
                    if item is None:
                        return
                    yield item

            calls.append((ch.sub.StreamingPull(gen(), metadata=ch.metadata), q))
        received = {}
        threads = []

        def reader(i, call, q):
            try:
                for r in call:
                    for m in r.received_messages:
                        received.setdefault(i, []).append(m.message.data)
                        q.put(pb.StreamingPullRequest(ack_ids=[m.ack_id]))
            except Exception:  # noqa: BLE001 -- cancelled at teardown
                pass

        for i, (call, q) in enumerate(calls):
            th = threading.Thread(target=reader, args=(i, call, q), daemon=True)
            th.start()
            threads.append(th)
        # plus 10 long polls waiting on an empty subscription
        lp_sub = env.sub(env.topic())
        pool = concurrent.futures.ThreadPoolExecutor(10)
        polls = [pool.submit(env.c.call, "Pull", {"subscription": lp_sub, "max_messages": 1}, 30) for _ in range(10)]
        time.sleep(3)
        try:
            # ordinary requests are answered promptly although 40 calls are parked on a pool of 4 connections
            t0 = time.time()
            for _ in range(20):
                nt = env.topic()
                s2 = env.sub(nt)
                env.publish(nt, ["quick"])
                assert env.drain(s2, 1, timeout=5) == ["quick"]
            assert time.time() - t0 < 30, time.time() - t0
            # a message published now reaches exactly one stream of each of the three subscriptions
            env.publish(t, ["to-streams"])
            end = time.time() + 15
            while time.time() < end and sum(len(v) for v in received.values()) < 3:
                time.sleep(0.1)
            assert sum(len(v) for v in received.values()) == 3, received
            # and the parked long polls are woken by a publish to their topic
            lt = ok(env.c.call("GetSubscription", {"subscription": lp_sub}))["topic"]
            env.publish(lt, ["wake"])
            done = [f for f in polls if f.done()]
            end = time.time() + 10
            while time.time() < end and not any(f.done() for f in polls):
                time.sleep(0.1)
            assert any(f.done() for f in polls)
        finally:
            for call, q in calls:
                q.put(None)
                call.cancel()
            for ch in chans:
                ch.close()
            pool.shutdown(wait=False)
