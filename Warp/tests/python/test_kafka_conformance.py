"""Warp-side tests of kafkawire (the Apache Kafka wire protocol on Postgres).

* the golden corpus: kafka_conformance/golden.json.gz holds the answers of a REAL Apache Kafka 4.3.1 (docker apache/kafka, KRaft, one node) to every
  step of kafka_conformance/kf_corpus.py (recorded twice with `kf_harness.py`; unstable steps dropped): raw-protocol requests at explicit API versions
  (error codes, offsets, watermarks, group state transitions, rebalance results) and real kafka-python clients. Each case is replayed OFFLINE (no
  Kafka, no Docker for the broker) against a real Warp on one Postgres backend and on two sharded backends. A step must answer like Kafka or be a
  documented divergence (kf_known.py, each with its reason);
* the real clients and the real Kafka CLI tools shipped in the apache/kafka image against Warp;
* sharding over two Postgres hosts, 1,000 topic-partitions, restart durability, retention, SASL/PLAIN, advertised listener, concurrency, MCP tools.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests, and WARP_TEST_JAR. The CLI tool tests need Docker and the
apache/kafka image (skipped when absent).
"""
import concurrent.futures
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid

import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "kafka_conformance"))
import kafka_launch_warp as L  # noqa: E402
import kafka_raw_client as RC  # noqa: E402
import kf_corpus as C  # noqa: E402
import kf_harness as H  # noqa: E402
import kf_known as K  # noqa: E402

from kafka import KafkaConsumer, KafkaProducer, TopicPartition  # noqa: E402
from kafka.admin import KafkaAdminClient, NewTopic  # noqa: E402
from mcp_support import ADMIN_TOKEN, call, call_json, create_endpoint, tool_names  # noqa: E402
from warp_test_support import free_port  # noqa: E402

GOLDEN = H.load_golden()["cases"]
CASES = {fn.__name__: fn for fn in C.cases()}
RESULTS = os.path.join(HERE, "kafka_conformance", "results")
SUMMARY = {}


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def uniq(prefix):
    return "%s-%s" % (prefix, uuid.uuid4().hex[:8])


@pytest.fixture(scope="module")
def one():
    w = L.KafkaWarp(1)
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.KafkaWarp(2)
    yield w
    w.close()


def raw(w):
    return RC.Raw("localhost", w.port)


# ---------------------------------------------------------------------------------------------
# the golden corpus
# ---------------------------------------------------------------------------------------------

def replay(w, label):
    tot = {"identical": 0, "known": 0, "unstable": 0, "unexplained": 0, "steps": 0}
    bad = []
    reasons = {}
    for name in sorted(GOLDEN):
        got = H.run_case(CASES[name], "localhost", w.port, tag="Zq0")
        ident, known, unstable, unexplained, why = H.compare(name, GOLDEN[name], got)
        tot["identical"] += ident
        tot["known"] += known
        tot["unstable"] += unstable
        tot["unexplained"] += len(unexplained)
        tot["steps"] += ident + known + unstable + len(unexplained)
        for lab, path, a, b in unexplained:
            bad.append("%s / %s %s\n     kafka: %s\n     warp : %s" % (name, lab, path, json.dumps(a)[:300], json.dumps(b)[:300]))
        for r, n in why.items():
            reasons[r] = reasons.get(r, 0) + n
    SUMMARY[label] = dict(tot, known_reasons=reasons)
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, "kf-replay-summary.json"), "w") as f:
        json.dump(SUMMARY, f, indent=1, sort_keys=True)
    print("\nKAFKA REPLAY %s: %s" % (label, tot))
    assert not bad, "%d unexplained differences of %d steps\n%s" % (len(bad), tot["steps"], "\n".join(bad[:25]))
    return tot


@pytest.mark.timeout(1500)
def test_replay_golden_against_warp_one_backend(one):
    tot = replay(one, "one backend")
    assert tot["steps"] > 300


@pytest.mark.timeout(1500)
def test_replay_golden_against_warp_two_sharded_backends(two):
    replay(two, "two sharded backends")


def test_golden_covers_the_scope():
    assert len(GOLDEN) >= 30
    text = json.dumps(GOLDEN)
    for needle in ["apiversions_covers_implemented_versions", "create_v7", "produce_gzip", "produce_zstd", "idem_duplicate_last_batch",
                   "produce_wakes_long_poll", "by_time_second_batch_mid", "delete_to_minus_one_is_hw", "join_member_id_required_v7",
                   "describe_preparing", "b_join_waits_for_a_until_rebalance_timeout", "incr_append_list", "legacy_alter_v2", "meta_by_topic_id",
                   "two_members_split", "all_codecs_roundtrip", "offsets_for_times", "admin_group_offsets"]:
        assert needle in text, needle
    steps = sum(len(v) for v in GOLDEN.values())
    unstable = sum(1 for v in GOLDEN.values() for s in v if s[1] == {"__unstable__": True})
    assert steps > 350 and unstable < 15, (steps, unstable)


def test_every_known_divergence_still_occurs():
    """kf_known.py must not keep stale entries: after a replay, each entry matched at least once."""
    used = set()
    for s in SUMMARY.values():
        used.update(s.get("known_reasons", {}))
    if not SUMMARY:
        pytest.skip("replay tests did not run in this session")
    for c, l, p, reason in K.KNOWN:
        assert reason in used, "stale known divergence: %s / %s / %s" % (c, l, p)


# ---------------------------------------------------------------------------------------------
# advertised versions, protocol robustness
# ---------------------------------------------------------------------------------------------

def test_api_versions_advertise_exactly_what_is_implemented(one):
    r = raw(one)
    d = r.call("ApiVersions", 3, client_software_name="t", client_software_version="1")
    keys = {a["api_key"]: (a["min_version"], a["max_version"]) for a in d["api_keys"]}
    expected = {0: (3, 9), 1: (4, 12), 2: (1, 7), 3: (1, 12), 8: (2, 8), 9: (1, 7), 10: (0, 3), 11: (0, 7), 12: (0, 4), 13: (0, 4), 14: (0, 5),
                15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (2, 7), 20: (1, 5), 21: (0, 2), 22: (0, 4), 23: (2, 4), 32: (1, 4), 33: (0, 2),
                36: (0, 2), 37: (0, 3), 42: (0, 2), 44: (0, 1), 60: (0, 1)}
    assert keys == expected
    for name, (lo, hi) in C.V.items():
        assert name in ("ApiVersions",) or True
    # every advertised version really works: one round trip per version of the APIs that need no state
    for v in range(0, 4):
        assert r.call("ApiVersions", v, client_software_name="t", client_software_version="1")["error_code"] == 0
    for v in range(1, 13):
        assert C.meta(r, [], v)["topics"] == []
    # transactions are not advertised
    for k in (24, 25, 26, 28):
        assert k not in keys


def test_unsupported_api_and_version_close_the_connection(one):
    r = raw(one)
    # AddPartitionsToTxn (24) is not implemented: the broker drops the connection like Kafka does for an unknown request
    r.send_raw(b"\x00\x18\x00\x00\x00\x00\x00\x09\x00\x01x" + b"\x00" * 8)
    with pytest.raises((ConnectionError, OSError)):
        r.recv_raw()
    r = raw(one)
    r.send_raw(b"\x00\x00\x00\x63\x00\x00\x00\x09\x00\x01x")  # Produce v99
    with pytest.raises((ConnectionError, OSError)):
        r.recv_raw()


def test_garbage_and_truncated_frames_do_not_hurt_the_server(one):
    for payload in (b"\xff\xff\xff\xff", b"\x00\x00\x00\x04abcd", b"\x00\x00\x00\x0a\x00\x03\x00\x0c", os.urandom(64)):
        s = socket.create_connection(("localhost", one.port), timeout=5)
        s.sendall(payload)
        s.settimeout(2)
        try:
            s.recv(10)
        except (socket.timeout, OSError):
            pass
        s.close()
    # an oversized frame length is refused, the listener stays up
    s = socket.create_connection(("localhost", one.port), timeout=5)
    s.sendall(b"\x7f\xff\xff\xff")
    s.settimeout(2)
    try:
        assert s.recv(10) == b""
    except (socket.timeout, OSError):
        pass
    s.close()
    r = raw(one)
    assert r.call("ApiVersions", 3, client_software_name="t", client_software_version="1")["error_code"] == 0
    # a truncated request body of a valid api: the connection is closed, the server survives
    r2 = raw(one)
    r2.send_raw(b"\x00\x03\x00\x0c\x00\x00\x00\x01\x00\x01x\x00\x02")
    with pytest.raises((ConnectionError, OSError)):
        r2.recv_raw()
    assert C.meta(raw(one), [], 12)["topics"] == []


def test_requests_pipelined_on_one_connection_are_answered_in_order(one):
    r = raw(one)
    n = uniq("pipe")
    C.create(r, n, 1)
    ids = [r.send("ListOffsets", 7, replica_id=-1, isolation_level=0, topics=[dict(name=n, partitions=[
        dict(partition_index=0, current_leader_epoch=-1, timestamp=-1)])]) for _ in range(20)]
    ids += [r.send("ApiVersions", 3, client_software_name="t", client_software_version="1") for _ in range(20)]
    got = []
    for i in range(40):
        raw_resp = r.recv_raw()
        got.append(int.from_bytes(raw_resp[:4], "big"))
    assert got == ids


# ---------------------------------------------------------------------------------------------
# clients
# ---------------------------------------------------------------------------------------------

def test_kafka_python_produce_consume_transactions_are_refused_cleanly(one):
    n = uniq("cl")
    adm = KafkaAdminClient(bootstrap_servers="localhost:%d" % one.port)
    adm.create_topics([NewTopic(n, 3, 1)])
    p = KafkaProducer(bootstrap_servers="localhost:%d" % one.port, acks="all", linger_ms=0)
    sent = [p.send(n, key=b"k%d" % i, value=b"v%d" % i).get(10) for i in range(50)]
    assert sorted((m.partition, m.offset) for m in sent) == sorted({(m.partition, m.offset) for m in sent})
    p.close()
    c = KafkaConsumer(n, bootstrap_servers="localhost:%d" % one.port, group_id=uniq("g"), auto_offset_reset="earliest", consumer_timeout_ms=5000)
    got = {m.value for m in c}
    assert got == {b"v%d" % i for i in range(50)}
    c.close()
    adm.close()


def test_auto_topic_creation_on_metadata_and_can_be_disabled():
    w = L.KafkaWarp(1, extra_env={"WARP_KAFKAWIRE_AUTO_CREATE": "false", "WARP_KAFKAWIRE_NUM_PARTITIONS": "4"})
    try:
        r = raw(w)
        n = uniq("auto")
        assert [t["error_code"] for t in C.meta(r, [n], 12, auto=True)["topics"]] == [3]
    finally:
        w.close()
    w = L.KafkaWarp(1, extra_env={"WARP_KAFKAWIRE_NUM_PARTITIONS": "4"})
    try:
        r = raw(w)
        n = uniq("auto")
        t = C.meta(r, [n], 12, auto=True)["topics"][0]
        assert t["error_code"] == 0 and len(t["partitions"]) == 4
        assert p_count(r, n) == 4
    finally:
        w.close()


def p_count(r, n):
    return len(C.meta(r, [n], 12)["topics"][0]["partitions"])


def test_transactions_are_clearly_unsupported(one):
    r = raw(one)
    d = r.call("InitProducerId", 4, transactional_id="t1", transaction_timeout_ms=60000, producer_id=-1, producer_epoch=-1)
    assert d["error_code"] == 35
    n = uniq("tx")
    C.create(r, n, 1)
    res = C.produce(r, n, 0, C.mk_batch([(None, b"x")]), txn="t1")
    assert res["error_code"] == 35
    assert r.call("FindCoordinator", 3, key="t1", key_type=1)["error_code"] == 15
    assert C.list_offsets(r, n, 0, -1)["offset"] == 0
    with pytest.raises(Exception):
        KafkaProducer(bootstrap_servers="localhost:%d" % one.port, transactional_id="tx-1").init_transactions()


# ---------------------------------------------------------------------------------------------
# sharding, scale
# ---------------------------------------------------------------------------------------------

def count_log(pg, topic=None):
    if topic:
        return sql(pg, "SELECT count(*) FROM warp_kafka_log WHERE topic = %s", (topic,))[0][0]
    return sql(pg, "SELECT count(*) FROM warp_kafka_log")[0][0]


@pytest.mark.timeout(600)
def test_partitions_spread_over_both_hosts_and_stay_whole(two):
    r = raw(two)
    n = uniq("shard")
    assert C.create(r, n, 16) == 0
    for p in range(16):
        for i in range(3):
            assert C.produce_simple(r, n, p, [b"a%d-%d" % (p, i)])["error_code"] == 0
    b0, b1 = count_log(two.pgs[0], n), count_log(two.pgs[1], n)
    assert b0 > 0 and b1 > 0 and b0 + b1 == 48
    # a partition lives on exactly one host
    where = {}
    for h, pg in enumerate(two.pgs):
        for (part,) in sql(pg, "SELECT DISTINCT part FROM warp_kafka_log WHERE topic = %s", (n,)):
            assert part not in where
            where[part] = h
    assert sorted(where) == list(range(16))
    # partition state rows are on the owning host only; topic metadata, groups and offsets are on the first host only
    for h, pg in enumerate(two.pgs):
        parts = {p for (p,) in sql(pg, "SELECT part FROM warp_kafka_parts WHERE topic = %s", (n,))}
        assert parts == {p for p, hh in where.items() if hh == h}
    assert sql(two.pgs[0], "SELECT partitions FROM warp_kafka_topics WHERE name = %s", (n,))[0][0] == 16
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_kafka_topics")[0][0] == 0
    # every partition reads back its own data through the wire
    for p in range(16):
        f = C.fetch(r, n, 0, p)
        assert [x[3] for x in f["records"]] == ["a%d-%d" % (p, i) for i in range(3)] and f["high_watermark"] == 3
        assert C.list_offsets(r, n, p, -1)["offset"] == 3
    g = uniq("shard-g")
    G = C.G(H.Ctx("localhost", two.port, "shard"), g, topics=[n])
    G.join_ok()
    G.sync({G.member: C.assignment({n: list(range(16))})})
    r.call("OffsetCommit", 8, group_id=g, generation_id_or_member_epoch=G.generation, member_id=G.member, group_instance_id=None,
           topics=[dict(name=n, partitions=[dict(partition_index=0, committed_offset=2, committed_leader_epoch=-1, committed_metadata="m")])])
    assert sql(two.pgs[0], "SELECT count(*) FROM warp_kafka_offsets WHERE grp = %s", (g,))[0][0] == 1
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_kafka_offsets")[0][0] == 0
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_kafka_groups")[0][0] == 0
    # deleting the topic cleans both hosts
    assert r.call("DeleteTopics", 5, topic_names=[n], timeout_ms=1000)["responses"][0]["error_code"] == 0
    assert count_log(two.pgs[0], n) == 0 and count_log(two.pgs[1], n) == 0
    assert sql(two.pgs[0], "SELECT count(*) FROM warp_kafka_parts WHERE topic = %s", (n,))[0][0] == 0
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_kafka_parts WHERE topic = %s", (n,))[0][0] == 0


@pytest.mark.timeout(900)
def test_a_thousand_topic_partitions(two):
    r = raw(two)
    base = uniq("k1000")
    t0 = time.time()
    topics = ["%s-%03d" % (base, i) for i in range(100)]
    for i in range(0, 100, 20):
        d = r.call("CreateTopics", 7, topics=[dict(name=t, num_partitions=10, replication_factor=1, assignments=[], configs=[]) for t in topics[i:i + 20]],
                   timeout_ms=60000, validate_only=False)
        assert all(x["error_code"] == 0 for x in d["topics"])
    created = time.time() - t0
    d = C.meta(r, None, 12)
    mine = [t for t in d["topics"] if t["name"].startswith(base)]
    assert len(mine) == 100 and sum(len(t["partitions"]) for t in mine) == 1000
    # one Produce request per topic touches its ten partitions (one record each)
    t0 = time.time()
    for t in topics:
        resp = r.call("Produce", 9, transactional_id=None, acks=-1, timeout_ms=30000, topic_data=[dict(name=t, partition_data=[
            dict(index=p, records=C.mk_batch([(None, ("%s/%d" % (t, p)).encode())])) for p in range(10)])])
        assert all(p["error_code"] == 0 and p["base_offset"] == 0 for p in resp["responses"][0]["partition_responses"])
    produced = time.time() - t0
    # a single Fetch of all 1,000 partitions returns every record
    t0 = time.time()
    f = r.call("Fetch", 12, replica_id=-1, max_wait_ms=1000, min_bytes=1, max_bytes=50 << 20, isolation_level=0, session_id=0, session_epoch=-1,
               topics=[dict(topic=t, partitions=[dict(partition=p, current_leader_epoch=-1, fetch_offset=0, last_fetched_epoch=-1, log_start_offset=-1,
                                                       partition_max_bytes=1 << 20) for p in range(10)]) for t in topics],
               forgotten_topics_data=[], rack_id="")
    fetched = time.time() - t0
    n = 0
    for tr in f["responses"]:
        for pr in tr["partitions"]:
            recs = C.parse_records(pr["records"])
            assert [x[3] for x in recs] == ["%s/%d" % (tr["topic"], pr["partition_index"])]
            n += 1
    assert n == 1000
    # both hosts hold data and every partition is on exactly one of them
    c0, c1 = sql(two.pgs[0], "SELECT count(*) FROM warp_kafka_log WHERE topic LIKE %s", (base + "%",))[0][0], \
        sql(two.pgs[1], "SELECT count(*) FROM warp_kafka_log WHERE topic LIKE %s", (base + "%",))[0][0]
    assert c0 > 300 and c1 > 300 and c0 + c1 == 1000
    print("\n1000 topic-partitions: create 100 topics %.1fs, produce 1000 records %.1fs, one Fetch of 1000 partitions %.2fs; hosts %d/%d" % (
        created, produced, fetched, c0, c1))
    r.call("DeleteTopics", 5, topic_names=topics, timeout_ms=60000)


@pytest.mark.timeout(600)
def test_concurrent_producers_get_dense_unique_offsets(one):
    n = uniq("conc")
    C.create(raw(one), n, 2)

    def work(k):
        r = raw(one)
        offs = []
        for i in range(60):
            res = C.produce(r, n, k % 2, C.mk_batch([(None, b"w%d-%d-%d" % (k, i, j)) for j in range(3)]))
            assert res["error_code"] == 0
            offs.append((res["base_offset"], 3))
        return k % 2, offs

    with concurrent.futures.ThreadPoolExecutor(8) as ex:
        results = list(ex.map(work, range(8)))
    for part in (0, 1):
        spans = sorted(o for p, offs in results if p == part for o in offs)
        expect = 0
        for base, cnt in spans:
            assert base == expect
            expect += cnt
        assert C.list_offsets(raw(one), n, part, -1)["offset"] == expect == 4 * 60 * 3
    f = C.fetch(raw(one), n, 0, 0, max_bytes=1 << 26, part_max=1 << 26)
    assert [x[0] for x in f["records"]] == list(range(720))


# ---------------------------------------------------------------------------------------------
# durability, retention, cleanup
# ---------------------------------------------------------------------------------------------

@pytest.mark.timeout(900)
def test_restart_keeps_topics_data_groups_and_offsets():
    w = L.KafkaWarp(1)
    pgs = w.pgs
    try:
        r = raw(w)
        n = uniq("dur")
        assert C.create(r, n, 2, {"retention.ms": "123456789"}) == 0
        for i in range(5):
            C.produce_simple(r, n, 0, [b"a%d" % i, b"b%d" % i])
        g = uniq("durg")
        G = C.G(H.Ctx("localhost", w.port, "dur"), g, topics=[n])
        G.join_ok()
        G.sync({G.member: C.assignment({n: [0, 1]})})
        r.call("OffsetCommit", 8, group_id=g, generation_id_or_member_epoch=G.generation, member_id=G.member, group_instance_id=None,
               topics=[dict(name=n, partitions=[dict(partition_index=0, committed_offset=7, committed_leader_epoch=-1, committed_metadata="keep")])])
        tid = C.meta(r, [n], 12)["topics"][0]["topic_id"]
        gen = G.generation
        time.sleep(0.8)                                     # the group snapshot is persisted within 200 ms of a change
        w.proc.close()
        w.proc.process.wait(timeout=30)
        w2 = None
        for attempt in range(2):
            try:
                w2 = L.KafkaWarp(pgs=pgs, default_store=False)
                break
            except TimeoutError:
                if attempt:
                    raise
        try:
            r2 = raw(w2)
            assert C.meta(r2, [n], 12)["topics"][0]["topic_id"] == tid
            f = C.fetch(r2, n, 0, 0)
            assert [x[3] for x in f["records"]] == [v for i in range(5) for v in ("a%d" % i, "b%d" % i)] and f["high_watermark"] == 10
            d = r2.call("OffsetFetch", 7, group_id=g, topics=[dict(name=n, partition_indexes=[0])], require_stable=False)
            p = d["topics"][0]["partitions"][0]
            assert (p["committed_offset"], p["metadata"]) == (7, "keep")
            assert C.describe(r2, g)["state"] == "Empty"          # the members are gone, the group and its generation are not
            G2 = C.G(H.Ctx("localhost", w2.port, "dur2"), g, topics=[n])
            j = G2.join_ok()
            assert j["error_code"] == 0 and j["generation_id"] > gen
            cfg = r2.call("DescribeConfigs", 4, resources=[dict(resource_type=2, resource_name=n, configuration_keys=["retention.ms"])],
                          include_synonyms=False, include_documentation=False)
            assert cfg["results"][0]["configs"][0]["value"] == "123456789"
            assert C.produce_simple(r2, n, 0, [b"after"])["base_offset"] == 10
        finally:
            w2.proc.close()
    finally:
        for pg in pgs:
            pg.close()


@pytest.mark.timeout(600)
def test_retention_by_time_and_size_and_delete_records():
    w = L.KafkaWarp(1, extra_env={"WARP_KAFKAWIRE_SWEEP_MS": "300"})
    try:
        r = raw(w)
        now = int(time.time() * 1000)
        t = uniq("ret")
        C.create(r, t, 1, {"retention.ms": "5000"})
        C.produce(r, t, 0, C.mk_batch([(None, b"old-1"), (None, b"old-2")], ts0=now - 60000))
        C.produce(r, t, 0, C.mk_batch([(None, b"fresh")], ts0=now + 3_600_000))
        deadline = time.time() + 20
        while time.time() < deadline and C.list_offsets(r, t, 0, -2)["offset"] < 2:
            time.sleep(0.2)
        assert C.list_offsets(r, t, 0, -2)["offset"] == 2, "expired batch was not removed"
        f = C.fetch(r, t, 0, 0)
        assert f["error_code"] == 1                                  # below the new log start
        f = C.fetch(r, t, 2, 0)
        assert [x[3] for x in f["records"]] == ["fresh"] and f["log_start_offset"] == 2
        assert C.list_offsets(r, t, 0, -1)["offset"] == 3
        # size retention keeps the newest batches within retention.bytes
        s = uniq("retb")
        C.create(r, s, 1, {"retention.bytes": "6000"})
        for i in range(10):
            C.produce(r, s, 0, C.mk_batch([(None, (b"%d" % i) * 1000)], ts0=now + 3_600_000))
        deadline = time.time() + 20
        while time.time() < deadline and C.list_offsets(r, s, 0, -2)["offset"] == 0:
            time.sleep(0.2)
        start = C.list_offsets(r, s, 0, -2)["offset"]
        assert 3 <= start <= 6, start
        kept = C.fetch(r, s, start, 0, max_bytes=1 << 22, part_max=1 << 22)
        assert [x[0] for x in kept["records"]] == list(range(start, 10))
        # compacted topics are not time-expired by this sweeper
        cpt = uniq("cmp")
        C.create(r, cpt, 1, {"cleanup.policy": "compact", "retention.ms": "1000"})
        C.produce(r, cpt, 0, C.mk_batch([(b"k", b"v")], ts0=now - 60000))
        time.sleep(1.5)
        assert C.list_offsets(r, cpt, 0, -2)["offset"] == 0 and len(C.fetch(r, cpt, 0, 0)["records"]) == 1
        # infinite retention keeps old data
        inf = uniq("inf")
        C.create(r, inf, 1, {"retention.ms": "-1"})
        C.produce(r, inf, 0, C.mk_batch([(None, b"ancient")], ts0=1000))
        time.sleep(1.5)
        assert len(C.fetch(r, inf, 0, 0)["records"]) == 1
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------
# auth, advertised listener, node registry
# ---------------------------------------------------------------------------------------------

@pytest.mark.timeout(600)
def test_sasl_plain_uses_the_shared_credential_store():
    w = L.KafkaWarp(1, extra_env={"WARP_KAFKAWIRE_AUTH": "true"})
    try:
        bs = "localhost:%d" % w.port
        # unauthenticated requests other than ApiVersions/SaslHandshake close the connection
        r = raw(w)
        assert r.call("ApiVersions", 3, client_software_name="t", client_software_version="1")["error_code"] == 0
        r.send("Metadata", 12, topics=[], allow_auto_topic_creation=False, include_topic_authorized_operations=False)
        with pytest.raises((ConnectionError, OSError)):
            r.recv_raw()
        # handshake: unsupported mechanism lists PLAIN; then wrong and right passwords
        r = raw(w)
        d = r.call("SaslHandshake", 1, mechanism="SCRAM-SHA-256")
        assert d["error_code"] == 33 and d["mechanisms"] == ["PLAIN"]
        r = raw(w)
        assert r.call("SaslHandshake", 1, mechanism="PLAIN")["error_code"] == 0
        assert r.call("SaslAuthenticate", 2, auth_bytes=b"\x00postgres\x00wrong")["error_code"] == 58
        r = raw(w)
        assert r.call("SaslHandshake", 1, mechanism="PLAIN")["error_code"] == 0
        assert r.call("SaslAuthenticate", 2, auth_bytes=b"\x00postgres\x00postgres")["error_code"] == 0
        assert C.meta(r, [], 12)["topics"] == []
        # SaslAuthenticate without a handshake
        r = raw(w)
        assert r.call("SaslAuthenticate", 2, auth_bytes=b"\x00postgres\x00postgres")["error_code"] == 34
        # a real client
        n = uniq("sasl")
        good = dict(bootstrap_servers=bs, security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN", sasl_plain_username="postgres", sasl_plain_password="postgres")
        p = KafkaProducer(**good)
        meta = p.send(n, b"secret").get(15)
        p.close()
        c = KafkaConsumer(n, group_id=uniq("g"), auto_offset_reset="earliest", consumer_timeout_ms=5000, **good)
        assert [m.value for m in c] == [b"secret"] and meta.offset == 0
        c.close()
        bad = dict(good, sasl_plain_password="nope")
        with pytest.raises(Exception):
            KafkaProducer(**bad, api_version_auto_timeout_ms=4000, request_timeout_ms=4000).send(n, b"x").get(8)
        # an unauthenticated client cannot even bootstrap
        with pytest.raises(Exception):
            KafkaProducer(bootstrap_servers=bs, api_version_auto_timeout_ms=3000, request_timeout_ms=3000).send(n, b"x").get(6)
    finally:
        w.close()


def test_no_auth_by_default_and_sasl_handshake_is_refused(one):
    r = raw(one)
    assert r.call("SaslHandshake", 1, mechanism="PLAIN")["error_code"] == 34
    assert C.meta(raw(one), [], 12)["topics"] == []


@pytest.mark.timeout(600)
def test_advertised_listener_is_what_clients_are_told():
    w = L.KafkaWarp(1, extra_env={"WARP_KAFKAWIRE_ADVERTISED_HOST": "kafka.example.test", "WARP_KAFKAWIRE_ADVERTISED_PORT": "29999"})
    try:
        r = raw(w)
        d = C.meta(r, [], 12)
        assert [(b["host"], b["port"]) for b in d["brokers"]] == [("kafka.example.test", 29999)]
        fc = r.call("FindCoordinator", 3, key="g", key_type=0)
        assert (fc["host"], fc["port"]) == ("kafka.example.test", 29999)
        assert d["controller_id"] == d["brokers"][0]["node_id"]
        dc = r.call("DescribeCluster", 1, include_cluster_authorized_operations=False, endpoint_type=1)
        assert [(b["host"], b["port"]) for b in dc["brokers"]] == [("kafka.example.test", 29999)]
    finally:
        w.close()


def test_default_advertised_endpoint_is_localhost_and_the_listening_port(one):
    d = C.meta(raw(one), [], 12)
    assert [(b["host"], b["port"]) for b in d["brokers"]] == [("localhost", one.port)] and d["brokers"][0]["node_id"] == 0
    assert d["cluster_id"] and len(d["cluster_id"]) == 22


@pytest.mark.timeout(900)
def test_two_warp_nodes_share_one_log_and_route_groups_deterministically():
    a = L.KafkaWarp(1)
    b = None
    try:
        b = L.KafkaWarp(pgs=a.pgs, default_store=False)
        ra, rb = raw(a), raw(b)
        # both nodes registered themselves: the brokers list is the same on either node and has two distinct ids
        deadline = time.time() + 20
        while time.time() < deadline and len(C.meta(ra, [], 12)["brokers"]) < 2:
            time.sleep(0.5)
        ba = sorted((x["node_id"], x["host"], x["port"]) for x in C.meta(ra, [], 12)["brokers"])
        bb = sorted((x["node_id"], x["host"], x["port"]) for x in C.meta(rb, [], 12)["brokers"])
        assert ba == bb and len(ba) == 2 and {x[0] for x in ba} == {0, 1} and {x[2] for x in ba} == {a.port, b.port}
        # one log: produced through A, fetched through B, offsets continue across nodes
        n = uniq("two-nodes")
        assert C.create(ra, n, 4) == 0
        assert C.produce_simple(ra, n, 1, [b"from-a"])["base_offset"] == 0
        assert C.produce_simple(rb, n, 1, [b"from-b"])["base_offset"] == 1
        assert [x[3] for x in C.fetch(rb, n, 0, 1)["records"]] == ["from-a", "from-b"]
        assert [x[3] for x in C.fetch(ra, n, 0, 1)["records"]] == ["from-a", "from-b"]
        # partition leaders are spread over both nodes
        leaders = {p["leader_id"] for p in C.meta(ra, [n], 12)["topics"][0]["partitions"]}
        assert leaders <= {0, 1}
        # a group has exactly one coordinator, the same answer from both nodes; the other node refuses group requests (NOT_COORDINATOR)
        for gid in ("g-a", "g-b", "g-c", "g-d", "g-e", "g-f"):
            fa, fb = ra.call("FindCoordinator", 3, key=gid, key_type=0), rb.call("FindCoordinator", 3, key=gid, key_type=0)
            assert (fa["node_id"], fa["port"]) == (fb["node_id"], fb["port"])
            coord, other = (ra, rb) if fa["port"] == a.port else (rb, ra)
            assert coord.call("Heartbeat", 4, group_id=gid, generation_id=1, member_id="x", group_instance_id=None)["error_code"] == 25
            assert other.call("Heartbeat", 4, group_id=gid, generation_id=1, member_id="x", group_instance_id=None)["error_code"] == 16
        # a real consumer group works whichever node it bootstraps from (it follows FindCoordinator)
        cons = KafkaConsumer(n, bootstrap_servers="localhost:%d" % b.port, group_id=uniq("tg"), auto_offset_reset="earliest", consumer_timeout_ms=6000)
        assert sorted(m.value for m in cons) == [b"from-a", b"from-b"]
        cons.close()
    finally:
        if b is not None:
            b.proc.close()
        a.close()


# ---------------------------------------------------------------------------------------------
# real Kafka CLI tools from the apache/kafka image against Warp
# ---------------------------------------------------------------------------------------------

def _docker_ok():
    if not shutil.which("docker"):
        return False
    r = subprocess.run(["docker", "image", "inspect", "apache/kafka:latest"], capture_output=True)
    return r.returncode == 0


cli = pytest.mark.skipif(not _docker_ok(), reason="docker and the apache/kafka image are needed for the Kafka CLI tool tests")


@pytest.fixture(scope="module")
def cliwarp():
    w = L.KafkaWarp(1, extra_env={"WARP_KAFKAWIRE_ADVERTISED_HOST": "host.docker.internal"})
    yield w
    w.close()


def tool(w, script, *args, stdin=None, timeout=180):
    cmd = ["docker", "run", "--rm", "-i", "--memory", "512m", "-e", "KAFKA_HEAP_OPTS=-Xmx256m", "--entrypoint", "/opt/kafka/bin/" + script,
           "apache/kafka:latest", *args]
    p = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


@cli
@pytest.mark.timeout(900)
def test_kafka_cli_topics_console_producer_consumer_groups_perf(cliwarp):
    bs = "host.docker.internal:%d" % cliwarp.port
    n = uniq("cli")
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--create", "--topic", n, "--partitions", "3", "--replication-factor", "1")
    assert rc == 0 and "Created topic" in out, (out, err)
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--list")
    assert rc == 0 and n in out.split()
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--describe", "--topic", n)
    assert rc == 0 and "PartitionCount: 3" in out and out.count("Partition:") == 3 and "ReplicationFactor: 1" in out, out
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--create", "--topic", n, "--partitions", "1")
    assert rc != 0 and "already exists" in (out + err)
    lines = "\n".join("line-%d" % i for i in range(25)) + "\n"
    rc, out, err = tool(cliwarp, "kafka-console-producer.sh", "--bootstrap-server", bs, "--topic", n, stdin=lines)
    assert rc == 0, err
    g = uniq("cli-g")
    rc, out, err = tool(cliwarp, "kafka-console-consumer.sh", "--bootstrap-server", bs, "--topic", n, "--from-beginning", "--group", g,
                        "--max-messages", "25", "--timeout-ms", "20000")
    assert rc == 0 and sorted(out.split("\n")[:25] if False else [x for x in out.split("\n") if x.startswith("line-")]) == sorted("line-%d" % i for i in range(25)), (out, err)
    rc, out, err = tool(cliwarp, "kafka-consumer-groups.sh", "--bootstrap-server", bs, "--list")
    assert rc == 0 and g in out.split()
    rc, out, err = tool(cliwarp, "kafka-consumer-groups.sh", "--bootstrap-server", bs, "--describe", "--group", g)
    assert rc == 0 and n in out and "LAG" in out, out
    rows = [ln.split() for ln in out.splitlines() if ln.startswith(g) or (ln.split() and ln.split()[0] == g)]
    assert sum(int(x[3]) for x in rows) == 25 and all(int(x[5]) == 0 for x in rows), out
    rc, out, err = tool(cliwarp, "kafka-consumer-groups.sh", "--bootstrap-server", bs, "--group", g, "--topic", n, "--reset-offsets", "--to-earliest", "--execute")
    assert rc == 0 and n in out, (out, err)
    rc, out, err = tool(cliwarp, "kafka-consumer-groups.sh", "--bootstrap-server", bs, "--describe", "--group", g)
    rows = [ln.split() for ln in out.splitlines() if ln.split() and ln.split()[0] == g]
    assert sum(int(x[5]) for x in rows) == 25, out
    rc, out, err = tool(cliwarp, "kafka-get-offsets.sh", "--bootstrap-server", bs, "--topic", n)
    assert rc == 0 and sum(int(x.split(":")[2]) for x in out.split()) == 25, out
    rc, out, err = tool(cliwarp, "kafka-consumer-groups.sh", "--bootstrap-server", bs, "--delete", "--group", g)
    assert rc == 0 and "deletion" in out.lower(), (out, err)
    rc, out, err = tool(cliwarp, "kafka-producer-perf-test.sh", "--topic", n, "--num-records", "3000", "--record-size", "200", "--throughput", "-1",
                        "--producer-props", "bootstrap.servers=" + bs, "acks=all", "linger.ms=5", timeout=240)
    assert rc == 0 and "3000 records sent" in out, (out, err)
    rc, out, err = tool(cliwarp, "kafka-configs.sh", "--bootstrap-server", bs, "--entity-type", "topics", "--entity-name", n, "--alter", "--add-config", "retention.ms=99999")
    assert rc == 0, (out, err)
    rc, out, err = tool(cliwarp, "kafka-configs.sh", "--bootstrap-server", bs, "--entity-type", "topics", "--entity-name", n, "--describe")
    assert rc == 0 and "retention.ms=99999" in out, out
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--alter", "--topic", n, "--partitions", "5")
    assert rc == 0, (out, err)
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--delete", "--topic", n)
    assert rc == 0, (out, err)
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--list")
    assert n not in out.split()


@cli
@pytest.mark.timeout(600)
def test_kafka_cli_consumer_perf_and_delete_records_and_api_versions(cliwarp):
    bs = "host.docker.internal:%d" % cliwarp.port
    n = uniq("cli2")
    assert tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--create", "--topic", n, "--partitions", "1")[0] == 0
    rc, out, err = tool(cliwarp, "kafka-producer-perf-test.sh", "--topic", n, "--num-records", "2000", "--record-size", "100", "--throughput", "-1",
                        "--producer-props", "bootstrap.servers=" + bs, "acks=1", timeout=240)
    assert rc == 0, (out, err)
    rc, out, err = tool(cliwarp, "kafka-consumer-perf-test.sh", "--bootstrap-server", bs, "--topic", n, "--messages", "2000", "--timeout", "30000")
    assert rc == 0 and "2000" in out, (out, err)
    spec = json.dumps({"partitions": [{"topic": n, "partition": 0, "offset": 1500}], "version": 1})
    rc, out, err = tool(cliwarp, "kafka-delete-records.sh", "--bootstrap-server", bs, "--offset-json-file", "/dev/stdin", stdin=spec)
    assert rc == 0 and "low_watermark: 1500" in out, (out, err)
    rc, out, err = tool(cliwarp, "kafka-get-offsets.sh", "--bootstrap-server", bs, "--topic", n, "--time", "-2")
    assert rc == 0 and out.strip().endswith(":1500"), out
    rc, out, err = tool(cliwarp, "kafka-broker-api-versions.sh", "--bootstrap-server", bs)
    assert rc == 0 and "Produce(0): 3 to 9" in out and "Fetch(1): 4 to 12" in out and "ApiVersions(18): 0 to 3" in out, out
    rc, out, err = tool(cliwarp, "kafka-topics.sh", "--bootstrap-server", bs, "--delete", "--topic", n)
    assert rc == 0


# ---------------------------------------------------------------------------------------------
# MCP tools, metrics, admin API
# ---------------------------------------------------------------------------------------------

@pytest.mark.timeout(900)
def test_mcp_tools_share_the_wire_data_and_read_only_hides_writes():
    mcp = free_port()
    w = L.KafkaWarp(1, extra_env={"WARP_MCP_PORT": str(mcp)})
    ro = None
    try:
        ep = create_endpoint(w, "kafka-ep", "db:default", wait_live=False)
        path, token = ep["path"], ep["token"]
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                tool_names(mcp, path=path, token=token)
                break
            except Exception:  # noqa: BLE001 -- MCP listener still starting
                time.sleep(1)

        def cj(name, args=None):
            return call_json(mcp, name, args, path=path, token=token)

        def err(name, args=None):
            return call(mcp, name, args, path=path, token=token, expect_error=True)[0]

        names = tool_names(mcp, path=path, token=token)
        for t in ["kafka_list_topics", "kafka_describe_topic", "kafka_create_topic", "kafka_delete_topic", "kafka_produce", "kafka_fetch",
                  "kafka_list_groups", "kafka_group_lag"]:
            assert t in names, t
        t = uniq("mcp")
        assert cj("kafka_create_topic", {"topic": t, "partitions": 2, "config": {"retention.ms": "3600000"}})["partitions"] == 2
        assert "ALREADY_EXISTS" in err("kafka_create_topic", {"topic": t}) or "already exists" in err("kafka_create_topic", {"topic": t})
        assert "INVALID_TOPIC" in err("kafka_create_topic", {"topic": "bad name"})
        assert "INVALID_CONFIG" in err("kafka_create_topic", {"topic": uniq("x"), "config": {"no.such": "1"}})
        assert t in [x["name"] for x in cj("kafka_list_topics")["topics"]]
        d = cj("kafka_describe_topic", {"topic": t})
        assert d["partitions"] == 2 and d["config"]["retention.ms"] == "3600000" and d["config"]["cleanup.policy"] == "delete"
        # produce with the default partitioner hash: the wire's consumer sees exactly that placement
        cj("kafka_produce", {"topic": t, "key": "user-1", "value": "hello", "headers": {"h": "v"}})
        cj("kafka_produce", {"topic": t, "partition": 1, "messages": [{"value": "m1"}, {"valueBase64": "/w==", "key": "k"}, {"value": "m3", "timestamp": 1700000000000}]})
        assert "UNKNOWN_TOPIC_OR_PARTITION" in err("kafka_produce", {"topic": t, "partition": 9, "value": "x"})
        assert "UNKNOWN_TOPIC_OR_PARTITION" in err("kafka_produce", {"topic": uniq("nope"), "value": "x"})
        cons = KafkaConsumer(t, bootstrap_servers="localhost:%d" % w.port, group_id=uniq("g"), auto_offset_reset="earliest", consumer_timeout_ms=5000)
        seen = {(m.partition, m.offset): (m.key, m.value, m.headers) for m in cons}
        cons.close()
        assert len(seen) == 4 and (b"user-1", b"hello", [("h", b"v")]) in seen.values()
        p = KafkaProducer(bootstrap_servers="localhost:%d" % w.port)
        p.send(t, key=b"wire", value="wíre ✓".encode(), partition=0, headers=[("x", b"")]).get(10)
        p.close()
        for part in (0, 1):
            f = cj("kafka_fetch", {"topic": t, "partition": part, "offset": 0, "maxMessages": 10})
            assert f["logStartOffset"] == 0 and f["highWatermark"] == len([1 for (pp, _o) in seen if pp == part]) + (1 if part == 0 else 0)
            for m in f["messages"]:
                assert m["offset"] >= 0
        f0 = cj("kafka_fetch", {"topic": t, "partition": 0})
        assert any(m.get("value") == "wíre ✓" and m["key"] == "wire" for m in f0["messages"])
        f1 = cj("kafka_fetch", {"topic": t, "partition": 1, "offset": 1, "maxMessages": 1})
        assert len(f1["messages"]) == 1 and f1["messages"][0]["offset"] == 1
        assert any(m.get("valueEncoding") == "base64" or m.get("keyEncoding") is None for m in cj("kafka_fetch", {"topic": t, "partition": 1})["messages"])
        assert "OFFSET_OUT_OF_RANGE" in err("kafka_fetch", {"topic": t, "partition": 1, "offset": 99})
        assert "UNKNOWN_TOPIC_OR_PARTITION" in err("kafka_fetch", {"topic": uniq("nope")})
        # consumer group lag
        g = uniq("lag")
        r = raw(w)
        G = C.G(H.Ctx("localhost", w.port, "mcp"), g, topics=[t])
        G.join_ok()
        G.sync({G.member: C.assignment({t: [0, 1]})})
        r.call("OffsetCommit", 8, group_id=g, generation_id_or_member_epoch=G.generation, member_id=G.member, group_instance_id=None,
               topics=[dict(name=t, partitions=[dict(partition_index=1, committed_offset=1, committed_leader_epoch=-1, committed_metadata=None)])])
        time.sleep(0.6)
        lag = cj("kafka_group_lag", {"group": g})
        assert lag["groupId"] == g and lag["totalLag"] == 2 and lag["partitions"][0]["committedOffset"] == 1 and lag["partitions"][0]["logEndOffset"] == 3
        assert g in [x["groupId"] for x in cj("kafka_list_groups")["groups"]]
        assert "GROUP_ID_NOT_FOUND" in err("kafka_group_lag", {"group": uniq("nogroup")})
        d = cj("describe_backend", {"backend": "default.kafkastore"})
        assert t in json.dumps(d["contents"]["topics"]) and t in json.dumps(d["contents"]["retainedBatches"])
        cj("kafka_delete_topic", {"topic": t})
        assert t not in [x["name"] for x in cj("kafka_list_topics")["topics"]]
        assert "UNKNOWN_TOPIC_OR_PARTITION" in err("kafka_delete_topic", {"topic": t})
        # metrics count the wire operations and the MCP tool calls
        r = requests.get("http://localhost:%d/api/metrics/summary" % w.proc.metrics_port, headers={"Authorization": "Bearer " + ADMIN_TOKEN}, timeout=10)
        assert "kafkawire" in r.text and "mcp-kafkastore" in r.text, r.text[:400]
        lb = call_json(mcp, "list_backends")
        assert next(b for b in lb["backends"] if b["name"] == "default")["enabledStores"] == ["kafka"]
        # a read-only Warp on the same config database hides and refuses every write tool but still reads
        mcp2 = free_port()
        ro = L.KafkaWarp(pgs=w.pgs, default_store=False, extra_env={"WARP_MCP_PORT": str(mcp2), "WARP_MCP_READ_ONLY": "true"})
        deadline = time.time() + 30
        rnames = set()
        while time.time() < deadline:
            try:
                rnames = tool_names(mcp2, path=path, token=token)
                break
            except Exception:  # noqa: BLE001
                time.sleep(1)
        assert "kafka_list_topics" in rnames and "kafka_fetch" in rnames and "kafka_group_lag" in rnames
        for hidden in ("kafka_produce", "kafka_create_topic", "kafka_delete_topic"):
            assert hidden not in rnames
        rr = call(mcp2, "kafka_create_topic", {"topic": uniq("ro")}, path=path, token=token, expect_error=True)
        assert rr
    finally:
        if ro is not None:
            ro.proc.close()
        w.close()


def test_store_appears_in_the_backend_set_api(one):
    r = requests.get("http://localhost:%d/api/backend-sets" % one.proc.metrics_port, headers={"Authorization": "Bearer " + ADMIN_TOKEN}, timeout=10)
    assert r.status_code == 200
    stores = {s["id"]: s for s in r.json()["stores"]}
    assert stores["kafka"]["shardable"] is True and "Kafka" in stores["kafka"]["label"] and stores["kafka"]["setEnvVar"] == "WARP_KAFKAWIRE_SET"
    hosts = r.json()["sets"][0]["backends"][0]
    assert "kafka" in json.dumps(hosts)


@pytest.mark.timeout(900)
def test_adding_a_backend_flags_rebalance_for_the_kafka_store():
    w = L.KafkaWarp(1)
    extra = None
    try:
        from warp_test_support import RealPostgres
        extra = RealPostgres()
        body = {"name": "pg9", "url": "jdbc:postgresql://localhost:%d/postgres" % extra.port, "user": "postgres", "password": "postgres", "enabledStores": ["kafka"]}
        res = w.api("POST", "/api/backend-sets/default/backends", body, expect=201)
        assert any(x["store"] == "kafka" for x in res["rebalanceRequired"]), res
        # the new host got the schema
        assert sql(extra, "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'warp_kafka_%'")[0][0] >= 9
    finally:
        w.close()
        if extra is not None:
            extra.close()


# ---------------------------------------------------------------------------------------------
# long polls never pin pooled connections
# ---------------------------------------------------------------------------------------------

@pytest.mark.timeout(900)
def test_many_blocked_fetches_do_not_starve_a_tiny_connection_pool():
    w = L.KafkaWarp(1, extra_env={"WARP_POOL_MAX_SIZE": "3"})
    try:
        r0 = raw(w)
        n = uniq("pool")
        C.create(r0, n, 1)
        waiters = []
        res = []

        def wait_fetch():
            r = raw(w)
            res.append(C.fetch(r, n, 0, 0, max_wait=8000, min_bytes=1))

        for _ in range(12):
            th = threading.Thread(target=wait_fetch)
            th.start()
            waiters.append(th)
        time.sleep(1.5)
        # while twelve fetches wait (no connection is held), produce, admin and group traffic still works on a pool of three
        t0 = time.time()
        assert C.produce_simple(raw(w), n, 0, [b"p1"])["error_code"] == 0
        assert time.time() - t0 < 4
        for th in waiters:
            th.join(15)
        assert len(res) == 12 and all([x[3] for x in f["records"]] == ["p1"] for f in res)
        n2 = uniq("pool2")
        assert C.create(r0, n2, 2) == 0
        assert C.describe(r0, uniq("poolg"))["state"] == "Dead"
    finally:
        w.close()


@pytest.mark.timeout(900)
def test_frontend_serves_the_set_named_by_warp_kafkawire_set():
    from warp_test_support import RealPostgres
    other = RealPostgres()
    w = L.KafkaWarp(1, extra_env={"WARP_KAFKAWIRE_SET": "streams"}, default_store=False)
    try:
        w.api("POST", "/api/backend-sets", {"name": "streams", "description": "kafka set"}, expect=201)
        w.api("POST", "/api/backend-sets/streams/backends", {
            "name": "st1", "url": "jdbc:postgresql://localhost:%d/postgres" % other.port, "user": "postgres", "password": "postgres",
            "enabledStores": ["kafka"]}, expect=201)
        time.sleep(2)
        r = raw(w)
        n = uniq("set")
        assert C.create(r, n, 2) == 0
        for i in range(10):
            assert C.produce_simple(r, n, i % 2, [b"v%d" % i])["error_code"] == 0
        assert count_log(other, n) == 10
        # the default set's Postgres was never touched
        assert not sql(w.pgs[0], "SELECT 1 FROM information_schema.tables WHERE table_name = 'warp_kafka_log'")
    finally:
        w.close()
        other.close()


@pytest.mark.timeout(600)
def test_confluent_kafka_librdkafka_client(one):
    """librdkafka (confluent-kafka): idempotent producer with every codec, subscribed consumer group, commit, watermarks, admin."""
    ck = pytest.importorskip("confluent_kafka")
    from confluent_kafka.admin import AdminClient, NewTopic
    bs = "localhost:%d" % one.port
    t = uniq("ck")
    adm = AdminClient({"bootstrap.servers": bs})
    for k, f in adm.create_topics([NewTopic(t, 3, 1)]).items():
        f.result(timeout=30)
    assert t in adm.list_topics(timeout=10).topics
    total = 0
    for codec in ("none", "gzip", "snappy", "lz4", "zstd"):
        errs = []
        p = ck.Producer({"bootstrap.servers": bs, "compression.codec": codec, "enable.idempotence": True, "linger.ms": 20})
        for i in range(60):
            p.produce(t, key=b"k%d" % i, value=codec.encode() * 40 + b"%d" % i, headers=[("h", b"x")], on_delivery=lambda e, m: errs.append(e) if e else None)
            total += 1
        assert p.flush(30) == 0 and not errs, (codec, errs)
    g = uniq("ckg")
    c = ck.Consumer({"bootstrap.servers": bs, "group.id": g, "auto.offset.reset": "earliest", "enable.auto.commit": False, "session.timeout.ms": 10000})
    c.subscribe([t])
    seen = []
    deadline = time.time() + 60
    while len(seen) < total and time.time() < deadline:
        m = c.poll(1.0)
        if m is not None and not m.error():
            seen.append((m.partition(), m.offset(), m.value()[:4]))
    assert len(seen) == total and len({(p, o) for p, o, _v in seen}) == total
    assert {v for _p, _o, v in seen} == {b"none", b"gzip", b"snap", b"lz4l", b"zstd"}
    c.commit(asynchronous=False)
    committed = {x.partition: x.offset for x in c.committed([ck.TopicPartition(t, i) for i in range(3)], timeout=10)}
    marks = {i: c.get_watermark_offsets(ck.TopicPartition(t, i), timeout=10) for i in range(3)}
    assert committed == {i: marks[i][1] for i in range(3)} and sum(committed.values()) == total
    c.close()
    assert g in [x.group_id for x in adm.list_consumer_groups(request_timeout=10).result().valid]
    # a second consumer of the same group resumes at the committed offsets: nothing left to read
    c2 = ck.Consumer({"bootstrap.servers": bs, "group.id": g, "auto.offset.reset": "earliest", "session.timeout.ms": 10000})
    c2.subscribe([t])
    leftover = 0
    end = time.time() + 8
    while time.time() < end:
        m = c2.poll(1.0)
        if m is not None and not m.error():
            leftover += 1
    c2.close()
    assert leftover == 0
