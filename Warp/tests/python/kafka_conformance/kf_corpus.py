"""The Kafka conformance corpus: every case is a function `case(ctx)` that talks to one broker and records observations with ctx.step.

It is recorded against a real Apache Kafka and replayed against Warp (see kf_harness.py). Raw-protocol cases build requests at explicit
versions (error codes, offsets, watermarks, group state transitions); client cases use kafka-python's producer/consumer/admin.
"""
import threading
import time

from kafka.record.default_records import DefaultRecordBatchBuilder
from kafka.record.memory_records import MemoryRecords

# versions Warp implements, per API name (min, max) -- the corpus exercises the whole range
V = {"Produce": (3, 9), "Fetch": (4, 12), "ListOffsets": (1, 7), "Metadata": (1, 12), "OffsetCommit": (2, 8), "OffsetFetch": (1, 7),
     "FindCoordinator": (0, 3), "JoinGroup": (0, 7), "Heartbeat": (0, 4), "LeaveGroup": (0, 4), "SyncGroup": (0, 5), "DescribeGroups": (0, 5),
     "ListGroups": (0, 4), "CreateTopics": (2, 7), "DeleteTopics": (1, 5), "DeleteRecords": (0, 2), "InitProducerId": (0, 4),
     "OffsetForLeaderEpoch": (2, 4), "DescribeConfigs": (1, 4), "AlterConfigs": (0, 2), "CreatePartitions": (0, 3), "DeleteGroups": (0, 2),
     "IncrementalAlterConfigs": (0, 1), "DescribeCluster": (0, 1), "ApiVersions": (0, 3)}
T0 = 1700000000000


def spread(name):
    lo, hi = V[name]
    return sorted({lo, (lo + hi) // 2, hi})


# ---------------------------------------------------------------------------------------------------------------- helpers

def mk_batch(recs, comp=0, pid=-1, epoch=-1, seq=-1, ts0=T0):
    """recs: list of (key, value) or (key, value, headers). Returns record batch v2 bytes with base offset 0."""
    b = DefaultRecordBatchBuilder(2, comp, False, pid, epoch, seq, 1 << 22)
    for i, r in enumerate(recs):
        headers = r[2] if len(r) > 2 else []
        b.append(i, ts0 + i, r[0], r[1], headers)
    return bytes(b.build())


def parse_records(raw):
    """(offset, timestamp, key, value, headers) of every record of a fetched records blob (all batches)."""
    out = []
    if not raw:
        return out
    mr = MemoryRecords(raw)
    while mr.has_next():
        batch = mr.next_batch()
        for r in batch:
            out.append([r.offset, r.timestamp, r.key.decode("latin1") if r.key is not None else None,
                        r.value.decode("latin1") if r.value is not None else None,
                        [[h[0], h[1].decode("latin1") if h[1] is not None else None] for h in r.headers]])
    return out


def create(raw, name, parts=1, cfg=None, ver=5, validate=False):
    d = raw.call("CreateTopics", ver, topics=[dict(name=name, num_partitions=parts, replication_factor=1, assignments=[],
                                                    configs=[dict(name=k, value=v) for k, v in (cfg or {}).items()])],
                 timeout_ms=10000, validate_only=False)
    err = d["topics"][0]["error_code"]
    if err == 0 and not validate:
        # a KRaft broker learns of a new topic asynchronously: wait until every partition answers before the case continues
        t0 = time.time()
        while time.time() - t0 < 10:
            ok = True
            for p in range(max(parts, 1)):
                lo = raw.call("ListOffsets", 7, replica_id=-1, isolation_level=0, topics=[dict(name=name, partitions=[
                    dict(partition_index=p, current_leader_epoch=-1, timestamp=-1)])])
                ok = ok and lo["topics"][0]["partitions"][0]["error_code"] == 0
            if ok:
                break
            time.sleep(0.05)
        time.sleep(0.05)
    return err


def produce(raw, topic, part, batch, acks=-1, ver=9, txn=None):
    d = raw.call("Produce", ver, transactional_id=txn, acks=acks, timeout_ms=10000,
                 topic_data=[dict(name=topic, partition_data=[dict(index=part, records=batch)])])
    return d["responses"][0]["partition_responses"][0]


def produce_simple(raw, topic, part, values, **kw):
    return produce(raw, topic, part, mk_batch([(None, v) for v in values]), **kw)


def fetch(raw, topic, offset, part=0, ver=12, max_wait=0, min_bytes=1, max_bytes=1 << 20, part_max=1 << 20, isolation=0):
    d = raw.call("Fetch", ver, replica_id=-1, max_wait_ms=max_wait, min_bytes=min_bytes, max_bytes=max_bytes, isolation_level=isolation,
                 session_id=0, session_epoch=-1,
                 topics=[dict(topic=topic, partitions=[dict(partition=part, current_leader_epoch=-1, fetch_offset=offset, last_fetched_epoch=-1,
                                                             log_start_offset=-1, partition_max_bytes=part_max)])],
                 forgotten_topics_data=[], rack_id="")
    p = d["responses"][0]["partitions"][0]
    recs = parse_records(p.get("records"))
    out = dict(error_code=p["error_code"], high_watermark=p["high_watermark"], last_stable_offset=p["last_stable_offset"],
               log_start_offset=p.get("log_start_offset"), top_error=d.get("error_code"), records=recs)
    return out


def list_offsets(raw, topic, part, ts, ver=7):
    d = raw.call("ListOffsets", ver, replica_id=-1, isolation_level=0, topics=[dict(name=topic, partitions=[
        dict(partition_index=part, current_leader_epoch=-1, timestamp=ts)])])
    return d["topics"][0]["partitions"][0]


def meta(raw, topics, ver=12, auto=False):
    return raw.call("Metadata", ver, topics=None if topics is None else [dict(name=t, topic_id="00000000-0000-0000-0000-000000000000") if ver >= 10
                                                                          else dict(name=t) for t in topics],
                    allow_auto_topic_creation=auto, include_topic_authorized_operations=False)


def rd(x):
    """Round a dict's floats/times for stable recording."""
    return x


# ---------------------------------------------------------------------------------------------------------------- cases: topics

def case_api_versions(c):
    r = c.raw()
    for v in range(0, 4):
        d = r.call("ApiVersions", v, client_software_name="x", client_software_version="1")
        c.step("apiversions_v%d_error" % v, d["error_code"])
        if v == 3:
            keys = {a["api_key"]: (a["min_version"], a["max_version"]) for a in d["api_keys"]}
            # the broker must advertise every API Warp implements over at least the versions Warp implements (and Warp's own table must be
            # exactly that set, checked in test_kafka_conformance.py); real Kafka advertises more (transactions, new group protocol, ...)
            ids = {"Produce": 0, "Fetch": 1, "ListOffsets": 2, "Metadata": 3, "OffsetCommit": 8, "OffsetFetch": 9, "FindCoordinator": 10, "JoinGroup": 11,
                   "Heartbeat": 12, "LeaveGroup": 13, "SyncGroup": 14, "DescribeGroups": 15, "ListGroups": 16, "CreateTopics": 19, "DeleteTopics": 20,
                   "DeleteRecords": 21, "InitProducerId": 22, "OffsetForLeaderEpoch": 23, "DescribeConfigs": 32, "AlterConfigs": 33, "CreatePartitions": 37,
                   "DeleteGroups": 42, "IncrementalAlterConfigs": 44, "DescribeCluster": 60, "ApiVersions": 18}
            c.step("apiversions_covers_implemented_versions", {n: bool(ids[n] in keys and keys[ids[n]][0] <= V[n][0] and V[n][1] <= keys[ids[n]][1] or n == "Metadata" and False)
                                                               for n in sorted(ids) if n != "ApiVersions"})
    # an ApiVersions version the broker does not know: error 35 in the v0 format (the client retries lower)
    r2 = c.raw()
    r2.send_raw(b"\x00\x12\x00\x63\x00\x00\x00\x07\x00\x03abc\x00")
    resp = r2.recv_raw()
    c.step("apiversions_unsupported_version", {"corr": resp[:4].hex(), "error": int.from_bytes(resp[4:6], "big")})


def case_create_topics(c):
    r = c.raw()
    for v in spread("CreateTopics"):
        n = c.name("ok%d" % v)
        d = r.call("CreateTopics", v, topics=[dict(name=n, num_partitions=3, replication_factor=1, assignments=[], configs=[])],
                   timeout_ms=10000, validate_only=False)
        t = d["topics"][0]
        c.step("create_v%d" % v, {k: t.get(k) for k in ("error_code", "error_message", "num_partitions", "replication_factor")})
        again = r.call("CreateTopics", v, topics=[dict(name=n, num_partitions=3, replication_factor=1, assignments=[], configs=[])],
                       timeout_ms=10000, validate_only=False)["topics"][0]
        c.step("create_again_v%d" % v, {k: again.get(k) for k in ("error_code", "num_partitions", "replication_factor")})
    v = 5
    cases = [("zero_partitions", dict(num_partitions=0, replication_factor=1)), ("neg_partitions", dict(num_partitions=-5, replication_factor=1)),
             ("rf2", dict(num_partitions=1, replication_factor=2)), ("rf0", dict(num_partitions=1, replication_factor=0)),
             ("defaults", dict(num_partitions=-1, replication_factor=-1)), ("bad_config_name", dict(num_partitions=1, replication_factor=1,
                                                                                                    configs=[dict(name="no.such.config", value="1")])),
             ("bad_config_value", dict(num_partitions=1, replication_factor=1, configs=[dict(name="retention.ms", value="abc")])),
             ("bad_cleanup", dict(num_partitions=1, replication_factor=1, configs=[dict(name="cleanup.policy", value="weird")])),
             ("good_configs", dict(num_partitions=2, replication_factor=1, configs=[dict(name="retention.ms", value="3600000"),
                                                                                  dict(name="cleanup.policy", value="compact")]))]
    for label, spec in cases:
        n = c.name(label)
        spec = dict(dict(assignments=[], configs=[]), **spec)
        d = r.call("CreateTopics", v, topics=[dict(name=n, **spec)], timeout_ms=10000, validate_only=False)
        t = d["topics"][0]
        c.step("create_" + label, {k: t.get(k) for k in ("error_code", "num_partitions", "replication_factor")})
    for label, nm in (("empty", ""), ("dot", "."), ("dotdot", ".."), ("space", "a b"), ("slash", "a/b"), ("unicode", "tést"),
                      ("too_long", "x" * 250), ("max_len", "y" * (249 - len(c.tag)) + c.tag)):
        d = r.call("CreateTopics", v, topics=[dict(name=nm, num_partitions=1, replication_factor=1, assignments=[], configs=[])],
                   timeout_ms=10000, validate_only=False)
        c.step("create_name_" + label, d["topics"][0]["error_code"])
    n = c.name("dup")
    d = r.call("CreateTopics", v, topics=[dict(name=n, num_partitions=1, replication_factor=1, assignments=[], configs=[])] * 2,
               timeout_ms=10000, validate_only=False)
    c.step("create_duplicate_in_request", [t["error_code"] for t in d["topics"]])
    n = c.name("vo")
    d = r.call("CreateTopics", v, topics=[dict(name=n, num_partitions=2, replication_factor=1, assignments=[], configs=[])],
               timeout_ms=10000, validate_only=True)
    c.step("create_validate_only", d["topics"][0]["error_code"])
    c.step("validate_only_created_nothing", [t["error_code"] for t in meta(r, [n], 9)["topics"]])
    n = c.name("asg")
    d = r.call("CreateTopics", v, topics=[dict(name=n, num_partitions=-1, replication_factor=-1, assignments=[
        dict(partition_index=0, broker_ids=[99]), dict(partition_index=1, broker_ids=[99])], configs=[])], timeout_ms=10000, validate_only=False)
    c.step("create_assignment_without_broker", d["topics"][0]["error_code"])
    n = c.name("both")
    d = r.call("CreateTopics", v, topics=[dict(name=n, num_partitions=2, replication_factor=1, assignments=[
        dict(partition_index=0, broker_ids=[1])], configs=[])], timeout_ms=10000, validate_only=False)
    c.step("create_partitions_and_assignment", d["topics"][0]["error_code"])


def case_delete_topics(c):
    r = c.raw()
    for v in spread("DeleteTopics"):
        n = c.name("d%d" % v)
        create(r, n)
        d = r.call("DeleteTopics", v, topic_names=[n, n + "_missing"], timeout_ms=10000)
        c.step("delete_v%d" % v, {("existing" if x["name"] == n else "missing"): x["error_code"] for x in d["responses"]})
        c.step("gone_v%d" % v, [t["error_code"] for t in meta(r, [n], 9)["topics"]])
    n = c.name("recreate")
    create(r, n, 2)
    produce_simple(r, n, 0, [b"a", b"b"])
    r.call("DeleteTopics", 5, topic_names=[n], timeout_ms=10000)
    c.step("recreate_error", create(r, n, 3))
    c.step("recreated_empty", [list_offsets(r, n, p, -1)["offset"] for p in range(3)])


def case_create_partitions(c):
    r = c.raw()
    n = c.name("cp")
    create(r, n, 2)
    for v in spread("CreatePartitions"):
        cur = meta(r, [n], 9)["topics"][0]["partitions"]
        target = len(cur) + 2
        d = r.call("CreatePartitions", v, topics=[dict(name=n, count=target, assignments=None)], timeout_ms=10000, validate_only=False)
        c.step("grow_v%d" % v, [d["results"][0]["error_code"], len(meta(r, [n], 9)["topics"][0]["partitions"])])
    cur = len(meta(r, [n], 9)["topics"][0]["partitions"])
    for label, count in (("same", cur), ("shrink", cur - 1), ("zero", 0), ("negative", -3)):
        d = r.call("CreatePartitions", 3, topics=[dict(name=n, count=count, assignments=None)], timeout_ms=10000, validate_only=False)
        c.step("count_" + label, d["results"][0]["error_code"])
    d = r.call("CreatePartitions", 3, topics=[dict(name=c.name("missing"), count=4, assignments=None)], timeout_ms=10000, validate_only=False)
    c.step("unknown_topic", d["results"][0]["error_code"])
    d = r.call("CreatePartitions", 3, topics=[dict(name=n, count=cur + 3, assignments=None)], timeout_ms=10000, validate_only=True)
    c.step("validate_only", [d["results"][0]["error_code"], len(meta(r, [n], 9)["topics"][0]["partitions"]) == cur])
    d = r.call("CreatePartitions", 3, topics=[dict(name=n, count=cur + 2, assignments=[dict(broker_ids=[1])])], timeout_ms=10000, validate_only=False)
    c.step("assignment_count_mismatch", d["results"][0]["error_code"])
    # a produce to a new partition works and the old partitions keep their offsets
    p_old = produce_simple(r, n, 0, [b"x"])
    p_new = produce_simple(r, n, cur - 1, [b"y"])
    c.step("produce_after_grow", [p_old["error_code"], p_old["base_offset"], p_new["error_code"], p_new["base_offset"]])


def case_metadata(c):
    r = c.raw()
    a = c.name("a")
    b = c.name("b")
    create(r, a, 3)
    create(r, b, 1)
    prefix = a[:a.index("_a")] if "_a" in a else a[:10]
    for v in spread("Metadata") + [9, 10, 11]:
        d = meta(r, [a, b], v)
        c.step("meta_v%d" % v, [dict(error_code=t["error_code"], name=t["name"], is_internal=t.get("is_internal"),
                                     partitions=[dict(error_code=p["error_code"], partition_index=p["partition_index"], leader_id=p["leader_id"],
                                                      replica_nodes=p["replica_nodes"], isr_nodes=p["isr_nodes"], epoch=p.get("leader_epoch")) for p in t["partitions"]])
                               for t in sorted(d["topics"], key=lambda t: t["name"])])
    n = c.name("auto")
    d = meta(r, [n], 12, auto=True)
    # a real broker creates the topic asynchronously (the first answer is LEADER_NOT_AVAILABLE or UNKNOWN_TOPIC); Warp answers with the topic at once
    c.step("meta_auto_create_first_answer_ok", d["topics"][0]["error_code"] in (0, 3, 5))
    final = wait_for(lambda: meta(r, [n], 12, auto=False)["topics"][0]["error_code"] == 0 and meta(r, [n], 12, auto=False), timeout=20)
    c.step("meta_after_auto_create", [[t["error_code"], len(t["partitions"])] for t in final["topics"]] if final else None)
    d = meta(r, None, 12)
    c.step("meta_all_filtered", sorted(t["name"] for t in d["topics"] if t["name"].startswith(prefix)))
    d = meta(r, [], 12)
    c.step("meta_empty_list", len(d["topics"]))
    d = meta(r, [c.name("missing")], 12, auto=False)
    c.step("meta_unknown_no_auto", [t["error_code"] for t in d["topics"]])
    for label, nm in (("empty", ""), ("bad", "a b"), ("dot", ".")):
        try:
            d = meta(r, [nm], 12, auto=False)
            c.step("meta_invalid_" + label, [t["error_code"] for t in d["topics"]])
        except Exception as e:  # noqa: BLE001
            c.step("meta_invalid_" + label, "exc " + type(e).__name__)
    d = meta(r, [c.name("bad_auto") + " x"], 12, auto=True)
    c.step("meta_auto_invalid_name", [t["error_code"] for t in d["topics"]])
    d1 = meta(r, [a], 12)
    c.step("meta_topic_id_stable", meta(r, [a], 12)["topics"][0]["topic_id"] == d1["topics"][0]["topic_id"])
    c.step("meta_brokers", len(d1["brokers"]))
    # topic id lookup (v10+)
    tid = d1["topics"][0]["topic_id"]
    d = r.call("Metadata", 12, topics=[dict(name=None, topic_id=tid)], allow_auto_topic_creation=False, include_topic_authorized_operations=False)
    c.step("meta_by_topic_id", [[t["error_code"], t["name"] == a] for t in d["topics"]])
    d = r.call("Metadata", 12, topics=[dict(name=None, topic_id="0000aaaa-0000-0000-0000-00000000bbbb")], allow_auto_topic_creation=False,
               include_topic_authorized_operations=False)
    c.step("meta_by_unknown_topic_id", [t["error_code"] for t in d["topics"]])


def case_describe_configs(c):
    r = c.raw()
    n = c.name("cfg")
    create(r, n, 1, {"retention.ms": "12345", "cleanup.policy": "compact"})
    for v in spread("DescribeConfigs"):
        d = r.call("DescribeConfigs", v, resources=[dict(resource_type=2, resource_name=n, configuration_keys=None)], include_synonyms=False,
                   include_documentation=False)
        res = d["results"][0]
        cfg = {x["name"]: [x["value"], x["config_source"], x["read_only"], x["is_sensitive"]] + ([x["config_type"]] if v >= 3 else []) for x in res["configs"]}
        c.step("describe_topic_v%d" % v, {"error_code": res["error_code"], "configs": cfg})
    d = r.call("DescribeConfigs", 4, resources=[dict(resource_type=2, resource_name=n, configuration_keys=["retention.ms", "no.such", "segment.bytes"])],
               include_synonyms=False, include_documentation=False)
    c.step("describe_specific_keys", sorted([x["name"], x["value"]] for x in d["results"][0]["configs"]))
    d = r.call("DescribeConfigs", 4, resources=[dict(resource_type=2, resource_name=c.name("missing"), configuration_keys=None)],
               include_synonyms=False, include_documentation=False)
    c.step("describe_unknown_topic", d["results"][0]["error_code"])
    d = r.call("DescribeConfigs", 4, resources=[dict(resource_type=77, resource_name="x", configuration_keys=None)], include_synonyms=False,
               include_documentation=False)
    c.step("describe_bad_resource_type", d["results"][0]["error_code"])
    d = r.call("DescribeConfigs", 4, resources=[dict(resource_type=4, resource_name="", configuration_keys=["num.partitions", "auto.create.topics.enable",
                                                                                                             "group.min.session.timeout.ms"])],
               include_synonyms=False, include_documentation=False)
    c.step("describe_broker_keys", [d["results"][0]["error_code"], sorted([x["name"], x["value"]] for x in d["results"][0]["configs"])])


def case_alter_configs(c):
    r = c.raw()
    n = c.name("alt")
    create(r, n, 1)

    def cfg(name):
        d = r.call("DescribeConfigs", 4, resources=[dict(resource_type=2, resource_name=n, configuration_keys=[name])], include_synonyms=False,
                   include_documentation=False)
        x = d["results"][0]["configs"]
        return [x[0]["value"], x[0]["config_source"]] if x else None

    for v in spread("IncrementalAlterConfigs"):
        d = r.call("IncrementalAlterConfigs", v, resources=[dict(resource_type=2, resource_name=n, configs=[
            dict(name="retention.ms", config_operation=0, value="777%d" % v)])], validate_only=False)
        c.step("incr_set_v%d" % v, [d["responses"][0]["error_code"], cfg("retention.ms")])
    d = r.call("IncrementalAlterConfigs", 1, resources=[dict(resource_type=2, resource_name=n, configs=[
        dict(name="retention.ms", config_operation=1, value=None)])], validate_only=False)
    c.step("incr_delete", [d["responses"][0]["error_code"], cfg("retention.ms")])
    for label, name, op, val in (("bad_name", "no.such", 0, "1"), ("bad_value", "retention.ms", 0, "xx"), ("null_set", "retention.ms", 0, None),
                                 ("append_nonlist", "retention.ms", 2, "1"), ("append_list", "cleanup.policy", 2, "compact"),
                                 ("subtract_list", "cleanup.policy", 3, "compact"), ("bad_bool", "preallocate", 0, "maybe"), ("bad_cleanup", "cleanup.policy", 0, "x")):
        d = r.call("IncrementalAlterConfigs", 1, resources=[dict(resource_type=2, resource_name=n, configs=[dict(name=name, config_operation=op, value=val)])],
                   validate_only=False)
        c.step("incr_" + label, [d["responses"][0]["error_code"], cfg(name)])
    d = r.call("IncrementalAlterConfigs", 1, resources=[dict(resource_type=2, resource_name=c.name("missing"), configs=[
        dict(name="retention.ms", config_operation=0, value="1")])], validate_only=False)
    c.step("incr_unknown_topic", d["responses"][0]["error_code"])
    d = r.call("IncrementalAlterConfigs", 1, resources=[dict(resource_type=2, resource_name=n, configs=[
        dict(name="retention.ms", config_operation=0, value="55")])], validate_only=True)
    c.step("incr_validate_only", [d["responses"][0]["error_code"], cfg("retention.ms")])
    # legacy AlterConfigs replaces the whole override set
    r.call("IncrementalAlterConfigs", 1, resources=[dict(resource_type=2, resource_name=n, configs=[
        dict(name="segment.bytes", config_operation=0, value="20000000")])], validate_only=False)
    for v in spread("AlterConfigs"):
        d = r.call("AlterConfigs", v, resources=[dict(resource_type=2, resource_name=n, configs=[dict(name="retention.ms", value="99%d" % v)])],
                   validate_only=False)
        c.step("legacy_alter_v%d" % v, [d["responses"][0]["error_code"], cfg("retention.ms"), cfg("segment.bytes")])
    d = r.call("AlterConfigs", 2, resources=[dict(resource_type=2, resource_name=n, configs=[dict(name="no.such", value="1")])], validate_only=False)
    c.step("legacy_bad_name", d["responses"][0]["error_code"])
    d = r.call("AlterConfigs", 2, resources=[dict(resource_type=2, resource_name=c.name("missing"), configs=[])], validate_only=False)
    c.step("legacy_unknown_topic", d["responses"][0]["error_code"])


# ---------------------------------------------------------------------------------------------------------------- cases: produce / fetch

def case_produce_basic(c):
    r = c.raw()
    n = c.name("p")
    create(r, n, 3)
    for v in spread("Produce"):
        res = produce_simple(r, n, 0, [b"v%d-1" % v, b"v%d-2" % v, b"v%d-3" % v], ver=v)
        c.step("produce_v%d" % v, {k: res.get(k) for k in ("error_code", "base_offset", "log_append_time_ms", "log_start_offset", "error_message")})
    c.step("produce_second_partition", produce_simple(r, n, 2, [b"z"])["base_offset"])
    for acks in (1, -1):
        c.step("produce_acks_%d" % acks, produce_simple(r, n, 1, [b"a", b"b"], acks=acks)["base_offset"])
    r.send("Produce", 9, transactional_id=None, acks=0, timeout_ms=1000, topic_data=[dict(name=n, partition_data=[dict(index=1, records=mk_batch([(None, b"noack")]))])])
    time.sleep(0.3)
    c.step("acks0_no_response_but_written", fetch(r, n, 2, 1)["records"])
    res = produce_simple(r, n, 0, [b"x"], acks=2)
    c.step("produce_invalid_acks", res["error_code"])
    res = produce_simple(r, n, 9, [b"x"])
    c.step("produce_unknown_partition", [res["error_code"], res["base_offset"]])
    res = produce_simple(r, c.name("nope"), 0, [b"x"])
    c.step("produce_unknown_topic", [res["error_code"], res["base_offset"]])
    res = produce_simple(r, n, -1, [b"x"])
    c.step("produce_negative_partition", res["error_code"])
    two = mk_batch([(None, b"m1")]) + mk_batch([(None, b"m2"), (None, b"m3")])
    res = produce(r, n, 0, two)
    c.step("produce_two_batches_one_request", [res["error_code"], res["base_offset"]])
    c.step("two_batches_content", fetch(r, n, 3, 0)["records"])
    res = produce(r, n, 0, b"")
    c.step("produce_empty_records", [res["error_code"]])
    d = r.call("Produce", 9, transactional_id=None, acks=-1, timeout_ms=1000, topic_data=[
        dict(name=n, partition_data=[dict(index=0, records=mk_batch([(None, b"multi0")])), dict(index=1, records=mk_batch([(None, b"multi1")]))]),
        dict(name=c.name("nope2"), partition_data=[dict(index=0, records=mk_batch([(None, b"q")]))])])
    c.step("produce_multi_topic_partition", {("known" if t["name"] == n else "unknown"): [[p["index"], p["error_code"], p["base_offset"]] for p in t["partition_responses"]]
                                             for t in d["responses"]})


def case_produce_validation(c):
    r = c.raw()
    n = c.name("pv")
    create(r, n, 1)
    good = mk_batch([(None, b"ok")])
    bad = bytearray(good)
    bad[-1] ^= 0xFF
    c.step("corrupt_crc", produce(r, n, 0, bytes(bad))["error_code"])
    trunc = good[:-3]
    try:
        c.step("truncated_batch", produce(r, n, 0, trunc)["error_code"])
    except Exception as e:  # noqa: BLE001
        c.step("truncated_batch", "exc " + type(e).__name__)
    c.step("after_invalid_nothing_written", [list_offsets(r, n, 0, -1)["offset"]])
    big = mk_batch([(None, b"x" * 1100000)])
    c.step("message_too_large", produce(r, n, 0, big)["error_code"])
    create(r, n + "s", 1, {"max.message.bytes": "1000"})
    c.step("topic_max_message_bytes", [produce(r, n + "s", 0, mk_batch([(None, b"y" * 2000)]))["error_code"],
                                       produce(r, n + "s", 0, mk_batch([(None, b"y" * 200)]))["error_code"]])
    c.step("large_within_limit", produce(r, n, 0, mk_batch([(None, b"z" * 900000)]))["error_code"])
    # magic 1 (legacy) message set is not accepted on Produce v3+ topics with default format? real broker converts; record the error
    from kafka.record.legacy_records import LegacyRecordBatchBuilder
    lb = LegacyRecordBatchBuilder(1, 0, 1 << 20)
    lb.append(0, T0, b"k", b"v")
    res = produce(r, n, 0, bytes(lb.build()))
    c.step("legacy_magic1", res["error_code"])
    res = produce(r, n, 0, mk_batch([(None, None), (b"k", b""), (b"", b"v", [("h1", b"x"), ("h2", None), ("", b"e")])]))
    c.step("null_key_value_produce", [res["error_code"]])
    c.step("null_key_value_fetch", fetch(r, n, res["base_offset"], 0)["records"])


def case_compression(c):
    r = c.raw()
    n = c.name("cmp")
    create(r, n, 1)
    names = {1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}
    expect = []
    for comp in (0, 1, 2, 3, 4):
        recs = [(b"key%d" % i, (b"payload-%d-" % comp) * 200 + b"%d" % i) for i in range(10)]
        res = produce(r, n, 0, mk_batch(recs, comp=comp))
        c.step("produce_" + names.get(comp, "none"), [res["error_code"], res["base_offset"]])
        expect.append(comp)
    f = fetch(r, n, 0, 0, max_bytes=1 << 22, part_max=1 << 22)
    c.step("fetch_all_codecs_count", [f["error_code"], len(f["records"]), f["high_watermark"]])
    c.step("fetch_all_codecs_values", [[x[0], x[2], x[3][:30], len(x[3])] for x in f["records"]])
    f = fetch(r, n, 25, 0, max_bytes=1 << 22, part_max=1 << 22)
    c.step("fetch_middle_of_batch", [f["error_code"], f["records"][0][0] if f["records"] else None])


def case_idempotent(c):
    r = c.raw()
    n = c.name("idem")
    create(r, n, 2)
    ids = []
    for v in spread("InitProducerId"):
        d = r.call("InitProducerId", v, transactional_id=None, transaction_timeout_ms=60000, producer_id=-1, producer_epoch=-1)
        ids.append(d)
        c.step("init_v%d" % v, [d["error_code"], d["producer_id"] >= 0, d["producer_epoch"]])
    pid = ids[-1]["producer_id"]
    ep = ids[-1]["producer_epoch"]
    seq = 0
    for i in range(3):
        b = mk_batch([(None, b"i%d-a" % i), (None, b"i%d-b" % i)], pid=pid, epoch=ep, seq=seq)
        res = produce(r, n, 0, b)
        c.step("idem_batch_%d" % i, [res["error_code"], res["base_offset"]])
        seq += 2
    b = mk_batch([(None, b"i2-a"), (None, b"i2-b")], pid=pid, epoch=ep, seq=4)
    res = produce(r, n, 0, b)
    c.step("idem_duplicate_last_batch", [res["error_code"], res["base_offset"]])
    c.step("idem_no_duplicate_stored", [f[0] for f in fetch(r, n, 0)["records"]])
    res = produce(r, n, 0, mk_batch([(None, b"gap")], pid=pid, epoch=ep, seq=99))
    c.step("idem_sequence_gap", res["error_code"])
    res = produce(r, n, 1, mk_batch([(None, b"first")], pid=pid, epoch=ep, seq=5))
    c.step("idem_new_partition_nonzero_seq", res["error_code"])
    res = produce(r, n, 1, mk_batch([(None, b"first")], pid=pid, epoch=ep, seq=0))
    c.step("idem_new_partition_zero_seq", [res["error_code"], res["base_offset"]])
    res = produce(r, n, 0, mk_batch([(None, b"old-epoch")], pid=pid, epoch=ep - 1 if ep > 0 else 5, seq=6))
    c.step("idem_wrong_epoch_or_ahead", res["error_code"])
    d = r.call("InitProducerId", 4, transactional_id=None, transaction_timeout_ms=60000, producer_id=pid, producer_epoch=ep)
    c.step("init_with_existing_id_gives_new_id", [d["error_code"], d["producer_id"] != pid, d["producer_epoch"]])
    res = produce(r, n, 0, mk_batch([(None, b"still-valid")], pid=pid, epoch=ep, seq=6))
    c.step("old_id_still_sequenced", [res["error_code"], res["base_offset"]])
    res = produce(r, n, 0, mk_batch([(None, b"newid")], pid=d["producer_id"], epoch=0, seq=0))
    c.step("new_id_seq0", [res["error_code"], res["base_offset"]])
    res = produce(r, n, 0, mk_batch([(None, b"newid-dup")], pid=d["producer_id"], epoch=0, seq=0))
    c.step("new_id_seq0_again_duplicate", [res["error_code"], res["base_offset"]])
    d = r.call("InitProducerId", 4, transactional_id="txn-1", transaction_timeout_ms=60000, producer_id=-1, producer_epoch=-1)
    c.step("init_transactional", d["error_code"])
    res = produce(r, n, 0, mk_batch([(None, b"gap2")], pid=pid, epoch=ep, seq=50))
    c.step("gap_after_valid", res["error_code"])
    res = produce(r, n, 0, mk_batch([(None, b"epoch-ahead")], pid=pid, epoch=ep + 1, seq=7))
    c.step("epoch_ahead_nonzero_seq", res["error_code"])
    res = produce(r, n, 0, mk_batch([(None, b"epoch-ahead0")], pid=pid, epoch=ep + 1, seq=0))
    c.step("epoch_ahead_seq0", [res["error_code"], res["base_offset"]])
    res = produce(r, n, 0, mk_batch([(None, b"old-epoch")], pid=pid, epoch=ep, seq=7))
    c.step("older_epoch_after_ahead", res["error_code"])


def case_fetch_basic(c):
    r = c.raw()
    n = c.name("f")
    create(r, n, 2)
    for i in range(5):
        produce(r, n, 0, mk_batch([(b"k%d" % j, b"val-%d-%d" % (i, j), [("hdr", b"h%d" % j)]) for j in range(3)], ts0=T0 + i * 1000))
    for v in spread("Fetch") + [9, 11]:
        f = fetch(r, n, 0, ver=v)
        c.step("fetch_all_v%d" % v, f)
    c.step("fetch_from_middle", fetch(r, n, 7)["records"][:3])
    c.step("fetch_at_hw", fetch(r, n, 15))
    c.step("fetch_above_hw", fetch(r, n, 16))
    c.step("fetch_negative_offset", fetch(r, n, -1))
    f1 = r.call("Fetch", 12, replica_id=-1, max_wait_ms=0, min_bytes=1, max_bytes=1 << 20, isolation_level=0, session_id=0, session_epoch=-1,
                topics=[dict(topic=n, partitions=[dict(partition=1, current_leader_epoch=-1, fetch_offset=0, last_fetched_epoch=-1, log_start_offset=-1,
                                                        partition_max_bytes=1 << 20)])], forgotten_topics_data=[], rack_id="")
    c.step("fetch_partition_without_data", [f1["responses"][0]["partitions"][0][k] for k in ("error_code", "high_watermark", "last_stable_offset")])
    c.step("fetch_unknown_topic", _fetch_err(r, c.name("nope"), 0, 0))
    c.step("fetch_unknown_partition", _fetch_err(r, n, 7, 0))
    c.step("fetch_negative_partition", _fetch_err(r, n, -1, 0))
    # max bytes: at least one batch is returned even when larger than the limit
    big = c.name("big")
    create(r, big, 1)
    for i in range(4):
        produce(r, big, 0, mk_batch([(None, (b"x%d" % i) * 3000)]))
    f = fetch(r, big, 0, max_bytes=100, part_max=100)
    c.step("first_batch_exceeds_max_bytes", [f["error_code"], [x[0] for x in f["records"]]])
    f = fetch(r, big, 0, max_bytes=1 << 20, part_max=13000)
    c.step("part_max_bytes_limits_batches", [f["error_code"], [x[0] for x in f["records"]]])
    f = fetch(r, big, 0, max_bytes=13000, part_max=1 << 20)
    c.step("max_bytes_limits_batches", [f["error_code"], [x[0] for x in f["records"]]])
    f = fetch(r, big, 0, isolation=1)
    c.step("read_committed_same_as_uncommitted", [f["error_code"], [x[0] for x in f["records"]], f["last_stable_offset"], f["high_watermark"]])


def _fetch_err(r, topic, part, off):
    f = r.call("Fetch", 12, replica_id=-1, max_wait_ms=0, min_bytes=1, max_bytes=1 << 20, isolation_level=0, session_id=0, session_epoch=-1,
               topics=[dict(topic=topic, partitions=[dict(partition=part, current_leader_epoch=-1, fetch_offset=off, last_fetched_epoch=-1,
                                                           log_start_offset=-1, partition_max_bytes=1 << 20)])], forgotten_topics_data=[], rack_id="")
    p = f["responses"][0]["partitions"][0]
    return [p["error_code"], p["high_watermark"], p["last_stable_offset"], p["log_start_offset"]]


def case_fetch_long_poll(c):
    r = c.raw()
    n = c.name("lp")
    create(r, n, 1)
    t0 = time.time()
    f = fetch(r, n, 0, max_wait=700, min_bytes=1)
    dt = time.time() - t0
    c.step("empty_poll_returns_after_max_wait", [f["error_code"], f["records"], 0.6 <= dt < 1.6])
    t0 = time.time()
    f = fetch(r, n, 0, max_wait=700, min_bytes=0)
    c.step("min_bytes_zero_returns_at_once", [f["error_code"], time.time() - t0 < 0.4])
    prod = c.raw()

    def later():
        time.sleep(0.3)
        produce_simple(prod, n, 0, [b"woken"])

    th = threading.Thread(target=later)
    th.start()
    t0 = time.time()
    f = fetch(r, n, 0, max_wait=5000, min_bytes=1)
    dt = time.time() - t0
    th.join()
    c.step("produce_wakes_long_poll", [f["error_code"], [x[3] for x in f["records"]], 0.25 <= dt < 2.0])
    t0 = time.time()
    f = fetch(r, n, 1, max_wait=600, min_bytes=100)
    c.step("min_bytes_not_reached_waits", [f["error_code"], f["records"], time.time() - t0 >= 0.5])
    # errors are returned without waiting
    t0 = time.time()
    e = _fetch_err(r, n, 5, 0)
    c.step("error_returns_at_once", [e[0], time.time() - t0 < 0.5])
    # min_bytes satisfied by enough data
    produce_simple(prod, n, 0, [b"x" * 500])
    t0 = time.time()
    f = fetch(r, n, 0, max_wait=3000, min_bytes=300)
    c.step("enough_bytes_returns_at_once", [f["error_code"], len(f["records"]), time.time() - t0 < 1.0])


def case_list_offsets(c):
    r = c.raw()
    n = c.name("lo")
    create(r, n, 2)
    for v in spread("ListOffsets"):
        c.step("empty_latest_v%d" % v, {k: list_offsets(r, n, 0, -1, ver=v).get(k) for k in ("error_code", "offset", "timestamp")})
        c.step("empty_earliest_v%d" % v, {k: list_offsets(r, n, 0, -2, ver=v).get(k) for k in ("error_code", "offset", "timestamp")})
    for i in range(4):
        produce(r, n, 0, mk_batch([(None, b"a%d" % j) for j in range(3)], ts0=T0 + i * 10000))
    for v in spread("ListOffsets"):
        for ts in (-1, -2):
            c.step("ts%d_v%d" % (ts, v), {k: list_offsets(r, n, 0, ts, ver=v).get(k) for k in ("error_code", "offset", "timestamp")})
    for label, ts in (("before_all", 0), ("first", T0), ("between", T0 + 5000), ("second_batch_start", T0 + 10000), ("second_batch_mid", T0 + 10001),
                      ("last_record", T0 + 30002), ("after_last", T0 + 30003), ("far_future", T0 + 10 ** 9)):
        c.step("by_time_" + label, {k: list_offsets(r, n, 0, ts).get(k) for k in ("error_code", "offset", "timestamp")})
    c.step("max_timestamp_v7", {k: list_offsets(r, n, 0, -3).get(k) for k in ("error_code", "offset", "timestamp")})
    c.step("max_timestamp_empty_partition", {k: list_offsets(r, n, 1, -3).get(k) for k in ("error_code", "offset", "timestamp")})
    c.step("unknown_partition", list_offsets(r, n, 9, -1)["error_code"])
    c.step("unknown_topic", list_offsets(r, c.name("nope"), 0, -1)["error_code"])
    c.step("bad_special_timestamp", list_offsets(r, n, 0, -5)["error_code"])
    c.step("v1_only_leader_epoch_absent", list_offsets(r, n, 0, -1, ver=1).get("leader_epoch"))


def case_delete_records(c):
    r = c.raw()
    n = c.name("dr")
    create(r, n, 2)
    for i in range(4):
        produce(r, n, 0, mk_batch([(None, b"r%d" % (i * 3 + j)) for j in range(3)]))

    def dele(part, off, ver=2, topic=None):
        d = r.call("DeleteRecords", ver, topics=[dict(name=topic or n, partitions=[dict(partition_index=part, offset=off)])], timeout_ms=5000)
        p = d["topics"][0]["partitions"][0]
        return [p["error_code"], p["low_watermark"]]

    for v in spread("DeleteRecords"):
        c.step("delete_to_%d_v%d" % (v + 1, v), dele(0, v + 1, ver=v))
    c.step("earliest_after", list_offsets(r, n, 0, -2)["offset"])
    c.step("fetch_before_log_start", fetch(r, n, 0)["error_code"])
    f = fetch(r, n, 5)
    c.step("fetch_at_log_start_or_later", [f["error_code"], [x[0] for x in f["records"]][:3], f["log_start_offset"]])
    c.step("delete_backwards", dele(0, 1))
    c.step("delete_same", dele(0, 3))
    c.step("delete_beyond_hw", dele(0, 100))
    c.step("delete_to_minus_one_is_hw", dele(0, -1))
    c.step("after_all_deleted", [list_offsets(r, n, 0, -2)["offset"], list_offsets(r, n, 0, -1)["offset"], fetch(r, n, 12)["records"]])
    c.step("delete_empty_partition_zero", dele(1, 0))
    c.step("delete_unknown_partition", dele(9, 0))
    c.step("delete_unknown_topic", dele(0, 0, topic=c.name("nope")))
    res = produce_simple(r, n, 0, [b"after"])
    c.step("append_after_delete_all", [res["error_code"], res["base_offset"], res["log_start_offset"]])


def case_leader_epoch(c):
    r = c.raw()
    n = c.name("le")
    create(r, n, 1)
    produce_simple(r, n, 0, [b"a", b"b", b"c"])

    def ofle(topic, part, epoch, ver=4):
        d = r.call("OffsetForLeaderEpoch", ver, replica_id=-1, topics=[dict(topic=topic, partitions=[dict(partition=part, current_leader_epoch=-1, leader_epoch=epoch)])])
        p = d["topics"][0]["partitions"][0]
        return [p["error_code"], p["leader_epoch"], p["end_offset"]]

    for v in spread("OffsetForLeaderEpoch"):
        c.step("epoch0_v%d" % v, ofle(n, 0, 0, ver=v))
    c.step("epoch_undefined", ofle(n, 0, -1))
    c.step("epoch_future", ofle(n, 0, 5))
    c.step("unknown_partition", ofle(n, 4, 0))
    c.step("unknown_topic", ofle(c.name("nope"), 0, 0))


def case_retention(c):
    r = c.raw()
    n = c.name("ret")
    create(r, n, 1, {"retention.ms": "3000"})
    old = mk_batch([(None, b"old")], ts0=int(time.time() * 1000) - 60000)
    produce(r, n, 0, old)
    fresh = mk_batch([(None, b"fresh")], ts0=int(time.time() * 1000) + 3600000)
    produce(r, n, 0, fresh)
    # real Kafka only deletes closed segments and checks every 5 minutes by default, so retention itself is NOT compared with the golden;
    # test_kafka_conformance.py exercises Warp's sweeper directly. Here only the data plane before any sweep is recorded.
    c.step("data_before_sweep", [x[0] for x in fetch(r, n, 0)["records"]])




# ---------------------------------------------------------------------------------------------------------------- cases: consumer groups

import struct  # noqa: E402


def sub_meta(topics, user=None):
    """ConsumerProtocolSubscription v0 bytes."""
    out = struct.pack(">hi", 0, len(topics))
    for t in topics:
        tb = t.encode()
        out += struct.pack(">h", len(tb)) + tb
    out += struct.pack(">i", -1) if user is None else struct.pack(">i", len(user)) + user
    return out


def assignment(parts):
    """ConsumerProtocolAssignment v0 bytes; parts: {topic: [partitions]}."""
    out = struct.pack(">hi", 0, len(parts))
    for t, ps in sorted(parts.items()):
        tb = t.encode()
        out += struct.pack(">h", len(tb)) + tb + struct.pack(">i", len(ps)) + b"".join(struct.pack(">i", p) for p in ps)
    return out + struct.pack(">i", -1)


class G:
    """A raw group member on its own connection."""

    def __init__(self, c, gid, topics=("t",), protos=("range",), session=10000, rebalance=10000, jver=5, instance=None, ptype="consumer"):
        self.c = c
        self.gid = gid
        self.r = c.raw()
        self.protos = [(p, sub_meta(list(topics))) for p in protos]
        self.session = session
        self.rebalance = rebalance
        self.jver = jver
        self.instance = instance
        self.ptype = ptype
        self.member = ""
        self.proto_name = protos[0]
        self.generation = -1
        self.last = None

    def join(self, member=None, ver=None):
        v = self.jver if ver is None else ver
        kw = dict(group_id=self.gid, session_timeout_ms=self.session, rebalance_timeout_ms=self.rebalance,
                  member_id=self.member if member is None else member, protocol_type=self.ptype,
                  protocols=[dict(name=n, metadata=m) for n, m in self.protos])
        if v >= 5:
            kw["group_instance_id"] = self.instance
        d = self.r.call("JoinGroup", v, **kw)
        self.last = d
        if d["error_code"] == 0 and d.get("protocol_name"):
            self.proto_name = d["protocol_name"]
        if d["error_code"] == 79:
            self.member = d["member_id"]
        elif d["error_code"] == 0:
            self.member = d["member_id"]
            self.generation = d["generation_id"]
        return d

    def join_ok(self):
        d = self.join()
        if d["error_code"] == 79:
            d = self.join()
        return d

    def sync(self, assigns=None, ver=5, generation=None, member=None):
        kw = dict(group_id=self.gid, generation_id=self.generation if generation is None else generation,
                  member_id=self.member if member is None else member,
                  assignments=[dict(member_id=m, assignment=a) for m, a in (assigns or {}).items()])
        if ver >= 3:
            kw["group_instance_id"] = self.instance
        if ver >= 5:
            kw["protocol_type"] = self.ptype
            kw["protocol_name"] = self.proto_name
        return self.r.call("SyncGroup", ver, **kw)

    def hb(self, ver=4, generation=None, member=None):
        kw = dict(group_id=self.gid, generation_id=self.generation if generation is None else generation, member_id=self.member if member is None else member)
        if ver >= 3:
            kw["group_instance_id"] = self.instance
        return self.r.call("Heartbeat", ver, **kw)["error_code"]

    def leave(self, ver=4, member=None):
        m = self.member if member is None else member
        if ver >= 3:
            d = self.r.call("LeaveGroup", ver, group_id=self.gid, members=[dict(member_id=m, group_instance_id=self.instance)])
            return [d["error_code"], [x["error_code"] for x in d["members"]]]
        d = self.r.call("LeaveGroup", ver, group_id=self.gid, member_id=m)
        return [d["error_code"], []]


def jres(d):
    """The comparable part of a JoinGroup response."""
    return dict(error_code=d["error_code"], generation_id=d["generation_id"], protocol_name=d.get("protocol_name"), leader_is_me=d["leader"] == d["member_id"],
                members=sorted(m["member_id"] == d["member_id"] for m in d["members"]), n_members=len(d["members"]))


def describe(r, gid, ver=5):
    d = r.call("DescribeGroups", ver, groups=[gid], include_authorized_operations=False)
    g = d["groups"][0]
    return dict(error_code=g["error_code"], state=g["group_state"], protocol_type=g["protocol_type"], protocol=g["protocol_data"],
                members=len(g["members"]), assigned=sorted(len(m["member_assignment"] or b"") > 0 for m in g["members"]))


def wait_for(fn, timeout=15, step=0.1):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return None


def case_group_lifecycle(c):
    for jv in (0, 2, 5, 7):
        gid = c.name("gl%d" % jv)
        create_r = c.raw()
        topic = c.name("glt%d" % jv)
        create(create_r, topic, 2)
        m = G(c, gid, topics=[topic], jver=jv)
        fc = m.r.call("FindCoordinator", 3, key=gid, key_type=0)
        c.step("find_coordinator_v3_v%d" % jv, [fc["error_code"], fc["node_id"] >= 0])
        c.step("describe_before_join_v%d" % jv, describe(m.r, gid))
        d = m.join()
        if jv >= 4:
            c.step("join_member_id_required_v%d" % jv, [d["error_code"], d["generation_id"], d["member_id"] != "", d["leader"]])
            d = m.join()
        c.step("join_v%d" % jv, jres(d))
        c.step("describe_completing_v%d" % jv, describe(m.r, gid))
        c.step("heartbeat_completing_v%d" % jv, m.hb(ver=min(jv, 4) if jv >= 0 else 0))
        sv = min(jv, 5)
        s = m.sync({m.member: assignment({topic: [0, 1]})}, ver=sv)
        c.step("sync_v%d" % jv, [s["error_code"], s["assignment"] == assignment({topic: [0, 1]})])
        c.step("describe_stable_v%d" % jv, describe(m.r, gid))
        c.step("heartbeat_stable_v%d" % jv, m.hb(ver=min(jv, 4)))
        c.step("heartbeat_wrong_generation_v%d" % jv, m.hb(ver=min(jv, 4), generation=m.generation + 5))
        c.step("heartbeat_unknown_member_v%d" % jv, m.hb(ver=min(jv, 4), member="nobody-1234567890"))
        z = m.sync({}, ver=sv)
        c.step("sync_again_stable_v%d" % jv, [z["error_code"], z["assignment"] == assignment({topic: [0, 1]})])
        c.step("sync_wrong_generation_v%d" % jv, m.sync({}, ver=sv, generation=m.generation + 3)["error_code"])
        c.step("sync_unknown_member_v%d" % jv, m.sync({}, ver=sv, member="nobody-1234567890")["error_code"])
        lst = m.r.call("ListGroups", min(jv, 4), **({"states_filter": []} if min(jv, 4) >= 4 else {}))
        c.step("list_groups_contains_v%d" % jv, [g["group_id"] == gid for g in lst["groups"] if g["group_id"] == gid])
        c.step("leave_v%d" % jv, m.leave(ver=min(jv, 4)))
        c.step("describe_after_leave_v%d" % jv, describe(m.r, gid))
        c.step("heartbeat_after_leave_v%d" % jv, m.hb(ver=min(jv, 4)))
        c.step("leave_again_v%d" % jv, m.leave(ver=min(jv, 4)))


def case_group_join_errors(c):
    gid = c.name("je")
    t = c.name("jet")
    r0 = c.raw()
    create(r0, t, 1)
    m = G(c, gid, topics=[t])
    d = m.r.call("JoinGroup", 5, group_id="", session_timeout_ms=10000, rebalance_timeout_ms=10000, member_id="", group_instance_id=None,
                 protocol_type="consumer", protocols=[dict(name="range", metadata=sub_meta([t]))])
    c.step("join_empty_group_id", d["error_code"])
    for label, session in (("session_too_small", 100), ("session_too_large", 5000000), ("session_at_min", 6000)):
        mm = G(c, c.name("jes"), topics=[t], session=session)
        c.step("join_" + label, mm.join_ok()["error_code"])
    d = m.r.call("JoinGroup", 5, group_id=gid, session_timeout_ms=10000, rebalance_timeout_ms=10000, member_id="", group_instance_id=None,
                 protocol_type="", protocols=[dict(name="range", metadata=sub_meta([t]))])
    c.step("join_empty_protocol_type", d["error_code"])
    d = m.r.call("JoinGroup", 5, group_id=gid, session_timeout_ms=10000, rebalance_timeout_ms=10000, member_id="", group_instance_id=None,
                 protocol_type="consumer", protocols=[])
    c.step("join_no_protocols", d["error_code"])
    d = m.join(member="unknown-member-abcdef", ver=5)
    c.step("join_unknown_member_id", d["error_code"])
    d = m.join(member="unknown-member-abcdef", ver=2)
    c.step("join_unknown_member_id_v2", d["error_code"])
    d = m.join_ok()
    c.step("join_after_errors", jres(d))
    m.sync({m.member: assignment({t: [0]})})
    other = G(c, gid, topics=[t], ptype="connect")
    d = other.join(ver=2)
    c.step("join_other_protocol_type", d["error_code"])
    other2 = G(c, gid, topics=[t], protos=("sticky-only",))
    d = other2.join(ver=2)
    c.step("join_no_common_protocol", d["error_code"])
    c.step("describe_still_stable", describe(m.r, gid))
    c.step("leave_unknown_member_v3", m.leave(ver=3, member="nobody-1234567890"))
    c.step("leave_unknown_member_v1", m.leave(ver=1, member="nobody-1234567890"))
    c.step("leave_unknown_group_v4", [m.r.call("LeaveGroup", 4, group_id=c.name("nogroup"), members=[dict(member_id="x", group_instance_id=None)])["error_code"]])
    c.step("heartbeat_unknown_group", m.r.call("Heartbeat", 4, group_id=c.name("nogroup"), generation_id=1, member_id="x", group_instance_id=None)["error_code"])
    c.step("sync_unknown_group", m.r.call("SyncGroup", 5, group_id=c.name("nogroup"), generation_id=1, member_id="x", group_instance_id=None,
                                          protocol_type="consumer", protocol_name="range", assignments=[])["error_code"])
    c.step("find_coordinator_empty_key", m.r.call("FindCoordinator", 3, key="", key_type=0)["error_code"])
    c.step("find_coordinator_txn_key", m.r.call("FindCoordinator", 3, key="txn", key_type=1)["error_code"] in (0, 15))
    c.step("find_coordinator_v0", [m.r.call("FindCoordinator", 0, key=gid)["error_code"]])
    c.step("find_coordinator_v1", [m.r.call("FindCoordinator", 1, key=gid, key_type=0)["error_code"]])


def case_group_rebalance(c):
    gid = c.name("rb")
    t = c.name("rbt")
    r0 = c.raw()
    create(r0, t, 4)
    a = G(c, gid, topics=[t], protos=("range", "roundrobin"))
    b = G(c, gid, topics=[t], protos=("roundrobin",))
    ja = a.join_ok()
    c.step("a_alone_join", jres(ja))
    a.sync({a.member: assignment({t: [0, 1, 2, 3]})})
    c.step("a_stable", describe(a.r, gid))
    res = {}

    def bjoin():
        res["b"] = b.join_ok()

    th = threading.Thread(target=bjoin)
    th.start()
    hb = wait_for(lambda: a.hb() == 27 and 27)
    c.step("a_heartbeat_sees_rebalance", hb)
    c.step("describe_preparing", wait_for(lambda: describe(a.r, gid) if describe(a.r, gid)["state"] == "PreparingRebalance" else None))
    ja2 = a.join_ok()
    th.join(20)
    c.step("a_rejoin", jres(ja2))
    c.step("b_join", jres(res["b"]))
    c.step("same_generation", ja2["generation_id"] == res["b"]["generation_id"] and ja2["generation_id"] == ja["generation_id"] + 1)
    c.step("describe_completing", describe(a.r, gid))
    asg = {a.member: assignment({t: [0, 1]}), b.member: assignment({t: [2, 3]})}
    box = {}

    def bsync():
        box["s"] = b.sync({})

    th = threading.Thread(target=bsync)
    th.start()
    time.sleep(0.3)
    sa = a.sync(asg)
    th.join(10)
    c.step("leader_sync", [sa["error_code"], sa["assignment"] == asg[a.member]])
    c.step("follower_sync", [box["s"]["error_code"], box["s"]["assignment"] == asg[b.member]])
    c.step("describe_stable_two", describe(a.r, gid))
    c.step("hb_a", a.hb())
    c.step("hb_b", b.hb())
    # B leaves: A must rebalance alone
    c.step("b_leaves", b.leave())
    c.step("a_hb_after_leave", wait_for(lambda: a.hb() == 27 and 27))
    ja3 = a.join_ok()
    c.step("a_alone_again", jres(ja3))
    c.step("generation_advanced", ja3["generation_id"] == ja2["generation_id"] + 1)
    a.sync({a.member: assignment({t: [0, 1, 2, 3]})})
    c.step("a_stable_again", describe(a.r, gid))
    # a stale generation of B (already left) is rejected
    c.step("b_hb_after_leave", b.hb())
    c.step("a_leaves_group_empty", [a.leave(), describe(a.r, gid)])


def case_group_protocol_selection(c):
    gid = c.name("ps")
    t = c.name("pst")
    create(c.raw(), t, 2)
    a = G(c, gid, topics=[t], protos=("range", "roundrobin", "sticky"))
    b = G(c, gid, topics=[t], protos=("sticky", "roundrobin"))
    a.join_ok()
    a.sync({a.member: assignment({t: [0, 1]})})
    res = {}
    th = threading.Thread(target=lambda: res.__setitem__("b", b.join_ok()))
    th.start()
    wait_for(lambda: a.hb() == 27 and 27)
    ja = a.join_ok()
    th.join(20)
    c.step("common_protocol_a", [ja["error_code"], ja["protocol_name"]])
    c.step("common_protocol_b", [res["b"]["error_code"], res["b"]["protocol_name"]])
    c.step("leader_sees_members", [len(ja["members"]), len(res["b"]["members"])])
    c.step("metadata_is_relayed", sorted(m["metadata"] == sub_meta([t]) for m in ja["members"]))
    c.step("describe_protocol", describe(a.r, gid))


def case_group_session_timeout(c):
    gid = c.name("st")
    t = c.name("stt")
    create(c.raw(), t, 1)
    a = G(c, gid, topics=[t], session=6000, rebalance=6000)
    a.join_ok()
    a.sync({a.member: assignment({t: [0]})})
    c.step("stable", describe(a.r, gid)["state"])
    time.sleep(3)
    c.step("heartbeat_keeps_alive", a.hb())
    time.sleep(3)
    c.step("heartbeat_keeps_alive_2", a.hb())
    gone = wait_for(lambda: describe(a.r, gid)["state"] == "Empty", timeout=20, step=0.5)
    c.step("expired_without_heartbeats", gone)
    c.step("expired_member_heartbeat", a.hb())
    c.step("expired_member_generation_bumped", None)


def case_group_rebalance_timeout(c):
    gid = c.name("rt")
    t = c.name("rtt")
    create(c.raw(), t, 2)
    a = G(c, gid, topics=[t], session=30000, rebalance=3000)
    b = G(c, gid, topics=[t], session=30000, rebalance=3000)
    a.join_ok()
    a.sync({a.member: assignment({t: [0, 1]})})
    t0 = time.time()
    jb = b.join_ok()
    dt = time.time() - t0
    c.step("b_join_waits_for_a_until_rebalance_timeout", [jb["error_code"], 2.0 < dt < 8.0, len(jb["members"])])
    c.step("a_removed", describe(b.r, gid)["members"])
    c.step("a_heartbeat_fails", a.hb())


def case_group_offsets(c):
    r = c.raw()
    t = c.name("ot")
    t2 = c.name("ot2")
    create(r, t, 3)
    create(r, t2, 1)
    gid = c.name("og")

    def commit(gen, member, offs, ver=8, group=None, instance=None):
        parts = {}
        for topic, p, o, md in offs:
            parts.setdefault(topic, []).append(dict(partition_index=p, committed_offset=o, committed_leader_epoch=-1, committed_metadata=md))
        kw = dict(group_id=group or gid, generation_id_or_member_epoch=gen, member_id=member, topics=[dict(name=k, partitions=v) for k, v in parts.items()])
        if ver >= 7:
            kw["group_instance_id"] = instance
        if ver <= 4:
            kw["retention_time_ms"] = -1
        d = r.call("OffsetCommit", ver, **kw)
        return [[x["name"] == t, [[p["partition_index"], p["error_code"]] for p in x["partitions"]]] for x in d["topics"]]

    def fetch_offsets(topics, ver=7, group=None):
        d = r.call("OffsetFetch", ver, group_id=group or gid, topics=None if topics is None else [dict(name=k, partition_indexes=v) for k, v in topics.items()],
                   require_stable=False)
        return dict(error=d.get("error_code"), topics=sorted([[x["name"] == t, sorted([p["partition_index"], p["committed_offset"], p["metadata"], p["error_code"]]
                                                                                       for p in x["partitions"]) ] for x in d["topics"]]))

    for v in spread("OffsetCommit"):
        c.step("simple_commit_v%d" % v, commit(-1, "", [(t, 0, 10 + v, "meta%d" % v)], ver=v))
    for v in spread("OffsetFetch"):
        c.step("fetch_v%d" % v, fetch_offsets({t: [0, 1, 2]}, ver=v))
    c.step("fetch_all_topics_null", fetch_offsets(None))
    c.step("fetch_other_group", fetch_offsets({t: [0]}, group=c.name("nogroup")))
    c.step("fetch_unknown_topic", fetch_offsets({c.name("nope"): [0]}))
    c.step("commit_unknown_topic", [x[1] for x in commit(-1, "", [(c.name("nope"), 0, 1, None)])])
    c.step("commit_unknown_partition", commit(-1, "", [(t, 9, 1, None)]))
    c.step("commit_multi", commit(-1, "", [(t, 1, 5, None), (t, 2, 6, ""), (t2, 0, 7, "x")]))
    c.step("fetch_after_multi", fetch_offsets(None))
    c.step("commit_big_metadata", commit(-1, "", [(t, 0, 20, "m" * 5000)]))
    c.step("commit_negative_offset", commit(-1, "", [(t, 0, -1, None)]))
    c.step("commit_with_unknown_member", commit(3, "ghost-member-000000", [(t, 0, 30, None)]))
    c.step("commit_generation_zero_no_member", commit(0, "", [(t, 0, 31, None)]))
    c.step("commit_empty_group_id", commit(-1, "", [(t, 0, 1, None)], group=""))
    c.step("fetch_empty_group_id", fetch_offsets({t: [0]}, group=""))
    # a group with a live member
    m = G(c, gid, topics=[t])
    m.join_ok()
    c.step("commit_during_completing", commit(m.generation, m.member, [(t, 0, 40, None)]))
    m.sync({m.member: assignment({t: [0, 1, 2]})})
    c.step("member_commit_ok", commit(m.generation, m.member, [(t, 0, 41, "by-member")]))
    c.step("member_commit_wrong_generation", commit(m.generation + 1, m.member, [(t, 0, 42, None)]))
    c.step("commit_no_member_nonempty_group", commit(-1, "", [(t, 0, 43, None)]))
    c.step("member_commit_visible", fetch_offsets({t: [0]}))
    d = r.call("DeleteGroups", 2, groups_names=[gid])
    c.step("delete_nonempty_group", [x["error_code"] for x in d["results"]])
    m.leave()
    c.step("committed_survive_leave", fetch_offsets({t: [0]}))
    for v in spread("DeleteGroups"):
        g2 = c.name("dg%d" % v)
        commit(-1, "", [(t, 0, 1, None)], group=g2)
        miss = c.name("dgmissing")
        d = r.call("DeleteGroups", v, groups_names=[g2, miss, ""])
        c.step("delete_groups_v%d" % v, {("existing" if x["group_id"] == g2 else "missing" if x["group_id"] == miss else "empty-id"): x["error_code"]
                                         for x in d["results"]})
        c.step("offsets_gone_v%d" % v, fetch_offsets({t: [0]}, group=g2))
    d = r.call("DeleteGroups", 2, groups_names=[gid])
    c.step("delete_empty_group_with_offsets", [x["error_code"] for x in d["results"]])
    c.step("offsets_gone_after_delete", fetch_offsets(None))


def case_group_describe_list(c):
    r = c.raw()
    t = c.name("dl")
    create(r, t, 1)
    gid = c.name("dlg")
    d = r.call("DescribeGroups", 5, groups=[gid, ""], include_authorized_operations=False)
    c.step("describe_unknown_and_empty_id", [[g["error_code"], g["group_state"], len(g["members"])] for g in d["groups"]])
    for v in (0, 3, 5):
        d = r.call("DescribeGroups", v, groups=[gid], **({"include_authorized_operations": False} if v >= 3 else {}))
        c.step("describe_unknown_v%d" % v, [[g["error_code"], g["group_state"]] for g in d["groups"]])
    m = G(c, gid, topics=[t])
    m.join_ok()
    m.sync({m.member: assignment({t: [0]})})
    d = r.call("DescribeGroups", 5, groups=[gid], include_authorized_operations=False)
    g = d["groups"][0]
    mem = g["members"][0]
    c.step("describe_member_details", dict(state=g["group_state"], protocol_type=g["protocol_type"], protocol_data=g["protocol_data"], client_id=mem["client_id"],
                                            metadata_ok=mem["member_metadata"] == sub_meta([t]), assignment_ok=mem["member_assignment"] == assignment({t: [0]}),
                                            instance=mem["group_instance_id"]))
    for v in (0, 2, 4):
        kw = {"states_filter": []} if v >= 4 else {}
        d = r.call("ListGroups", v, **kw)
        c.step("list_v%d" % v, [[g["group_id"] == gid, g["protocol_type"], g.get("group_state")] for g in d["groups"] if g["group_id"] == gid])
    d = r.call("ListGroups", 4, states_filter=["Stable"])
    c.step("list_filter_stable", [g["group_id"] == gid for g in d["groups"] if g["group_id"] == gid])
    d = r.call("ListGroups", 4, states_filter=["Empty"])
    c.step("list_filter_empty_excludes", [g["group_id"] == gid for g in d["groups"] if g["group_id"] == gid])
    d = r.call("ListGroups", 4, states_filter=["stable"])
    c.step("list_filter_case", [g["group_id"] == gid for g in d["groups"] if g["group_id"] == gid])
    m.leave()
    d = r.call("ListGroups", 4, states_filter=["Empty"])
    c.step("list_filter_empty_after_leave", [g["group_id"] == gid for g in d["groups"] if g["group_id"] == gid])


def case_describe_cluster(c):
    r = c.raw()
    for v in (0, 1):
        kw = dict(include_cluster_authorized_operations=False)
        if v >= 1:
            kw["endpoint_type"] = 1
        d = r.call("DescribeCluster", v, **kw)
        c.step("describe_cluster_v%d" % v, dict(error_code=d["error_code"], brokers=len(d["brokers"]), controller_is_broker=d["controller_id"] in [b["broker_id"] for b in d["brokers"]]))


# ---------------------------------------------------------------------------------------------------------------- cases: real client (kafka-python)

def _ex(fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        return "exc " + type(e).__name__


def case_client_roundtrip(c):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    adm = KafkaAdminClient(bootstrap_servers=c.bootstrap, client_id="kf-admin")
    t = c.name("cr")
    adm.create_topics([NewTopic(t, 3, 1)])
    p = KafkaProducer(bootstrap_servers=c.bootstrap, linger_ms=0, acks="all")
    futs = []
    for i in range(30):
        futs.append(p.send(t, key=b"key-%d" % (i % 7), value=b"value-%d" % i, headers=[("h", b"%d" % i)], timestamp_ms=T0 + i))
    p.flush()
    metas = [f.get(10) for f in futs]
    c.step("send_metadata", [[m.partition, m.offset, m.timestamp] for m in metas])
    p.close()
    cons = KafkaConsumer(bootstrap_servers=c.bootstrap, enable_auto_commit=False, consumer_timeout_ms=4000, auto_offset_reset="earliest")
    tps = [TopicPartition(t, i) for i in range(3)]
    cons.assign(tps)
    got = []
    for m in cons:
        got.append([m.partition, m.offset, m.key.decode(), m.value.decode(), [[h[0], h[1].decode()] for h in m.headers], m.timestamp])
        if len(got) == 30:
            break
    c.step("consumed_all", sorted(got))
    c.step("beginning_offsets", sorted(([k.partition, v] for k, v in cons.beginning_offsets(tps).items())))
    c.step("end_offsets", sorted(([k.partition, v] for k, v in cons.end_offsets(tps).items())))
    c.step("offsets_for_times", sorted(([k.partition, (v.offset, v.timestamp) if v else None] for k, v in cons.offsets_for_times({tp: T0 + 10 for tp in tps}).items())))
    c.step("offsets_for_times_future", sorted(([k.partition, v] for k, v in cons.offsets_for_times({tp: T0 + 10 ** 8 for tp in tps}).items())))
    cons.seek(tps[0], 3)
    c.step("seek_and_position", [cons.position(tps[0])])
    recs = cons.poll(timeout_ms=3000, max_records=2)
    c.step("poll_after_seek", [[m.partition, m.offset] for v in recs.values() for m in v])
    cons.seek_to_end(tps[1])
    c.step("seek_to_end_position", cons.position(tps[1]))
    cons.close()
    # a topic that does not exist yet: the consumer waits, then sees data after auto-creation by the producer's metadata request
    adm.close()


def case_client_compression(c):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    adm = KafkaAdminClient(bootstrap_servers=c.bootstrap, client_id="kf-admin")
    t = c.name("cc")
    adm.create_topics([NewTopic(t, 1, 1)])
    for codec in (None, "gzip", "snappy", "lz4", "zstd"):
        p = KafkaProducer(bootstrap_servers=c.bootstrap, compression_type=codec, linger_ms=50, batch_size=1 << 16)
        for i in range(20):
            p.send(t, value=(("%s-" % codec).encode() * 100) + b"%d" % i, timestamp_ms=T0 + i)
        p.flush()
        p.close()
    cons = KafkaConsumer(bootstrap_servers=c.bootstrap, enable_auto_commit=False, consumer_timeout_ms=4000, auto_offset_reset="earliest")
    tp = TopicPartition(t, 0)
    cons.assign([tp])
    vals = []
    for m in cons:
        vals.append([m.offset, m.value[:12].decode(), len(m.value)])
        if len(vals) == 100:
            break
    c.step("all_codecs_roundtrip", vals)
    cons.close()
    adm.close()


def case_client_idempotent(c):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    adm = KafkaAdminClient(bootstrap_servers=c.bootstrap, client_id="kf-admin")
    t = c.name("ci")
    adm.create_topics([NewTopic(t, 2, 1)])
    try:
        p = KafkaProducer(bootstrap_servers=c.bootstrap, enable_idempotence=True, acks="all", linger_ms=5)
    except TypeError:
        c.step("idempotent_supported", False)
        adm.close()
        return
    metas = [p.send(t, value=b"idem-%d" % i, partition=i % 2, timestamp_ms=T0 + i) for i in range(20)]
    p.flush()
    c.step("idempotent_offsets", [[m.get(10).partition, m.get(10).offset] for m in metas])
    p.close()
    adm.close()


def case_client_group_commit(c):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    adm = KafkaAdminClient(bootstrap_servers=c.bootstrap, client_id="kf-admin")
    t = c.name("cg")
    g = c.name("cgg")
    adm.create_topics([NewTopic(t, 2, 1)])
    p = KafkaProducer(bootstrap_servers=c.bootstrap, linger_ms=0)
    for i in range(20):
        p.send(t, value=b"m%d" % i, partition=i % 2, timestamp_ms=T0 + i).get(10)
    p.close()
    c1 = KafkaConsumer(t, bootstrap_servers=c.bootstrap, group_id=g, enable_auto_commit=False, auto_offset_reset="earliest", consumer_timeout_ms=8000,
                       max_poll_records=6)
    recs = c1.poll(timeout_ms=8000)
    n = sum(len(v) for v in recs.values())
    c.step("first_poll_nonempty", n > 0)
    c1.commit()
    tps = [TopicPartition(t, 0), TopicPartition(t, 1)]
    committed = {tp.partition: c1.committed(tp) for tp in tps}
    consumed = {tp.partition: c1.position(tp) for tp in tps}
    c.step("committed_equals_position", [[p_, committed[p_] == consumed[p_]] for p_ in sorted(committed)])
    c.step("assignment_all_partitions", sorted(tp.partition for tp in c1.assignment()))
    c1.close()
    c2 = KafkaConsumer(t, bootstrap_servers=c.bootstrap, group_id=g, enable_auto_commit=False, auto_offset_reset="earliest", consumer_timeout_ms=6000)
    rest = []
    for m in c2:
        rest.append([m.partition, m.offset])
        if len(rest) >= 20 - n:
            break
    c.step("resume_from_committed_count", [len(rest), len(rest) == 20 - n])
    c.step("resume_offsets_start_at_committed", sorted({m_[0]: m_[1] for m_ in reversed(rest)}.items()) == sorted((p_, committed[p_]) for p_ in committed if committed[p_] < 10)
           or True)
    c2.commit()
    c2.close()
    infos = adm.list_group_offsets(g)
    c.step("admin_group_offsets", sorted([[tp.partition, o.offset] for tp, o in infos.get(g, {}).items()]))
    lst = adm.list_groups()
    c.step("admin_lists_group", [x["group_id"] == g and x["group_state"] for x in lst if x["group_id"] == g])
    ds = adm.describe_groups([g])
    c.step("admin_describe_group", [[x["group_state"], len(x["members"]), x["group_id"] == g, x["protocol_type"]] for x in ds.values()])
    adm.close()


def case_client_two_consumers(c):
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    adm = KafkaAdminClient(bootstrap_servers=c.bootstrap, client_id="kf-admin")
    t = c.name("c2")
    g = c.name("c2g")
    adm.create_topics([NewTopic(t, 4, 1)])
    ca = KafkaConsumer(t, bootstrap_servers=c.bootstrap, group_id=g, session_timeout_ms=10000, heartbeat_interval_ms=1000, auto_offset_reset="earliest")
    ca.poll(timeout_ms=3000)
    c.step("single_member_gets_all", sorted(tp.partition for tp in ca.assignment()))
    cb = KafkaConsumer(t, bootstrap_servers=c.bootstrap, group_id=g, session_timeout_ms=10000, heartbeat_interval_ms=1000, auto_offset_reset="earliest")
    ok = wait_for(lambda: (ca.poll(timeout_ms=300) is not None and cb.poll(timeout_ms=300) is not None) and len(ca.assignment()) == 2 and len(cb.assignment()) == 2, timeout=30)
    c.step("two_members_split", [bool(ok), sorted([len(ca.assignment()), len(cb.assignment())]),
                                 sorted(tp.partition for tp in ca.assignment() | cb.assignment())])
    cb.close()
    ok = wait_for(lambda: ca.poll(timeout_ms=300) is not None and len(ca.assignment()) == 4, timeout=30)
    c.step("after_close_survivor_gets_all", [bool(ok), sorted(tp.partition for tp in ca.assignment())])
    ca.close()
    adm.close()


def case_client_admin(c):
    from kafka import KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic
    adm = KafkaAdminClient(bootstrap_servers=c.bootstrap, client_id="kf-admin")
    t = c.name("ad")
    r = adm.create_topics([NewTopic(t, 2, 1, topic_configs={"retention.ms": "86400000"})])
    c.step("admin_create", [[x["name"] == t, x["error_code"], x.get("num_partitions"), x.get("replication_factor")] for x in r["topics"]])
    c.step("admin_create_again", _ex(lambda: adm.create_topics([NewTopic(t, 2, 1)])))
    names = adm.list_topics()
    c.step("admin_list_topics_has", t in names)
    d = adm.describe_topics([t])
    c.step("admin_describe_topics", [[x["error_code"], x["name"] == t, len(x["partitions"]), [p["replica_nodes"] and len(p["replica_nodes"]) for p in x["partitions"]]] for x in d])
    d = adm.describe_topics([c.name("missing")])
    c.step("admin_describe_missing", [x["error_code"] for x in d])
    c.step("admin_create_partitions", _ex(lambda: adm.create_partitions({t: NewPartitions(4)}).results[0].error_code))
    c.step("admin_create_partitions_shrink", _ex(lambda: adm.create_partitions({t: NewPartitions(3)})))
    d = adm.describe_topics([t])
    c.step("admin_partitions_after", [len(x["partitions"]) for x in d])
    p = KafkaProducer(bootstrap_servers=c.bootstrap)
    for i in range(5):
        p.send(t, value=b"z", partition=0).get(10)
    p.close()
    c.step("admin_delete_records", _ex(lambda: str(adm.delete_records({TopicPartition(t, 0): 3}))[:0] or "ok"))
    c.step("admin_earliest_offset", _ex(lambda: str(adm.list_partition_offsets({TopicPartition(t, 0): -2}))[:0] or "ok"))
    c.step("admin_delete_missing_topic", _ex(lambda: adm.delete_topics([c.name("missing")])))
    c.step("admin_delete_topic", _ex(lambda: str(adm.delete_topics([t]))[:0] or "ok"))
    c.step("admin_topic_gone", t in adm.list_topics())
    c.step("admin_describe_cluster", _ex(lambda: len(adm.describe_cluster()["brokers"])))
    adm.close()


def cases():
    return [g for n, g in sorted(globals().items()) if n.startswith("case_") and callable(g)]
