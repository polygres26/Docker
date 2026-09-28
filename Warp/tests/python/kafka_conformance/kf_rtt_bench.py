"""RTT bench of kafkawire vs a real Apache Kafka: the same requests, one connection, sequential, median and p99 in milliseconds.
`python3 kf_rtt_bench.py --kafka-port 29092 [--n 300]` starts Warp (one and two Postgres backends) on local Postgres itself and prints a markdown
section for docs/RTT_BASELINE_2026.md. Raw protocol requests are used so the numbers are broker round trips, not client-library overhead; the
"kafka-python" rows use the real client (Producer.send().get() with acks=all, and produce-to-consume through a consumer group member)."""
import argparse
import json
import os
import statistics
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
os.environ.setdefault("WARP_TEST_PG_LOCAL", "1")

import kafka_raw_client as RC  # noqa: E402
import kf_corpus as C  # noqa: E402
import kf_harness as H  # noqa: E402


def stats(xs):
    xs = sorted(xs)
    return statistics.median(xs), xs[max(0, int(len(xs) * 0.99) - 1)]


def bench(host, port, n, tag):
    r = RC.Raw(host, port)
    t = "rtt-%s-%d" % (tag, int(time.time()))
    C.create(r, t, 1)
    out = {}
    one = C.mk_batch([(None, b"x" * 100)])
    ten = C.mk_batch([(None, b"y" * 100)] * 10)
    hundred = C.mk_batch([(None, b"z" * 100)] * 100)
    for _ in range(20):
        C.produce(r, t, 0, one)

    def timed(label, fn, count=n):
        xs = []
        for i in range(count):
            t0 = time.perf_counter()
            fn(i)
            xs.append((time.perf_counter() - t0) * 1000)
        out[label] = stats(xs)

    timed("Produce 1 record, acks=all", lambda i: C.produce(r, t, 0, one, acks=-1))
    timed("Produce 1 record, acks=1", lambda i: C.produce(r, t, 0, one, acks=1))
    timed("Produce 10 records, acks=all", lambda i: C.produce(r, t, 0, ten, acks=-1))
    timed("Produce 100 records, acks=all", lambda i: C.produce(r, t, 0, hundred, acks=-1), max(50, n // 4))
    hw = C.list_offsets(r, t, 0, -1)["offset"]
    timed("Fetch 1 batch of 10 records (existing data)", lambda i: C.fetch(r, t, max(0, hw - 11 - (i % 5) * 10), 0, max_wait=0, part_max=4000))
    timed("Fetch 100 records (existing data)", lambda i: C.fetch(r, t, max(0, hw - 110), 0, max_wait=0, part_max=1 << 20))
    timed("Fetch at the end (empty, max_wait=0)", lambda i: C.fetch(r, t, hw, 0, max_wait=0))
    timed("ListOffsets latest", lambda i: C.list_offsets(r, t, 0, -1))
    timed("Metadata (one topic)", lambda i: C.meta(r, [t], 12))
    g = "rttg-%s-%d" % (tag, int(time.time()))
    m = C.G(H.Ctx(host, port, "rtt"), g, topics=[t])
    m.join_ok()
    m.sync({m.member: C.assignment({t: [0]})})
    timed("Heartbeat", lambda i: m.hb())

    def commit(i):
        r.call("OffsetCommit", 8, group_id=g, generation_id_or_member_epoch=m.generation, member_id=m.member, group_instance_id=None,
               topics=[dict(name=t, partitions=[dict(partition_index=0, committed_offset=i, committed_leader_epoch=-1, committed_metadata=None)])])

    timed("OffsetCommit", commit)
    timed("OffsetFetch", lambda i: r.call("OffsetFetch", 7, group_id=g, topics=[dict(name=t, partition_indexes=[0])], require_stable=False))

    # end to end: a consumer's long-poll Fetch is waiting when the producer sends; latency = send -> the fetch returns the record
    prod = RC.Raw(host, port)
    cons = RC.Raw(host, port)
    e2e = []
    for i in range(max(60, n // 4)):
        nxt = C.list_offsets(cons, t, 0, -1)["offset"]
        box = {}

        def waiter():
            box["f"] = C.fetch(cons, t, nxt, 0, max_wait=5000, min_bytes=1)
            box["t"] = time.perf_counter()

        th = threading.Thread(target=waiter)
        th.start()
        time.sleep(0.05)
        t0 = time.perf_counter()
        C.produce(prod, t, 0, one, acks=-1)
        th.join()
        e2e.append((box["t"] - t0) * 1000)
    out["produce to long-polling consumer (end to end)"] = stats(e2e)
    r.call("DeleteTopics", 5, topic_names=[t], timeout_ms=10000)

    # the real client
    from kafka import KafkaConsumer, KafkaProducer
    bs = "%s:%d" % (host, port)
    tt = t + "-py"
    C.create(r, tt, 1)
    p = KafkaProducer(bootstrap_servers=bs, acks="all", linger_ms=0)
    for _ in range(20):
        p.send(tt, b"w").get(10)
    xs = []
    for i in range(n):
        t0 = time.perf_counter()
        p.send(tt, b"x" * 100).get(10)
        xs.append((time.perf_counter() - t0) * 1000)
    out["kafka-python send().get(), acks=all"] = stats(xs)
    c = KafkaConsumer(tt, bootstrap_servers=bs, group_id="rtt-py-" + tag, auto_offset_reset="latest", fetch_max_wait_ms=2000)
    c.poll(timeout_ms=3000)
    c.poll(timeout_ms=500)
    xs = []
    for i in range(max(40, n // 6)):
        t0 = time.perf_counter()
        p.send(tt, b"e2e").get(10)
        got = 0
        while not got:
            got = sum(len(v) for v in c.poll(timeout_ms=2000).values())
        xs.append((time.perf_counter() - t0) * 1000)
    out["kafka-python produce -> poll() sees it (end to end)"] = stats(xs)
    p.close()
    c.close()
    r.call("DeleteTopics", 5, topic_names=[tt], timeout_ms=10000)
    r.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka-port", type=int, default=29092)
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    import kafka_launch_warp as L
    results = {}
    results["Apache Kafka 4.3.1"] = bench("localhost", a.kafka_port, a.n, "k")
    for shards in (1, 2):
        w = L.KafkaWarp(shards)
        try:
            results["Warp, %d Postgres" % shards] = bench("localhost", w.port, a.n, "w%d" % shards)
        finally:
            w.close()
    cols = list(results)
    print("| operation | " + " | ".join("%s median / p99" % c for c in cols) + " |")
    print("|---|" + "---|" * len(cols))
    for op in results[cols[0]]:
        print("| %s | " % op + " | ".join("%.2f / %.2f" % results[c][op] for c in cols) + " |")
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    with open(os.path.join(HERE, "results", "kf-rtt.json"), "w") as f:
        json.dump(results, f, indent=1)


if __name__ == "__main__":
    main()
