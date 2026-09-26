"""Round-trip times of small Pub/Sub operations: Google's official Pub/Sub emulator (oracle, Docker) vs Warp (native Postgres).

  python3 ps_rtt_bench.py --oracle localhost:PORT [--warp-shards 1|2] [--n 300]

Every call goes over one gRPC channel, sequentially, from the same client for both sides (no credentials). Prints median / p95 in
milliseconds per operation. The emulator keeps everything in memory (no durability, no fsync), Warp commits to Postgres: the
comparison is of two products with different guarantees, not of two implementations of the same one.
"""
import argparse
import queue
import statistics
import sys
import threading
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ps_client as C  # noqa: E402
from google.pubsub.v1 import pubsub_pb2 as pb  # noqa: E402


def bench(target, n, tag):
    c = C.PsClient(target)
    run = f"{tag}{int(time.time()) % 100000}"
    P = f"projects/rtt-{run}"
    T, S = f"{P}/topics/rtt-topic", f"{P}/subscriptions/rtt-sub"
    assert c.call("CreateTopic", {"name": T})[0] == "OK"
    assert c.call("CreateSubscription", {"name": S, "topic": T, "ack_deadline_seconds": 60})[0] == "OK"
    res = {}

    def t(name, fn, cnt=n):
        xs = []
        for i in range(cnt):
            st = time.perf_counter()
            r = fn(i)
            xs.append((time.perf_counter() - st) * 1000)
            assert r[0] == "OK", (name, r[:2])
        xs.sort()
        res[name] = (statistics.median(xs), xs[max(0, int(len(xs) * 0.95) - 1)])

    msg = {"data": "eA==" * 256}  # 768 bytes
    for i in range(30):  # warm-up
        c.call("Publish", {"topic": T, "messages": [msg]})
    while True:
        r = c.call("Pull", {"subscription": S, "max_messages": 1000, "return_immediately": True})
        if not r[2]:
            break
        c.call("Acknowledge", {"subscription": S, "ack_ids": [m["ack_id"] for m in r[2]["received_messages"]]})
    t("Publish 1 message (~1 KB)", lambda i: c.call("Publish", {"topic": T, "messages": [msg]}))
    t("Publish 10 messages", lambda i: c.call("Publish", {"topic": T, "messages": [msg] * 10}), max(20, n // 5))
    ack_ids = []
    # drain what was published, timing Pull of 1 message each
    def pull1(i):
        r = c.call("Pull", {"subscription": S, "max_messages": 1, "return_immediately": True})
        if r[2]:
            ack_ids.extend(m["ack_id"] for m in r[2]["received_messages"])
        return r
    t("Pull 1 message (available)", pull1)
    it = iter(ack_ids)
    t("Acknowledge 1 message", lambda i: c.call("Acknowledge", {"subscription": S, "ack_ids": [next(it)]}), min(n, len(ack_ids)))
    t("GetSubscription", lambda i: c.call("GetSubscription", {"subscription": S}))
    while True:  # drain the backlog so that the stream only sees what is published below
        r = c.call("Pull", {"subscription": S, "max_messages": 1000, "return_immediately": True})
        if not r[2]:
            break
        c.call("Acknowledge", {"subscription": S, "ack_ids": [m["ack_id"] for m in r[2]["received_messages"]]})
    time.sleep(0.5)
    # publish -> StreamingPull delivery latency (one open stream, message published from the same client)
    q = queue.Queue()
    stop = threading.Event()

    def gen():
        yield pb.StreamingPullRequest(subscription=S, stream_ack_deadline_seconds=60)
        while not stop.is_set():
            try:
                yield q.get(timeout=0.1)
            except queue.Empty:
                pass

    call = c.sub.StreamingPull(gen())
    got = queue.Queue()

    def reader():
        try:
            for r in call:
                for m in r.received_messages:
                    got.put((time.perf_counter(), m.ack_id))
        except Exception:  # noqa: BLE001
            pass

    threading.Thread(target=reader, daemon=True).start()
    time.sleep(1.5)
    lat = []
    for i in range(min(n, 200)):
        st = time.perf_counter()
        c.call("Publish", {"topic": T, "messages": [msg]})
        at, ack = got.get(timeout=10)
        lat.append((at - st) * 1000)
        q.put(pb.StreamingPullRequest(ack_ids=[ack]))
    lat.sort()
    res["Publish -> StreamingPull delivery"] = (statistics.median(lat), lat[int(len(lat) * 0.95) - 1])
    stop.set()
    call.cancel()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp-shards", type=int, default=1)
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    out = {}
    if a.oracle:
        out["Pub/Sub emulator (Docker)"] = bench(a.oracle, a.n, "o")
    import ps_launch_warp
    w = ps_launch_warp.PsWarp(a.warp_shards)
    try:
        out[f"Warp ({a.warp_shards} native Postgres)"] = bench(f"localhost:{w.grpc_port}", a.n, "w")
    finally:
        w.close()
    names = list(next(iter(out.values())))
    print(f"{'operation':36}" + "".join(f"{k:>38}" for k in out))
    for nm in names:
        print(f"{nm:36}" + "".join(f"{v[nm][0]:>22.2f} ms med /{v[nm][1]:>6.2f} p95" for v in out.values()))


if __name__ == "__main__":
    main()
