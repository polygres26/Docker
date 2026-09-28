"""RTT / throughput comparison of amqpwire against a real RabbitMQ with the same pika client:
   python3 amqp_rtt_bench.py --rabbit 25672 --warp PORT [--warp2 PORT] [--n 500] [--json out.json]
publish+confirm round trip, basic.get round trip, publish-to-delivery latency of an idle consumer, queue.declare+delete, pipelined publish
and consume throughput. Warp keeps everything in Postgres (a durable write per publish), RabbitMQ in its own message store."""
import argparse
import json
import statistics
import threading
import time
import uuid

import pika


def conn(port, heartbeat=60):
    return pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", port, "/", pika.PlainCredentials("guest", "guest"), heartbeat=heartbeat))


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def stats(xs):
    return {"median_ms": round(statistics.median(xs) * 1000, 3), "p95_ms": round(pct(xs, 0.95) * 1000, 3), "mean_ms": round(statistics.mean(xs) * 1000, 3)}


def bench(port, n):
    out = {}
    c = conn(port)
    ch = c.channel()
    q = "bench-" + uuid.uuid4().hex[:8]
    ch.queue_declare(q, durable=True)
    props = pika.BasicProperties(delivery_mode=2)
    body = b"x" * 100
    # warm-up
    for _ in range(50):
        ch.basic_publish("", q, body, props)
    ch.queue_purge(q)
    # publish + confirm (synchronous, persistent)
    ch.confirm_delivery()
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        ch.basic_publish("", q, body, props)
        ts.append(time.perf_counter() - t)
    out["publish_confirm"] = stats(ts)
    # basic.get + ack
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        m, p, b = ch.basic_get(q)
        ch.basic_ack(m.delivery_tag)
        ts.append(time.perf_counter() - t)
    out["get_ack"] = stats(ts)
    # queue.declare + delete
    ts = []
    for i in range(max(20, n // 10)):
        name = f"{q}-{i}"
        t = time.perf_counter()
        ch.queue_declare(name, durable=True)
        ch.queue_delete(name)
        ts.append(time.perf_counter() - t)
    out["declare_delete"] = stats(ts)
    # publish -> delivery to an idle consumer on another connection
    c2 = conn(port)
    ch2 = c2.channel()
    ch2.basic_qos(prefetch_count=10)
    lat = []
    ev = threading.Event()

    def on_msg(ch_, m, p, b):
        lat.append(time.perf_counter() - float(b))
        ch_.basic_ack(m.delivery_tag)
        ev.set()
    ch2.basic_consume(q, on_msg)
    for _ in range(min(n, 200)):
        ev.clear()
        ch.basic_publish("", q, repr(time.perf_counter()).encode(), props)
        deadline = time.time() + 5
        while not ev.is_set() and time.time() < deadline:
            c2.process_data_events(0.01)
    out["idle_consumer_latency"] = stats(lat)
    c2.close()
    # pipelined throughput
    ch.confirm_delivery() if False else None
    c3 = conn(port)
    ch3 = c3.channel()
    total = 5000
    t = time.perf_counter()
    for _ in range(total):
        ch3.basic_publish("", q, body, props)
    ch3.queue_declare(q, passive=True)          # a synchronous round trip: everything before it was processed
    tp = time.perf_counter() - t
    out["pipelined_publish_msgs_per_s"] = round(total / tp)
    ch3.basic_qos(prefetch_count=200)
    got = [0]
    t = time.perf_counter()

    def on_bulk(ch_, m, p, b):
        got[0] += 1
        if got[0] % 100 == 0 or got[0] == total:
            ch_.basic_ack(m.delivery_tag, multiple=True)
    ch3.basic_consume(q, on_bulk)
    deadline = time.time() + 60
    while got[0] < total and time.time() < deadline:
        c3.process_data_events(0.05)
    out["consume_ack_msgs_per_s"] = round(got[0] / (time.perf_counter() - t))
    c3.close()
    ch.queue_delete(q)
    c.close()
    return out


def bench10(port, n):
    """AMQP 1.0 with python-qpid-proton: send + settlement round trip, and send-to-receive latency of an idle receiver."""
    from proton import Message
    from proton.utils import BlockingConnection
    out = {}
    q = "bench10-" + uuid.uuid4().hex[:8]
    pc = conn(port)
    pc.channel().queue_declare(q, durable=True)
    c = BlockingConnection(f"amqp://guest:guest@127.0.0.1:{port}", allowed_mechs="PLAIN")
    s = c.create_sender(f"/queues/{q}")
    body = b"x" * 100
    for _ in range(50):
        s.send(Message(body=body, durable=True))
    pc.channel().queue_purge(q)
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        s.send(Message(body=body, durable=True))          # returns when the broker settled it (accepted)
        ts.append(time.perf_counter() - t)
    out["send_settle"] = stats(ts)
    pc.channel().queue_purge(q)
    r = c.create_receiver(f"/queues/{q}", credit=10)
    lat = []
    for _ in range(min(n, 200)):
        t = time.perf_counter()
        s.send(Message(body=body, durable=True))
        m = r.receive(timeout=5)
        r.accept()
        lat.append(time.perf_counter() - t)
    out["send_to_receive_accept"] = stats(lat)
    c.close()
    pc.channel().queue_delete(q)
    pc.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rabbit", type=int, required=True)
    ap.add_argument("--warp", type=int, required=True)
    ap.add_argument("--warp2", type=int)
    ap.add_argument("--n", type=int, default=500)
    ap.add_argument("--json")
    ap.add_argument("--amqp10", action="store_true", help="also measure AMQP 1.0 with python-qpid-proton")
    a = ap.parse_args()
    res = {"rabbitmq": bench(a.rabbit, a.n), "warp": bench(a.warp, a.n)}
    if a.warp2:
        res["warp_2_backends"] = bench(a.warp2, a.n)
    if a.amqp10:
        res["amqp10"] = {"rabbitmq": bench10(a.rabbit, a.n), "warp": bench10(a.warp, a.n)}
        if a.warp2:
            res["amqp10"]["warp_2_backends"] = bench10(a.warp2, a.n)
    print(json.dumps(res, indent=1))
    if a.json:
        json.dump(res, open(a.json, "w"), indent=1)


if __name__ == "__main__":
    main()
