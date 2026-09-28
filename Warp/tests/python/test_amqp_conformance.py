"""Warp-side tests of amqpwire (AMQP 0-9-1 on Postgres: what RabbitMQ clients speak).

* the golden corpus: amqp_conformance/golden.json.gz holds the answers of a REAL RabbitMQ 4.3 to every step of amqp_conformance/amqp_corpus.py
  (recorded twice with `amqp_harness.py record`; unstable steps dropped) over raw AMQP 0-9-1 frames: reply methods, error codes and texts, delivery
  order, tags, redelivery flags, content properties, returns, confirms, dead-lettering, TTL. Each case is replayed OFFLINE (no Docker, no RabbitMQ)
  against a real Warp on one Postgres backend and on two sharded backends. A step must answer like RabbitMQ or be a documented divergence
  (amqp_known.py, each with its reason);
* real pika clients (BlockingConnection and the asynchronous SelectConnection): publish/confirm, consume/ack/nack, prefetch, transactions,
  mandatory returns, direct reply-to RPC, consumer_cancel_notify, dead-lettering, heartbeats;
* sharding over two Postgres hosts: queues land on both, exactly one host per queue, exchanges only on the first host, a fan-out publish
  reaches queues on both hosts, the cross-host outbox re-delivers what a crashed publisher left behind;
* AMQP 1.0 on the same port (protocol-header sniffing): amqp10_conformance corpus (golden10.json.gz, raw frames against a REAL RabbitMQ 4.3: SASL, open, begin,
  attach, flow, transfer, disposition, detach, end, close, message section conversion in both directions) replayed against Warp, and the real python-qpid-proton client;
* authentication (PLAIN against WARP_AUTH_CREDENTIALS), vhosts, user_id validation, WARP_POOL_MAX_SIZE=4 starvation with idle consumers,
  restart durability, MCP data tools, metrics, concurrency (no loss, no duplicate) and heartbeats.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests, and WARP_TEST_JAR.
"""
import concurrent.futures
import gzip
import json
import os
import struct
import sys
import threading
import time
import uuid

import pika
import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "amqp_conformance"))
import amqp_harness as H  # noqa: E402
import amqp_known as K  # noqa: E402
import amqp_launch_warp as L  # noqa: E402
import amqp_raw as R  # noqa: E402
import amqp10_harness as H10  # noqa: E402
import amqp10_raw as R10  # noqa: E402

from mcp_support import ADMIN_TOKEN, call, call_json, create_endpoint, tool_names  # noqa: E402

GOLDEN = H.load_golden()
CORPUS = H.load_corpus()
STEPS = sum(len(c) for c in GOLDEN.values())
GOLDEN10 = H10.load_golden()
CORPUS10 = H10.load_corpus()
STEPS10 = sum(len(c) for c in GOLDEN10.values())


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def params(w, vhost="/", user="guest", password="guest", heartbeat=None, **kw):
    return pika.ConnectionParameters("127.0.0.1", w.port, vhost, pika.PlainCredentials(user, password), heartbeat=heartbeat,
                                     blocked_connection_timeout=30, **kw)


def blocking(w, **kw):
    return pika.BlockingConnection(params(w, **kw))


def uniq(prefix="t"):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def one():
    w = L.AmqpWarp(1)
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.AmqpWarp(2)
    yield w
    w.close()


@pytest.fixture(scope="module")
def strict():
    """Auth on (two users), a second vhost, a tiny connection pool: nothing may pin a pooled connection while consumers idle."""
    w = L.AmqpWarp(1, extra_env={"WARP_AUTH_CREDENTIALS": "alice=s3cret;bob=hunter2", "WARP_AMQPWIRE_AUTH": "true",
                                 "WARP_AMQPWIRE_VHOSTS": "/,tenant-a", "WARP_POOL_MAX_SIZE": "4", "WARP_MCP_READ_ONLY": "false"})
    yield w
    w.close()


# ---------------------------------------------------------------------------------------------
# the golden corpus
# ---------------------------------------------------------------------------------------------

def replay(w):
    actual = {c.name: H.run_case("127.0.0.1", w.port, c) for c in CORPUS}
    diffs, used_patterns, used_results = H.compare(GOLDEN, actual, CORPUS, K)
    text = "\n".join(f"{c}[{i}] ({k}) {s[:160]}\n   expected {json.dumps(e)[:500]}\n   actual   {json.dumps(a)[:500]}" for c, i, s, e, a, k in diffs[:20])
    assert not diffs, f"{len(diffs)} mismatches of {STEPS} steps\n{text}"
    assert used_patterns == set(range(len(K.MESSAGE_PATTERNS))), f"stale message patterns: {set(range(len(K.MESSAGE_PATTERNS))) - used_patterns}"
    assert used_results == set(K.RESULT_DIVERGENCES), f"stale result divergences: {set(K.RESULT_DIVERGENCES) - used_results}"
    return actual


def replay10(w):
    actual = {c.name: H10.run_case("127.0.0.1", w.port, c) for c in CORPUS10}
    diffs, used = H10.compare(GOLDEN10, actual, CORPUS10, K)
    text = "\n".join(f"{c}[{i}] ({k}) {s[:160]}\n   expected {json.dumps(e)[:600]}\n   actual   {json.dumps(a)[:600]}" for c, i, s, e, a, k in diffs[:20])
    assert not diffs, f"{len(diffs)} AMQP 1.0 mismatches of {STEPS10} steps\n{text}"
    assert used == set(K.RESULT_DIVERGENCES10), f"stale AMQP 1.0 divergences: {set(K.RESULT_DIVERGENCES10) - used}"


def test_replay_amqp10_golden_against_warp_one_and_two_backends(one, two):
    assert STEPS10 > 250
    replay10(one)
    replay10(two)
    for pg in two.pgs:
        assert sql(pg, "SELECT count(*) FROM warp_amqp_msgs")[0][0] == 0


def test_replay_golden_against_warp_one_backend(one):
    assert STEPS > 1400
    replay(one)


def test_replay_golden_against_warp_two_sharded_backends(two):
    replay(two)
    # the corpus deleted its queues and exchanges: nothing may be left on either host
    assert sql(two.pgs[0], "SELECT count(*) FROM warp_amqp_msgs")[0][0] == 0
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_amqp_msgs")[0][0] == 0
    assert sql(two.pgs[1], "SELECT count(*) FROM warp_amqp_exchanges")[0][0] == 0   # topology lives on the first host only
    assert sql(two.pgs[0], "SELECT count(*) FROM warp_amqp_bindings")[0][0] == 0


def test_golden_covers_the_scope():
    assert len(GOLDEN) >= 110
    text = json.dumps(GOLDEN)
    for needle in ["x-death", "NO_ROUTE", "PRECONDITION_FAILED", "RESOURCE_LOCKED", "ACCESS_REFUSED", "NOT_FOUND", "basic.nack", "basic.return",
                   "channel.flow", "consumer_cancel_notify", "amq.rabbitmq.reply-to", "x-max-priority", "all-with-x", "any-with-x", "tx.commit",
                   "confirm.select", "basic.recover", "x-last-death-reason", "x-first-death-queue", "original-expiration", "maxlen", "expired",
                   "rejected", "FRAME_ERROR", "UNEXPECTED_FRAME", "CHANNEL_ERROR", "NOT_IMPLEMENTED", "alternate-exchange"]:
        assert needle in text, needle
    assert sum(1 for c in GOLDEN.values() for s in c if s.get("unstable")) < 20
    assert set(K.DOCUMENTED) >= {"amqp-1.0", "mechanisms", "plugins"}


# ---------------------------------------------------------------------------------------------
# real pika clients
# ---------------------------------------------------------------------------------------------

class TestPikaBlocking:
    def test_publish_confirm_get_consume_ack(self, one):
        c = blocking(one)
        ch = c.channel()
        q = uniq("q")
        ch.queue_declare(q, durable=True)
        ch.confirm_delivery()
        for i in range(20):
            ch.basic_publish("", q, f"m{i}".encode(), pika.BasicProperties(delivery_mode=2, message_id=str(i), headers={"n": i, "s": "x"}))
        m, props, body = ch.basic_get(q)
        assert body == b"m0" and props.headers == {"n": 0, "s": "x"} and props.delivery_mode == 2 and m.message_count == 19
        ch.basic_ack(m.delivery_tag)
        got = []
        for m, props, body in ch.consume(q, auto_ack=False, inactivity_timeout=2):
            if m is None:
                break
            got.append(body)
            ch.basic_ack(m.delivery_tag)
            if len(got) == 19:
                break
        ch.cancel()
        assert got == [f"m{i}".encode() for i in range(1, 20)]
        assert ch.queue_declare(q, passive=True).method.message_count == 0
        ch.queue_delete(q)
        c.close()

    def test_topic_headers_and_fanout_routing(self, one):
        c = blocking(one)
        ch = c.channel()
        t, h, f = uniq("t"), uniq("h"), uniq("f")
        ch.exchange_declare(t, "topic", durable=True)
        ch.exchange_declare(h, "headers", durable=True)
        ch.exchange_declare(f, "fanout", durable=True)
        qs = {n: uniq(n) for n in ("star", "hash", "hdr", "fan")}
        for q in qs.values():
            ch.queue_declare(q, durable=True)
        ch.queue_bind(qs["star"], t, "a.*.c")
        ch.queue_bind(qs["hash"], t, "a.#")
        ch.queue_bind(qs["hdr"], h, "", arguments={"x-match": "any", "k": "v", "n": 1})
        ch.queue_bind(qs["fan"], f)
        ch.basic_publish(t, "a.b.c", b"1")
        ch.basic_publish(t, "a.b.c.d", b"2")
        ch.basic_publish(h, "", b"3", pika.BasicProperties(headers={"n": 1}))
        ch.basic_publish(h, "", b"4", pika.BasicProperties(headers={"n": 2}))
        ch.basic_publish(f, "whatever", b"5")

        def drain(q):
            out = []
            while True:
                m, p, b = ch.basic_get(q, auto_ack=True)
                if m is None:
                    return out
                out.append(b)
        assert drain(qs["star"]) == [b"1"]
        assert drain(qs["hash"]) == [b"1", b"2"]
        assert drain(qs["hdr"]) == [b"3"]
        assert drain(qs["fan"]) == [b"5"]
        for q in qs.values():
            ch.queue_delete(q)
        for e in (t, h, f):
            ch.exchange_delete(e)
        c.close()

    def test_mandatory_unroutable_is_returned(self, one):
        c = blocking(one)
        ch = c.channel()
        ch.confirm_delivery()
        with pytest.raises(pika.exceptions.UnroutableError) as e:
            ch.basic_publish("", uniq("nobody"), b"x", mandatory=True)
        assert e.value.messages[0].method.reply_code == 312
        ch.basic_publish("", uniq("nobody"), b"x", mandatory=False)   # silently dropped
        c.close()

    def test_prefetch_limits_unacked_messages_and_redelivery_flag(self, one):
        c = blocking(one)
        ch = c.channel()
        q = uniq("q")
        ch.queue_declare(q, durable=True)
        for i in range(5):
            ch.basic_publish("", q, str(i).encode())
        ch.basic_qos(prefetch_count=2)
        seen = []
        ch.basic_consume(q, lambda ch_, m, p, b: seen.append((b, m.delivery_tag, m.redelivered)))
        c.process_data_events(1.0)
        assert [b for b, _, _ in seen] == [b"0", b"1"]
        ch.basic_ack(seen[0][1])
        c.process_data_events(1.0)
        assert [b for b, _, _ in seen] == [b"0", b"1", b"2"]
        ch.basic_nack(seen[2][1], requeue=True)      # goes back to its place and is redelivered flagged
        c.process_data_events(1.0)
        assert seen[3][0] == b"2" and seen[3][2] is True
        c.close()  # closing the connection requeues the unacked ones
        c2 = blocking(one)
        ch2 = c2.channel()
        rest = []
        while True:
            m, p, b = ch2.basic_get(q, auto_ack=True)
            if m is None:
                break
            rest.append((b, m.redelivered))
        assert sorted(b for b, _ in rest) == [b"1", b"2", b"3", b"4"]
        assert dict(rest)[b"1"] is True and dict(rest)[b"4"] is False
        ch2.queue_delete(q)
        c2.close()

    def test_transactions(self, one):
        c = blocking(one)
        ch = c.channel()
        q = uniq("q")
        ch.queue_declare(q, durable=True)
        ch.tx_select()
        ch.basic_publish("", q, b"a")
        ch.basic_publish("", q, b"b")
        assert ch.queue_declare(q, passive=True).method.message_count == 0
        ch.tx_commit()
        assert ch.queue_declare(q, passive=True).method.message_count == 2
        ch.basic_publish("", q, b"c")
        ch.tx_rollback()
        assert ch.queue_declare(q, passive=True).method.message_count == 2
        ch.queue_delete(q)
        c.close()

    def test_direct_reply_to_rpc(self, one):
        srv = blocking(one)
        cli = blocking(one)
        sch, cch = srv.channel(), cli.channel()
        q = uniq("rpc")
        sch.queue_declare(q, durable=True)

        def serve(ch, m, p, b):
            ch.basic_publish("", p.reply_to, b"pong:" + b, pika.BasicProperties(correlation_id=p.correlation_id))
            ch.basic_ack(m.delivery_tag)
        sch.basic_consume(q, serve)
        replies = []
        cch.basic_consume("amq.rabbitmq.reply-to", lambda ch, m, p, b: replies.append((p.correlation_id, b)), auto_ack=True)
        cch.basic_publish("", q, b"ping", pika.BasicProperties(reply_to="amq.rabbitmq.reply-to", correlation_id="c1"))
        deadline = time.time() + 5
        while not replies and time.time() < deadline:
            srv.process_data_events(0.05)
            cli.process_data_events(0.05)
        assert replies == [("c1", b"pong:ping")]
        sch.queue_delete(q)
        srv.close()
        cli.close()

    def test_consumer_cancel_notify_when_the_queue_is_deleted(self, one):
        c = blocking(one)
        ch = c.channel()
        q = uniq("q")
        ch.queue_declare(q, durable=True)
        cancelled = []
        ch.add_on_cancel_callback(lambda frame: cancelled.append(frame.method.consumer_tag))
        tag = ch.basic_consume(q, lambda *a: None)
        other = blocking(one)
        other.channel().queue_delete(q)
        c.process_data_events(1.0)
        assert cancelled == [tag]
        other.close()
        c.close()

    def test_dead_lettering_ttl_and_max_length(self, one):
        c = blocking(one)
        ch = c.channel()
        dlx, dlq, q = uniq("dlx"), uniq("dlq"), uniq("q")
        ch.exchange_declare(dlx, "fanout", durable=True)
        ch.queue_declare(dlq, durable=True)
        ch.queue_bind(dlq, dlx)
        ch.queue_declare(q, durable=True, arguments={"x-dead-letter-exchange": dlx, "x-message-ttl": 400, "x-max-length": 2})
        for i in range(4):
            ch.basic_publish("", q, str(i).encode())
        time.sleep(1.8)
        got = []
        while True:
            m, p, b = ch.basic_get(dlq, auto_ack=True)
            if m is None:
                break
            got.append((b, p.headers["x-first-death-reason"], p.headers["x-death"][0]["count"]))
        assert sorted(got) == [(b"0", "maxlen", 1), (b"1", "maxlen", 1), (b"2", "expired", 1), (b"3", "expired", 1)]
        for n in (q, dlq):
            ch.queue_delete(n)
        c.close()

    def test_heartbeats_keep_an_idle_connection_alive(self, one):
        c = blocking(one, heartbeat=2)
        ch = c.channel()
        t = time.time()
        while time.time() - t < 7:
            c.process_data_events(0.2)
        assert c.is_open and ch.is_open
        c.close()

    def test_silent_client_is_dropped_after_two_missed_heartbeats(self, one):
        c, info = R.open_connection("127.0.0.1", one.port, heartbeat=1)
        t = time.time()
        closed = False
        while time.time() - t < 8:
            e = c.read_event(0.5)
            if e and e.get("closed"):
                closed = True
                break
        assert closed and 2 <= time.time() - t < 6
        c.close()

    def test_unacked_messages_are_redelivered_after_an_abrupt_disconnect(self, one):
        q = uniq("q")
        raw, _ = R.open_connection("127.0.0.1", one.port)
        raw.send(1, "channel.open")
        raw.read_event(5)
        raw.send(1, "queue.declare", queue=q, durable=True, arguments={})
        raw.read_event(5)
        raw.publish(1, "", q, b"x")
        raw.send(1, "basic.get", queue=q)
        assert raw.read_event(5)["m"] == "basic.get-ok"
        raw.close()                      # TCP reset without connection.close: the unacknowledged message must come back
        c2 = blocking(one)
        ch2 = c2.channel()
        deadline = time.time() + 8
        m2 = None
        while time.time() < deadline and m2 is None:
            m2, p2, b2 = ch2.basic_get(q, auto_ack=True)
            time.sleep(0.1)
        assert m2 is not None and m2.redelivered is True and b2 == b"x"
        ch2.queue_delete(q)
        c2.close()


class TestPikaAsync:
    def _run(self, w, on_open, timeout=15):
        result = {}
        conn = pika.SelectConnection(params(w), on_open_callback=lambda c: on_open(c, result),
                                     on_open_error_callback=lambda c, e: result.setdefault("error", e))
        threading.Timer(timeout, lambda: conn.ioloop.add_callback_threadsafe(conn.ioloop.stop)).start()
        conn.ioloop.start()
        return result

    def test_publish_with_confirms_and_consume(self, one):
        q = uniq("q")
        n = 200

        def on_open(conn, result):
            result.update(acks=[], nacks=[], got=[])

            def on_channel(ch):
                def on_declared(_):
                    ch.confirm_delivery(lambda frame: (result["acks"] if frame.method.NAME == "Basic.Ack" else result["nacks"]).append(frame.method.delivery_tag))
                    for i in range(n):
                        ch.basic_publish("", q, str(i).encode())

                    def on_msg(ch_, m, p, b):
                        result["got"].append(int(b))
                        ch_.basic_ack(m.delivery_tag)
                        if len(result["got"]) == n and len(result["acks"]) >= n:
                            conn.close()
                    ch.basic_consume(q, on_msg)
                ch.queue_declare(q, durable=True, callback=on_declared)
            conn.channel(on_open_callback=on_channel)
            conn.add_on_close_callback(lambda c, r: c.ioloop.stop())

        r = self._run(one, on_open)
        assert "error" not in r
        assert r["got"] == list(range(n)) and sorted(r["acks"]) == list(range(1, n + 1)) and not r["nacks"]
        c = blocking(one)
        c.channel().queue_delete(q)
        c.close()

    def test_reject_publish_overflow_is_nacked(self, one):
        q = uniq("q")

        def on_open(conn, result):
            result.update(acks=[], nacks=[])

            def on_channel(ch):
                def on_declared(_):
                    ch.confirm_delivery(lambda f: (result["acks"] if f.method.NAME == "Basic.Ack" else result["nacks"]).append(f.method.delivery_tag))
                    for i in range(4):
                        ch.basic_publish("", q, b"x")
                    conn.ioloop.call_later(1.5, conn.close)
                ch.queue_declare(q, durable=True, arguments={"x-max-length": 2, "x-overflow": "reject-publish"}, callback=on_declared)
            conn.channel(on_open_callback=on_channel)
            conn.add_on_close_callback(lambda c, r: c.ioloop.stop())

        r = self._run(one, on_open)
        assert sorted(r["acks"]) == [1, 2] and sorted(r["nacks"]) == [3, 4]
        c = blocking(one)
        c.channel().queue_delete(q)
        c.close()


# ---------------------------------------------------------------------------------------------
# sharding over two Postgres hosts
# ---------------------------------------------------------------------------------------------

def outbox_payload(vhost, msg_id, exchange, rk, body, queues):
    def ls(b):
        b = b.encode() if isinstance(b, str) else b
        return struct.pack(">I", len(b)) + b
    props = struct.pack(">H", 0)
    return (b"\x01" + ls(vhost) + ls(msg_id) + ls(exchange) + ls(rk) + ls(props) + ls(body) + struct.pack(">q", -1) + struct.pack(">I", 0) + b"\x00\x00"
            + ls(b"") + ls(b"") + struct.pack(">I", len(queues)) + b"".join(ls(q) for q in queues))


class TestSharding:
    def test_queues_spread_over_both_hosts_and_live_on_exactly_one(self, two):
        c = blocking(two)
        ch = c.channel()
        names = [uniq("sq") for _ in range(40)]
        for q in names:
            ch.queue_declare(q, durable=True)
            ch.basic_publish("", q, q.encode())
        time.sleep(0.3)
        on0 = {r[0] for r in sql(two.pgs[0], "SELECT name FROM warp_amqp_queues")} & set(names)
        on1 = {r[0] for r in sql(two.pgs[1], "SELECT name FROM warp_amqp_queues")} & set(names)
        assert on0 and on1 and not (on0 & on1) and on0 | on1 == set(names)
        for q in names:
            owner = two.pgs[0] if q in on0 else two.pgs[1]
            assert sql(owner, "SELECT count(*) FROM warp_amqp_msgs WHERE queue=%s", (q,))[0][0] == 1
            m, p, b = ch.basic_get(q, auto_ack=True)
            assert b == q.encode()
            ch.queue_delete(q)
        assert sql(two.pgs[1], "SELECT count(*) FROM warp_amqp_exchanges")[0][0] == 0
        assert sql(two.pgs[0], "SELECT count(*) FROM warp_amqp_exchanges")[0][0] >= 6      # amq.* on the home host only
        c.close()

    def test_fanout_reaches_queues_on_both_hosts_and_the_outbox_is_empty_afterwards(self, two):
        c = blocking(two)
        ch = c.channel()
        ex = uniq("fx")
        ch.exchange_declare(ex, "fanout", durable=True)
        qs = [uniq("fq") for _ in range(12)]
        for q in qs:
            ch.queue_declare(q, durable=True)
            ch.queue_bind(q, ex)
        hosts = {sql(pg, "SELECT count(*) FROM warp_amqp_queues WHERE name = ANY(%s)", (qs,))[0][0] for pg in two.pgs}
        assert 0 not in hosts     # both hosts hold some of them
        ch.confirm_delivery()
        for i in range(10):
            ch.basic_publish(ex, "", f"m{i}".encode())
        for q in qs:
            got = []
            while True:
                m, p, b = ch.basic_get(q, auto_ack=True)
                if m is None:
                    break
                got.append(b)
            assert got == [f"m{i}".encode() for i in range(10)], q
        assert sql(two.pgs[0], "SELECT count(*) FROM warp_amqp_outbox")[0][0] == 0
        for q in qs:
            ch.queue_delete(q)
        ch.exchange_delete(ex)
        c.close()

    def test_outbox_row_of_a_crashed_publisher_is_delivered_by_the_sweeper(self, two):
        c = blocking(two)
        ch = c.channel()
        qs = [uniq("ob") for _ in range(8)]
        for q in qs:
            ch.queue_declare(q, durable=True)
        mid = uuid.uuid4().hex
        payload = outbox_payload("/", mid, "", "x", b"from-outbox", qs)
        sql(two.pgs[0], "INSERT INTO warp_amqp_outbox (payload, created_at) VALUES (%s, now() - interval '1 minute')", (psycopg2.Binary(payload),))
        deadline = time.time() + 20
        while time.time() < deadline and sql(two.pgs[0], "SELECT count(*) FROM warp_amqp_outbox")[0][0] > 0:
            time.sleep(0.3)
        assert sql(two.pgs[0], "SELECT count(*) FROM warp_amqp_outbox")[0][0] == 0
        for q in qs:
            m, p, b = ch.basic_get(q, auto_ack=True)
            assert b == b"from-outbox", q
            assert ch.basic_get(q)[0] is None     # exactly once: replaying is idempotent per (queue, message id)
        for q in qs:
            ch.queue_delete(q)
        c.close()

    def test_exchange_to_exchange_chain_across_hosts(self, two):
        c = blocking(two)
        ch = c.channel()
        a, b = uniq("a"), uniq("b")
        ch.exchange_declare(a, "topic")
        ch.exchange_declare(b, "fanout")
        ch.exchange_bind(b, a, "x.#")
        qs = [uniq("c") for _ in range(10)]
        for q in qs:
            ch.queue_declare(q, durable=True)
            ch.queue_bind(q, b)
        ch.basic_publish(a, "x.y.z", b"deep")
        for q in qs:
            assert ch.basic_get(q, auto_ack=True)[2] == b"deep"
            ch.queue_delete(q)
        ch.exchange_delete(a)
        ch.exchange_delete(b)
        c.close()

    def test_dead_letter_from_a_queue_on_one_host_into_a_queue_on_the_other(self, two):
        c = blocking(two)
        ch = c.channel()
        dlx = uniq("dlx")
        ch.exchange_declare(dlx, "direct", durable=True)
        # find two queue names that hash to different hosts
        src = dst = None
        while src is None or dst is None:
            n = uniq("dq")
            ch.queue_declare(n, durable=True)
            owner = 0 if sql(two.pgs[0], "SELECT count(*) FROM warp_amqp_queues WHERE name=%s", (n,))[0][0] else 1
            if owner == 0 and src is None:
                src = n
            elif owner == 1 and dst is None:
                dst = n
            else:
                ch.queue_delete(n)
        ch.queue_delete(src)
        ch.queue_declare(src, durable=True, arguments={"x-dead-letter-exchange": dlx})
        ch.queue_bind(dst, dlx, src)
        ch.basic_publish("", src, b"cross")
        m, p, b = ch.basic_get(src)
        ch.basic_nack(m.delivery_tag, requeue=False)
        time.sleep(0.3)
        m, p, b = ch.basic_get(dst, auto_ack=True)
        assert b == b"cross" and p.headers["x-death"][0]["queue"] == src
        for n in (src, dst):
            ch.queue_delete(n)
        ch.exchange_delete(dlx)
        c.close()


# ---------------------------------------------------------------------------------------------
# authentication, vhosts, pool, restart
# ---------------------------------------------------------------------------------------------

class TestStrict:
    def test_plain_login_against_the_credential_store(self, strict):
        c = blocking(strict, user="alice", password="s3cret")
        assert c.is_open
        c.close()
        c = blocking(strict, user="bob", password="hunter2")
        c.close()
        for user, pw in (("alice", "wrong"), ("nobody", "x"), ("", "")):
            with pytest.raises((pika.exceptions.ProbableAuthenticationError, pika.exceptions.AMQPConnectionError, pika.exceptions.ProbableAccessDeniedError)):
                blocking(strict, user=user, password=pw)

    def test_refused_login_is_a_403_close_with_rabbitmqs_text(self, strict):
        c, info = R.open_connection("127.0.0.1", strict.port, user="alice", password="nope")
        e = info["tune"]
        assert e["m"] == "connection.close" and e["a"]["reply_code"] == 403
        assert e["a"]["reply_text"] == "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN. For details see the broker logfile."
        c.close()

    def test_amqplain_mechanism(self, strict):
        c = R.RawConn("127.0.0.1", strict.port)
        c.send_raw(b"AMQP\x00\x00\x09\x01")
        c.read_event(5)
        table = R.enc_table({"LOGIN": "alice", "PASSWORD": "s3cret"})[4:]
        c.send(0, "connection.start-ok", client_properties={}, mechanism="AMQPLAIN", response=table, locale="en_US")
        assert c.read_event(5)["m"] == "connection.tune"
        c.close()

    def test_user_id_must_be_the_authenticated_user(self, strict):
        c = blocking(strict, user="alice", password="s3cret")
        ch = c.channel()
        q = uniq("q")
        ch.queue_declare(q, durable=True)
        ch.basic_publish("", q, b"ok", pika.BasicProperties(user_id="alice"))
        with pytest.raises(pika.exceptions.ChannelClosedByBroker) as e:
            ch.basic_publish("", q, b"forged", pika.BasicProperties(user_id="bob"))
            ch.queue_declare(q, passive=True)
        assert e.value.reply_code == 406 and "authenticated user was 'alice'" in e.value.reply_text
        c.close()

    def test_vhosts_are_isolated_and_unknown_vhosts_refused(self, strict):
        a = blocking(strict, user="alice", password="s3cret", vhost="tenant-a")
        b = blocking(strict, user="alice", password="s3cret")
        q = uniq("q")
        a.channel().queue_declare(q, durable=True)
        with pytest.raises(pika.exceptions.ChannelClosedByBroker) as e:
            b.channel().queue_declare(q, passive=True)
        assert e.value.reply_code == 404
        b.channel().queue_declare(q, durable=True)
        a.channel().basic_publish("", q, b"in-a")
        assert a.channel().basic_get(q, auto_ack=True)[2] == b"in-a"
        assert b.channel().basic_get(q)[0] is None
        with pytest.raises(pika.exceptions.ProbableAccessDeniedError):
            blocking(strict, user="alice", password="s3cret", vhost="tenant-zzz")
        a.close()
        b.close()

    def test_many_idle_consumers_do_not_pin_the_tiny_connection_pool(self, strict):
        # 6 connections x 5 channels = 30 idle consumers (the license tier caps concurrent TCP sessions, so many channels, few connections)
        conns = []
        for i in range(6):
            c = blocking(strict, user="alice", password="s3cret")
            for k in range(5):
                ch = c.channel()
                q = uniq(f"idle{i}-{k}")
                ch.queue_declare(q, durable=True)
                ch.basic_consume(q, lambda *a: None)
                conns.append((c, ch, q))
        time.sleep(1.5)        # every consumer polls; with 4 pooled connections a pinned one per consumer would starve them
        pub = blocking(strict, user="alice", password="s3cret")
        pch = pub.channel()
        pch.confirm_delivery()
        got = []
        c0, ch0, q0 = conns[0]
        ch0.basic_cancel(ch0.consumer_tags[0]) if ch0.consumer_tags else None
        ch0.basic_consume(q0, lambda ch_, m, p, b: got.append(b))
        t = time.time()
        pch.basic_publish("", q0, b"ping")
        deadline = time.time() + 5
        while not got and time.time() < deadline:
            c0.process_data_events(0.1)
        assert got == [b"ping"] and time.time() - t < 5
        t = time.time()
        pch.queue_declare(uniq("fresh"), durable=True)
        assert time.time() - t < 5
        for c in {id(c): c for c, _, _ in conns}.values():
            c.close()
        pub.close()

    def test_mcp_tools_list_publish_look_and_purge(self, strict):
        class Shim:
            metrics_port = strict.proc.metrics_port
            frontend_port = strict.mcp_port
        ep = create_endpoint(Shim, uniq("amqp"), "db:default")
        port, path, token = strict.mcp_port, ep["path"], ep["token"]
        names = tool_names(port, path, token)
        for t in ("amqp_list_exchanges", "amqp_list_queues", "amqp_list_bindings", "amqp_get_messages", "amqp_publish", "amqp_purge_queue",
                  "amqp_declare_queue", "amqp_declare_exchange", "amqp_bind", "amqp_delete_queue"):
            assert t in names, t
        q, x = uniq("mq"), uniq("mx")
        cj = lambda name, args=None: call_json(port, name, args, path=path, token=token)  # noqa: E731
        cj("amqp_declare_exchange", {"exchange": x, "type": "topic"})
        cj("amqp_declare_queue", {"queue": q, "arguments": {"x-max-length": 5}})
        cj("amqp_bind", {"source": x, "destination": q, "routingKey": "a.#"})
        assert q in [r["name"] for r in cj("amqp_list_queues")]
        assert x in [r["name"] for r in cj("amqp_list_exchanges")]
        assert {"source": x, "destination": q, "destination_type": "queue", "routing_key": "a.#", "arguments": {}} in cj("amqp_list_bindings")
        assert cj("amqp_publish", {"exchange": x, "routingKey": "a.b", "data": "hello", "properties": {"content_type": "text/plain", "priority": 3}})["routed"] is True
        assert cj("amqp_publish", {"exchange": x, "routingKey": "z", "data": "lost"})["routed"] is False
        msgs = cj("amqp_get_messages", {"queue": q})
        assert msgs[0]["body"] == "hello" and msgs[0]["properties"]["content_type"] == "text/plain" and msgs[0]["routing_key"] == "a.b"
        # the wire protocol sees what the tool published, and the tool looked without consuming
        c = blocking(strict, user="alice", password="s3cret")
        wch = c.channel()
        m, p, b = wch.basic_get(q)
        assert b == b"hello" and p.priority == 3
        wch.basic_ack(m.delivery_tag)
        c.close()
        assert call(port, "amqp_get_messages", {"queue": uniq("missing")}, path=path, token=token, expect_error=True)[0].startswith("NOT_FOUND - no queue")
        assert cj("amqp_purge_queue", {"queue": q})["purged"] == 0
        cj("amqp_delete_queue", {"queue": q})
        cj("amqp_delete_exchange", {"exchange": x})

    def test_metrics_count_amqp_operations_and_mcp_describes_the_store(self, strict):
        c = blocking(strict, user="alice", password="s3cret")
        ch = c.channel()
        q = uniq("q")
        ch.queue_declare(q, durable=True)
        ch.basic_publish("", q, b"x")
        ch.basic_get(q, auto_ack=True)
        c.close()
        r = requests.get(f"http://localhost:{strict.proc.metrics_port}/api/metrics/summary", headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, timeout=5)
        assert "amqpwire" in r.text and "basic.publish" in r.text and "basic.get" in r.text and "queue.declare" in r.text
        deadline = time.time() + 20
        d = None
        while time.time() < deadline:
            try:
                d = call_json(strict.mcp_port, "describe_backend", {"backend": "default.amqp"})
                break
            except Exception:  # noqa: BLE001 -- MCP still starting
                time.sleep(1)
        assert d is not None and q in d["contents"]["queues"] and d["engine"] == "warp-emulated" and "amqp_publish" in d["tools"]
        lb = call_json(strict.mcp_port, "list_backends")
        default = next(b for b in lb["backends"] if b["name"] == "default")
        assert default["enabledStores"] == ["amqp"]

    def test_read_only_mcp_hides_the_write_tools(self, strict):
        ro = L.AmqpWarp(1, extra_env={"WARP_MCP_READ_ONLY": "true"}, pgs=strict.pgs, default_store=False)
        try:
            class Shim:
                metrics_port = ro.proc.metrics_port
                frontend_port = ro.mcp_port
            ep = create_endpoint(Shim, uniq("amqpro"), "db:default")
            names = tool_names(ro.mcp_port, ep["path"], ep["token"])
            assert {"amqp_list_queues", "amqp_get_messages", "amqp_list_exchanges", "amqp_list_bindings"} <= names
            for t in ("amqp_publish", "amqp_purge_queue", "amqp_declare_queue", "amqp_delete_queue", "amqp_bind", "amqp_unbind", "amqp_declare_exchange",
                      "amqp_delete_exchange"):
                assert t not in names, t
        finally:
            ro.proc.close()


class TestRestart:
    def test_queues_messages_and_bindings_survive_a_restart_and_unacked_come_back(self):
        w = L.AmqpWarp(1)
        try:
            c = blocking(w)
            ch = c.channel()
            q, x = uniq("dq"), uniq("dx")
            ch.exchange_declare(x, "direct", durable=True)
            ch.queue_declare(q, durable=True, arguments={"x-max-priority": 3})
            ch.queue_bind(q, x, "k")
            for i in range(5):
                ch.basic_publish(x, "k", str(i).encode(), pika.BasicProperties(delivery_mode=2, priority=i % 3, headers={"i": i}))
            m, p, b = ch.basic_get(q)          # one message is delivered but never acknowledged
            first = b
            c._impl.socket.close() if hasattr(c._impl, "socket") else None
            w.restart(graceful=True)
            deadline = time.time() + 30
            c2 = None
            while time.time() < deadline:
                try:
                    c2 = blocking(w)
                    break
                except pika.exceptions.AMQPConnectionError:
                    time.sleep(0.5)
            ch2 = c2.channel()
            assert ch2.queue_declare(q, passive=True).method.message_count in (4, 5)
            got = []
            deadline = time.time() + 40
            while len(got) < 5 and time.time() < deadline:
                m, p, b = ch2.basic_get(q, auto_ack=True)
                if m is None:
                    time.sleep(0.3)
                    continue
                got.append((b, m.redelivered, p.headers["i"], p.delivery_mode))
            assert sorted(b for b, *_ in got) == [b"0", b"1", b"2", b"3", b"4"]
            # priority order survived (1, 4 then 0, 3); the message that was delivered but never acknowledged comes back flagged redelivered (its position depends on
            # how soon the sweeper freed the lease: it has the highest priority, so first if freed before the first get, last if freed after the others were taken)
            assert [i for _, _, i, _ in got if i != 2] == [1, 4, 0, 3]
            assert [r for _, r, i, _ in got if i == 2] == [True] and all(r is False for _, r, i, _ in got if i != 2)
            assert first == b"2" and all(d == 2 for *_, d in got)
            ch2.basic_publish(x, "k", b"after-restart")       # the binding survived too
            assert ch2.basic_get(q, auto_ack=True)[2] == b"after-restart"
            ch2.queue_delete(q)
            ch2.exchange_delete(x)
            c2.close()
        finally:
            w.close()


class TestCrash:
    def test_leases_and_exclusive_queues_of_a_killed_instance_are_released_by_the_survivor(self):
        w = L.AmqpWarp(1)
        try:
            q, ex = uniq("cq"), uniq("cx")
            raw, _ = R.open_connection("127.0.0.1", w.port)
            raw.send(1, "channel.open")
            raw.read_event(5)
            raw.send(1, "queue.declare", queue=q, durable=True, arguments={})
            raw.read_event(5)
            raw.send(1, "queue.declare", queue=ex, durable=False, exclusive=True, arguments={})
            raw.read_event(5)
            for i in range(3):
                raw.publish(1, "", q, f"m{i}".encode())
            raw.send(1, "basic.get", queue=q)
            assert raw.read_event(5)["m"] == "basic.get-ok"      # m0 is leased and never acknowledged
            w.restart(graceful=False)                           # kill -9: no chance to say goodbye
            c = blocking(w)
            ch = c.channel()
            deadline = time.time() + 60
            while time.time() < deadline and ch.queue_declare(q, passive=True).method.message_count < 3:
                time.sleep(1)
            assert ch.queue_declare(q, passive=True).method.message_count == 3      # the dead instance's lease was released
            m, p, b = ch.basic_get(q, auto_ack=True)
            assert b == b"m0" and m.redelivered is True
            deadline = time.time() + 30
            gone = False
            while time.time() < deadline and not gone:
                try:
                    ch.queue_declare(ex, passive=True)
                    time.sleep(1)
                except pika.exceptions.ChannelClosedByBroker as e:
                    assert e.reply_code == 404
                    gone = True
                    ch = c.channel()
            assert gone                                        # its exclusive queue died with it
            ch.queue_delete(q)
            c.close()
        finally:
            w.close()


# ---------------------------------------------------------------------------------------------
# concurrency
# ---------------------------------------------------------------------------------------------

class TestConcurrency:
    def test_competing_consumers_get_every_message_exactly_once(self, two):
        q = uniq("cc")
        c = blocking(two)
        c.channel().queue_declare(q, durable=True)
        c.close()
        n_pub, per = 4, 250
        total = n_pub * per
        seen = {}
        lock = threading.Lock()
        done = threading.Event()

        def consumer(k):
            conn = blocking(two)
            ch = conn.channel()
            ch.basic_qos(prefetch_count=20)

            def on_msg(ch_, m, p, b):
                with lock:
                    seen.setdefault(b, []).append(k)
                    if len(seen) == total:
                        done.set()
                ch_.basic_ack(m.delivery_tag)
            ch.basic_consume(q, on_msg)
            while not done.is_set():
                conn.process_data_events(0.1)
            conn.close()

        def publisher(k):
            conn = blocking(two)
            ch = conn.channel()
            ch.confirm_delivery()
            for i in range(per):
                ch.basic_publish("", q, f"{k}-{i}".encode())
            conn.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            cons = [ex.submit(consumer, k) for k in range(4)]
            pubs = [ex.submit(publisher, k) for k in range(n_pub)]
            for f in pubs:
                f.result(timeout=120)
            assert done.wait(60), f"only {len(seen)} of {total} messages arrived"
            for f in cons:
                f.result(timeout=30)
        assert all(len(v) == 1 for v in seen.values())
        assert len({k for v in seen.values() for k in v}) > 1          # the work was shared
        c = blocking(two)
        assert c.channel().queue_declare(q, passive=True).method.message_count == 0
        c.channel().queue_delete(q)
        c.close()

    def test_per_channel_order_is_kept_with_many_publishers(self, one):
        q = uniq("ord")
        c = blocking(one)
        ch = c.channel()
        ch.queue_declare(q, durable=True)

        def pub(k):
            conn = blocking(one)
            chn = conn.channel()
            for i in range(100):
                chn.basic_publish("", q, f"{k}:{i}".encode())
            conn.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(pub, range(4)))
        per = {}
        while True:
            m, p, b = ch.basic_get(q, auto_ack=True)
            if m is None:
                break
            k, i = b.decode().split(":")
            per.setdefault(k, []).append(int(i))
        assert sorted(per) == ["0", "1", "2", "3"] and all(v == list(range(100)) for v in per.values())
        ch.queue_delete(q)
        c.close()

    def test_a_large_body_and_a_large_backlog(self, one):
        q = uniq("big")
        c = blocking(one)
        ch = c.channel()
        ch.queue_declare(q, durable=True)
        body = os.urandom(3 * 1024 * 1024)
        ch.basic_publish("", q, body)
        assert ch.basic_get(q, auto_ack=True)[2] == body
        for i in range(3000):
            ch.basic_publish("", q, str(i).encode())
        assert ch.queue_declare(q, passive=True).method.message_count == 3000
        assert ch.queue_purge(q).method.message_count == 3000
        ch.queue_delete(q)
        c.close()


# ---------------------------------------------------------------------------------------------
# AMQP 1.0 with the real python-qpid-proton client
# ---------------------------------------------------------------------------------------------

proton = pytest.importorskip("proton")
from proton import Message as PMessage  # noqa: E402
from proton import symbol as psymbol  # noqa: E402
from proton.handlers import MessagingHandler  # noqa: E402
from proton.reactor import Container  # noqa: E402
from proton.utils import BlockingConnection as PConn  # noqa: E402
from proton.utils import LinkDetached, SendException  # noqa: E402


def pconn(w, user="guest", password="guest", **kw):
    return PConn(f"amqp://{user}:{password}@127.0.0.1:{w.port}", allowed_mechs="PLAIN", **kw)


class TestProton:
    def test_send_receive_and_the_0_9_1_view_of_the_same_message(self, one):
        q = uniq("q")
        pc = blocking(one)
        pc.channel().queue_declare(q, durable=True)
        c = pconn(one)
        s = c.create_sender(f"/queues/{q}")
        s.send(PMessage(body="hello", id="m1", subject="sub", content_type="text/plain", correlation_id="c1", reply_to="rt", ttl=60.0, durable=True, priority=4,
                        properties={"a": 1, "b": "two"}))
        m, p, b = pc.channel().basic_get(q)
        assert p.message_id == "m1"                # the amqp-value body is carried as its encoding, like RabbitMQ does
        assert p.correlation_id == "c1" and p.reply_to == "rt" and p.expiration == "60000" and p.delivery_mode == 2 and p.headers["a"] == 1
        assert p.type == "amqp-1.0" and b.endswith(b"hello")
        pc.channel().queue_delete(q)
        pc.close()
        c.close()

    def test_1_0_to_1_0_keeps_the_message_and_receives_what_0_9_1_published(self, one):
        q = uniq("q")
        pc = blocking(one)
        pch = pc.channel()
        pch.queue_declare(q, durable=True)
        c = pconn(one)
        s = c.create_sender(f"/queues/{q}")
        s.send(PMessage(body={"k": [1, 2, "three"]}, id=7, subject="keep", properties={"n": 5}, annotations={psymbol("x-mine"): "v"}))
        pch.basic_publish("", q, b"from-091", pika.BasicProperties(content_type="text/plain", message_id="m2", headers={"h": "v"}, delivery_mode=2, priority=3, expiration="60000"))
        r = c.create_receiver(f"/queues/{q}")
        m1 = r.receive(timeout=5)
        assert m1.body == {"k": [1, 2, "three"]} and m1.id == 7 and m1.subject == "keep" and m1.properties == {"n": 5} and m1.annotations[psymbol("x-mine")] == "v"
        assert m1.annotations[psymbol("x-routing-key")] == q and m1.annotations[psymbol("x-exchange")] == ""
        r.accept()
        m2 = r.receive(timeout=5)
        assert bytes(m2.body) == b"from-091" and m2.id == "m2" and m2.content_type == "text/plain" and m2.properties == {"h": "v"} and m2.durable is True and m2.ttl == 60.0
        r.accept()
        assert pch.queue_declare(q, passive=True).method.message_count == 0
        pch.queue_delete(q)
        pc.close()
        c.close()

    def test_outcomes_map_onto_the_store(self, one):
        q, dlx, dlq = uniq("q"), uniq("dlx"), uniq("dlq")
        pc = blocking(one)
        pch = pc.channel()
        pch.exchange_declare(dlx, "fanout", durable=True)
        pch.queue_declare(dlq, durable=True)
        pch.queue_bind(dlq, dlx)
        pch.queue_declare(q, durable=True, arguments={"x-dead-letter-exchange": dlx})
        for i in range(6):
            pch.basic_publish("", q, f"m{i}".encode())
        c = pconn(one)
        r = c.create_receiver(f"/queues/{q}", credit=6)
        m0 = r.receive(timeout=5)
        assert bytes(m0.body) == b"m0" and m0.delivery_count == 0 and m0.first_acquirer
        r.accept()                                  # accepted: gone
        r.receive(timeout=5)
        r.reject()                                  # rejected: dead-lettered
        m2 = r.receive(timeout=5)
        r.release(delivered=False)                  # released: back in the queue unchanged
        m3 = r.receive(timeout=5)
        r.release(delivered=True)                   # modified{delivery-failed}: back, counted as a failed delivery
        assert [bytes(m2.body), bytes(m3.body)] == [b"m2", b"m3"]
        c.container.timeout = 0.2
        for _ in range(5):
            c.container.process()                    # proton only writes the dispositions when it processes events
        deadline = time.time() + 8
        m = None
        while m is None and time.time() < deadline:
            m, p, b = pch.basic_get(dlq, auto_ack=True)
            time.sleep(0.1)
        assert b == b"m1" and p.headers["x-death"][0]["reason"] == "rejected"
        got = {}
        for _ in range(4):                           # m4, m5 (leased before) and the two returned ones
            g = r.receive(timeout=5)
            got[bytes(g.body)] = g
        assert set(got) == {b"m2", b"m3", b"m4", b"m5"}
        assert got[b"m2"].delivery_count == 0 and got[b"m3"].delivery_count == 0     # proton's release(delivered=True) is modified without delivery-failed
        c.close()
        time.sleep(0.6)
        rest = []
        while True:
            m, p, b = pch.basic_get(q, auto_ack=True)
            if m is None:
                break
            rest.append(b)
        assert sorted(rest) == [b"m2", b"m3", b"m4", b"m5"]     # everything unsettled went back when the connection closed
        pch.queue_delete(q)
        pch.queue_delete(dlq)
        pch.exchange_delete(dlx)
        pc.close()

    def test_errors_are_link_detaches_with_rabbitmqs_conditions(self, one):
        c = pconn(one)
        with pytest.raises(LinkDetached) as e:
            c.create_receiver(f"/queues/{uniq('nope')}")
        assert e.value.condition == "amqp:not-found"
        with pytest.raises(LinkDetached) as e:
            c.create_sender(f"/exchanges/{uniq('nox')}/k")
        assert e.value.condition == "amqp:not-found"
        with pytest.raises(LinkDetached) as e:
            c.create_receiver("/wrong/address")
        assert e.value.condition == "amqp:invalid-field" and "amqp_address_v1_not_permitted" in str(e.value)
        c.close()

    def test_unroutable_messages_are_released_and_routed_ones_accepted(self, one):
        x, q = uniq("x"), uniq("q")
        pc = blocking(one)
        pch = pc.channel()
        pch.exchange_declare(x, "direct", durable=True)
        pch.queue_declare(q, durable=True)
        pch.queue_bind(q, x, "hit")
        c = pconn(one)
        s_hit = c.create_sender(f"/exchanges/{x}/hit")
        s_miss = c.create_sender(f"/exchanges/{x}/miss")
        s_hit.send(PMessage(body="ok"))
        with pytest.raises(SendException):
            s_miss.send(PMessage(body="lost"))
        assert pch.queue_declare(q, passive=True).method.message_count == 1
        pch.queue_delete(q)
        pch.exchange_delete(x)
        pc.close()
        c.close()

    def test_event_driven_container_sends_and_receives_a_thousand_messages(self, one):
        q = uniq("q")
        pc = blocking(one)
        pc.channel().queue_declare(q, durable=True)
        n = 1000

        class H(MessagingHandler):
            def __init__(self):
                super().__init__(prefetch=50)
                self.sent = self.accepted = 0
                self.got = []

            def on_start(self, event):
                conn = event.container.connect(f"amqp://guest:guest@127.0.0.1:{one.port}", allowed_mechs="PLAIN")
                self.sender = event.container.create_sender(conn, f"/queues/{q}")
                self.receiver = event.container.create_receiver(conn, f"/queues/{q}")

            def on_sendable(self, event):
                while event.sender.credit and self.sent < n:
                    event.sender.send(PMessage(body=str(self.sent), durable=True))
                    self.sent += 1

            def on_accepted(self, event):
                self.accepted += 1

            def on_message(self, event):
                self.got.append(int(event.message.body))
                if len(self.got) == n:
                    event.connection.close()

        h = H()
        Container(h).run()
        assert h.got == list(range(n)) and h.accepted == n
        pc.channel().queue_delete(q)
        pc.close()

    def test_large_messages_and_heartbeats(self, one):
        q = uniq("q")
        pc = blocking(one)
        pc.channel().queue_declare(q, durable=True)
        c = pconn(one, heartbeat=2)
        body = os.urandom(2 * 1024 * 1024)
        snd = c.create_sender(f"/queues/{q}")
        snd.send(PMessage(body=body))
        r = c.create_receiver(f"/queues/{q}")
        assert bytes(r.receive(timeout=10).body) == body
        r.accept()
        c.container.timeout = 0.2
        end = time.time() + 6
        while time.time() < end:                 # idle for three heartbeat intervals (the client keeps reading): the connection must still be alive
            c.container.process()
        snd.send(PMessage(body="still-here"))
        assert r.receive(timeout=5).body == "still-here"
        pc.channel().queue_delete(q)
        pc.close()
        c.close()

    def test_cross_host_fanout_over_two_backends(self, two):
        x = uniq("fx")
        pc = blocking(two)
        pch = pc.channel()
        pch.exchange_declare(x, "fanout", durable=True)
        qs = [uniq("fq") for _ in range(10)]
        for q in qs:
            pch.queue_declare(q, durable=True)
            pch.queue_bind(q, x)
        c = pconn(two)
        c.create_sender(f"/exchanges/{x}").send(PMessage(body="to-all"))
        for q in qs:
            r = c.create_receiver(f"/queues/{q}")
            assert r.receive(timeout=5).body == "to-all"
            r.accept()
            r.close()
            pch.queue_delete(q)
        pch.exchange_delete(x)
        pc.close()
        c.close()

    def test_sasl_plain_against_the_credential_store_and_user_id_check(self, strict):
        c = pconn(strict, user="alice", password="s3cret")
        q = uniq("q")
        pch = blocking(strict, user="alice", password="s3cret")
        pch.channel().queue_declare(q, durable=True)
        s = c.create_sender(f"/queues/{q}")
        s.send(PMessage(body="x", user_id=b"alice"))
        with pytest.raises(LinkDetached) as e:
            s.send(PMessage(body="forged", user_id=b"bob"))
        assert e.value.condition == "amqp:unauthorized-access"
        c.close()
        with pytest.raises(Exception):
            pconn(strict, user="alice", password="wrong")
        # a plain AMQP header (no SASL) is refused when logins are required: the answer is the SASL header
        raw = R10.Raw10("127.0.0.1", strict.port)
        raw.send_raw(b"AMQP\x00\x01\x00\x00")
        assert raw.read_header() == b"AMQP\x03\x01\x00\x00"
        raw.close()
        pch.channel().queue_delete(q)
        pch.close()

    def test_a_header_that_is_neither_gets_the_0_9_1_header_back(self, one):
        for probe in (b"AMQP\x00\x00\x09\x02", b"AMQP\x02\x01\x00\x00", b"GET / HT"):
            raw = R10.Raw10("127.0.0.1", one.port)
            raw.send_raw(probe)
            assert raw.read_header() == b"AMQP\x00\x00\x09\x01"
            raw.close()
