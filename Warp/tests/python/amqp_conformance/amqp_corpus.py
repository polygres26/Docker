"""The differential corpus of amqpwire: every scenario drives raw AMQP 0-9-1 connections (amqp_raw.py) and records what the broker answers.
Recorded once against a real RabbitMQ 4 (golden.json.gz), replayed offline against Warp. Queue and exchange names come from s.n() (unique
per run, normalized to P in the recording); queues are durable unless a scenario is about the other flags because RabbitMQ 4 refuses
transient non-exclusive queues (a documented divergence: Warp accepts them)."""
import amqp_raw as R
from amqp_harness import case


def std(s, cn="a", ch=1, **kw):
    s.conn(cn, **kw)
    s.chan(cn, ch)


def qd(s, cn, ch, q, durable=True, **kw):
    args = kw.pop("arguments", {})
    return s.x(cn, ch, "queue.declare", queue=q, durable=durable, arguments=args, **kw)


def xd(s, cn, ch, ex, typ="direct", durable=True, **kw):
    args = kw.pop("arguments", {})
    return s.x(cn, ch, "exchange.declare", exchange=ex, type=typ, durable=durable, arguments=args, **kw)


def qb(s, cn, ch, q, ex, rk="", **args):
    return s.x(cn, ch, "queue.bind", queue=q, exchange=ex, routing_key=rk, arguments=args)


def reopen(s, cn, ch):
    s.chan(cn, ch)


# ============================================================ A. connection and channel level

@case("conn_handshake_and_close")
def _(s):
    s.conn("a")
    s.x("a", 0, "connection.close", reply_code=200, reply_text="bye", class_id=0, method_id=0)


@case("conn_heartbeat_frames")
def _(s):
    s.conn("a", heartbeat=2)
    s.hb("a", 3.5)


@case("conn_bad_vhost")
def _(s):
    s.conn("a", vhost="no-such-vhost")


@case("conn_bad_protocol_header")
def _(s):
    s.header_probe(b"AMQP\x00\x00\x09\x02")


@case("conn_method_before_channel_open")
def _(s):
    s.conn("a")
    s.x("a", 1, "queue.declare", queue=s.n("q"), durable=True, arguments={})


@case("conn_channel_open_twice")
def _(s):
    std(s)
    s.x("a", 1, "channel.open")


@case("conn_unknown_method")
def _(s):
    std(s)
    s.raw("a", b"\x01\x00\x01\x00\x00\x00\x04\x00\x3c\x00\x63\xce")
    s.x("a", 1, "basic.get", queue="", no_ack=True)


@case("conn_bad_frame_end")
def _(s):
    std(s)
    s.raw("a", b"\x01\x00\x01\x00\x00\x00\x04\x00\x5a\x00\x0a\x00")


@case("conn_content_without_publish")
def _(s):
    std(s)
    s.conns["a"].frame(2, 1, b"\x00\x3c\x00\x00" + b"\x00" * 8 + b"\x00\x00")
    s.drain("a", 1.0, first=1.0)


@case("conn_method_during_content")
def _(s):
    std(s)
    s.conns["a"].send(1, "basic.publish", exchange="", routing_key="x")
    s.x("a", 1, "queue.declare", queue=s.n("q"), durable=True, arguments={})


@case("conn_frame_on_channel_zero")
def _(s):
    std(s)
    s.conns["a"].frame(3, 0, b"x")
    s.drain("a", 1.0, first=1.0)


@case("conn_wrong_class_content_header")
def _(s):
    std(s)
    s.conns["a"].send(1, "basic.publish", exchange="", routing_key="x")
    s.conns["a"].frame(2, 1, b"\x00\x28\x00\x00" + b"\x00" * 7 + b"\x01\x00\x00")
    s.drain("a", 1.0, first=1.0)


@case("channel_close_and_reuse")
def _(s):
    std(s)
    s.x("a", 1, "channel.close", reply_code=200, reply_text="bye", class_id=0, method_id=0)
    s.chan("a", 1)
    s.x("a", 1, "channel.close", reply_code=200, reply_text="bye", class_id=0, method_id=0)
    s.chan("a", 2)
    s.x("a", 3, "queue.declare", queue="", arguments={}, durable=True)


@case("channel_error_then_ignored_methods")
def _(s):
    std(s)
    s.x("a", 1, "queue.declare", queue=s.n("nope"), passive=True, arguments={})
    # the channel is now in error: methods are ignored until channel.close-ok (sent by the harness)
    s.x("a", 1, "queue.declare", queue=s.n("q"), durable=True, arguments={}, reply=False)
    reopen(s, "a", 1)
    s.x("a", 1, "queue.declare", queue=s.n("q"), durable=True, arguments={})


@case("channel_flow")
def _(s):
    std(s)
    s.x("a", 1, "channel.flow", active=True)
    s.x("a", 1, "channel.flow", active=False)


@case("channel_two_channels_independent")
def _(s):
    std(s)
    s.chan("a", 2)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 2, "queue.declare", queue=s.n("nope"), passive=True, arguments={})
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


# ============================================================ B. exchanges

@case("ex_declare_types")
def _(s):
    std(s)
    for t in ("direct", "fanout", "topic", "headers"):
        xd(s, "a", 1, s.n("x_" + t), t)
    xd(s, "a", 1, s.n("x_nw"), "direct", no_wait=True, reply=False)
    s.x("a", 1, "exchange.declare", exchange=s.n("x_direct"), type="direct", passive=True, arguments={})
    s.x("a", 1, "exchange.declare", exchange=s.n("x_nw"), type="", passive=True, arguments={})


@case("ex_declare_passive_missing_and_default")
def _(s):
    std(s)
    s.x("a", 1, "exchange.declare", exchange=s.n("missing"), type="direct", passive=True, arguments={})
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.declare", exchange="", type="direct", passive=True, arguments={})
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.declare", exchange="", type="direct", durable=True, arguments={})
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.delete", exchange="")
    reopen(s, "a", 1)


@case("ex_redeclare_equivalence")
def _(s):
    std(s)
    x = s.n("x")
    xd(s, "a", 1, x, "topic", durable=True)
    xd(s, "a", 1, x, "topic", durable=True)
    for kw in ({"typ": "direct"}, {"durable": False}, {"auto_delete": True}, {"internal": True}, {"arguments": {"alternate-exchange": "zz"}}):
        xd(s, "a", 1, x, **({"typ": "topic"} | kw))
        reopen(s, "a", 1)
    xd(s, "a", 1, x, "topic", arguments={"a": 1})
    reopen(s, "a", 1)


@case("ex_reserved_prefix")
def _(s):
    std(s)
    xd(s, "a", 1, "amq.custom", "direct")
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.declare", exchange="amq.direct", type="direct", passive=True, arguments={})
    xd(s, "a", 1, "amq.direct", "direct")
    xd(s, "a", 1, "amq.topic", "direct")
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.delete", exchange="amq.direct")
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.delete", exchange="amq.custom")
    reopen(s, "a", 1)


@case("ex_unknown_type")
def _(s):
    std(s)
    xd(s, "a", 1, s.n("x"), "bogus")
    std(s, "b")
    xd(s, "b", 1, s.n("x2"), "")
    std(s, "c")
    xd(s, "c", 1, s.n("x3"), "x-consistent-hash")


@case("ex_delete")
def _(s):
    std(s)
    x, q = s.n("x"), s.n("q")
    s.x("a", 1, "exchange.delete", exchange=x)
    xd(s, "a", 1, x, "direct")
    qd(s, "a", 1, q)
    qb(s, "a", 1, q, x, "k")
    s.x("a", 1, "exchange.delete", exchange=x, if_unused=True)
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.delete", exchange=x)
    s.pub("a", 1, x, "k", "z")
    reopen(s, "a", 1)
    xd(s, "a", 1, x, "direct")
    s.x("a", 1, "exchange.delete", exchange=x, if_unused=True)
    xd(s, "a", 1, x, "direct")
    s.x("a", 1, "exchange.delete", exchange=x, no_wait=True, reply=False)
    s.x("a", 1, "exchange.declare", exchange=x, type="direct", passive=True, arguments={})


@case("ex_bind_exchange_to_exchange")
def _(s):
    std(s)
    a, b, q = s.n("a"), s.n("b"), s.n("q")
    xd(s, "a", 1, a, "fanout")
    xd(s, "a", 1, b, "direct")
    qd(s, "a", 1, q)
    s.x("a", 1, "exchange.bind", destination=b, source=a, routing_key="", arguments={})
    qb(s, "a", 1, q, b, "k")
    s.pub("a", 1, a, "ignored", "via-e2e")
    s.getall("a", 1, q)
    s.x("a", 1, "exchange.unbind", destination=b, source=a, routing_key="", arguments={})
    s.x("a", 1, "exchange.unbind", destination=b, source=a, routing_key="", arguments={})
    s.pub("a", 1, a, "ignored", "after-unbind")
    s.getall("a", 1, q)
    s.x("a", 1, "exchange.bind", destination=s.n("nodest"), source=a, routing_key="", arguments={})
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.bind", destination=b, source=s.n("nosrc"), routing_key="", arguments={})
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.bind", destination=b, source="", routing_key="", arguments={})
    reopen(s, "a", 1)
    s.x("a", 1, "exchange.bind", destination=a, source=a, routing_key="", arguments={}, no_wait=True, reply=False)


@case("ex_autodelete")
def _(s):
    std(s)
    x, q1, q2 = s.n("x"), s.n("q1"), s.n("q2")
    s.x("a", 1, "exchange.declare", exchange=x, type="direct", durable=True, auto_delete=True, arguments={})
    qd(s, "a", 1, q1)
    qd(s, "a", 1, q2)
    qb(s, "a", 1, q1, x, "1")
    qb(s, "a", 1, q2, x, "2")
    s.x("a", 1, "queue.unbind", queue=q1, exchange=x, routing_key="1", arguments={})
    s.x("a", 1, "exchange.declare", exchange=x, type="direct", passive=True, arguments={})
    s.x("a", 1, "queue.unbind", queue=q2, exchange=x, routing_key="2", arguments={})
    s.x("a", 1, "exchange.declare", exchange=x, type="direct", passive=True, arguments={})
    reopen(s, "a", 1)
    # an auto-delete exchange that never had a binding stays
    x2 = s.n("x2")
    s.x("a", 1, "exchange.declare", exchange=x2, type="direct", durable=True, auto_delete=True, arguments={})
    s.x("a", 1, "exchange.declare", exchange=x2, type="direct", passive=True, arguments={})
    # deleting the bound queue removes the binding and the auto-delete exchange
    x3 = s.n("x3")
    s.x("a", 1, "exchange.declare", exchange=x3, type="direct", durable=True, auto_delete=True, arguments={})
    qb(s, "a", 1, q1, x3, "1")
    s.x("a", 1, "queue.delete", queue=q1)
    s.x("a", 1, "exchange.declare", exchange=x3, type="direct", passive=True, arguments={})


@case("ex_internal")
def _(s):
    std(s)
    i, q, x = s.n("i"), s.n("q"), s.n("x")
    s.x("a", 1, "exchange.declare", exchange=i, type="fanout", durable=True, internal=True, arguments={})
    qd(s, "a", 1, q)
    qb(s, "a", 1, q, i)
    s.pub("a", 1, i, "k", "direct-publish")
    reopen(s, "a", 1)
    xd(s, "a", 1, x, "fanout")
    s.x("a", 1, "exchange.bind", destination=i, source=x, routing_key="", arguments={})
    s.pub("a", 1, x, "k", "through-e2e")
    s.getall("a", 1, q)


@case("ex_alternate_exchange")
def _(s):
    std(s)
    ae, ae2, x, q1, q2, q3 = (s.n(n) for n in ("ae", "ae2", "x", "q1", "q2", "q3"))
    xd(s, "a", 1, ae2, "fanout")
    xd(s, "a", 1, ae, "fanout", arguments={"alternate-exchange": ae2})
    xd(s, "a", 1, x, "direct", arguments={"alternate-exchange": ae})
    for q in (q1, q2, q3):
        qd(s, "a", 1, q)
    qb(s, "a", 1, q1, x, "hit")
    qb(s, "a", 1, q2, ae, "")
    qb(s, "a", 1, q3, ae2, "")
    s.pub("a", 1, x, "hit", "m-hit")
    s.pub("a", 1, x, "miss", "m-miss")
    for q in (q1, q2, q3):
        s.getall("a", 1, q)
    qb(s, "a", 1, q2, ae, "")
    s.x("a", 1, "queue.unbind", queue=q2, exchange=ae, routing_key="", arguments={})
    s.pub("a", 1, x, "miss", "m-miss2", mandatory=True)
    for q in (q1, q2, q3):
        s.getall("a", 1, q)
    s.pub("a", 1, x, "miss", "m-miss3", mandatory=True)  # AE has queue q3 bound via ae2: routed, no return
    s.x("a", 1, "queue.unbind", queue=q3, exchange=ae2, routing_key="", arguments={})
    s.pub("a", 1, x, "miss", "m-miss4", mandatory=True)  # nothing anywhere: returned


# ============================================================ C. queues

@case("q_declare_basic")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "1")
    s.pub("a", 1, "", q, "2")
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    qd(s, "a", 1, q)
    qd(s, "a", 1, s.n("nw"), no_wait=True, reply=False)
    s.x("a", 1, "queue.declare", queue=s.n("nw"), passive=True, arguments={})
    r = qd(s, "a", 1, "")
    s.x("a", 1, "queue.declare", queue="", passive=True, arguments={})  # passive with an empty name = the last declared queue
    qd(s, "a", 1, "", exclusive=True, durable=False)
    qd(s, "a", 1, "", exclusive=True, durable=False, auto_delete=True)


@case("q_declare_equivalence")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-message-ttl": 60000})
    qd(s, "a", 1, q, arguments={"x-message-ttl": 60000})
    qd(s, "a", 1, q, durable=False, exclusive=True)
    reopen(s, "a", 1)
    qd(s, "a", 1, q, auto_delete=True)
    reopen(s, "a", 1)
    qd(s, "a", 1, q, arguments={"x-message-ttl": 1000})
    reopen(s, "a", 1)
    qd(s, "a", 1, q)
    reopen(s, "a", 1)
    qd(s, "a", 1, q, arguments={"x-message-ttl": 60000, "x-max-length": 5})
    reopen(s, "a", 1)
    qd(s, "a", 1, q, arguments={"x-message-ttl": "60000"})
    reopen(s, "a", 1)


@case("q_exclusive_other_connection")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    qd(s, "a", 1, q, durable=False, exclusive=True)
    qd(s, "b", 1, q, durable=False, exclusive=True)
    reopen(s, "b", 1)
    s.x("b", 1, "queue.declare", queue=q, passive=True, arguments={})
    reopen(s, "b", 1)
    s.x("b", 1, "basic.consume", queue=q, consumer_tag="t", arguments={})
    reopen(s, "b", 1)
    s.x("b", 1, "basic.get", queue=q)
    reopen(s, "b", 1)
    s.x("b", 1, "queue.purge", queue=q)
    reopen(s, "b", 1)
    s.x("b", 1, "queue.delete", queue=q)
    reopen(s, "b", 1)
    qb(s, "b", 1, q, "amq.direct", "k")
    reopen(s, "b", 1)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    qd(s, "a", 1, q, durable=False, exclusive=True)
    s.x("a", 1, "connection.close", reply_code=200, reply_text="bye", class_id=0, method_id=0) if False else None
    s.conns["a"].close()
    s.sleep(1.0)
    s.x("b", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("q_reserved_name")
def _(s):
    std(s)
    qd(s, "a", 1, "amq.custom")
    reopen(s, "a", 1)
    s.x("a", 1, "queue.declare", queue="amq.custom", passive=True, arguments={})
    reopen(s, "a", 1)
    qd(s, "a", 1, "amq.gen-fixedname")
    reopen(s, "a", 1)


@case("q_arguments_valid_and_invalid")
def _(s):
    std(s)
    ok = [{"x-message-ttl": 5000}, {"x-expires": 60000}, {"x-max-length": 10}, {"x-max-length-bytes": 1000}, {"x-max-priority": 5},
          {"x-overflow": "drop-head"}, {"x-overflow": "reject-publish"}, {"x-dead-letter-exchange": "amq.direct", "x-dead-letter-routing-key": "k"},
          {"x-queue-type": "classic"}, {"x-unknown-thing": 1}, {"x-queue-mode": "lazy"}]
    for i, a in enumerate(ok):
        qd(s, "a", 1, s.n(f"ok{i}"), arguments=a)
    bad = [{"x-message-ttl": "abc"}, {"x-message-ttl": -1}, {"x-expires": 0}, {"x-expires": "x"}, {"x-max-length": -5}, {"x-max-length": "5"},
           {"x-max-priority": 300}, {"x-max-priority": "high"}, {"x-overflow": "bogus"}, {"x-queue-type": "bogus"}, {"x-dead-letter-exchange": 5},
           {"x-dead-letter-routing-key": 5}, {"x-message-ttl": 1.5}, {"x-max-length-bytes": -1}]
    for i, a in enumerate(bad):
        qd(s, "a", 1, s.n(f"bad{i}"), arguments=a)
        reopen(s, "a", 1)


@case("q_quorum_type")
def _(s):
    std(s)
    qd(s, "a", 1, s.n("qq"), arguments={"x-queue-type": "quorum"})
    qd(s, "a", 1, s.n("qq"), arguments={"x-queue-type": "quorum"})
    s.x("a", 1, "queue.declare", queue=s.n("qq3"), durable=True, exclusive=True, arguments={"x-queue-type": "quorum"})
    reopen(s, "a", 1)
    qd(s, "a", 1, s.n("qq"), arguments={"x-queue-type": "classic"})
    reopen(s, "a", 1)
    qd(s, "a", 1, s.n("qc"), arguments={"x-queue-type": "classic"})
    qd(s, "a", 1, s.n("qc"))
    s.x("a", 1, "queue.declare", queue=s.n("qq2"), durable=False, arguments={"x-queue-type": "quorum"})


@case("q_delete")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    s.x("a", 1, "queue.delete", queue=q)
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "1")
    s.pub("a", 1, "", q, "2")
    s.x("a", 1, "queue.delete", queue=q, if_empty=True)
    reopen(s, "a", 1)
    s.x("b", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.x("a", 1, "queue.delete", queue=q, if_unused=True)
    reopen(s, "a", 1)
    s.x("a", 1, "queue.delete", queue=q)
    s.drain("b", 0.3)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    reopen(s, "a", 1)
    qd(s, "a", 1, q)
    s.x("a", 1, "queue.delete", queue=q, if_empty=True, if_unused=True)
    qd(s, "a", 1, q)
    s.x("a", 1, "queue.delete", queue=q, no_wait=True, reply=False)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("q_purge")
def _(s):
    std(s)
    q = s.n("q")
    s.x("a", 1, "queue.purge", queue=q)
    reopen(s, "a", 1)
    qd(s, "a", 1, q)
    s.x("a", 1, "queue.purge", queue=q)
    for i in range(5):
        s.pub("a", 1, "", q, str(i))
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "queue.purge", queue=q)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "queue.purge", queue=q, no_wait=True, reply=False)
    s.x("a", 1, "basic.get", queue=q)


@case("q_bind_unbind")
def _(s):
    std(s)
    q, x = s.n("q"), s.n("x")
    xd(s, "a", 1, x, "direct")
    qb(s, "a", 1, q, x, "k")
    reopen(s, "a", 1)
    qd(s, "a", 1, q)
    qb(s, "a", 1, q, s.n("nox"), "k")
    reopen(s, "a", 1)
    qb(s, "a", 1, q, x, "k")
    qb(s, "a", 1, q, x, "k")
    qb(s, "a", 1, q, x, "k", **{"arg": 1})
    s.pub("a", 1, x, "k", "one")
    s.getall("a", 1, q)
    s.x("a", 1, "queue.unbind", queue=q, exchange=x, routing_key="k", arguments={})
    s.pub("a", 1, x, "k", "two")
    s.getall("a", 1, q)
    s.x("a", 1, "queue.unbind", queue=q, exchange=x, routing_key="k", arguments={"arg": 1})
    s.x("a", 1, "queue.unbind", queue=q, exchange=x, routing_key="k", arguments={})
    s.x("a", 1, "queue.unbind", queue=s.n("noq"), exchange=x, routing_key="k", arguments={})
    s.x("a", 1, "queue.unbind", queue=q, exchange=s.n("nox"), routing_key="k", arguments={})
    s.x("a", 1, "queue.bind", queue=q, exchange="", routing_key="k", arguments={})
    reopen(s, "a", 1)
    s.x("a", 1, "queue.bind", queue=q, exchange=x, routing_key="nw", no_wait=True, arguments={}, reply=False)
    s.x("a", 1, "queue.unbind", queue=q, exchange=x, routing_key="nw", arguments={})
    # an empty queue name means the last declared queue of the channel
    r = qd(s, "a", 1, "")
    s.x("a", 1, "queue.bind", queue="", exchange=x, routing_key="last", arguments={})
    s.pub("a", 1, x, "last", "to-last")
    s.x("a", 1, "basic.get", queue="", no_ack=True)


@case("q_autodelete")
def _(s):
    std(s)
    q, q2 = s.n("q"), s.n("q2")
    s.x("a", 1, "queue.declare", queue=q, durable=True, auto_delete=True, arguments={})
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c2", arguments={})
    s.x("a", 1, "basic.cancel", consumer_tag="c1")
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.cancel", consumer_tag="c2")
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    reopen(s, "a", 1)
    # an auto-delete queue without any consumer ever stays
    s.x("a", 1, "queue.declare", queue=q2, durable=True, auto_delete=True, arguments={})
    s.x("a", 1, "queue.declare", queue=q2, passive=True, arguments={})
    # deleted when the consuming channel closes
    s.x("a", 1, "basic.consume", queue=q2, consumer_tag="c3", arguments={})
    s.x("a", 1, "channel.close", reply_code=200, reply_text="bye", class_id=0, method_id=0)
    s.chan("a", 1)
    s.x("a", 1, "queue.declare", queue=q2, passive=True, arguments={})


@case("q_consumer_count_in_declare_ok")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c2", arguments={})
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.cancel", consumer_tag="c1")
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("q_x_expires")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-expires": 700})
    s.sleep(0.3)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})  # touching keeps it alive
    s.sleep(1.6)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    reopen(s, "a", 1)
    q2 = s.n("q2")
    qd(s, "a", 1, q2, arguments={"x-expires": 700})
    s.x("a", 1, "basic.consume", queue=q2, consumer_tag="c1", arguments={})
    s.sleep(1.6)
    s.x("a", 1, "queue.declare", queue=q2, passive=True, arguments={})


# ============================================================ D. routing

def pubget(s, ex, rk, q, body="m", **kw):
    s.pub("a", 1, ex, rk, body, **kw)


@case("route_direct")
def _(s):
    std(s)
    x, q1, q2, q3 = s.n("x"), s.n("q1"), s.n("q2"), s.n("q3")
    xd(s, "a", 1, x, "direct")
    for q in (q1, q2, q3):
        qd(s, "a", 1, q)
    qb(s, "a", 1, q1, x, "red")
    qb(s, "a", 1, q2, x, "red")
    qb(s, "a", 1, q2, x, "blue")
    qb(s, "a", 1, q3, x, "")
    for k in ("red", "blue", "green", "", "RED", "red "):
        s.pub("a", 1, x, k, "k=" + k)
    for q in (q1, q2, q3):
        s.getall("a", 1, q)


@case("route_fanout")
def _(s):
    std(s)
    x, q1, q2 = s.n("x"), s.n("q1"), s.n("q2")
    xd(s, "a", 1, x, "fanout")
    qd(s, "a", 1, q1)
    qd(s, "a", 1, q2)
    qb(s, "a", 1, q1, x, "a")
    qb(s, "a", 1, q2, x, "b")
    s.pub("a", 1, x, "whatever", "1")
    s.pub("a", 1, x, "", "2")
    s.getall("a", 1, q1)
    s.getall("a", 1, q2)


TOPIC_PATTERNS = ["a.b.c", "a.*.c", "a.#", "#", "*", "*.*", "*.*.*", "#.c", "a.#.c", "a.b.#", "*.b.*", "#.b.#", "a.*", "*.c", "a.b.c.#", "#.#",
                  "a.#.#.c", "a..c", "", "a.*.*.d", "*.#", "#.*", "a.b", "b.#"]
TOPIC_KEYS = ["a.b.c", "a.c", "a", "a.b", "a.b.c.d", "b.b.c", "a..c", "", ".", "a.", ".a", "x.y.z", "a.b.b.c", "c", "a.x.c.d", "a.b.c.c", "b"]


@case("route_topic_table")
def _(s):
    std(s)
    x = s.n("x")
    xd(s, "a", 1, x, "topic")
    qs = []
    for i, p in enumerate(TOPIC_PATTERNS):
        q = s.n(f"t{i}")
        qd(s, "a", 1, q)
        s.x("a", 1, "queue.bind", queue=q, exchange=x, routing_key=p, arguments={}, no_wait=False)
        qs.append(q)
    for k in TOPIC_KEYS:
        s.pub("a", 1, x, k, "key:" + k, quiet=0.02)
    for q in qs:
        s.getall("a", 1, q)


@case("route_headers_exchange")
def _(s):
    std(s)
    x = s.n("x")
    xd(s, "a", 1, x, "headers")
    specs = [("all", {"x-match": "all", "a": 1, "b": "two"}), ("any", {"x-match": "any", "a": 1, "b": "two"}), ("default", {"a": 1, "b": "two"}),
             ("allx", {"x-match": "all-with-x", "x-k": "v", "a": 1}), ("anyx", {"x-match": "any-with-x", "x-k": "v", "zz": 9}),
             ("none", {"x-match": "all"}), ("anynone", {"x-match": "any"}), ("bool", {"x-match": "all", "flag": True}),
             ("void", {"x-match": "all", "a": None}), ("str1", {"x-match": "all", "a": "1"}), ("float", {"x-match": "all", "f": 1.5})]
    qs = []
    for name, args in specs:
        q = s.n("h_" + name)
        qd(s, "a", 1, q)
        s.x("a", 1, "queue.bind", queue=q, exchange=x, routing_key="", arguments=args)
        qs.append(q)
    msgs = [{"a": 1, "b": "two"}, {"a": 1}, {"b": "two"}, {"a": 1, "b": "two", "c": 3}, {"x-k": "v", "a": 1}, {"x-k": "v"}, {}, {"flag": True},
            {"flag": False}, {"a": "1"}, {"f": 1.5}, {"a": R.I64(1)}, {"a": 1.0}, {"zz": 9}, {"a": R.I8(1), "b": "two"}]
    for i, h in enumerate(msgs):
        s.pub("a", 1, x, "ignored", f"h{i}", quiet=0.02, headers=h)
    s.pub("a", 1, x, "no-headers-at-all", "hnone", quiet=0.02)
    for q in qs:
        s.getall("a", 1, q)


@case("route_headers_invalid_xmatch")
def _(s):
    std(s)
    x, q = s.n("x"), s.n("q")
    xd(s, "a", 1, x, "headers")
    qd(s, "a", 1, q)
    for v in ("bogus", "ALL", 5):
        s.x("a", 1, "queue.bind", queue=q, exchange=x, routing_key="", arguments={"x-match": v, "a": 1})
        reopen(s, "a", 1)
    # on a non-headers exchange x-match is just an argument
    x2 = s.n("x2")
    xd(s, "a", 1, x2, "direct")
    s.x("a", 1, "queue.bind", queue=q, exchange=x2, routing_key="k", arguments={"x-match": "bogus"})


@case("route_default_exchange_and_unroutable")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "to-q")
    s.pub("a", 1, "", s.n("missing"), "dropped")
    s.pub("a", 1, "", s.n("missing"), "returned", mandatory=True, content_type="text/plain", headers={"h": 1})
    s.pub("a", 1, "", q, "routed-mandatory", mandatory=True)
    s.getall("a", 1, q)


@case("route_mandatory_return_details")
def _(s):
    std(s)
    x = s.n("x")
    xd(s, "a", 1, x, "direct")
    s.pub("a", 1, x, "nobody", b"\x00\x01\x02binary", mandatory=True, delivery_mode=2, priority=3, correlation_id="c1", message_id="m1")
    xd(s, "a", 1, s.n("f"), "fanout")
    s.pub("a", 1, s.n("f"), "", "", mandatory=True)
    s.pub("a", 1, x, "big", b"y" * 200000, mandatory=True)


@case("route_immediate_flag")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "imm", immediate=True)
    std(s, "b")
    s.pub("b", 1, "", s.n("none"), "imm-unroutable", immediate=True, mandatory=True)


@case("route_cc_bcc")
def _(s):
    std(s)
    x, q1, q2, q3 = s.n("x"), s.n("q1"), s.n("q2"), s.n("q3")
    xd(s, "a", 1, x, "direct")
    for q, k in ((q1, "main"), (q2, "cc1"), (q3, "bcc1")):
        qd(s, "a", 1, q)
        qb(s, "a", 1, q, x, k)
    s.pub("a", 1, x, "main", "with-cc", headers={"CC": ["cc1"], "BCC": ["bcc1"], "other": 1})
    for q in (q1, q2, q3):
        s.getall("a", 1, q)
    s.pub("a", 1, "", q1, "default-cc", headers={"CC": [q2]})
    for q in (q1, q2, q3):
        s.getall("a", 1, q)


@case("route_e2e_chain_and_cycle")
def _(s):
    std(s)
    a, b, c, q = s.n("a"), s.n("b"), s.n("c"), s.n("q")
    xd(s, "a", 1, a, "topic")
    xd(s, "a", 1, b, "fanout")
    xd(s, "a", 1, c, "direct")
    qd(s, "a", 1, q)
    s.x("a", 1, "exchange.bind", destination=b, source=a, routing_key="x.*", arguments={})
    s.x("a", 1, "exchange.bind", destination=c, source=b, routing_key="", arguments={})
    s.x("a", 1, "exchange.bind", destination=a, source=c, routing_key="x.y", arguments={})  # cycle
    qb(s, "a", 1, q, c, "x.y")
    qb(s, "a", 1, q, a, "x.#")
    s.pub("a", 1, a, "x.y", "chain")
    s.pub("a", 1, a, "z", "nomatch", mandatory=True)
    s.getall("a", 1, q)


@case("route_one_message_many_queues")
def _(s):
    std(s)
    x = s.n("x")
    xd(s, "a", 1, x, "fanout")
    qs = [s.n(f"q{i}") for i in range(4)]
    for q in qs:
        qd(s, "a", 1, q)
        qb(s, "a", 1, q, x)
    s.pub("a", 1, x, "rk", "shared", content_type="text/plain", headers={"h": [1, 2]}, delivery_mode=2)
    for q in qs:
        s.getall("a", 1, q)


# ============================================================ E. basic

ALL_PROPS = dict(content_type="application/json", content_encoding="gzip", delivery_mode=2, priority=7, correlation_id="corr-1", reply_to="reply-q",
                 expiration="600000", message_id="msg-1", timestamp=1700000000, type="t.type", user_id="guest", app_id="app-1")


@case("basic_props_roundtrip")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    hdrs = {"s": "str", "i": 5, "neg": -7, "big": R.I64(1 << 40), "short": R.I16(300), "byte": R.I8(-3), "d": 3.25, "t": True, "f": False, "n": None,
            "ts": R.Ts(1700000000), "dec": R.Dec((2, 12345)), "bin": b"\x00\xff", "list": [1, "two", [3], {"k": "v"}], "nested": {"a": {"b": 1}},
            "empty": "", "uni": "héllo ☃"}
    s.pub("a", 1, "", q, "everything", headers=hdrs, **ALL_PROPS)
    s.getall("a", 1, q)
    s.pub("a", 1, "", q, "cluster", cluster_id="cl-1")
    s.pub("a", 1, "", q, "nothing")
    s.pub("a", 1, "", q, "empty-strings", content_type="", correlation_id="", headers={})
    s.getall("a", 1, q)


@case("basic_body_sizes")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for n in (0, 1, 131063, 131064, 131065, 262128, 300000):
        s.pub("a", 1, "", q, bytes((i * 7) % 251 for i in range(n)), quiet=0.02)
    s.getall("a", 1, q)


@case("basic_get")
def _(s):
    std(s)
    q = s.n("q")
    s.x("a", 1, "basic.get", queue=q)
    reopen(s, "a", 1)
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.get", queue=q)
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}")
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q, no_ack=True)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.reject", delivery_tag=3, requeue=True, reply=False)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q)


@case("basic_ack_semantics")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(6):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    for _ in range(6):
        s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.ack", delivery_tag=2, reply=False)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.ack", delivery_tag=2, reply=False)  # double ack: unknown delivery tag
    reopen(s, "a", 1)
    std(s, "b")
    qd(s, "b", 1, q)
    s.x("b", 1, "basic.ack", delivery_tag=1, reply=False)  # tag from another channel/connection
    reopen(s, "b", 1)
    s.x("b", 1, "basic.ack", delivery_tag=0, multiple=True, reply=False)  # nothing outstanding: allowed
    s.x("b", 1, "basic.ack", delivery_tag=99, multiple=True, reply=False)
    reopen(s, "b", 1)


@case("basic_ack_multiple")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(8):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    for _ in range(8):
        s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.ack", delivery_tag=3, multiple=True, reply=False)
    s.x("a", 1, "basic.ack", delivery_tag=5, reply=False)
    s.x("a", 1, "basic.ack", delivery_tag=4, multiple=True, reply=False)
    s.x("a", 1, "basic.recover", requeue=True)
    s.getall("a", 1, q)


@case("basic_nack_reject_requeue")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(6):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    for _ in range(6):
        s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.nack", delivery_tag=2, multiple=True, requeue=True, reply=False)
    s.x("a", 1, "basic.reject", delivery_tag=3, requeue=False, reply=False)
    s.x("a", 1, "basic.nack", delivery_tag=4, requeue=False, reply=False)
    s.x("a", 1, "basic.reject", delivery_tag=5, requeue=True, reply=False)
    s.x("a", 1, "basic.nack", delivery_tag=6, multiple=False, requeue=True, reply=False)
    s.x("a", 1, "basic.reject", delivery_tag=77, requeue=True, reply=False)
    reopen(s, "a", 1)
    s.getall("a", 1, q)


@case("basic_consume_basics")
def _(s):
    std(s)
    q = s.n("q")
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    reopen(s, "a", 1)
    qd(s, "a", 1, q)
    for i in range(3):
        s.pub("a", 1, "", q, f"pre{i}", quiet=0.02)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="", arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="fixed", arguments={}, no_wait=True, reply=False)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="fixed", arguments={})
    reopen(s, "a", 1)
    s.pub("a", 1, "", q, "live1")
    s.x("a", 1, "basic.ack", delivery_tag=1, multiple=True, reply=False)
    s.x("a", 1, "basic.cancel", consumer_tag="nonexistent")
    s.x("a", 1, "basic.cancel", consumer_tag="fixed", no_wait=True, reply=False)
    s.pub("a", 1, "", q, "after-cancel")
    s.drain("a", 0.3)


@case("basic_consume_cancel_cycle")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", no_ack=True, arguments={})
    for i in range(3):
        s.pub("a", 1, "", q, f"a{i}")
    s.x("a", 1, "basic.cancel", consumer_tag="c1")
    s.pub("a", 1, "", q, "b0")
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", no_ack=True, arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("basic_qos_prefetch_per_consumer")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=2)
    for i in range(6):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.drain("a", 0.3)
    s.x("a", 1, "basic.ack", delivery_tag=4, multiple=True, reply=False)
    s.drain("a", 0.3)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=0)
    s.drain("a", 0.3)


@case("basic_qos_prefetch_global")
def _(s):
    std(s)
    q1, q2 = s.n("q1"), s.n("q2")
    qd(s, "a", 1, q1)
    qd(s, "a", 1, q2)
    for i in range(4):
        s.pub("a", 1, "", q1, f"a{i}", quiet=0.02)
        s.pub("a", 1, "", q2, f"b{i}", quiet=0.02)
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=3, **{"global": True})
    s.x("a", 1, "basic.consume", queue=q1, consumer_tag="c1", arguments={})
    s.x("a", 1, "basic.consume", queue=q2, consumer_tag="c2", arguments={})
    s.drain("a", 0.4)
    s.x("a", 1, "basic.ack", delivery_tag=1, multiple=True, reply=False)
    s.drain("a", 0.4)


@case("basic_qos_invalid_size")
def _(s):
    std(s)
    s.x("a", 1, "basic.qos", prefetch_size=100, prefetch_count=1)


@case("basic_two_consumers_round_robin")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=1)
    s.x("b", 1, "basic.qos", prefetch_size=0, prefetch_count=1)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="ca", arguments={})
    s.x("b", 1, "basic.consume", queue=q, consumer_tag="cb", arguments={})
    for i in range(6):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.sleep(0.4)
    s.drain("a", 0.3)
    s.drain("b", 0.3)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.sleep(0.3)
    s.drain("a", 0.3)
    s.drain("b", 0.3)


@case("basic_consumer_cancel_notify")
def _(s):
    std(s, "a")
    std(s, "b", client_props={"product": "no-caps"})
    std(s, "c")
    q = s.n("q")
    qd(s, "c", 1, q)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="ca", arguments={})
    s.x("b", 1, "basic.consume", queue=q, consumer_tag="cb", arguments={})
    s.x("c", 1, "queue.delete", queue=q)
    s.drain("a", 0.4)
    s.drain("b", 0.4)
    s.x("b", 1, "basic.cancel", consumer_tag="cb")


@case("basic_exclusive_consumer")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", exclusive=True, arguments={})
    s.x("b", 1, "basic.consume", queue=q, consumer_tag="c2", arguments={})
    reopen(s, "b", 1)
    s.x("a", 1, "basic.cancel", consumer_tag="c1")
    s.x("b", 1, "basic.consume", queue=q, consumer_tag="c2", arguments={})
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c3", exclusive=True, arguments={})
    reopen(s, "a", 1)


@case("basic_consumer_priority")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=2)
    s.x("b", 1, "basic.qos", prefetch_size=0, prefetch_count=2)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="lo", arguments={"x-priority": 1})
    s.x("b", 1, "basic.consume", queue=q, consumer_tag="hi", arguments={"x-priority": 10})
    for i in range(5):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.sleep(0.4)
    s.drain("a", 0.3)
    s.drain("b", 0.3)


@case("basic_recover")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "basic.recover", requeue=True)
    s.drain("a", 0.4)
    s.x("a", 1, "basic.recover-async", requeue=True, reply=False)
    s.drain("a", 0.4)
    s.x("a", 1, "basic.recover", requeue=False)
    s.drain("a", 0.4)
    s.x("a", 1, "basic.ack", delivery_tag=100, multiple=True, reply=False)


@case("basic_redelivered_after_disconnect")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.conns["a"].close()
    s.sleep(0.6)
    s.getall("b", 1, q)


@case("basic_channel_close_requeues")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "m0")
    s.chan("a", 2)
    s.x("a", 2, "basic.get", queue=q)
    s.x("a", 2, "channel.close", reply_code=200, reply_text="bye", class_id=0, method_id=0)
    s.x("a", 1, "basic.get", queue=q)


@case("basic_unacked_not_in_message_count")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "queue.purge", queue=q)
    s.x("a", 1, "queue.delete", queue=q)


@case("basic_publish_missing_exchange")
def _(s):
    std(s)
    s.pub("a", 1, s.n("missing"), "k", "lost", wait=False)
    s.pub("a", 1, s.n("missing"), "k", "ignored", wait=False)
    reopen(s, "a", 1)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.get", queue=q)


@case("basic_message_order")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(40):
        s.pub("a", 1, "", q, f"{i:03d}", quiet=0.0)
    s.sleep(0.3)
    s.getall("b", 1, q)


@case("basic_direct_reply_to")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("rpc")
    qd(s, "b", 1, q)
    s.x("a", 1, "basic.consume", queue="amq.rabbitmq.reply-to", consumer_tag="rt", no_ack=True, arguments={})
    s.pub("a", 1, "", q, "request", reply_to="amq.rabbitmq.reply-to", correlation_id="c-1")
    got = s.x("b", 1, "basic.get", queue=q, no_ack=True)
    rt = None
    try:
        rt = got[0]["props"]["reply_to"]
    except (IndexError, KeyError):
        pass
    if rt:
        raw_rt = rt.replace("<tok1>", s.tok_raw(1)) if hasattr(s, "tok_raw") else rt
        s.pub("b", 1, "", raw_rt, "response", correlation_id="c-1")
    s.drain("a", 0.4)
    s.x("a", 1, "basic.consume", queue="amq.rabbitmq.reply-to", consumer_tag="rt2", no_ack=True, arguments={})
    reopen(s, "a", 1)
    std(s, "c")
    s.x("c", 1, "basic.consume", queue="amq.rabbitmq.reply-to", consumer_tag="rt3", arguments={})
    reopen(s, "c", 1)
    s.pub("c", 1, "", q, "no-consumer", reply_to="amq.rabbitmq.reply-to")


@case("basic_persistent_flag")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "p2", delivery_mode=2)
    s.pub("a", 1, "", q, "p1", delivery_mode=1)
    s.pub("a", 1, "", q, "p0")
    s.getall("a", 1, q)


@case("basic_publish_property_validation")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "bad-user", user_id="somebody-else", wait=False)
    reopen(s, "a", 1)
    s.pub("a", 1, "", q, "bad-exp", expiration="soon")
    reopen(s, "a", 1)
    s.pub("a", 1, "", q, "neg-exp", expiration="-5")
    reopen(s, "a", 1)
    s.pub("a", 1, "", q, "ok-user", user_id="guest")
    s.getall("a", 1, q)


# ============================================================ F. publisher confirms and transactions

@case("confirm_basics")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "confirm.select")
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", wait=True)
    s.pub("a", 1, "", s.n("none"), "unrouted-no-mandatory", wait=True)
    s.pub("a", 1, "", s.n("none"), "unrouted-mandatory", mandatory=True, wait=True)
    s.x("a", 1, "confirm.select")
    s.pub("a", 1, "", q, "after-second-select", wait=True)
    s.getall("a", 1, q)


@case("confirm_select_nowait_and_many")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "confirm.select", no_wait=True, reply=False)
    for i in range(20):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.0)
    s.drain("a", 0.5)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("confirm_fanout_and_missing_exchange")
def _(s):
    std(s)
    x = s.n("x")
    xd(s, "a", 1, x, "fanout")
    for i in range(3):
        q = s.n(f"q{i}")
        qd(s, "a", 1, q)
        qb(s, "a", 1, q, x)
    s.x("a", 1, "confirm.select")
    s.pub("a", 1, x, "", "to-three", wait=True)
    s.pub("a", 1, s.n("missing"), "", "to-nowhere", wait=True)
    reopen(s, "a", 1)


@case("confirm_reject_publish_nack")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-max-length": 2, "x-overflow": "reject-publish"})
    s.x("a", 1, "confirm.select")
    for i in range(4):
        s.pub("a", 1, "", q, f"m{i}", wait=True)
    s.getall("a", 1, q)
    for i in range(2):
        s.pub("a", 1, "", q, f"n{i}", wait=True)


@case("confirm_tx_mode_switching")
def _(s):
    std(s)
    s.x("a", 1, "confirm.select")
    s.x("a", 1, "tx.select")
    reopen(s, "a", 1)
    s.x("a", 1, "tx.select")
    s.x("a", 1, "confirm.select")
    reopen(s, "a", 1)
    s.x("a", 1, "tx.commit")
    reopen(s, "a", 1)
    s.x("a", 1, "tx.rollback")
    reopen(s, "a", 1)


@case("tx_publish_commit_rollback")
def _(s):
    std(s)
    std(s, "b")
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "tx.select")
    s.pub("a", 1, "", q, "c1")
    s.pub("a", 1, "", q, "c2")
    s.x("b", 1, "basic.get", queue=q)
    s.x("a", 1, "tx.commit")
    s.getall("b", 1, q)
    s.pub("a", 1, "", q, "r1")
    s.x("a", 1, "tx.rollback")
    s.x("b", 1, "basic.get", queue=q)
    s.pub("a", 1, "", q, "again")
    s.x("a", 1, "tx.commit")
    s.x("a", 1, "tx.commit")
    s.getall("b", 1, q)


@case("tx_acks_and_return")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "tx.select")
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.x("a", 1, "basic.reject", delivery_tag=2, requeue=True, reply=False)
    s.x("a", 1, "tx.rollback")
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.x("a", 1, "tx.commit")
    s.pub("a", 1, "", s.n("none"), "unroutable", mandatory=True)
    s.x("a", 1, "tx.commit")
    s.getall("a", 1, q)


# ============================================================ G. dead-lettering, TTL, limits, priorities

def dlx_setup(s, qargs=None, dlq_name="dlq", dlx_type="direct", dlx_key=None):
    std(s)
    dlx, dlq, q = s.n("dlx"), s.n(dlq_name), s.n("q")
    xd(s, "a", 1, dlx, dlx_type)
    qd(s, "a", 1, dlq)
    qb(s, "a", 1, dlq, dlx, dlx_key if dlx_key is not None else q)
    args = {"x-dead-letter-exchange": dlx}
    args.update(qargs or {})
    qd(s, "a", 1, q, arguments=args)
    return dlx, dlq, q


def rewrap(v):
    """Turns the decoded JSON-ish form of a field value (timestamps, decimals, bytes) back into what the raw encoder writes."""
    if isinstance(v, dict):
        if set(v) == {"ts"}:
            return R.Ts(v["ts"])
        if set(v) == {"decimal"}:
            return R.Dec(tuple(v["decimal"]))
        if set(v) == {"bytes"}:
            return bytes.fromhex(v["bytes"])
        return {k: rewrap(x) for k, x in v.items()}
    if isinstance(v, list):
        return [rewrap(x) for x in v]
    return v


def tag_of(out):
    for e in out:
        if e.get("m") in ("basic.get-ok", "basic.deliver"):
            return e["a"]["delivery_tag"]
    return 0


@case("dlx_reject_and_x_death")
def _(s):
    dlx, dlq, q = dlx_setup(s)
    s.pub("a", 1, "", q, "victim", headers={"keep": "me"}, message_id="mid", expiration="60000")
    t = tag_of(s.x("a", 1, "basic.get", queue=q))
    s.x("a", 1, "basic.reject", delivery_tag=t, requeue=False, reply=False)
    s.getall("a", 1, dlq)
    s.pub("a", 1, "", q, "victim2")
    t = tag_of(s.x("a", 1, "basic.get", queue=q))
    s.x("a", 1, "basic.nack", delivery_tag=t, requeue=False, reply=False)
    s.pub("a", 1, "", q, "victim3")
    s.pub("a", 1, "", q, "victim4")
    s.x("a", 1, "basic.get", queue=q)
    t = tag_of(s.x("a", 1, "basic.get", queue=q))
    s.x("a", 1, "basic.nack", delivery_tag=t, multiple=True, requeue=False, reply=False)
    s.getall("a", 1, dlq)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("dlx_death_count_increments")
def _(s):
    dlx, dlq, q = dlx_setup(s)
    # bounce the dead letter back into the source queue and reject it again: the x-death entry's count grows
    s.pub("a", 1, "", q, "bouncer")
    for i in range(3):
        t = tag_of(s.x("a", 1, "basic.get", queue=q))
        s.x("a", 1, "basic.reject", delivery_tag=t, requeue=False, reply=False)
        got = s.x("a", 1, "basic.get", queue=dlq)
        hdrs = rewrap(s.last_raw[0]["props"]["headers"]) if s.last_raw and "props" in s.last_raw[0] else None
        s.x("a", 1, "basic.ack", delivery_tag=tag_of(got), reply=False)
        s.pub("a", 1, "", q, "bouncer-again" + str(i), headers=hdrs)
    s.getall("a", 1, dlq)


@case("dlx_message_ttl_expiry")
def _(s):
    dlx, dlq, q = dlx_setup(s, {"x-message-ttl": 300})
    s.pub("a", 1, "", q, "expiring", headers={"h": 1})
    s.pub("a", 1, "", q, "expiring2", expiration="60000")
    s.sleep(1.5)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.getall("a", 1, dlq)


@case("dlx_per_message_ttl")
def _(s):
    dlx, dlq, q = dlx_setup(s)
    s.pub("a", 1, "", q, "e300", expiration="300", headers={"a": 1})
    s.pub("a", 1, "", q, "e100", expiration="100")
    s.pub("a", 1, "", q, "forever")
    s.sleep(1.5)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.getall("a", 1, dlq)
    s.getall("a", 1, q)


@case("dlx_queue_and_message_ttl_min")
def _(s):
    dlx, dlq, q = dlx_setup(s, {"x-message-ttl": 5000})
    s.pub("a", 1, "", q, "shorter-msg-ttl", expiration="200")
    s.sleep(1.2)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.getall("a", 1, dlq)


@case("dlx_maxlen_drop_head")
def _(s):
    dlx, dlq, q = dlx_setup(s, {"x-max-length": 2})
    for i in range(5):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.05)
    s.sleep(0.3)
    s.getall("a", 1, dlq)
    s.getall("a", 1, q)


@case("maxlen_without_dlx")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-max-length": 3})
    for i in range(6):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.getall("a", 1, q)
    q2 = s.n("q2")
    qd(s, "a", 1, q2, arguments={"x-max-length-bytes": 10})
    for i in range(5):
        s.pub("a", 1, "", q2, "1234", quiet=0.02)
    s.x("a", 1, "queue.declare", queue=q2, passive=True, arguments={})
    s.getall("a", 1, q2)


@case("dlx_routing_key_override")
def _(s):
    std(s)
    dlx, dlq, q, other = s.n("dlx"), s.n("dlq"), s.n("q"), s.n("other")
    xd(s, "a", 1, dlx, "direct")
    qd(s, "a", 1, dlq)
    qd(s, "a", 1, other)
    qb(s, "a", 1, dlq, dlx, "special")
    qb(s, "a", 1, other, dlx, q)
    qd(s, "a", 1, q, arguments={"x-dead-letter-exchange": dlx, "x-dead-letter-routing-key": "special"})
    s.pub("a", 1, "", q, "m")
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.reject", delivery_tag=1, requeue=False, reply=False)
    s.getall("a", 1, dlq)
    s.getall("a", 1, other)


@case("dlx_cycle_dropped")
def _(s):
    std(s)
    x1, x2, q1, q2 = s.n("x1"), s.n("x2"), s.n("q1"), s.n("q2")
    xd(s, "a", 1, x1, "fanout")
    xd(s, "a", 1, x2, "fanout")
    qd(s, "a", 1, q1, arguments={"x-message-ttl": 200, "x-dead-letter-exchange": x2})
    qd(s, "a", 1, q2, arguments={"x-message-ttl": 200, "x-dead-letter-exchange": x1})
    qb(s, "a", 1, q1, x1)
    qb(s, "a", 1, q2, x2)
    s.pub("a", 1, x1, "", "loop")
    s.sleep(2.0)
    s.x("a", 1, "queue.declare", queue=q1, passive=True, arguments={})
    s.x("a", 1, "queue.declare", queue=q2, passive=True, arguments={})


@case("dlx_missing_exchange_drops")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-message-ttl": 200, "x-dead-letter-exchange": s.n("ghost")})
    s.pub("a", 1, "", q, "m")
    s.sleep(1.0)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("dlx_fanout_multiple_dlqs")
def _(s):
    dlx, dlq, q = dlx_setup(s, dlx_type="fanout", dlx_key="")
    dlq2 = s.n("dlq2")
    qd(s, "a", 1, dlq2)
    qb(s, "a", 1, dlq2, dlx, "")
    s.pub("a", 1, "", q, "m", delivery_mode=2, priority=4, content_type="text/plain", correlation_id="c")
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.reject", delivery_tag=1, requeue=False, reply=False)
    s.getall("a", 1, dlq)
    s.getall("a", 1, dlq2)


@case("ttl_queue_level_no_dlx")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-message-ttl": 300})
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.sleep(1.5)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.get", queue=q)


@case("ttl_unacked_message_not_expired")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-message-ttl": 300})
    s.pub("a", 1, "", q, "m")
    s.x("a", 1, "basic.get", queue=q)
    s.sleep(1.2)
    s.x("a", 1, "basic.reject", delivery_tag=1, requeue=True, reply=False)
    s.x("a", 1, "basic.get", queue=q)


@case("priority_queue_order")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-max-priority": 5})
    for i, p in enumerate([1, 5, 3, 5, 0, 9, 2, 4, 3, None]):
        kw = {} if p is None else {"priority": p}
        s.pub("a", 1, "", q, f"m{i}p{p}", quiet=0.02, **kw)
    s.getall("a", 1, q)
    q2 = s.n("plain")
    qd(s, "a", 1, q2)
    for i, p in enumerate([1, 5, 3]):
        s.pub("a", 1, "", q2, f"m{i}p{p}", quiet=0.02, priority=p)
    s.getall("a", 1, q2)


@case("persistence_and_redelivery_flags")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02, delivery_mode=2)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.reject", delivery_tag=1, requeue=True, reply=False)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.recover", requeue=True)
    s.getall("a", 1, q)


# ============================================================ H. more behaviours

@case("route_one_copy_per_queue_for_many_bindings")
def _(s):
    std(s)
    x, q = s.n("x"), s.n("q")
    xd(s, "a", 1, x, "topic")
    qd(s, "a", 1, q)
    for p in ("a.*", "a.b", "#", "*.b"):
        qb(s, "a", 1, q, x, p)
    s.pub("a", 1, x, "a.b", "once")
    s.getall("a", 1, q)
    x2 = s.n("x2")
    xd(s, "a", 1, x2, "fanout")
    qb(s, "a", 1, q, x2, "k1")
    qb(s, "a", 1, q, x2, "k2")
    s.pub("a", 1, x2, "k1", "fan-once")
    s.getall("a", 1, q)


@case("route_after_queue_deleted")
def _(s):
    std(s)
    x, q1, q2 = s.n("x"), s.n("q1"), s.n("q2")
    xd(s, "a", 1, x, "fanout")
    qd(s, "a", 1, q1)
    qd(s, "a", 1, q2)
    qb(s, "a", 1, q1, x)
    qb(s, "a", 1, q2, x)
    s.x("a", 1, "queue.delete", queue=q1)
    s.pub("a", 1, x, "", "after-delete", mandatory=True)
    s.getall("a", 1, q2)
    s.x("a", 1, "queue.delete", queue=q2)
    s.pub("a", 1, x, "", "nobody", mandatory=True)
    qd(s, "a", 1, q1)
    s.pub("a", 1, x, "", "fresh-queue-not-bound", mandatory=True)
    s.getall("a", 1, q1)


@case("q_exclusive_removed_on_disconnect")
def _(s):
    std(s)
    std(s, "b")
    q1, q2 = s.n("q1"), s.n("q2")
    qd(s, "a", 1, q1, durable=False, exclusive=True)
    qd(s, "a", 1, q2, durable=False, exclusive=True, auto_delete=True)
    s.pub("a", 1, "", q1, "m")
    s.x("b", 1, "queue.declare", queue=q1, passive=True, arguments={})
    reopen(s, "b", 1)
    s.conns["a"].close()
    s.sleep(1.0)
    s.x("b", 1, "queue.declare", queue=q1, passive=True, arguments={})
    reopen(s, "b", 1)
    s.x("b", 1, "queue.declare", queue=q2, passive=True, arguments={})


@case("ttl_head_only_expiry")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "long", expiration="60000")
    s.pub("a", 1, "", q, "short", expiration="200")
    s.sleep(1.2)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    got = s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("ttl_zero_and_large")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "zero", expiration="0")
    s.pub("a", 1, "", q, "big", expiration="4294967295")
    s.sleep(0.5)
    s.getall("a", 1, q)
    s.pub("a", 1, "", q, "toobig", expiration="4294967296", wait=False)
    s.sleep(0.3)
    s.getall("a", 1, q)


@case("basic_no_local_and_consume_args")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "own")
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="nl", no_local=True, arguments={"x-cancel-on-ha-failover": True})
    s.drain("a", 0.3)


@case("basic_cancel_keeps_unacked")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "basic.cancel", consumer_tag="c1")
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.x("a", 1, "basic.nack", delivery_tag=3, multiple=True, requeue=True, reply=False)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q)


@case("basic_ack_from_other_channel")
def _(s):
    std(s)
    s.chan("a", 2)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "m")
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 2, "basic.ack", delivery_tag=1, reply=False)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("basic_get_no_ack_removes")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, "m")
    s.x("a", 1, "basic.get", queue=q, no_ack=True)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)


@case("reject_publish_without_confirms_and_bytes")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-max-length": 2, "x-overflow": "reject-publish"})
    for i in range(4):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.getall("a", 1, q)
    q2 = s.n("q2")
    qd(s, "a", 1, q2, arguments={"x-max-length-bytes": 8, "x-overflow": "reject-publish"})
    s.x("a", 1, "confirm.select")
    for i in range(4):
        s.pub("a", 1, "", q2, "abc", wait=True)
    s.getall("a", 1, q2)


@case("reject_publish_with_dlx_overflow")
def _(s):
    dlx, dlq, q = dlx_setup(s, {"x-max-length": 1, "x-overflow": "reject-publish-dlx"})
    s.x("a", 1, "confirm.select")
    for i in range(3):
        s.pub("a", 1, "", q, f"m{i}", wait=True)
    s.getall("a", 1, q)
    s.getall("a", 1, dlq)


@case("names_and_keys_extremes")
def _(s):
    std(s)
    long_q = s.n("q") + "x" * (255 - len(s.n("q")))
    r = qd(s, "a", 1, long_q)
    s.pub("a", 1, "", long_q, "into-long-named")
    s.x("a", 1, "basic.get", queue=long_q, no_ack=True)
    uni = s.n("qé☃")
    qd(s, "a", 1, uni)
    s.pub("a", 1, "", uni, "unicode")
    s.x("a", 1, "basic.get", queue=uni, no_ack=True)
    x = s.n("x")
    xd(s, "a", 1, x, "direct")
    k = "k" * 255
    qb(s, "a", 1, uni, x, k)
    s.pub("a", 1, x, k, "long-key")
    s.x("a", 1, "basic.get", queue=uni, no_ack=True)
    xd(s, "a", 1, s.n("e") + "y" * 200, "fanout")


@case("conn_channel_max_and_numbers")
def _(s):
    std(s)
    s.chan("a", 2047)
    s.chan("a", 2048)


@case("conn_frame_max_below_minimum")
def _(s):
    s.conn("a", frame_max=4096)
    s.conn("b", frame_max=100)


@case("conn_frame_max_small_splits_bodies")
def _(s):
    s.conn("a", frame_max=8192)
    s.chan("a", 1)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.pub("a", 1, "", q, b"z" * 10000, delivery_mode=2)
    s.x("a", 1, "basic.get", queue=q, no_ack=True)


@case("conn_unsupported_mechanism")
def _(s):
    s.conn("a", mech="FOO")


@case("conn_bad_credentials_no_auth_on_warp")
def _(s):
    s.conn("a", password="definitely-wrong")
    s.chan("a", 1)


@case("basic_consume_two_queues_one_channel")
def _(s):
    std(s)
    q1, q2 = s.n("q1"), s.n("q2")
    qd(s, "a", 1, q1)
    qd(s, "a", 1, q2)
    for i in range(3):
        s.pub("a", 1, "", q1, f"a{i}", quiet=0.02)
        s.pub("a", 1, "", q2, f"b{i}", quiet=0.02)
    s.x("a", 1, "basic.consume", queue=q1, consumer_tag="c1", arguments={})
    s.x("a", 1, "basic.consume", queue=q2, consumer_tag="c2", arguments={})
    s.drain("a", 0.4)
    s.x("a", 1, "basic.ack", delivery_tag=6, multiple=True, reply=False)
    s.x("a", 1, "queue.declare", queue=q1, passive=True, arguments={})


@case("basic_qos_change_midstream")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(6):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=1)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c1", arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=3)
    s.drain("a", 0.4)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c2", arguments={})
    s.drain("a", 0.3)


@case("priority_queue_with_consumer")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q, arguments={"x-max-priority": 3})
    for i, p in enumerate([1, 3, 2, 3, 1]):
        s.pub("a", 1, "", q, f"m{i}p{p}", quiet=0.02, priority=p)
    s.x("a", 1, "basic.qos", prefetch_size=0, prefetch_count=2)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="c", arguments={})
    s.drain("a", 0.3)
    s.x("a", 1, "basic.ack", delivery_tag=2, multiple=True, reply=False)
    s.drain("a", 0.3)
    s.x("a", 1, "basic.reject", delivery_tag=3, requeue=True, reply=False)
    s.drain("a", 0.3)


@case("queue_delete_with_unacked_then_ack")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(2):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "queue.delete", queue=q)
    s.x("a", 1, "basic.ack", delivery_tag=1, reply=False)
    s.x("a", 1, "queue.declare", queue=q, passive=True, arguments={})


@case("exchange_declare_alternate_exchange_missing_ok")
def _(s):
    std(s)
    x = s.n("x")
    xd(s, "a", 1, x, "direct", arguments={"alternate-exchange": s.n("ghost")})
    s.pub("a", 1, x, "k", "into-void", mandatory=True)


@case("basic_consumer_tag_reuse_across_channels")
def _(s):
    std(s)
    s.chan("a", 2)
    q = s.n("q")
    qd(s, "a", 1, q)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="same", arguments={})
    s.x("a", 2, "basic.consume", queue=q, consumer_tag="same", arguments={})
    s.pub("a", 1, "", q, "m0")
    s.pub("a", 1, "", q, "m1")
    s.drain("a", 0.3)
    s.x("a", 1, "basic.consume", queue=q, consumer_tag="same", arguments={})
    reopen(s, "a", 1)
    s.x("a", 2, "basic.cancel", consumer_tag="same")
    s.x("a", 2, "basic.cancel", consumer_tag="same")


@case("basic_consume_after_purge_and_requeue_order")
def _(s):
    std(s)
    q = s.n("q")
    qd(s, "a", 1, q)
    for i in range(4):
        s.pub("a", 1, "", q, f"m{i}", quiet=0.02)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "basic.get", queue=q)
    s.x("a", 1, "queue.purge", queue=q)
    s.x("a", 1, "basic.nack", delivery_tag=2, multiple=True, requeue=True, reply=False)
    s.pub("a", 1, "", q, "m4")
    s.getall("a", 1, q)
