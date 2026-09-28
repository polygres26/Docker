"""The AMQP 1.0 differential corpus of amqpwire (recorded against RabbitMQ 4.3, replayed against Warp): raw frames, see amqp10_harness.py."""
import amqp10_raw as R
from amqp10_harness import case

ACCEPTED, REJECTED, RELEASED, MODIFIED = 0x24, 0x25, 0x26, 0x27


def std(s, cn="a", **kw):
    s.conn(cn, **kw)


def sess(s, cn="a", **kw):
    s.conn(cn, **kw)          # SASL, open and begin on channel 0


def state(code, *fields):
    return R.Desc(code, list(fields))


@case("p10_open_and_sasl")
def _(s):
    s.conn("a")
    s.x("a", "close", label="close")


@case("p10_open_without_sasl_and_anonymous")
def _(s):
    s.conn("a", sasl=False)
    s.conn("b", mech="ANONYMOUS")
    s.conn("c", mech="BOGUS")


@case("p10_idle_timeout_heartbeats")
def _(s):
    s.conn("a", idle=2000, begin=False)
    s.hb("a", 3.5)


@case("p10_receiver_attach_and_errors")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_recv("a", "r-ok", f"/queues/{q}", handle=0)
    s.attach_recv("a", "r-missing", f"/queues/{s.n('nope')}", handle=1)
    s.attach_recv("a", "r-bad-addr", "/nonsense/x", handle=2)
    s.attach_recv("a", "r-bare-name", q, handle=3)
    s.attach_recv("a", "r-exchange", "/exchanges/amq.direct/k", handle=4)


@case("p10_sender_attach_and_errors")
def _(s):
    sess(s)
    q, x = s.n("q"), s.n("x")
    s.q091(q)
    s.v091("exchange.declare", exchange=x, type="direct", durable=True, arguments={})
    s.attach_send("a", "s-queue", f"/queues/{q}", handle=0)
    s.attach_send("a", "s-exchange-key", f"/exchanges/{x}/k1", handle=1)
    s.attach_send("a", "s-exchange", f"/exchanges/{x}", handle=2)
    s.attach_send("a", "s-missing-queue", f"/queues/{s.n('nope')}", handle=3)
    s.attach_send("a", "s-missing-exchange", f"/exchanges/{s.n('nox')}/k", handle=4)
    s.attach_send("a", "s-bad", "/wat", handle=5)
    s.attach_send("a", "s-anon", None, handle=6)
    s.attach_send("a", "s-default-exchange", "/exchanges//k", handle=7)


@case("p10_send_unsettled_to_queue_and_read_over_091")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    s.send("a", 0, 0, R.message(data=b"payload"))
    s.send("a", 0, 1, R.message(body="text-value"))
    s.send("a", 0, 2, R.message(data=b"", header={"durable": True}))
    s.v091("basic.get", queue=q, no_ack=True)
    s.v091("basic.get", queue=q, no_ack=True)
    s.v091("basic.get", queue=q, no_ack=True)
    s.v091("basic.get", queue=q, no_ack=True)


@case("p10_send_presettled_and_split_settle_modes")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s1", f"/queues/{q}", handle=0, snd=1)
    s.send("a", 0, 0, R.message(data=b"presettled"), settled=True)
    s.attach_send("a", "s2", f"/queues/{q}", handle=1, snd=0)
    s.send("a", 1, 1, R.message(data=b"unsettled-mode"), settled=False)
    s.send("a", 1, 2, R.message(data=b"settled-in-unsettled-mode"), settled=True)
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_routing_outcomes")
def _(s):
    sess(s)
    x, q, q2 = s.n("x"), s.n("q"), s.n("q2")
    s.v091("exchange.declare", exchange=x, type="topic", durable=True, arguments={})
    s.q091(q)
    s.q091(q2)
    s.v091("queue.bind", queue=q, exchange=x, routing_key="a.*", arguments={})
    s.attach_send("a", "s-key", f"/exchanges/{x}/a.b", handle=0)
    s.attach_send("a", "s-nokey", f"/exchanges/{x}/zzz", handle=1)
    s.attach_send("a", "s-subject", f"/exchanges/{x}", handle=2)
    s.send("a", 0, 0, R.message(data=b"routed"))
    s.send("a", 1, 1, R.message(data=b"unroutable"))
    s.send("a", 2, 2, R.message(data=b"by-subject", properties={"subject": "a.c"}))
    s.send("a", 2, 3, R.message(data=b"no-subject"))
    s.v091("queue.declare", queue=q, passive=True, arguments={})
    s.v091("queue.delete", queue=q2)
    s.attach_send("a", "s-deleted-queue", f"/queues/{q2}", handle=3)


@case("p10_anonymous_sender_uses_to")
def _(s):
    sess(s)
    q, x = s.n("q"), s.n("x")
    s.q091(q)
    s.v091("exchange.declare", exchange=x, type="fanout", durable=True, arguments={})
    s.v091("queue.bind", queue=q, exchange=x, routing_key="", arguments={})
    s.attach_send("a", "anon", None, handle=0)
    s.send("a", 0, 0, R.message(data=b"to-queue", properties={"to": f"/queues/{q}"}))
    s.send("a", 0, 1, R.message(data=b"to-exchange", properties={"to": f"/exchanges/{x}"}))
    s.send("a", 0, 2, R.message(data=b"no-to"))
    s.send("a", 0, 3, R.message(data=b"bad-to", properties={"to": "/wat"}))
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_receive_credit_and_delivery_state")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    for i in range(4):
        s.pub091("", q, f"m{i}".encode())
    s.attach_recv("a", "r", f"/queues/{q}", handle=0)
    s.flow("a", 0, 2, delivery_count=0)
    s.flow("a", 0, 1, delivery_count=2, next_out=2)
    s.disp("a", 0, 2, state=state(ACCEPTED))
    s.flow("a", 0, 5, delivery_count=3, next_out=3)
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_receiver_no_credit_gets_nothing_and_late_credit_works")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_recv("a", "r", f"/queues/{q}", handle=0)
    s.pub091("", q, b"late")
    s.drain("a", quiet=0.5)
    s.flow("a", 0, 1)


@case("p10_dispositions_and_dead_letter")
def _(s):
    sess(s)
    dlx, dlq, q = s.n("dlx"), s.n("dlq"), s.n("q")
    s.v091("exchange.declare", exchange=dlx, type="fanout", durable=True, arguments={})
    s.q091(dlq)
    s.v091("queue.bind", queue=dlq, exchange=dlx, routing_key="", arguments={})
    s.q091(q, **{"x-dead-letter-exchange": dlx})
    for i in range(6):
        s.pub091("", q, f"m{i}".encode())
    s.attach_recv("a", "r", f"/queues/{q}", handle=0)
    s.flow("a", 0, 6)
    s.disp("a", 0, state=state(ACCEPTED))
    s.disp("a", 1, state=state(REJECTED, None))
    s.disp("a", 2, state=state(RELEASED))
    s.disp("a", 3, state=state(MODIFIED, True, None, None))
    s.disp("a", 4, state=state(MODIFIED, False, True, None))
    s.disp("a", 5, state=state(MODIFIED, None, None, None))
    s.v091("queue.declare", queue=q, passive=True, arguments={})
    s.v091("queue.declare", queue=dlq, passive=True, arguments={})
    s.v091("basic.get", queue=dlq, no_ack=True)
    s.v091("basic.get", queue=dlq, no_ack=True)
    s.flow("a", 0, 10, delivery_count=6, next_out=6)


@case("p10_redelivery_header_counts")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.pub091("", q, b"one")
    s.attach_recv("a", "r", f"/queues/{q}", handle=0, credit=1)
    s.disp("a", 0, state=state(MODIFIED, True, None, None))
    s.flow("a", 0, 1, delivery_count=1, next_out=1)
    s.disp("a", 1, state=state(RELEASED))
    s.flow("a", 0, 1, delivery_count=2, next_out=2)
    s.disp("a", 2, state=state(ACCEPTED))
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_detach_and_end_requeue_unsettled")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    for i in range(3):
        s.pub091("", q, f"m{i}".encode())
    s.attach_recv("a", "r", f"/queues/{q}", handle=0, credit=3)
    s.v091("queue.declare", queue=q, passive=True, arguments={})
    s.detach("a", 0)
    s.v091("queue.declare", queue=q, passive=True, arguments={})
    s.attach_recv("a", "r2", f"/queues/{q}", handle=1, credit=2)
    s.x("a", "end", label="end")
    s.v091("queue.declare", queue=q, passive=True, arguments={})
    s.v091("basic.get", queue=q, no_ack=True)


@case("p10_connection_close_requeues_unsettled")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.pub091("", q, b"held")
    s.attach_recv("a", "r", f"/queues/{q}", handle=0, credit=1)
    s.x("a", "close", label="close")
    s.sleep(0.4)
    s.v091("basic.get", queue=q, no_ack=True)


@case("p10_drain")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    for i in range(3):
        s.pub091("", q, f"m{i}".encode())
    s.attach_recv("a", "r", f"/queues/{q}", handle=0)
    s.flow("a", 0, 10, delivery_count=0, drain=True)
    s.flow("a", 0, 5, delivery_count=13, next_out=3, drain=True)


@case("p10_presettled_receiver_acks_on_delivery")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    for i in range(3):
        s.pub091("", q, f"m{i}".encode())
    s.attach_recv("a", "r", f"/queues/{q}", handle=0, snd=1, credit=5)
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_flow_window_violation_and_unknown_handle")
def _(s):
    sess(s)
    s.flow("a", 9, 5, next_in=5)
    s.conn("b")
    s.x("b", "flow", handle=R.UInt(7), delivery_count=R.UInt(0), link_credit=R.UInt(1), next_incoming_id=R.UInt(0), incoming_window=R.UInt(10), next_outgoing_id=R.UInt(0),
        outgoing_window=R.UInt(10))


@case("p10_transfer_on_unknown_handle_and_session")
def _(s):
    sess(s)
    s.send("a", 5, 0, R.message(data=b"x"))
    s.conn("b")
    s.x("b", "attach", ch=3, name="n", handle=R.UInt(0), role=True, source=R.Desc(0x28, ["/queues/x"]), target=R.Desc(0x29, [None]))


@case("p10_exclusive_queue_is_locked")
def _(s):
    sess(s)
    q = s.n("q")
    s.v091("queue.declare", queue=q, durable=False, exclusive=True, arguments={})
    s.attach_recv("a", "r", f"/queues/{q}", handle=0)
    s.attach_send("a", "s", f"/queues/{q}", handle=1)


@case("p10_dynamic_receiver_node")
def _(s):
    sess(s)
    s.attach_recv("a", "dyn", None, handle=0, dynamic=True)


@case("p10_large_messages_and_max_message_size")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    big = bytes((i * 7) % 251 for i in range(150000))
    msg = R.message(data=big)
    half = len(msg) // 2
    s.send("a", 0, 0, msg[:half], more=True)
    s.x("a", "transfer", body=msg[half:], handle=R.UInt(0), more=False, label="second-part")
    s.v091("basic.get", queue=q, no_ack=True)
    s.attach_recv("a", "r", f"/queues/{q}", handle=1)
    s.pub091("", q, big)
    s.flow("a", 1, 1, delivery_count=0)


@case("p10_message_conversion_1_0_to_0_9_1")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    full = R.message(data=b"body", header={"durable": True, "priority": R.UByte(5), "ttl": R.UInt(60000)},
                     properties={"message_id": "id1", "user_id": R.Binary(b"guest"), "to": "/queues/x", "subject": "subj", "reply_to": "/queues/rq", "correlation_id": "c1",
                                 "content_type": R.Sym("text/plain"), "content_encoding": R.Sym("gzip"), "creation_time": R.Ts(1700000000000), "group_id": "g1"},
                     app={"s": "str", "i": 5, "neg": -7, "big": 1 << 40, "d": 2.5, "t": True, "f": False, "n": None, "sym": R.Sym("symval"), "b": R.Binary(b"\x00\x01"),
                          "ts": R.Ts(1700000000000)},
                     ann={R.Sym("x-opt"): "v", R.Sym("x-basic-type"): "typ"})
    s.send("a", 0, 0, full)
    s.v091("basic.get", queue=q, no_ack=True)
    s.send("a", 0, 1, R.message(body="hello", properties={"message_id": R.ULong(42), "correlation_id": R.Binary(b"\x01\x02")}))
    s.v091("basic.get", queue=q, no_ack=True)
    s.send("a", 0, 2, R.message(body=R.Sym("sym-body"), properties={"message_id": R.uuid.UUID("12345678-1234-5678-1234-567812345678")}))
    s.v091("basic.get", queue=q, no_ack=True)
    s.send("a", 0, 3, R.message(body={"k": "v", "n": 1}))
    s.v091("basic.get", queue=q, no_ack=True)
    s.send("a", 0, 4, R.message(body=[1, "two", 3.5]))
    s.v091("basic.get", queue=q, no_ack=True)
    s.send("a", 0, 5, R.message(header={"durable": False, "ttl": R.UInt(0)}, data=b"d"))
    s.v091("basic.get", queue=q, no_ack=True)


@case("p10_message_conversion_0_9_1_to_1_0")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_recv("a", "r", f"/queues/{q}", handle=0, credit=20)
    s.pub091("", q, b"plain")
    s.pub091("", q, b"all-props", content_type="text/plain", content_encoding="gzip", delivery_mode=2, priority=3, correlation_id="c", reply_to="rt", expiration="60000",
             message_id="m1", timestamp=1700000000, type="typ", user_id="guest", app_id="app", headers={"s": "str", "i": 5, "neg": -7, "big": R.I64(1 << 40), "d": 2.5,
                                                                                                     "t": True, "f": False, "n": None, "ts": R.Ts(1700000000),
                                                                                                     "bin": b"\x00\x01", "x-custom": "xv", "list": [1, "a"],
                                                                                                     "nested": {"a": 1}})
    s.pub091("", q, b"", delivery_mode=1)
    s.pub091("", q, "é☃".encode(), content_type="application/json")
    s.drain("a", quiet=0.5)


@case("p10_one_zero_roundtrip_keeps_the_original_sections")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    s.send("a", 0, 0, R.message(body={"a": [1, 2]}, header={"durable": True}, properties={"message_id": "x", "subject": "keep", "to": "/queues/whatever", "group_sequence": R.UInt(3),
                                                                                      "reply_to_group_id": "rg", "absolute_expiry_time": R.Ts(4102444800000)},
                                app={"k": "v"}, ann={R.Sym("x-mine"): 7}))
    s.attach_recv("a", "r", f"/queues/{q}", handle=1, credit=5)
    s.drain("a", quiet=0.5)


@case("p10_ttl_expiry_and_dead_letter")
def _(s):
    sess(s)
    dlx, dlq, q = s.n("dlx"), s.n("dlq"), s.n("q")
    s.v091("exchange.declare", exchange=dlx, type="fanout", durable=True, arguments={})
    s.q091(dlq)
    s.v091("queue.bind", queue=dlq, exchange=dlx, routing_key="", arguments={})
    s.q091(q, **{"x-dead-letter-exchange": dlx})
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    s.send("a", 0, 0, R.message(data=b"short-lived", header={"ttl": R.UInt(300)}))
    s.sleep(1.6)
    s.v091("queue.declare", queue=q, passive=True, arguments={})
    s.v091("basic.get", queue=dlq, no_ack=True)


@case("p10_two_receivers_share_a_queue")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    for i in range(4):
        s.pub091("", q, f"m{i}".encode())
    s.attach_recv("a", "r1", f"/queues/{q}", handle=0, credit=1)
    s.attach_recv("a", "r2", f"/queues/{q}", handle=1, credit=1)
    s.drain("a", quiet=0.5)


@case("p10_queue_deleted_under_a_receiver")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_recv("a", "r", f"/queues/{q}", handle=0, credit=1)
    s.v091("queue.delete", queue=q)
    s.drain("a", quiet=0.6)


@case("p10_user_id_must_match_the_login")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    s.send("a", 0, 0, R.message(data=b"forged", properties={"user_id": R.Binary(b"somebody-else")}))
    s.send("a", 0, 1, R.message(data=b"ok", properties={"user_id": R.Binary(b"guest")}))
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_settle_mode_mismatch_detaches")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s-settled-mode", f"/queues/{q}", handle=0, snd=1)
    s.send("a", 0, 0, R.message(data=b"unsettled-in-settled-mode"), settled=False)
    s.attach_send("a", "s-unsettled-mode", f"/queues/{q}", handle=1, snd=0)
    s.send("a", 1, 1, R.message(data=b"settled-in-unsettled-mode"), settled=True)
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_two_sessions_and_session_end")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.x("a", "begin", ch=1, next_outgoing_id=R.UInt(0), incoming_window=R.UInt(2000), outgoing_window=R.UInt(2000), label="second session")
    s.attach_send("a", "s1", f"/queues/{q}", handle=0, ch=0)
    s.attach_send("a", "s2", f"/queues/{q}", handle=0, ch=1)
    s.send("a", 0, 0, R.message(data=b"from-session-0"), ch=0)
    s.send("a", 0, 0, R.message(data=b"from-session-1"), ch=1)
    s.x("a", "end", ch=1, label="end second")
    s.send("a", 0, 1, R.message(data=b"still-works"), ch=0)
    s.send("a", 0, 0, R.message(data=b"session-gone"), ch=1)
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_flow_echo_and_link_stealing")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_recv("a", "same-name", f"/queues/{q}", handle=0)
    s.flow("a", 0, 3, echo=True)
    s.attach_recv("a", "same-name", f"/queues/{q}", handle=1)
    s.attach_recv("a", "other-name", f"/queues/{q}", handle=1)


@case("p10_reject_publish_overflow_outcome")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q, **{"x-max-length": 1, "x-overflow": "reject-publish"})
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    s.send("a", 0, 0, R.message(data=b"first"))
    s.send("a", 0, 1, R.message(data=b"overflow"))
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_deleted_exchange_and_queue_after_attach")
def _(s):
    sess(s)
    q, x = s.n("q"), s.n("x")
    s.q091(q)
    s.v091("exchange.declare", exchange=x, type="fanout", durable=True, arguments={})
    s.attach_send("a", "sq", f"/queues/{q}", handle=0)
    s.attach_send("a", "sx", f"/exchanges/{x}", handle=1)
    s.v091("queue.delete", queue=q)
    s.v091("exchange.delete", exchange=x)
    s.send("a", 0, 0, R.message(data=b"to-deleted-queue"))
    s.send("a", 1, 1, R.message(data=b"to-deleted-exchange"))


@case("p10_dead_lettered_message_read_over_1_0")
def _(s):
    sess(s)
    dlx, dlq, q = s.n("dlx"), s.n("dlq"), s.n("q")
    s.v091("exchange.declare", exchange=dlx, type="fanout", durable=True, arguments={})
    s.q091(dlq)
    s.v091("queue.bind", queue=dlq, exchange=dlx, routing_key="", arguments={})
    s.q091(q, **{"x-dead-letter-exchange": dlx})
    s.pub091("", q, b"victim", headers={"keep": "me"})
    s.v091("basic.get", queue=q)
    s.v091("basic.reject", delivery_tag=1, requeue=False)
    s.attach_recv("a", "r", f"/queues/{dlq}", handle=0, credit=2)


@case("p10_malformed_and_odd_transfers")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.attach_send("a", "s", f"/queues/{q}", handle=0)
    s.send("a", 0, 0, b"this is not an amqp message")
    s.send("a", 0, 1, b"")
    s.x("a", "transfer", body=R.message(data=b"format"), handle=R.UInt(0), delivery_id=R.UInt(2), delivery_tag=R.Binary(b"t2"), message_format=R.UInt(1), settled=False,
        label="other message format")
    s.x("a", "transfer", body=R.message(data=b"aborted-part"), handle=R.UInt(0), delivery_id=R.UInt(3), delivery_tag=R.Binary(b"t3"), message_format=R.UInt(0), settled=False,
        more=True, label="first part")
    s.x("a", "transfer", body=b"", handle=R.UInt(0), aborted=True, label="abort")
    s.send("a", 0, 4, R.message(data=b"after-abort"))
    s.v091("queue.declare", queue=q, passive=True, arguments={})


@case("p10_priority_and_more_conversions")
def _(s):
    sess(s)
    q, qp = s.n("q"), s.n("qp")
    s.q091(q)
    s.q091(qp, **{"x-max-priority": 9})
    s.attach_send("a", "s1", f"/queues/{q}", handle=0)
    s.attach_send("a", "s2", f"/queues/{qp}", handle=1)
    s.send("a", 0, 0, R.message(data=b"p", header={"priority": R.UByte(7)}))
    s.send("a", 1, 1, R.message(data=b"p", header={"priority": R.UByte(7)}))
    s.send("a", 1, 2, R.message(data=b"footer-and-delivery-ann", ann={R.Sym("x-a"): 1}))
    s.v091("basic.get", queue=q, no_ack=True)
    s.v091("basic.get", queue=qp, no_ack=True)
    s.v091("basic.get", queue=qp, no_ack=True)
    s.attach_recv("a", "r", f"/queues/{qp}", handle=2)
    s.pub091("", qp, b"prio", priority=4)
    s.flow("a", 2, 2)


@case("p10_receive_with_settled_transfer_when_client_wants_second_mode")
def _(s):
    sess(s)
    q = s.n("q")
    s.q091(q)
    s.pub091("", q, b"m0")
    s.attach_recv("a", "r", f"/queues/{q}", handle=0, rcv=1, credit=1)
    s.disp("a", 0, state=state(ACCEPTED), settled=False)
    s.v091("queue.declare", queue=q, passive=True, arguments={})
