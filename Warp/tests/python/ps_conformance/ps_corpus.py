"""Differential corpus for pubsubwire: gRPC call sequences replayed against Google's official Pub/Sub emulator (oracle) and Warp.

A case is a list of steps run in a fresh project ($P) with a per-run token ($R) in resource names. Step kinds:
  {"rpc": "CreateTopic", "req": {...}, "save": {"var": "response.path"}, "sleep": secs, "loose": True}
  {"pull": {"sub": ..., "n": 10}, "ack": True}      Pull (+ acknowledge everything received), records the data
  {"drain": {"sub": ..., "rounds": 6, "ack": True}} repeated Pull+Ack, records the data grouped by ordering key
  {"stream": {"sub": ..., "max_messages": n, "timeout": secs, "ack": True}}   StreamingPull, records data + properties
Strings may use $P (project), $R (run token) and $name (values saved by earlier steps). Data fields are base64 in the JSON.
"""
import base64


def b(s):
    return base64.b64encode(s.encode()).decode()


def T(n):
    return f"projects/$P/topics/{n}-$R"


def SUB(n):
    return f"projects/$P/subscriptions/{n}-$R"


def SNAP(n):
    return f"projects/$P/snapshots/{n}-$R"


def rpc(name, req=None, **kw):
    return {"rpc": name, "req": req or {}, **kw}


def topic(n, **kw):
    return rpc("CreateTopic", {"name": T(n), **kw})


def sub(n, t, **kw):
    return rpc("CreateSubscription", {"name": SUB(n), "topic": T(t), **kw})


def pub(t, *msgs):
    return rpc("Publish", {"topic": T(t), "messages": [m if isinstance(m, dict) else {"data": b(m)} for m in msgs]})


def pull(s, n=10, ack=True, **kw):
    return {"pull": {"sub": SUB(s), "n": n}, "ack": ack, **kw}


def m(data, **attrs):
    d = {"data": b(data)}
    ok = attrs.pop("ordering_key", None)
    if attrs:
        d["attributes"] = {k: v for k, v in attrs.items()}
    if ok:
        d["ordering_key"] = ok
    return d


CASES = {}

CASES["topic_crud"] = [
    topic("t1", labels={"env": "test", "team": "a"}),
    topic("t1"),
    rpc("GetTopic", {"topic": T("t1")}),
    rpc("GetTopic", {"topic": T("missing")}),
    rpc("GetTopic", {"topic": "garbage"}),
    topic("1abc") | {"req": {"name": "projects/$P/topics/1abc"}},
    rpc("CreateTopic", {"name": "projects/$P/topics/goog-x"}),
    rpc("CreateTopic", {"name": "projects/$P/topics/ab"}),
    rpc("CreateTopic", {"name": "projects/$P/subscriptions/x-$R"}),
    rpc("ListTopics", {"project": "projects/$P"}),
    rpc("UpdateTopic", {"topic": {"name": T("t1"), "labels": {"env": "prod"}}, "update_mask": {"paths": ["labels"]}}),
    rpc("GetTopic", {"topic": T("t1")}),
    rpc("UpdateTopic", {"topic": {"name": T("t1"), "labels": {"env": "x"}}}),
    rpc("UpdateTopic", {"topic": {"name": T("t1")}, "update_mask": {"paths": ["nonsense"]}}),
    rpc("UpdateTopic", {"topic": {"name": T("nope"), "labels": {"a": "b"}}, "update_mask": {"paths": ["labels"]}}),
    rpc("UpdateTopic", {"topic": {"name": T("t1"), "message_retention_duration": "3600s"}, "update_mask": {"paths": ["message_retention_duration"]}}),
    rpc("DeleteTopic", {"topic": T("t1")}),
    rpc("DeleteTopic", {"topic": T("t1")}),
    rpc("ListTopics", {"project": "projects/$P"}),
]

CASES["topic_list_paging"] = [topic(f"page{i}") for i in range(5)] + [
    rpc("ListTopics", {"project": "projects/$P", "page_size": 2}, save={"tok": "next_page_token"}),
    rpc("ListTopics", {"project": "projects/$P", "page_size": 2, "page_token": "$tok"}, save={"tok2": "next_page_token"}),
    rpc("ListTopics", {"project": "projects/$P", "page_size": 2, "page_token": "$tok2"}),
    rpc("ListTopics", {"project": "projects/$P", "page_size": -1}),
]

CASES["publish_basic"] = [
    topic("pb"),
    pub("pb", "hello", m("with attrs", k="v", k2="v2"), m("ordered", ordering_key="key1")),
    rpc("Publish", {"topic": T("pb"), "messages": []}),
    rpc("Publish", {"topic": T("pb"), "messages": [{}]}),
    rpc("Publish", {"topic": T("pb"), "messages": [{"attributes": {"only": "attrs"}}]}),
    pub("missing", "x"),
    rpc("Publish", {"topic": "garbage", "messages": [{"data": b("x")}]}),
    rpc("Publish", {"topic": T("pb"), "messages": [{"data": b("x"), "attributes": {"goog-reserved": "x"}}]}),
    rpc("Publish", {"topic": T("pb"), "messages": [{"data": b("x"), "attributes": {"": "x"}}]}),
    rpc("Publish", {"topic": T("pb"), "messages": [{"data": b("x")}] * 1000}),
    rpc("Publish", {"topic": T("pb"), "messages": [{"data": b("x")}] * 1001}),
]

CASES["sub_crud"] = [
    topic("s"),
    sub("a", "s"),
    sub("a", "s"),
    rpc("CreateSubscription", {"name": SUB("nt"), "topic": T("missing")}),
    rpc("CreateSubscription", {"name": "projects/$P/subscriptions/1bad", "topic": T("s")}),
    rpc("CreateSubscription", {"name": SUB("ack601"), "topic": T("s"), "ack_deadline_seconds": 601}),
    rpc("CreateSubscription", {"name": SUB("ack5"), "topic": T("s"), "ack_deadline_seconds": 5}),
    sub("b", "s", ack_deadline_seconds=30, labels={"x": "y"}, retain_acked_messages=True,
        message_retention_duration="86400s", enable_message_ordering=True),
    rpc("CreateSubscription", {"name": SUB("ret5"), "topic": T("s"), "message_retention_duration": "300s"}),
    rpc("CreateSubscription", {"name": SUB("ret8d"), "topic": T("s"), "message_retention_duration": "691200s"}),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    rpc("GetSubscription", {"subscription": SUB("b")}),
    rpc("GetSubscription", {"subscription": SUB("missing")}),
    rpc("ListSubscriptions", {"project": "projects/$P"}),
    rpc("ListTopicSubscriptions", {"topic": T("s")}),
    rpc("ListTopicSubscriptions", {"topic": T("missing")}),
    rpc("UpdateSubscription", {"subscription": {"name": SUB("a"), "ack_deadline_seconds": 60}, "update_mask": {"paths": ["ack_deadline_seconds"]}}),
    rpc("UpdateSubscription", {"subscription": {"name": SUB("a"), "ack_deadline_seconds": 700}, "update_mask": {"paths": ["ack_deadline_seconds"]}}),
    rpc("UpdateSubscription", {"subscription": {"name": SUB("a"), "labels": {"k": "v"}}, "update_mask": {"paths": ["labels"]}}),
    rpc("UpdateSubscription", {"subscription": {"name": SUB("a"), "ack_deadline_seconds": 60}}),
    rpc("UpdateSubscription", {"subscription": {"name": SUB("a"), "topic": T("s")}, "update_mask": {"paths": ["topic"]}}),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    rpc("DeleteSubscription", {"subscription": SUB("a")}),
    rpc("DeleteSubscription", {"subscription": SUB("a")}),
    rpc("ListTopicSubscriptions", {"topic": T("s")}),
    rpc("DeleteTopic", {"topic": T("s")}),
    rpc("GetSubscription", {"subscription": SUB("b")}),
    rpc("ListSubscriptions", {"project": "projects/$P"}),
]

CASES["pull_ack"] = [
    topic("pa"), sub("a", "pa"),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 10, "return_immediately": True}),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 0, "return_immediately": True}),
    rpc("Pull", {"subscription": SUB("missing"), "max_messages": 1, "return_immediately": True}),
    pub("pa", "m1", "m2", "m3"),
    pull("a", 2, ack=False, save={"a1": "received_messages.0.ack_id", "a2": "received_messages.1.ack_id"}),
    pull("a", 10, ack=False, save={"a3": "received_messages.0.ack_id"}),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 10, "return_immediately": True}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["$a1", "$a2"]}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["$a1"]}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": []}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["not-an-ack-id"]}),
    rpc("Acknowledge", {"subscription": SUB("missing"), "ack_ids": ["$a3"]}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": ["$a3"], "ack_deadline_seconds": 601}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": [], "ack_deadline_seconds": 20}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": ["$a3"], "ack_deadline_seconds": 20}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["$a3"]}),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 10, "return_immediately": True}),
]

CASES["nack_redelivery"] = [
    topic("nr"), sub("a", "nr"),
    pub("nr", "x1", "x2"),
    pull("a", 10, ack=False, save={"a1": "received_messages.0.ack_id", "a2": "received_messages.1.ack_id"}),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 10, "return_immediately": True}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": ["$a1"], "ack_deadline_seconds": 0}),
    pull("a", 10, ack=False, sleep=0.5, save={"b1": "received_messages.0.ack_id"}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": ["$a2"], "ack_deadline_seconds": 1}),
    pull("a", 10, ack=True, sleep=2.5),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["$b1"]}),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 10, "return_immediately": True}),
]

CASES["fanout_three_subs"] = [
    topic("fo"), sub("a", "fo"), sub("b", "fo"), sub("c", "fo"),
    pub("fo", "one", "two"),
    pull("a"), pull("b"), pull("c"),
    pull("a"),
    pub("fo", "three"),
    pull("b"),
]

CASES["filter"] = [
    topic("fl"),
    sub("eq", "fl", filter='attributes.color = "red"'),
    sub("has", "fl", filter="attributes:size"),
    sub("prefix", "fl", filter='hasPrefix(attributes.name, "ab")'),
    sub("not", "fl", filter='NOT attributes:color'),
    sub("combo", "fl", filter='(attributes.color = "red" OR attributes.color = "blue") AND NOT attributes:size'),
    rpc("CreateSubscription", {"name": SUB("bad1"), "topic": T("fl"), "filter": "attributes.color ="}),
    rpc("CreateSubscription", {"name": SUB("bad2"), "topic": T("fl"), "filter": "foo = 1"}),
    rpc("CreateSubscription", {"name": SUB("bad3"), "topic": T("fl"), "filter": "(attributes:x"}),
    pub("fl", m("1", color="red"), m("2", color="blue", size="L"), m("3", name="abc"), m("4", color="red", size="S"), m("5"),
        m("6", color="blue")),
    pull("eq"), pull("has"), pull("prefix"), pull("not"), pull("combo"),
    rpc("GetSubscription", {"subscription": SUB("eq")}),
]

CASES["ordering"] = [
    topic("od"), sub("a", "od", enable_message_ordering=True), sub("plain", "od"),
    pub("od", m("a1", ordering_key="A"), m("b1", ordering_key="B"), m("a2", ordering_key="A"), m("a3", ordering_key="A"),
        m("b2", ordering_key="B"), m("n1")),
    {"drain": {"sub": SUB("a"), "rounds": 8}},
    pub("od", m("a4", ordering_key="A")),
    {"drain": {"sub": SUB("a"), "rounds": 3}},
    {"drain": {"sub": SUB("plain"), "rounds": 3}},
]

CASES["ordering_redelivery_order"] = [
    topic("or"), sub("a", "or", enable_message_ordering=True),
    pub("or", m("k1", ordering_key="K"), m("k2", ordering_key="K"), m("k3", ordering_key="K")),
    pull("a", 10, ack=False, save={"a1": "received_messages.0.ack_id"}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": ["$a1"], "ack_deadline_seconds": 0}),
    pull("a", 10, ack=False, sleep=0.5),
]

CASES["dead_letter"] = [
    topic("dl"), topic("dlq"), sub("src", "dl", dead_letter_policy={"dead_letter_topic": T("dlq"), "max_delivery_attempts": 5}),
    sub("dead", "dlq"),
    rpc("CreateSubscription", {"name": SUB("bad"), "topic": T("dl"), "dead_letter_policy": {"dead_letter_topic": T("dlq"), "max_delivery_attempts": 3}}),
    rpc("CreateSubscription", {"name": SUB("bad2"), "topic": T("dl"), "dead_letter_policy": {"dead_letter_topic": T("nodlq"), "max_delivery_attempts": 5}}),
    rpc("GetSubscription", {"subscription": SUB("src")}),
    pub("dl", m("poison", k="v")),
    {"nack_loop": {"sub": SUB("src"), "times": 5}},
    rpc("Pull", {"subscription": SUB("src"), "max_messages": 10, "return_immediately": True}),
    pull("dead", 10, ack=False),
]

CASES["retry_policy"] = [
    topic("rp"),
    sub("a", "rp", retry_policy={"minimum_backoff": "2s", "maximum_backoff": "10s"}),
    rpc("CreateSubscription", {"name": SUB("bad"), "topic": T("rp"), "retry_policy": {"minimum_backoff": "20s", "maximum_backoff": "10s"}}),
    rpc("CreateSubscription", {"name": SUB("bad2"), "topic": T("rp"), "retry_policy": {"minimum_backoff": "700s"}}),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    pub("rp", "r1"),
    pull("a", 10, ack=False, save={"a1": "received_messages.0.ack_id"}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": ["$a1"], "ack_deadline_seconds": 0}),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 10, "return_immediately": True}),
    pull("a", 10, ack=True, sleep=2.6),
]

CASES["snapshot_seek"] = [
    topic("sn"), sub("a", "sn"), sub("b", "sn"),
    pub("sn", "s1", "s2", "s3"),
    pull("a", 10, ack=True),
    rpc("CreateSnapshot", {"name": SNAP("snap1"), "subscription": SUB("a"), "labels": {"l": "v"}}),
    rpc("CreateSnapshot", {"name": SNAP("snap1"), "subscription": SUB("a")}),
    rpc("CreateSnapshot", {"name": SNAP("snap2"), "subscription": SUB("missing")}),
    rpc("GetSnapshot", {"snapshot": SNAP("snap1")}),
    rpc("GetSnapshot", {"snapshot": SNAP("nosnap")}),
    rpc("ListSnapshots", {"project": "projects/$P"}),
    rpc("ListTopicSnapshots", {"topic": T("sn")}),
    pub("sn", "s4", "s5"),
    pull("a", 10, ack=True),
    rpc("Seek", {"subscription": SUB("a"), "snapshot": SNAP("snap1")}),
    pull("a", 10, ack=True),
    rpc("Seek", {"subscription": SUB("a"), "snapshot": SNAP("nosnap")}),
    rpc("Seek", {"subscription": SUB("a")}),
    rpc("Seek", {"subscription": SUB("missing"), "snapshot": SNAP("snap1")}),
    rpc("UpdateSnapshot", {"snapshot": {"name": SNAP("snap1"), "labels": {"n": "1"}}, "update_mask": {"paths": ["labels"]}}),
    rpc("GetSnapshot", {"snapshot": SNAP("snap1")}),
    rpc("DeleteSnapshot", {"snapshot": SNAP("snap1")}),
    rpc("DeleteSnapshot", {"snapshot": SNAP("snap1")}),
]

CASES["seek_time"] = [
    topic("st"), sub("a", "st", retain_acked_messages=True),
    pub("st", "old1", "old2"),
    {"mark": "t0", "sleep": 1.2},
    pub("st", "new1", "new2"),
    pull("a", 10, ack=True),
    rpc("Seek", {"subscription": SUB("a"), "time": "$t0"}),
    pull("a", 10, ack=True),
    rpc("Seek", {"subscription": SUB("a"), "time": "2099-01-01T00:00:00Z"}),
    pull("a", 10, ack=True),
]

CASES["detach"] = [
    topic("dt"), sub("a", "dt"),
    pub("dt", "d1"),
    rpc("DetachSubscription", {"subscription": SUB("a")}),
    rpc("DetachSubscription", {"subscription": SUB("missing")}),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 1, "return_immediately": True}),
    rpc("ListTopicSubscriptions", {"topic": T("dt")}),
    pub("dt", "d2"),
]

CASES["push_config"] = [
    topic("pc"), sub("a", "pc"),
    rpc("ModifyPushConfig", {"subscription": SUB("a"), "push_config": {"push_endpoint": "https://example.com/push",
        "attributes": {"x-goog-version": "v1"}, "oidc_token": {"service_account_email": "sa@example.com", "audience": "aud"}}}),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    rpc("ModifyPushConfig", {"subscription": SUB("a"), "push_config": {"push_endpoint": "not a url"}}),
    rpc("ModifyPushConfig", {"subscription": SUB("missing"), "push_config": {}}),
    rpc("ModifyPushConfig", {"subscription": SUB("a"), "push_config": {}}),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    rpc("CreateSubscription", {"name": SUB("p2"), "topic": T("pc"), "push_config": {"push_endpoint": "https://example.com/p2",
        "no_wrapper": {"write_metadata": True}}}),
    rpc("GetSubscription", {"subscription": SUB("p2")}),
]

CASES["exactly_once"] = [
    topic("eo"), sub("a", "eo", enable_exactly_once_delivery=True),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    pub("eo", "e1", "e2"),
    pull("a", 10, ack=False, save={"a1": "received_messages.0.ack_id", "a2": "received_messages.1.ack_id"}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["$a1"]}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["$a1"]}),
    rpc("ModifyAckDeadline", {"subscription": SUB("a"), "ack_ids": ["$a1"], "ack_deadline_seconds": 30}),
    rpc("Acknowledge", {"subscription": SUB("a"), "ack_ids": ["$a2", "bogus"]}),
]

CASES["streaming_pull"] = [
    topic("sp"), sub("a", "sp"),
    pub("sp", "p1", "p2", "p3", "p4", "p5"),
    {"stream": {"sub": SUB("a"), "max_messages": 5, "timeout": 5.0, "ack": True}},
    rpc("Pull", {"subscription": SUB("a"), "max_messages": 10, "return_immediately": True}),
    {"stream": {"sub": SUB("missing"), "max_messages": 1, "timeout": 2.0}},
    {"stream": {"sub": SUB("a"), "max_messages": 1, "timeout": 1.5}},
]

CASES["streaming_flow_control"] = [
    topic("sf"), sub("a", "sf"),
    pub("sf", *[f"f{i}" for i in range(10)]),
    {"stream": {"sub": SUB("a"), "max_messages": 10, "timeout": 2.5, "ack": False, "max_outstanding": 3}},
]

CASES["iam"] = [
    topic("im"), sub("a", "im"),
    rpc("GetIamPolicy", {"resource": T("im")}),
    rpc("SetIamPolicy", {"resource": T("im"), "policy": {"bindings": [{"role": "roles/pubsub.publisher", "members": ["user:a@example.com"]}]}}),
    rpc("GetIamPolicy", {"resource": T("im")}),
    rpc("TestIamPermissions", {"resource": SUB("a"), "permissions": ["pubsub.subscriptions.consume", "pubsub.subscriptions.get"]}),
    rpc("GetIamPolicy", {"resource": T("missing")}),
]

AVRO = '{"type":"record","name":"Rec","fields":[{"name":"a","type":"string"},{"name":"n","type":"int"}]}'
CASES["schema"] = [
    rpc("CreateSchema", {"parent": "projects/$P", "schema_id": "sch-$R", "schema": {"type": "AVRO", "definition": AVRO}}),
    rpc("CreateSchema", {"parent": "projects/$P", "schema_id": "sch-$R", "schema": {"type": "AVRO", "definition": AVRO}}),
    rpc("CreateSchema", {"parent": "projects/$P", "schema_id": "bad-$R", "schema": {"type": "AVRO", "definition": "{not avro"}}),
    rpc("GetSchema", {"name": "projects/$P/schemas/sch-$R"}),
    rpc("GetSchema", {"name": "projects/$P/schemas/none-$R"}),
    rpc("ListSchemas", {"parent": "projects/$P"}),
    rpc("ValidateSchema", {"parent": "projects/$P", "schema": {"type": "AVRO", "definition": AVRO}}),
    rpc("ValidateSchema", {"parent": "projects/$P", "schema": {"type": "AVRO", "definition": "nope"}}),
    rpc("ValidateMessage", {"parent": "projects/$P", "name": "projects/$P/schemas/sch-$R", "encoding": "JSON",
                            "message": b('{"a":"x","n":3}')}),
    rpc("ValidateMessage", {"parent": "projects/$P", "name": "projects/$P/schemas/sch-$R", "encoding": "JSON",
                            "message": b('{"a":"x"}')}),
    rpc("CreateTopic", {"name": T("st"), "schema_settings": {"schema": "projects/$P/schemas/sch-$R", "encoding": "JSON"}}),
    rpc("CreateTopic", {"name": T("st2"), "schema_settings": {"schema": "projects/$P/schemas/none-$R", "encoding": "JSON"}}),
    pub("st", b'{"a":"x","n":3}'.decode()),
    pub("st", "not json"),
    rpc("DeleteSchema", {"name": "projects/$P/schemas/sch-$R"}),
]

CASES["topic_delete_keeps_subs"] = [
    topic("dk"), sub("a", "dk"),
    pub("dk", "keep"),
    rpc("DeleteTopic", {"topic": T("dk")}),
    rpc("GetSubscription", {"subscription": SUB("a")}),
    pull("a", 10, ack=True),
    pub("dk", "x"),
    rpc("CreateSubscription", {"name": SUB("b"), "topic": T("dk")}),
]

CASES["message_fields"] = [
    topic("mf"), sub("a", "mf"),
    pub("mf", m("full", k="v", ordering_key="ok")),
    pull("a", 10, ack=True),
    pub("mf", {"data": b("only data")}),
    pull("a", 10, ack=True),
    pub("mf", {"attributes": {"a": "1"}}),
    pull("a", 10, ack=True),
]
