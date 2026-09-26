"""Raw-gRPC Pub/Sub client for the conformance harness (the Google client libraries are not installed): stubs generated from the
vendored protos live in ps_stubs/ (a namespace-package root, no __init__.py, so it merges with the installed google.protobuf)."""
import os
import sys
import time

STUBS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ps_stubs")
if STUBS not in sys.path:
    sys.path.insert(0, STUBS)

import grpc  # noqa: E402
from google.iam.v1 import iam_policy_pb2, iam_policy_pb2_grpc, policy_pb2  # noqa: E402,F401
from google.protobuf import json_format  # noqa: E402
from google.pubsub.v1 import pubsub_pb2 as pb, pubsub_pb2_grpc as pbg, schema_pb2 as sb, schema_pb2_grpc as sbg  # noqa: E402

# rpc name -> (stub attribute group, request class)
UNARY = {}
for _name in ("CreateTopic", "UpdateTopic", "Publish", "GetTopic", "ListTopics", "ListTopicSubscriptions", "ListTopicSnapshots",
              "DeleteTopic", "DetachSubscription"):
    UNARY[_name] = "pub"
for _name in ("CreateSubscription", "GetSubscription", "UpdateSubscription", "ListSubscriptions", "DeleteSubscription",
              "ModifyAckDeadline", "Acknowledge", "Pull", "ModifyPushConfig", "GetSnapshot", "ListSnapshots", "CreateSnapshot",
              "UpdateSnapshot", "DeleteSnapshot", "Seek"):
    UNARY[_name] = "sub"
for _name in ("CreateSchema", "GetSchema", "ListSchemas", "DeleteSchema", "ValidateSchema", "ValidateMessage", "ListSchemaRevisions",
              "CommitSchema", "RollbackSchema", "DeleteSchemaRevision"):
    UNARY[_name] = "sch"
for _name in ("GetIamPolicy", "SetIamPolicy", "TestIamPermissions"):
    UNARY[_name] = "iam"


def _camel(path):
    return ".".join(p[0] + p.title().replace("_", "")[1:] if False else _c(p) for p in path.split("."))


def _c(p):
    parts = p.split("_")
    return parts[0] + "".join(x.capitalize() for x in parts[1:])


class PsClient:
    def __init__(self, target, token=None):
        self.target = target
        self.channel = grpc.insecure_channel(target, options=[("grpc.max_receive_message_length", 32 * 1024 * 1024)])
        self.pub = pbg.PublisherStub(self.channel)
        self.sub = pbg.SubscriberStub(self.channel)
        self.sch = sbg.SchemaServiceStub(self.channel)
        self.iam = iam_policy_pb2_grpc.IAMPolicyStub(self.channel)
        self.metadata = [("authorization", f"Bearer {token}")] if token else None

    def close(self):
        self.channel.close()

    def request_class(self, rpc):
        stub = getattr(self, UNARY[rpc])
        method = getattr(stub, rpc)
        # the request class is what the stub's serializer belongs to: look it up by name in the generated modules
        for mod in (pb, sb, iam_policy_pb2):
            cls = getattr(mod, rpc + "Request", None)
            if cls is not None:
                return cls
        if rpc == "CreateTopic":
            return pb.Topic
        if rpc == "CreateSubscription":
            return pb.Subscription
        raise KeyError(rpc)

    def call(self, rpc, req, timeout=30):
        """req is a dict (proto JSON, snake_case or camelCase) or a message. Returns (code_name, details, response_dict|None,
        response_message|None, error_details_dict)."""
        cls = self.request_class(rpc)
        if isinstance(req, dict) and isinstance(req.get("update_mask"), dict):
            req = {**req, "update_mask": ",".join(_camel(p) for p in req["update_mask"]["paths"])}
        msg = req if not isinstance(req, dict) else json_format.ParseDict(req, cls())
        f = getattr(getattr(self, UNARY[rpc]), rpc)
        try:
            resp = f(msg, timeout=timeout, metadata=self.metadata)
            return "OK", "", json_format.MessageToDict(resp, preserving_proto_field_name=True), resp, {}
        except grpc.RpcError as e:
            info = {}
            try:
                from google.rpc import error_details_pb2  # noqa: F401
            except Exception:  # noqa: BLE001 -- google.rpc not generated here: the raw trailer is enough
                pass
            for k, v in e.trailing_metadata() or ():
                if k == "grpc-status-details-bin":
                    info["details_bin"] = v
            return e.code().name, e.details() or "", None, None, info

    def pull(self, sub, n=10, immediate=True, timeout=30):
        return self.call("Pull", {"subscription": sub, "max_messages": n, "return_immediately": immediate}, timeout)

    def streaming_pull(self, sub, ack_deadline=10, max_outstanding=1000, reqs=None, max_messages=None, timeout=5.0, ack=False,
                       stop_after=None):
        """Opens a StreamingPull, collects messages until `max_messages` arrived or `timeout` passed. With ack=True acks each
        batch on the same stream. Returns (received list of ReceivedMessage, responses list, error code name or None)."""
        import queue
        import threading
        q = queue.Queue()
        done = threading.Event()

        def gen():
            q.put(None)
            yield pb.StreamingPullRequest(subscription=sub, stream_ack_deadline_seconds=ack_deadline,
                                          max_outstanding_messages=max_outstanding)
            for r in reqs or []:
                yield json_format.ParseDict(r, pb.StreamingPullRequest()) if isinstance(r, dict) else r
            while not done.is_set():
                try:
                    item = ackq.get(timeout=0.1)
                except queue.Empty:
                    continue
                yield item

        ackq = queue.Queue()
        call = self.sub.StreamingPull(gen(), metadata=self.metadata)
        got, responses, err = [], [], None
        deadline = time.time() + timeout

        def reader():
            nonlocal err
            try:
                for r in call:
                    responses.append(r)
                    got.extend(r.received_messages)
                    if ack and r.received_messages:
                        ackq.put(pb.StreamingPullRequest(ack_ids=[m.ack_id for m in r.received_messages]))
            except grpc.RpcError as e:
                err = (e.code().name, e.details())

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        while time.time() < deadline and t.is_alive():
            if max_messages is not None and len(got) >= max_messages:
                break
            time.sleep(0.02)
        if stop_after:
            time.sleep(stop_after)
        done.set()
        call.cancel()
        t.join(2)
        return got, responses, err
