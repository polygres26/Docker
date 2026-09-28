"""Warp-side tests of firestorewire (Google Cloud Firestore v1 gRPC + REST on Postgres).

* the golden corpus: fs_conformance/golden.json.gz holds the OFFICIAL Firestore emulator's answers to every step of
  fs_conformance/fs_corpus.py (recorded twice per case with `fs_harness.py --record`, unstable steps dropped); each case is replayed
  OFFLINE (no Docker) against a real Warp on one Postgres backend and on two sharded backends and must produce the same normalised
  answer, or a documented divergence (fs_known.py: the emulator is a Cloud Datastore adapter with gaps -- descending key scans,
  PartitionQuery, find_nearest, pessimistic transaction locks -- where it deviates from real Firestore Warp implements Firestore);
* the behaviours the emulator cannot be the oracle for: incremental Listen change feed, resume tokens, optimistic-transaction
  contention, sharding across hosts, collection groups at scale, vector search, auth;
* scale and robustness: a 10,000-document query paged with cursors while Warp runs with -Xmx300m, and WARP_POOL_MAX_SIZE=4 with 30 idle
  Listen streams (idle streams hold no pooled connection).

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests.
"""
import concurrent.futures
import gzip
import json
import os
import sys
import threading
import time

import grpc
import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fs_conformance"))
import fs_corpus  # noqa: E402
import fs_harness as H  # noqa: E402
import fs_launch_warp as L  # noqa: E402
from google.firestore.v1 import (common_pb2, document_pb2 as D, firestore_pb2 as F, firestore_pb2_grpc as G,  # noqa: E402,F401
                                 query_pb2 as Q, write_pb2 as W)
from google.protobuf import json_format  # noqa: E402
from google.protobuf import timestamp_pb2  # noqa: E402

with gzip.open(os.path.join(HERE, "fs_conformance", "golden.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)

MD = [("authorization", "Bearer owner")]


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


@pytest.fixture(scope="module")
def one():
    w = L.FsWarp(1, extra_env={"JAVA_TOOL_OPTIONS": "-Xmx300m"})
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.FsWarp(2)
    yield w
    w.close()


class Client:
    """A raw gRPC Firestore client on one database of a Warp."""

    def __init__(self, host, project=None):
        self.project = project or "t" + os.urandom(5).hex()
        self.db = f"projects/{self.project}/databases/(default)"
        self.ch = grpc.insecure_channel(host)
        self.st = G.FirestoreStub(self.ch)
        self.meta = MD + [("google-cloud-resource-prefix", self.db)]

    def close(self):
        self.ch.close()

    def name(self, path):
        return f"{self.db}/documents/{path}"

    def commit(self, *writes, txn=b""):
        return self.st.Commit(F.CommitRequest(database=self.db, writes=list(writes), transaction=txn), metadata=self.meta)

    def set(self, path, **fields):
        return self.commit(W.Write(update=D.Document(name=self.name(path), fields={k: val(v) for k, v in fields.items()})))

    def delete(self, path):
        return self.commit(W.Write(delete=self.name(path)))

    def get(self, path, txn=b""):
        return self.st.GetDocument(F.GetDocumentRequest(name=self.name(path), transaction=txn), metadata=self.meta)

    def query(self, q, parent="", **kw):
        req = F.RunQueryRequest(parent=self.db + "/documents" + (("/" + parent) if parent else ""), structured_query=q, **kw)
        return [r for r in self.st.RunQuery(req, metadata=self.meta)]

    def docs(self, q, parent=""):
        return [r.document.name.split("/documents/")[1] for r in self.query(q, parent) if r.HasField("document")]

    def begin(self, **opts):
        return self.st.BeginTransaction(F.BeginTransactionRequest(database=self.db, options=common_pb2.TransactionOptions(**opts)),
                                        metadata=self.meta).transaction


def val(v):
    if isinstance(v, D.Value):
        return v
    if isinstance(v, bool):
        return D.Value(boolean_value=v)
    if isinstance(v, int):
        return D.Value(integer_value=v)
    if isinstance(v, float):
        return D.Value(double_value=v)
    if isinstance(v, str):
        return D.Value(string_value=v)
    raise TypeError(v)


def coll(cid, group=False):
    return Q.StructuredQuery(**{"from": [Q.StructuredQuery.CollectionSelector(collection_id=cid, all_descendants=group)]})


def order(q, field, desc=False):
    q.order_by.add(field=Q.StructuredQuery.FieldReference(field_path=field),
                   direction=Q.StructuredQuery.DESCENDING if desc else Q.StructuredQuery.ASCENDING)
    return q


# ---------------------------------------------------------------------------------------------------------------- golden corpus

def _replay(w, case):
    bad = H.replay_golden(w.host, case, GOLDEN[case])
    assert not bad, "\n".join(f"{st}\n  expected {json.dumps(exp)[:400]}\n  got      {json.dumps(got)[:400]}" for st, exp, got in bad[:5])


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_single_backend(one, case):
    _replay(one, case)


@pytest.mark.parametrize("case", sorted(GOLDEN))
def test_golden_corpus_two_sharded_backends(two, case):
    _replay(two, case)


# ---------------------------------------------------------------------------------------------------------------- sharding

def test_documents_land_on_both_hosts_and_queries_merge(two):
    c = Client(two.host)
    try:
        c.commit(*[W.Write(update=D.Document(name=c.name(f"s/d{i:03d}"), fields={"n": val(i % 7), "i": val(i)})) for i in range(200)])
        counts = [sql(pg, "SELECT count(*) FROM warp_firestore_docs WHERE db = %s", (c.db,))[0][0] for pg in two.pgs]
        assert sum(counts) == 200 and all(n > 40 for n in counts), counts
        # exact global order across hosts, ascending and by another field, with limit and offset
        got = c.docs(coll("s"))
        assert got == [f"s/d{i:03d}" for i in range(200)]
        q = order(order(coll("s"), "n"), "i", desc=True)
        q.offset = 5
        q.limit.value = 20
        expect = sorted(range(200), key=lambda i: (i % 7, -i))[5:25]
        assert c.docs(q) == [f"s/d{i:03d}" for i in expect]
        # a get reads only the owner; a delete removes from the owner
        assert c.get("s/d042").fields["i"].integer_value == 42
        c.delete("s/d042")
        assert sum(sql(pg, "SELECT count(*) FROM warp_firestore_docs WHERE db = %s", (c.db,))[0][0] for pg in two.pgs) == 199
    finally:
        c.close()


def test_cross_host_commit_is_atomic(two):
    c = Client(two.host)
    try:
        paths = [f"a/x{i}" for i in range(12)]
        c.commit(*[W.Write(update=D.Document(name=c.name(p), fields={"v": val(1)})) for p in paths])
        # one failing precondition among many hosts' writes: nothing is applied anywhere
        writes = [W.Write(update=D.Document(name=c.name(p), fields={"v": val(2)})) for p in paths]
        writes.append(W.Write(update=D.Document(name=c.name("a/missing"), fields={"v": val(2)}), current_document=common_pb2.Precondition(exists=True)))
        with pytest.raises(grpc.RpcError) as e:
            c.commit(*writes)
        assert e.value.code() == grpc.StatusCode.NOT_FOUND
        assert all(c.get(p).fields["v"].integer_value == 1 for p in paths)
        c.commit(*writes[:-1])
        assert all(c.get(p).fields["v"].integer_value == 2 for p in paths)
    finally:
        c.close()


def test_collection_group_and_subcollections_across_hosts(two):
    c = Client(two.host)
    try:
        c.commit(*[W.Write(update=D.Document(name=c.name(f"users/u{i}/posts/p{j}"), fields={"n": val(i * 10 + j)})) for i in range(6) for j in range(3)])
        assert len(c.docs(coll("posts", group=True))) == 18
        assert c.docs(coll("posts"), parent="users/u2") == ["users/u2/posts/p0", "users/u2/posts/p1", "users/u2/posts/p2"]
        cg = order(coll("posts", group=True), "n", desc=True)
        cg.limit.value = 4
        assert c.docs(cg) == [f"users/u5/posts/p{j}" for j in (2, 1, 0)] + ["users/u4/posts/p2"]
        ids = c.st.ListCollectionIds(F.ListCollectionIdsRequest(parent=c.name("users/u1")), metadata=c.meta).collection_ids
        assert list(ids) == ["posts"]
    finally:
        c.close()


def test_project_named_like_a_backend_is_pinned_to_that_backend(two):
    # ConnectionRouter: a project (or database) id that names a backend uses only that backend's host
    c = Client(two.host, project="pg2")
    try:
        c.commit(*[W.Write(update=D.Document(name=c.name(f"pin/d{i}"), fields={"i": val(i)})) for i in range(40)])
        counts = [sql(pg, "SELECT count(*) FROM warp_firestore_docs WHERE db = %s", (c.db,))[0][0] for pg in two.pgs]
        assert counts == [0, 40], counts
        assert len(c.docs(coll("pin"))) == 40
    finally:
        c.close()


# ---------------------------------------------------------------------------------------------------------------- transactions

def test_transaction_contention_one_aborts_and_the_retry_succeeds(one):
    c = Client(one.host)
    try:
        c.set("acct/a", balance=100)
        t1 = c.begin(read_write=common_pb2.TransactionOptions.ReadWrite())
        t2 = c.begin(read_write=common_pb2.TransactionOptions.ReadWrite())
        b1 = c.get("acct/a", txn=t1).fields["balance"].integer_value
        b2 = c.get("acct/a", txn=t2).fields["balance"].integer_value
        c.commit(W.Write(update=D.Document(name=c.name("acct/a"), fields={"balance": val(b1 + 10)})), txn=t1)
        with pytest.raises(grpc.RpcError) as e:
            c.commit(W.Write(update=D.Document(name=c.name("acct/a"), fields={"balance": val(b2 + 20)})), txn=t2)
        assert e.value.code() == grpc.StatusCode.ABORTED
        # the retry reads the committed value
        t3 = c.begin(read_write=common_pb2.TransactionOptions.ReadWrite())
        b3 = c.get("acct/a", txn=t3).fields["balance"].integer_value
        assert b3 == 110
        c.commit(W.Write(update=D.Document(name=c.name("acct/a"), fields={"balance": val(b3 + 20)})), txn=t3)
        assert c.get("acct/a").fields["balance"].integer_value == 130
    finally:
        c.close()


def test_transaction_reading_a_missing_doc_aborts_when_it_is_created_meanwhile(one):
    c = Client(one.host)
    try:
        t = c.begin(read_write=common_pb2.TransactionOptions.ReadWrite())
        with pytest.raises(grpc.RpcError):
            c.get("z/new", txn=t)
        c.set("z/new", v=1)
        with pytest.raises(grpc.RpcError) as e:
            c.commit(W.Write(update=D.Document(name=c.name("z/other"), fields={"v": val(2)})), txn=t)
        assert e.value.code() == grpc.StatusCode.ABORTED
    finally:
        c.close()


def test_concurrent_increments_lose_nothing(two):
    c = Client(two.host)
    try:
        c.set("cnt/c", n=0)
        inc = W.DocumentTransform.FieldTransform(field_path="n", increment=D.Value(integer_value=1))

        def bump(_):
            for _i in range(10):
                c.commit(W.Write(update=D.Document(name=c.name("cnt/c")), update_mask=common_pb2.DocumentMask(), update_transforms=[inc]))

        with concurrent.futures.ThreadPoolExecutor(8) as ex:
            list(ex.map(bump, range(8)))
        assert c.get("cnt/c").fields["n"].integer_value == 80
    finally:
        c.close()


def test_read_only_transaction_and_read_time_snapshots(one):
    c = Client(one.host)
    try:
        r1 = c.set("snap/a", v=1)
        t = c.begin(read_only=common_pb2.TransactionOptions.ReadOnly())
        c.set("snap/a", v=2)
        assert c.get("snap/a", txn=t).fields["v"].integer_value == 1
        assert c.get("snap/a").fields["v"].integer_value == 2
        old = c.st.GetDocument(F.GetDocumentRequest(name=c.name("snap/a"), read_time=r1.commit_time), metadata=c.meta)
        assert old.fields["v"].integer_value == 1
        r3 = c.delete("snap/a")
        assert c.st.GetDocument(F.GetDocumentRequest(name=c.name("snap/a"), read_time=r1.commit_time), metadata=c.meta).fields["v"].integer_value == 1
        with pytest.raises(grpc.RpcError) as e:
            c.get("snap/a")
        assert e.value.code() == grpc.StatusCode.NOT_FOUND
        assert r3.commit_time.seconds > 0
    finally:
        c.close()


# ---------------------------------------------------------------------------------------------------------------- Listen

def listen(c, targets, n_first, timeout=10):
    """Opens a Listen stream, sends the targets, returns (queue of responses, close())."""
    import queue
    reqs = queue.Queue()
    out = queue.Queue()
    done = object()

    def gen():
        while True:
            x = reqs.get()
            if x is done:
                return
            yield x

    call = c.st.Listen(gen(), metadata=c.meta, timeout=120)

    def reader():
        try:
            for r in call:
                out.put(r)
        except grpc.RpcError:
            pass
        out.put(None)

    threading.Thread(target=reader, daemon=True).start()
    for t in targets:
        reqs.put(F.ListenRequest(database=c.db, add_target=t))

    def take(k, timeout=timeout):
        got = []
        end = time.time() + timeout
        while len(got) < k and time.time() < end:
            try:
                x = out.get(timeout=max(0.05, end - time.time()))
            except queue.Empty:
                break
            if x is None:
                break
            got.append(x)
        return got

    def close():
        reqs.put(done)
        call.cancel()

    return take, close, reqs


def qtarget(c, tid, q):
    return F.Target(query=F.Target.QueryTarget(parent=c.db + "/documents", structured_query=q), target_id=tid)


def kinds(rs):
    out = []
    for r in rs:
        t = r.WhichOneof("response_type")
        if t == "target_change":
            out.append(f"TC:{F.TargetChange.TargetChangeType.Name(r.target_change.target_change_type)}")
        elif t == "document_change":
            out.append("DC:" + r.document_change.document.name.split("/")[-1] + ":" + ",".join(map(str, r.document_change.target_ids)))
        elif t == "document_delete":
            out.append("DD:" + r.document_delete.document.split("/")[-1])
        elif t == "document_remove":
            out.append("DR:" + r.document_remove.document.split("/")[-1])
        else:
            out.append(t)
    return out


@pytest.mark.parametrize("w", ["one", "two"])
def test_listen_incremental_changes(request, w):
    warp = request.getfixturevalue(w)
    c = Client(warp.host)
    try:
        c.set("l/a", n=1)
        c.set("l/b", n=2)
        q = order(coll("l"), "n")
        take, close, _ = listen(c, [qtarget(c, 1, q)], 6)
        first = take(5)
        assert kinds(first) == ["TC:ADD", "DC:a:1", "DC:b:1", "TC:CURRENT", "TC:NO_CHANGE"]
        c.set("l/c", n=3)  # added
        assert kinds(take(2)) == ["DC:c:1", "TC:NO_CHANGE"]
        c.set("l/a", n=10)  # modified
        assert kinds(take(2)) == ["DC:a:1", "TC:NO_CHANGE"]
        c.commit(W.Write(update=D.Document(name=c.name("l/b"), fields={"other": val(1)})))  # replaced without n: no longer matches
        assert kinds(take(2)) == ["DR:b", "TC:NO_CHANGE"]
        c.delete("l/c")
        assert kinds(take(2)) == ["DD:c", "TC:NO_CHANGE"]
        c.set("elsewhere/x", n=1)  # not in the query: nothing (only the poll interval passes)
        assert take(1, timeout=1.5) == []
        close()
    finally:
        c.close()


def test_listen_window_query_and_document_target(one):
    c = Client(one.host)
    try:
        for i in range(5):
            c.set(f"w/d{i}", n=i)
        q = order(coll("w"), "n", desc=True)
        q.limit.value = 2
        dt = F.Target(documents=F.Target.DocumentsTarget(documents=[c.name("w/d0"), c.name("w/dz")]), target_id=2)
        take, close, _ = listen(c, [qtarget(c, 1, q), dt], 0)
        first = take(11)
        ks = kinds(first)
        assert ks[:4] == ["TC:ADD", "DC:d4:1", "DC:d3:1", "TC:CURRENT"], ks
        assert "DC:d0:2" in ks and "DD:dz" in ks
        c.set("w/d9", n=9)  # enters the top two, d3 falls out
        got = kinds(take(3))
        assert "DC:d9:1" in got and "DR:d3" in got, got
        close()
    finally:
        c.close()


def test_listen_resume_token_replays_only_the_difference(one):
    c = Client(one.host)
    try:
        c.set("r/a", n=1)
        c.set("r/b", n=2)
        take, close, _ = listen(c, [qtarget(c, 1, coll("r"))], 0)
        first = take(5)
        token = [r.target_change.resume_token for r in first if r.WhichOneof("response_type") == "target_change" and r.target_change.resume_token][-1]
        close()
        c.set("r/c", n=3)
        c.delete("r/a")
        t = qtarget(c, 1, coll("r"))
        t.resume_token = token
        take, close, _ = listen(c, [t], 0)
        got = kinds(take(6))
        assert "DC:c:1" in got and "DD:a" in got and "DC:b:1" not in got, got
        assert any(k.startswith("filter") or k == "filter" for k in got), got  # existence filter with the count
        close()
    finally:
        c.close()


def test_listen_receives_changes_made_through_another_warp_node(one):
    # a second Warp process on the same Postgres: its commit NOTIFYs, the first node's Listen wakes and streams the change
    from warp_test_support import WarpProcess, isolated_ports
    env = {**one.env, **isolated_ports("WARP_FIRESTOREWIRE_PORT")}
    other = WarpProcess(one.pgs[0], "WARP_FIRESTOREWIRE_PORT", frontend_name="firestorewire", extra_env=env)
    a = Client(one.host)
    b = Client(f"localhost:{other.frontend_port}", project=a.project)
    try:
        a.set("n/x", v=1)
        take, close, _ = listen(a, [qtarget(a, 1, coll("n"))], 0)
        assert kinds(take(4))[-1] == "TC:NO_CHANGE"
        b.set("n/y", v=2)
        got = kinds(take(2, timeout=8))
        assert got == ["DC:y:1", "TC:NO_CHANGE"], got
        close()
    finally:
        a.close()
        b.close()
        other.close()


# ---------------------------------------------------------------------------------------------------------------- Write stream

def test_write_stream_handshake_tokens_and_atomic_batches(one):
    c = Client(one.host)
    try:
        import queue
        reqs = queue.Queue()

        def gen():
            while True:
                x = reqs.get()
                if x is None:
                    return
                yield x

        call = c.st.Write(gen(), metadata=c.meta)
        reqs.put(F.WriteRequest(database=c.db))
        hs = next(call)
        assert hs.stream_id and hs.stream_token
        reqs.put(F.WriteRequest(stream_token=hs.stream_token, writes=[
            W.Write(update=D.Document(name=c.name("ws/a"), fields={"n": val(1)})),
            W.Write(update=D.Document(name=c.name("ws/b"), fields={"n": val(2)}))]))
        r = next(call)
        assert len(r.write_results) == 2 and r.stream_token != hs.stream_token and r.commit_time.seconds > 0
        reqs.put(F.WriteRequest(stream_token=r.stream_token, writes=[
            W.Write(update=D.Document(name=c.name("ws/c"), fields={"n": val(3)})),
            W.Write(update=D.Document(name=c.name("ws/zz"), fields={"n": val(3)}), current_document=common_pb2.Precondition(exists=True))]))
        with pytest.raises(grpc.RpcError) as e:
            next(call)
        assert e.value.code() == grpc.StatusCode.NOT_FOUND
        with pytest.raises(grpc.RpcError):
            c.get("ws/c")
    finally:
        c.close()


# ---------------------------------------------------------------------------------------------------------------- vector search, REST, auth

def test_find_nearest_vector_search(one):
    c = Client(one.host)
    try:
        def vec(*xs):
            return D.Value(map_value=D.MapValue(fields={"__type__": val("__vector__"), "value": D.Value(array_value=D.ArrayValue(values=[val(float(x)) for x in xs]))}))

        for i, v in enumerate([(0, 0), (1, 1), (5, 5), (2, 0)]):
            c.commit(W.Write(update=D.Document(name=c.name(f"vec/v{i}"), fields={"emb": vec(*v), "k": val(i)})))
        c.set("vec/none", k=9)
        q = coll("vec")
        q.find_nearest.vector_field.field_path = "emb"
        q.find_nearest.query_vector.CopyFrom(vec(0, 0))
        q.find_nearest.distance_measure = Q.StructuredQuery.FindNearest.EUCLIDEAN
        q.find_nearest.limit.value = 3
        q.find_nearest.distance_result_field = "dist"
        rs = [r.document for r in c.query(q) if r.HasField("document")]
        assert [d.name.split("/")[-1] for d in rs] == ["v0", "v1", "v3"]
        assert abs(rs[1].fields["dist"].double_value - 2 ** 0.5) < 1e-9
        q.find_nearest.distance_threshold.value = 1.5
        assert [r.document.name.split("/")[-1] for r in c.query(q) if r.HasField("document")] == ["v0", "v1"]
    finally:
        c.close()


def test_rest_and_emulator_clear_endpoint(one):
    c = Client(one.host)
    try:
        base = f"http://{one.host}/v1/{c.db}/documents"
        r = requests.patch(f"{base}/rest/a", json={"fields": {"n": {"integerValue": "1"}}}, headers={"Authorization": "Bearer owner"})
        assert r.status_code == 200 and r.json()["fields"]["n"]["integerValue"] == "1"
        q = requests.post(f"{base}:runQuery", json={"structuredQuery": {"from": [{"collectionId": "rest"}]}}).json()
        assert q[0]["document"]["name"].endswith("/rest/a") and q[-1]["done"] is True
        assert requests.get(f"{base}/rest/nope").status_code == 404
        assert requests.delete(f"http://{one.host}/emulator/v1/{c.db}/documents").status_code == 200
        assert requests.get(f"{base}/rest/a").status_code == 404
    finally:
        c.close()


def test_bearer_token_list_is_enforced(tmp_path):
    w = L.FsWarp(1, extra_env={"WARP_FIRESTOREWIRE_TOKENS": "s3cret,other"})
    try:
        ch = grpc.insecure_channel(w.host)
        st = G.FirestoreStub(ch)
        req = F.ListCollectionIdsRequest(parent="projects/p/databases/(default)/documents")
        with pytest.raises(grpc.RpcError) as e:
            st.ListCollectionIds(req, metadata=[("authorization", "Bearer owner")])
        assert e.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert st.ListCollectionIds(req, metadata=[("authorization", "Bearer s3cret")]).collection_ids == []
        assert requests.get(f"http://{w.host}/v1/projects/p/databases/(default)/documents/a/b").status_code == 401
        assert requests.get(f"http://{w.host}/v1/projects/p/databases/(default)/documents/a/b", headers={"Authorization": "Bearer other"}).status_code == 404
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------------------------- scale and robustness

def test_ten_thousand_documents_paged_with_cursors_and_bounded_memory(one):
    c = Client(one.host)
    try:
        for start in range(0, 10000, 500):
            c.commit(*[W.Write(update=D.Document(name=c.name(f"big/d{i:05d}"), fields={"i": val(i), "g": val(i % 10), "pad": val("x" * 100)}))
                       for i in range(start, start + 500)])
        # the whole collection in name order, streamed
        n = 0
        prev = ""
        for r in c.st.RunQuery(F.RunQueryRequest(parent=c.db + "/documents", structured_query=coll("big")), metadata=c.meta):
            if r.HasField("document"):
                assert r.document.name > prev
                prev = r.document.name
                n += 1
        assert n == 10000
        # cursor paging in another order, 1000 at a time
        q = order(order(coll("big"), "g"), "i", desc=True)
        seen = []
        cursor = None
        while True:
            qq = Q.StructuredQuery()
            qq.CopyFrom(q)
            qq.limit.value = 1000
            if cursor is not None:
                qq.start_at.CopyFrom(cursor)
            page = [r.document for r in c.query(qq) if r.HasField("document")]
            if not page:
                break
            seen += [d.name for d in page]
            last = page[-1]
            cursor = Q.Cursor(before=False, values=[last.fields["g"], last.fields["i"]])
        assert len(seen) == 10000 and len(set(seen)) == 10000
        assert seen[0].endswith("d09990") and seen[-1].endswith("d00009")  # g=0 by i desc first, g=9 last
    finally:
        c.close()


def test_idle_listen_streams_do_not_starve_a_tiny_pool():
    w = L.FsWarp(1, extra_env={"WARP_POOL_MAX_SIZE": "4"})
    c = Client(w.host)
    try:
        c.set("p/a", n=1)
        closers = []
        for i in range(30):
            take, close, _ = listen(c, [qtarget(c, 1, coll("p"))], 0)
            assert kinds(take(4))[-1] == "TC:NO_CHANGE"
            closers.append(close)
        # 30 open streams, pool of 4: ordinary requests still complete promptly, and streams still receive changes
        t0 = time.time()
        for i in range(20):
            c.set(f"p/x{i}", n=i)
            assert c.get(f"p/x{i}").fields["n"].integer_value == i
        assert time.time() - t0 < 10
        take2, close2, _ = listen(c, [qtarget(c, 1, coll("p"))], 0)
        assert len(take2(24)) == 24  # ADD, the 21 documents, CURRENT, NO_CHANGE
        c.set("p/late", n=99)
        assert kinds(take2(2, timeout=8)) == ["DC:late:1", "TC:NO_CHANGE"]
        for x in closers + [close2]:
            x()
    finally:
        c.close()
        w.close()
