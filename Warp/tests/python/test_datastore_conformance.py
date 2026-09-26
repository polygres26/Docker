"""Warp-side tests of datastorewire (Google Cloud Datastore v1 gRPC + REST on Postgres).

* the golden corpus: ds_conformance/golden.json.gz holds the OFFICIAL Cloud Datastore emulator's answers to every step of
  ds_conformance/ds_corpus.py (recorded twice per case with `ds_harness.py --record`, unstable steps dropped); each case is replayed
  OFFLINE (no Docker) against a real Warp on one Postgres backend and on two sharded backends and must produce the same normalised
  answer, or a documented divergence (ds_known.py: the emulator is the LEGACY Datastore -- no RunAggregationQuery, IN / NOT_IN /
  NOT_EQUAL / OR, one inequality property, ancestor-only queries in transactions -- Warp implements the current Datastore API);
* tests for what the emulator cannot be the oracle for: aggregation queries, IN / NOT_IN / NOT_EQUAL / OR, entity groups on one host,
  id allocation across hosts, optimistic transaction contention, property transforms, paging 10,000 entities with cursors under a
  small heap, auth.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests.
"""
import concurrent.futures
import gzip
import json
import os
import sys

import grpc
import psycopg2
import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "ds_conformance"))
import ds_harness as H  # noqa: E402
import ds_launch_warp as L  # noqa: E402
from google.datastore.v1 import datastore_pb2 as DP, datastore_pb2_grpc as DG, entity_pb2 as E, query_pb2 as Q  # noqa: E402

with gzip.open(os.path.join(HERE, "ds_conformance", "golden.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)

MD = [("authorization", "Bearer owner")]


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


@pytest.fixture(scope="module")
def one():
    w = L.DsWarp(1, extra_env={"JAVA_TOOL_OPTIONS": "-Xmx300m"})
    yield w
    w.close()


@pytest.fixture(scope="module")
def two():
    w = L.DsWarp(2)
    yield w
    w.close()


def key(*path):
    k = E.Key()
    for kind, ident in path:
        el = k.path.add(kind=kind)
        if isinstance(ident, int):
            el.id = ident
        elif ident is not None:
            el.name = ident
    return k


def V(x):
    if isinstance(x, E.Value):
        return x
    if isinstance(x, bool):
        return E.Value(boolean_value=x)
    if isinstance(x, int):
        return E.Value(integer_value=x)
    if isinstance(x, float):
        return E.Value(double_value=x)
    if isinstance(x, str):
        return E.Value(string_value=x)
    if isinstance(x, list):
        return E.Value(array_value=E.ArrayValue(values=[V(i) for i in x]))
    raise TypeError(x)


def ent(k, **props):
    return E.Entity(key=k, properties={n: V(v) for n, v in props.items()})


class Client:
    def __init__(self, host, project=None):
        self.project = project or "t" + os.urandom(5).hex()
        self.ch = grpc.insecure_channel(host)
        self.st = DG.DatastoreStub(self.ch)

    def close(self):
        self.ch.close()

    def commit(self, *muts, txn=b"", mode=None):
        r = DP.CommitRequest(project_id=self.project, mutations=list(muts))
        if txn:
            r.transaction = txn
            r.mode = DP.CommitRequest.TRANSACTIONAL
        else:
            r.mode = mode or DP.CommitRequest.NON_TRANSACTIONAL
        return self.st.Commit(r, metadata=MD)

    def upsert(self, k, **props):
        return self.commit(DP.Mutation(upsert=ent(k, **props)))

    def lookup(self, *keys, txn=b""):
        r = DP.LookupRequest(project_id=self.project, keys=list(keys))
        if txn:
            r.read_options.transaction = txn
        return self.st.Lookup(r, metadata=MD)

    def query(self, q, txn=b"", ns=""):
        r = DP.RunQueryRequest(project_id=self.project, query=q)
        if ns:
            r.partition_id.project_id = self.project
            r.partition_id.namespace_id = ns
        if txn:
            r.read_options.transaction = txn
        return self.st.RunQuery(r, metadata=MD).batch

    def names(self, q, **kw):
        return [(e.entity.key.path[-1].name or e.entity.key.path[-1].id) for e in self.query(q, **kw).entity_results]

    def begin(self, ro=False):
        o = DP.TransactionOptions()
        if ro:
            o.read_only.SetInParent()
        else:
            o.read_write.SetInParent()
        return self.st.BeginTransaction(DP.BeginTransactionRequest(project_id=self.project, transaction_options=o), metadata=MD).transaction

    def agg(self, q, *aggs):
        r = DP.RunAggregationQueryRequest(project_id=self.project)
        r.aggregation_query.nested_query.CopyFrom(q)
        for a in aggs:
            r.aggregation_query.aggregations.add().CopyFrom(a)
        return self.st.RunAggregationQuery(r, metadata=MD).batch.aggregation_results[0].aggregate_properties


def kq(kind, **kw):
    q = Q.Query()
    if kind:
        q.kind.add(name=kind)
    for k, v in kw.items():
        if k == "limit":
            q.limit.value = v
        else:
            setattr(q, k, v)
    return q


def where(q, prop, op, value):
    """AND-adds a property filter."""
    if q.filter.WhichOneof("filter_type") is None:
        pf = q.filter.property_filter
    elif q.filter.WhichOneof("filter_type") == "property_filter":
        old = Q.PropertyFilter()
        old.CopyFrom(q.filter.property_filter)
        q.filter.Clear()
        q.filter.composite_filter.op = Q.CompositeFilter.AND
        q.filter.composite_filter.filters.add().property_filter.CopyFrom(old)
        pf = q.filter.composite_filter.filters.add().property_filter
    else:
        pf = q.filter.composite_filter.filters.add().property_filter
    pf.property.name = prop
    pf.op = op
    pf.value.CopyFrom(V(value) if not isinstance(value, E.Value) else value)
    return q


def order(q, prop, desc=False):
    q.order.add(property=Q.PropertyReference(name=prop), direction=Q.PropertyOrder.DESCENDING if desc else Q.PropertyOrder.ASCENDING)
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


# ---------------------------------------------------------------------------------------------------------------- sharding and entity groups

def test_entity_groups_stay_on_one_host_and_kinds_scatter(two):
    c = Client(two.host)
    try:
        muts = []
        for r in range(40):
            muts.append(DP.Mutation(upsert=ent(key(("Root", f"r{r}")), n=r)))
            for ch in range(3):
                muts.append(DP.Mutation(upsert=ent(key(("Root", f"r{r}"), ("Child", f"c{ch}")), n=r * 10 + ch)))
        c.commit(*muts)
        rows = [sql(pg, "SELECT root_key, count(*) FROM warp_datastore_entities WHERE ns LIKE %s GROUP BY 1", (c.project + "/%",)) for pg in two.pgs]
        assert all(len(r) > 5 for r in rows), [len(r) for r in rows]
        roots = [set(bytes(x[0]) for x in r) for r in rows]
        assert not (roots[0] & roots[1]), "an entity group must live on exactly one host"
        assert all(x[1] == 4 for r in rows for x in r)  # each root with its 3 children
        # ancestor query reads one host and returns the whole group in key order
        q = kq("Child")
        where(q, "__key__", Q.PropertyFilter.HAS_ANCESTOR, E.Value(key_value=key(("Root", "r7"))))
        assert c.names(q) == ["c0", "c1", "c2"]
        # kind query scatter-gathers in exact global key order; ordered by a property with limit/offset
        assert len(c.names(kq("Root"))) == 40
        q = order(kq("Child"), "n", desc=True)
        q.limit.value = 5
        q.offset = 2
        expect = sorted([r * 10 + ch for r in range(40) for ch in range(3)], reverse=True)[2:7]
        got = [e.entity.properties["n"].integer_value for e in c.query(q).entity_results]
        assert got == expect
    finally:
        c.close()


def test_project_named_like_a_backend_is_pinned_to_that_backend(two):
    c = Client(two.host, project="pg2")
    try:
        c.commit(*[DP.Mutation(upsert=ent(key(("Pin", f"p{i}")), i=i)) for i in range(40)])
        counts = [sql(pg, "SELECT count(*) FROM warp_datastore_entities WHERE ns LIKE 'pg2/%'")[0][0] for pg in two.pgs]
        assert counts == [0, 40], counts
        assert len(c.names(kq("Pin"))) == 40
    finally:
        c.close()


def test_allocated_ids_are_unique_across_hosts(two):
    c = Client(two.host)
    try:
        ids = set()
        for _ in range(5):
            r = c.commit(*[DP.Mutation(insert=ent(key(("Auto", None)), n=i)) for i in range(20)])
            for m in r.mutation_results:
                ids.add(m.key.path[0].id)
        assert len(ids) == 100 and 0 not in ids
        ks = c.query(kq("Auto")).entity_results
        assert len(ks) == 100
        got = c.st.AllocateIds(DP.AllocateIdsRequest(project_id=c.project, keys=[key(("Auto", None))] * 3), metadata=MD).keys
        assert len({k.path[0].id for k in got}) == 3 and not ({k.path[0].id for k in got} & ids)
    finally:
        c.close()


# ---------------------------------------------------------------------------------------------------------------- queries the emulator lacks

def test_in_not_in_not_equal_and_or_filters(one):
    c = Client(one.host)
    try:
        for i, (city, age) in enumerate([("Paris", 30), ("Rome", 25), ("Paris", 35), ("Oslo", 25), ("Rome", 40)]):
            c.upsert(key(("P", f"p{i}")), city=city, age=age)
        q = kq("P")
        where(q, "age", Q.PropertyFilter.IN, V([25, 40]))
        assert c.names(q) == ["p1", "p3", "p4"]
        q = kq("P")
        where(q, "city", Q.PropertyFilter.NOT_IN, V(["Rome", "Oslo"]))
        assert c.names(q) == ["p0", "p2"]
        q = kq("P")
        where(q, "age", Q.PropertyFilter.NOT_EQUAL, 25)
        assert c.names(q) == ["p0", "p2", "p4"]
        q = kq("P")
        q.filter.composite_filter.op = Q.CompositeFilter.OR
        for city in ("Oslo", "Paris"):
            f = q.filter.composite_filter.filters.add().property_filter
            f.property.name = "city"
            f.op = Q.PropertyFilter.EQUAL
            f.value.string_value = city
        assert c.names(q) == ["p0", "p2", "p3"]
        q = kq("P")
        where(q, "age", Q.PropertyFilter.GREATER_THAN_OR_EQUAL, 25)
        where(q, "city", Q.PropertyFilter.GREATER_THAN, "A")  # two inequality properties (lifted in the current Datastore)
        assert c.names(q) == ["p1", "p3", "p0", "p2", "p4"]
    finally:
        c.close()


def test_aggregation_queries(one):
    c = Client(one.host)
    try:
        for i in range(10):
            c.upsert(key(("A", i + 1)), v=i, g="even" if i % 2 == 0 else "odd", f=i / 2.0)
        agg = Q.AggregationQuery.Aggregation
        r = c.agg(kq("A"), agg(count=agg.Count(), alias="n"), agg(sum=agg.Sum(property=Q.PropertyReference(name="v")), alias="s"),
                  agg(avg=agg.Avg(property=Q.PropertyReference(name="v")), alias="a"))
        assert (r["n"].integer_value, r["s"].integer_value, r["a"].double_value) == (10, 45, 4.5)
        q = where(kq("A"), "g", Q.PropertyFilter.EQUAL, "odd")
        assert c.agg(q, agg(count=agg.Count(), alias="n"))["n"].integer_value == 5
        up = agg.Count()
        up.up_to.value = 3
        assert c.agg(kq("A"), agg(count=up, alias="n"))["n"].integer_value == 3
        assert c.agg(kq("A"), agg(sum=agg.Sum(property=Q.PropertyReference(name="f")), alias="s"))["s"].double_value == 22.5
        empty = where(kq("A"), "g", Q.PropertyFilter.EQUAL, "none")
        r = c.agg(empty, agg(count=agg.Count(), alias="n"), agg(avg=agg.Avg(property=Q.PropertyReference(name="v")), alias="a"))
        assert r["n"].integer_value == 0 and r["a"].WhichOneof("value_type") == "null_value"
    finally:
        c.close()


def test_property_transforms_and_mutation_preconditions(one):
    c = Client(one.host)
    try:
        k = key(("T", "t1"))
        c.upsert(k, n=5, tags=["a"])
        m = DP.Mutation(upsert=ent(k, n=5, tags=["a"]))
        t = m.property_transforms.add(property="n")
        t.increment.integer_value = 3
        t2 = m.property_transforms.add(property="tags")
        t2.append_missing_elements.values.append(V("a"))
        t2.append_missing_elements.values.append(V("b"))
        t3 = m.property_transforms.add(property="ts")
        t3.set_to_server_value = DP.PropertyTransform.REQUEST_TIME
        r = c.commit(m)
        assert r.mutation_results[0].transform_results[0].integer_value == 8
        e = c.lookup(k).found[0].entity
        assert e.properties["n"].integer_value == 8 and [v.string_value for v in e.properties["tags"].array_value.values] == ["a", "b"]
        assert e.properties["ts"].WhichOneof("value_type") == "timestamp_value"
        # base_version conflict: nothing is applied, conflict_detected reported
        ver = c.lookup(k).found[0].version
        ok = DP.Mutation(update=ent(k, n=100), base_version=ver)
        assert not c.commit(ok).mutation_results[0].conflict_detected
        stale = DP.Mutation(update=ent(k, n=200), base_version=ver)
        res = c.commit(stale).mutation_results[0]
        assert res.conflict_detected and c.lookup(k).found[0].entity.properties["n"].integer_value == 100
    finally:
        c.close()


# ---------------------------------------------------------------------------------------------------------------- transactions

def test_transaction_contention_aborts_one_and_retry_succeeds(one):
    c = Client(one.host)
    try:
        k = key(("Acct", "a"))
        c.upsert(k, balance=100)
        t1 = c.begin()
        t2 = c.begin()
        b1 = c.lookup(k, txn=t1).found[0].entity.properties["balance"].integer_value
        b2 = c.lookup(k, txn=t2).found[0].entity.properties["balance"].integer_value
        c.commit(DP.Mutation(upsert=ent(k, balance=b1 + 10)), txn=t1)
        with pytest.raises(grpc.RpcError) as e:
            c.commit(DP.Mutation(upsert=ent(k, balance=b2 + 20)), txn=t2)
        assert e.value.code() == grpc.StatusCode.ABORTED
        t3 = c.begin()
        b3 = c.lookup(k, txn=t3).found[0].entity.properties["balance"].integer_value
        assert b3 == 110
        c.commit(DP.Mutation(upsert=ent(k, balance=b3 + 20)), txn=t3)
        assert c.lookup(k).found[0].entity.properties["balance"].integer_value == 130
    finally:
        c.close()


def test_transactions_across_entity_groups_are_atomic_on_two_hosts(two):
    c = Client(two.host)
    try:
        keys = [key(("G", f"g{i}")) for i in range(12)]
        c.commit(*[DP.Mutation(upsert=ent(k, v=1)) for k in keys])
        t = c.begin()
        for k in keys:
            c.lookup(k, txn=t)
        c.upsert(keys[3], v=99)  # someone else changes one entity of the read set
        with pytest.raises(grpc.RpcError) as e:
            c.commit(*[DP.Mutation(upsert=ent(k, v=2)) for k in keys], txn=t)
        assert e.value.code() == grpc.StatusCode.ABORTED
        vals = [f.entity.properties["v"].integer_value for f in c.lookup(*keys).found]
        assert vals == [1, 1, 1, 99] + [1] * 8
        # an insert that collides rolls back the whole commit on every host
        with pytest.raises(grpc.RpcError) as e:
            c.commit(*[DP.Mutation(upsert=ent(k, v=3)) for k in keys[:6]], DP.Mutation(insert=ent(keys[7], v=3)))
        assert e.value.code() == grpc.StatusCode.ALREADY_EXISTS
        assert [f.entity.properties["v"].integer_value for f in c.lookup(*keys[:3]).found] == [1, 1, 1]
    finally:
        c.close()


def test_concurrent_transactions_serialise_increments(two):
    c = Client(two.host)
    try:
        k = key(("Cnt", "c"))
        c.upsert(k, n=0)

        def bump(_):
            done = 0
            while done < 5:
                t = c.begin()
                n = c.lookup(k, txn=t).found[0].entity.properties["n"].integer_value
                try:
                    c.commit(DP.Mutation(upsert=ent(k, n=n + 1)), txn=t)
                    done += 1
                except grpc.RpcError as e:
                    assert e.code() == grpc.StatusCode.ABORTED

        with concurrent.futures.ThreadPoolExecutor(6) as ex:
            list(ex.map(bump, range(6)))
        assert c.lookup(k).found[0].entity.properties["n"].integer_value == 30
    finally:
        c.close()


# ---------------------------------------------------------------------------------------------------------------- GQL and REST

def test_gql_with_bindings_and_disallowed_literals(one):
    c = Client(one.host)
    try:
        for i in range(6):
            c.upsert(key(("Q", f"q{i}")), n=i, s="x" if i % 2 else "y")
        r = DP.RunQueryRequest(project_id=c.project)
        r.gql_query.query_string = "SELECT * FROM Q WHERE n >= @lo AND s = @s ORDER BY n DESC LIMIT @lim"
        r.gql_query.named_bindings["lo"].value.integer_value = 2
        r.gql_query.named_bindings["s"].value.string_value = "x"
        r.gql_query.named_bindings["lim"].value.integer_value = 2
        b = c.st.RunQuery(r, metadata=MD).batch
        assert [e.entity.key.path[0].name for e in b.entity_results] == ["q5", "q3"]
        r2 = DP.RunQueryRequest(project_id=c.project)
        r2.gql_query.query_string = "SELECT * FROM Q WHERE n > 2"
        with pytest.raises(grpc.RpcError) as e:
            c.st.RunQuery(r2, metadata=MD)
        assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT and "Disallowed literal" in e.value.details()
    finally:
        c.close()


def test_rest_and_reset(one):
    c = Client(one.host)
    try:
        base = f"http://{one.host}/v1/projects/{c.project}"
        r = requests.post(f"{base}:commit", json={"mode": "NON_TRANSACTIONAL", "mutations": [{"upsert": {"key": {"path": [{"kind": "R", "name": "a"}]}, "properties": {"n": {"integerValue": "1"}}}}]})
        assert r.status_code == 200 and r.json()["mutationResults"][0]["version"]
        got = requests.post(f"{base}:lookup", json={"keys": [{"path": [{"kind": "R", "name": "a"}]}]}).json()
        assert got["found"][0]["entity"]["properties"]["n"]["integerValue"] == "1"
        assert requests.post(f"{base}:commit", json={"mode": "NON_TRANSACTIONAL", "mutations": [{"insert": {"key": {"path": [{"kind": "R", "name": "a"}]}}}]}).status_code == 409
        assert requests.post(f"http://{one.host}/reset").status_code == 200
        assert "found" not in requests.post(f"{base}:lookup", json={"keys": [{"path": [{"kind": "R", "name": "a"}]}]}).json()
    finally:
        c.close()


def test_bearer_token_list_is_enforced():
    w = L.DsWarp(1, extra_env={"WARP_DATASTOREWIRE_TOKENS": "s3cret"})
    try:
        c = Client(w.host)
        req = DP.LookupRequest(project_id="p", keys=[key(("K", "a"))])
        with pytest.raises(grpc.RpcError) as e:
            c.st.Lookup(req, metadata=[("authorization", "Bearer owner")])
        assert e.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert len(c.st.Lookup(req, metadata=[("authorization", "Bearer s3cret")]).missing) == 1
        assert requests.post(f"http://{w.host}/v1/projects/p:lookup", json={}).status_code == 401
    finally:
        w.close()


# ---------------------------------------------------------------------------------------------------------------- scale

def test_ten_thousand_entities_paged_with_cursors_under_a_small_heap(one):
    c = Client(one.host)
    try:
        for start in range(0, 10000, 500):
            c.commit(*[DP.Mutation(upsert=ent(key(("Big", i + 1)), i=i, g=i % 10, pad="x" * 100)) for i in range(start, start + 500)])
        assert c.agg(kq("Big"), Q.AggregationQuery.Aggregation(count=Q.AggregationQuery.Aggregation.Count(), alias="n"))["n"].integer_value == 10000
        seen = []
        cursor = b""
        while True:
            q = order(order(kq("Big", limit=1000), "g"), "i", desc=True)
            if cursor:
                q.start_cursor = cursor
            b = c.query(q)
            if not b.entity_results:
                break
            seen += [e.entity.key.path[0].id for e in b.entity_results]
            cursor = b.end_cursor
        assert len(seen) == 10000 and len(set(seen)) == 10000
        assert seen[0] == 9991 and seen[-1] == 10  # g=0 by i desc first (i=9990 -> id 9991); g=9 last (i=9 -> id 10)
    finally:
        c.close()
