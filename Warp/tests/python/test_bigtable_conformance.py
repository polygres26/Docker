"""Warp-side tests of bigtablewire (Google Cloud Bigtable gRPC data + table admin API on Postgres).

* the golden corpus: bt_conformance/golden.json.gz holds Google's official Bigtable emulator's answers to every step of
  bt_conformance/bt_corpus.py (each case recorded three times with `bt_harness.py --record`; unstable steps dropped). Each case is
  replayed OFFLINE (no Docker) against a real Warp on one Postgres backend and on two sharded backends (with a tiny scan page size
  so every multi-page path runs). A step must answer like the emulator, or be a documented divergence (bt_known.py: where the
  emulator is lenient or incomplete, Warp implements Cloud Bigtable);
* the behaviour the emulator cannot be the oracle for: reversed scans, garbage collection of column family rules, chunked responses
  of large cells, concurrent ReadModifyWriteRow / CheckAndMutateRow exactness, SampleRowKeys;
* sharding: rows land on BOTH Postgres hosts, reads merge the shards in key order (also reversed and with a row limit), row-level
  writes stay on one host, DropRowRange / DeleteTable / dropped families clean every host;
* WARP_POOL_MAX_SIZE=4 with 30 stalled ReadRows streams must not starve other requests; bearer tokens.

Needs WARP_TEST_PG_LOCAL=1 (native Postgres) or Docker like the other Warp tests. The Google client libraries are not installed;
tests use raw gRPC stubs generated from the vendored protos (bt_conformance/bt_stubs).
"""
import base64
import concurrent.futures
import gzip
import json
import os
import sys
import threading
import time

import psycopg2
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "bt_conformance"))
import bt_client as C  # noqa: E402
import bt_corpus  # noqa: E402
import bt_harness as H  # noqa: E402
import bt_launch_warp as L  # noqa: E402

with gzip.open(os.path.join(HERE, "bt_conformance", "golden.json.gz"), "rt") as _f:
    GOLDEN = json.load(_f)

_n = [0]


def uniq(prefix="x"):
    _n[0] += 1
    return f"{prefix}{int(time.time() * 1000) % 10**9}n{_n[0]}"


def sql(pg, statement, args=None):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname="postgres") as c:
        cur = c.cursor()
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def b64(s):
    return base64.b64encode(s.encode() if isinstance(s, str) else s).decode()


def unb64(s):
    return base64.b64decode(s or "")


def ok(res):
    code, msg = res[0], res[1]
    assert code == "OK", (code, msg)
    return res[2]


def assemble(body):
    """ReadRows responses -> [(row key, [(family, qualifier, ts, value, labels)])]."""
    rows, _ = H.assemble(body)
    return [(r["k"], [(f, q, ts, v, lab) for f, q, ts, v, lab in r["cells"]]) for r in rows]


class Env:
    """A project/instance with a few table helpers over a BtClient."""

    def __init__(self, warp, target=None):
        self.warp = warp
        self.c = C.BtClient(target or f"localhost:{warp.grpc_port}")
        self.parent = f"projects/{uniq('p')}/instances/inst"

    def table(self, fams=("cf",), name=None, **kw):
        name = name or uniq("t")
        fam = fams if isinstance(fams, dict) else {f: {} for f in fams}
        ok(self.c.call("CreateTable", {"parent": self.parent, "table_id": name, "table": {"column_families": fam, **kw}}))
        return f"{self.parent}/tables/{name}"

    def set(self, t, row, fam, q, v, ts=-1):
        ok(self.c.call("MutateRow", {"table_name": t, "row_key": b64(row), "mutations": [
            {"set_cell": {"family_name": fam, "column_qualifier": b64(q), "timestamp_micros": ts, "value": b64(v)}}]}))

    def bulk(self, t, n, fam="cf", prefix="k", width=6, value=lambda i: f"v{i}", cols=("q",), ts=1000):
        for lo in range(0, n, 500):
            entries = [{"row_key": b64(f"{prefix}{i:0{width}d}"), "mutations": [
                {"set_cell": {"family_name": fam, "column_qualifier": b64(c), "timestamp_micros": ts, "value": b64(value(i))}} for c in cols]}
                for i in range(lo, min(n, lo + 500))]
            res = ok(self.c.call("MutateRows", {"table_name": t, "entries": entries}))
            assert all(int(e.get("status", {}).get("code", 0) or 0) == 0 for m in res for e in m["entries"])

    def read(self, t, **kw):
        return assemble(ok(self.c.call("ReadRows", {"table_name": t, **kw})))

    def keys(self, t, **kw):
        return [k.decode() for k, _ in self.read(t, **kw)]

    def close(self):
        self.c.close()


# one backend: the tweaks make the paging (25 cells per page), the GC sweep (1 s) and SampleRowKeys (2000 bytes) testable
@pytest.fixture(scope="class")
def one():
    w = L.BtWarp(1, extra_env={"WARP_BIGTABLEWIRE_SCAN_PAGE_CELLS": "25", "WARP_BIGTABLEWIRE_GC_INTERVAL_SECONDS": "1",
                               "WARP_BIGTABLEWIRE_SAMPLE_BYTES": "2000"})
    yield w
    w.close()


@pytest.fixture(scope="class")
def two():
    w = L.BtWarp(2, extra_env={"WARP_BIGTABLEWIRE_SCAN_PAGE_CELLS": "25", "WARP_BIGTABLEWIRE_SAMPLE_BYTES": "2000"})
    yield w
    w.close()


@pytest.fixture(scope="class")
def strict():
    w = L.BtWarp(1, extra_env={"WARP_POOL_MAX_SIZE": "4", "WARP_BIGTABLEWIRE_TOKENS": "bt-test-token,other-token"})
    yield w
    w.close()


def _replay(w, case):
    client = C.BtClient(f"localhost:{w.grpc_port}")
    try:
        ident, known, unexpected = H.replay_golden(GOLDEN, client, only=case)
    finally:
        client.close()
    compared = sum(1 for s in GOLDEN["cases"][case] if s is not None)
    assert ident + known + len(unexpected) == compared, "not every step was compared"
    assert not unexpected, "\n".join(f"{n}#{i} [{k}]\n  emulator {json.dumps(g)[:500]}\n  warp     {json.dumps(x)[:500]}"
                                     for n, i, k, g, x in unexpected[:5])
    return ident, known


class TestSingleBackend:
    @pytest.mark.parametrize("case", sorted(GOLDEN["cases"]))
    def test_golden_corpus(self, one, case):
        _replay(one, case)

    def test_golden_corpus_is_the_whole_corpus(self):
        assert set(GOLDEN["cases"]) == set(bt_corpus.CASES)
        assert all(len(GOLDEN["cases"][n]) == len(s) for n, s in bt_corpus.CASES.items())

    def test_every_known_divergence_has_a_reason(self):
        import bt_known
        for k, (kind, reason) in bt_known.KNOWN.items():
            assert kind in ("msg", "body", "code", "any") and len(reason) > 20, k
            case = k.split("#")[0]
            assert case in bt_corpus.CASES, k

    # ------------------------------------------------------------------------------------------ reads

    def test_reversed_scans(self, one):
        env = Env(one)
        t = env.table(("cf", "cf2"))
        env.bulk(t, 60, cols=("a", "b"))
        for i in (5, 6):
            env.set(t, f"k{i:06d}", "cf2", "z", "extra", 1000)
        fwd = env.keys(t)
        assert fwd == sorted(fwd) and len(fwd) == 60
        assert env.keys(t, reversed=True) == fwd[::-1]
        rng = {"row_ranges": [{"start_key_closed": b64("k000010"), "end_key_open": b64("k000020")}]}
        assert env.keys(t, rows=rng, reversed=True) == fwd[10:20][::-1]
        assert env.keys(t, rows=rng, reversed=True, rows_limit=3) == fwd[10:20][::-1][:3]
        pts = {"row_keys": [b64(k) for k in ("k000003", "k000001", "k000050", "nope")]}
        assert env.keys(t, rows=pts, reversed=True) == ["k000050", "k000003", "k000001"]
        # cells of a reversed row keep the standard order (family, qualifier, newest first)
        row = env.read(t, rows={"row_keys": [b64("k000005")]}, reversed=True)[0][1]
        assert [(f, q) for f, q, *_ in row] == [("cf", b"a"), ("cf", b"b"), ("cf2", b"z")]

    def test_paging_reads_rows_completely_across_page_boundaries(self, one):
        env = Env(one)
        t = env.table()
        env.bulk(t, 300, cols=[f"c{j:02d}" for j in range(7)])  # 7 cells per row, 25 per page: rows straddle pages
        rows = env.read(t)
        assert len(rows) == 300
        assert all(len(cells) == 7 for _, cells in rows)
        assert [k.decode() for k, _ in rows] == [f"k{i:06d}" for i in range(300)]
        big = env.read(t, rows={"row_ranges": [{"start_key_closed": b64("k000100"), "end_key_open": b64("k000200")}]}, rows_limit=40)
        assert len(big) == 40 and big[0][0] == b"k000100"
        keys = [f"k{i:06d}" for i in range(0, 300, 3)]
        assert env.keys(t, rows={"row_keys": [b64(k) for k in keys]}) == keys

    def test_wide_row_with_many_versions_is_assembled_from_many_pages(self, one):
        env = Env(one)
        t = env.table()
        for v in range(1, 61):
            env.set(t, "w", "cf", "q", f"v{v}", v * 1000)
        cells = env.read(t)[0][1]
        assert [ts for _, _, ts, *_ in cells] == [v * 1000 for v in range(60, 0, -1)]
        assert cells[0][3] == b"v60"

    def test_chunk_stream_is_compact_and_splits_large_values(self, one):
        env = Env(one)
        t = env.table(("cf", "cf2"))
        env.set(t, "a", "cf", "q1", "1", 1000)
        env.set(t, "a", "cf", "q1", "0", 500 * 1000)
        env.set(t, "a", "cf", "q2", "2", 1000)
        env.set(t, "a", "cf2", "q2", "3", 1000)
        env.set(t, "b", "cf", "q1", "x" * (2 * 1024 * 1024 + 5), 1000)
        env.set(t, "c", "cf", "q1", "4", 1000)
        msgs = ok(env.c.call("ReadRows", {"table_name": t}))
        chunks = [ch for m in msgs for ch in m.get("chunks", [])]
        # row key only on a row's first chunk; family/qualifier only when they change; commit_row ends every row
        firsts = [ch for ch in chunks if ch.get("row_key")]
        assert [unb64(ch["row_key"]) for ch in firsts] == [b"a", b"b", b"c"]
        a = chunks[:4]
        assert a[0]["family_name"] == "cf" and unb64(a[0]["qualifier"]) == b"q1"
        assert "family_name" not in a[1] and "qualifier" not in a[1]  # older version of the same column
        assert "family_name" not in a[2] and unb64(a[2]["qualifier"]) == b"q2"
        assert a[3]["family_name"] == "cf2" and unb64(a[3]["qualifier"]) == b"q2" and a[3]["commit_row"]
        assert sum(1 for ch in chunks if ch.get("commit_row")) == 3
        big = [ch for ch in chunks if ch.get("value_size")]
        assert big and all(int(ch["value_size"]) == 2 * 1024 * 1024 + 5 for ch in big)
        assert sum(len(unb64(ch.get("value", ""))) for ch in chunks[4:-1]) == 2 * 1024 * 1024 + 5
        rows = assemble(msgs)
        assert [k for k, _ in rows] == [b"a", b"b", b"c"] and len(rows[1][1][0][3]) == 2 * 1024 * 1024 + 5

    def test_sample_row_keys_cover_the_table(self, one):
        env = Env(one)
        t = env.table()
        env.bulk(t, 400, value=lambda i: "v" * 40)
        res = ok(env.c.call("SampleRowKeys", {"table_name": t}))
        keys = [unb64(m.get("row_key", "")) for m in res]
        offs = [int(m.get("offset_bytes", 0) or 0) for m in res]
        assert len(res) > 3 and keys[-1] == b"" and keys[:-1] == sorted(set(keys[:-1]))
        assert offs == sorted(offs) and offs[-1] >= 400 * 40
        assert all(k.startswith(b"k") for k in keys[:-1])

    # ------------------------------------------------------------------------------------------ garbage collection

    def test_gc_max_versions_keeps_the_newest_versions_only(self, one):
        env = Env(one)
        t = env.table({"cf": {"gc_rule": {"max_num_versions": 2}}, "keep": {}})
        for v in range(1, 6):
            env.set(t, "r", "cf", "q", f"v{v}", v * 1000)
            env.set(t, "r", "keep", "q", f"v{v}", v * 1000)
        end = time.time() + 20
        while time.time() < end:
            cells = env.read(t)[0][1]
            if sum(1 for c in cells if c[0] == "cf") == 2:
                break
            time.sleep(0.3)
        cells = env.read(t)[0][1]
        assert [(c[2], c[3]) for c in cells if c[0] == "cf"] == [(5000, b"v5"), (4000, b"v4")]
        assert sum(1 for c in cells if c[0] == "keep") == 5

    def test_gc_max_age_union_and_intersection(self, one):
        env = Env(one)
        now = int(time.time() * 1000) * 1000
        h = 3600 * 1_000_000
        t = env.table({"age": {"gc_rule": {"max_age": "3600s"}},
                       "union": {"gc_rule": {"union": {"rules": [{"max_num_versions": 3}, {"max_age": "3600s"}]}}},
                       "inter": {"gc_rule": {"intersection": {"rules": [{"max_num_versions": 1}, {"max_age": "3600s"}]}}}})
        for fam in ("age", "union", "inter"):
            for i, ts in enumerate((now - 5 * h, now - 4 * h, now - 10_000_000, now - 5_000_000, now - 1_000_000)):
                env.set(t, "r", fam, "q", f"{fam}{i}", ts // 1000 * 1000)
        want = {"age": 3, "union": 3, "inter": 3}  # age: 3 recent; union: recent 3 (old ones exceed either rule); inter: old AND beyond newest
        end = time.time() + 20
        while time.time() < end:
            got = {f: sum(1 for c in env.read(t)[0][1] if c[0] == f) for f in want}
            if got == want:
                break
            time.sleep(0.3)
        assert got == want, got
        # an intersection removes only cells that are both too old and beyond the newest version: the 2 old ones go, 3 recent stay
        vals = [c[3] for c in env.read(t)[0][1] if c[0] == "inter"]
        assert vals == [b"inter4", b"inter3", b"inter2"]

    def test_family_gc_rule_change_applies_to_existing_cells(self, one):
        env = Env(one)
        t = env.table(("cf",))
        for v in range(1, 5):
            env.set(t, "r", "cf", "q", f"v{v}", v * 1000)
        ok(env.c.call("ModifyColumnFamilies", {"name": t, "modifications": [{"id": "cf", "update": {"gc_rule": {"max_num_versions": 1}}}]}))
        end = time.time() + 20
        while time.time() < end and len(env.read(t)[0][1]) != 1:
            time.sleep(0.3)
        assert [c[3] for c in env.read(t)[0][1]] == [b"v4"]

    # ------------------------------------------------------------------------------------------ concurrency

    def test_concurrent_increments_are_exact(self, one):
        env = Env(one)
        t = env.table()

        def work(_):
            c = C.BtClient(f"localhost:{one.grpc_port}")
            for _ in range(25):
                ok(c.call("ReadModifyWriteRow", {"table_name": t, "row_key": b64("ctr"), "rules": [
                    {"family_name": "cf", "column_qualifier": b64("n"), "increment_amount": 1}]}))
            c.close()

        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            list(pool.map(work, range(8)))
        cells = env.read(t, filter={"cells_per_column_limit_filter": 1})[0][1]
        assert int.from_bytes(cells[0][3], "big") == 200

    def test_concurrent_appends_lose_nothing(self, one):
        env = Env(one)
        t = env.table()

        def work(i):
            c = C.BtClient(f"localhost:{one.grpc_port}")
            for _ in range(10):
                ok(c.call("ReadModifyWriteRow", {"table_name": t, "row_key": b64("s"), "rules": [
                    {"family_name": "cf", "column_qualifier": b64("q"), "append_value": b64(chr(ord("a") + i))}]}))
            c.close()

        with concurrent.futures.ThreadPoolExecutor(6) as pool:
            list(pool.map(work, range(6)))
        val = env.read(t, filter={"cells_per_column_limit_filter": 1})[0][1][0][3].decode()
        assert len(val) == 60 and all(val.count(chr(ord("a") + i)) == 10 for i in range(6))

    def test_check_and_mutate_is_atomic_only_one_claimant_wins(self, one):
        env = Env(one)
        t = env.table()

        def claim(i):
            c = C.BtClient(f"localhost:{one.grpc_port}")
            res = ok(c.call("CheckAndMutateRow", {"table_name": t, "row_key": b64("lock"),
                                                  "predicate_filter": {"column_qualifier_regex_filter": b64("owner")},
                                                  "false_mutations": [{"set_cell": {"family_name": "cf", "column_qualifier": b64("owner"),
                                                                                    "timestamp_micros": 1000, "value": b64(f"c{i}")}}]}))
            c.close()
            return res.get("predicate_matched", False)

        with concurrent.futures.ThreadPoolExecutor(16) as pool:
            results = list(pool.map(claim, range(16)))
        assert results.count(False) == 1 and results.count(True) == 15
        owner = env.read(t)[0][1]
        assert len(owner) == 1 and owner[0][3].startswith(b"c")

    def test_mutate_rows_concurrent_batches_on_overlapping_rows_do_not_deadlock(self, one):
        env = Env(one)
        t = env.table()
        keys = [f"r{i:03d}" for i in range(40)]

        def work(i):
            c = C.BtClient(f"localhost:{one.grpc_port}")
            order = keys[::-1] if i % 2 else keys
            for round_ in range(4):
                entries = [{"row_key": b64(k), "mutations": [{"set_cell": {"family_name": "cf", "column_qualifier": b64(f"w{i}"),
                                                                          "timestamp_micros": (round_ + 1) * 1000, "value": b64("x")}}]} for k in order]
                res = ok(c.call("MutateRows", {"table_name": t, "entries": entries}, timeout=60))
                assert all(int(e.get("status", {}).get("code", 0) or 0) == 0 for m in res for e in m["entries"])
            c.close()

        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            list(pool.map(work, range(8)))
        rows = env.read(t)
        assert len(rows) == 40 and all(len({c[1] for c in cells}) == 8 for _, cells in rows)

    # ------------------------------------------------------------------------------------------ catalog / misc

    def test_catalog_row_and_cells_are_in_postgres(self, one):
        env = Env(one)
        t = env.table(("cf",))
        env.set(t, "r", "cf", "q", "v", 1000)
        assert sql(one.pgs[0], "SELECT count(*) FROM warp_bt_tables WHERE name = %s", (t,))[0][0] == 1
        assert sql(one.pgs[0], "SELECT count(*) FROM warp_bt_cells WHERE tbl = %s", (t,))[0][0] == 1
        ok(env.c.call("DeleteTable", {"name": t}))
        assert sql(one.pgs[0], "SELECT count(*) FROM warp_bt_tables WHERE name = %s", (t,))[0][0] == 0
        assert sql(one.pgs[0], "SELECT count(*) FROM warp_bt_cells WHERE tbl = %s", (t,))[0][0] == 0

    def test_recreated_table_starts_empty_and_paged_list_tables(self, one):
        env = Env(one)
        names = [uniq("lt") for _ in range(5)]
        for n in names:
            env.table(name=n)
        env.set(f"{env.parent}/tables/{names[0]}", "r", "cf", "q", "v", 1000)
        ok(env.c.call("DeleteTable", {"name": f"{env.parent}/tables/{names[0]}"}))
        env.table(name=names[0])
        assert env.read(f"{env.parent}/tables/{names[0]}") == []
        seen, token = [], ""
        while True:
            r = ok(env.c.call("ListTables", {"parent": env.parent, "page_size": 2, "page_token": token}))
            seen += [x["name"].rsplit("/", 1)[1] for x in r.get("tables", [])]
            token = r.get("next_page_token", "")
            if not token:
                break
        assert seen == sorted(names)

    def test_metrics_see_the_store(self, one):
        Env(one).table()
        assert "bigtablewire" in one.proc.metrics_text()

    def test_unimplemented_methods_answer_unimplemented(self, one):
        env = Env(one)
        t = env.table()
        code, msg, _ = env.c.call("PingAndWarm", {"name": env.parent})
        assert code == "OK"
        from google.bigtable.v2 import bigtable_pb2 as bt
        import grpc
        with pytest.raises(grpc.RpcError) as e:
            env.c.data.ReadChangeStream(bt.ReadChangeStreamRequest(table_name=t))
            list(env.c.data.ReadChangeStream(bt.ReadChangeStreamRequest(table_name=t)))
        assert e.value.code() == grpc.StatusCode.UNIMPLEMENTED

    def test_column_family_value_types_are_rejected(self, one):
        env = Env(one)
        code, msg, _ = env.c.call("CreateTable", {"parent": env.parent, "table_id": uniq("vt"), "table": {"column_families": {
            "agg": {"value_type": {"aggregate_type": {"input_type": {"int64_type": {"encoding": {"big_endian_bytes": {}}}}, "sum": {}}}}}}})
        assert code == "UNIMPLEMENTED", (code, msg)


class TestTwoShardedBackends:
    @pytest.mark.parametrize("case", sorted(GOLDEN["cases"]))
    def test_golden_corpus(self, two, case):
        _replay(two, case)

    def _counts(self, w, t):
        return [sql(pg, "SELECT count(DISTINCT row_key) FROM warp_bt_cells WHERE tbl = %s", (t,))[0][0] for pg in w.pgs]

    def test_rows_land_on_both_hosts_and_the_catalog_on_the_first(self, two):
        env = Env(two)
        t = env.table()
        env.bulk(t, 200)
        c0, c1 = self._counts(two, t)
        assert c0 + c1 == 200 and c0 > 40 and c1 > 40, (c0, c1)
        assert sql(two.pgs[0], "SELECT count(*) FROM warp_bt_tables WHERE name = %s", (t,))[0][0] == 1
        assert sql(two.pgs[1], "SELECT count(*) FROM warp_bt_tables WHERE name = %s", (t,))[0][0] == 0
        # one row is on exactly one host, always the same one
        for i in range(0, 200, 17):
            k = f"k{i:06d}".encode()
            n = [sql(pg, "SELECT count(*) FROM warp_bt_cells WHERE tbl = %s AND row_key = %s", (t, psycopg2.Binary(k)))[0][0] for pg in two.pgs]
            assert sorted(n) == [0, 1]
            env.set(t, k.decode(), "cf", "q", "again", 2000)
            n2 = [sql(pg, "SELECT count(*) FROM warp_bt_cells WHERE tbl = %s AND row_key = %s", (t, psycopg2.Binary(k)))[0][0] for pg in two.pgs]
            assert n2 == [x * 2 for x in n]

    def test_reads_merge_the_shards_in_key_order(self, two):
        env = Env(two)
        t = env.table()
        env.bulk(t, 300, cols=("a", "b", "c"))
        all_keys = [f"k{i:06d}" for i in range(300)]
        assert env.keys(t) == all_keys
        assert env.keys(t, reversed=True) == all_keys[::-1]
        assert env.keys(t, rows_limit=17) == all_keys[:17]
        rng = {"row_ranges": [{"start_key_open": b64("k000050"), "end_key_closed": b64("k000120")},
                              {"start_key_closed": b64("k000200"), "end_key_open": b64("k000210")}],
               "row_keys": [b64("k000001"), b64("k000060"), b64("k000299"), b64("zzz")]}
        want = ["k000001"] + all_keys[51:121] + all_keys[200:210] + ["k000299"]
        assert env.keys(t, rows=rng) == want
        assert env.keys(t, rows=rng, reversed=True) == want[::-1]
        assert env.keys(t, rows=rng, rows_limit=5) == want[:5]
        rows = env.read(t)
        assert all([c[1] for c in cells] == [b"a", b"b", b"c"] for _, cells in rows)

    def test_row_writes_are_atomic_per_row_on_the_owning_host(self, two):
        env = Env(two)
        t = env.table(("cf", "cf2"))
        keys = [f"row{i:03d}" for i in range(40)]
        muts = [{"set_cell": {"family_name": "cf", "column_qualifier": b64("a"), "timestamp_micros": 1000, "value": b64("1")}},
                {"set_cell": {"family_name": "cf2", "column_qualifier": b64("b"), "timestamp_micros": 1000, "value": b64("2")}}]
        res = ok(env.c.call("MutateRows", {"table_name": t, "entries": [{"row_key": b64(k), "mutations": muts} for k in keys]
                                           + [{"row_key": b64("bad"), "mutations": [{"set_cell": {"family_name": "zz", "column_qualifier": b64("a"),
                                                                                                   "timestamp_micros": 1000, "value": b64("1")}}]}]}))
        codes = [int(e.get("status", {}).get("code", 0) or 0) for m in res for e in m["entries"]]
        assert codes[:40] == [0] * 40 and codes[40] == 13
        assert env.keys(t) == sorted(keys)
        assert all(len(c) == 2 for _, c in env.read(t))
        # CheckAndMutate / RMW work on whichever host owns the row
        for k in keys[:10]:
            r = ok(env.c.call("CheckAndMutateRow", {"table_name": t, "row_key": b64(k), "predicate_filter": {"family_name_regex_filter": "cf2"},
                                                    "true_mutations": [{"delete_from_family": {"family_name": "cf"}}]}))
            assert r["predicate_matched"] is True
            ok(env.c.call("ReadModifyWriteRow", {"table_name": t, "row_key": b64(k), "rules": [
                {"family_name": "cf2", "column_qualifier": b64("b"), "append_value": b64("+")}]}))
        rows = dict(env.read(t, rows={"row_keys": [b64(k) for k in keys[:10]]}))
        assert all([c[0] for c in cells] == ["cf2", "cf2"] and cells[0][3] == b"2+" for cells in rows.values())

    def test_drop_row_range_delete_table_and_family_drop_clean_every_host(self, two):
        env = Env(two)
        t = env.table(("cf", "gone"))
        env.bulk(t, 120)
        env.bulk(t, 120, fam="gone")
        ok(env.c.call("DropRowRange", {"name": t, "row_key_prefix": b64("k0000")}))
        left = env.keys(t)
        assert left == [f"k{i:06d}" for i in range(100, 120)] and all(self._counts(two, t))
        ok(env.c.call("ModifyColumnFamilies", {"name": t, "modifications": [{"id": "gone", "drop": True}]}))
        for pg in two.pgs:
            assert sql(pg, "SELECT count(*) FROM warp_bt_cells WHERE tbl = %s AND family = 'gone'", (t,))[0][0] == 0
        ok(env.c.call("DropRowRange", {"name": t, "delete_all_data_from_table": True}))
        assert self._counts(two, t) == [0, 0]
        env.bulk(t, 30)
        ok(env.c.call("DeleteTable", {"name": t}))
        assert self._counts(two, t) == [0, 0]

    def test_sample_row_keys_merge_the_shards(self, two):
        env = Env(two)
        t = env.table()
        env.bulk(t, 400, value=lambda i: "v" * 40)
        res = ok(env.c.call("SampleRowKeys", {"table_name": t}))
        keys = [unb64(m.get("row_key", "")) for m in res]
        offs = [int(m.get("offset_bytes", 0) or 0) for m in res]
        assert len(res) > 3 and keys[-1] == b"" and keys[:-1] == sorted(set(keys[:-1])) and offs == sorted(offs) and offs[-1] >= 400 * 40

    def test_concurrent_increments_across_hosts_are_exact(self, two):
        env = Env(two)
        t = env.table()
        rows = [f"ctr{i}" for i in range(6)]

        def work(i):
            c = C.BtClient(f"localhost:{two.grpc_port}")
            for _ in range(10):
                for r in rows:
                    ok(c.call("ReadModifyWriteRow", {"table_name": t, "row_key": b64(r), "rules": [
                        {"family_name": "cf", "column_qualifier": b64("n"), "increment_amount": 1}]}))
            c.close()

        with concurrent.futures.ThreadPoolExecutor(6) as pool:
            list(pool.map(work, range(6)))
        for r, cells in env.read(t, filter={"cells_per_column_limit_filter": 1}):
            assert int.from_bytes(cells[0][3], "big") == 60, r
        assert all(self._counts(two, t))


class TestPoolAndAuth:
    def test_bearer_token_required_when_configured(self, strict):
        anon = C.BtClient(f"localhost:{strict.grpc_port}")
        code, msg, _ = anon.call("ListTables", {"parent": "projects/x/instances/i"})
        assert code == "UNAUTHENTICATED", (code, msg)
        from google.bigtable.admin.v2 import bigtable_table_admin_pb2 as adm
        import grpc

        def with_token(tok):
            ch = grpc.insecure_channel(f"localhost:{strict.grpc_port}")
            stub = C.admg.BigtableTableAdminStub(ch)
            try:
                stub.ListTables(adm.ListTablesRequest(parent="projects/x/instances/i"), metadata=[("authorization", f"Bearer {tok}")])
                return "OK"
            except grpc.RpcError as e:
                return e.code().name
            finally:
                ch.close()

        assert with_token("wrong") == "UNAUTHENTICATED"
        assert with_token("bt-test-token") == "OK"
        assert with_token("other-token") == "OK"

    def test_pool_of_4_with_30_stalled_read_streams_does_not_starve_other_requests(self, strict):
        from google.bigtable.v2 import bigtable_pb2 as bt, data_pb2 as dt
        import grpc
        tok = [("authorization", "Bearer bt-test-token")]
        ch = grpc.insecure_channel(f"localhost:{strict.grpc_port}")
        adm_stub = C.admg.BigtableTableAdminStub(ch)
        data = C.btg.BigtableStub(ch)
        from google.bigtable.admin.v2 import bigtable_table_admin_pb2 as adm, table_pb2 as tbl
        parent = f"projects/{uniq('p')}/instances/inst"
        t = f"{parent}/tables/big"
        adm_stub.CreateTable(adm.CreateTableRequest(parent=parent, table_id="big", table=tbl.Table(column_families={"cf": tbl.ColumnFamily()})),
                             metadata=tok)
        for lo in range(0, 4000, 500):
            data.MutateRows(bt.MutateRowsRequest(table_name=t, entries=[bt.MutateRowsRequest.Entry(
                row_key=b"k%06d" % i, mutations=[dt.Mutation(set_cell=dt.Mutation.SetCell(
                    family_name="cf", column_qualifier=b"q", timestamp_micros=1000, value=b"x" * 400))]) for i in range(lo, lo + 500)]),
                metadata=tok).__next__()
        # 30 streams that read one message and then stall: the server parks in flow control between pages
        stalled = []
        chans = []
        for _ in range(30):
            c2 = grpc.insecure_channel(f"localhost:{strict.grpc_port}", options=[("grpc.http2.max_pings_without_data", 0)])
            chans.append(c2)
            call = C.btg.BigtableStub(c2).ReadRows(bt.ReadRowsRequest(table_name=t), metadata=tok)
            next(call)
            stalled.append(call)
        try:
            t0 = time.time()
            for i in range(25):
                r = data.MutateRow(bt.MutateRowRequest(table_name=t, row_key=b"quick%d" % i, mutations=[dt.Mutation(
                    set_cell=dt.Mutation.SetCell(family_name="cf", column_qualifier=b"q", timestamp_micros=1000, value=b"v"))]), metadata=tok,
                    timeout=20)
                got = list(data.ReadRows(bt.ReadRowsRequest(table_name=t, rows=dt.RowSet(row_keys=[b"quick%d" % i])), metadata=tok, timeout=20))
                assert got and got[0].chunks[0].row_key == b"quick%d" % i
            assert time.time() - t0 < 30, time.time() - t0
        finally:
            for call in stalled:
                call.cancel()
            for c2 in chans:
                c2.close()
            ch.close()
