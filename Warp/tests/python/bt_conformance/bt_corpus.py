"""Differential corpus for bigtablewire: gRPC call sequences replayed against Google's Bigtable emulator (oracle) and Warp.

A case is a list of steps run in a fresh project ($P) with a per-run token ($R) in table names. Step kinds:
  {"rpc": "MutateRow", "req": {...}, "save": {"var": "response.path"}, "sleep": secs, "names": True}
Strings may use $P (project), $R (run token) and $name (values saved by earlier steps). bytes fields are base64 in the JSON;
b("text") makes them. "names": True keeps only the table names of a ListTables answer that carry this run's token (the emulator lists
the tables of every instance it holds).
"""
import base64


def b(s):
    return base64.b64encode(s if isinstance(s, bytes) else s.encode()).decode()


def i64(n):
    return b(n.to_bytes(8, "big", signed=True))


INST = "projects/$P/instances/inst"


def T(n):
    return f"{INST}/tables/{n}-$R"


def rpc(name, req=None, **kw):
    return {"rpc": name, "req": req or {}, **kw}


def table(n, fams=("cf",), **kw):
    t = {"column_families": fams if isinstance(fams, dict) else {f: {} for f in fams}}
    t.update(kw)
    return rpc("CreateTable", {"parent": INST, "table_id": f"{n}-$R", "table": t})


def sc(fam, q, v, ts=1000):
    return {"set_cell": {"family_name": fam, "column_qualifier": b(q), "timestamp_micros": ts, "value": b(v)}}


def dcol(fam, q, start=None, end=None):
    d = {"family_name": fam, "column_qualifier": b(q)}
    if start is not None or end is not None:
        d["time_range"] = {}
        if start is not None:
            d["time_range"]["start_timestamp_micros"] = start
        if end is not None:
            d["time_range"]["end_timestamp_micros"] = end
    return {"delete_from_column": d}


def dfam(fam):
    return {"delete_from_family": {"family_name": fam}}


DROW = {"delete_from_row": {}}


def mut(t, row, *muts, **kw):
    return rpc("MutateRow", {"table_name": T(t), "row_key": b(row), "mutations": list(muts)}, **kw)


def read(t, rows=None, flt=None, limit=None, **kw):
    r = {"table_name": T(t)}
    if rows is not None:
        r["rows"] = rows
    if flt is not None:
        r["filter"] = flt
    if limit is not None:
        r["rows_limit"] = limit
    r.update(kw)
    return rpc("ReadRows", r)


def keys(*ks):
    return {"row_keys": [b(k) for k in ks]}


def rng(**kw):
    return {"row_ranges": [{k: (b(v) if isinstance(v, str) else v) for k, v in kw.items()}]}


def rmw(t, row, *rules, **kw):
    return rpc("ReadModifyWriteRow", {"table_name": T(t), "row_key": b(row), "rules": list(rules)}, **kw)


def append(fam, q, v):
    return {"family_name": fam, "column_qualifier": b(q), "append_value": b(v)}


def incr(fam, q, n):
    return {"family_name": fam, "column_qualifier": b(q), "increment_amount": n}


def cam(t, row, pred=None, yes=(), no=()):
    r = {"table_name": T(t), "row_key": b(row), "true_mutations": list(yes), "false_mutations": list(no)}
    if pred is not None:
        r["predicate_filter"] = pred
    return rpc("CheckAndMutateRow", r)


def mrows(t, *entries):
    return rpc("MutateRows", {"table_name": T(t), "entries": [{"row_key": b(k), "mutations": list(m)} for k, m in entries]})


def fam_re(x):
    return {"family_name_regex_filter": x}


def qual_re(x):
    return {"column_qualifier_regex_filter": b(x)}


def val_re(x):
    return {"value_regex_filter": b(x)}


def key_re(x):
    return {"row_key_regex_filter": b(x)}


def chain(*f):
    return {"chain": {"filters": list(f)}}


def inter(*f):
    return {"interleave": {"filters": list(f)}}


def cond(pred, yes=None, no=None):
    c = {"predicate_filter": pred}
    if yes is not None:
        c["true_filter"] = yes
    if no is not None:
        c["false_filter"] = no
    return {"condition": c}


PASS = {"pass_all_filter": True}
BLOCK = {"block_all_filter": True}
STRIP = {"strip_value_transformer": True}


def label(x):
    return {"apply_label_transformer": x}


CASES = {}


def case(name, *steps):
    CASES[name] = list(steps)


# --------------------------------------------------------------------------------------------------------------- table admin
case("table_crud",
     table("t1", ("cf", "cf2")),
     table("t1", ("cf",)),
     rpc("GetTable", {"name": T("t1")}),
     rpc("GetTable", {"name": T("t1"), "view": "FULL"}),
     rpc("GetTable", {"name": T("t1"), "view": "SCHEMA_VIEW"}),
     rpc("GetTable", {"name": T("t1"), "view": "NAME_ONLY"}),
     table("t2", ()),
     table("t3", {"a": {"gc_rule": {"max_num_versions": 3}}, "b": {"gc_rule": {"max_age": "3600s"}},
                  "c": {"gc_rule": {"union": {"rules": [{"max_num_versions": 5}, {"max_age": "10s"}]}}},
                  "d": {"gc_rule": {"intersection": {"rules": [{"max_num_versions": 2}, {"max_age": "7200.5s"}]}}}}),
     rpc("GetTable", {"name": T("t3")}),
     rpc("ListTables", {"parent": INST}, names=True),
     rpc("ListTables", {"parent": INST, "view": "FULL"}, names=True),
     rpc("DeleteTable", {"name": T("t2")}),
     rpc("DeleteTable", {"name": T("t2")}),
     rpc("GetTable", {"name": T("t2")}),
     rpc("ListTables", {"parent": INST}, names=True),
     table("t2", ("z",)),
     rpc("GetTable", {"name": T("t2")}),
     rpc("CreateTable", {"parent": INST, "table_id": f"t5-$R", "table": {"column_families": {"cf": {}}},
                         "initial_splits": [{"key": b("g")}, {"key": b("p")}]}),
     rpc("GetTable", {"name": T("t5")}))

case("table_names",
     rpc("CreateTable", {"parent": INST, "table_id": "", "table": {}}),
     rpc("CreateTable", {"parent": INST, "table_id": "bad/name-$R", "table": {}}),
     rpc("CreateTable", {"parent": INST, "table_id": "sp ace-$R", "table": {}}),
     rpc("CreateTable", {"parent": "bad", "table_id": "ok-$R", "table": {}}),
     rpc("CreateTable", {"parent": INST, "table_id": "dot.dash_under-9-$R", "table": {}}),
     rpc("CreateTable", {"parent": INST, "table_id": "x" * 200, "table": {}}),
     table("fam", {"": {}}),
     table("fam2", {"a b": {}}),
     table("fam3", {"ok-1.2_3": {}}),
     rpc("GetTable", {"name": "garbage"}),
     rpc("GetTable", {"name": T("nope")}),
     rpc("DeleteTable", {"name": "garbage"}),
     rpc("DeleteTable", {"name": T("nope")}),
     rpc("ListTables", {"parent": "projects/$P/instances/nothing-here"}, names=True))

case("modify_families",
     table("m", ("cf", "cf2")),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "cf3", "create": {"gc_rule": {"max_age": "3600s"}}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "cf3", "create": {}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "nope", "update": {}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "nope", "drop": True}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "cf2", "update": {"gc_rule": {"max_num_versions": 5}}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [
         {"id": "cf2", "update": {"gc_rule": {"max_num_versions": 7}}, "update_mask": "gcRule"}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [
         {"id": "cf2", "update": {"gc_rule": {"union": {"rules": [{"max_num_versions": 5}, {"max_age": "10s"}]}}}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "cf2", "update": {}}]}),
     rpc("GetTable", {"name": T("m")}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "cf", "drop": True}, {"id": "n1", "create": {}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": []}),
     rpc("ModifyColumnFamilies", {"name": T("m") + "zz", "modifications": [{"id": "x", "create": {}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "", "create": {}}]}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "bad name", "create": {}}]}),
     rpc("GetTable", {"name": T("m")}),
     rpc("ModifyColumnFamilies", {"name": T("m"), "modifications": [{"id": "n2", "create": {}}, {"id": "nope", "drop": True}]}),
     rpc("GetTable", {"name": T("m")}))

case("family_drop_data",
     table("fd", ("cf", "keep")),
     mut("fd", "r1", sc("cf", "q", "1"), sc("keep", "q", "2")),
     read("fd"),
     rpc("ModifyColumnFamilies", {"name": T("fd"), "modifications": [{"id": "cf", "drop": True}]}),
     read("fd"),
     mut("fd", "r1", sc("cf", "q", "3")),
     rpc("ModifyColumnFamilies", {"name": T("fd"), "modifications": [{"id": "cf", "create": {}}]}),
     read("fd"),
     mut("fd", "r1", sc("cf", "q", "4")),
     read("fd"))

case("consistency",
     table("c"),
     rpc("GenerateConsistencyToken", {"name": T("c")}, save={"tok": "consistency_token"}),
     rpc("CheckConsistency", {"name": T("c"), "consistency_token": "$tok"}),
     rpc("CheckConsistency", {"name": T("c"), "consistency_token": "wrong"}),
     rpc("GenerateConsistencyToken", {"name": T("nope")}),
     rpc("CheckConsistency", {"name": T("nope"), "consistency_token": "x"}))

case("update_table",
     table("u", ("cf",)),
     rpc("UpdateTable", {"table": {"name": T("u"), "deletion_protection": True}, "update_mask": "deletionProtection"}),
     rpc("GetTable", {"name": T("u")}),
     rpc("DeleteTable", {"name": T("u")}),
     rpc("UpdateTable", {"table": {"name": T("u"), "deletion_protection": False}, "update_mask": "deletionProtection"}),
     rpc("DeleteTable", {"name": T("u")}),
     rpc("UpdateTable", {"table": {"name": T("nope"), "deletion_protection": True}, "update_mask": "deletionProtection"}),
     table("u2", ("cf",), deletion_protection=True),
     rpc("DeleteTable", {"name": T("u2")}))

# --------------------------------------------------------------------------------------------------------------- mutations
case("mutate_basic",
     table("mb", ("cf", "cf2")),
     mut("mb", "r1", sc("cf", "q", "v1", 1000)),
     read("mb"),
     mut("mb", "r1", sc("cf", "q", "v1b", 1000)),
     mut("mb", "r1", sc("cf", "q", "v2", 2000), sc("cf", "q", "v3", 3000), sc("cf", "other", "x", 500)),
     read("mb", keys("r1")),
     mut("mb", "r2", sc("cf", "q", "server", -1)),
     read("mb", keys("r2")),
     mut("mb", "r3", sc("cf", "q", "zero", 0)),
     read("mb", keys("r3")),
     mut("mb", "r4", sc("cf", "q", "neg", -2)),
     mut("mb", "r4", sc("cf", "q", "nonmilli", 1234)),
     mut("mb", "r4", sc("nofam", "q", "x", 1000)),
     mut("mb", "r4", sc("cf", "q", "ok", 1000), sc("nofam", "q", "x", 1000)),
     read("mb", keys("r4")),
     rpc("MutateRow", {"table_name": T("mb"), "row_key": "", "mutations": [sc("cf", "q", "x")]}),
     rpc("MutateRow", {"table_name": T("mb"), "row_key": b("r5"), "mutations": []}),
     rpc("MutateRow", {"table_name": T("mb"), "row_key": b("r5"), "mutations": [{}]}),
     rpc("MutateRow", {"table_name": "garbage", "row_key": b("r5"), "mutations": [sc("cf", "q", "x")]}),
     rpc("MutateRow", {"table_name": T("missing"), "row_key": b("r5"), "mutations": [sc("cf", "q", "x")]}),
     mut("mb", "r6", sc("cf", "", "empty-qualifier", 1000), sc("cf", "q", "", 1000)),
     read("mb", keys("r6")),
     mut("mb", "r7", sc("cf", "q", "a" * 1000, 1000)),
     read("mb", keys("r7"), STRIP))

case("mutate_delete",
     table("md", ("cf", "cf2")),
     mut("md", "d1", sc("cf", "a", "1", 1000), sc("cf", "a", "2", 2000), sc("cf", "a", "3", 3000), sc("cf", "b", "1", 1000),
         sc("cf2", "a", "x", 1000)),
     mut("md", "d1", dcol("cf", "a", 2000, 3000)),
     read("md", keys("d1")),
     mut("md", "d1", dcol("cf", "a", 1000)),
     read("md", keys("d1")),
     mut("md", "d1", dcol("cf", "b", 5000, 1000)),
     mut("md", "d1", dcol("cf", "b", 1000, 1000)),
     mut("md", "d1", dcol("cf", "b", 1001, 2000)),
     mut("md", "d1", dcol("cf", "b", None, 2000)),
     read("md", keys("d1")),
     mut("md", "d1", dcol("zz", "b")),
     mut("md", "d1", dfam("nope")),
     mut("md", "d1", dfam("cf2")),
     read("md", keys("d1")),
     mut("md", "d1", sc("cf", "z", "1", 1000)),
     mut("md", "d1", DROW),
     read("md", keys("d1")),
     mut("md", "d2", sc("cf", "a", "1", 1000), dfam("cf"), sc("cf2", "z", "1", 1000)),
     read("md", keys("d2")),
     mut("md", "d3", sc("cf", "a", "1", 1000), DROW, sc("cf", "b", "2", 1000)),
     read("md", keys("d3")),
     mut("md", "d4", sc("cf", "a", "1", 1000), sc("cf", "a", "2", 2000), dcol("cf", "a")),
     read("md", keys("d4")),
     mut("md", "never", DROW),
     mut("md", "never", dfam("cf")),
     read("md", keys("never")))

case("mutate_aggregates",
     table("ag", ("cf",)),
     mut("ag", "r", {"add_to_cell": {"family_name": "cf", "column_qualifier": {"raw_value": b("q")},
                                     "timestamp": {"raw_timestamp_micros": 1000}, "input": {"int_value": 5}}}),
     mut("ag", "r", {"merge_to_cell": {"family_name": "cf", "column_qualifier": {"raw_value": b("q")},
                                       "timestamp": {"raw_timestamp_micros": 1000}, "input": {"int_value": 5}}}),
     read("ag"))

case("mutate_rows",
     table("mr", ("cf",)),
     mrows("mr", ("a", [sc("cf", "q", "1")]), ("b", [sc("cf", "q", "2")]), ("c", [sc("cf", "q", "3")])),
     read("mr"),
     mrows("mr", ("a", [sc("cf", "q", "1x")]), ("b", [sc("zz", "q", "bad")]), ("c", [sc("cf", "q", "3x", 1234)]), ("d", [sc("cf", "q", "4")])),
     read("mr"),
     mrows("mr", ("e", [sc("cf", "q", "1", 1000)]), ("e", [sc("cf", "q", "2", 2000)]), ("e", [sc("cf", "q", "1b", 1000)])),
     read("mr", keys("e")),
     mrows("mr", ("f", []), ("g", [sc("cf", "q", "1")])),
     read("mr", keys("f", "g")),
     mrows("mr", ("", [sc("cf", "q", "1")]), ("h", [sc("cf", "q", "1")])),
     read("mr", keys("h")),
     rpc("MutateRows", {"table_name": T("mr"), "entries": []}),
     rpc("MutateRows", {"table_name": T("nope"), "entries": [{"row_key": b("a"), "mutations": [sc("cf", "q", "1")]}]}),
     mrows("mr", ("a", [DROW]), ("b", [dfam("cf")]), ("e", [dcol("cf", "q", 1000, 2000)])),
     read("mr"),
     mrows("mr", *[(f"bulk{i:04d}", [sc("cf", "q", f"v{i}"), sc("cf", "r", f"w{i}")]) for i in range(300)]),
     read("mr", rng(start_key_closed="bulk0100", end_key_open="bulk0110")),
     read("mr", rng(start_key_closed="bulk", end_key_open="bulk~"), STRIP, limit=3))

# --------------------------------------------------------------------------------------------------------------- reads
_ROWS = [mut("rs", k, sc("cf", "q", "v" + k)) for k in ("a", "b", "c", "d", "e", "ab", "b\x00", "\x01", "zz")]

case("read_rowset",
     table("rs", ("cf",)),
     *_ROWS,
     read("rs"),
     read("rs", {}),
     read("rs", keys("c", "a", "nope", "a")),
     read("rs", keys("")),
     read("rs", rng(start_key_closed="b", end_key_open="d")),
     read("rs", rng(start_key_open="b", end_key_open="d")),
     read("rs", rng(start_key_closed="b", end_key_closed="d")),
     read("rs", rng(start_key_open="b", end_key_closed="d")),
     read("rs", rng(start_key_closed="b", end_key_closed="b")),
     read("rs", rng(start_key_closed="b", end_key_open="b")),
     read("rs", rng(start_key_open="b", end_key_open="b")),
     read("rs", rng(start_key_closed="d", end_key_open="b")),
     read("rs", rng(start_key_open="d", end_key_closed="b")),
     read("rs", rng(start_key_closed="c")),
     read("rs", rng(end_key_open="c")),
     read("rs", rng(end_key_closed="c")),
     read("rs", {"row_ranges": [{}]}),
     read("rs", rng(start_key_closed="", end_key_open="b")),
     read("rs", rng(start_key_open="", end_key_open="b")),
     read("rs", rng(start_key_closed="b\x00")),
     read("rs", {"row_ranges": [{"start_key_closed": b("a"), "end_key_open": b("c")}, {"start_key_closed": b("b"), "end_key_open": b("e")}]}),
     read("rs", {"row_ranges": [{"start_key_closed": b("d")}, {"end_key_closed": b("a")}], "row_keys": [b("c"), b("d")]}),
     read("rs", {"row_ranges": [{"start_key_closed": b("a"), "end_key_open": b("c")}], "row_keys": [b("b"), b("e")]}),
     read("rs", {"row_ranges": [{"start_key_closed": b("a"), "end_key_open": b("c")}, {"start_key_closed": b("d"), "end_key_open": b("z")}]}, limit=3),
     read("rs", limit=1),
     read("rs", limit=100),
     read("rs", keys("a", "b", "c"), limit=2),
     read("rs", limit=0),
     read("rs", limit=-1),
     read("rs", keys("zz", "\x01", "b\x00", "ab")),
     read("rs", rng(start_key_closed="a", end_key_open="b"), flt=key_re("a.*")),
     read("rs", app_profile_id="anything"),
     read("rs", request_stats_view="REQUEST_STATS_FULL"),
     read("rs", reversed=True),
     read("rs", keys("a", "c"), reversed=True),
     read("rs", rng(start_key_closed="b", end_key_open="e"), limit=2, reversed=True),
     rpc("ReadRows", {"table_name": "garbage"}),
     rpc("ReadRows", {"table_name": T("nope")}),
     rpc("ReadRows", {"table_name": ""}))

case("read_empty",
     table("re", ("cf",)),
     read("re"),
     read("re", keys("x")),
     read("re", rng(start_key_closed="a")),
     read("re", flt=STRIP),
     rpc("SampleRowKeys", {"table_name": T("re")}))

_MF = [mut("mf", k, sc("cf1", "a", f"{k}-1a-old", 1000), sc("cf1", "a", f"{k}-1a", 2000), sc("cf1", "b", f"{k}-1b", 1000),
           sc("cf2", "a", f"{k}-2a", 3000), sc("cf3", "c", f"{k}-3c", 4000)) for k in ("r1", "r2", "r3")]

case("filters_basic",
     table("mf", ("cf1", "cf2", "cf3")),
     *_MF,
     read("mf", keys("r1")),
     read("mf", keys("r1"), PASS),
     read("mf", keys("r1"), BLOCK),
     read("mf", keys("r1"), {"sink": True}),
     read("mf", keys("r1"), {}),
     read("mf", flt=key_re("r[12]")),
     read("mf", flt=key_re("r")),
     read("mf", flt=key_re("r.")),
     read("mf", flt=key_re("r1|r3")),
     read("mf", flt=key_re("^r2$")),
     read("mf", flt=key_re(".*")),
     read("mf", flt=key_re("")),
     read("mf", flt=key_re("R1")),
     read("mf", flt=key_re("(?i)R1")),
     read("mf", flt=key_re("r\\d")),
     read("mf", flt=key_re("[[:alpha:]][[:digit:]]")),
     read("mf", keys("r1"), fam_re("cf[12]")),
     read("mf", keys("r1"), fam_re("cf")),
     read("mf", keys("r1"), fam_re("cf.*")),
     read("mf", keys("r1"), fam_re("cf3|cf1")),
     read("mf", keys("r1"), fam_re("nomatch")),
     read("mf", keys("r1"), qual_re("a")),
     read("mf", keys("r1"), qual_re("[ab]")),
     read("mf", keys("r1"), qual_re(".")),
     read("mf", keys("r1"), qual_re("")),
     read("mf", keys("r1"), val_re("r1-1.*")),
     read("mf", keys("r1"), val_re(".*-old")),
     read("mf", keys("r1"), val_re("r1-1a")),
     read("mf", keys("r1"), val_re("R1.*")),
     read("mf", keys("r1"), val_re("(?s).*")),
     read("mf", keys("r1"), val_re("\\C*")),
     read("mf", keys("r1"), val_re("[a-z0-9]+-3c")))

case("filters_regex_errors",
     table("fr", ("cf",)),
     mut("fr", "r", sc("cf", "q", "v")),
     read("fr", flt=key_re("(")),
     read("fr", flt=key_re("[")),
     read("fr", flt=key_re("a**")),
     read("fr", flt=key_re("*a")),
     read("fr", flt=key_re("(?=a)")),
     read("fr", flt=key_re("(a)\\1")),
     read("fr", keys("r"), fam_re("(")),
     read("fr", keys("r"), qual_re("[")),
     read("fr", keys("r"), val_re("(?P<n")),
     read("fr", keys("r"), val_re("\\")))

case("filters_binary",
     table("fb", ("cf",)),
     rpc("MutateRow", {"table_name": T("fb"), "row_key": b(b"\x00\x01\xff"), "mutations": [
         {"set_cell": {"family_name": "cf", "column_qualifier": b(b"\xff\xfe"), "timestamp_micros": 1000, "value": b(b"\x00\x80\xff")}}]}),
     rpc("MutateRow", {"table_name": T("fb"), "row_key": b(b"\xc3\xa9t\xc3\xa9"), "mutations": [
         {"set_cell": {"family_name": "cf", "column_qualifier": b("qé"), "timestamp_micros": 1000, "value": b("café")}}]}),
     read("fb"),
     read("fb", flt=val_re("caf.")),
     read("fb", flt=val_re("caf\\C\\C")),
     read("fb", flt=val_re("café")),
     read("fb", flt=qual_re("q.")),
     read("fb", flt=key_re("ét.*")),
     read("fb", {"row_ranges": [{"start_key_closed": b(b"\x00\x01"), "end_key_open": b(b"\xff")}]}),
     read("fb", {"row_ranges": [{"start_key_open": b(b"\xc3\xa9t\xc3\xa9")}]}),
     read("fb", {"row_ranges": [{"end_key_open": b(b"\xc3")}]}),
     read("fb", {"row_keys": [b(b"\x00\x01\xff"), b(b"\xc3\xa9t\xc3\xa9")]}))

case("filters_ranges",
     table("fg", ("cf1", "cf2")),
     mut("fg", "r1", sc("cf1", "a", "va1", 1000), sc("cf1", "a", "va2", 2000), sc("cf1", "b", "vb", 3000), sc("cf1", "c", "vc", 4000),
         sc("cf1", "d", "vd", 5000), sc("cf2", "a", "wa", 1000), sc("cf2", "z", "wz", 2000)),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "cf1", "start_qualifier_closed": b("b"), "end_qualifier_open": b("d")}}),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "cf1", "start_qualifier_open": b("b"), "end_qualifier_closed": b("d")}}),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "cf1", "start_qualifier_closed": b("b")}}),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "cf1", "end_qualifier_open": b("b")}}),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "cf1"}}),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "cf2", "start_qualifier_closed": b("a"), "end_qualifier_closed": b("a")}}),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "zz", "start_qualifier_closed": b("a")}}),
     read("fg", keys("r1"), {"column_range_filter": {"start_qualifier_closed": b("a")}}),
     read("fg", keys("r1"), {"column_range_filter": {"family_name": "cf1", "start_qualifier_closed": b("z"), "end_qualifier_open": b("a")}}),
     read("fg", keys("r1"), {"value_range_filter": {"start_value_closed": b("va1"), "end_value_open": b("vc")}}),
     read("fg", keys("r1"), {"value_range_filter": {"start_value_open": b("va1"), "end_value_closed": b("vc")}}),
     read("fg", keys("r1"), {"value_range_filter": {"start_value_closed": b("vc")}}),
     read("fg", keys("r1"), {"value_range_filter": {"end_value_open": b("vb")}}),
     read("fg", keys("r1"), {"value_range_filter": {}}),
     read("fg", keys("r1"), {"value_range_filter": {"start_value_closed": b("z"), "end_value_open": b("a")}}),
     read("fg", keys("r1"), {"timestamp_range_filter": {"start_timestamp_micros": 1000, "end_timestamp_micros": 3000}}),
     read("fg", keys("r1"), {"timestamp_range_filter": {"start_timestamp_micros": 2000}}),
     read("fg", keys("r1"), {"timestamp_range_filter": {"end_timestamp_micros": 2000}}),
     read("fg", keys("r1"), {"timestamp_range_filter": {}}),
     read("fg", keys("r1"), {"timestamp_range_filter": {"start_timestamp_micros": 5000, "end_timestamp_micros": 1000}}),
     read("fg", keys("r1"), {"timestamp_range_filter": {"start_timestamp_micros": 2000, "end_timestamp_micros": 2000}}),
     read("fg", keys("r1"), {"timestamp_range_filter": {"start_timestamp_micros": 2500, "end_timestamp_micros": 2600}}))

case("filters_limits",
     table("fl", ("cf",)),
     mut("fl", "r1", sc("cf", "a", "a1", 1000), sc("cf", "a", "a2", 2000), sc("cf", "a", "a3", 3000), sc("cf", "b", "b1", 1000),
         sc("cf", "b", "b2", 2000), sc("cf", "c", "c1", 1000)),
     mut("fl", "r2", sc("cf", "a", "a1", 1000)),
     read("fl", keys("r1"), {"cells_per_row_limit_filter": 1}),
     read("fl", keys("r1"), {"cells_per_row_limit_filter": 2}),
     read("fl", keys("r1"), {"cells_per_row_limit_filter": 100}),
     read("fl", keys("r1"), {"cells_per_row_limit_filter": 0}),
     read("fl", keys("r1"), {"cells_per_row_limit_filter": -1}),
     read("fl", keys("r1"), {"cells_per_row_offset_filter": 0}),
     read("fl", keys("r1"), {"cells_per_row_offset_filter": 1}),
     read("fl", keys("r1"), {"cells_per_row_offset_filter": 4}),
     read("fl", keys("r1"), {"cells_per_row_offset_filter": 6}),
     read("fl", keys("r1"), {"cells_per_row_offset_filter": 100}),
     read("fl", keys("r1"), {"cells_per_column_limit_filter": 1}),
     read("fl", keys("r1"), {"cells_per_column_limit_filter": 2}),
     read("fl", keys("r1"), {"cells_per_column_limit_filter": 0}),
     read("fl", keys("r1"), {"cells_per_column_limit_filter": -3}),
     read("fl", flt={"cells_per_row_offset_filter": 1}),
     read("fl", flt={"cells_per_row_limit_filter": 1}),
     read("fl", keys("r1"), chain({"cells_per_row_offset_filter": 1}, {"cells_per_row_limit_filter": 2})),
     read("fl", keys("r1"), chain({"cells_per_row_limit_filter": 2}, {"cells_per_row_offset_filter": 1})),
     read("fl", keys("r1"), chain({"cells_per_column_limit_filter": 2}, {"cells_per_row_limit_filter": 4})),
     read("fl", keys("r1"), chain(qual_re("a"), {"cells_per_column_limit_filter": 1})),
     read("fl", keys("r1"), chain({"cells_per_column_limit_filter": 1}, qual_re("a"))))

case("filters_transformers",
     table("ft", ("cf",)),
     mut("ft", "r1", sc("cf", "a", "a1", 1000), sc("cf", "a", "a2", 2000), sc("cf", "b", "b1", 1000)),
     read("ft", keys("r1"), STRIP),
     read("ft", keys("r1"), {"strip_value_transformer": False}),
     read("ft", keys("r1"), label("lbl")),
     read("ft", keys("r1"), label("")),
     read("ft", keys("r1"), chain(label("one"), label("two"))),
     read("ft", keys("r1"), chain(label("one"), STRIP)),
     read("ft", keys("r1"), chain(STRIP, val_re(""))),
     read("ft", keys("r1"), chain(val_re("a.*"), STRIP)),
     read("ft", keys("r1"), inter(label("x"), label("y"))),
     read("ft", keys("r1"), inter(qual_re("a"), qual_re("b"))),
     read("ft", keys("r1"), inter(qual_re("b"), qual_re("a"))),
     read("ft", keys("r1"), inter(qual_re("a"), qual_re("a"))),
     read("ft", keys("r1"), inter(chain(qual_re("a"), label("A")), chain(qual_re("b"), label("B")), {"cells_per_column_limit_filter": 1})),
     read("ft", keys("r1"), inter(BLOCK, qual_re("b"))),
     read("ft", keys("r1"), inter(BLOCK, BLOCK)),
     read("ft", keys("r1"), chain(inter(qual_re("a"), qual_re("b")), {"cells_per_column_limit_filter": 1})),
     read("ft", keys("r1"), chain(PASS, PASS)),
     read("ft", keys("r1"), chain({"sink": True}, STRIP)),
     read("ft", keys("r1"), chain(BLOCK, PASS)),
     read("ft", keys("r1"), {"pass_all_filter": False}),
     read("ft", keys("r1"), {"block_all_filter": False}),
     read("ft", keys("r1"), {"sink": False}))

case("filters_condition",
     table("fc", ("cf",)),
     mut("fc", "r1", sc("cf", "a", "yes", 1000), sc("cf", "b", "b1", 1000)),
     mut("fc", "r2", sc("cf", "a", "no", 1000), sc("cf", "b", "b2", 1000)),
     read("fc", flt=cond(val_re("yes"), STRIP)),
     read("fc", flt=cond(val_re("yes"), STRIP, label("else"))),
     read("fc", flt=cond(val_re("yes"), None, label("else"))),
     read("fc", flt=cond(val_re("yes"))),
     read("fc", flt=cond(val_re("nomatch"), STRIP, {"cells_per_row_limit_filter": 1})),
     read("fc", flt=cond(qual_re("a"), qual_re("b"), qual_re("a"))),
     read("fc", flt=cond(chain(qual_re("a"), val_re("no")), PASS, BLOCK)),
     read("fc", flt=cond(BLOCK, PASS, STRIP)),
     read("fc", flt=cond(PASS, STRIP)),
     read("fc", flt=chain(cond(val_re("yes"), PASS, BLOCK), label("kept"))),
     read("fc", flt=cond(cond(val_re("yes"), PASS, BLOCK), qual_re("b"), qual_re("a"))),
     read("fc", flt=cond({}, STRIP)),
     read("fc", flt={"condition": {"true_filter": STRIP}}),
     read("fc", flt=cond(key_re("r1"), qual_re("a"), qual_re("b"))))

case("filters_errors",
     table("fe", ("cf",)),
     mut("fe", "r", sc("cf", "q", "v")),
     read("fe", flt={"chain": {"filters": []}}),
     read("fe", flt=chain(PASS)),
     read("fe", flt={"interleave": {"filters": []}}),
     read("fe", flt=inter(PASS)),
     read("fe", flt={"row_sample_filter": 0.0}),
     read("fe", flt={"row_sample_filter": 1.0}),
     read("fe", flt={"row_sample_filter": 2.0}),
     read("fe", flt={"row_sample_filter": -0.5}),
     read("fe", flt=chain(PASS, chain(PASS))),
     read("fe", flt=cond(chain(PASS))),
     read("fe", flt=inter(PASS, {"cells_per_row_limit_filter": 0})),
     read("fe", keys("nope"), chain(PASS)),
     read("fe", flt={"row_sample_filter": 0.999999}, limit=1))

# --------------------------------------------------------------------------------------------------------------- conditional / RMW
case("check_and_mutate",
     table("ca", ("cf", "cf2")),
     mut("ca", "r1", sc("cf", "q", "yes", 1000), sc("cf2", "z", "zz", 1000)),
     cam("ca", "r1", PASS, yes=[sc("cf", "t", "true", 1000)], no=[sc("cf", "f", "false", 1000)]),
     cam("ca", "r1", BLOCK, yes=[sc("cf", "t2", "true", 1000)], no=[sc("cf", "f2", "false", 1000)]),
     read("ca", keys("r1")),
     cam("ca", "new", PASS, yes=[sc("cf", "t", "true", 1000)], no=[sc("cf", "f", "false", 1000)]),
     read("ca", keys("new")),
     cam("ca", "r1", None, yes=[sc("cf", "n1", "true", 1000)], no=[sc("cf", "n2", "false", 1000)]),
     cam("ca", "absent", None, yes=[sc("cf", "n1", "true", 1000)], no=[sc("cf", "n2", "false", 1000)]),
     read("ca", keys("absent")),
     cam("ca", "r1", val_re("yes"), yes=[dcol("cf", "q")], no=[]),
     read("ca", keys("r1")),
     cam("ca", "r1", val_re("yes"), yes=[sc("cf", "never", "x", 1000)], no=[sc("cf", "else", "x", 1000)]),
     cam("ca", "r1", chain(fam_re("cf2"), val_re("zz")), yes=[DROW], no=[sc("cf", "kept", "x", 1000)]),
     read("ca", keys("r1")),
     cam("ca", "r1", PASS),
     cam("ca", "", PASS, yes=[sc("cf", "t", "x", 1000)]),
     cam("ca", "r9", PASS, yes=[sc("nofam", "t", "x", 1000)]),
     cam("ca", "r9", PASS, yes=[], no=[sc("nofam", "t", "x", 1000)]),
     cam("ca", "r9", PASS, yes=[{}]),
     cam("nope", "r1", PASS, yes=[sc("cf", "t", "x", 1000)]),
     cam("ca", "r1", chain(PASS), yes=[sc("cf", "t", "x", 1000)]),
     cam("ca", "r1", {"row_sample_filter": 5.0}, yes=[sc("cf", "t", "x", 1000)]),
     cam("ca", "r2", None, yes=[sc("cf", "a", "1", 1000)], no=[sc("cf", "a", "2", 1000)]),
     cam("ca", "r2", None, yes=[sc("cf", "a", "1b", 1000)], no=[sc("cf", "a", "2b", 1000)]),
     read("ca", keys("r2")),
     cam("ca", "r3", {"condition": {"predicate_filter": PASS}}, yes=[sc("cf", "a", "1", 1000)], no=[sc("cf", "a", "2", 1000)]),
     read("ca", keys("r3")))

case("rmw_append",
     table("ra", ("cf", "cf2")),
     rmw("ra", "r1", append("cf", "q", "a")),
     rmw("ra", "r1", append("cf", "q", "b")),
     rmw("ra", "r1", append("cf", "q", "c"), append("cf", "q", "d")),
     rmw("ra", "r1", append("cf", "q2", "x"), append("cf2", "q", "y")),
     read("ra", keys("r1"), {"cells_per_column_limit_filter": 1}),
     rmw("ra", "r2", append("cf", "q", "")),
     read("ra", keys("r2"), {"cells_per_column_limit_filter": 1}),
     rmw("ra", "r3", {"family_name": "cf", "column_qualifier": b("bin"), "append_value": b(b"\x00\xff\x01")}),
     rmw("ra", "r3", {"family_name": "cf", "column_qualifier": b("bin"), "append_value": b(b"\x02")}),
     read("ra", keys("r3"), {"cells_per_column_limit_filter": 1}),
     rmw("ra", "r4", append("nofam", "q", "a")),
     rmw("ra", "r4", {"family_name": "cf", "column_qualifier": b("q")}),
     rmw("ra", "r4"),
     read("ra", keys("r4")),
     rmw("nope", "r4", append("cf", "q", "a")),
     rmw("ra", "r5", append("cf", "q", "x" * 5000)),
     rmw("ra", "r5", append("cf", "q", "y" * 5000)),
     read("ra", keys("r5"), {"cells_per_column_limit_filter": 1}),
     read("ra", keys("r5"), chain({"cells_per_column_limit_filter": 1}, STRIP)))

case("rmw_increment",
     table("ri", ("cf",)),
     rmw("ri", "c", incr("cf", "n", 5)),
     rmw("ri", "c", incr("cf", "n", 5)),
     rmw("ri", "c", incr("cf", "n", -20)),
     rmw("ri", "c", incr("cf", "n", 0)),
     rmw("ri", "c", incr("cf", "n", 1), incr("cf", "n", 2), incr("cf", "m", 7)),
     read("ri", keys("c"), {"cells_per_column_limit_filter": 1}),
     rmw("ri", "big", incr("cf", "n", 9223372036854775807)),
     rmw("ri", "big", incr("cf", "n", 1)),
     rmw("ri", "neg", incr("cf", "n", -9223372036854775808)),
     mut("ri", "raw", {"set_cell": {"family_name": "cf", "column_qualifier": b("n"), "timestamp_micros": 1000, "value": i64(40)}}),
     rmw("ri", "raw", incr("cf", "n", 2)),
     read("ri", keys("raw")),
     mut("ri", "str", sc("cf", "n", "notanumber", 1000)),
     rmw("ri", "str", incr("cf", "n", 1)),
     mut("ri", "empty", sc("cf", "n", "", 1000)),
     rmw("ri", "empty", incr("cf", "n", 3)),
     rmw("ri", "mix", incr("cf", "n", 1), append("cf", "s", "x")),
     rmw("ri", "mix", append("cf", "n", "x")),
     rmw("ri", "mix", incr("cf", "n", 1)),
     rmw("ri", "", incr("cf", "n", 1)),
     rmw("ri", "u", incr("nofam", "n", 1)))

case("rmw_timestamps",
     table("rt", ("cf",)),
     mut("rt", "r", sc("cf", "q", "5", 9000000000000000)),
     rmw("rt", "r", append("cf", "q", "x")),
     read("rt", keys("r")),
     mut("rt", "s", sc("cf", "q", "old", 1000)),
     rmw("rt", "s", append("cf", "q", "+new")),
     read("rt", keys("s")))

case("drop_row_range",
     table("dr", ("cf",)),
     *[mut("dr", k, sc("cf", "q", k)) for k in ("a1", "a2", "b1", "b2", "ab", "a", "\xffz")],
     rpc("DropRowRange", {"name": T("dr"), "row_key_prefix": b("a")}),
     read("dr"),
     rpc("DropRowRange", {"name": T("dr"), "row_key_prefix": b("b1")}),
     read("dr"),
     rpc("DropRowRange", {"name": T("dr"), "row_key_prefix": b("nomatch")}),
     rpc("DropRowRange", {"name": T("dr"), "row_key_prefix": b(b"\xff")}),
     read("dr"),
     rpc("DropRowRange", {"name": T("dr")}),
     rpc("DropRowRange", {"name": T("dr"), "row_key_prefix": ""}),
     rpc("DropRowRange", {"name": T("nope"), "delete_all_data_from_table": True}),
     mut("dr", "keep", sc("cf", "q", "k")),
     rpc("DropRowRange", {"name": T("dr"), "delete_all_data_from_table": True}),
     read("dr"),
     mut("dr", "after", sc("cf", "q", "k")),
     read("dr"))

case("sample_row_keys",
     table("sk", ("cf",)),
     rpc("SampleRowKeys", {"table_name": T("sk")}),
     mrows("sk", *[(f"k{i:04d}", [sc("cf", "q", "v" * 50)]) for i in range(200)]),
     rpc("SampleRowKeys", {"table_name": T("sk")}),
     rpc("SampleRowKeys", {"table_name": T("nope")}),
     rpc("SampleRowKeys", {"table_name": "garbage"}))

case("ping",
     rpc("PingAndWarm", {"name": INST}))

case("large_data",
     table("ld", ("cf",)),
     mut("ld", "big", sc("cf", "q", "x" * (3 * 1024 * 1024 + 17))),
     read("ld", keys("big"), chain({"cells_per_row_limit_filter": 1}, {"strip_value_transformer": True})),
     read("ld", keys("big"), val_re("x{100}.*")),
     mut("ld", "wide", *[sc("cf", f"q{i:05d}", f"v{i}") for i in range(3000)]),
     read("ld", keys("wide"), {"cells_per_row_limit_filter": 2500}),
     read("ld", keys("wide"), chain({"cells_per_row_offset_filter": 2990}, {"cells_per_row_limit_filter": 4})),
     mrows("ld", *[(f"row{i:05d}", [sc("cf", "q", f"v{i}")]) for i in range(800)]),
     mrows("ld", *[(f"row{i:05d}", [sc("cf", "r", f"w{i}")]) for i in range(800, 1600)]),
     read("ld", rng(start_key_closed="row", end_key_open="rowz"), chain({"cells_per_row_limit_filter": 1}, STRIP), limit=1500),
     read("ld", rng(start_key_closed="row00700", end_key_open="row00705")),
     read("ld", rng(start_key_closed="row01598")))

case("versions",
     table("vs", ("cf",)),
     *[mut("vs", "r", sc("cf", "q", f"v{t}", t * 1000)) for t in range(1, 8)],
     read("vs", keys("r")),
     read("vs", keys("r"), {"cells_per_column_limit_filter": 3}),
     read("vs", keys("r"), {"timestamp_range_filter": {"start_timestamp_micros": 3000, "end_timestamp_micros": 6000}}),
     mut("vs", "r", dcol("cf", "q", 3000, 5000)),
     read("vs", keys("r")),
     mut("vs", "r", dcol("cf", "q", None, 2000)),
     read("vs", keys("r")))
