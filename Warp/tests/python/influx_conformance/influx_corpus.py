"""Differential corpus. Every case is a list of steps replayed IDENTICALLY against real InfluxDB 1.8
(the oracle) and Warp's influxwire. `{db}` is replaced by a per-case database created before the case
and dropped after it. Step forms (tuples):
  ("w", body, {params}[, {headers}])          POST /write
  ("wgz", body, {params})                     POST /write with Content-Encoding: gzip
  ("q", influxql, {params}[, method])         GET (default) or POST /query
  ("qh", influxql, {params}, {headers})       GET /query with extra headers
  ("raw", method, path, {params}, body, {headers})
Case flags: name -> (steps, opts). opts: {"norm": ["time"]} normalises server-clock timestamps.
"""

D = 1577836800  # 2020-01-01T00:00:00Z in seconds
NS = 1_000_000_000


def ts(sec):
    return D * NS + sec * NS


# Standard dataset: 3 series (host a/b in us, host c in eu), 6 points each at 10s spacing, float+int+str+bool
def dataset():
    lines = []
    for host, region, base, off in (("a", "us", 10, 0), ("b", "us", 20, 1), ("c", "eu", 30, 2)):
        for i in range(6):
            lines.append(f'cpu,host={host},region={region} usage={base + i * 1.5},idle={100 - base - i},'
                         f'n={i}i,ok={"true" if i % 2 == 0 else "false"},msg="m{host}{i}" {ts(i * 10 + off)}')
    return "\n".join(lines)


DATA = ("w", dataset(), {})
CASES = {}


def case(name, steps, **opts):
    assert name not in CASES, name
    CASES[name] = (steps, opts)


# ---------------------------------------------------------------- LINE PROTOCOL
case("lp_basic_types", [("w", "m,t=1 f=1.5,i=2i,s=\"x\",b=true 1000000000", {}),
                        ("q", "SELECT * FROM m", {}), ("q", "SHOW FIELD KEYS", {}),
                        ("q", "SHOW TAG KEYS", {})])
for i, sp in enumerate(["t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE", "tRUE", "fALSE", "yes", "1", "TrUe"]):
    case(f"lp_bool_{i}_{sp}", [("w", f"m v={sp} 1", {}), ("q", "SELECT v FROM m", {})])
for i, num in enumerate(["1", "1.5", "-1", "-1.5", ".5", "5.", "1e3", "1E3", "1e-3", "+1", "1e+3", "0", "-0", "1.7976931348623157e308",
                         "1e999", "NaN", "inf", "0x10", "1_0", "1.5.5", "--1"]):
    case(f"lp_float_{i}", [("w", f"m v={num} 1", {}), ("q", "SELECT v FROM m", {})])
for i, num in enumerate(["1i", "-1i", "0i", "+1i", "9223372036854775807i", "9223372036854775808i", "-9223372036854775808i",
                         "-9223372036854775809i", "1.5i", "i", "1ii", "1u", "18446744073709551615u", "1e3i", "007i"]):
    case(f"lp_int_{i}", [("w", f"m v={num} 1", {}), ("q", "SELECT v FROM m", {})])
case("lp_string_escapes", [("w", 'm s="a b,c=d" 1\nm s="q\\"uote" 2\nm s="back\\\\slash" 3\nm s="nl\nx" 4\nm s="tab\\t" 5\nm s="\\n" 6', {}),
                           ("q", "SELECT s FROM m", {})])
case("lp_tag_escapes", [("w", 'm,a\\ b=c\\,d,e\\=f=g\\=h v=1 1\nm\\,x\\ y,k=v v=2 2', {}),
                        ("q", "SHOW MEASUREMENTS", {}), ("q", "SHOW TAG KEYS", {}), ("q", 'SHOW TAG VALUES WITH KEY = "a b"', {}),
                        ("q", "SELECT * FROM /.*/", {})])
case("lp_field_key_escape", [("w", 'm f\\ k=1,g\\=h=2,i\\,j=3 1', {}), ("q", "SELECT * FROM m", {}), ("q", "SHOW FIELD KEYS", {})])
case("lp_backslash_literal", [("w", 'm,t=a\\b v=1 1', {}), ("q", "SELECT * FROM m", {})])
case("lp_unicode", [("w", 'météo,ville=Zürich,😀=x temp=1.5,"n"=1,名前="値" 1', {}), ("q", "SELECT * FROM /.*/", {}),
                    ("q", "SHOW MEASUREMENTS", {})])
for i, (p, mul) in enumerate([("n", 1), ("ns", 1), ("u", 1000), ("us", 1000), ("ms", 10**6), ("s", 10**9), ("m", 60 * 10**9), ("h", 3600 * 10**9), (None, 1)]):
    case(f"lp_precision_{p}", [("w", "m v=1 1500", {"precision": p} if p else {}), ("q", "SELECT v FROM m", {}),
                               ("q", "SELECT v FROM m", {"epoch": "ns"})])
case("lp_precision_invalid", [("w", "m v=1 1", {"precision": "x"}), ("w", "m v=1 1", {"precision": "seconds"})])
case("lp_precision_v2_names", [("w", "m v=1 1", {"precision": "ns"}), ("w", "m v=2 2", {"precision": "us"})])
case("lp_no_timestamp", [("w", "m v=1", {}), ("q", "SELECT count(v) FROM m", {})], norm=["time"])

case("lp_timestamp_negative", [("w", "m v=1 -1", {}), ("q", "SELECT v FROM m", {})])
case("lp_timestamp_zero", [("w", "m v=1 0", {}), ("q", "SELECT v FROM m", {})])
case("lp_timestamp_bad", [("w", "m v=1 1.5", {}), ("w", "m v=1 1e3", {}), ("w", "m v=1 12a", {}), ("w", "m v=1 9223372036854775807", {}),
                          ("w", "m v=1 9223372036854775808", {})])
case("lp_timestamp_range", [("w", "m v=1 -9223372036854775808", {}), ("w", "m v=2 -9223372036854775807", {}),
                            ("w", "m v=3 9223372036854775806", {}), ("q", "SELECT v FROM m", {})])
case("lp_dup_same_ts_overwrite", [("w", "m,a=1 v=1 100", {}), ("w", "m,a=1 v=2 100", {}), ("q", "SELECT * FROM m", {})])
case("lp_dup_same_ts_merge_fields", [("w", "m,a=1 v=1 100", {}), ("w", "m,a=1 w=2 100", {}), ("q", "SELECT * FROM m", {}),
                                     ("w", "m,a=1 v=9,x=3 100", {}), ("q", "SELECT * FROM m", {})])
case("lp_dup_in_batch", [("w", "m v=1 100\nm v=2 100\nm w=5 100", {}), ("q", "SELECT * FROM m", {})])
case("lp_dup_diff_tags_same_ts", [("w", "m,a=1 v=1 100\nm,a=2 v=2 100", {}), ("q", "SELECT * FROM m", {})])
case("lp_multiline", [("w", "a v=1 1\nb v=2 2\na v=3 3\n", {}), ("q", "SHOW MEASUREMENTS", {}), ("q", "SELECT * FROM a", {}), ("q", "SELECT * FROM b", {})])
case("lp_blank_and_comments", [("w", "# comment\n\nm v=1 1\n   \n# another\nm v=2 2\n\n", {}), ("q", "SELECT v FROM m", {})])
case("lp_only_comments", [("w", "# c\n", {}), ("w", "", {}), ("w", "\n\n", {})])
case("lp_crlf", [("w", "m v=1 1\r\nm v=2 2\r\n", {}), ("q", "SELECT v FROM m", {})])
case("lp_trailing_space_ts", [("w", "m v=1 1 ", {}), ("w", "m v=1  2", {}), ("w", "m  v=1 3", {}), ("w", " m v=1 4", {}),
                              ("q", "SELECT v FROM m", {})])
case("lp_tab_separator", [("w", "m\tv=1 1", {}), ("w", "m v=1\t1", {})])
for i, bad in enumerate(["m", "m v", "m v=", "m =1", "m,a v=1", "m,a= v=1", "m,=b v=1", "m v=1,", "m v=1,,w=2", ",a=1 v=1",
                         "m v=\"x", "m v=1 1 1", "m,a=1,a=2 v=1", "m v=1,v=2", "m v=abc", "m v=1abc", "m,a=1 ", "m,a=1", 'm v="x"y', 'm v="x" ,w=1',
                         "m,a=b,c v=1", "m,a=b v=1,", "m v=1 ,w=2", "m v=--1", "m v=1e", "m v=e1", "m v=.", "m v=1 i"]):
    case(f"lp_bad_{i}", [("w", bad, {}), ("q", "SELECT * FROM m", {})], norm=["time"])
case("lp_partial_write", [("w", "m v=1 1\nbadline\nm v=3 3\nalso bad\nm v=5 5", {}), ("q", "SELECT v FROM m", {})])
case("lp_partial_write_first_bad", [("w", "bad\nm v=3 3", {}), ("q", "SELECT v FROM m", {})])
case("lp_type_conflict_float_then_int", [("w", "m v=1.5 1", {}), ("w", "m v=2i 2", {}), ("q", "SELECT * FROM m", {}), ("q", "SHOW FIELD KEYS", {})])
case("lp_type_conflict_int_then_float", [("w", "m v=1i 1", {}), ("w", "m v=2.5 2", {}), ("q", "SELECT * FROM m", {})])
case("lp_type_conflict_str_then_float", [("w", 'm v="a" 1', {}), ("w", "m v=2.5 2", {}), ("q", "SELECT * FROM m", {})])
case("lp_type_conflict_bool_then_str", [("w", 'm v=true 1', {}), ("w", 'm v="a" 2', {}), ("q", "SELECT * FROM m", {})])
case("lp_type_conflict_same_batch", [("w", "m v=1.5 1\nm v=2i 2\nm v=3.5 3", {}), ("q", "SELECT * FROM m", {})])
case("lp_type_conflict_other_field_ok", [("w", "m v=1.5,w=1i 1", {}), ("w", "m v=2.5,w=2.5 2", {}), ("q", "SELECT * FROM m", {})])
case("lp_type_conflict_diff_tags", [("w", "m,a=1 v=1.5 1", {}), ("w", "m,a=2 v=2i 2", {}), ("q", "SELECT * FROM m", {})])
case("lp_type_across_measurements", [("w", "m v=1.5 1\nn v=2i 2", {}), ("q", "SELECT * FROM m", {}), ("q", "SELECT * FROM n", {})])
case("lp_tag_field_same_key", [("w", "m,a=1 a=2 1", {}), ("q", "SELECT * FROM m", {}), ("q", "SHOW TAG KEYS", {}), ("q", "SHOW FIELD KEYS", {}),
                               ("q", "SELECT a FROM m", {}), ("q", 'SELECT "a"::tag, "a"::field FROM m', {})])
case("lp_tag_field_same_key_conflict_types", [("w", "m,a=1 v=1 1", {}), ("w", "m a=2i 2", {}), ("q", "SELECT * FROM m", {})])
case("lp_long_names", [("w", f"{'x' * 300},{'t' * 300}={'v' * 300} {'f' * 300}=1 1", {}), ("q", "SHOW MEASUREMENTS", {})])
case("lp_very_long_string", [("w", 'm s="' + 'a' * 100000 + '" 1', {}), ("q", "SELECT length(s) FROM m", {})])
case("lp_special_measurement_names", [("w", 'a-b v=1 1\nSELECT v=1 2\n_x v=1 3\n1abc v=1 4\nA.B v=1 5\nMixedCase v=1 6', {}), ("q", "SHOW MEASUREMENTS", {})])
case("lp_case_sensitive", [("w", "Cpu v=1 1\ncpu v=2 2\nCPU,Host=a,host=b V=3,v=4 3", {}), ("q", "SHOW MEASUREMENTS", {}), ("q", "SELECT * FROM cpu", {}),
                           ("q", "SELECT * FROM Cpu", {}), ("q", "SELECT * FROM CPU", {})])
case("lp_gzip", [("wgz", "m v=1 1\nm v=2 2", {}), ("q", "SELECT v FROM m", {})])
case("lp_gzip_bad", [("raw", "POST", "/write", {"db": "{db}"}, "notgzip", {"Content-Encoding": "gzip"})])
case("lp_content_types", [("w", "m v=1 1", {}, {"Content-Type": "text/plain"}), ("w", "m v=2 2", {}, {"Content-Type": "application/octet-stream"}),
                          ("w", "m v=3 3", {}, {"Content-Type": "application/x-www-form-urlencoded"}), ("q", "SELECT count(v) FROM m", {})])
case("lp_write_no_db_param", [("raw", "POST", "/write", {}, "m v=1 1", {})])
case("lp_write_nonexistent_db", [("raw", "POST", "/write", {"db": "nope_db"}, "m v=1 1", {})])
case("lp_write_empty_db", [("raw", "POST", "/write", {"db": ""}, "m v=1 1", {})])
case("lp_write_get_method", [("raw", "GET", "/write", {"db": "{db}"}, None, {})])
case("lp_write_put_method", [("raw", "PUT", "/write", {"db": "{db}"}, "m v=1 1", {})])
case("lp_write_rp_default", [("w", "m v=1 1", {"rp": "autogen"}), ("q", "SELECT v FROM m", {})])
case("lp_write_rp_missing", [("w", "m v=1 1", {"rp": "nosuchrp"})])
case("lp_write_rp_created", [("q", "CREATE RETENTION POLICY r1 ON {db} DURATION 0s REPLICATION 1", {}, "POST"), ("w", "m v=1 1", {"rp": "r1"}),
                             ("q", "SELECT v FROM m", {}), ("q", 'SELECT v FROM r1.m', {}), ("q", 'SELECT v FROM "{db}".r1.m', {}),
                             ("q", 'SELECT v FROM autogen.m', {})])
case("lp_write_consistency_param", [("w", "m v=1 1", {"consistency": "all"}), ("w", "m v=1 1", {"consistency": "bogus"})])
case("lp_many_fields_tags", [("w", ",".join(["m"] + [f"t{i}=v{i}" for i in range(50)]) + " " + ",".join(f"f{i}={i}i" for i in range(50)) + " 1", {}),
                             ("q", "SELECT f0, f49, t0, t49 FROM m", {})])
case("lp_tag_empty_value", [("w", "m,a= v=1 1", {}), ("q", "SELECT * FROM m", {})])
case("lp_tag_sorting", [("w", "m,z=1,a=2,m=3 v=1 1", {}), ("q", "SHOW SERIES", {}), ("q", "SELECT * FROM m", {})])
case("lp_field_order", [("w", "m z=1,a=2,m=3 1", {}), ("q", "SELECT * FROM m", {})])
case("lp_float_formats_out", [("w", "m a=1.0,b=100000000000000000000.0,c=0.000001,d=1.5e-10,e=-0.0,f=123456789.123456789,g=1e21 1", {}),
                              ("q", "SELECT * FROM m", {})])
case("lp_int_extremes_out", [("w", "m a=9223372036854775807i,b=-9223372036854775808i 1", {}), ("q", "SELECT * FROM m", {})])
case("lp_v2_style_write_endpoint", [("raw", "POST", "/api/v2/write", {"org": "o", "bucket": "{db}"}, "m v=1 1", {})])

# ---------------------------------------------------------------- ENDPOINTS
case("ep_ping_get", [("raw", "GET", "/ping", {}, None, {})], hdrs=["X-Influxdb-Version"])
case("ep_ping_head", [("raw", "HEAD", "/ping", {}, None, {})])
case("ep_ping_verbose", [("raw", "GET", "/ping", {"verbose": "true"}, None, {})])
case("ep_health", [("raw", "GET", "/health", {}, None, {})], norm=["version"])
case("ep_404", [("raw", "GET", "/nothing", {}, None, {})])
case("ep_query_no_q", [("raw", "GET", "/query", {"db": "{db}"}, None, {})])
case("ep_query_put", [("raw", "PUT", "/query", {"q": "SHOW DATABASES"}, None, {})])
case("ep_query_post_form", [("raw", "POST", "/query", {}, "q=SHOW+MEASUREMENTS&db={db}", {"Content-Type": "application/x-www-form-urlencoded"})])
case("ep_query_post_body_q", [("raw", "POST", "/query", {"db": "{db}"}, "q=SHOW+MEASUREMENTS", {"Content-Type": "application/x-www-form-urlencoded"})])
case("ep_flux_query", [("raw", "POST", "/api/v2/query", {"org": "o"}, 'from(bucket:"b") |> range(start:-1h)', {"Content-Type": "application/vnd.flux"})],
     accepted={"/api/v2/query": "Flux is out of scope: real 1.8 answers 403 (flux disabled), Warp answers 501 with a clear 'not supported' JSON error"})
case("ep_debug_vars", [("raw", "GET", "/debug/requests", {}, None, {})], accepted={"/debug/": "InfluxDB /debug endpoints (pprof, vars, requests) are not implemented"})
case("ep_ready", [("raw", "GET", "/ready", {}, None, {})])
case("ep_auth_bad_basic", [("raw", "GET", "/query", {"q": "SHOW DATABASES", "u": "x", "p": "y"}, None, {})], norm=["dbs"])

# ---------------------------------------------------------------- SCHEMA / SHOW / DDL
case("show_databases", [("q", "SHOW DATABASES", {}), ("q", "CREATE DATABASE {db}_2", {}, "POST"), ("q", "SHOW DATABASES", {}),
                        ("q", "DROP DATABASE {db}_2", {}, "POST"), ("q", "SHOW DATABASES", {})], norm=["dbs"])
case("create_db_twice", [("q", "CREATE DATABASE {db}", {}, "POST"), ("q", "CREATE DATABASE {db}", {}, "POST")])
case("create_db_bad", [("q", "CREATE DATABASE", {}, "POST"), ("q", "CREATE DATABASE 1x", {}, "POST"), ("q", 'CREATE DATABASE "a b"', {}, "POST"),
                       ("q", 'DROP DATABASE "a b"', {}, "POST")])
case("create_db_with_rp", [("q", "CREATE DATABASE {db}_3 WITH DURATION 1d REPLICATION 1 NAME myrp", {}, "POST"), ("q", "SHOW RETENTION POLICIES ON {db}_3", {}),
                           ("q", "DROP DATABASE {db}_3", {}, "POST")])
case("drop_db_nonexistent", [("q", "DROP DATABASE nosuchdb_zz", {}, "POST")])
case("drop_db_then_query", [("w", "m v=1 1", {}), ("q", "DROP DATABASE {db}", {}, "POST"), ("q", "SELECT * FROM m", {}), ("q", "SHOW MEASUREMENTS", {}),
                            ("q", "CREATE DATABASE {db}", {}, "POST"), ("q", "SHOW MEASUREMENTS", {}), ("q", "SELECT * FROM m", {})])
case("show_measurements_basic", [DATA, ("q", "SHOW MEASUREMENTS", {}), ("q", "SHOW MEASUREMENTS LIMIT 1", {}), ("q", "SHOW MEASUREMENTS WITH MEASUREMENT = cpu", {}),
                                 ("q", "SHOW MEASUREMENTS WITH MEASUREMENT =~ /c.*/", {}), ("q", "SHOW MEASUREMENTS WITH MEASUREMENT =~ /x/", {}),
                                 ("q", "SHOW MEASUREMENTS WHERE host = 'a'", {}), ("q", "SHOW MEASUREMENTS ON {db}", {}), ("q", "SHOW MEASUREMENTS OFFSET 1", {})])
case("show_measurements_empty", [("q", "SHOW MEASUREMENTS", {}), ("q", "SHOW TAG KEYS", {}), ("q", "SHOW FIELD KEYS", {}), ("q", "SHOW SERIES", {}),
                                 ("q", "SHOW TAG VALUES WITH KEY = host", {})])
case("show_tag_keys", [DATA, ("q", "SHOW TAG KEYS", {}), ("q", "SHOW TAG KEYS FROM cpu", {}), ("q", "SHOW TAG KEYS FROM cpu LIMIT 1", {}),
                       ("q", "SHOW TAG KEYS FROM cpu WHERE host = 'a'", {}), ("q", "SHOW TAG KEYS FROM nomeas", {}), ("q", "SHOW TAG KEYS ON {db}", {}),
                       ("q", "SHOW TAG KEYS FROM /c.*/", {}), ("q", "SHOW TAG KEYS OFFSET 1", {})])
case("show_tag_values", [DATA, ("q", "SHOW TAG VALUES WITH KEY = host", {}), ("q", "SHOW TAG VALUES FROM cpu WITH KEY = region", {}),
                         ("q", "SHOW TAG VALUES FROM cpu WITH KEY IN (host, region)", {}), ("q", "SHOW TAG VALUES FROM cpu WITH KEY =~ /h.*/", {}),
                         ("q", "SHOW TAG VALUES FROM cpu WITH KEY != region", {}), ("q", "SHOW TAG VALUES FROM cpu WITH KEY = host LIMIT 2", {}),
                         ("q", "SHOW TAG VALUES FROM cpu WITH KEY = host WHERE region = 'eu'", {}), ("q", "SHOW TAG VALUES FROM cpu WITH KEY = host OFFSET 1", {}),
                         ("q", "SHOW TAG VALUES FROM cpu WITH KEY = nosuch", {}), ("q", "SHOW TAG VALUES", {}), ("q", "SHOW TAG VALUES CARDINALITY WITH KEY = host", {}),
                         ("q", "SHOW TAG VALUES FROM cpu WITH KEY = host WHERE time > 0", {})])
case("show_field_keys", [DATA, ("q", "SHOW FIELD KEYS", {}), ("q", "SHOW FIELD KEYS FROM cpu", {}), ("q", "SHOW FIELD KEYS FROM nomeas", {}),
                         ("q", "SHOW FIELD KEYS ON {db}", {}), ("q", "SHOW FIELD KEYS FROM /c.*/", {}), ("q", "SHOW FIELD KEYS LIMIT 2", {})])
case("show_field_keys_multi_type", [("w", 'm a=1i,b=1.5,c="s",d=true 1', {}), ("q", "SHOW FIELD KEYS", {})])
case("show_series", [DATA, ("q", "SHOW SERIES", {}), ("q", "SHOW SERIES FROM cpu", {}), ("q", "SHOW SERIES FROM cpu WHERE host = 'a'", {}),
                     ("q", "SHOW SERIES WHERE region = 'eu'", {}), ("q", "SHOW SERIES LIMIT 1", {}), ("q", "SHOW SERIES OFFSET 1", {}),
                     ("q", "SHOW SERIES ON {db}", {}), ("q", "SHOW SERIES FROM nomeas", {}), ("q", "SHOW SERIES WHERE time > 0", {})])
case("show_series_cardinality", [DATA, ("q", "SHOW SERIES CARDINALITY", {}), ("q", "SHOW MEASUREMENT CARDINALITY", {}), ("q", "SHOW TAG KEY CARDINALITY", {}),
                                 ("q", "SHOW FIELD KEY CARDINALITY", {})])
case("show_rp", [("q", "SHOW RETENTION POLICIES", {}), ("q", "SHOW RETENTION POLICIES ON {db}", {}), ("q", "SHOW RETENTION POLICIES ON nosuchdb_zz", {}),
                 ("q", "SHOW RETENTION POLICIES", {"db": ""})])
case("show_misc", [("q", "SHOW USERS", {}), ("q", "SHOW GRANTS FOR admin", {}), ("q", "SHOW QUERIES", {}), ("q", "SHOW STATS", {}), ("q", "SHOW DIAGNOSTICS", {}),
                   ("q", "SHOW CONTINUOUS QUERIES", {}), ("q", "SHOW SUBSCRIPTIONS", {}), ("q", "SHOW SHARDS", {}), ("q", "SHOW SHARD GROUPS", {}),
                   ("q", "SHOW NOTHING", {}), ("q", "SHOW", {})], accepted={"SHOW USERS": "server-global state (users survive across cases in the oracle)", "SHOW CONTINUOUS": "lists every database of the server"}, skip_compare=["SHOW STATS", "SHOW DIAGNOSTICS", "SHOW SHARDS", "SHOW SHARD GROUPS", "SHOW QUERIES"])
case("create_drop_rp", [("q", "CREATE RETENTION POLICY r2 ON {db} DURATION 1h REPLICATION 1", {}, "POST"), ("q", "SHOW RETENTION POLICIES", {}),
                        ("q", "CREATE RETENTION POLICY r2 ON {db} DURATION 1h REPLICATION 1", {}, "POST"),
                        ("q", "CREATE RETENTION POLICY r2 ON {db} DURATION 2h REPLICATION 1", {}, "POST"),
                        ("q", "ALTER RETENTION POLICY r2 ON {db} DURATION 3h DEFAULT", {}, "POST"), ("q", "SHOW RETENTION POLICIES", {}),
                        ("q", "DROP RETENTION POLICY r2 ON {db}", {}, "POST"), ("q", "SHOW RETENTION POLICIES", {}),
                        ("q", "DROP RETENTION POLICY nosuch ON {db}", {}, "POST"), ("q", "CREATE RETENTION POLICY r3 ON nosuchdb_zz DURATION 1h REPLICATION 1", {}, "POST")])
case("drop_measurement", [DATA, ("q", "DROP MEASUREMENT cpu", {}, "POST"), ("q", "SHOW MEASUREMENTS", {}), ("q", "SELECT * FROM cpu", {}), ("q", "DROP MEASUREMENT cpu", {}, "POST"),
                          ("w", "cpu v=1 1", {}), ("q", "SELECT * FROM cpu", {})])
case("drop_measurement_get", [DATA, ("q", "DROP MEASUREMENT cpu", {}), ("q", "SHOW MEASUREMENTS", {})])
case("drop_measurement_type_reset", [("w", "m v=1.5 1", {}), ("q", "DROP MEASUREMENT m", {}, "POST"), ("w", "m v=2i 2", {}), ("q", "SELECT * FROM m", {}), ("q", "SHOW FIELD KEYS", {})])
case("drop_series", [DATA, ("q", "DROP SERIES FROM cpu WHERE host = 'a'", {}, "POST"), ("q", "SELECT count(usage) FROM cpu GROUP BY host", {}),
                     ("q", "SHOW SERIES", {}), ("q", "DROP SERIES FROM cpu", {}, "POST"), ("q", "SHOW MEASUREMENTS", {}),
                     ("q", "DROP SERIES FROM cpu WHERE time > 0", {}, "POST"), ("q", "DROP SERIES", {}, "POST")])
case("drop_series_regex_where_tag", [DATA, ("q", "DROP SERIES WHERE region = 'eu'", {}, "POST"), ("q", "SHOW SERIES", {})])
case("delete_from", [DATA, ("q", f"DELETE FROM cpu WHERE time < {ts(20)}", {}, "POST"), ("q", "SELECT count(usage) FROM cpu", {}),
                     ("q", "DELETE FROM cpu WHERE host = 'a'", {}, "POST"), ("q", "SELECT count(usage) FROM cpu GROUP BY host", {}),
                     ("q", f"DELETE FROM cpu WHERE host = 'b' AND time >= {ts(30)}", {}, "POST"), ("q", "SELECT count(usage) FROM cpu GROUP BY host", {}),
                     ("q", "DELETE FROM cpu WHERE usage > 1", {}, "POST"), ("q", "DELETE FROM nomeas", {}, "POST"),
                     ("q", "DELETE FROM cpu", {}, "POST"), ("q", "SHOW MEASUREMENTS", {})])
case("delete_get", [DATA, ("q", "DELETE FROM cpu WHERE host = 'a'", {})])
case("delete_time_forms", [DATA, ("q", "DELETE FROM cpu WHERE time < '2020-01-01T00:00:20Z'", {}, "POST"), ("q", "SELECT count(usage) FROM cpu", {}),
                           ("q", "DELETE WHERE time >= '2020-01-01T00:00:40Z'", {}, "POST"), ("q", "SELECT count(usage) FROM cpu", {})])
case("select_into", [DATA, ("q", "SELECT usage INTO cpu_copy FROM cpu", {}, "POST"), ("q", "SELECT * FROM cpu_copy", {}), ("q", "SELECT usage INTO cpu_copy2 FROM cpu", {}),
                     ("q", "SELECT mean(usage) INTO cpu_mean FROM cpu GROUP BY time(30s), host", {}, "POST"), ("q", "SELECT * FROM cpu_mean", {}),
                     ("q", "SELECT * INTO cpu_all FROM cpu GROUP BY *", {}, "POST"), ("q", "SELECT * FROM cpu_all", {}), ("q", "SHOW MEASUREMENTS", {}),
                     ("q", "SELECT usage INTO {db}.autogen.cpu_q FROM cpu", {}, "POST"), ("q", "SELECT count(*) FROM cpu_q", {}),
                     ("q", "SELECT usage INTO :MEASUREMENT FROM /c.*/", {}, "POST")])
case("multi_statement", [DATA, ("q", "SHOW MEASUREMENTS; SELECT count(usage) FROM cpu; SHOW TAG KEYS", {}),
                         ("q", "SELECT count(usage) FROM cpu; SELECT * FROM nomeas; SELECT max(usage) FROM cpu", {}),
                         ("q", "SELECT 1; SHOW MEASUREMENTS", {}), ("q", "SHOW MEASUREMENTS;", {}), ("q", ";", {}), ("q", "SHOW MEASUREMENTS;;SHOW MEASUREMENTS", {}),
                         ("q", "SELECT count(usage) FROM cpu; DROP DATABASE nosuchdb_zz", {}, "POST")])
case("write_stmt_via_get", [("q", "CREATE DATABASE {db}_g", {}), ("q", "SHOW DATABASES", {}), ("q", "DROP DATABASE {db}_g", {}), ("q", "SHOW DATABASES", {}),
                            ("q", "CREATE RETENTION POLICY rg ON {db} DURATION 1h REPLICATION 1", {}), ("q", "SHOW RETENTION POLICIES", {})], norm=["dbs"])
case("kill_query_like", [("q", "KILL QUERY 99999", {}, "POST"), ("q", "CREATE USER u1 WITH PASSWORD 'x'", {}, "POST"), ("q", "SHOW USERS", {})])

# ---------------------------------------------------------------- SELECT: shapes
S = [DATA]
case("sel_star", S + [("q", "SELECT * FROM cpu", {})])
case("sel_star_limit", S + [("q", "SELECT * FROM cpu LIMIT 3", {}), ("q", "SELECT * FROM cpu LIMIT 0", {}), ("q", "SELECT * FROM cpu LIMIT -1", {}),
                            ("q", "SELECT * FROM cpu LIMIT 3 OFFSET 2", {}), ("q", "SELECT * FROM cpu OFFSET 2", {}), ("q", "SELECT * FROM cpu LIMIT 100 OFFSET 100", {})])
case("sel_fields", S + [("q", "SELECT usage FROM cpu", {}), ("q", "SELECT usage, idle FROM cpu", {}), ("q", "SELECT idle, usage FROM cpu", {}),
                        ("q", "SELECT usage, usage FROM cpu", {}), ("q", "SELECT usage AS u, idle AS i FROM cpu", {}), ("q", "SELECT usage AS x, idle AS x FROM cpu", {}),
                        ("q", 'SELECT "usage" FROM "cpu"', {}), ("q", 'SELECT "usage" FROM "{db}"."autogen"."cpu"', {}), ("q", 'SELECT usage FROM {db}..cpu', {}),
                        ("q", "SELECT nosuch FROM cpu", {}), ("q", "SELECT usage, nosuch FROM cpu", {})])
case("sel_tags_in_list", S + [("q", "SELECT host FROM cpu", {}), ("q", "SELECT host, usage FROM cpu", {}), ("q", "SELECT usage, host FROM cpu", {}),
                              ("q", "SELECT host, region FROM cpu", {}), ("q", "SELECT host::tag FROM cpu", {}), ("q", "SELECT usage::field FROM cpu", {}),
                              ("q", "SELECT usage::float FROM cpu", {}), ("q", "SELECT n::integer FROM cpu", {}), ("q", "SELECT n::float FROM cpu", {}),
                              ("q", "SELECT msg::string FROM cpu", {}), ("q", "SELECT ok::boolean FROM cpu", {}), ("q", "SELECT usage::integer FROM cpu", {}),
                              ("q", "SELECT host::field FROM cpu", {})])
case("sel_star_types", S + [("q", "SELECT *::field FROM cpu", {}), ("q", "SELECT *::tag FROM cpu", {}), ("q", "SELECT /u.*/ FROM cpu", {}), ("q", "SELECT /h.*/ FROM cpu", {}),
                            ("q", "SELECT /^(us|id)/ FROM cpu", {}), ("q", "SELECT /nomatch/ FROM cpu", {})])
case("sel_field_values_json", S + [("q", "SELECT usage, n, ok, msg FROM cpu WHERE host = 'a'", {})])
case("sel_from_forms", S + [("q", "SELECT count(usage) FROM cpu, cpu", {}), ("q", "SELECT usage FROM /c.*/", {}), ("q", "SELECT usage FROM /nomatch/", {}),
                            ("q", "SELECT usage FROM cpu, nomeas", {}), ("q", "SELECT usage FROM nomeas", {}), ("q", "SELECT usage FROM cpu LIMIT 1", {})])
case("sel_multi_measurement", [("w", "a v=1 1\na v=2 2\nb v=3 1\nb v=4 2", {}), ("q", "SELECT * FROM a, b", {}), ("q", "SELECT sum(v) FROM a, b", {}),
                               ("q", "SELECT * FROM /.*/", {}), ("q", "SELECT sum(v) FROM /.*/", {}), ("q", "SELECT count(v) FROM /.*/ GROUP BY time(1s)", {})])
case("sel_no_from", [("q", "SELECT 1", {}), ("q", "SELECT 1+1", {}), ("q", "SELECT now()", {}), ("q", "SELECT", {}), ("q", "SELECT FROM cpu", {})])
case("sel_null_fields", [("w", "m a=1i 1\nm b=2i 2\nm a=3i,b=4i 3", {}), ("q", "SELECT * FROM m", {}), ("q", "SELECT a, b FROM m", {}), ("q", "SELECT a FROM m", {}),
                         ("q", "SELECT b FROM m", {}), ("q", "SELECT a + b FROM m", {}), ("q", "SELECT count(a), count(b) FROM m", {}),
                         ("q", "SELECT a FROM m WHERE b > 0", {}), ("q", "SELECT sum(a) FROM m GROUP BY time(1s)", {})])
case("sel_time_only", S + [("q", "SELECT time FROM cpu", {}), ("q", "SELECT time, usage FROM cpu", {}), ("q", "SELECT usage, time FROM cpu", {}), ("q", "SELECT time AS t, usage FROM cpu", {})])

# ---------------------------------------------------------------- SELECT: WHERE
W = lambda name, wheres, base="SELECT usage, host FROM cpu": case(name, S + [("q", f"{base} WHERE {w}", {}) for w in wheres])
W("where_tag_eq", ["host = 'a'", "host = 'zz'", "host = ''", '"host" = \'a\'', "host='a'", "'a' = host", "host = \"a\"", "host = a"])
W("where_tag_neq", ["host != 'a'", "host <> 'a'", "host != 'zz'", "host != ''", "host <> ''"])
W("where_tag_regex", ["host =~ /a/", "host =~ /^[ab]$/", "host !~ /a/", "host =~ /A/", "host =~ /(?i)A/", "host =~ /.*/", "host =~ /^$/", "host !~ /^$/",
                      "region =~ /u.*/ AND host !~ /b/", "host =~ /[/", "host =~ /a|c/", "host =~ /a/ OR host =~ /c/", "host =~ 'a'", "/a/ =~ host"])
W("where_tag_ordering", ["host > 'a'", "host < 'b'", "host >= 'b'", "host <= 'b'"])
W("where_field_cmp", ["usage > 20", "usage >= 20", "usage < 20", "usage <= 20", "usage = 20", "usage != 20", "usage <> 20", "usage > 20.5", "usage > 20 AND usage < 30",
                      "usage < 12 OR usage > 35", "usage = 10", "usage > -5", "usage > 1e1", "20 < usage", "usage > '20'", "usage > true"])
W("where_field_int", ["n > 2", "n = 3", "n != 3", "n >= 5", "n < 1", "n = 3.0", "n > 2.5", "n = 3i"])
W("where_field_str", ["msg = 'ma0'", "msg != 'ma0'", "msg =~ /^ma/", "msg !~ /^m[ab]/", "msg > 'mb0'", "msg = ma0", "msg = 'nope'", "msg =~ /a.*3/"])
W("where_field_bool", ["ok = true", "ok = false", "ok != true", "ok = 't'", "ok = TRUE", "ok = 1", "ok"], base="SELECT ok, n FROM cpu")
W("where_and_or", ["host = 'a' AND region = 'us'", "host = 'a' OR host = 'b'", "host = 'a' AND usage > 12 OR host = 'c'", "host = 'a' AND (usage > 12 OR host = 'c')",
                   "(host = 'a' OR host = 'b') AND usage > 25", "((host = 'a'))", "NOT host = 'a'", "host = 'a' and usage > 12", "host = 'a' or host = 'b'",
                   "host = 'a' AND", "AND host = 'a'", "()", "(host = 'a'", "host = 'a')", "host", "1", "1 = 1", "'a' = 'a'", "true", "host = 'a' AND nosuch = 1",
                   "nosuch = 'x'", "nosuch != 'x'", "nosuch =~ /x/", "nosuch > 1"])
W("where_arith", ["usage + 1 > 30", "usage * 2 > 60", "usage / 2 > 15", "usage - 10 > 20", "n % 2 = 0", "usage + idle > 100", "n + 1 = 3", "usage > n * 10", "n & 1 = 1", "1 + 1 = 2"])
W("where_time_abs", [f"time > {ts(20)}", f"time >= {ts(20)}", f"time < {ts(20)}", f"time <= {ts(20)}", f"time = {ts(20)}", f"time != {ts(20)}",
                     "time > '2020-01-01T00:00:20Z'", "time >= '2020-01-01T00:00:20Z' AND time < '2020-01-01T00:00:40Z'", "time > '2020-01-01 00:00:20'",
                     "time > '2020-01-01'", "time > '2020-01-01T00:00:20.5Z'", "time > '2020-01-01T00:00:20+01:00'", "time > 'garbage'", "time > 1577836820000000000",
                     "time > 20s", "time > 1577836820s", "time > 1577836820000ms", "time > 1577836820000000u", "time = '2020-01-01T00:00:20Z'",
                     "time > '2020-01-01T00:00:20Z' OR host = 'c'", "time < '2020-01-01T00:00:20Z' OR time > '2020-01-01T00:00:40Z'",
                     "time > '2020-01-01T00:00:20Z' AND time < '2020-01-01T00:00:20Z'", "time > now()", "time < now()", "time > now() - 100000d", "time > now() + 1h",
                     "time < now() - 1h", "time > now() - 1h AND time < now() + 1h", "now() > time", "time > 0", "time < 0", "time > -1", "time > '2020-01-01T00:00:20Z' AND host = 'c'",
                     "time > usage", "time + 1 > 5", "time =~ /a/", "time > '2020-01-01T00:00:20Z' - 1h"])
W("where_time_only_unit", ["time > now() - 10w", "time > now() - 1y", "time > now() - 30m", "time > now() - 10u", "time > now() - 10ns", "time > now() - 1"])
case("where_time_ordering_bounds", S + [("q", "SELECT usage FROM cpu WHERE time >= '2020-01-01T00:00:10Z' AND time <= '2020-01-01T00:00:30Z' AND host = 'a'", {}),
                                        ("q", "SELECT usage FROM cpu WHERE time > '2020-01-01T00:00:10Z' AND time < '2020-01-01T00:00:30Z' AND host = 'a'", {})])
case("where_string_quoting", [("w", "m,t=it's v=1 1\nm,t=say\\ \"hi\" v=2 2\nm,t=a\\\\b v=3 3", {}), ("q", "SELECT v FROM m WHERE t = 'it\\'s'", {}),
                              ("q", "SELECT v FROM m WHERE t = 'a\\\\b'", {}), ("q", "SELECT v FROM m WHERE t = 'say \"hi\"'", {}), ("q", "SELECT * FROM m", {})])
case("where_tag_missing_semantics", [("w", "m,a=1 v=1 1\nm v=2 2\nm,b=2 v=3 3", {}), ("q", "SELECT v FROM m WHERE a = ''", {}), ("q", "SELECT v FROM m WHERE a != ''", {}),
                                     ("q", "SELECT v FROM m WHERE a != '1'", {}), ("q", "SELECT v FROM m WHERE a !~ /1/", {}), ("q", "SELECT v FROM m WHERE a =~ /.*/", {}),
                                     ("q", "SELECT v FROM m WHERE a =~ /^$/", {}), ("q", "SELECT v FROM m WHERE b = ''", {}), ("q", "SELECT v FROM m WHERE a = '' AND b = ''", {})])
case("where_field_missing_semantics", [("w", "m a=1i 1\nm b=2i 2\nm a=3i,b=4i 3", {}), ("q", "SELECT * FROM m WHERE a > 0", {}), ("q", "SELECT * FROM m WHERE a != 1", {}),
                                       ("q", "SELECT * FROM m WHERE a > 0 OR b > 0", {}), ("q", "SELECT * FROM m WHERE a < 100 AND b < 100", {}),
                                       ("q", "SELECT * FROM m WHERE NOT a > 0", {})])

# ---------------------------------------------------------------- ORDER / LIMIT / SLIMIT
case("order_by", S + [("q", "SELECT usage FROM cpu WHERE host = 'a' ORDER BY time DESC", {}), ("q", "SELECT usage FROM cpu WHERE host = 'a' ORDER BY time ASC", {}),
                      ("q", "SELECT usage FROM cpu ORDER BY time DESC LIMIT 4", {}), ("q", "SELECT usage FROM cpu ORDER BY time", {}), ("q", "SELECT usage FROM cpu ORDER BY usage", {}),
                      ("q", "SELECT usage FROM cpu GROUP BY host ORDER BY time DESC LIMIT 2", {}), ("q", "SELECT mean(usage) FROM cpu GROUP BY time(20s) ORDER BY time DESC", {}),
                      ("q", "SELECT usage FROM cpu ORDER BY time DESC LIMIT 2 OFFSET 1", {}), ("q", "SELECT * FROM cpu ORDER BY time DESC LIMIT 3", {}),
                      ("q", "SELECT usage FROM cpu ORDER BY DESC", {}), ("q", "SELECT usage FROM cpu ORDER time", {})])
case("slimit", S + [("q", "SELECT usage FROM cpu GROUP BY host SLIMIT 1", {}), ("q", "SELECT usage FROM cpu GROUP BY host SLIMIT 2 SOFFSET 1", {}),
                    ("q", "SELECT usage FROM cpu GROUP BY host SOFFSET 1", {}), ("q", "SELECT usage FROM cpu GROUP BY host SLIMIT 0", {}),
                    ("q", "SELECT usage FROM cpu GROUP BY host LIMIT 1 SLIMIT 1", {}), ("q", "SELECT usage FROM cpu GROUP BY host LIMIT 2 OFFSET 1 SLIMIT 2 SOFFSET 1", {}),
                    ("q", "SELECT usage FROM cpu SLIMIT 1", {}), ("q", "SELECT count(usage) FROM cpu GROUP BY host, region SLIMIT 2", {})])

# ---------------------------------------------------------------- GROUP BY tags
case("group_by_tag", S + [("q", "SELECT count(usage) FROM cpu GROUP BY host", {}), ("q", "SELECT count(usage) FROM cpu GROUP BY host, region", {}),
                          ("q", "SELECT count(usage) FROM cpu GROUP BY region", {}), ("q", "SELECT count(usage) FROM cpu GROUP BY *", {}),
                          ("q", "SELECT usage FROM cpu GROUP BY host", {}), ("q", "SELECT usage FROM cpu GROUP BY *", {}), ("q", "SELECT usage FROM cpu GROUP BY host, region LIMIT 1", {}),
                          ("q", "SELECT count(usage) FROM cpu GROUP BY nosuch", {}), ("q", 'SELECT count(usage) FROM cpu GROUP BY "host"', {}),
                          ("q", "SELECT count(usage) FROM cpu GROUP BY /h.*/", {}), ("q", "SELECT count(usage) FROM cpu GROUP BY host::tag", {}),
                          ("q", "SELECT count(usage) FROM cpu GROUP BY usage", {}), ("q", "SELECT * FROM cpu GROUP BY host LIMIT 1", {}),
                          ("q", "SELECT host, usage FROM cpu GROUP BY host LIMIT 1", {}), ("q", "SELECT count(usage) FROM cpu WHERE host = 'a' GROUP BY region", {}),
                          ("q", "SELECT count(usage) FROM cpu GROUP BY region, host", {})])
case("group_by_tag_missing", [("w", "m,a=1 v=1 1\nm v=2 2\nm,a=1 v=3 3", {}), ("q", "SELECT sum(v) FROM m GROUP BY a", {}), ("q", "SELECT v FROM m GROUP BY a", {}),
                              ("q", "SELECT sum(v) FROM m GROUP BY a, b", {})])

# ---------------------------------------------------------------- Aggregates
AGGS = ["count", "distinct", "integral", "mean", "median", "mode", "spread", "stddev", "sum", "first", "last", "max", "min"]
for a in AGGS:
    case(f"agg_{a}", S + [("q", f"SELECT {a}(usage) FROM cpu", {}), ("q", f"SELECT {a}(n) FROM cpu", {}), ("q", f"SELECT {a}(usage), {a}(idle) FROM cpu", {}),
                          ("q", f"SELECT {a}(usage) FROM cpu GROUP BY host", {}), ("q", f"SELECT {a}(usage) FROM cpu WHERE host = 'a' GROUP BY time(20s)", {}),
                          ("q", f"SELECT {a}(usage) FROM cpu WHERE time >= {ts(0)} AND time < {ts(60)} GROUP BY time(20s), host", {}),
                          ("q", f"SELECT {a}(msg) FROM cpu", {}), ("q", f"SELECT {a}(ok) FROM cpu", {}), ("q", f"SELECT {a}(host) FROM cpu", {}),
                          ("q", f"SELECT {a}(*) FROM cpu", {}), ("q", f"SELECT {a}(/u.*/) FROM cpu", {}), ("q", f"SELECT {a}(nosuch) FROM cpu", {}),
                          ("q", f"SELECT {a}(usage) FROM nomeas", {}), ("q", f"SELECT {a}(usage) AS x FROM cpu", {}),
                          ("q", f"SELECT {a}(usage) FROM cpu WHERE host = 'zz'", {}), ("q", f"SELECT {a}(usage) FROM cpu WHERE host = 'zz' GROUP BY host", {})])
case("agg_count_star", S + [("q", "SELECT count(*) FROM cpu", {}), ("q", "SELECT count(usage), count(msg) FROM cpu", {}), ("q", "SELECT count(distinct(usage)) FROM cpu", {}),
                            ("q", "SELECT count(1) FROM cpu", {}), ("q", "SELECT count() FROM cpu", {}), ("q", "SELECT count(usage, idle) FROM cpu", {}),
                            ("q", "SELECT count(distinct(host)) FROM cpu", {}), ("q", "SELECT count(distinct(msg)) FROM cpu", {}), ("q", "SELECT count(*), sum(usage) FROM cpu", {})])
case("agg_distinct_forms", S + [("q", "SELECT DISTINCT(region) FROM cpu", {}), ("q", "SELECT DISTINCT(n) FROM cpu", {}), ("q", "SELECT DISTINCT(msg) FROM cpu GROUP BY host", {}),
                                ("q", "SELECT DISTINCT(n), DISTINCT(usage) FROM cpu", {}), ("q", "SELECT DISTINCT n FROM cpu", {}), ("q", "SELECT DISTINCT(n) FROM cpu GROUP BY time(30s)", {})])
case("agg_percentile", S + [(f"q", f"SELECT percentile(usage, {p}) FROM cpu", {}) for p in (0, 1, 10, 25, 50, 75, 90, 99, 100, 101, -1, 50.5)] +
     [("q", "SELECT percentile(usage) FROM cpu", {}), ("q", "SELECT percentile(usage, 'a') FROM cpu", {}), ("q", "SELECT percentile(n, 50) FROM cpu", {}),
      ("q", "SELECT percentile(usage, 50) FROM cpu GROUP BY host", {}), ("q", "SELECT percentile(usage, 50) FROM cpu GROUP BY time(30s)", {}),
      ("q", "SELECT percentile(usage, 50), percentile(idle, 50) FROM cpu", {})])
case("agg_sample_top_bottom", S + [("q", "SELECT top(usage, 3) FROM cpu", {}), ("q", "SELECT bottom(usage, 3) FROM cpu", {}), ("q", "SELECT top(usage, 2) FROM cpu GROUP BY host", {}),
                                   ("q", "SELECT top(usage, host, 2) FROM cpu", {}), ("q", "SELECT bottom(usage, 2), host FROM cpu", {}), ("q", "SELECT top(usage, 1) FROM cpu GROUP BY time(30s)", {}),
                                   ("q", "SELECT top(usage) FROM cpu", {}), ("q", "SELECT top(usage, 0) FROM cpu", {}), ("q", "SELECT top(usage, 100) FROM cpu", {}),
                                   ("q", "SELECT top(usage, host, region, 2) FROM cpu", {})])
case("agg_first_last_ties", [("w", "m v=1 5\nm w=2 5\nm v=3 9\nm v=4 9", {}), ("q", "SELECT first(v), last(v) FROM m", {}), ("q", "SELECT first(*), last(*) FROM m", {}),
                             ("q", "SELECT first(v), last(w) FROM m", {})])
case("agg_first_last_with_tags", S + [("q", "SELECT first(usage), host FROM cpu", {}), ("q", "SELECT last(usage), host, region FROM cpu", {}), ("q", "SELECT max(usage), host FROM cpu", {}),
                                      ("q", "SELECT min(usage), n FROM cpu", {}), ("q", "SELECT max(usage), min(usage), host FROM cpu", {}), ("q", "SELECT sum(usage), host FROM cpu", {}),
                                      ("q", "SELECT first(usage), n FROM cpu GROUP BY host", {}), ("q", "SELECT max(usage), idle FROM cpu GROUP BY time(30s)", {})])
case("agg_selector_time", S + [("q", "SELECT max(usage) FROM cpu", {}), ("q", "SELECT min(usage) FROM cpu", {}), ("q", "SELECT first(usage) FROM cpu", {}), ("q", "SELECT last(usage) FROM cpu", {}),
                               ("q", "SELECT sum(usage) FROM cpu", {}), ("q", "SELECT count(usage) FROM cpu", {}),
                               ("q", f"SELECT max(usage) FROM cpu WHERE time >= {ts(10)}", {}), ("q", f"SELECT count(usage) FROM cpu WHERE time >= {ts(10)}", {}),
                               ("q", f"SELECT count(usage) FROM cpu WHERE time >= {ts(10)} AND time < {ts(30)}", {}),
                               ("q", "SELECT count(usage) FROM cpu WHERE time < now()", {})])
case("agg_int_vs_float_types", [("w", "m i=1i,f=1.5 1\nm i=2i,f=2.5 2\nm i=4i,f=3.5 3", {}), ("q", "SELECT mean(i), sum(i), min(i), max(i), median(i), mode(i), spread(i), stddev(i), first(i), last(i), count(i) FROM m", {}),
                                ("q", "SELECT mean(f), sum(f), min(f), max(f), median(f), spread(f), stddev(f) FROM m", {}), ("q", "SELECT integral(i), integral(f) FROM m", {}),
                                ("q", "SELECT sum(i) / count(i) FROM m", {}), ("q", "SELECT sum(i) * 2 FROM m", {}), ("q", "SELECT mean(i) * 2 FROM m", {})])
case("agg_median_even", [("w", "m v=1 1\nm v=2 2\nm v=3 3\nm v=10 4", {}), ("q", "SELECT median(v) FROM m", {}), ("q", "SELECT percentile(v, 50) FROM m", {}), ("q", "SELECT mode(v) FROM m", {}),
                         ("q", "SELECT stddev(v) FROM m", {}), ("q", "SELECT spread(v) FROM m", {})])
case("agg_mode_ties", [("w", "m v=1 1\nm v=2 2\nm v=2 3\nm v=1 4\nm v=3 5", {}), ("q", "SELECT mode(v) FROM m", {})])
case("agg_stddev_single", [("w", "m v=1 1", {}), ("q", "SELECT stddev(v) FROM m", {}), ("q", "SELECT mean(v), median(v), spread(v), stddev(v) FROM m", {})])
case("agg_integral_units", S + [("q", "SELECT integral(usage) FROM cpu WHERE host = 'a'", {}), ("q", "SELECT integral(usage, 1s) FROM cpu WHERE host = 'a'", {}),
                          ("q", "SELECT integral(usage, 10s) FROM cpu WHERE host = 'a'", {}), ("q", "SELECT integral(usage, 1m) FROM cpu WHERE host = 'a'", {}),
                          ("q", "SELECT integral(usage) FROM cpu WHERE host = 'a' GROUP BY time(30s)", {}), ("q", "SELECT integral(usage) FROM cpu GROUP BY host", {}),
                          ("q", "SELECT integral(usage, 1x) FROM cpu", {}), ("q", "SELECT integral(n) FROM cpu WHERE host = 'a'", {})])
case("agg_mixed_agg_and_field", S + [("q", "SELECT count(usage), usage FROM cpu", {}), ("q", "SELECT mean(usage), host FROM cpu", {}), ("q", "SELECT usage, mean(usage) FROM cpu", {}),
                                     ("q", "SELECT count(usage) + usage FROM cpu", {})])
case("agg_arith", S + [("q", "SELECT mean(usage) * 2 FROM cpu", {}), ("q", "SELECT mean(usage) + mean(idle) FROM cpu", {}), ("q", "SELECT max(usage) - min(usage) FROM cpu", {}),
                       ("q", "SELECT sum(usage) / count(usage) FROM cpu", {}), ("q", "SELECT sum(usage) / 0 FROM cpu", {}), ("q", "SELECT (sum(usage) + 1) * 2 FROM cpu", {}),
                       ("q", "SELECT sum(n) / 4 FROM cpu", {}), ("q", "SELECT sum(n) % 4 FROM cpu", {}), ("q", "SELECT mean(usage) AS m, mean(idle) AS i FROM cpu GROUP BY host", {}),
                       ("q", "SELECT mean(usage) + 1 AS m FROM cpu", {}), ("q", "SELECT 2 * mean(usage) FROM cpu", {}), ("q", "SELECT -mean(usage) FROM cpu", {}),
                       ("q", "SELECT mean(usage) * mean(nosuch) FROM cpu", {}), ("q", "SELECT count(usage) * 1.5 FROM cpu", {})])
case("agg_nested", S + [("q", "SELECT max(mean(usage)) FROM cpu GROUP BY time(20s)", {}), ("q", "SELECT mean(sum(usage)) FROM cpu GROUP BY time(20s)", {}),
                        ("q", "SELECT max(mean(usage)) FROM cpu", {}), ("q", "SELECT sum(count(usage)) FROM cpu GROUP BY time(20s)", {}),
                        ("q", "SELECT derivative(mean(usage)) FROM cpu GROUP BY time(20s)", {}), ("q", "SELECT abs(mean(usage)) FROM cpu GROUP BY time(20s)", {}),
                        ("q", "SELECT round(mean(usage)) FROM cpu GROUP BY time(20s)", {})])

# ---------------------------------------------------------------- GROUP BY time
case("gbt_basic", S + [("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                       ("q", "SELECT mean(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(30s)", {}),
                       ("q", "SELECT mean(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(1m)", {}),
                       ("q", "SELECT mean(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(1h)", {}),
                       ("q", "SELECT mean(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(7s)", {}),
                       ("q", "SELECT mean(usage) FROM cpu WHERE time >= '2020-01-01T00:00:03Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                       ("q", "SELECT mean(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time <= '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                       ("q", "SELECT mean(usage) FROM cpu WHERE time > '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                       ("q", "SELECT count(usage) FROM cpu GROUP BY time(20s)", {}), ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' GROUP BY time(20s)", {}),
                       ("q", "SELECT count(usage) FROM cpu WHERE time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                       ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s), host", {}),
                       ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY host, time(20s)", {}),
                       ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s) LIMIT 2", {}),
                       ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s) LIMIT 2 OFFSET 1", {}),
                       ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s) ORDER BY time DESC", {})])
_R = "WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:02:00Z'"
case("gbt_fill", [("w", "m v=1 " + str(ts(5)) + "\nm v=3 " + str(ts(65)) + "\nm v=8 " + str(ts(85)), {})] +
     [("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(20s) fill({f})", {}) for f in ("null", "none", "previous", "linear", "0", "-1", "1.5", "99", "'x'", "foo", "")] +
     [("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(20s)", {}), ("q", f"SELECT count(v) FROM m {_R} GROUP BY time(20s)", {}),
      ("q", f"SELECT count(v) FROM m {_R} GROUP BY time(20s) fill(none)", {}), ("q", f"SELECT count(v) FROM m {_R} GROUP BY time(20s) fill(5)", {}),
      ("q", f"SELECT count(v) FROM m {_R} GROUP BY time(20s) fill(previous)", {}), ("q", f"SELECT sum(v) FROM m {_R} GROUP BY time(20s) fill(0)", {}),
      ("q", f"SELECT sum(v), count(v) FROM m {_R} GROUP BY time(20s) fill(0)", {}), ("q", f"SELECT max(v), min(v) FROM m {_R} GROUP BY time(20s) fill(linear)", {}),
      ("q", f"SELECT first(v), last(v) FROM m {_R} GROUP BY time(20s) fill(previous)", {}), ("q", f"SELECT median(v) FROM m {_R} GROUP BY time(20s) fill(0)", {}),
      ("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(20s) fill(previous) LIMIT 3", {}), ("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(20s) fill(linear) ORDER BY time DESC", {}),
      ("q", f"SELECT mean(v) FROM m WHERE time >= '2020-01-01T00:00:40Z' AND time < '2020-01-01T00:02:00Z' GROUP BY time(20s) fill(previous)", {}),
      ("q", f"SELECT mean(v) FROM m WHERE time >= '2020-01-01T00:00:40Z' AND time < '2020-01-01T00:02:00Z' GROUP BY time(20s) fill(linear)", {}),
      ("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(20s) fill(null) fill(0)", {}), ("q", f"SELECT mean(v) FROM m {_R} fill(0)", {}),
      ("q", f"SELECT v FROM m {_R} GROUP BY time(20s) fill(0)", {})])
case("gbt_fill_tags", [("w", "m,h=a v=1 " + str(ts(5)) + "\nm,h=b v=3 " + str(ts(65)), {}),
                       ("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(30s), h fill(0)", {}), ("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(30s), h", {}),
                       ("q", f"SELECT mean(v) FROM m {_R} GROUP BY time(30s), h fill(none)", {}), ("q", f"SELECT mean(v) FROM m {_R} GROUP BY h, time(30s) fill(previous)", {})])
case("gbt_offset", S + [("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s, 5s)", {}),
                        ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s, -5s)", {}),
                        ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s, 25s)", {}),
                        ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s, now())", {}),
                        ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s, 0s)", {}),
                        ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(1d, 12h)", {})], skip_compare=["now())"])
case("gbt_intervals", S + [("q", f"SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time({i})", {})
                           for i in ("1s", "10s", "1m", "1h", "1d", "1w", "500ms", "0s", "-1s", "1", "1x", "10", "1y", "5m", "24h", "7d")])
case("gbt_no_agg", S + [("q", "SELECT usage FROM cpu GROUP BY time(10s)", {}), ("q", "SELECT * FROM cpu GROUP BY time(10s)", {})])
case("gbt_no_time_range_bounds", S + [("q", "SELECT count(usage) FROM cpu GROUP BY time(1h)", {}), ("q", "SELECT count(usage) FROM cpu GROUP BY time(1d)", {}),
                                      ("q", "SELECT count(usage) FROM cpu WHERE host = 'a' GROUP BY time(30s)", {}),
                                      ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' GROUP BY time(1h)", {})])
case("gbt_now_upper_bound", S + [("q", "SELECT count(usage) FROM cpu WHERE time >= '2019-12-31T23:59:00Z' GROUP BY time(1h) fill(none)", {}),
                                 ("q", "SELECT count(usage) FROM cpu WHERE time >= now() - 1h GROUP BY time(1h)", {})], norm=["nowbuckets"])
case("gbt_aligned_epoch_odd", S + [("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:07Z' AND time < '2020-01-01T00:00:47Z' GROUP BY time(13s)", {}),
                                   ("q", "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:07Z' AND time < '2020-01-01T00:00:47Z' GROUP BY time(13s) fill(none)", {})])
case("gbt_selectors", S + [("q", "SELECT first(usage), last(usage) FROM cpu WHERE host = 'a' AND time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                           ("q", "SELECT max(usage), min(usage) FROM cpu WHERE host = 'a' AND time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                           ("q", "SELECT spread(usage), stddev(usage) FROM cpu WHERE host = 'a' AND time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {})])

# ---------------------------------------------------------------- transformations
TRANS = ["derivative", "non_negative_derivative", "difference", "non_negative_difference", "elapsed", "moving_average", "cumulative_sum"]
for t in TRANS:
    args = {"moving_average": ", 3"}.get(t, "")
    case(f"trans_{t}", S + [("q", f"SELECT {t}(usage{args}) FROM cpu WHERE host = 'a'", {}), ("q", f"SELECT {t}(usage{args}) FROM cpu GROUP BY host", {}),
                            ("q", f"SELECT {t}(n{args}) FROM cpu WHERE host = 'a'", {}), ("q", f"SELECT {t}(msg{args}) FROM cpu WHERE host = 'a'", {}),
                            ("q", f"SELECT {t}(usage{args}) FROM cpu", {}), ("q", f"SELECT {t}(mean(usage){args}) FROM cpu WHERE host = 'a' AND time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)", {}),
                            ("q", f"SELECT {t}(usage{args}) FROM cpu WHERE host = 'a' ORDER BY time DESC", {}), ("q", f"SELECT {t}(usage{args}) FROM cpu WHERE host = 'a' LIMIT 2", {}),
                            ("q", f"SELECT {t}(usage{args}) FROM cpu WHERE host = 'a' LIMIT 2 OFFSET 1", {}), ("q", f"SELECT {t}(usage{args}), idle FROM cpu WHERE host = 'a'", {}),
                            ("q", f"SELECT {t}(*{args}) FROM cpu WHERE host = 'a'", {}), ("q", f"SELECT {t}(nosuch{args}) FROM cpu", {}), ("q", f"SELECT {t}(usage{args}) AS x FROM cpu WHERE host = 'a'", {})])
case("trans_units", S + [("q", f"SELECT derivative(usage, {u}) FROM cpu WHERE host = 'a'", {}) for u in ("1s", "10s", "1m", "1ms", "1h", "1d", "1w", "1x", "5s")] +
     [("q", f"SELECT elapsed(usage, {u}) FROM cpu WHERE host = 'a'", {}) for u in ("1s", "1ms", "1m", "1u", "1ns", "1x")] +
     [("q", "SELECT non_negative_derivative(usage, 1s) FROM cpu WHERE host = 'a'", {}), ("q", "SELECT derivative(usage, 0s) FROM cpu", {}), ("q", "SELECT derivative(usage, 1) FROM cpu", {})])
case("trans_negative_values", [("w", "m v=5 1000000000\nm v=3 2000000000\nm v=9 3000000000\nm v=2 4000000000", {}), ("q", "SELECT derivative(v) FROM m", {}),
                               ("q", "SELECT non_negative_derivative(v) FROM m", {}), ("q", "SELECT difference(v) FROM m", {}), ("q", "SELECT non_negative_difference(v) FROM m", {}),
                               ("q", "SELECT cumulative_sum(v) FROM m", {}), ("q", "SELECT moving_average(v, 2) FROM m", {}), ("q", "SELECT elapsed(v) FROM m", {}),
                               ("q", "SELECT moving_average(v, 1) FROM m", {}), ("q", "SELECT moving_average(v, 5) FROM m", {}), ("q", "SELECT moving_average(v, 0) FROM m", {}),
                               ("q", "SELECT moving_average(v) FROM m", {})])
case("trans_gbt_fill", [("w", "m v=1 " + str(ts(5)) + "\nm v=3 " + str(ts(65)) + "\nm v=8 " + str(ts(85)), {}),
                        ("q", f"SELECT derivative(mean(v), 1s) FROM m {_R} GROUP BY time(20s)", {}), ("q", f"SELECT derivative(mean(v), 1s) FROM m {_R} GROUP BY time(20s) fill(0)", {}),
                        ("q", f"SELECT difference(mean(v)) FROM m {_R} GROUP BY time(20s) fill(previous)", {}), ("q", f"SELECT cumulative_sum(mean(v)) FROM m {_R} GROUP BY time(20s) fill(0)", {}),
                        ("q", f"SELECT moving_average(mean(v), 2) FROM m {_R} GROUP BY time(20s) fill(0)", {}), ("q", f"SELECT elapsed(mean(v), 1s) FROM m {_R} GROUP BY time(20s)", {})])
MATH1 = ["abs", "acos", "asin", "atan", "ceil", "cos", "cot", "exp", "floor", "ln", "log10", "log2", "round", "sin", "sqrt", "tan"]
case("math_funcs", [("w", "m v=-2.5,w=0.5,i=4i,z=0.0 1\nm v=2.5,w=0.25,i=-9i,z=1.0 2\nm v=0,w=1,i=0i,z=2.0 3", {})] +
    [("q", f"SELECT {f}(v), {f}(w), {f}(i) FROM m", {}) for f in MATH1] +
    [("q", "SELECT pow(v, 2), pow(w, 0.5), pow(i, 2) FROM m", {}), ("q", "SELECT log(w, 2), log(i, 2) FROM m", {}), ("q", "SELECT atan2(v, w) FROM m", {}),
     ("q", "SELECT abs(v), floor(w) FROM m", {}), ("q", "SELECT sqrt(v) FROM m", {}), ("q", "SELECT ln(z) FROM m", {}), ("q", "SELECT round(v) AS r FROM m", {}),
     ("q", "SELECT abs(*) FROM m", {}), ("q", "SELECT abs() FROM m", {}), ("q", "SELECT abs(v, 1) FROM m", {}), ("q", "SELECT pow(v) FROM m", {}), ("q", "SELECT abs(host) FROM m", {}),
     ("q", "SELECT sqrt(sum(v)) FROM m", {}), ("q", "SELECT abs(sum(v)) FROM m", {}), ("q", "SELECT sum(abs(v)) FROM m", {}), ("q", "SELECT sum(v * 2) FROM m", {}),
     ("q", "SELECT floor(v * 1.5) FROM m", {}), ("q", "SELECT pow(2, 3) FROM m", {})], accepted={"acos(": "Go vs Java libm differ in the last digit", "atan(": "Go vs Java libm differ in the last digit", "exp(": "Go vs Java libm differ in the last digit", "tan(": "Go vs Java libm differ in the last digit", "atan2(": "Go vs Java libm differ in the last digit"})

case("expr_arith", S + [("q", "SELECT usage + idle FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage - idle FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage * 2 FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT usage / 2 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT n / 2 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT n % 2 FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT n + 1 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT n * n FROM cpu WHERE host = 'a'", {}), ("q", "SELECT n / 0 FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT usage / 0 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT n % 0 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT (usage + idle) * 2 FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT usage + 1 AS x FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage + 1, usage FROM cpu WHERE host = 'a'", {}), ("q", "SELECT -usage FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT usage + msg FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage + ok FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage + host FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT 1 + usage FROM cpu WHERE host = 'a'", {}), ("q", "SELECT 2 * 3 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage + nosuch FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT n & 3, n | 8, n ^ 1 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage & 1 FROM cpu WHERE host = 'a'", {}), ("q", "SELECT usage + 1 * 2 FROM cpu", {}),
                        ("q", "SELECT usage FROM cpu WHERE host = 'a' AND usage + 1 > 12", {}), ("q", "SELECT 1.5 + n FROM cpu WHERE host = 'a'", {}), ("q", "SELECT n + 1.5 FROM cpu WHERE host = 'a'", {}),
                        ("q", "SELECT 9223372036854775807 + n FROM cpu WHERE host = 'a'", {})])
case("select_literals", S + [("q", "SELECT usage, 1 FROM cpu LIMIT 2", {}), ("q", "SELECT usage, 'x' FROM cpu LIMIT 2", {}), ("q", "SELECT 1, 2 FROM cpu LIMIT 2", {}),
                             ("q", "SELECT usage, true FROM cpu LIMIT 2", {})])

# ---------------------------------------------------------------- subqueries
case("subquery", S + [("q", "SELECT * FROM (SELECT usage FROM cpu)", {}), ("q", "SELECT max(usage) FROM (SELECT usage FROM cpu)", {}),
                      ("q", "SELECT max(m) FROM (SELECT mean(usage) AS m FROM cpu GROUP BY time(20s), host)", {}),
                      ("q", "SELECT max(m) FROM (SELECT mean(usage) AS m FROM cpu GROUP BY host)", {}),
                      ("q", "SELECT mean(m) FROM (SELECT max(usage) AS m FROM cpu GROUP BY host)", {}),
                      ("q", "SELECT count(m) FROM (SELECT mean(usage) AS m FROM cpu GROUP BY time(20s)) WHERE m > 20", {}),
                      ("q", "SELECT * FROM (SELECT mean(usage) FROM cpu GROUP BY host)", {}), ("q", "SELECT m FROM (SELECT mean(usage) AS m FROM cpu GROUP BY host) WHERE host = 'a'", {}),
                      ("q", "SELECT sum(usage) FROM (SELECT usage FROM cpu WHERE host = 'a') WHERE time > 0", {}), ("q", "SELECT * FROM (SELECT * FROM cpu) LIMIT 2", {}),
                      ("q", "SELECT max FROM (SELECT max(usage) FROM cpu GROUP BY host)", {}), ("q", "SELECT count(usage) FROM (SELECT usage FROM cpu), (SELECT idle FROM cpu)", {}),
                      ("q", "SELECT * FROM (SELECT usage FROM cpu) GROUP BY host", {}), ("q", "SELECT max(usage) FROM (SELECT usage FROM cpu GROUP BY host) GROUP BY host", {}),
                      ("q", "SELECT * FROM (SELECT count(usage) FROM cpu GROUP BY time(20s))", {}), ("q", "SELECT * FROM (SELECT nosuch FROM cpu)", {}),
                      ("q", "SELECT * FROM ()", {}), ("q", "SELECT usage FROM (SELECT usage, idle FROM cpu)", {}), ("q", "SELECT usage + idle FROM (SELECT usage, idle FROM cpu WHERE host = 'a')", {})])

# ---------------------------------------------------------------- output formats
case("fmt_epoch", S + [("q", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 2", {"epoch": e}) for e in ("ns", "u", "µ", "ms", "s", "m", "h", "x", "n", "us", "")] +
     [("q", "SELECT count(usage) FROM cpu WHERE host = 'a' GROUP BY time(20s)", {"epoch": "ms"}), ("q", "SHOW MEASUREMENTS", {"epoch": "s"}),
      ("q", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 2", {"epoch": "ms", "pretty": "true"})])
case("fmt_pretty", S + [("q", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 2", {"pretty": "true"}), ("q", "SHOW MEASUREMENTS", {"pretty": "true"}), ("q", "SELECT * FROM nomeas", {"pretty": "true"})], raw_text=True)
case("fmt_chunked", S + [("q", "SELECT usage FROM cpu", {"chunked": "true"}), ("q", "SELECT usage FROM cpu", {"chunked": "true", "chunk_size": "5"}),
                         ("q", "SELECT usage FROM cpu GROUP BY host", {"chunked": "true", "chunk_size": "4"}), ("q", "SELECT usage FROM cpu; SELECT idle FROM cpu", {"chunked": "true", "chunk_size": "10"}),
                         ("q", "SELECT usage FROM cpu", {"chunked": "true", "chunk_size": "1"}), ("q", "SELECT usage FROM cpu", {"chunked": "true", "chunk_size": "0"}),
                         ("q", "SELECT usage FROM cpu", {"chunk_size": "5"}), ("q", "SHOW MEASUREMENTS", {"chunked": "true"}), ("q", "SELECT * FROM nomeas", {"chunked": "true"}),
                         ("q", "SELECT usage FROM cpu", {"chunked": "false"}), ("q", "SELECT usage FROM cpu", {"chunked": "bogus"})], raw_text=True)
case("fmt_csv", S + [("qh", "SELECT usage, host FROM cpu WHERE host = 'a'", {}, {"Accept": "application/csv"}), ("qh", "SELECT count(usage) FROM cpu GROUP BY host", {}, {"Accept": "application/csv"}),
                     ("qh", "SHOW MEASUREMENTS", {}, {"Accept": "application/csv"}), ("qh", "SELECT * FROM nomeas", {}, {"Accept": "application/csv"}),
                     ("qh", "SELECT usage FROM cpu WHERE host = 'a'", {"epoch": "ms"}, {"Accept": "application/csv"}), ("qh", "SELECT usage FROM cpu; SELECT idle FROM cpu WHERE host = 'a'", {}, {"Accept": "application/csv"}),
                     ("qh", "select bad", {}, {"Accept": "application/csv"}), ("qh", "SELECT msg FROM cpu WHERE host = 'a' LIMIT 1", {}, {"Accept": "text/csv"}),
                     ("qh", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 1", {}, {"Accept": "application/x-msgpack"}), ("qh", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 1", {}, {"Accept": "*/*"}),
                     ("qh", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 1", {}, {"Accept": "application/json"}),
                     ("qh", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 1", {}, {"Accept-Encoding": "gzip"})], raw_text=True, accepted={"x-msgpack": "msgpack responses are not implemented (JSON is returned)"})

case("fmt_params_binding", S + [("q", "SELECT usage FROM cpu WHERE host = $h", {"params": '{"h": "a"}'}), ("q", "SELECT usage FROM cpu WHERE usage > $u", {"params": '{"u": 30}'}),
                                ("q", "SELECT usage FROM cpu WHERE host = $h AND usage > $u", {"params": '{"h": "c", "u": 30}'}), ("q", "SELECT usage FROM cpu WHERE host = $h", {"params": '{}'}),
                                ("q", "SELECT usage FROM cpu WHERE host = $h", {}), ("q", "SELECT usage FROM cpu WHERE host = $h", {"params": 'notjson'}),
                                ("q", "SELECT usage FROM cpu WHERE ok = $b", {"params": '{"b": true}'}), ("q", "SELECT usage FROM cpu WHERE host =~ $r", {"params": '{"r": "^a"}'}),
                                ("q", "SELECT usage FROM cpu WHERE time > $t", {"params": '{"t": "2020-01-01T00:00:30Z"}'}), ("q", "SELECT usage FROM cpu WHERE host = $h", {"params": '{"h": 5}'}),
                                ("q", "SELECT $x FROM cpu", {"params": '{"x": 1}'}), ("q", "SELECT usage FROM cpu WHERE host = $h LIMIT $l", {"params": '{"h": "a", "l": 2}'}),
                                ("q", "SELECT usage FROM cpu WHERE host = $h", {"params": '{"h": null}'}), ("q", "SELECT usage FROM cpu WHERE host = $h", {"params": '{"h": ["a"]}'})])
case("fmt_db_param", S + [("q", "SELECT count(usage) FROM cpu", {"db": ""}), ("q", "SELECT count(usage) FROM cpu", {"db": "nosuchdb_zz"}), ("q", "SELECT count(usage) FROM {db}..cpu", {"db": ""}),
                          ("q", "SELECT count(usage) FROM {db}.autogen.cpu", {"db": ""}), ("q", "SELECT count(usage) FROM nosuchdb_zz..cpu", {"db": ""}),
                          ("q", "SELECT count(usage) FROM {db}.nosuchrp.cpu", {}), ("q", "SHOW MEASUREMENTS", {"db": ""}), ("q", "SHOW MEASUREMENTS", {"db": "nosuchdb_zz"}),
                          ("q", "SHOW MEASUREMENTS ON {db}", {"db": ""}), ("q", "SHOW TAG KEYS", {"db": ""}), ("q", "SHOW FIELD KEYS", {"db": ""}), ("q", "SHOW SERIES", {"db": ""}),
                          ("q", "SHOW DATABASES", {"db": ""}), ("q", "SHOW DATABASES", {"db": "nosuchdb_zz"}), ("q", "SHOW RETENTION POLICIES", {"db": "nosuchdb_zz"})], norm=["dbs"])
case("fmt_rp_param", S + [("q", "SELECT count(usage) FROM cpu", {"rp": "autogen"}), ("q", "SELECT count(usage) FROM cpu", {"rp": "nosuchrp"})])
case("fmt_time_precision_rfc3339", [("w", "m v=1 1500000000123456789\nm v=2 1500000000123456000\nm v=3 1500000000123000000\nm v=4 1500000000000000000\nm v=5 1500000060000000000", {}),
                                    ("q", "SELECT v FROM m", {}), ("q", "SELECT v FROM m", {"epoch": "ns"}), ("q", "SELECT v FROM m", {"epoch": "u"}), ("q", "SELECT v FROM m", {"epoch": "ms"})])

# ---------------------------------------------------------------- errors: syntax
BAD = ["select", "select *", "select * from", "select * from m where", "select * from m where time", "select * from m group", "select * from m group by",
       "select * from m limit", "select * from m limit x", "select * from m limit 1.5", "select * from m offset", "select * from m order by time asc desc",
       "select * from m garbage", "select * from m;;", "select *,", "select , from m", "select * from m where (a = 1", "select * from m where a = ", "select * from m where a == 1",
       "select * from m where a = = 1", "select count( from m", "select count(v from m", "select count(v)) from m", "selec * from m", "SELECT * FORM m", "select * from 'm'",
       "select * from \"m", "select * from m where a = 'x", "select * from m where a =~ /x", "select * from m group by time()", "select * from m group by time(",
       "select mean(v) from m group by time(1)", "select mean(v) from m group by time(1s) fill", "select mean(v) from m group by time(1s) fill(", "select mean(v) from m group by time(1s) fill()",
       "select v from m where time > now(", "select v from m where time > now", "select v from m where time > now() -", "select v from m where a = 1 and", "select v from m where a = 1 or",
       "select v from m where a in (1)", "select v from m where a like 'x'", "select v from m where a between 1 and 2", "select v from m where a is null", "select v as from m",
       "select v as 1 from m", "select v as \"a b\" from m", "select v from m limit -1 offset -1", "select v from m slimit", "select v from m soffset 1", "select 'a' from m",
       "select v from m where 'a'", "select 1.5.5 from m", "select v from m where a = 1.5.5", "select v::foo from m", "select v:: from m", "select (v from m", "select v) from m",
       "select v from m order by time desc,", "select v from m order by v", "select * from m into n", "select * into from m", "select * into n", "insert into m values (1)",
       "update m set v = 1", "create table t (a int)", "drop table t", "show tables", "show measurements from m", "show tag keys where", "show tag values", "show field keys from",
       "show series from", "create database", "create database a b", "drop database", "drop measurement", "drop series", "delete", "delete from", "delete from m where",
       "explain select * from m", "explain analyze select * from m", "", " ", "-- comment", "select * from m -- comment", "/* c */ select * from m", "SELECT * FROM m\n\n WHERE\nv >", "select\t*\tfrom\tm"]
case("syntax_errors", [DATA] + [("q", b, {}) for b in BAD] + [("q", b, {}, "POST") for b in BAD[:30]], accepted={"explain analyze": "EXPLAIN ANALYZE timings/plan text are engine-specific"})

case("syntax_errors_more", [DATA] + [("q", b, {}) for b in [
    "SELECT usage FROM cpu WHERE host = 'a' AND", "SELECT usage FROM cpu WHERE host = 'a' GROUP BY time(1s) fill(foo)", "SELECT usage, FROM cpu",
    "SELECT usage FROM cpu WHERE (host = 'a'", "SELECT usage FROM cpu GROUP time(1s)", "SELECT * FROM cpu WHERE usage > > 1", "SELECT mean() FROM cpu",
    "SELECT mean(usage, 1) FROM cpu", "SELECT nosuchfunc(usage) FROM cpu", "SELECT max() FROM cpu", "SELECT count(usage) FROM cpu GROUP BY time(1h) fill(0) LIMIT",
    "SELECT usage FROM cpu LIMIT 1 LIMIT 2", "SELECT usage FROM cpu WHERE host = 'a' WHERE usage > 1", "SELECT usage FROM cpu FROM cpu", "SELECT usage FROM WHERE",
    "SELECT usage FROM cpu WHERE usage >", "SELECT usage FROM cpu WHERE time > '2020' AND", "SELECT DISTINCT FROM cpu", "SELECT DISTINCT(usage, idle) FROM cpu",
    "SELECT count(DISTINCT usage) FROM cpu", "SELECT count(distinct(usage)) FROM cpu", "SELECT * FROM cpu WHERE host = 'a' GROUP BY", "SELECT 1 +", "SELECT + 1",
    "SELECT usage FROM cpu ORDER BY time DESC ASC", "SELECT usage FROM cpu GROUP BY time(1s), time(2s)", "SELECT usage FROM cpu GROUP BY host, host",
    "SELECT sum(usage) FROM cpu GROUP BY time(1s, 2s, 3s)", "SELECT sum(usage) FROM cpu GROUP BY time(1s) fill(0) fill(1)"]])
case("semantic_errors", S + [("q", b, {}) for b in [
    "SELECT usage, mean(usage) FROM cpu", "SELECT mean(usage), derivative(usage) FROM cpu", "SELECT max(usage), top(usage, 2) FROM cpu", "SELECT count(usage) FROM cpu GROUP BY time(0s)",
    "SELECT usage FROM cpu GROUP BY time(1s)", "SELECT mean(usage) FROM cpu GROUP BY time(1s) fill(0) LIMIT 1 SLIMIT 1", "SELECT mean(usage) FROM cpu WHERE time > now() GROUP BY time(1m) LIMIT 1",
    "SELECT * FROM cpu WHERE host", "SELECT * FROM cpu WHERE usage", "SELECT count(*), usage FROM cpu", "SELECT first(usage), mean(usage) FROM cpu", "SELECT derivative(usage), mean(usage) FROM cpu",
    "SELECT mean(usage) FROM cpu GROUP BY time(1s) FILL(0)", "SELECT MEAN(usage) FROM cpu", "SELECT Mean(usage) FROM cpu", "SELECT median(usage), percentile(usage, 90) FROM cpu",
    "SELECT usage FROM cpu GROUP BY time(1s) fill(previous)", "SELECT mean(usage) FROM cpu GROUP BY time(1s), host, region LIMIT 1", "SELECT mean(usage) + usage FROM cpu",
    "SELECT sample(usage, 2) FROM cpu", "SELECT sample(usage) FROM cpu", "SELECT sample(usage, 2) FROM cpu GROUP BY host", "SELECT holt_winters(mean(usage), 3, 1) FROM cpu GROUP BY time(10s)",
    "SELECT count(usage) FROM cpu WHERE time > now() - 1h GROUP BY time(1m)", "SELECT mean(usage) FROM cpu GROUP BY time(10s) fill(0) ORDER BY time DESC"]] , skip_compare=["SELECT sample(usage, 2) FROM cpu", "SELECT sample(usage, 2) FROM cpu GROUP BY host"], norm=["nowbuckets"], accepted={"holt_winters": "HOLT_WINTERS (Nelder-Mead fit) is not implemented: Warp returns a clear error"})

case("string_funcs_and_misc", S + [("q", b, {}) for b in [
    "SELECT length(msg) FROM cpu", "SELECT upper(msg) FROM cpu", "SELECT msg FROM cpu WHERE msg =~ /a[0-2]/", "SELECT usage FROM cpu WHERE msg = 'ma3'", "SELECT count(msg) FROM cpu WHERE msg != 'ma3'",
    "SELECT * FROM cpu WHERE host = 'a' AND time = '2020-01-01T00:00:20Z'", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 1 OFFSET 5", "SELECT usage FROM cpu WHERE host = 'a' LIMIT 1 OFFSET 6"]])
case("quoting_identifiers", [("w", 'my\\ meas,my\\ tag=v\\ 1 my\\ field=1i 1', {}), ("q", 'SELECT "my field" FROM "my meas"', {}), ("q", 'SELECT * FROM "my meas" WHERE "my tag" = \'v 1\'', {}),
                             ("q", 'SELECT "my field" AS "out name" FROM "my meas"', {}), ("q", 'SELECT * FROM my meas', {}), ("q", 'SELECT "my field" FROM "my meas" GROUP BY "my tag"', {}),
                             ("q", 'SHOW TAG VALUES FROM "my meas" WITH KEY = "my tag"', {}), ("q", 'SELECT "my\\"f" FROM "my meas"', {}), ("q", "SELECT 'my field' FROM \"my meas\"", {}),
                             ("q", 'SELECT * FROM "my meas" WHERE "my field" > 0', {})])
case("keyword_idents", [("w", "select,from=v where=1i,limit=2i 1", {}), ("q", 'SELECT "where", "limit" FROM "select"', {}), ("q", "SELECT where FROM select", {}), ("q", 'SELECT * FROM "select" WHERE "from" = \'v\'', {}),
                        ("q", 'SELECT "where" FROM "select" WHERE "limit" > 1', {})])
case("cross_series_fields", [("w", "m,h=a v=1i,w=1.5 1\nm,h=b v=2i,x=\"s\" 2\nm,h=c w=3.5,y=true 3", {}), ("q", "SELECT * FROM m", {}), ("q", "SELECT * FROM m GROUP BY *", {}), ("q", "SELECT v, w, x, y FROM m", {}),
                             ("q", "SELECT sum(v), sum(w) FROM m", {}), ("q", "SELECT sum(v), sum(w) FROM m GROUP BY h", {}), ("q", "SELECT count(*) FROM m", {}), ("q", "SELECT count(*) FROM m GROUP BY h", {})])
case("many_points", [("w", "\n".join(f"big,k={i % 7} v={i}i,f={i * 0.25} {ts(i)}" for i in range(500)), {}),
                     ("q", "SELECT count(v), sum(v), mean(f), min(v), max(v), median(v), stddev(f), spread(v) FROM big", {}), ("q", "SELECT count(v) FROM big GROUP BY k", {}),
                     ("q", "SELECT sum(v) FROM big WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:08:20Z' GROUP BY time(1m), k", {}),
                     ("q", "SELECT mean(f) FROM big WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:08:20Z' GROUP BY time(90s) fill(previous)", {}),
                     ("q", "SELECT * FROM big WHERE k = '3' LIMIT 3 OFFSET 2", {}), ("q", "SELECT percentile(v, 95), mode(v) FROM big", {}), ("q", "SELECT derivative(mean(v), 1s) FROM big WHERE time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:08:20Z' GROUP BY time(1m)", {}),
                     ("q", "SELECT top(v, 3), k FROM big", {}), ("q", "SELECT distinct(k) FROM big", {}), ("q", "SELECT last(v) FROM big GROUP BY k", {})])
case("float_precision", [("w", "m a=0.1,b=0.2,c=0.30000000000000004,d=1e-7,e=123456789012345678 1", {}), ("q", "SELECT a + b, c, d, e FROM m", {}), ("q", "SELECT sum(a) + sum(b) FROM m", {}), ("q", "SELECT mean(a), stddev(a) FROM m", {})])
case("null_and_nan_out", [("w", "m v=1 1\nm v=2 2", {}), ("q", "SELECT v / 0 FROM m", {}), ("q", "SELECT sqrt(-1 * v) FROM m", {}), ("q", "SELECT ln(0 * v) FROM m", {}), ("q", "SELECT stddev(v) FROM m WHERE v > 5", {}),
                          ("q", "SELECT mean(v) FROM m WHERE v > 5", {}), ("q", "SELECT count(v) FROM m WHERE v > 5", {}), ("q", "SELECT sum(v) FROM m WHERE v > 5", {}), ("q", "SELECT max(v) FROM m WHERE v > 5", {}),
                          ("q", "SELECT count(v) FROM m WHERE v > 5 GROUP BY time(1s)", {}), ("q", "SELECT count(v) FROM m WHERE time >= 0 AND time < 3 AND v > 5 GROUP BY time(1ns)", {}),
                          ("q", "SELECT count(v) FROM m WHERE time >= 0 AND time < 3 AND v > 5 GROUP BY time(1ns) fill(none)", {})])
case("sql_injection_like", [DATA, ("q", "SELECT usage FROM cpu WHERE host = 'a''; DROP TABLE x; --'", {}), ("q", "SELECT usage FROM cpu WHERE host = 'a\\'; DROP TABLE x; --'", {}),
                            ("q", "SELECT \"usage\"; DROP TABLE x FROM cpu", {}), ("q", "SELECT usage FROM \"cpu; DROP TABLE x\"", {}), ("q", "SELECT count(usage) FROM cpu", {})])


# ---- safety: real InfluxDB materialises every bucket between the first point and now() when a GROUP BY time()
# query has no lower time bound (millions of rows for 2020 data; the oracle container OOMs). Bound every valid
# GROUP BY time(<n><unit>) query that has no explicit lower bound to a 2-minute window.
import re as _re
_BOUND = "time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:02:00Z'"


def _bound(q):
    if not _re.search(r"group by time\(\d+[a-z]+", q, _re.I) or "now()" in q.lower():
        return q
    lo = _re.search(r"time\s*>=?\s*['0-9n]", q, _re.I)
    hi = _re.search(r"time\s*<=?\s*['0-9n]", q, _re.I)
    add = []
    if not lo:
        add.append("time >= '2020-01-01T00:00:00Z'")
    if not hi:
        add.append("time < '2020-01-01T00:02:00Z'")
    if not add:
        return q
    cond = " AND ".join(add)
    m = _re.search(r"\bwhere\b", q, _re.I)
    if m:
        return q[:m.end()] + " " + cond + " AND " + q[m.end():].lstrip()
    m = _re.search(r"\bgroup by time", q, _re.I)
    return q[:m.start()] + "WHERE " + cond + " " + q[m.start():]


for _n, (_steps, _o) in list(CASES.items()):
    if _n.startswith("syntax_errors"):
        continue
    CASES[_n] = ([(_s[0], _bound(_s[1])) + tuple(_s[2:]) if _s[0] in ("q", "qh") else _s for _s in _steps], _o)


# Divergences that are accepted for every case (step substring -> reason); see README.md "Classification".
ACCEPT_GLOBAL = {
    "shard": "deletion errors carry an InfluxDB-internal shard id",
    "DELETE FROM cpu WHERE usage > 1": "deletion errors carry an InfluxDB-internal shard id",
    "integral(usage) FROM cpu GROUP BY host": "the series order of integral() is not deterministic in InfluxDB itself",
    "integral(usage) FROM cpu WHERE time >= 1577836800000000000 AND time < 1577836860000000000 GROUP BY time(20s), host":
        "integral() with GROUP BY time+tag ending exactly on a bucket boundary drops the last window in InfluxDB",
    "notjson": "the text of Go's JSON decoder error for a malformed params value",
    "select v:: from m": "rare parse-error position/list variant for a dangling ::",
    "order by time desc,": "InfluxQL reports a different column for a trailing comma in ORDER BY",
}
