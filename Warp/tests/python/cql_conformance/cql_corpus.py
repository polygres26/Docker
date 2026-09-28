"""The differential corpus of cqlwire: every case runs against a real Cassandra (recorded once into golden.json.gz) and against Warp.
Each case gets its own keyspace {ks}; steps are CQL strings (simple statements) or P (prepared) / Q (options) / B (batch) / W (wait)."""
import datetime
import decimal
import ipaddress
import uuid

from cassandra.util import Date, Duration, Time

from cql_harness import B, Case, M, P, Q, U, W

T = "{ks}."
U1 = uuid.UUID("11111111-1111-1111-1111-111111111111")
U2 = uuid.UUID("22222222-2222-2222-2222-222222222222")
TU1 = uuid.UUID("d2177dd0-eaa2-11de-a572-001b779c76e3")  # a version 1 uuid


def ddl_cases():
    c = []
    c.append(Case("ddl_keyspace", [
        "DROP KEYSPACE IF EXISTS k_ddl_keyspace_a",
        "DROP KEYSPACE IF EXISTS k_ddl_keyspace_b",
        "CREATE KEYSPACE k_ddl_keyspace_a WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2} AND durable_writes = false",
        "CREATE KEYSPACE k_ddl_keyspace_a WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "CREATE KEYSPACE IF NOT EXISTS k_ddl_keyspace_a WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = 'k_ddl_keyspace_a'",
        "ALTER KEYSPACE k_ddl_keyspace_a WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
        "SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = 'k_ddl_keyspace_a'",
        "ALTER KEYSPACE k_ddl_keyspace_nope WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "CREATE KEYSPACE k_ddl_keyspace_b WITH replication = {'class': 'SimpleStrategy'}",
        "CREATE KEYSPACE k_ddl_keyspace_b WITH replication = {'class': 'Bogus', 'replication_factor': 1}",
        "CREATE KEYSPACE k_ddl_keyspace_b",
        "CREATE KEYSPACE \"bad-name\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "DROP KEYSPACE k_ddl_keyspace_a",
        "DROP KEYSPACE k_ddl_keyspace_a",
        "DROP KEYSPACE IF EXISTS k_ddl_keyspace_a",
        "USE k_ddl_keyspace_nope",
        "USE " + "{ks}",
        "CREATE TABLE tt (a int PRIMARY KEY)",
        "DROP KEYSPACE system",
        "CREATE TABLE system.tt (a int PRIMARY KEY)",
        "SELECT keyspace_name, replication FROM system_schema.keyspaces WHERE keyspace_name = 'k_ddl_keyspace_b'",
        "DROP KEYSPACE IF EXISTS k_ddl_keyspace_b",
    ]))
    c.append(Case("ddl_table", [
        "CREATE TABLE {ks}.t1 (a int, b text, c bigint, PRIMARY KEY (a))",
        "CREATE TABLE {ks}.t1 (a int, b text, c bigint, PRIMARY KEY (a))",
        "CREATE TABLE IF NOT EXISTS {ks}.t1 (a int, b text, c bigint, PRIMARY KEY (a))",
        "CREATE TABLE {ks}.t2 (a int PRIMARY KEY, b text)",
        "CREATE TABLE {ks}.t3 (a int, b int, c int, d int, PRIMARY KEY ((a, b), c)) WITH CLUSTERING ORDER BY (c DESC)",
        "CREATE TABLE {ks}.t4 (a int, b int, c int, d int, s int static, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b ASC, c DESC) AND comment = 'hello' AND default_time_to_live = 100",
        "SELECT table_name, comment, default_time_to_live, gc_grace_seconds, flags FROM system_schema.tables WHERE keyspace_name = '{ks}'",
        "SELECT table_name, column_name, clustering_order, kind, position, type FROM system_schema.columns WHERE keyspace_name = '{ks}'",
        "SELECT table_name, column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = 't4'",
        "CREATE TABLE {ks}.bad1 (a int, b int)",
        "CREATE TABLE {ks}.bad2 (a int PRIMARY KEY, a text)",
        "CREATE TABLE {ks}.bad3 (a int, PRIMARY KEY (b))",
        "CREATE TABLE {ks}.bad4 (a int, b int, s int static, PRIMARY KEY (a))",
        "CREATE TABLE {ks}.bad5 (a list<int> PRIMARY KEY)",
        "CREATE TABLE {ks}.bad6 (a int PRIMARY KEY, b list<list<int>>)",
        "CREATE TABLE {ks}.bad7 (a int, b int, PRIMARY KEY (a, b, b))",
        "CREATE TABLE {ks}.bad8 (a duration PRIMARY KEY)",
        "CREATE TABLE {ks}.bad9 (a counter PRIMARY KEY)",
        "CREATE TABLE {ks}.bad10 (a int PRIMARY KEY, c counter)",
        "CREATE TABLE {ks}.bad11 (a int, b int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (c ASC)",
        "CREATE TABLE {ks}.bad12 (a int PRIMARY KEY) WITH nonsense = 1",
        "CREATE TABLE {ks}.bad13 (a int PRIMARY KEY, primary int)",
        "CREATE TABLE nokeyspace.t (a int PRIMARY KEY)",
        "ALTER TABLE {ks}.t1 ADD d text",
        "ALTER TABLE {ks}.t1 ADD d text",
        "ALTER TABLE {ks}.t1 ADD (e int, f list<text>)",
        "ALTER TABLE {ks}.t1 DROP c",
        "ALTER TABLE {ks}.t1 DROP a",
        "ALTER TABLE {ks}.t1 DROP zz",
        "ALTER TABLE {ks}.t1 RENAME a TO aa",
        "ALTER TABLE {ks}.t1 RENAME b TO bb",
        "ALTER TABLE {ks}.t1 WITH comment = 'changed'",
        "ALTER TABLE {ks}.t1 ALTER b TYPE blob",
        "ALTER TABLE {ks}.nope ADD x int",
        "SELECT column_name, kind, type FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = 't1'",
        "SELECT comment FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't1'",
        "INSERT INTO {ks}.t1 (a, b, d) VALUES (1, 'x', 'y')",
        "SELECT * FROM {ks}.t1",
        "ALTER TABLE {ks}.t1 DROP d",
        "SELECT * FROM {ks}.t1",
        "ALTER TABLE {ks}.t1 ADD d int",
        "SELECT * FROM {ks}.t1",
        "TRUNCATE {ks}.t1",
        "SELECT * FROM {ks}.t1",
        "TRUNCATE {ks}.nope",
        "DROP TABLE {ks}.t1",
        "DROP TABLE {ks}.t1",
        "DROP TABLE IF EXISTS {ks}.t1",
        "SELECT * FROM {ks}.t1",
    ]))
    c.append(Case("ddl_types", [
        "CREATE TYPE {ks}.addr (street text, city text, zip int)",
        "CREATE TYPE {ks}.addr (street text)",
        "CREATE TYPE IF NOT EXISTS {ks}.addr (street text)",
        "CREATE TYPE {ks}.dup (a int, a text)",
        "CREATE TYPE {ks}.person (name text, home frozen<addr>, tags set<text>)",
        "CREATE TYPE {ks}.person (name text, home frozen<addr>, tags frozen<set<text>>)",
        "SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = '{ks}'",
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, p frozen<person>, l list<frozen<addr>>)",
        "ALTER TYPE {ks}.addr ADD country text",
        "ALTER TYPE {ks}.addr ADD country text",
        "ALTER TYPE {ks}.addr RENAME city TO town",
        "SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = '{ks}' AND type_name = 'addr'",
        "DROP TYPE {ks}.addr",
        "DROP TYPE {ks}.nope",
        "DROP TYPE IF EXISTS {ks}.nope",
        "DROP TABLE {ks}.t",
        "DROP TYPE {ks}.person",
        "DROP TYPE {ks}.addr",
        "SELECT type_name FROM system_schema.types WHERE keyspace_name = '{ks}'",
    ]))
    c.append(Case("ddl_index", [
        "CREATE TABLE {ks}.t (k int, c int, v int, w text, m map<text, int>, l list<int>, s set<text>, PRIMARY KEY (k, c))",
        "CREATE INDEX ON {ks}.t (v)",
        "CREATE INDEX byw ON {ks}.t (w)",
        "CREATE INDEX byw ON {ks}.t (w)",
        "CREATE INDEX IF NOT EXISTS byw ON {ks}.t (w)",
        "CREATE INDEX bad1 ON {ks}.t (nope)",
        "CREATE INDEX bad2 ON {ks}.t (m)",
        "CREATE INDEX mk ON {ks}.t (keys(m))",
        "CREATE INDEX ml ON {ks}.t (l)",
        "CREATE INDEX ms ON {ks}.t (s)",
        "CREATE INDEX mc ON {ks}.t (k)",
        "SELECT index_name, kind, options FROM system_schema.indexes WHERE keyspace_name = '{ks}'",
        "INSERT INTO {ks}.t (k, c, v, w) VALUES (1, 1, 10, 'a')",
        "INSERT INTO {ks}.t (k, c, v, w) VALUES (1, 2, 20, 'b')",
        "INSERT INTO {ks}.t (k, c, v, w) VALUES (2, 1, 10, 'b')",
        "SELECT k, c FROM {ks}.t WHERE v = 10",
        "SELECT k, c FROM {ks}.t WHERE w = 'b'",
        "SELECT k, c FROM {ks}.t WHERE w = 'b' AND v = 20",
        "SELECT k, c FROM {ks}.t WHERE v = 10 AND k = 1",
        "DROP INDEX byw",
        "DROP INDEX byw",
        "DROP INDEX IF EXISTS byw",
        "SELECT k, c FROM {ks}.t WHERE w = 'b'",
        "SELECT k, c FROM {ks}.t WHERE w = 'b' ALLOW FILTERING",
    ]))
    return c


def type_cases():
    c = []
    cols = [("a_ascii", "ascii", "'abc'"), ("a_bigint", "bigint", "9223372036854775807"), ("a_blob", "blob", "0xCAFE"),
            ("a_bool", "boolean", "true"), ("a_date", "date", "'2020-02-29'"), ("a_decimal", "decimal", "3.14159265358979323846"),
            ("a_double", "double", "2.718281828459045"), ("a_float", "float", "1.5"), ("a_inet", "inet", "'192.168.1.10'"),
            ("a_int", "int", "-2147483648"), ("a_small", "smallint", "32767"), ("a_text", "text", "'héllo 世界'"),
            ("a_time", "time", "'13:45:30.123456789'"), ("a_ts", "timestamp", "'2021-03-04 05:06:07.089+0000'"),
            ("a_tuuid", "timeuuid", "d2177dd0-eaa2-11de-a572-001b779c76e3"), ("a_tiny", "tinyint", "-128"),
            ("a_uuid", "uuid", "11111111-1111-1111-1111-111111111111"), ("a_varchar", "varchar", "'vc'"),
            ("a_varint", "varint", "123456789012345678901234567890"), ("a_dur", "duration", "1y2mo3d4h5m6s7ms")]
    ddl = "CREATE TABLE {ks}.t (k int PRIMARY KEY, " + ", ".join(f"{n} {t}" for n, t, _ in cols) + ")"
    ins = "INSERT INTO {ks}.t (k, " + ", ".join(n for n, _, _ in cols) + ") VALUES (1, " + ", ".join(v for _, _, v in cols) + ")"
    steps = [ddl, ins, "SELECT * FROM {ks}.t", "SELECT k, a_date, a_time, a_ts, a_dur FROM {ks}.t WHERE k = 1"]
    steps += [f"SELECT {n} FROM {{ks}}.t WHERE k = 1" for n, _, _ in cols]
    steps += ["INSERT INTO {ks}.t (k) VALUES (2)", "SELECT * FROM {ks}.t WHERE k = 2",
              "SELECT k, a_int, a_text FROM {ks}.t"]
    c.append(Case("types_native", steps))

    c.append(Case("types_prepared", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, a_ascii ascii, a_bigint bigint, a_blob blob, a_bool boolean, a_date date, a_decimal decimal, "
        "a_double double, a_float float, a_inet inet, a_int int, a_small smallint, a_text text, a_time time, a_ts timestamp, a_tuuid timeuuid, "
        "a_tiny tinyint, a_uuid uuid, a_varint varint, a_dur duration)",
        P("INSERT INTO {ks}.t (k, a_ascii, a_bigint, a_blob, a_bool, a_date, a_decimal, a_double, a_float, a_inet, a_int, a_small, a_text, a_time, "
          "a_ts, a_tuuid, a_tiny, a_uuid, a_varint, a_dur) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
          [1, "ascii", -9223372036854775808, b"\x00\x01\xff", False, Date(datetime.date(1969, 7, 20)), decimal.Decimal("-0.000123456789"),
           -0.0, 3.5, ipaddress.ip_address("::1"), 2147483647, -32768, "", Time(12345678901234), datetime.datetime(1969, 12, 31, 23, 59, 59, 999000),
           TU1, 127, U2, -98765432109876543210, Duration(1, 2, 3)]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [1]),
        P("INSERT INTO {ks}.t (k, a_int, a_text, a_blob) VALUES (?, ?, ?, ?)", [2, None, None, None]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [2]),
        P("INSERT INTO {ks}.t (k, a_int) VALUES (?, ?)", ["notanint", 1]),
        P("SELECT a_int, a_text FROM {ks}.t WHERE k IN ?", [[1, 2, 3]]),
        P("SELECT a_int, a_text FROM {ks}.t WHERE k IN (?, ?)", [1, 2]),
    ]))

    c.append(Case("types_edge", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, i int, b bigint, d double, f float, dec decimal, v varint, s text, bl blob, ts timestamp, dt date, tm time)",
        "INSERT INTO {ks}.t (k, i, b) VALUES (1, 2147483648, 1)",
        "INSERT INTO {ks}.t (k, i, b) VALUES (2, -2147483649, 1)",
        "INSERT INTO {ks}.t (k, i) VALUES (3, 1.5)",
        "INSERT INTO {ks}.t (k, i) VALUES (4, 'x')",
        "INSERT INTO {ks}.t (k, s) VALUES (5, 12)",
        "INSERT INTO {ks}.t (k, s) VALUES (6, '')",
        "INSERT INTO {ks}.t (k, bl) VALUES (7, 0x)",
        "INSERT INTO {ks}.t (k, bl) VALUES (8, 0xZZ)",
        "INSERT INTO {ks}.t (k, d, f) VALUES (9, NaN, Infinity)",
        "INSERT INTO {ks}.t (k, d, f) VALUES (10, -Infinity, 1e10)",
        "INSERT INTO {ks}.t (k, d, f) VALUES (11, 1e308, 3.4e38)",
        "INSERT INTO {ks}.t (k, d, f) VALUES (12, 1e400, 1)",
        "INSERT INTO {ks}.t (k, dec, v) VALUES (13, 1e-30, 99999999999999999999999999999999)",
        "INSERT INTO {ks}.t (k, dec, v) VALUES (14, -123.4500, -1)",
        "INSERT INTO {ks}.t (k, ts) VALUES (15, '2020-13-01')",
        "INSERT INTO {ks}.t (k, ts) VALUES (16, 1600000000000)",
        "INSERT INTO {ks}.t (k, ts) VALUES (17, '2020-01-01')",
        "INSERT INTO {ks}.t (k, ts) VALUES (18, '2020-01-01T10:20:30Z')",
        "INSERT INTO {ks}.t (k, ts) VALUES (19, '2020-01-01 10:20:30.5+0200')",
        "INSERT INTO {ks}.t (k, ts) VALUES (20, '2020-01-01 10:20')",
        "INSERT INTO {ks}.t (k, dt) VALUES (21, '1900-01-01')",
        "INSERT INTO {ks}.t (k, dt) VALUES (22, '2020-02-30')",
        "INSERT INTO {ks}.t (k, tm) VALUES (23, '23:59:59.999999999')",
        "INSERT INTO {ks}.t (k, tm) VALUES (24, '24:00:00')",
        "INSERT INTO {ks}.t (k, dt) VALUES (25, 0)",
        "INSERT INTO {ks}.t (k, tm) VALUES (26, 86399999999999)",
        "SELECT * FROM {ks}.t",
        "SELECT k, i, b FROM {ks}.t WHERE i = 2147483647 ALLOW FILTERING",
        "SELECT k, d FROM {ks}.t WHERE d > 0 ALLOW FILTERING",
        "SELECT k, ts FROM {ks}.t WHERE ts > '2020-01-01' ALLOW FILTERING",
        "SELECT k, ts FROM {ks}.t WHERE ts = '2020-01-01 10:20:30+0000' ALLOW FILTERING",
    ]))

    c.append(Case("types_collections", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, l list<int>, s set<text>, m map<text, int>, fl frozen<list<int>>, fm frozen<map<int, text>>, "
        "tp tuple<int, text, boolean>, nested map<text, frozen<list<int>>>, ls set<frozen<set<int>>>)",
        "INSERT INTO {ks}.t (k, l, s, m, fl, fm, tp, nested, ls) VALUES (1, [3,1,2,1], {'b','a','c','a'}, {'x': 1, 'a': 2}, [5,4], {2: 'two', 1: 'one'}, "
        "(1, 'a', true), {'k': [1,2], 'a': []}, {{3,1}, {2}})",
        "SELECT * FROM {ks}.t WHERE k = 1",
        "INSERT INTO {ks}.t (k, l, s, m) VALUES (2, [], {}, {})",
        "SELECT * FROM {ks}.t WHERE k = 2",
        "INSERT INTO {ks}.t (k, l, s, m) VALUES (3, null, null, null)",
        "SELECT * FROM {ks}.t WHERE k = 3",
        "INSERT INTO {ks}.t (k, l) VALUES (4, [1, null])",
        "INSERT INTO {ks}.t (k, s) VALUES (5, {'a', null})",
        "INSERT INTO {ks}.t (k, m) VALUES (6, {'a': null})",
        "INSERT INTO {ks}.t (k, tp) VALUES (7, (1, null, null))",
        "INSERT INTO {ks}.t (k, tp) VALUES (8, (1, 'a', true, 5))",
        "INSERT INTO {ks}.t (k, tp) VALUES (9, (1, 'a'))",
        "SELECT k, tp FROM {ks}.t WHERE k IN (7, 9)",
        "INSERT INTO {ks}.t (k, l) VALUES (10, {1, 2})",
        "INSERT INTO {ks}.t (k, s) VALUES (11, [1, 2])",
        "INSERT INTO {ks}.t (k, m) VALUES (12, {1: 2})",
        "UPDATE {ks}.t SET l = l + [9, 8] WHERE k = 1",
        "UPDATE {ks}.t SET l = [0] + l WHERE k = 1",
        "UPDATE {ks}.t SET l[1] = 100 WHERE k = 1",
        "UPDATE {ks}.t SET l[99] = 100 WHERE k = 1",
        "UPDATE {ks}.t SET l = l - [1, 100] WHERE k = 1",
        "SELECT l FROM {ks}.t WHERE k = 1",
        "DELETE l[0] FROM {ks}.t WHERE k = 1",
        "SELECT l FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET s = s + {'z', 'a'} WHERE k = 1",
        "UPDATE {ks}.t SET s = s - {'a', 'nope'} WHERE k = 1",
        "SELECT s FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET m = m + {'y': 7, 'x': 70} WHERE k = 1",
        "UPDATE {ks}.t SET m['q'] = 5 WHERE k = 1",
        "UPDATE {ks}.t SET m = m - {'a'} WHERE k = 1",
        "DELETE m['x'] FROM {ks}.t WHERE k = 1",
        "SELECT m, m['y'] FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET fl = fl + [1] WHERE k = 1",
        "UPDATE {ks}.t SET fl = [7] WHERE k = 1",
        "SELECT fl FROM {ks}.t WHERE k = 1",
        "DELETE s FROM {ks}.t WHERE k = 1",
        "SELECT s, l FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET s = {'r'}, l = [1] WHERE k = 1",
        "SELECT s, l FROM {ks}.t WHERE k = 1",
        "SELECT k FROM {ks}.t WHERE s CONTAINS 'r' ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE l CONTAINS 1 ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE m CONTAINS KEY 'y' ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE m CONTAINS 7 ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE s CONTAINS 'r'",
        "SELECT k FROM {ks}.t WHERE m['y'] = 7 ALLOW FILTERING",
        P("INSERT INTO {ks}.t (k, l, s, m, fl, fm, tp) VALUES (?, ?, ?, ?, ?, ?, ?)",
          [20, [1, 2, 3], {"x", "y"}, {"a": 1}, [1], {1: "a"}, (5, "t", False)]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [20]),
        P("UPDATE {ks}.t SET l = l + ?, s = s + ?, m = m + ? WHERE k = ?", [[4], {"z"}, {"b": 2}, 20]),
        P("SELECT l, s, m FROM {ks}.t WHERE k = ?", [20]),
    ]))

    c.append(Case("types_udt", [
        "CREATE TYPE {ks}.addr (street text, zip int, tags frozen<list<text>>)",
        "CREATE TYPE {ks}.person (name text, home frozen<addr>, extra map<text, int>)",
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, a frozen<addr>, p frozen<person>, la list<frozen<addr>>, ma map<text, frozen<addr>>)",
        "INSERT INTO {ks}.t (k, a) VALUES (1, {street: 'Main', zip: 12345, tags: ['x', 'y']})",
        "INSERT INTO {ks}.t (k, a) VALUES (2, {street: 'Only'})",
        "INSERT INTO {ks}.t (k, a) VALUES (3, {zip: 9})",
        "INSERT INTO {ks}.t (k, a) VALUES (4, {nofield: 1})",
        "INSERT INTO {ks}.t (k, p) VALUES (5, {name: 'n', home: {street: 's', zip: 1, tags: []}, extra: {'a': 1}})",
        "SELECT * FROM {ks}.t",
        "SELECT a.street, a.zip FROM {ks}.t WHERE k = 1",
        "SELECT p.home.street FROM {ks}.t WHERE k = 5",
        "SELECT k FROM {ks}.t WHERE a = {street: 'Only'} ALLOW FILTERING",
        "UPDATE {ks}.t SET a = {street: 'New', zip: 2} WHERE k = 1",
        "SELECT a FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET la = la + [{street: 'a', zip: 1}, {street: 'b'}] WHERE k = 1",
        "UPDATE {ks}.t SET ma = ma + {'h': {street: 'home'}} WHERE k = 1",
        "SELECT la, ma FROM {ks}.t WHERE k = 1",
        "SELECT a.nope FROM {ks}.t WHERE k = 1",
    ]))

    c.append(Case("types_tuple_keys", [
        "CREATE TABLE {ks}.t (k frozen<tuple<int, text>>, c frozen<list<int>>, v int, PRIMARY KEY (k, c))",
        "INSERT INTO {ks}.t (k, c, v) VALUES ((1, 'a'), [1, 2], 1)",
        "INSERT INTO {ks}.t (k, c, v) VALUES ((1, 'a'), [1], 2)",
        "INSERT INTO {ks}.t (k, c, v) VALUES ((1, 'a'), [], 3)",
        "INSERT INTO {ks}.t (k, c, v) VALUES ((1, 'a'), [2], 4)",
        "INSERT INTO {ks}.t (k, c, v) VALUES ((0, 'z'), [5], 5)",
        "SELECT * FROM {ks}.t",
        "SELECT v FROM {ks}.t WHERE k = (1, 'a')",
        "SELECT v FROM {ks}.t WHERE k = (1, 'a') AND c > [1]",
        "SELECT v FROM {ks}.t WHERE k = (1, 'a') AND c = [1, 2]",
    ]))
    return c


def dml_cases():
    c = []
    c.append(Case("dml_basic", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, a text, b int)",
        "INSERT INTO {ks}.t (k, a, b) VALUES (1, 'x', 10)",
        "INSERT INTO {ks}.t (k, a) VALUES (1, 'y')",
        "SELECT * FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET b = 20 WHERE k = 2",
        "SELECT * FROM {ks}.t WHERE k = 2",
        "UPDATE {ks}.t SET a = null, b = null WHERE k = 2",
        "SELECT * FROM {ks}.t WHERE k = 2",
        "INSERT INTO {ks}.t (k) VALUES (3)",
        "SELECT * FROM {ks}.t WHERE k = 3",
        "UPDATE {ks}.t SET a = null WHERE k = 3",
        "SELECT * FROM {ks}.t WHERE k = 3",
        "DELETE FROM {ks}.t WHERE k = 3",
        "SELECT * FROM {ks}.t WHERE k = 3",
        "DELETE a FROM {ks}.t WHERE k = 1",
        "SELECT * FROM {ks}.t WHERE k = 1",
        "INSERT INTO {ks}.t (a, b) VALUES ('x', 1)",
        "INSERT INTO {ks}.t (k, nope) VALUES (1, 1)",
        "INSERT INTO {ks}.t (k, a, a) VALUES (1, 'a', 'b')",
        "INSERT INTO {ks}.t (k, a) VALUES (1)",
        "INSERT INTO {ks}.t (k, a) VALUES (null, 'x')",
        "INSERT INTO {ks}.nope (k) VALUES (1)",
        "UPDATE {ks}.t SET a = 'z'",
        "UPDATE {ks}.t SET a = 'z' WHERE b = 1",
        "UPDATE {ks}.t SET k = 5 WHERE k = 1",
        "UPDATE {ks}.t SET a = 'z' WHERE k > 1",
        "UPDATE {ks}.t SET a = 'z' WHERE k IN (1, 2)",
        "SELECT * FROM {ks}.t",
        "DELETE FROM {ks}.t WHERE k IN (1, 2)",
        "SELECT * FROM {ks}.t",
        "DELETE k FROM {ks}.t WHERE k = 1",
        "DELETE FROM {ks}.t",
        "INSERT INTO {ks}.t (k, a) VALUES (9, 'x') USING TTL -1",
        "INSERT INTO {ks}.t (k, a) VALUES (9, 'x') USING TIMESTAMP 1000 AND TTL 3600",
        "SELECT k, a, writetime(a) FROM {ks}.t WHERE k = 9",
        "INSERT INTO {ks}.t (k, a) VALUES (9, 'older') USING TIMESTAMP 500",
        "SELECT k, a, writetime(a) FROM {ks}.t WHERE k = 9",
        "INSERT INTO {ks}.t (k, a) VALUES (9, 'newer') USING TIMESTAMP 2000",
        "SELECT k, a, writetime(a) FROM {ks}.t WHERE k = 9",
        "DELETE FROM {ks}.t USING TIMESTAMP 1500 WHERE k = 9",
        "SELECT k, a, writetime(a) FROM {ks}.t WHERE k = 9",
        "DELETE FROM {ks}.t USING TIMESTAMP 2500 WHERE k = 9",
        "SELECT k, a FROM {ks}.t WHERE k = 9",
        "INSERT INTO {ks}.t (k, a) VALUES (9, 'after delete') USING TIMESTAMP 2400",
        "SELECT k, a FROM {ks}.t WHERE k = 9",
    ]))
    c.append(Case("dml_json", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, a text, b int, l list<int>, m map<text, int>, u uuid, ts timestamp, d double, bl blob, dt date)",
        "INSERT INTO {ks}.t JSON '{\"k\": 1, \"a\": \"hi\", \"b\": 5, \"l\": [1, 2], \"m\": {\"x\": 1}, \"u\": \"11111111-1111-1111-1111-111111111111\", "
        "\"ts\": \"2020-01-01 00:00:00.000Z\", \"d\": 1.5, \"bl\": \"0xcafe\", \"dt\": \"2020-01-01\"}'",
        "SELECT JSON * FROM {ks}.t",
        "SELECT JSON k, a, l FROM {ks}.t WHERE k = 1",
        "INSERT INTO {ks}.t JSON '{\"k\": 2, \"a\": null}'",
        "SELECT * FROM {ks}.t WHERE k = 2",
        "INSERT INTO {ks}.t JSON '{\"k\": 3, \"nope\": 1}'",
        "INSERT INTO {ks}.t JSON '{\"k\": \"x\"}'",
        "INSERT INTO {ks}.t JSON '{\"a\": \"noKey\"}'",
        "INSERT INTO {ks}.t JSON 'not json'",
        "INSERT INTO {ks}.t JSON '{\"k\": 4, \"a\": \"u\"}' DEFAULT UNSET",
        "SELECT toJson(a), toJson(l), toJson(m), toJson(ts), toJson(u) FROM {ks}.t WHERE k = 1",
        "SELECT k, fromJson('5') FROM {ks}.t WHERE k = 1",
        "INSERT INTO {ks}.t (k, b, l) VALUES (5, fromJson('7'), fromJson('[1,2]'))",
        "SELECT * FROM {ks}.t WHERE k = 5",
    ]))
    c.append(Case("dml_clustering", [
        "CREATE TABLE {ks}.t (p int, c1 int, c2 text, v int, PRIMARY KEY (p, c1, c2))",
        "INSERT INTO {ks}.t (p, c1, c2, v) VALUES (1, 1, 'b', 1)",
        "INSERT INTO {ks}.t (p, c1, c2, v) VALUES (1, 1, 'a', 2)",
        "INSERT INTO {ks}.t (p, c1, c2, v) VALUES (1, 2, 'a', 3)",
        "INSERT INTO {ks}.t (p, c1, c2, v) VALUES (1, 3, 'z', 4)",
        "INSERT INTO {ks}.t (p, c1, c2, v) VALUES (1, -1, 'q', 5)",
        "INSERT INTO {ks}.t (p, c1, c2, v) VALUES (2, 1, 'a', 6)",
        "SELECT * FROM {ks}.t WHERE p = 1",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 = 1",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 = 1 AND c2 = 'a'",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 > 1",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 >= 1 AND c1 < 3",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 = 1 AND c2 > 'a'",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 = 1 AND c2 >= 'a' AND c2 <= 'b'",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 IN (3, 1)",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 = 1 AND c2 IN ('b', 'a', 'x')",
        "SELECT * FROM {ks}.t WHERE p = 1 AND (c1, c2) > (1, 'a')",
        "SELECT * FROM {ks}.t WHERE p = 1 AND (c1, c2) >= (1, 'b') AND (c1, c2) < (3, 'a')",
        "SELECT * FROM {ks}.t WHERE p = 1 AND (c1, c2) IN ((1, 'a'), (2, 'a'))",
        "SELECT * FROM {ks}.t WHERE p = 1 ORDER BY c1 DESC",
        "SELECT * FROM {ks}.t WHERE p = 1 ORDER BY c1 DESC, c2 DESC",
        "SELECT * FROM {ks}.t WHERE p = 1 ORDER BY c1 ASC, c2 ASC",
        "SELECT * FROM {ks}.t WHERE p = 1 ORDER BY c1 ASC, c2 DESC",
        "SELECT * FROM {ks}.t WHERE p = 1 ORDER BY c2",
        "SELECT * FROM {ks}.t WHERE p = 1 ORDER BY v",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 = 1 ORDER BY c2 DESC",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 > 0 ORDER BY c1 DESC LIMIT 2",
        "SELECT * FROM {ks}.t WHERE p = 1 LIMIT 3",
        "SELECT * FROM {ks}.t WHERE p = 1 LIMIT 0",
        "SELECT * FROM {ks}.t WHERE p = 1 LIMIT -1",
        "SELECT * FROM {ks}.t LIMIT 4",
        "SELECT * FROM {ks}.t PER PARTITION LIMIT 1",
        "SELECT * FROM {ks}.t WHERE p IN (1, 2) PER PARTITION LIMIT 2",
        "SELECT * FROM {ks}.t WHERE c1 = 1",
        "SELECT * FROM {ks}.t WHERE c1 = 1 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c2 = 'a'",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c2 = 'a' ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 > 1 AND c2 = 'a' ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 > 1 AND c2 = 'a'",
        "SELECT * FROM {ks}.t WHERE p = 1 AND v = 3 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE p = 1 AND v = 3",
        "SELECT * FROM {ks}.t WHERE v > 2 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE p IN (2, 1) AND c1 = 1",
        "SELECT * FROM {ks}.t WHERE p IN (1, 2) ORDER BY c1 DESC",
        "SELECT * FROM {ks}.t WHERE p > 1",
        "SELECT * FROM {ks}.t WHERE p > 1 ALLOW FILTERING",
        "SELECT p, c1 FROM {ks}.t",
        "SELECT DISTINCT p FROM {ks}.t",
        "SELECT DISTINCT p, c1 FROM {ks}.t",
        "SELECT count(*) FROM {ks}.t",
        "SELECT count(*) FROM {ks}.t WHERE p = 1",
        "SELECT count(v), min(c1), max(c1), sum(v), avg(v) FROM {ks}.t WHERE p = 1",
        "SELECT count(*) FROM {ks}.t WHERE p = 3",
        "SELECT sum(v), min(c2), max(c2) FROM {ks}.t WHERE p = 3",
        "SELECT p, c1, c2 AS cc, v AS vv FROM {ks}.t WHERE p = 2",
        "SELECT nope FROM {ks}.t",
        "SELECT * FROM {ks}.nope",
        "SELECT c1 + 1, v * 2, v - 1, v / 2, v % 2 FROM {ks}.t WHERE p = 1 LIMIT 2",
        "DELETE FROM {ks}.t WHERE p = 1 AND c1 = 1 AND c2 = 'a'",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c1 = 1",
        "DELETE FROM {ks}.t WHERE p = 1 AND c1 = 1",
        "SELECT * FROM {ks}.t WHERE p = 1",
        "DELETE FROM {ks}.t WHERE p = 1 AND c1 > 2",
        "SELECT * FROM {ks}.t WHERE p = 1",
        "DELETE v FROM {ks}.t WHERE p = 1 AND c1 = 2",
        "DELETE FROM {ks}.t WHERE p = 1 AND c2 = 'a'",
        "UPDATE {ks}.t SET v = 1 WHERE p = 1 AND c1 = 2",
        "UPDATE {ks}.t SET v = 1 WHERE p = 1 AND c1 > 2 AND c2 = 'a'",
        "DELETE FROM {ks}.t WHERE p = 1",
        "SELECT * FROM {ks}.t",
    ]))
    c.append(Case("dml_order_desc", [
        "CREATE TABLE {ks}.t (p int, a int, b text, v int, PRIMARY KEY (p, a, b)) WITH CLUSTERING ORDER BY (a DESC, b ASC)",
        "INSERT INTO {ks}.t (p, a, b, v) VALUES (1, 1, 'x', 1)",
        "INSERT INTO {ks}.t (p, a, b, v) VALUES (1, 3, 'y', 2)",
        "INSERT INTO {ks}.t (p, a, b, v) VALUES (1, 3, 'a', 3)",
        "INSERT INTO {ks}.t (p, a, b, v) VALUES (1, 2, 'm', 4)",
        "INSERT INTO {ks}.t (p, a, b, v) VALUES (1, -5, 'q', 5)",
        "SELECT a, b FROM {ks}.t WHERE p = 1",
        "SELECT a, b FROM {ks}.t WHERE p = 1 ORDER BY a ASC, b DESC",
        "SELECT a, b FROM {ks}.t WHERE p = 1 ORDER BY a DESC, b ASC",
        "SELECT a, b FROM {ks}.t WHERE p = 1 ORDER BY a ASC",
        "SELECT a, b FROM {ks}.t WHERE p = 1 AND a > 1",
        "SELECT a, b FROM {ks}.t WHERE p = 1 AND a >= 2 AND a < 3",
        "SELECT a, b FROM {ks}.t WHERE p = 1 AND a < 3",
        "SELECT a, b FROM {ks}.t WHERE p = 1 AND a <= 2 AND a > -5",
        "SELECT a, b FROM {ks}.t WHERE p = 1 AND a = 3 AND b > 'a'",
        "SELECT a, b FROM {ks}.t WHERE p = 1 AND a IN (1, 3)",
        "SELECT a, b FROM {ks}.t WHERE p = 1 AND a > 1 ORDER BY a ASC",
        "DELETE FROM {ks}.t WHERE p = 1 AND a >= 2 AND a <= 3",
        "SELECT a, b FROM {ks}.t WHERE p = 1",
    ]))
    c.append(Case("dml_static", [
        "CREATE TABLE {ks}.t (p int, c int, s text static, s2 int static, v int, PRIMARY KEY (p, c))",
        "INSERT INTO {ks}.t (p, s) VALUES (1, 'static only')",
        "SELECT * FROM {ks}.t",
        "SELECT * FROM {ks}.t WHERE p = 1",
        "SELECT p, s FROM {ks}.t WHERE p = 1",
        "SELECT DISTINCT p, s FROM {ks}.t",
        "INSERT INTO {ks}.t (p, c, v) VALUES (1, 1, 10)",
        "INSERT INTO {ks}.t (p, c, v, s2) VALUES (1, 2, 20, 5)",
        "SELECT * FROM {ks}.t WHERE p = 1",
        "SELECT p, c, s, v FROM {ks}.t WHERE p = 1 AND c = 2",
        "SELECT s FROM {ks}.t WHERE p = 1",
        "UPDATE {ks}.t SET s = 'changed' WHERE p = 1",
        "SELECT p, c, s FROM {ks}.t WHERE p = 1",
        "INSERT INTO {ks}.t (p, c, s) VALUES (2, 1, 'with row')",
        "SELECT * FROM {ks}.t WHERE p = 2",
        "DELETE FROM {ks}.t WHERE p = 2 AND c = 1",
        "SELECT * FROM {ks}.t WHERE p = 2",
        "DELETE s FROM {ks}.t WHERE p = 1",
        "SELECT * FROM {ks}.t WHERE p = 1",
        "SELECT * FROM {ks}.t WHERE s = 'x' ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE p = 1 AND s2 = 5 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE p = 1 AND c = 1 AND s2 = 5 ALLOW FILTERING",
        "UPDATE {ks}.t SET s = 'x' WHERE p = 1 AND c = 1",
        "SELECT * FROM {ks}.t",
        "DELETE FROM {ks}.t WHERE p = 1",
        "SELECT * FROM {ks}.t",
    ]))
    c.append(Case("dml_partition_key", [
        "CREATE TABLE {ks}.t (a int, b text, c int, v int, PRIMARY KEY ((a, b), c))",
        "INSERT INTO {ks}.t (a, b, c, v) VALUES (1, 'x', 1, 1)",
        "INSERT INTO {ks}.t (a, b, c, v) VALUES (1, 'y', 1, 2)",
        "INSERT INTO {ks}.t (a, b, c, v) VALUES (2, 'x', 1, 3)",
        "INSERT INTO {ks}.t (a, b, c, v) VALUES (2, 'x', 2, 4)",
        "SELECT * FROM {ks}.t",
        "SELECT * FROM {ks}.t WHERE a = 1",
        "SELECT * FROM {ks}.t WHERE a = 1 AND b = 'x'",
        "SELECT * FROM {ks}.t WHERE a = 2 AND b = 'x' AND c = 2",
        "SELECT * FROM {ks}.t WHERE a IN (1, 2) AND b = 'x'",
        "SELECT * FROM {ks}.t WHERE a IN (2, 1) AND b IN ('y', 'x')",
        "SELECT * FROM {ks}.t WHERE a = 1 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE b = 'x' ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE token(a, b) > 0",
        "SELECT a, b, token(a, b) FROM {ks}.t",
        "SELECT a, b FROM {ks}.t WHERE token(a, b) > token(1, 'y')",
        "SELECT a, b FROM {ks}.t WHERE token(a, b) >= token(1, 'y') AND token(a, b) <= token(2, 'x')",
        "SELECT a, b FROM {ks}.t WHERE token(a) > 0",
        "SELECT a, b FROM {ks}.t WHERE token(b, a) > 0",
        "SELECT a, b FROM {ks}.t WHERE token(a, b) > 0 AND a = 1",
        "SELECT a, b FROM {ks}.t WHERE a > 1",
        "SELECT a, b FROM {ks}.t WHERE a > 1 ALLOW FILTERING",
        "SELECT DISTINCT a, b FROM {ks}.t",
        "SELECT count(*) FROM {ks}.t",
        "SELECT * FROM {ks}.t LIMIT 2",
        "INSERT INTO {ks}.t (a, c, v) VALUES (9, 9, 9)",
        "INSERT INTO {ks}.t (a, b, c, v) VALUES (9, '', 9, 9)",
        "SELECT * FROM {ks}.t WHERE a = 9 AND b = ''",
    ]))
    c.append(Case("dml_token_order", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)",
        *[f"INSERT INTO {{ks}}.t (k, v) VALUES ({i}, {i})" for i in range(1, 31)],
        "SELECT k, token(k) FROM {ks}.t",
        "SELECT k FROM {ks}.t WHERE k IN (5, 3, 1, 30, 12)",
        "SELECT k FROM {ks}.t WHERE k IN (5, 3, 3, 1)",
        "SELECT k FROM {ks}.t WHERE token(k) > -3000000000000000000 AND token(k) < 3000000000000000000",
        "SELECT k FROM {ks}.t WHERE token(k) >= -9223372036854775808 LIMIT 3",
        "SELECT token(k) FROM {ks}.t WHERE k = 1",
        "CREATE TABLE {ks}.t2 (k text PRIMARY KEY)",
        *[f"INSERT INTO {{ks}}.t2 (k) VALUES ('key{i}')" for i in range(12)],
        "INSERT INTO {ks}.t2 (k) VALUES ('')",
        "INSERT INTO {ks}.t2 (k) VALUES ('héllo')",
        "SELECT k, token(k) FROM {ks}.t2",
        "CREATE TABLE {ks}.t3 (a bigint, b blob, c uuid, PRIMARY KEY ((a, b, c)))",
        "INSERT INTO {ks}.t3 (a, b, c) VALUES (1, 0x00ff, 11111111-1111-1111-1111-111111111111)",
        "INSERT INTO {ks}.t3 (a, b, c) VALUES (-1, 0x, 22222222-2222-2222-2222-222222222222)",
        "INSERT INTO {ks}.t3 (a, b, c) VALUES (9223372036854775807, 0x0102030405060708090a0b0c0d0e0f1011, 33333333-3333-3333-3333-333333333333)",
        "SELECT a, b, c, token(a, b, c) FROM {ks}.t3",
        "CREATE TABLE {ks}.t4 (a double, b decimal, c inet, d boolean, PRIMARY KEY ((a, b), c, d))",
        "INSERT INTO {ks}.t4 (a, b, c, d) VALUES (1.5, 12.34, '10.0.0.1', true)",
        "INSERT INTO {ks}.t4 (a, b, c, d) VALUES (-0.5, -1e5, '::1', false)",
        "SELECT a, b, c, d, token(a, b) FROM {ks}.t4",
    ]))
    c.append(Case("dml_ttl", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, a text, b int)",
        "INSERT INTO {ks}.t (k, a, b) VALUES (1, 'x', 1) USING TTL 3",
        "INSERT INTO {ks}.t (k, a, b) VALUES (2, 'y', 2) USING TTL 100",
        "INSERT INTO {ks}.t (k, a, b) VALUES (3, 'z', 3)",
        Q("SELECT k, ttl(a) > 90, ttl(a) <= 100, ttl(b) IS NULL FROM {ks}.t WHERE k = 2", x=lambda r: [(r[0][0], True)]),
        Q("SELECT k, ttl(a) FROM {ks}.t WHERE k = 3"),
        "UPDATE {ks}.t USING TTL 3 SET a = 'temp' WHERE k = 3",
        "SELECT k, a, b FROM {ks}.t WHERE k = 3",
        W(4.5),
        "SELECT k, a, b FROM {ks}.t",
        "SELECT * FROM {ks}.t WHERE k = 1",
        "ALTER TABLE {ks}.t WITH default_time_to_live = 100",
        "INSERT INTO {ks}.t (k, a) VALUES (4, 'dflt')",
        Q("SELECT k, ttl(a) > 90 FROM {ks}.t WHERE k = 4", x=lambda r: [(r[0][0], r[0][1])]),
        "INSERT INTO {ks}.t (k, a) VALUES (5, 'zero') USING TTL 0",
        Q("SELECT k, ttl(a) FROM {ks}.t WHERE k = 5"),
        "INSERT INTO {ks}.t (k, a) VALUES (6, 'big') USING TTL 999999999",
        "SELECT ttl(k) FROM {ks}.t WHERE k = 1",
    ]))
    c.append(Case("dml_functions", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, ts timestamp, u uuid, tu timeuuid, d date, b blob, i int, t text, f double, bi bigint)",
        "INSERT INTO {ks}.t (k, ts, u, tu, d, b, i, t, f, bi) VALUES (1, '2020-05-06 07:08:09.123+0000', 11111111-1111-1111-1111-111111111111, "
        "d2177dd0-eaa2-11de-a572-001b779c76e3, '2020-05-06', 0x0102, 42, 'txt', 2.5, 9000000000)",
        "SELECT toTimestamp(tu), toUnixTimestamp(tu), toDate(tu), toDate(ts), toUnixTimestamp(ts), toTimestamp(d), toUnixTimestamp(d) FROM {ks}.t WHERE k = 1",
        "SELECT dateOf(tu), unixTimestampOf(tu) FROM {ks}.t WHERE k = 1",
        "SELECT minTimeuuid('2020-01-01 00:00:00+0000'), maxTimeuuid('2020-01-01 00:00:00+0000') FROM {ks}.t WHERE k = 1",
        "SELECT minTimeuuid(ts), maxTimeuuid(ts) FROM {ks}.t WHERE k = 1",
        "SELECT blobAsInt(intAsBlob(i)), textAsBlob(t), blobAsText(textAsBlob(t)), bigintAsBlob(bi), blobAsBigint(bigintAsBlob(bi)) FROM {ks}.t WHERE k = 1",
        "SELECT blobAsInt(b) FROM {ks}.t WHERE k = 1",
        "SELECT cast(i as text), cast(i as bigint), cast(i as double), cast(f as int), cast(bi as varint), cast(d as timestamp), cast(ts as date), cast(u as text), cast(f as text) FROM {ks}.t WHERE k = 1",
        "SELECT cast(t as int) FROM {ks}.t WHERE k = 1",
        "SELECT i + 1, i - 1, i * 2, i / 5, i % 5, f * 2, bi + i, -i FROM {ks}.t WHERE k = 1",
        "SELECT i / 0 FROM {ks}.t WHERE k = 1",
        "SELECT ts + 1d, ts - 1h, d + 1d FROM {ks}.t WHERE k = 1",
        Q("SELECT writetime(i), ttl(i) FROM {ks}.t WHERE k = 1", x=lambda r: [(r[0][0] > 0, r[0][1])]),
        "SELECT writetime(k) FROM {ks}.t WHERE k = 1",
        "SELECT ttl(k) FROM {ks}.t WHERE k = 1",
        "SELECT nofunction(i) FROM {ks}.t WHERE k = 1",
        "SELECT i, i AS ii, t AS tt FROM {ks}.t WHERE k = 1",
        "SELECT toJson(i), toJson(t), toJson(f), toJson(d), toJson(b), toJson(ts), toJson(tu) FROM {ks}.t WHERE k = 1",
        Q("SELECT now() FROM {ks}.t WHERE k = 1", x=lambda r: [(len(str(r[0][0])),)]),
        Q("SELECT uuid() FROM {ks}.t WHERE k = 1", x=lambda r: [(len(str(r[0][0])),)]),
        "UPDATE {ks}.t SET tu = now() WHERE k = 1",
        Q("SELECT toTimestamp(tu) > '2020-01-01' FROM {ks}.t WHERE k = 1", x=lambda r: [(1,)]),
        "INSERT INTO {ks}.t (k, u) VALUES (2, uuid())",
        "INSERT INTO {ks}.t (k, ts) VALUES (3, toTimestamp(now()))",
        "INSERT INTO {ks}.t (k, ts, d) VALUES (4, currentTimestamp(), currentDate())",
        "SELECT abs(i), abs(-5), abs(f) FROM {ks}.t WHERE k = 1",
        "SELECT count(*) AS c FROM {ks}.t",
    ]))
    return c


def counter_cases():
    return [
        Case("counters", [
            "CREATE TABLE {ks}.c (k int PRIMARY KEY, a counter, b counter)",
            "UPDATE {ks}.c SET a = a + 1 WHERE k = 1",
            "UPDATE {ks}.c SET a = a + 5, b = b - 2 WHERE k = 1",
            "SELECT * FROM {ks}.c WHERE k = 1",
            "UPDATE {ks}.c SET a = a - 100 WHERE k = 2",
            "SELECT * FROM {ks}.c WHERE k = 2",
            "UPDATE {ks}.c SET a = 5 WHERE k = 3",
            "INSERT INTO {ks}.c (k, a) VALUES (4, 1)",
            "UPDATE {ks}.c SET a = a + 1 WHERE k IN (5, 6)",
            "SELECT * FROM {ks}.c",
            "DELETE b FROM {ks}.c WHERE k = 1",
            "SELECT * FROM {ks}.c WHERE k = 1",
            "DELETE FROM {ks}.c WHERE k = 2",
            "SELECT * FROM {ks}.c WHERE k = 2",
            "UPDATE {ks}.c SET a = a + 1 WHERE k = 2",
            "SELECT * FROM {ks}.c WHERE k = 2",
            "SELECT writetime(a) > 0 FROM {ks}.c WHERE k = 1",
            "UPDATE {ks}.c USING TTL 10 SET a = a + 1 WHERE k = 1",
            "UPDATE {ks}.c SET a = a + 9223372036854775807 WHERE k = 7",
            "UPDATE {ks}.c SET a = a + 1 WHERE k = 7",
            "SELECT * FROM {ks}.c WHERE k = 7",
            "CREATE TABLE {ks}.cc (p int, c int, n counter, PRIMARY KEY (p, c))",
            "UPDATE {ks}.cc SET n = n + 3 WHERE p = 1 AND c = 1",
            "UPDATE {ks}.cc SET n = n + 4 WHERE p = 1 AND c = 2",
            "UPDATE {ks}.cc SET n = n + 1 WHERE p = 1 AND c = 1",
            "SELECT * FROM {ks}.cc WHERE p = 1",
            "SELECT sum(n), count(*) FROM {ks}.cc WHERE p = 1",
            P("UPDATE {ks}.cc SET n = n + ? WHERE p = ? AND c = ?", [10, 1, 1]),
            "SELECT n FROM {ks}.cc WHERE p = 1 AND c = 1",
            "CREATE TABLE {ks}.mixed (k int PRIMARY KEY, a counter, b int)",
            B("counter", ["UPDATE {ks}.c SET a = a + 1 WHERE k = 20", "UPDATE {ks}.c SET a = a + 2 WHERE k = 21"]),
            "SELECT * FROM {ks}.c WHERE k IN (20, 21)",
            B("counter", ["UPDATE {ks}.c SET a = a + 1 WHERE k = 20", "UPDATE {ks}.cc SET n = n + 1 WHERE p = 1 AND c = 1"]),
            B("logged", ["UPDATE {ks}.c SET a = a + 1 WHERE k = 20"]),
            B("unlogged", ["UPDATE {ks}.c SET a = a + 1 WHERE k = 20"]),
            "CREATE TABLE {ks}.st (p int, c int, sc counter static, n counter, PRIMARY KEY (p, c))",
        ]),
    ]


def lwt_cases():
    return [
        Case("lwt", [
            "CREATE TABLE {ks}.t (k int PRIMARY KEY, a text, b int, l list<int>, m map<text,int>)",
            "INSERT INTO {ks}.t (k, a, b) VALUES (1, 'x', 10) IF NOT EXISTS",
            "INSERT INTO {ks}.t (k, a, b) VALUES (1, 'y', 20) IF NOT EXISTS",
            "SELECT * FROM {ks}.t WHERE k = 1",
            "UPDATE {ks}.t SET a = 'z' WHERE k = 1 IF a = 'x'",
            "UPDATE {ks}.t SET a = 'w' WHERE k = 1 IF a = 'x'",
            "UPDATE {ks}.t SET a = 'w' WHERE k = 1 IF a = 'z' AND b = 10",
            "UPDATE {ks}.t SET a = 'w' WHERE k = 1 IF a = 'z' AND b = 11",
            "UPDATE {ks}.t SET b = 5 WHERE k = 1 IF b > 5",
            "UPDATE {ks}.t SET b = 5 WHERE k = 1 IF b >= 10",
            "UPDATE {ks}.t SET b = 6 WHERE k = 1 IF b != 5",
            "UPDATE {ks}.t SET b = 7 WHERE k = 1 IF b IN (1, 6)",
            "UPDATE {ks}.t SET b = 8 WHERE k = 1 IF b IN (1, 2)",
            "UPDATE {ks}.t SET b = 8 WHERE k = 99 IF b = 1",
            "UPDATE {ks}.t SET b = 8 WHERE k = 99 IF EXISTS",
            "UPDATE {ks}.t SET b = 8 WHERE k = 1 IF EXISTS",
            "UPDATE {ks}.t SET b = 8 WHERE k = 1 IF nope = 1",
            "UPDATE {ks}.t SET b = 8 WHERE k = 1 IF k = 1",
            "SELECT * FROM {ks}.t WHERE k = 1",
            "UPDATE {ks}.t SET a = null WHERE k = 2 IF a = null",
            "SELECT * FROM {ks}.t WHERE k = 2",
            "INSERT INTO {ks}.t (k, a) VALUES (3, 'n') IF NOT EXISTS USING TTL 100",
            "INSERT INTO {ks}.t (k, a) VALUES (3, 'n') IF NOT EXISTS USING TIMESTAMP 5",
            "DELETE FROM {ks}.t WHERE k = 3 IF a = 'no'",
            "DELETE FROM {ks}.t WHERE k = 3 IF a = 'n'",
            "SELECT * FROM {ks}.t WHERE k = 3",
            "DELETE FROM {ks}.t WHERE k = 3 IF EXISTS",
            "INSERT INTO {ks}.t (k, l, m) VALUES (4, [1, 2], {'a': 1}) IF NOT EXISTS",
            "UPDATE {ks}.t SET b = 1 WHERE k = 4 IF l = [1, 2]",
            "UPDATE {ks}.t SET b = 2 WHERE k = 4 IF l[0] = 1 AND m['a'] = 1",
            "UPDATE {ks}.t SET b = 3 WHERE k = 4 IF l[0] = 9",
            "SELECT * FROM {ks}.t WHERE k = 4",
            P("INSERT INTO {ks}.t (k, a) VALUES (?, ?) IF NOT EXISTS", [10, "p"]),
            P("INSERT INTO {ks}.t (k, a) VALUES (?, ?) IF NOT EXISTS", [10, "q"]),
            P("UPDATE {ks}.t SET a = ? WHERE k = ? IF a = ?", ["r", 10, "p"]),
            P("UPDATE {ks}.t SET a = ? WHERE k = ? IF a = ?", ["s", 10, "wrong"]),
            "CREATE TABLE {ks}.c (p int, c int, v int, s int static, PRIMARY KEY (p, c))",
            "INSERT INTO {ks}.c (p, c, v) VALUES (1, 1, 1) IF NOT EXISTS",
            "INSERT INTO {ks}.c (p, c, v) VALUES (1, 1, 2) IF NOT EXISTS",
            "INSERT INTO {ks}.c (p, c, v) VALUES (1, 2, 2) IF NOT EXISTS",
            "UPDATE {ks}.c SET v = 9 WHERE p = 1 AND c = 1 IF v = 1",
            "UPDATE {ks}.c SET s = 5 WHERE p = 1 IF s = null",
            "UPDATE {ks}.c SET s = 6 WHERE p = 1 IF s = 5",
            "UPDATE {ks}.c SET v = 9 WHERE p = 1 IF v = 1",
            "SELECT * FROM {ks}.c",
            B("logged", ["INSERT INTO {ks}.c (p, c, v) VALUES (2, 1, 1) IF NOT EXISTS", "INSERT INTO {ks}.c (p, c, v) VALUES (2, 2, 1) IF NOT EXISTS"]),
            B("logged", ["INSERT INTO {ks}.c (p, c, v) VALUES (2, 1, 1) IF NOT EXISTS", "INSERT INTO {ks}.c (p, c, v) VALUES (2, 3, 1) IF NOT EXISTS"]),
            B("logged", ["INSERT INTO {ks}.c (p, c, v) VALUES (3, 1, 1) IF NOT EXISTS", "INSERT INTO {ks}.c (p, c, v) VALUES (4, 1, 1) IF NOT EXISTS"]),
            "SELECT * FROM {ks}.c",
        ]),
    ]


def batch_cases():
    return [
        Case("batches", [
            "CREATE TABLE {ks}.t (k int, c int, v text, PRIMARY KEY (k, c))",
            "CREATE TABLE {ks}.u (k int PRIMARY KEY, v int)",
            B("logged", ["INSERT INTO {ks}.t (k, c, v) VALUES (1, 1, 'a')", "INSERT INTO {ks}.t (k, c, v) VALUES (1, 2, 'b')",
                         "INSERT INTO {ks}.t (k, c, v) VALUES (2, 1, 'c')", "INSERT INTO {ks}.u (k, v) VALUES (1, 1)"]),
            "SELECT * FROM {ks}.t",
            "SELECT * FROM {ks}.u",
            B("unlogged", ["UPDATE {ks}.t SET v = 'z' WHERE k = 1 AND c = 1", "DELETE FROM {ks}.t WHERE k = 1 AND c = 2",
                           "INSERT INTO {ks}.t (k, c, v) VALUES (3, 3, 'x')"]),
            "SELECT * FROM {ks}.t",
            B("logged", [("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [10, 1, "p"]), ("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [10, 2, "q"])]),
            "SELECT * FROM {ks}.t WHERE k = 10",
            B("logged", ["INSERT INTO {ks}.t (k, c, v) VALUES (20, 1, 'a')", "INSERT INTO {ks}.nope (k) VALUES (1)"]),
            "SELECT * FROM {ks}.t WHERE k = 20",
            B("logged", ["INSERT INTO {ks}.t (k, c) VALUES (20, 1) USING TIMESTAMP 5", "INSERT INTO {ks}.t (k, c, v) VALUES (20, 1, 'x') USING TIMESTAMP 4"]),
            "SELECT k, c, v, writetime(v) FROM {ks}.t WHERE k = 20",
            B("logged", ["SELECT * FROM {ks}.t"]),
            "BEGIN BATCH INSERT INTO {ks}.t (k, c, v) VALUES (30, 1, 'a'); INSERT INTO {ks}.t (k, c, v) VALUES (30, 2, 'b'); APPLY BATCH",
            "SELECT * FROM {ks}.t WHERE k = 30",
            "BEGIN UNLOGGED BATCH USING TIMESTAMP 1000 INSERT INTO {ks}.t (k, c, v) VALUES (31, 1, 'a'); INSERT INTO {ks}.t (k, c, v) VALUES (31, 2, 'b'); APPLY BATCH",
            "SELECT k, c, writetime(v) FROM {ks}.t WHERE k = 31",
            "BEGIN BATCH USING TIMESTAMP 1000 INSERT INTO {ks}.t (k, c, v) VALUES (32, 1, 'a') USING TIMESTAMP 5; APPLY BATCH",
            "BEGIN BATCH INSERT INTO {ks}.t (k, c, v) VALUES (33, 1, 'a'); UPDATE {ks}.t SET v = 'b' WHERE k = 33 AND c = 1; APPLY BATCH",
            "SELECT * FROM {ks}.t WHERE k = 33",
            "BEGIN COUNTER BATCH INSERT INTO {ks}.t (k, c, v) VALUES (34, 1, 'a'); APPLY BATCH",
        ]),
    ]


def paging_cases():
    ins = [f"INSERT INTO {{ks}}.t (p, c, v) VALUES ({p}, {c}, {p * 100 + c})" for p in range(1, 6) for c in range(1, 8)]
    return [
        Case("paging", [
            "CREATE TABLE {ks}.t (p int, c int, v int, PRIMARY KEY (p, c))",
            *ins,
            Q("SELECT * FROM {ks}.t WHERE p = 3", page=3),
            Q("SELECT * FROM {ks}.t WHERE p = 3", page=7),
            Q("SELECT * FROM {ks}.t WHERE p = 3", page=100),
            Q("SELECT * FROM {ks}.t", page=10),
            Q("SELECT * FROM {ks}.t", page=1),
            Q("SELECT * FROM {ks}.t LIMIT 12", page=5),
            Q("SELECT * FROM {ks}.t WHERE p = 2 ORDER BY c DESC", page=3),
            Q("SELECT * FROM {ks}.t WHERE p IN (1, 2, 3)", page=4),
            Q("SELECT * FROM {ks}.t WHERE p = 1 AND c > 2 AND c < 6", page=2),
            Q("SELECT v FROM {ks}.t WHERE v > 300 ALLOW FILTERING", page=4),
            Q("SELECT DISTINCT p FROM {ks}.t", page=2),
            Q("SELECT count(*) FROM {ks}.t", page=3),
            Q("SELECT * FROM {ks}.t PER PARTITION LIMIT 2", page=3),
            P("SELECT * FROM {ks}.t WHERE p = ?", [4], page=3),
            P("SELECT * FROM {ks}.t", [], page=8),
            Q("SELECT * FROM {ks}.t WHERE token(p) > 0", page=6),
        ]),
    ]


def system_cases():
    return [
        Case("system_tables", [
            Q("SELECT key, bootstrapped, cql_version, data_center, partitioner, rack FROM system.local"),
            Q("SELECT key FROM system.local WHERE key = 'local'"),
            Q("SELECT key FROM system.local WHERE key = 'nope'"),
            Q("SELECT count(*) FROM system.peers", x=lambda r: [tuple(r[0])]),
            "SELECT column_name, kind, type FROM system_schema.columns WHERE keyspace_name = 'system' AND table_name = 'local'",
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'system_schema'",
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'system_schema'",
            "CREATE TABLE {ks}.t (a int, b int, c text, PRIMARY KEY ((a), b))",
            "SELECT keyspace_name, table_name, flags, gc_grace_seconds, default_time_to_live, bloom_filter_fp_chance, caching, compaction, compression, "
            "speculative_retry, read_repair, min_index_interval, max_index_interval, crc_check_chance, memtable_flush_period_in_ms FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't'",
            "SELECT * FROM system_schema.columns WHERE keyspace_name = '{ks}'",
            "SELECT keyspace_name, table_name, column_name FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = 't' AND column_name = 'c'",
            "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'",
            "SELECT * FROM system_schema.indexes WHERE keyspace_name = '{ks}'",
            "SELECT * FROM system_schema.types WHERE keyspace_name = '{ks}'",
            "SELECT * FROM system_schema.views WHERE keyspace_name = '{ks}'",
            "SELECT * FROM system_schema.functions WHERE keyspace_name = '{ks}'",
            "SELECT * FROM system_schema.aggregates WHERE keyspace_name = '{ks}'",
            "SELECT * FROM system_schema.triggers WHERE keyspace_name = '{ks}'",
            "SELECT * FROM system_schema.dropped_columns WHERE keyspace_name = '{ks}'",
            "DELETE FROM system_schema.tables WHERE keyspace_name = 'x'",
            "SELECT * FROM system.nope",
        ]),
    ]


def error_cases():
    return [
        Case("errors", [
            "SELEC * FROM x",
            "SELECT",
            "SELECT * FROM",
            "INSERT INTO",
            "CREATE TABLE",
            "FOO BAR",
            "",
            ";",
            "SELECT * FROM {ks}.t WHERE",
            "SELECT 'unterminated FROM t",
            "CREATE TABLE {ks}.t (a int PRIMARY KEY, b text)",
            "SELECT * FROM {ks}.t WHERE a = 'x'",
            "SELECT * FROM {ks}.t WHERE a = 1 AND a = 2",
            "SELECT * FROM {ks}.t WHERE a IN (1) AND a = 1",
            "SELECT * FROM {ks}.t WHERE nope = 1",
            "SELECT * FROM {ks}.t WHERE b = 'x'",
            "SELECT * FROM {ks}.t WHERE b LIKE 'x%'",
            "SELECT * FROM {ks}.t WHERE a = ?",
            "INSERT INTO {ks}.t (a, b) VALUES (1, ?)",
            "INSERT INTO {ks}.t (a, b) VALUES (1, 'x'",
            "INSERT INTO {ks}.t (a, b) VALUES (1, 'x') IF",
            "UPDATE {ks}.t SET b = 'x' WHERE a = 1 IF",
            "SELECT * FROM {ks}.t LIMIT 'x'",
            "SELECT * FROM {ks}.t ORDER BY a",
            "SELECT * FROM {ks}.t WHERE a = 1 ORDER BY b",
            "SELECT * FROM {ks}.t GROUP BY a",
            "SELECT a, count(*) FROM {ks}.t",
            "SELECT DISTINCT b FROM {ks}.t",
            "SELECT * FROM {ks}.t WHERE a IN ()",
            "SELECT a AS x, b AS x FROM {ks}.t WHERE a = 1",
            "CREATE ROLE r WITH PASSWORD = 'x'",
            "CREATE MATERIALIZED VIEW {ks}.v AS SELECT * FROM {ks}.t WHERE b IS NOT NULL PRIMARY KEY (b, a)",
            "CREATE FUNCTION {ks}.f (a int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return a;'",
            "GRANT SELECT ON {ks}.t TO r",
            "LIST ROLES",
            "USE",
            "DROP TABLE",
            "TRUNCATE",
            "BEGIN BATCH APPLY BATCH",
            "BEGIN BATCH SELECT * FROM {ks}.t; APPLY BATCH",
            "SELECT * FROM {ks}.t WHERE a = 1 ALLOW FILTERING LIMIT 1",
            "select * from {ks}.T where A = 1",
            "SELECT * FROM {ks}.\"t\" WHERE \"a\" = 1",
            "SELECT * FROM {ks}.\"T\" WHERE a = 1",
            "CREATE TABLE {ks}.\"MixedCase\" (\"Key\" int PRIMARY KEY, \"Val\" text)",
            "INSERT INTO {ks}.\"MixedCase\" (\"Key\", \"Val\") VALUES (1, 'x')",
            "SELECT * FROM {ks}.\"MixedCase\"",
            "SELECT * FROM {ks}.MixedCase",
            "SELECT \"Key\", \"Val\" FROM {ks}.\"MixedCase\"",
            "SELECT Key FROM {ks}.\"MixedCase\"",
            "SELECT * FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = 'MixedCase'",
        ]),
    ]


def order_cases():
    """Clustering order of every orderable type: exercises the order-preserving key encoding against Cassandra's comparators."""
    vals = {
        "int": ["0", "-1", "5", "-2147483648", "2147483647", "100"],
        "bigint": ["0", "-1", "9223372036854775807", "-9223372036854775808", "42", "-42"],
        "smallint": ["0", "-1", "32767", "-32768", "100"],
        "tinyint": ["0", "-1", "127", "-128", "5"],
        "varint": ["0", "-1", "1", "255", "256", "-256", "-255", "123456789012345678901234567890", "-123456789012345678901234567890", "65536", "-65536"],
        "decimal": ["0", "1", "-1", "1.5", "-1.5", "0.001", "-0.001", "100", "1E+10", "-1E+10", "3.14159", "1.50", "2"],
        "double": ["0.0", "-0.0", "1.5", "-1.5", "1e300", "-1e300", "NaN", "Infinity", "-Infinity", "0.1"],
        "float": ["0.0", "1.5", "-1.5", "1e30", "-1e30", "0.1"],
        "text": ["''", "'a'", "'b'", "'ab'", "'B'", "'\u00e9'", "'z'", "'aa'", "'\u4e16'"],
        "ascii": ["''", "'a'", "'b'", "'ab'", "'B'", "'zz'"],
        "blob": ["0x", "0x00", "0x01", "0x0000", "0x00ff", "0xff", "0x0001", "0xff00"],
        "boolean": ["true", "false"],
        "date": ["'2020-01-01'", "'1970-01-01'", "'1969-12-31'", "'2100-06-15'", "'1900-01-01'"],
        "time": ["'00:00:00'", "'23:59:59.999999999'", "'12:00:00'", "'12:00:00.000000001'"],
        "timestamp": ["'2020-01-01 00:00:00+0000'", "0", "-1", "1600000000123", "'1969-12-31 23:59:59.999+0000'"],
        "uuid": ["11111111-1111-1111-1111-111111111111", "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
                 "d2177dd0-eaa2-11de-a572-001b779c76e3", "550e8400-e29b-41d4-a716-446655440000", "22222222-2222-4222-8222-222222222222"],
        "timeuuid": ["d2177dd0-eaa2-11de-a572-001b779c76e3", "a747c000-2c29-11ea-8080-808080808080", "a747c000-2c29-11ea-7f7f-7f7f7f7f7f7f",
                     "573d5730-8f68-11ea-8080-808080808080", "00000000-0000-1000-8000-000000000000"],
        "inet": ["'1.2.3.4'", "'10.0.0.1'", "'255.255.255.255'", "'::1'", "'2001:db8::1'", "'0.0.0.0'"],
        "frozen<tuple<int, text>>": ["(1, 'a')", "(1, 'b')", "(0, 'z')", "(1, null)", "(-1, 'a')"],
        "frozen<list<int>>": ["[]", "[1]", "[1, 2]", "[2]", "[1, 1]", "[-1]"],
        "frozen<set<text>>": ["{}", "{'a'}", "{'a', 'b'}", "{'b'}", "{'c', 'a'}"],
        "frozen<map<int, text>>": ["{1: 'a'}", "{1: 'b'}", "{2: 'a'}", "{1: 'a', 2: 'b'}"],
    }
    cases = []
    for typ, vs in vals.items():
        name = "order_" + typ.replace("<", "_").replace(">", "").replace(",", "").replace(" ", "")
        steps = [f"CREATE TABLE {{ks}}.a (p int, c {typ}, v int, PRIMARY KEY (p, c))",
                 f"CREATE TABLE {{ks}}.d (p int, c {typ}, v int, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)"]
        for i, v in enumerate(vs):
            steps.append(f"INSERT INTO {{ks}}.a (p, c, v) VALUES (1, {v}, {i})")
            steps.append(f"INSERT INTO {{ks}}.d (p, c, v) VALUES (1, {v}, {i})")
        v2 = vs[min(2, len(vs) - 1)]
        steps += ["SELECT c, v FROM {ks}.a WHERE p = 1", "SELECT c, v FROM {ks}.d WHERE p = 1",
                  "SELECT c, v FROM {ks}.a WHERE p = 1 ORDER BY c DESC", "SELECT c, v FROM {ks}.d WHERE p = 1 ORDER BY c ASC",
                  f"SELECT v FROM {{ks}}.a WHERE p = 1 AND c > {vs[1]}", f"SELECT v FROM {{ks}}.d WHERE p = 1 AND c > {vs[1]}",
                  f"SELECT v FROM {{ks}}.a WHERE p = 1 AND c <= {v2}", f"SELECT v FROM {{ks}}.d WHERE p = 1 AND c <= {v2}",
                  f"SELECT v FROM {{ks}}.a WHERE p = 1 AND c >= {vs[0]} AND c < {v2}", f"SELECT v FROM {{ks}}.d WHERE p = 1 AND c >= {vs[0]} AND c < {v2}",
                  f"SELECT v FROM {{ks}}.a WHERE p = 1 AND c = {vs[1]}", f"SELECT v FROM {{ks}}.d WHERE p = 1 AND c IN ({vs[1]}, {vs[0]})",
                  f"DELETE FROM {{ks}}.a WHERE p = 1 AND c = {vs[0]}", "SELECT c FROM {ks}.a WHERE p = 1"]
        if typ not in ("boolean",) and not typ.startswith("frozen"):
            steps += [f"CREATE TABLE {{ks}}.k (c {typ} PRIMARY KEY, v int)"] + \
                     [f"INSERT INTO {{ks}}.k (c, v) VALUES ({v}, {i})" for i, v in enumerate(vs)] + \
                     ["SELECT c, token(c) FROM {ks}.k"]
        else:
            steps += [f"CREATE TABLE {{ks}}.k (c {typ} PRIMARY KEY, v int)"] + \
                     [f"INSERT INTO {{ks}}.k (c, v) VALUES ({v}, {i})" for i, v in enumerate(vs)] + \
                     ["SELECT c, token(c) FROM {ks}.k"]
        cases.append(Case(name, steps))
    return cases


def more_cases():
    c = []
    c.append(Case("tombstones", [
        "CREATE TABLE {ks}.t (k int, c int, v text, w text, PRIMARY KEY (k, c))",
        "INSERT INTO {ks}.t (k, c, v, w) VALUES (1, 1, 'a', 'b') USING TIMESTAMP 100",
        "DELETE FROM {ks}.t USING TIMESTAMP 200 WHERE k = 1 AND c = 1",
        "INSERT INTO {ks}.t (k, c, v) VALUES (1, 1, 'older') USING TIMESTAMP 150",
        "SELECT * FROM {ks}.t WHERE k = 1",
        "INSERT INTO {ks}.t (k, c, v) VALUES (1, 1, 'newer') USING TIMESTAMP 300",
        "SELECT k, c, v, w, writetime(v) FROM {ks}.t WHERE k = 1",
        "DELETE v FROM {ks}.t USING TIMESTAMP 400 WHERE k = 1 AND c = 1",
        "UPDATE {ks}.t USING TIMESTAMP 350 SET v = 'lost' WHERE k = 1 AND c = 1",
        "SELECT k, c, v, w FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t USING TIMESTAMP 450 SET v = 'won' WHERE k = 1 AND c = 1",
        "SELECT k, c, v, w FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t USING TIMESTAMP 500 SET v = null WHERE k = 2 AND c = 1",
        "SELECT * FROM {ks}.t WHERE k = 2",
        "INSERT INTO {ks}.t (k, c, v) VALUES (2, 1, 'x') USING TIMESTAMP 400",
        "SELECT * FROM {ks}.t WHERE k = 2",
        "INSERT INTO {ks}.t (k, c, v) VALUES (3, 1, 'x') USING TIMESTAMP 100",
        "INSERT INTO {ks}.t (k, c, v) VALUES (3, 1, null) USING TIMESTAMP 200",
        "SELECT * FROM {ks}.t WHERE k = 3",
        "INSERT INTO {ks}.t (k, c, v) VALUES (3, 2, 'y') USING TIMESTAMP 100",
        "DELETE FROM {ks}.t USING TIMESTAMP 150 WHERE k = 3",
        "SELECT * FROM {ks}.t WHERE k = 3",
        "DELETE FROM {ks}.t USING TIMESTAMP 250 WHERE k = 3",
        "SELECT * FROM {ks}.t WHERE k = 3",
        "SELECT count(*) FROM {ks}.t",
        "CREATE TABLE {ks}.m (k int PRIMARY KEY, l list<int>, s set<int>, mp map<int,int>)",
        "INSERT INTO {ks}.m (k, s, mp) VALUES (1, {1,2}, {1: 1}) USING TIMESTAMP 100",
        "DELETE s[1] FROM {ks}.m USING TIMESTAMP 200 WHERE k = 1",
        "DELETE mp[1] FROM {ks}.m USING TIMESTAMP 200 WHERE k = 1",
        "SELECT * FROM {ks}.m WHERE k = 1",
        "UPDATE {ks}.m USING TIMESTAMP 150 SET mp[1] = 5 WHERE k = 1",
        "SELECT * FROM {ks}.m WHERE k = 1",
        "UPDATE {ks}.m USING TIMESTAMP 250 SET mp[1] = 6 WHERE k = 1",
        "SELECT * FROM {ks}.m WHERE k = 1",
    ]))
    c.append(Case("meta_schema", [
        "CREATE TYPE {ks}.addr (street text, zip int)",
        "CREATE TABLE {ks}.t1 (a int, b text, c bigint, d frozen<addr>, e map<text, frozen<list<int>>>, f set<uuid>, PRIMARY KEY ((a, b), c)) WITH CLUSTERING ORDER BY (c DESC)",
        "CREATE TABLE {ks}.t2 (k int PRIMARY KEY, v text, s counter) ",
        "CREATE TABLE {ks}.t3 (k int PRIMARY KEY, v text) WITH comment = 'a table' AND default_time_to_live = 60 AND gc_grace_seconds = 100",
        "CREATE TABLE {ks}.t4 (p int, c int, s text static, v int, PRIMARY KEY (p, c))",
        "CREATE INDEX v_idx ON {ks}.t3 (v)",
        "CREATE INDEX ON {ks}.t4 (v)",
        M(),
        "ALTER TABLE {ks}.t4 ADD extra list<text>",
        "ALTER TABLE {ks}.t3 WITH comment = 'changed'",
        M(),
    ]))
    c.append(Case("prepared_flow", [
        "CREATE TABLE {ks}.t (k int, c int, v text, l list<int>, PRIMARY KEY (k, c))",
        P("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [1, 1, "a"]),
        P("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [1, 2, "b"]),
        P("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [2, 1, "c"]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [1]),
        P("SELECT * FROM {ks}.t WHERE k = ? AND c > ?", [1, 1]),
        P("SELECT * FROM {ks}.t WHERE k IN ?", [[1, 2]]),
        P("SELECT * FROM {ks}.t WHERE k IN (?, ?) AND c = ?", [1, 2, 1]),
        P("SELECT * FROM {ks}.t WHERE k = ? LIMIT ?", [1, 1]),
        P("SELECT k, c FROM {ks}.t WHERE token(k) > ? ", [-9223372036854775808]),
        P("SELECT * FROM {ks}.t WHERE k = ? AND c = ? AND v = ? ALLOW FILTERING", [1, 1, "a"]),
        P("UPDATE {ks}.t SET v = ? WHERE k = ? AND c = ?", ["z", 1, 1]),
        P("UPDATE {ks}.t USING TTL ? SET v = ? WHERE k = ? AND c = ?", [1000, "ttl", 1, 2]),
        P("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?", [9, 9, "ts", 12345]),
        P("SELECT k, c, writetime(v) FROM {ks}.t WHERE k = ?", [9]),
        P("DELETE FROM {ks}.t WHERE k = ? AND c = ?", [1, 1]),
        P("DELETE v FROM {ks}.t WHERE k = ? AND c = ?", [1, 2]),
        P("SELECT * FROM {ks}.t", []),
        P("INSERT INTO {ks}.t (k, c, l) VALUES (?, ?, ?)", [5, 5, [1, 2, 3]]),
        P("UPDATE {ks}.t SET l[?] = ? WHERE k = ? AND c = ?", [1, 99, 5, 5]),
        P("SELECT l FROM {ks}.t WHERE k = ? AND c = ?", [5, 5]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [None]),
        P("SELECT * FROM {ks}.nope WHERE k = ?", [1]),
        P("SELECT nope FROM {ks}.t WHERE k = ?", [1]),
        P("SELEC * FROM {ks}.t", []),
        U("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [7, 7, "unset"], unset=[2]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [7]),
        U("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [7, 7, "u"], unset=[1]),
        U("UPDATE {ks}.t SET v = ?, l = ? WHERE k = ? AND c = ?", ["v8", [1], 8, 8], unset=[1]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [8]),
        B("logged", [("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [20, 1, "b1"]), ("UPDATE {ks}.t SET v = ? WHERE k = ? AND c = ?", ["b2", 20, 1]),
                     ("INSERT INTO {ks}.t (k, c, v) VALUES (?, ?, ?)", [20, 2, "b3"])]),
        P("SELECT * FROM {ks}.t WHERE k = ?", [20]),
        P("ALTER TABLE {ks}.t ADD x int", []),
        P("SELECT * FROM {ks}.t WHERE k = ?", [20]),
    ]))
    c.append(Case("keyspace_use", [
        "USE {ks}",
        "CREATE TABLE t (k int PRIMARY KEY, v int)",
        "INSERT INTO t (k, v) VALUES (1, 1)",
        "SELECT * FROM t",
        "SELECT * FROM {ks}.t",
        "USE system",
        "SELECT key FROM local",
        "SELECT * FROM t",
        "USE \"{ks}\"",
        "SELECT * FROM t",
        "USE nonexistent",
        "SELECT * FROM t",
        "CREATE TABLE \"Quoted\" (\"K\" int PRIMARY KEY, \"V\" text)",
        "INSERT INTO \"Quoted\" (\"K\", \"V\") VALUES (1, 'x')",
        "SELECT \"K\", \"V\" FROM \"Quoted\"",
        "SELECT k FROM \"Quoted\"",
        "ALTER TABLE \"Quoted\" ADD \"Extra\" int",
        "SELECT * FROM \"Quoted\"",
        "SELECT JSON * FROM \"Quoted\"",
        "DROP TABLE \"Quoted\"",
        "DROP TABLE Quoted",
    ]))
    c.append(Case("wide_partition", [
        "CREATE TABLE {ks}.t (p int, c int, v text, PRIMARY KEY (p, c))",
        *[B("unlogged", [f"INSERT INTO {{ks}}.t (p, c, v) VALUES (1, {i}, 'v{i}')" for i in range(base, base + 50)]) for base in range(0, 300, 50)],
        Q("SELECT count(*) FROM {ks}.t WHERE p = 1"),
        Q("SELECT c FROM {ks}.t WHERE p = 1", page=64),
        Q("SELECT c FROM {ks}.t WHERE p = 1 ORDER BY c DESC", page=100),
        Q("SELECT c FROM {ks}.t WHERE p = 1 AND c >= 100 AND c < 200", page=33),
        Q("SELECT c FROM {ks}.t WHERE p = 1 LIMIT 70", page=25),
        Q("SELECT c, v FROM {ks}.t WHERE p = 1 AND c IN (5, 250, 100, 299, 300)"),
        Q("SELECT min(c), max(c), count(v) FROM {ks}.t WHERE p = 1"),
        "DELETE FROM {ks}.t WHERE p = 1 AND c >= 10 AND c < 290",
        Q("SELECT c FROM {ks}.t WHERE p = 1", page=7),
        "TRUNCATE {ks}.t",
        "SELECT count(*) FROM {ks}.t",
    ]))
    c.append(Case("recreate_table", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)",
        "INSERT INTO {ks}.t (k, v) VALUES (1, 1)",
        "DROP TABLE {ks}.t",
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, v text)",
        "SELECT * FROM {ks}.t",
        "INSERT INTO {ks}.t (k, v) VALUES (1, 'one')",
        "SELECT * FROM {ks}.t",
        "DROP TABLE {ks}.t",
        "CREATE TABLE {ks}.t (k text, c int, PRIMARY KEY (k, c))",
        "SELECT * FROM {ks}.t",
        "DROP KEYSPACE {ks}",
        "SELECT * FROM {ks}.t",
        "CREATE KEYSPACE {ks} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "SELECT * FROM {ks}.t",
    ]))
    c.append(Case("udt_evolution", [
        "CREATE TYPE {ks}.p (a int, b text)",
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, x frozen<p>, l list<frozen<p>>)",
        "INSERT INTO {ks}.t (k, x, l) VALUES (1, {a: 1, b: 'one'}, [{a: 2}, {b: 'x'}])",
        "ALTER TYPE {ks}.p ADD c boolean",
        "SELECT * FROM {ks}.t WHERE k = 1",
        "INSERT INTO {ks}.t (k, x) VALUES (2, {a: 5, b: 'five', c: true})",
        "SELECT x FROM {ks}.t WHERE k = 2",
        "SELECT x.c FROM {ks}.t WHERE k IN (1, 2)",
        "ALTER TYPE {ks}.p RENAME b TO bb",
        "SELECT * FROM {ks}.t WHERE k = 2",
        "INSERT INTO {ks}.t (k, x) VALUES (3, {a: 1, b: 'old name'})",
        "INSERT INTO {ks}.t (k, x) VALUES (3, {a: 1, bb: 'new name'})",
        "SELECT * FROM {ks}.t WHERE k = 3",
    ]))
    c.append(Case("null_and_empty", [
        "CREATE TABLE {ks}.t (k text, c text, b blob, v int, PRIMARY KEY (k, c))",
        "INSERT INTO {ks}.t (k, c, v) VALUES ('', 'x', 1)",
        "INSERT INTO {ks}.t (k, c, v) VALUES ('a', '', 2)",
        "INSERT INTO {ks}.t (k, c, v) VALUES ('a', null, 3)",
        "INSERT INTO {ks}.t (k, c, v) VALUES (null, 'a', 3)",
        "SELECT * FROM {ks}.t WHERE k = 'a'",
        "SELECT * FROM {ks}.t WHERE k = ''",
        "SELECT * FROM {ks}.t WHERE k = 'a' AND c = ''",
        "SELECT * FROM {ks}.t WHERE k = null",
        "SELECT * FROM {ks}.t WHERE k = 'a' AND c = null",
        "SELECT * FROM {ks}.t WHERE k = 'a' AND c > ''",
        "SELECT * FROM {ks}.t WHERE v = null ALLOW FILTERING",
        "INSERT INTO {ks}.t (k, c, b) VALUES ('e', 'e', 0x)",
        "SELECT k, c, b, v FROM {ks}.t WHERE k = 'e'",
        "SELECT k, c, b FROM {ks}.t WHERE k = 'e' AND b = 0x ALLOW FILTERING",
        "UPDATE {ks}.t SET v = 1 WHERE k = 'a' AND c = null",
        "DELETE FROM {ks}.t WHERE k = 'a' AND c = null",
        "UPDATE {ks}.t SET v = null WHERE k = 'a' AND c = ''",
        "SELECT * FROM {ks}.t WHERE k = 'a'",
    ]))
    c.append(Case("index_collections", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, l list<int>, s set<text>, m map<text, int>, fl frozen<list<int>>, v int)",
        "CREATE INDEX il ON {ks}.t (l)",
        "CREATE INDEX is1 ON {ks}.t (s)",
        "CREATE INDEX imk ON {ks}.t (keys(m))",
        "CREATE INDEX imv ON {ks}.t (values(m))",
        "CREATE INDEX ime ON {ks}.t (entries(m))",
        "CREATE INDEX ifl ON {ks}.t (full(fl))",
        "CREATE INDEX iv ON {ks}.t (v)",
        "INSERT INTO {ks}.t (k, l, s, m, fl, v) VALUES (1, [1, 2], {'a', 'b'}, {'x': 1, 'y': 2}, [1], 10)",
        "INSERT INTO {ks}.t (k, l, s, m, fl, v) VALUES (2, [2, 3], {'b', 'c'}, {'y': 3}, [2], 20)",
        "INSERT INTO {ks}.t (k, l, s, m, fl, v) VALUES (3, [], {}, {}, [1], 10)",
        "SELECT k FROM {ks}.t WHERE l CONTAINS 2",
        "SELECT k FROM {ks}.t WHERE s CONTAINS 'b'",
        "SELECT k FROM {ks}.t WHERE m CONTAINS KEY 'y'",
        "SELECT k FROM {ks}.t WHERE m CONTAINS 3",
        "SELECT k FROM {ks}.t WHERE m['y'] = 2",
        "SELECT k FROM {ks}.t WHERE fl = [1]",
        "SELECT k FROM {ks}.t WHERE v = 10",
        "SELECT k FROM {ks}.t WHERE v = 10 AND s CONTAINS 'a'",
        "SELECT k FROM {ks}.t WHERE v = 10 AND s CONTAINS 'a' ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE l CONTAINS 1 AND l CONTAINS 2",
        "SELECT k FROM {ks}.t WHERE s CONTAINS 'zz'",
        "SELECT index_name, kind, options FROM system_schema.indexes WHERE keyspace_name = '{ks}'",
    ]))
    c.append(Case("select_misc", [
        "CREATE TABLE {ks}.t (k int, c int, v int, PRIMARY KEY (k, c))",
        *[f"INSERT INTO {{ks}}.t (k, c, v) VALUES ({k}, {c}, {k * 10 + c})" for k in range(1, 4) for c in range(1, 4)],
        "SELECT k, count(*) FROM {ks}.t GROUP BY k",
        "SELECT k, c, count(*), sum(v), max(v) FROM {ks}.t GROUP BY k, c",
        "SELECT k, sum(v) FROM {ks}.t WHERE k IN (1, 2) GROUP BY k",
        "SELECT k, c FROM {ks}.t GROUP BY k",
        "SELECT k, c FROM {ks}.t GROUP BY c",
        "SELECT k, v FROM {ks}.t GROUP BY k",
        "SELECT DISTINCT k FROM {ks}.t LIMIT 2",
        "SELECT DISTINCT k FROM {ks}.t WHERE k IN (3, 1)",
        "SELECT count(*) FROM {ks}.t LIMIT 1",
        "SELECT * FROM {ks}.t WHERE k = 1 ORDER BY c DESC LIMIT 2",
        "SELECT * FROM {ks}.t WHERE k = 1 AND c IN (3, 1) ORDER BY c DESC",
        "SELECT * FROM {ks}.t WHERE k = 1 PER PARTITION LIMIT 1",
        "SELECT * FROM {ks}.t PER PARTITION LIMIT 2 LIMIT 5",
        "SELECT * FROM {ks}.t WHERE k = 1 PER PARTITION LIMIT 0",
        "SELECT k, c AS cc FROM {ks}.t WHERE k = 1 AND c = 1",
        "SELECT (int) 5, 'lit', 1.5, true, 0x00, null FROM {ks}.t WHERE k = 1 AND c = 1",
        "SELECT 1 FROM {ks}.t WHERE k = 1",
        "SELECT k, c, v FROM {ks}.t WHERE k = 1 AND c IN (1, 1, 2)",
        "SELECT * FROM {ks}.t WHERE k IN (1, 2) AND c IN (2, 3)",
        "SELECT * FROM {ks}.t WHERE k = 1 AND c > 1 AND c < 1",
        "SELECT * FROM {ks}.t WHERE k = 1 AND c > 3",
        "SELECT * FROM {ks}.t WHERE k = 1 AND c = 1 AND c > 0",
        "SELECT * FROM {ks}.t WHERE k = 1 AND c > 1 AND c > 2",
        "SELECT * FROM {ks}.t WHERE k = 1 AND (c) = (1)",
        "SELECT * FROM {ks}.t WHERE (k) = (1)",
        "SELECT * FROM {ks}.t WHERE k = 1 AND c IN ()",
        "SELECT * FROM {ks}.t WHERE k IN (1) AND c > 1",
        "SELECT * FROM {ks}.t WHERE k = 1 AND v > 12 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE v IN (11, 22) ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE v > 20 AND v < 30 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE k >= 2 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE token(k) >= 0 ALLOW FILTERING",
        "SELECT * FROM {ks}.t WHERE c > 1 AND v < 30 ALLOW FILTERING",
    ]))
    return c


def describe_cases():
    mine = lambda r: [row for row in r if row[0] == "k_describe"]
    return [
        Case("describe", [
            "CREATE TYPE {ks}.addr (street text, zip int)",
            "CREATE TYPE {ks}.person (name text, home frozen<addr>, tags frozen<list<text>>, m map<text, frozen<addr>>)",
            "CREATE TABLE {ks}.k1 (k int PRIMARY KEY, v text)",
            "CREATE TABLE {ks}.k2 (a int, b int, v int, PRIMARY KEY ((a, b)))",
            "CREATE TABLE {ks}.k3 (a int, b int, c int, v int, PRIMARY KEY ((a, b), c)) WITH CLUSTERING ORDER BY (c ASC) AND default_time_to_live = 5 "
            "AND caching = {'keys': 'NONE', 'rows_per_partition': '10'} AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '50'} "
            "AND comment = 'it''s' AND gc_grace_seconds = 100 AND bloom_filter_fp_chance = 0.5 AND speculative_retry = '50ms'",
            "CREATE TABLE {ks}.t (p int, c text, d int, s int static, v map<text, frozen<addr>>, w list<int>, x set<uuid>, PRIMARY KEY (p, c, d)) WITH CLUSTERING ORDER BY (c DESC, d ASC) AND comment = 'hi'",
            "CREATE TABLE {ks}.\"Mixed\" (\"Id\" int PRIMARY KEY, \"Val\" text, cnt int)",
            "CREATE TABLE {ks}.cn (k int PRIMARY KEY, c counter)",
            "CREATE INDEX idx1 ON {ks}.t (w)",
            "CREATE INDEX idx2 ON {ks}.t (keys(v))",
            "CREATE INDEX idx3 ON {ks}.k1 (v)",
            "CREATE INDEX idx4 ON {ks}.t (values(v))",
            "CREATE TABLE {ks}.fz (k frozen<list<int>> PRIMARY KEY, f frozen<map<int, text>>, u frozen<addr>)",
            "CREATE INDEX idx5 ON {ks}.fz (f)",
            "DESCRIBE KEYSPACE {ks}",
            "DESCRIBE ONLY KEYSPACE {ks}",
            "DESCRIBE TABLE {ks}.t",
            "DESCRIBE TABLE {ks}.k1",
            "DESCRIBE TABLE {ks}.k3",
            "DESCRIBE TABLE {ks}.\"Mixed\"",
            "DESCRIBE TYPE {ks}.person",
            "DESCRIBE INDEX {ks}.idx1",
            "DESCRIBE {ks}.t",
            "DESCRIBE {ks}",
            Q("DESCRIBE TYPES", x=lambda r: [row for row in r if row[0] == "k_describe"]),
            Q("DESCRIBE TABLES", x=lambda r: [row for row in r if row[0] == "k_describe"]),
            "USE {ks}",
            "DESCRIBE TABLES",
            "DESCRIBE TYPES",
            "DESCRIBE KEYSPACE",
            "DESCRIBE TABLE k2",
            "DESCRIBE TYPE addr",
            "DESCRIBE INDEX idx3",
            "DESCRIBE t",
            "DESCRIBE idx1",
            "DESCRIBE addr",
            "DESCRIBE nothere",
            "DESCRIBE TABLE nothere",
            "DESCRIBE TYPE nothere",
            "DESCRIBE INDEX nothere",
            "DESCRIBE KEYSPACE nothere",
            "DESCRIBE TABLE nothere.t",
            "DESCRIBE FUNCTIONS",
            "DESCRIBE AGGREGATES",
            "DESCRIBE MATERIALIZED VIEW nothere",
            Q("DESCRIBE KEYSPACES", x=lambda r: [row for row in r if row[0].startswith("k_describe")]),
            Q("DESCRIBE SCHEMA", x=mine),
            Q("DESCRIBE FULL SCHEMA", x=mine),
            Q("DESCRIBE CLUSTER", x=lambda r: [row[1:3] for row in r]),
            Q("DESCRIBE CLUSTER", x=lambda r: [(len(r[0]), len(r[0][3]))]),
            "DESC KEYSPACES",
            "DESCRIBE",
            "DESCRIBE FOO BAR BAZ",
        ]),
    ]


def extra_cases():
    c = []
    c.append(Case("ttl_semantics", [
        "CREATE TABLE {ks}.t (k int, c int, a text, b text, l list<int>, s set<int>, m map<int,int>, st text static, PRIMARY KEY (k, c))",
        "INSERT INTO {ks}.t (k, c, a, b) VALUES (1, 1, 'a', 'b') USING TTL 2",
        "UPDATE {ks}.t USING TTL 100 SET a = 'a2' WHERE k = 1 AND c = 1",
        "INSERT INTO {ks}.t (k, c, a) VALUES (2, 1, 'x') USING TTL 100",
        "UPDATE {ks}.t USING TTL 2 SET b = 'temp' WHERE k = 2 AND c = 1",
        "INSERT INTO {ks}.t (k, c, l, s, m) VALUES (3, 1, [1, 2], {1, 2}, {1: 1}) USING TTL 2",
        "UPDATE {ks}.t USING TTL 100 SET l = l + [3], s = s + {3}, m = m + {3: 3} WHERE k = 3 AND c = 1",
        "INSERT INTO {ks}.t (k, c, st) VALUES (4, 1, 'stat') USING TTL 2",
        "INSERT INTO {ks}.t (k, c, a) VALUES (4, 2, 'stays')",
        Q("SELECT k, c, ttl(a) > 0, ttl(b) FROM {ks}.t WHERE k = 2", x=lambda r: [(r[0][0], r[0][1], r[0][2], r[0][3] is not None)]),
        "SELECT ttl(l) FROM {ks}.t WHERE k = 3",
        "SELECT writetime(s) FROM {ks}.t WHERE k = 3",
        W(3.5),
        "SELECT k, c, a, b FROM {ks}.t WHERE k IN (1, 2)",
        "SELECT k, c, l, s, m FROM {ks}.t WHERE k = 3",
        "SELECT k, c, st, a FROM {ks}.t WHERE k = 4",
        "SELECT * FROM {ks}.t",
        "INSERT INTO {ks}.t (k, c, a) VALUES (5, 1, 'x') IF NOT EXISTS USING TTL 2",
        W(2.5),
        "INSERT INTO {ks}.t (k, c, a) VALUES (5, 1, 'again') IF NOT EXISTS",
        "SELECT * FROM {ks}.t WHERE k = 5",
    ]))
    c.append(Case("static_lwt_batch", [
        "CREATE TABLE {ks}.t (p int, c int, s int static, v int, PRIMARY KEY (p, c))",
        "INSERT INTO {ks}.t (p, s) VALUES (1, 10) IF NOT EXISTS",
        "INSERT INTO {ks}.t (p, s) VALUES (1, 11) IF NOT EXISTS",
        "UPDATE {ks}.t SET s = 12 WHERE p = 1 IF s = 10",
        "UPDATE {ks}.t SET s = 13 WHERE p = 1 IF s = 10",
        "INSERT INTO {ks}.t (p, c, v) VALUES (1, 1, 1) IF NOT EXISTS",
        "UPDATE {ks}.t SET v = 2 WHERE p = 1 AND c = 1 IF s = 12",
        "UPDATE {ks}.t SET v = 3 WHERE p = 1 AND c = 1 IF s = 99 AND v = 2",
        B("logged", ["UPDATE {ks}.t SET s = 20 WHERE p = 1 IF s = 12", "INSERT INTO {ks}.t (p, c, v) VALUES (1, 2, 5) IF NOT EXISTS"]),
        B("logged", ["UPDATE {ks}.t SET s = 21 WHERE p = 1 IF s = 12", "INSERT INTO {ks}.t (p, c, v) VALUES (1, 3, 5) IF NOT EXISTS"]),
        "SELECT * FROM {ks}.t",
        "UPDATE {ks}.t SET v = 9 WHERE p = 1 AND c IN (1, 2) IF v = 2",
        "DELETE s FROM {ks}.t WHERE p = 1 IF s = 20",
        "DELETE FROM {ks}.t WHERE p = 1 AND c = 1 IF v = 2",
        "DELETE FROM {ks}.t WHERE p = 1 AND c = 2 IF v = 5",
        "SELECT * FROM {ks}.t",
        "DELETE FROM {ks}.t WHERE p = 1 IF EXISTS",
        "DELETE FROM {ks}.t WHERE p = 1 IF s = 5",
    ]))
    c.append(Case("collections_more", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, l list<text>, s set<int>, m map<text, list<int>>, fm frozen<map<text,int>>, ls list<frozen<set<int>>>)",
        "INSERT INTO {ks}.t (k, l) VALUES (1, ['a', 'b', 'c', 'b'])",
        "UPDATE {ks}.t SET l = l - ['b'] WHERE k = 1",
        "SELECT l FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET l[0] = null WHERE k = 1",
        "SELECT l FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET l = ['x'] + l WHERE k = 1",
        "UPDATE {ks}.t SET l = l + ['y', 'z'] WHERE k = 1",
        "UPDATE {ks}.t SET l = ['p', 'q'] + l WHERE k = 1",
        "SELECT l FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET l[10] = 'no' WHERE k = 1",
        "UPDATE {ks}.t SET l[-1] = 'no' WHERE k = 1",
        "DELETE l[1] FROM {ks}.t WHERE k = 1",
        "SELECT l, l[0], l[2] FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET s = s + {5, 3}, s = s + {4} WHERE k = 1",
        "UPDATE {ks}.t SET s = {9} , s = s + {8} WHERE k = 2",
        "SELECT s FROM {ks}.t WHERE k IN (1, 2)",
        "UPDATE {ks}.t SET m = {'a': [1]} WHERE k = 1",
        "UPDATE {ks}.t SET m['b'] = [2, 3] WHERE k = 1",
        "UPDATE {ks}.t SET m = m + {'c': []} WHERE k = 1",
        "SELECT m FROM {ks}.t WHERE k = 1",
        "SELECT m['a'], m['nope'] FROM {ks}.t WHERE k = 1",
        "UPDATE {ks}.t SET fm = {'x': 1} WHERE k = 3",
        "UPDATE {ks}.t SET fm['y'] = 2 WHERE k = 3",
        "UPDATE {ks}.t SET ls = ls + [{1, 2}, {3}] WHERE k = 4",
        "UPDATE {ks}.t SET ls[0] = {9} WHERE k = 4",
        "SELECT ls FROM {ks}.t WHERE k = 4",
        "INSERT INTO {ks}.t (k, l, s, m) VALUES (5, ['q'], {1}, {'z': [1]})",
        "INSERT INTO {ks}.t (k, l, s, m) VALUES (5, null, null, null)",
        "SELECT * FROM {ks}.t WHERE k = 5",
        "INSERT INTO {ks}.t (k, l, s, m) VALUES (5, ['q'], {1}, {'z': [1]})",
        "SELECT * FROM {ks}.t WHERE k = 5",
        "UPDATE {ks}.t SET l = [] WHERE k = 5",
        "SELECT * FROM {ks}.t WHERE k = 5",
        "DELETE FROM {ks}.t WHERE k = 5",
        "SELECT * FROM {ks}.t WHERE k = 5",
        "SELECT k, l FROM {ks}.t WHERE l = ['x'] ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE s = {1} ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE fm = {'x': 1} ALLOW FILTERING",
        "SELECT count(l), count(s) FROM {ks}.t",
        "SELECT max(l) FROM {ks}.t",
    ]))
    c.append(Case("counters_more", [
        "CREATE TABLE {ks}.c (k text, c int, a counter, b counter, PRIMARY KEY (k, c))",
        "UPDATE {ks}.c SET a = a + 1 WHERE k = 'x' AND c = 1",
        "UPDATE {ks}.c SET a = a + 1, b = b + 10 WHERE k = 'x' AND c IN (1, 2, 3)",
        "SELECT * FROM {ks}.c WHERE k = 'x'",
        "SELECT sum(a), sum(b), avg(a), min(b), max(b), count(a) FROM {ks}.c WHERE k = 'x'",
        "UPDATE {ks}.c SET a = a + ? WHERE k = 'x' AND c = 1",
        P("UPDATE {ks}.c SET a = a + ? WHERE k = ? AND c = ?", [-5, "x", 1]),
        P("UPDATE {ks}.c SET a = a - ? WHERE k = ? AND c = ?", [3, "x", 1]),
        "SELECT * FROM {ks}.c WHERE k = 'x' AND c = 1",
        "DELETE a FROM {ks}.c WHERE k = 'x' AND c = 2",
        "SELECT * FROM {ks}.c WHERE k = 'x' AND c = 2",
        "DELETE FROM {ks}.c WHERE k = 'x' AND c = 3",
        "SELECT * FROM {ks}.c WHERE k = 'x'",
        "INSERT INTO {ks}.c (k, c, a) VALUES ('y', 1, 1)",
        "UPDATE {ks}.c SET a = a + 1 WHERE k = 'x' AND c > 1",
        "UPDATE {ks}.c SET a = a + 1 WHERE k = 'x'",
        "UPDATE {ks}.c USING TIMESTAMP 5 SET a = a + 1 WHERE k = 'x' AND c = 1",
        "UPDATE {ks}.c SET a = a + 1 WHERE k = 'x' AND c = 1 IF a = 1",
        "UPDATE {ks}.c SET a = a + 1.5 WHERE k = 'x' AND c = 1",
        "SELECT a + 1, b * 2 FROM {ks}.c WHERE k = 'x' AND c = 1",
        "ALTER TABLE {ks}.c ADD d int",
        "ALTER TABLE {ks}.c ADD d counter",
        "ALTER TABLE {ks}.c ADD e counter",
        "TRUNCATE {ks}.c",
        "SELECT * FROM {ks}.c",
    ]))
    c.append(Case("multi_partition_batches", [
        "CREATE TABLE {ks}.a (k int, c int, v int, PRIMARY KEY (k, c))",
        "CREATE TABLE {ks}.b (k int PRIMARY KEY, v text)",
        B("logged", [f"INSERT INTO {{ks}}.a (k, c, v) VALUES ({k}, {c}, {k * c})" for k in range(1, 8) for c in range(1, 4)]
          + [f"INSERT INTO {{ks}}.b (k, v) VALUES ({k}, 'v{k}')" for k in range(1, 8)]),
        "SELECT count(*) FROM {ks}.a",
        "SELECT * FROM {ks}.b",
        B("unlogged", [f"UPDATE {{ks}}.a SET v = v + 100 WHERE k = {k} AND c = 1" for k in range(1, 8)]),
        "SELECT k, c, v FROM {ks}.a WHERE k IN (1, 2, 3, 4, 5, 6, 7) AND c = 1",
        B("logged", [f"DELETE FROM {{ks}}.a WHERE k = {k}" for k in range(1, 5)] + ["DELETE FROM {ks}.b WHERE k IN (5, 6)"]),
        "SELECT DISTINCT k FROM {ks}.a",
        "SELECT * FROM {ks}.b",
        B("logged", [("INSERT INTO {ks}.a (k, c, v) VALUES (?, ?, ?)", [20 + i, i, i]) for i in range(20)]),
        "SELECT count(*) FROM {ks}.a",
        Q("SELECT k, c FROM {ks}.a", page=9),
        Q("SELECT k, c FROM {ks}.a WHERE token(k) > -4000000000000000000 AND token(k) < 4000000000000000000", page=5),
    ]))
    c.append(Case("time_functions", [
        "CREATE TABLE {ks}.t (k int PRIMARY KEY, ts timestamp, d date, tm time, tu timeuuid, du duration)",
        "INSERT INTO {ks}.t (k, ts, d, tm, tu, du) VALUES (1, '2020-01-31 10:20:30.123+0000', '2020-01-31', '10:20:30.123456789', now(), 1mo2d3h)",
        "INSERT INTO {ks}.t (k, tu) VALUES (2, minTimeuuid('2020-01-31 10:20:30+0000'))",
        "INSERT INTO {ks}.t (k, tu) VALUES (3, maxTimeuuid('2020-01-31 10:20:30+0000'))",
        "SELECT k, toTimestamp(tu), toDate(tu), toUnixTimestamp(tu) FROM {ks}.t WHERE k IN (2, 3)",
        "SELECT ts + 1mo, ts - 1d, ts + 1h30m, d + 1mo, d - 1d FROM {ks}.t WHERE k = 1",
        "SELECT d + 5h FROM {ks}.t WHERE k = 1",
        "SELECT tm + 1h FROM {ks}.t WHERE k = 1",
        "SELECT ts, toDate(ts), toUnixTimestamp(ts), cast(ts as text), cast(d as text), cast(tm as text), cast(du as text) FROM {ks}.t WHERE k = 1",
        "SELECT cast(d as timestamp), cast(ts as date), cast(k as text), cast(k as double), cast(ts as bigint) FROM {ks}.t WHERE k = 1",
        "SELECT k FROM {ks}.t WHERE ts > '2020-01-01' ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE tu > minTimeuuid('2020-01-31 10:20:30+0000') ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE du = 1mo2d3h ALLOW FILTERING",
        "SELECT k FROM {ks}.t WHERE du > 1d ALLOW FILTERING",
        "SELECT k, du FROM {ks}.t WHERE k = 1",
        "INSERT INTO {ks}.t (k, du) VALUES (4, P1Y2M3DT4H5M6S)",
        "INSERT INTO {ks}.t (k, du) VALUES (5, P0001-02-03T04:05:06)",
        "INSERT INTO {ks}.t (k, du) VALUES (6, '1h30m')",
        "INSERT INTO {ks}.t (k, du) VALUES (8, -1d)",
        "SELECT k, du FROM {ks}.t WHERE k IN (4, 5, 6, 8)",
        "CREATE TABLE {ks}.bad (k duration PRIMARY KEY)",
        "CREATE TABLE {ks}.bad2 (k int, c duration, PRIMARY KEY (k, c))",
    ]))
    return c


def fuzz_cases():
    """Seeded random statement sequences (explicit, mostly increasing timestamps so the result is deterministic) over one table with every kind of
    column; the SELECTs in between compare the state Cassandra and Warp have reached."""
    import random
    cases = []
    for seed in range(24):
        rnd = random.Random(1000 + seed)
        order = " WITH CLUSTERING ORDER BY (c DESC)" if seed % 3 == 2 else ""
        steps = ["CREATE TABLE {ks}.t (p int, c int, v int, s text, l list<int>, st set<int>, m map<int, int>, sc int static, PRIMARY KEY (p, c))" + order]
        ts = [1000]

        def T():
            ts[0] += rnd.choice([10, 10, 10, 10, 3, 25, -4, -12])
            return ts[0]

        def pc():
            return rnd.randint(1, 3), rnd.randint(1, 4)

        for _ in range(130):
            p, cc = pc()
            k = rnd.random()
            if k < 0.16:
                cols = ["p", "c"]
                vals = [str(p), str(cc)]
                for name, gen in [("v", lambda: str(rnd.randint(0, 9))), ("s", lambda: "'s%d'" % rnd.randint(0, 4)),
                                  ("l", lambda: "[%s]" % ",".join(str(rnd.randint(0, 5)) for _ in range(rnd.randint(0, 3)))),
                                  ("st", lambda: "{%s}" % ",".join(str(x) for x in sorted({rnd.randint(0, 5) for _ in range(rnd.randint(0, 3))}))),
                                  ("m", lambda: "{%s}" % ",".join("%d:%d" % (k, rnd.randint(0, 9)) for k in sorted(rnd.sample(range(4), rnd.randint(0, 3))))),
                                  ("sc", lambda: str(rnd.randint(0, 9)))]:
                    if rnd.random() < 0.5:
                        cols.append(name)
                        vals.append(gen() if rnd.random() > 0.1 else "null")
                steps.append(f"INSERT INTO {{ks}}.t ({', '.join(cols)}) VALUES ({', '.join(vals)}) USING TIMESTAMP {T()}")
            elif k < 0.42:
                which = rnd.choice(["v", "s", "l+", "+l", "l[]", "l-", "st+", "st-", "m[]", "m+", "m-", "sc", "vnull", "lset", "stset", "mset"])
                sets = {
                    "v": f"v = {rnd.randint(0, 9)}", "s": f"s = 's{rnd.randint(0, 4)}'", "l+": f"l = l + [{rnd.randint(0, 5)}, {rnd.randint(0, 5)}]",
                    "+l": f"l = [{rnd.randint(0, 5)}] + l", "l[]": f"l[{rnd.randint(0, 2)}] = {rnd.randint(0, 5)}", "l-": f"l = l - [{rnd.randint(0, 5)}]",
                    "st+": f"st = st + {{{rnd.randint(0, 5)}, {rnd.randint(0, 5)}}}", "st-": f"st = st - {{{rnd.randint(0, 5)}}}",
                    "m[]": f"m[{rnd.randint(0, 3)}] = {rnd.randint(0, 9)}", "m+": f"m = m + {{{rnd.randint(0, 3)}: {rnd.randint(0, 9)}}}",
                    "m-": f"m = m - {{{rnd.randint(0, 3)}}}", "sc": f"sc = {rnd.randint(0, 9)}", "vnull": "v = null",
                    "lset": f"l = [{rnd.randint(0, 5)}, {rnd.randint(0, 5)}]", "stset": f"st = {{{rnd.randint(0, 5)}}}", "mset": f"m = {{{rnd.randint(0, 3)}: {rnd.randint(0, 9)}}}",
                }
                where = f"p = {p}" if which == "sc" else f"p = {p} AND c = {cc}"
                steps.append(f"UPDATE {{ks}}.t USING TIMESTAMP {T()} SET {sets[which]} WHERE {where}")
            elif k < 0.55:
                what = rnd.choice(["row", "row", "col", "elem", "partition", "range", "static"])
                t = T()
                if what == "row":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c = {cc}")
                elif what == "col":
                    steps.append(f"DELETE {rnd.choice(['v', 's', 'l', 'st', 'm'])} FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c = {cc}")
                elif what == "elem":
                    steps.append(f"DELETE m[{rnd.randint(0, 3)}] FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c = {cc}")
                elif what == "partition":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p}")
                elif what == "range":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c > {rnd.randint(0, 2)} AND c <= {rnd.randint(2, 4)}")
                else:
                    steps.append(f"DELETE sc FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p}")
            else:
                what = rnd.choice(["part", "part", "row", "rng", "desc", "lim", "cnt", "in", "filter", "all", "sc", "wt"])
                q = {
                    "part": f"SELECT * FROM {{ks}}.t WHERE p = {p}", "row": f"SELECT * FROM {{ks}}.t WHERE p = {p} AND c = {cc}",
                    "rng": f"SELECT c, v, l, st, m FROM {{ks}}.t WHERE p = {p} AND c >= {rnd.randint(1, 2)} AND c < {rnd.randint(2, 5)}",
                    "desc": f"SELECT c, s, m FROM {{ks}}.t WHERE p = {p} ORDER BY c DESC", "lim": f"SELECT * FROM {{ks}}.t WHERE p = {p} LIMIT {rnd.randint(1, 3)}",
                    "cnt": f"SELECT count(*), count(v), max(c), sum(v) FROM {{ks}}.t WHERE p = {p}", "in": f"SELECT * FROM {{ks}}.t WHERE p IN (3, 1, 2) AND c IN (2, 4)",
                    "filter": f"SELECT p, c, v FROM {{ks}}.t WHERE v > {rnd.randint(0, 8)} ALLOW FILTERING", "all": "SELECT * FROM {ks}.t",
                    "sc": f"SELECT p, sc FROM {{ks}}.t WHERE p = {p} LIMIT 1", "wt": f"SELECT c, writetime(v), writetime(s) FROM {{ks}}.t WHERE p = {p}",
                }[what]
                steps.append(q)
        steps.append("SELECT * FROM {ks}.t")
        steps.append("SELECT p, count(*) FROM {ks}.t GROUP BY p")
        cases.append(Case(f"fuzz_{seed}", steps))
    # two clustering columns (int, text) with prefix / slice deletes, LWT and IN reads
    for seed in range(10):
        rnd = random.Random(2000 + seed)
        steps = ["CREATE TABLE {ks}.t (p int, c int, d text, v int, w text, st set<text>, PRIMARY KEY (p, c, d)) WITH CLUSTERING ORDER BY (c ASC, d DESC)"]
        ts = [5000]

        def T():
            ts[0] += rnd.choice([10, 10, 10, 4, 22, -6])
            return ts[0]

        def key():
            return rnd.randint(1, 2), rnd.randint(1, 3), rnd.choice(["a", "b", "c", ""])

        for _ in range(110):
            p, cc, d = key()
            k = rnd.random()
            if k < 0.25:
                steps.append(f"INSERT INTO {{ks}}.t (p, c, d, v, w) VALUES ({p}, {cc}, '{d}', {rnd.randint(0, 9)}, 'w{rnd.randint(0, 3)}') USING TIMESTAMP {T()}")
            elif k < 0.40:
                which = rnd.choice(["v = %d" % rnd.randint(0, 9), "w = null", "st = st + {'%s'}" % rnd.choice("xyz"), "v = null"])
                steps.append(f"UPDATE {{ks}}.t USING TIMESTAMP {T()} SET {which} WHERE p = {p} AND c = {cc} AND d = '{d}'")
            elif k < 0.55:
                what = rnd.choice(["row", "prefix", "part", "slice", "sliced", "col"])
                t = T()
                if what == "row":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c = {cc} AND d = '{d}'")
                elif what == "prefix":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c = {cc}")
                elif what == "part":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p}")
                elif what == "slice":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c >= {rnd.randint(1, 2)} AND c < {rnd.randint(2, 4)}")
                elif what == "sliced":
                    steps.append(f"DELETE FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c = {cc} AND d > 'a'")
                else:
                    steps.append(f"DELETE v FROM {{ks}}.t USING TIMESTAMP {t} WHERE p = {p} AND c = {cc} AND d = '{d}'")
            elif k < 0.62:
                steps.append(f"UPDATE {{ks}}.t SET v = {rnd.randint(0, 9)} WHERE p = {p} AND c = {cc} AND d = '{d}' IF v = {rnd.randint(0, 9)}")
            elif k < 0.66:
                steps.append(f"INSERT INTO {{ks}}.t (p, c, d, v) VALUES ({p}, {cc}, '{d}', 1) IF NOT EXISTS")
            else:
                q = rnd.choice([
                    f"SELECT * FROM {{ks}}.t WHERE p = {p}", f"SELECT c, d, v FROM {{ks}}.t WHERE p = {p} AND c = {cc}",
                    f"SELECT * FROM {{ks}}.t WHERE p = {p} AND c IN (1, 3) AND d IN ('a', 'c')", f"SELECT * FROM {{ks}}.t WHERE p = {p} AND c > {cc}",
                    f"SELECT * FROM {{ks}}.t WHERE p = {p} AND c = {cc} AND d >= 'b'", f"SELECT * FROM {{ks}}.t WHERE p = {p} ORDER BY c DESC",
                    f"SELECT * FROM {{ks}}.t WHERE p = {p} AND (c, d) > ({cc}, 'b')", f"SELECT count(*) FROM {{ks}}.t WHERE p = {p}",
                    "SELECT * FROM {ks}.t", f"SELECT c, d, writetime(v) FROM {{ks}}.t WHERE p = {p} LIMIT 2"])
                steps.append(q)
        steps.append("SELECT * FROM {ks}.t")
        cases.append(Case(f"fuzz_ck_{seed}", steps))
    return cases


def cases():
    out = []
    for f in (ddl_cases, type_cases, dml_cases, counter_cases, lwt_cases, batch_cases, paging_cases, system_cases, error_cases, order_cases,
              more_cases, describe_cases, extra_cases, fuzz_cases):
        out.extend(f())
    return out
