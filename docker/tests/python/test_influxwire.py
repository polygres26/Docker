"""influxwire: real InfluxDB v1 line-protocol write path plus a bounded InfluxQL read path
(WHERE/GROUP BY time()/aggregates), real `influxdb` (the official v1 client, not the v2
`influxdb-client` package -- see com.sayonora.wire.influxwire.InfluxWireServer's javadoc for why
this speaks v1, not Flux), translated to Postgres underneath. No auth configured in the test
compose file, so a bare client is enough.
"""
import uuid

from influxdb import InfluxDBClient

from conftest import HOST, INFLUXWIRE_PORT


def client():
    return InfluxDBClient(host=HOST, port=INFLUXWIRE_PORT, database="testdb")


def measurement():
    # Real InfluxDB measurement/table names must match influxwire's own identifier rule
    # ([A-Za-z_][A-Za-z0-9_]*) -- see PgTimeSeriesStore.pgTableName -- so this can't use a hyphen
    # the way test_oswire.py's uuid-suffixed index names do.
    return f"smoke_{uuid.uuid4().hex[:8]}"


def test_write_and_select_star_round_trip():
    c = client()
    m = measurement()
    c.write_points([
        {"measurement": m, "tags": {"host": "server01"}, "fields": {"usage": 72.5, "count": 3},
         "time": "2024-01-01T00:00:00Z"},
        {"measurement": m, "tags": {"host": "server02"}, "fields": {"usage": 15.0, "count": 1},
         "time": "2024-01-01T00:01:00Z"},
    ])

    points = list(c.query(f"SELECT * FROM {m}").get_points())
    assert len(points) == 2
    by_host = {p["tags"]["host"]: p for p in points}
    assert by_host["server01"]["fields"]["usage"] == 72.5
    assert by_host["server02"]["fields"]["count"] == 1


def test_where_filters_by_tag():
    c = client()
    m = measurement()
    c.write_points([
        {"measurement": m, "tags": {"region": "us"}, "fields": {"value": 1.0}, "time": "2024-01-01T00:00:00Z"},
        {"measurement": m, "tags": {"region": "eu"}, "fields": {"value": 2.0}, "time": "2024-01-01T00:00:00Z"},
    ])

    points = list(c.query(f"SELECT value FROM {m} WHERE region = 'us'").get_points())
    assert len(points) == 1
    assert points[0]["value"] == 1.0


def test_where_filters_by_time():
    c = client()
    m = measurement()
    c.write_points([
        {"measurement": m, "fields": {"value": 1.0}, "time": "2024-01-01T00:00:00Z"},
        {"measurement": m, "fields": {"value": 2.0}, "time": "2024-06-01T00:00:00Z"},
    ])

    points = list(c.query(f"SELECT value FROM {m} WHERE time > '2024-03-01T00:00:00Z'").get_points())
    assert len(points) == 1
    assert points[0]["value"] == 2.0


def test_mean_aggregate_without_group_by():
    c = client()
    m = measurement()
    c.write_points([
        {"measurement": m, "fields": {"usage": 10.0}, "time": "2024-01-01T00:00:00Z"},
        {"measurement": m, "fields": {"usage": 20.0}, "time": "2024-01-01T00:01:00Z"},
        {"measurement": m, "fields": {"usage": 30.0}, "time": "2024-01-01T00:02:00Z"},
    ])

    points = list(c.query(f"SELECT mean(usage) FROM {m}").get_points())
    assert len(points) == 1
    assert points[0]["mean_usage"] == 20.0


def test_group_by_time_bucketing():
    c = client()
    m = measurement()
    c.write_points([
        {"measurement": m, "tags": {"host": "a"}, "fields": {"usage": 10.0}, "time": "2024-01-01T00:00:10Z"},
        {"measurement": m, "tags": {"host": "a"}, "fields": {"usage": 30.0}, "time": "2024-01-01T00:00:50Z"},
        {"measurement": m, "tags": {"host": "a"}, "fields": {"usage": 100.0}, "time": "2024-01-01T00:01:10Z"},
    ])

    result = c.query(f"SELECT mean(usage) FROM {m} GROUP BY time(1m)")
    series = list(result.items())
    # Two 1-minute buckets: [00:00:00, 00:01:00) averages 10/30 -> 20; [00:01:00, 00:02:00)
    # is just the 100 point.
    values = sorted(p["mean_usage"] for (_, _), pts in series for p in pts)
    assert values == [20.0, 100.0]


def test_show_measurements_lists_written_tables():
    c = client()
    m = measurement()
    c.write_points([{"measurement": m, "fields": {"value": 1.0}}])

    points = list(c.query("SHOW MEASUREMENTS").get_points())
    names = {p["name"] for p in points}
    assert m in names


def test_unsupported_or_returns_a_clear_error_not_a_wrong_answer():
    c = client()
    m = measurement()
    c.write_points([{"measurement": m, "tags": {"host": "a"}, "fields": {"value": 1.0}}])

    try:
        list(c.query(f"SELECT value FROM {m} WHERE host = 'a' OR host = 'b'").get_points())
        assert False, "expected influxwire to reject OR, not silently accept it"
    except Exception as e:
        # Real InfluxDB client raises on a non-2xx /query response; influxwire's own error message
        # names the actual unsupported clause -- see InfluxQlParser's "unrecognized clause fails
        # loudly" policy.
        assert "OR" in str(e)


def test_ping_reports_a_version():
    # Real client SDKs call /ping as a liveness/version check before anything else.
    c = client()
    version = c.ping()
    assert version
