"""list_backends / describe_backend for Kafka and Cassandra backends (real containers) registered
next to the default Postgres, and WARP_MCP_READ_ONLY on the Warp-emulated stores. Kafka/Cassandra
have no MCP data tools (they are federated SQL sources); they are reported with type, description
and live contents (topics; keyspaces/tables). Real Warp subprocess; no mocks.
"""
import json
import os

import pytest

from warp_test_support import WarpProcess, RealPostgres, RealKafka, RealCassandra, free_port
from mcp_support import ADMIN_TOKEN, call, call_json, create_endpoint, tool_names

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def infra():
    pg, kafka, cass = RealPostgres(), RealKafka(), RealCassandra()
    kafka.create_topic("orders-topic")
    kafka.create_topic("payments-topic")
    assert cass.cql("CREATE KEYSPACE shop WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
                    ).returncode == 0
    assert cass.cql("CREATE TABLE shop.orders (id int PRIMARY KEY, item text)").returncode == 0
    assert cass.cql("CREATE TABLE shop.customers (id int PRIMARY KEY, name text)").returncode == 0
    yield {"pg": pg, "kafka": kafka, "cass": cass}
    cass.close()
    kafka.close()
    pg.close()


@pytest.fixture(scope="module")
def warp(infra):
    proc = WarpProcess(infra["pg"], "WARP_MCP_PORT", frontend_name="mcp", extra_env={
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": (f"default=jdbc:postgresql://localhost:{infra['pg'].port}/postgres|postgres|postgres;"
                          f"events=kafka://{infra['kafka'].bootstrap_servers}/orders-topic?format=json&fields=id,item;"
                          f"catalog=cassandra://{infra['cass'].contact_point}/shop.orders?localDc=datacenter1&allowFullScan=true"),
        "WARP_BACKEND_DESCRIPTIONS": json.dumps({"events": "Order events stream (Kafka)",
                                                 "catalog": "Shop catalogue (Cassandra)"}),
        "WARP_MCP_EMULATED_STORES": "all", "WARP_MCP_READ_ONLY": "true",
        "WARP_DYNAMOWIRE_PORT": free_port(), "WARP_INFLUXWIRE_PORT": free_port(), "WARP_MONGOWIRE_PORT": free_port(),
    })
    yield proc
    proc.close()


def test_kafka_and_cassandra_are_listed_with_type_and_description(warp):
    out = call_json(warp.frontend_port, "list_backends")
    by = {b["name"]: b for b in out["backends"]}
    assert by["events"]["type"] == "kafka" and by["events"]["description"] == "Order events stream (Kafka)"
    assert by["catalog"]["type"] == "cassandra" and by["catalog"]["description"] == "Shop catalogue (Cassandra)"
    assert by["events"]["tools"] == [] and by["catalog"]["tools"] == []      # no data tools; federated SQL only


def test_describe_kafka_lists_topics(warp):
    d = call_json(warp.frontend_port, "describe_backend", {"backend": "events"})
    assert d["contents"]["topics"] == ["orders-topic", "payments-topic"]
    assert "orders_topic" in d["contents"]["declaredTables"] or d["contents"]["declaredTables"]


def test_describe_cassandra_lists_keyspaces_and_tables(warp):
    d = call_json(warp.frontend_port, "describe_backend", {"backend": "catalog"})
    assert d["contents"]["keyspaces"] == {"shop": ["customers", "orders"]}


def test_kafka_backend_has_no_data_tools_and_says_so(warp):
    ep = create_endpoint(warp, "events-only", "db:events")
    names = tool_names(warp.frontend_port, ep["path"], ep["token"])
    assert names == {"list_backends", "describe_backend"}
    assert call_json(warp.frontend_port, "describe_backend", path=ep["path"], token=ep["token"])["type"] == "kafka"


def test_read_only_hides_and_refuses_write_tools(warp):
    p = warp.frontend_port
    names = tool_names(p)
    assert {"get_item", "query_table", "find", "query_influxql", "list_tables"} <= names
    assert not ({"put_item", "update_item", "delete_item", "create_table", "insert-many", "update-many",
                 "delete-many", "write_line_protocol"} & names)
    for tool, args in [("put_item", {"tableName": "t", "item": {"id": "1"}}),
                       ("insert-many", {"database": "d", "collection": "c", "documents": [{"a": 1}]}),
                       ("write_line_protocol", {"db": "d", "data": "m v=1"})]:
        assert "Unknown tool" in call(p, tool, args, expect_error=True)[0]
    # reads still work
    assert "TableNames" in call_json(p, "list_tables", {"backend": "default.dynamodb"})
