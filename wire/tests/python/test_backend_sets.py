"""Backend sets + enabled stores admin API, against a real Warp and real Postgres containers.

Every backend belongs to a backend set; a Postgres backend can be asked to HOST protocol stores
(influxdb, mongodb, sqs, neo4j, opensearch, dynamodb) -- enabling creates the schema in that
Postgres. Tables are checked by querying information_schema on the Postgres itself.
"""
import json
import os
import time

import psycopg2
import pytest
import requests

from mcp_support import ADMIN_TOKEN
from warp_test_support import RealPostgres, WarpProcess, isolated_ports

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


# The protocol frontends serve the set holding `default` unless told otherwise (WARP_<PROTO>_SET).
FRONTEND_SETS = {"WARP_TRUSTED_BACKEND_HOSTS": "localhost", "WARP_DYNAMOWIRE_SET": "analytics",
                 "WARP_SQSWIRE_SET": "analytics", "WARP_BOLTWIRE_SET": "analytics",
                 "WARP_INFLUXWIRE_SET": "analytics", "WARP_MONGOWIRE_SET": "analytics",
                 "WARP_OSWIRE_SET": "analytics"}


def pg_url(pg):
    return f"jdbc:postgresql://localhost:{pg.port}/postgres"


def tables(pg):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        cur = c.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        return {r[0] for r in cur.fetchall()}


def marker_rows(pg):
    with psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        cur = c.cursor()
        cur.execute("SELECT store FROM warp_enabled_stores")
        return {r[0] for r in cur.fetchall()}


class Api:
    def __init__(self, warp):
        self.warp = warp

    def call(self, method, path, body=None, expect=None):
        resp = requests.request(method, f"http://localhost:{self.warp.metrics_port}{path}", timeout=30,
                                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, json=body)
        if expect is not None:
            assert resp.status_code == expect, (resp.status_code, resp.text)
        return resp

    def sets(self, health=False):
        return self.call("GET", "/api/backend-sets" + ("?health=true" if health else ""), expect=200).json()

    def find(self, backend):
        for s in self.sets()["sets"]:
            for b in s["backends"]:
                if b["name"] == backend:
                    return s, b
        return None, None


@pytest.fixture(scope="module")
def infra():
    pgs = [RealPostgres() for _ in range(3)]
    yield pgs
    for p in pgs:
        p.close()


@pytest.fixture(scope="module")
def warp(infra):
    # no WARP_BACKENDS: the implicit `default` backend (the config Postgres) is migrated into the
    # "default" set without any rewrite
    proc = WarpProcess(infra[0], "WARP_SQSWIRE_PORT", frontend_name="sqswire",
                       extra_env={**FRONTEND_SETS, **isolated_ports("WARP_SQSWIRE_PORT")})
    yield proc
    proc.close()


@pytest.fixture(scope="module")
def api(warp):
    return Api(warp)


def test_existing_config_migrates_into_the_default_set(api):
    data = api.sets(health=True)
    assert [s["name"] for s in data["sets"]] == ["default"]
    default_set = data["sets"][0]
    assert default_set["isDefaultSet"] is True
    b = default_set["backends"][0]
    assert b["name"] == "default" and b["type"] == "postgres" and b["enabledStores"] == []
    assert b["canHostStores"] is True and b["health"]["ok"] is True
    assert "password" not in b and "postgres:postgres" not in json.dumps(b)  # no credentials leak
    assert data["maxBackends"] == 3
    assert {s["id"] for s in data["stores"]} == {"influxdb", "mongodb", "sqs", "neo4j", "opensearch", "dynamodb"}


def test_backend_without_a_set_is_rejected(api, infra):
    r = api.call("POST", "/api/backends", {"name": "x", "url": pg_url(infra[1]), "user": "postgres",
                                            "password": "postgres"})
    assert r.status_code == 400 and "set" in r.json()["error"]
    r = api.call("POST", "/api/backend-sets/nosuchset/backends",
                 {"name": "x", "url": pg_url(infra[1]), "user": "postgres", "password": "postgres"})
    assert r.status_code == 404 and "does not exist" in r.json()["error"]


def test_create_set_and_duplicates(api):
    r = api.call("POST", "/api/backend-sets", {"name": "analytics", "description": "Analytics fleet"}, expect=201)
    assert r.json()["set"]["description"] == "Analytics fleet"
    assert api.call("POST", "/api/backend-sets", {"name": "analytics"}).status_code == 409
    assert api.call("POST", "/api/backend-sets", {"name": "bad name!"}).status_code == 400
    assert api.call("POST", "/api/backend-sets", {"name": "default"}).status_code == 409


def test_add_postgres_backend_with_stores_creates_the_schema_in_that_postgres(api, infra):
    default_pg, pg2 = infra[0], infra[1]
    before = tables(pg2)
    assert "_dynamo_tables" not in before
    r = api.call("POST", "/api/backend-sets/analytics/backends", {
        "name": "pg2", "url": pg_url(pg2), "user": "postgres", "password": "postgres",
        "description": "second postgres", "enabledStores": ["dynamodb", "sqs", "neo4j", "mongodb"]}, expect=201)
    body = r.json()
    assert body["ok"] is True and body["backend"]["enabledStores"] == ["dynamodb", "sqs", "neo4j", "mongodb"]
    assert "password" not in body["backend"]
    # the frontends' hosts moved from the legacy default to pg2 -> data on default is NOT moved
    stores = {r_["store"] for r_ in body["rebalanceRequired"]}
    assert {"dynamodb", "sqs", "neo4j"} <= stores
    # real tables, in pg2 -- and not in the default Postgres
    t = tables(pg2)
    assert {"_dynamo_tables", "sqs_queues_catalog", "warp_graph_nodes", "warp_graph_edges",
            "warp_enabled_stores"} <= t
    assert marker_rows(pg2) == {"dynamodb", "sqs", "neo4j", "mongodb"}
    # (default keeps the catalogs the frontends created there at startup, before any store was enabled:
    # a store enabled nowhere keeps its legacy home on `default`)
    s, b = api.find("pg2")
    assert s["name"] == "analytics" and b["description"] == "second postgres"
    assert s["stores"]["dynamodb"]["hosts"] == ["pg2"] and s["stores"]["dynamodb"]["sharded"] is False
    assert s["stores"]["dynamodb"]["servedFromThisSet"] is True


def test_hot_reload_reaches_the_registry_and_other_instances(api, warp, infra):
    # the legacy read endpoint sees the new set membership + enabled stores once reloaded
    deadline = time.time() + 15
    seen = None
    while time.time() < deadline:
        backends = api.call("GET", "/api/backends", expect=200).json()
        seen = next((b for b in backends if b["name"] == "pg2"), None)
        if seen and seen.get("enabledStores"):
            break
        time.sleep(0.2)
    assert seen and seen["backendSet"] == "analytics" and "dynamodb" in seen["enabledStores"], seen
    # a SECOND Warp instance on the same config store picks the change up without a restart
    second = WarpProcess(infra[0], "WARP_SQSWIRE_PORT", frontend_name="sqswire-2",
                         extra_env={**FRONTEND_SETS, **isolated_ports("WARP_SQSWIRE_PORT")})
    try:
        other = Api(second)
        # change on the FIRST instance after the second one is up
        api.call("PATCH", "/api/backend-sets/analytics/backends/pg2", {"description": "renamed by instance 1"},
                 expect=200)
        deadline = time.time() + 15
        got = None
        while time.time() < deadline:
            backends = other.call("GET", "/api/backends", expect=200).json()
            got = next((b for b in backends if b["name"] == "pg2"), None)
            desc = got and got.get("description")
            if desc == "renamed by instance 1":
                break
            time.sleep(0.2)
        assert got and got["description"] == "renamed by instance 1", got
        assert got["enabledStores"] == ["dynamodb", "sqs", "neo4j", "mongodb"]
    finally:
        second.close()


def test_stores_only_on_postgres_backends(api):
    r = api.call("POST", "/api/backend-sets/analytics/backends", {
        "name": "legacy-mysql", "url": "jdbc:mysql://localhost:3306/db", "user": "u", "password": "p",
        "enabledStores": ["sqs"]})
    assert r.status_code == 400 and "Postgres" in r.json()["error"]
    _, b = api.find("legacy-mysql")
    assert b is None
    # unknown store names are rejected with the valid list
    r = api.call("PATCH", "/api/backend-sets/analytics/backends/pg2", {"enabledStores": ["redis"]})
    assert r.status_code == 400 and "influxdb" in r.json()["error"]
    # raw config route cannot bypass the rule either
    cfg = api.call("GET", "/api/config", expect=200).json()
    r = api.call("PUT", "/api/config", {"backends": (cfg["backends"] or "") + ";my=jdbc:mysql://localhost:3306/db|u|p",
                                        "backendStores": "my=sqs"})
    assert r.status_code == 400


def test_neo4j_only_once_per_set_and_license_cap(api, infra):
    pg3 = infra[2]
    r = api.call("POST", "/api/backend-sets/analytics/backends", {
        "name": "pg3", "url": pg_url(pg3), "user": "postgres", "password": "postgres",
        "enabledStores": ["neo4j"]})
    assert r.status_code == 400 and "ONE backend per backend set" in r.json()["error"]
    assert "warp_graph_nodes" not in tables(pg3), "a rejected request must leave no schema behind"
    # without neo4j the same backend is fine, and shares dynamodb with pg2 -> sharded set
    r = api.call("POST", "/api/backend-sets/analytics/backends", {
        "name": "pg3", "url": pg_url(pg3), "user": "postgres", "password": "postgres",
        "enabledStores": ["dynamodb", "influxdb"]}, expect=201)
    s, _ = api.find("pg3")
    assert s["stores"]["dynamodb"]["hosts"] == ["pg2", "pg3"] and s["stores"]["dynamodb"]["sharded"] is True
    assert {"dynamodb"} <= {x["store"] for x in r.json()["rebalanceRequired"]}
    assert "_dynamo_tables" in tables(pg3)
    # Developer license: default + pg2 + pg3 is the cap
    r = api.call("POST", "/api/backend-sets/analytics/backends", {
        "name": "pg4", "url": pg_url(pg3), "user": "postgres", "password": "postgres"})
    assert r.status_code == 400 and "capped" in r.json()["error"]


def test_disable_a_store_keeps_its_tables(api, infra):
    pg2 = infra[1]
    api.call("PATCH", "/api/backend-sets/analytics/backends/pg2", {"enabledStores": ["dynamodb"]}, expect=200)
    _, b = api.find("pg2")
    assert b["enabledStores"] == ["dynamodb"]
    assert {"sqs_queues_catalog", "warp_graph_nodes"} <= tables(pg2), "disabling must never drop data"
    # editing keeps the stored password when none is sent
    assert api.call("POST", "/api/backend-sets/analytics/backends/pg2/test", expect=200).json()["ok"] is True


def test_delete_rules(api):
    assert api.call("DELETE", "/api/backend-sets/analytics").status_code == 409          # not empty
    assert api.call("DELETE", "/api/backend-sets/default").status_code == 409            # holds default
    assert api.call("DELETE", "/api/backend-sets/analytics/backends/default").status_code == 404  # wrong set
    assert api.call("DELETE", "/api/backend-sets/default/backends/default").status_code == 409
    assert api.call("DELETE", "/api/backend-sets/nope").status_code == 404
    api.call("DELETE", "/api/backend-sets/analytics/backends/pg3", expect=200)
    r = api.call("DELETE", "/api/backend-sets/analytics/backends/pg2", expect=200)
    assert any("NOT deleted" in w for w in r.json()["warnings"])
    api.call("DELETE", "/api/backend-sets/analytics", expect=200)
    assert [s["name"] for s in api.sets()["sets"]] == ["default"]


def test_env_based_backends_groups_and_descriptions_map_onto_sets(infra):
    # a FRESH config Postgres: an existing warp_config row wins over env, so the env is only
    # bootstrapped into a config store that has none
    default_pg, pg2 = RealPostgres(), infra[1]
    proc = WarpProcess(default_pg, "WARP_SQSWIRE_PORT", frontend_name="sqswire-env", extra_env={
        **isolated_ports("WARP_SQSWIRE_PORT"),
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": f"default={pg_url(default_pg)}|postgres|postgres;pg2={pg_url(pg2)}|postgres|postgres",
        "WARP_BACKEND_GROUPS": "sales:plain=pg2",
        "WARP_BACKEND_GROUP_DESCRIPTIONS": json.dumps({"sales": "Sales team"}),
        "WARP_BACKEND_DESCRIPTIONS": json.dumps({"pg2": "orders"}),
        "WARP_BACKEND_STORES": "pg2=mongodb"})
    try:
        api = Api(proc)
        data = api.sets()
        names = [s["name"] for s in data["sets"]]
        by = {s["name"]: s for s in data["sets"]}
        assert names == ["default", "sales"], names
        assert [b["name"] for b in by["default"]["backends"]] == ["default"]
        assert by["sales"]["description"] == "Sales team"
        assert by["sales"]["backends"][0]["name"] == "pg2"
        assert by["sales"]["backends"][0]["enabledStores"] == ["mongodb"]
        assert by["sales"]["backends"][0]["description"] == "orders"
    finally:
        proc.close()
        default_pg.close()
