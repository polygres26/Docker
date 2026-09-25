"""User-created MCP endpoints with optional expiry, against a real Warp subprocess: create through
the admin API, use over HTTP JSON-RPC (/e/<id> + bearer token), expiry enforced on every request
with server time, revoke/extend/make-permanent hot-reload, scope narrowing. Real Postgres backends,
real waits; no mocks.
"""
import datetime
import json
import os
import time

import psycopg2
import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres
from mcp_support import ADMIN_TOKEN, admin, call, call_json, create_endpoint, tool_names

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def infra():
    default_pg, pg2 = RealPostgres(), RealPostgres()
    with psycopg2.connect(host="localhost", port=pg2.port, user="postgres", password="postgres",
                          dbname="postgres") as c:
        c.autocommit = True
        c.cursor().execute("CREATE TABLE pg2_things(id int primary key)")
    yield {"default_pg": default_pg, "pg2": pg2}
    pg2.close()
    default_pg.close()


@pytest.fixture(scope="module")
def warp(infra):
    proc = WarpProcess(infra["default_pg"], "WARP_MCP_PORT", frontend_name="mcp", extra_env={
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": (f"default=jdbc:postgresql://localhost:{infra['default_pg'].port}/postgres|postgres|postgres;"
                          f"pg2=jdbc:postgresql://localhost:{infra['pg2'].port}/postgres|postgres|postgres"),
        "WARP_BACKEND_GROUPS": "finance:plain=pg2",
        "WARP_MCP_REQUIRE_ENDPOINT": "true",
    })
    yield proc
    proc.close()


def status(warp, ep, token=None, path=None):
    r = requests.post(f"http://localhost:{warp.frontend_port}{path or ep['path']}", timeout=10,
                      headers={"Authorization": f"Bearer {token or ep['token']}"},
                      json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    return r.status_code


def wait_status(warp, ep, want, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if status(warp, ep) == want:
            return True
        time.sleep(0.1)
    return False


# ---------------------------------------------------------------------------

def test_listener_requires_an_endpoint_when_configured(warp):
    r = requests.post(f"http://localhost:{warp.frontend_port}/", timeout=10,
                      json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert r.status_code == 401


def test_endpoint_with_short_expiry_works_then_is_refused(warp):
    ep = create_endpoint(warp, "short-lived", "db:pg2", ttlSeconds=6)
    assert ep["expiresAt"] and ep["token"].startswith("wmcp_") and ep["path"] == f"/e/{ep['id']}"
    assert status(warp, ep) == 200
    assert "execute_sql" in tool_names(warp.frontend_port, ep["path"], ep["token"])
    rows = json.loads(call(warp.frontend_port, "execute_sql", {"sql": "SELECT count(*) AS n FROM pg2_things"},
                           path=ep["path"], token=ep["token"])[0])
    assert rows == [{"n": 0}]
    assert wait_status(warp, ep, 401, timeout=15), "endpoint kept working after its expiry"
    # tools/call is refused too, with a generic (non-leaky) message and HTTP 401
    r = requests.post(f"http://localhost:{warp.frontend_port}{ep['path']}", timeout=10,
                      headers={"Authorization": f"Bearer {ep['token']}"},
                      json={"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                            "params": {"name": "execute_sql", "arguments": {"sql": "select 1"}}})
    assert r.status_code == 401 and "result" not in r.json()
    assert "unknown, expired or revoked" in r.json()["error"]["message"]
    view = admin(warp, "GET", f"/api/mcp-endpoints/{ep['id']}", expect=200).json()
    assert view["status"] == "expired" and "token" not in view and "tokenHash" not in view


def test_expiry_accepts_iso_offset_timestamps(warp):
    tz = datetime.timezone(datetime.timedelta(hours=-5))
    when = (datetime.datetime.now(tz) + datetime.timedelta(seconds=5)).isoformat(timespec="seconds")
    ep = create_endpoint(warp, "offset-expiry", "db:pg2", expiresAt=when)
    assert status(warp, ep) == 200
    assert wait_status(warp, ep, 401, timeout=15)


def test_never_expiring_endpoint_keeps_working(warp):
    ep = create_endpoint(warp, "forever", "db:pg2")
    assert ep["expiresAt"] is None
    time.sleep(2)
    assert status(warp, ep) == 200
    listed = {e["name"]: e for e in admin(warp, "GET", "/api/mcp-endpoints", expect=200).json()}
    assert listed["forever"]["status"] == "active" and listed["forever"]["expiresAt"] is None
    assert all("token" not in e and "tokenHash" not in e for e in listed.values())


def test_extend_revive_and_make_permanent(warp):
    ep = create_endpoint(warp, "extendable", "db:pg2", ttlSeconds=4)
    admin(warp, "PATCH", f"/api/mcp-endpoints/{ep['id']}", {"ttlSeconds": 3600}, expect=200)
    time.sleep(6)                                        # past the ORIGINAL expiry
    assert status(warp, ep) == 200                       # extension took effect
    # shorten it, let it lapse, then revive by setting a new future expiry
    admin(warp, "PATCH", f"/api/mcp-endpoints/{ep['id']}", {"ttlSeconds": 2}, expect=200)
    assert wait_status(warp, ep, 401, timeout=15)
    admin(warp, "PATCH", f"/api/mcp-endpoints/{ep['id']}", {"ttlSeconds": 3600}, expect=200)
    assert wait_status(warp, ep, 200, timeout=15)
    # remove the expiry entirely
    r = admin(warp, "PATCH", f"/api/mcp-endpoints/{ep['id']}", {"expiresAt": None}, expect=200).json()
    assert r["expiresAt"] is None
    assert status(warp, ep) == 200


def test_revoke_takes_effect_without_restart(warp):
    ep = create_endpoint(warp, "revocable", "db:pg2")
    assert status(warp, ep) == 200
    admin(warp, "DELETE", f"/api/mcp-endpoints/{ep['id']}", expect=200)
    start = time.time()
    assert wait_status(warp, ep, 401, timeout=10)
    assert time.time() - start < 5, "revocation was not prompt"
    admin(warp, "GET", f"/api/mcp-endpoints/{ep['id']}", expect=404)


def test_bad_credentials_and_creation_validation(warp):
    ep = create_endpoint(warp, "creds", "db:pg2")
    other = create_endpoint(warp, "creds-other", "db:pg2")
    assert status(warp, ep, token="wmcp_wrong") == 401
    assert status(warp, ep, token=other["token"]) == 401          # another endpoint's token
    r = requests.post(f"http://localhost:{warp.frontend_port}{ep['path']}", timeout=10,
                      json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert r.status_code == 401                                   # no credential
    past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)).isoformat()
    for body, fragment in [
            ({"name": "e1", "scope": "db:pg2", "expiresAt": past}, "not in the future"),
            ({"name": "e2", "scope": "db:pg2", "expiresAt": "tomorrow"}, "ISO-8601"),
            ({"name": "e3", "scope": "db:pg2", "expiresAt": "2099-01-01T00:00:00"}, "timezone"),
            ({"name": "e4", "scope": "db:pg2", "expiresAt": "2099-01-01T00:00:00Z", "ttlSeconds": 5}, "not both"),
            ({"name": "e5", "scope": "db:pg2", "ttlSeconds": -1}, "positive"),
            ({"name": "e6", "scope": "db:nosuch"}, "not a registered backend"),
            ({"name": "e7", "scope": "group:nosuch"}, "no registered member"),
            ({"name": "e8", "scope": "banana"}, "invalid"),
            ({"scope": "db:pg2"}, "name is required")]:
        resp = admin(warp, "POST", "/api/mcp-endpoints", body)
        assert resp.status_code == 400 and fragment in resp.json()["error"], (body, resp.text)
    admin(warp, "POST", "/api/mcp-endpoints", {"name": "creds", "scope": "db:pg2"}, expect=409)


def test_scoped_endpoints_cannot_reach_other_backends_and_set_endpoint_lists_backends(warp):
    only_default = create_endpoint(warp, "only-default", "db:default")
    port = warp.frontend_port
    err = call(port, "execute_sql", {"sql": "SELECT * FROM pg2_things"}, path=only_default["path"],
               token=only_default["token"], expect_error=True)[0]
    assert "pg2_things" in err or "42" in err
    err = call(port, "execute_sql", {"sql": "select 1", "backend": "pg2"}, path=only_default["path"],
               token=only_default["token"], expect_error=True)[0]
    assert "42501" in err

    finance = create_endpoint(warp, "finance-set", "group:finance", description="finance agents")
    out = call_json(port, "list_backends", path=finance["path"], token=finance["token"])
    assert out["scope"]["type"] == "group" and [b["name"] for b in out["backends"]] == ["pg2"]
    assert out["endpoint"]["name"] == "finance-set"
    everything = create_endpoint(warp, "everything", "all")
    out = call_json(port, "list_backends", path=everything["path"], token=everything["token"])
    assert {b["name"] for b in out["backends"]} >= {"default", "pg2"}
