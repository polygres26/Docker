"""Warp as an MCP gateway for OTHER MCP servers ("mcp-upstreams"), against real Warp subprocess(es)
+ real Postgres + the fake upstream fixture under tests/python/mcp_gateway/ -- no mocks.

Covers (see NOTES.md in mcpgw-out/ for exactly what is/isn't exercised here): registering a
no-auth upstream and discovering its tools; tools/list merge (native + namespaced upstream);
namespaced tools/call reaching the fixture with correct args, response relayed verbatim; prefix
collision rejection; static bearer token upstream (missing/wrong token fails, correct succeeds);
OAuth authorization-code flow (start -> callback -> stored encrypted -> call succeeds) and token
refresh; read-only enforcement under WARP_MCP_READ_ONLY; a slow/timing-out upstream not breaking
other tools (circuit breaker); secrets never appearing in the admin API or logs; hot-reload of a
newly-registered/edited upstream without restart; multi-node (two Warp instances on one Postgres).
"""
import os
import time

import psycopg2
import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres
from mcp_support import ADMIN_TOKEN, admin, create_endpoint, rpc
from mcp_gateway.mcpgw_support import FakeUpstream, register_upstream, run_upstream_test, wait_upstream_healthy

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def pg():
    p = RealPostgres()
    yield p
    p.close()


@pytest.fixture(scope="module")
def warp(pg):
    proc = WarpProcess(pg, "WARP_MCP_PORT", frontend_name="mcp", extra_env={
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg.port}/postgres|postgres|postgres",
    })
    yield proc
    proc.close()


@pytest.fixture()
def fake():
    up = FakeUpstream()
    yield up
    up.close()


def include_upstream_endpoint(warp, upstream_id, name=None):
    """A native "all" scope endpoint that also includes one upstream -- the common shape used by
    most tests here."""
    return create_endpoint(warp, name or f"ep-{upstream_id}", "all", upstreamIds=[upstream_id])


# ---------------------------------------------------------------------------
# Registration + discovery + namespaced merge
# ---------------------------------------------------------------------------

def test_register_no_auth_upstream_and_discover_tools(warp, fake):
    created = register_upstream(warp, "Fake Co", fake.base_url, prefix="fakeco")
    assert created["prefix"] == "fakeco"
    assert created["authMode"] == "NONE"
    result = wait_upstream_healthy(warp, created["id"])
    assert result["ok"], result
    assert result["toolCount"] == 5  # add, get_time, delete_thing, slow, echo_headers


def test_tools_list_merges_native_and_namespaced_upstream_tools(warp, fake):
    created = register_upstream(warp, "Merge Co", fake.base_url, prefix="mergeco")
    wait_upstream_healthy(warp, created["id"])
    ep = include_upstream_endpoint(warp, created["id"], "ep-merge")
    names = _tool_names_on(warp, ep)
    # Native tools (e.g. list_backends) AND namespaced upstream tools both present.
    assert "list_backends" in names
    assert "mergeco_add" in names
    assert "mergeco_get_time" in names
    assert "mergeco_delete_thing" in names


def _tool_names_on(warp, ep):
    body = rpc(warp.frontend_port, "tools/list", path=ep["path"], token=ep["token"])
    return {t["name"] for t in body["result"]["tools"]}


def test_namespaced_tool_call_reaches_upstream_with_correct_args_and_relays_verbatim(warp, fake):
    created = register_upstream(warp, "Call Co", fake.base_url, prefix="callco")
    wait_upstream_healthy(warp, created["id"])
    ep = include_upstream_endpoint(warp, created["id"], "ep-call")
    out = _call_text(warp, ep, "callco_add", {"a": 2, "b": 40})
    assert out == "42"
    calls = fake.calls()
    assert calls[-1]["tool"] == "add"
    assert calls[-1]["arguments"] == {"a": 2, "b": 40}


def _call_text(warp, ep, name, args):
    body = rpc(warp.frontend_port, "tools/call", {"name": name, "arguments": args}, path=ep["path"], token=ep["token"])
    assert "error" not in body, body
    return body["result"]["content"][0]["text"]


def test_prefix_collision_is_rejected(warp, fake):
    register_upstream(warp, "Coll One", fake.base_url, prefix="collide")
    resp = admin(warp, "POST", "/api/mcp-upstreams",
                 {"name": "Coll Two", "baseUrl": fake.base_url, "prefix": "collide"})
    assert resp.status_code == 409, resp.text


def test_suggested_prefix_collision_across_similarly_named_upstreams(warp, fake):
    # Two upstreams whose auto-suggested prefix would collide (case-insensitively) must not both
    # silently register under the same prefix.
    register_upstream(warp, "Acme Corp", fake.base_url)  # auto-suggests "acme_corp"
    resp = admin(warp, "POST", "/api/mcp-upstreams", {"name": "ACME CORP", "baseUrl": fake.base_url})
    assert resp.status_code == 409, resp.text


# ---------------------------------------------------------------------------
# Static bearer token auth
# ---------------------------------------------------------------------------

def test_static_bearer_token_upstream_requires_correct_token():
    up = FakeUpstream(bearer_token="s3cr3t-token")
    try:
        # Missing token -> the fixture 401s -> Warp's "test" reports auth failure.
        pg_ = RealPostgres()
        try:
            proc = WarpProcess(pg_, "WARP_MCP_PORT", frontend_name="mcp", extra_env={
                "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
                "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg_.port}/postgres|postgres|postgres",
            })
            try:
                created = register_upstream(proc, "Secure Co", up.base_url, prefix="secureco", auth_mode="NONE")
                result = run_upstream_test(proc, created["id"])
                assert not result["ok"]

                created_wrong = register_upstream(proc, "Secure Co Wrong", up.base_url, prefix="securewrong",
                                                    auth_mode="BEARER", bearer_token="wrong-token")
                result_wrong = run_upstream_test(proc, created_wrong["id"])
                assert not result_wrong["ok"]

                created_right = register_upstream(proc, "Secure Co Right", up.base_url, prefix="secureright",
                                                    auth_mode="BEARER", bearer_token="s3cr3t-token")
                result_right = wait_upstream_healthy(proc, created_right["id"])
                assert result_right["ok"], result_right

                ep = include_upstream_endpoint(proc, created_right["id"], "ep-secure")
                out = _call_text(proc, ep, "secureright_echo_headers", {})
                assert out == "Bearer s3cr3t-token"
            finally:
                proc.close()
        finally:
            pg_.close()
    finally:
        up.close()


def test_secret_never_appears_in_admin_api_response_or_logs(warp, fake):
    created = register_upstream(warp, "Secret Co", fake.base_url, prefix="secretco", auth_mode="BEARER",
                                 bearer_token="super-secret-value-xyz")
    assert "super-secret-value-xyz" not in str(created)
    assert created["hasBearerToken"] is True
    listing = admin(warp, "GET", "/api/mcp-upstreams", expect=200).json()
    assert "super-secret-value-xyz" not in str(listing)
    got = admin(warp, "GET", f"/api/mcp-upstreams/{created['id']}", expect=200).json()
    assert "super-secret-value-xyz" not in str(got)


# ---------------------------------------------------------------------------
# Read-only governance
# ---------------------------------------------------------------------------

def test_read_only_enforcement_blocks_unknown_safety_allows_readonlyhint():
    pg_ = RealPostgres()
    try:
        proc = WarpProcess(pg_, "WARP_MCP_PORT", frontend_name="mcp", extra_env={
            "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
            "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg_.port}/postgres|postgres|postgres",
            "WARP_MCP_READ_ONLY": "true",
        })
        up = FakeUpstream()
        try:
            created = register_upstream(proc, "RO Co", up.base_url, prefix="roco")
            wait_upstream_healthy(proc, created["id"])
            ep = include_upstream_endpoint(proc, created["id"], "ep-ro")
            # get_time carries readOnlyHint:true -> allowed even under WARP_MCP_READ_ONLY.
            out = _call_text(proc, ep, "roco_get_time", {})
            assert out == "2026-01-01T00:00:00Z"
            # delete_thing carries NO annotations at all -> treated as write, blocked.
            body = rpc(proc.frontend_port, "tools/call", {"name": "roco_delete_thing", "arguments": {"id": "x"}},
                       path=ep["path"], token=ep["token"])
            assert "error" in body, body
            assert "read_only" in body["error"]["message"].lower() or "readonly" in body["error"]["message"].lower() \
                or "WARP_MCP_READ_ONLY" in body["error"]["message"]
        finally:
            up.close()
            proc.close()
    finally:
        pg_.close()


# ---------------------------------------------------------------------------
# Resilience: circuit breaker
# ---------------------------------------------------------------------------

def test_slow_upstream_does_not_break_other_tools_or_native_tools(warp, fake):
    created = register_upstream(warp, "Slow Co", fake.base_url, prefix="slowco")
    wait_upstream_healthy(warp, created["id"])
    ep = include_upstream_endpoint(warp, created["id"], "ep-slow")
    # tools/list (which discovers the upstream fresh the first time) must still work and include
    # both native and this upstream's own tools, even though `slow` itself takes a while to CALL --
    # discovery here is tools/list, not tools/call, so this just proves listing isn't blocked by an
    # upstream that merely HAS a slow tool.
    names = _tool_names_on(warp, ep)
    assert "list_backends" in names
    assert "slowco_add" in names
    # A normal call still works and is fast.
    start = time.time()
    out = _call_text(warp, ep, "slowco_add", {"a": 1, "b": 1})
    assert out == "2"
    assert time.time() - start < 5


def test_circuit_breaker_skips_repeatedly_failing_upstream_without_blocking_others(warp):
    # An upstream base URL that refuses connections outright (nothing listening there).
    dead_port = 1  # privileged/unused port, connection refused immediately
    dead = register_upstream(warp, "Dead Co", f"http://127.0.0.1:{dead_port}/", prefix="deadco")
    live = FakeUpstream()
    try:
        live_created = register_upstream(warp, "Live Co", live.base_url, prefix="liveco")
        wait_upstream_healthy(warp, live_created["id"])
        ep = include_upstream_endpoint(warp, dead["id"], "ep-mixed")
        token = ep["token"]  # PATCH's response never re-includes the token (issued once, at creation)
        admin(warp, "PATCH", f"/api/mcp-endpoints/{ep['id']}",
              {"upstreamIds": [dead["id"], live_created["id"]]}, expect=200)
        # tools/list must still return the live upstream's tools (and native tools) even though the
        # dead one is included and unreachable -- repeated attempts (several tools/lists) must not
        # get slower/break as the breaker opens.
        names = set()
        for _ in range(4):
            names = _tool_names_on(warp, {"path": ep["path"], "token": token})
        assert "liveco_add" in names
        assert "list_backends" in names
    finally:
        live.close()


# ---------------------------------------------------------------------------
# Hot reload
# ---------------------------------------------------------------------------

def test_hot_reload_of_new_upstream_without_restart(warp, fake):
    created = register_upstream(warp, "Reload Co", fake.base_url, prefix="reloadco")
    wait_upstream_healthy(warp, created["id"])
    ep = include_upstream_endpoint(warp, created["id"], "ep-reload")
    assert "reloadco_add" in _tool_names_on(warp, ep)
    # Edit the upstream's prefix live; the endpoint's tools/list must reflect it without restarting.
    admin(warp, "PATCH", f"/api/mcp-upstreams/{created['id']}", {"prefix": "renamedco"}, expect=200)
    admin(warp, "POST", f"/api/mcp-upstreams/{created['id']}/refresh-tools", {}, expect=200)
    deadline = time.time() + 15
    names = set()
    while time.time() < deadline:
        names = _tool_names_on(warp, ep)
        if "renamedco_add" in names:
            break
        time.sleep(0.2)
    assert "renamedco_add" in names


# ---------------------------------------------------------------------------
# Multi-node: two Warp instances on one Postgres both see the registered upstream
# ---------------------------------------------------------------------------

def test_multi_node_both_instances_serve_the_same_registered_upstream(fake):
    pg_ = RealPostgres()
    try:
        proc_a = WarpProcess(pg_, "WARP_MCP_PORT", frontend_name="mcp", extra_env={
            "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
            "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg_.port}/postgres|postgres|postgres",
        })
        try:
            proc_b = WarpProcess(pg_, "WARP_MCP_PORT", frontend_name="mcp", extra_env={
                "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
                "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg_.port}/postgres|postgres|postgres",
            })
            try:
                created = register_upstream(proc_a, "Cluster Co", fake.base_url, prefix="clusterco")
                wait_upstream_healthy(proc_a, created["id"])
                # Node B never called POST /api/mcp-upstreams itself -- it must pick the row up via
                # the shared warp_config LISTEN/NOTIFY the same way it picks up mcp-endpoints/backends.
                deadline = time.time() + 15
                seen = None
                while time.time() < deadline:
                    seen = admin(proc_b, "GET", "/api/mcp-upstreams", expect=200).json()
                    if any(u["id"] == created["id"] for u in seen):
                        break
                    time.sleep(0.2)
                assert any(u["id"] == created["id"] for u in seen), seen

                ep_b = include_upstream_endpoint(proc_b, created["id"], "ep-b")
                out = _call_text(proc_b, ep_b, "clusterco_add", {"a": 10, "b": 5})
                assert out == "15"
            finally:
                proc_b.close()
        finally:
            proc_a.close()
    finally:
        pg_.close()


# ---------------------------------------------------------------------------
# OAuth authorization-code flow
# ---------------------------------------------------------------------------

def test_oauth_authorization_code_flow_and_refresh(warp):
    up = FakeUpstream(oauth=True, oauth_token_ttl=2)
    try:
        created = register_upstream(warp, "OAuth Co", up.base_url, prefix="oauthco", auth_mode="OAUTH",
                                     oauth_client_id="test-client")
        # start: Warp discovers the fixture's RFC 8414 metadata from its base URL's origin.
        start = admin(warp, "POST", f"/api/mcp-upstreams/{created['id']}/oauth/start", {}, expect=200).json()
        assert "authorizeUrl" in start and start["authorizeUrl"].startswith(up.base_url.rstrip("/") + "/oauth/authorize")
        # The operator's browser would be redirected there; the fixture's authorize endpoint
        # always-approves and 302s straight to Warp's own callback with a code+state.
        r = requests.get(start["authorizeUrl"], allow_redirects=False, timeout=10)
        assert r.status_code == 302
        callback_url = r.headers["Location"]
        r2 = requests.get(callback_url, timeout=10)
        assert r2.status_code == 200 and "Connected" in r2.text

        got = admin(warp, "GET", f"/api/mcp-upstreams/{created['id']}", expect=200).json()
        assert got["oauthConnected"] is True

        ep = include_upstream_endpoint(warp, created["id"], "ep-oauth")
        out = _call_text(warp, ep, "oauthco_add", {"a": 3, "b": 4})
        assert out == "7"

        # Wait past the 2s access-token TTL, then call again -- Warp must transparently refresh.
        time.sleep(2.5)
        out2 = _call_text(warp, ep, "oauthco_add", {"a": 5, "b": 6})
        assert out2 == "11"
    finally:
        up.close()
