"""Shared JSON-RPC-over-HTTP MCP client helpers + admin-API helpers for the MCP backend-model tests."""
import itertools
import json
import time

import requests

ADMIN_TOKEN = "warp-polywire-test-admin-token"
_ids = itertools.count(1)


def rpc(port, method, params=None, path="/", token=None, expect_http=200):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.post(f"http://localhost:{port}{path}", timeout=30, headers=headers, json={
        "jsonrpc": "2.0", "id": next(_ids), "method": method, "params": params or {}})
    assert resp.status_code == expect_http, (resp.status_code, resp.text)
    return resp.json()


def tool_defs(port, path="/", token=None):
    return {t["name"]: t for t in rpc(port, "tools/list", path=path, token=token)["result"]["tools"]}


def tool_names(port, path="/", token=None):
    return set(tool_defs(port, path, token))


def call(port, name, args=None, path="/", token=None, expect_error=False):
    body = rpc(port, "tools/call", {"name": name, "arguments": args or {}}, path=path, token=token)
    if expect_error and "error" in body:
        return [body["error"]["message"]]
    assert "error" not in body, body
    result = body["result"]
    assert result["isError"] is expect_error, result
    return [c["text"] for c in result["content"]]


def call_json(port, name, args=None, path="/", token=None):
    return json.loads(call(port, name, args, path, token)[-1])


def admin(warp, method, path, body=None, expect=None):
    resp = requests.request(method, f"http://localhost:{warp.metrics_port}{path}", timeout=30,
                            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, json=body)
    if expect is not None:
        assert resp.status_code == expect, (resp.status_code, resp.text)
    return resp


def create_endpoint(warp, name, scope, wait_live=True, **kw):
    """Creates an endpoint through the admin API; by default waits (bounded) for the config
    hot-reload (LISTEN/NOTIFY) to make it live on the MCP listener."""
    ep = admin(warp, "POST", "/api/mcp-endpoints", {"name": name, "scope": scope, **kw}, expect=201).json()
    if wait_live:
        deadline = time.time() + 15
        while True:
            r = requests.post(f"http://localhost:{warp.frontend_port}{ep['path']}", timeout=10,
                              headers={"Authorization": f"Bearer {ep['token']}"},
                              json={"jsonrpc": "2.0", "id": 0, "method": "tools/list"})
            if r.status_code == 200 or time.time() > deadline:
                break
            time.sleep(0.1)
    return ep
