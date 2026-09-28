"""An MCP endpoint created / extended / revoked through ONE Warp instance's admin API is honored by
a SECOND instance sharing the same config database (warp_config + LISTEN/NOTIFY), including expiry
evaluated on the second instance's own clock. Two real Warp subprocesses, one real Postgres (two
live instances is within the Developer license's 3-instance cap). No mocks.
"""
import os
import time

import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres
from mcp_support import ADMIN_TOKEN

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN


@pytest.fixture(scope="module")
def nodes():
    pg = RealPostgres()
    a = WarpProcess(pg, "WARP_MCP_PORT", frontend_name="node-a")
    b = WarpProcess(pg, "WARP_MCP_PORT", frontend_name="node-b")
    yield a, b
    b.close()
    a.close()
    pg.close()


def status(node, ep):
    return requests.post(f"http://localhost:{node.frontend_port}{ep['path']}", timeout=10,
                         headers={"Authorization": f"Bearer {ep['token']}"},
                         json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).status_code


def wait_status(node, ep, want, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if status(node, ep) == want:
            return True
        time.sleep(0.1)
    return False


def admin_a(a, method, path, body=None):
    return requests.request(method, f"http://localhost:{a.metrics_port}{path}", timeout=20, json=body,
                            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})


def test_create_extend_expire_revoke_propagate_to_the_other_instance(nodes):
    a, b = nodes
    ep = admin_a(a, "POST", "/api/mcp-endpoints", {"name": "shared", "scope": "all", "ttlSeconds": 5}).json()
    assert wait_status(b, ep, 200), "endpoint created on node A never became live on node B"
    assert wait_status(a, ep, 200)
    assert wait_status(b, ep, 401, timeout=15), "node B kept serving the endpoint after its expiry"
    r = admin_a(a, "PATCH", f"/api/mcp-endpoints/{ep['id']}", {"expiresAt": None})
    assert r.status_code == 200
    assert wait_status(b, ep, 200), "extension on node A did not revive the endpoint on node B"
    assert admin_a(a, "DELETE", f"/api/mcp-endpoints/{ep['id']}").status_code == 200
    assert wait_status(b, ep, 401, timeout=10), "revocation on node A was not honored by node B"
    assert wait_status(a, ep, 401, timeout=10)
