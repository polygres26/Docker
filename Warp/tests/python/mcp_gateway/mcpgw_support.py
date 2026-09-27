"""Test-support helpers for the MCP-gateway-for-other-servers feature: launches the fake upstream
(mcpgw_fake_upstream.py) as its own subprocess (so different tests can run different
bearer/oauth configs concurrently without global-state collisions), plus small admin-API
convenience wrappers layered on top of the shared mcp_support helpers.
"""
import os
import socket
import subprocess
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # tests/python
from mcp_support import ADMIN_TOKEN, admin  # noqa: E402

FIXTURE_DIR = os.path.dirname(os.path.abspath(__file__))


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeUpstream:
    """One running instance of mcpgw_fake_upstream.py, as a subprocess."""

    def __init__(self, bearer_token=None, oauth=False, oauth_token_ttl=None):
        self.port = free_port()
        env = dict(os.environ)
        if bearer_token:
            env["MCPGW_FAKE_BEARER_TOKEN"] = bearer_token
        else:
            env.pop("MCPGW_FAKE_BEARER_TOKEN", None)
        if oauth:
            env["MCPGW_FAKE_OAUTH"] = "1"
        else:
            env.pop("MCPGW_FAKE_OAUTH", None)
        if oauth_token_ttl is not None:
            env["MCPGW_FAKE_OAUTH_TOKEN_TTL"] = str(oauth_token_ttl)
        self.proc = subprocess.Popen(
            [sys.executable, os.path.join(FIXTURE_DIR, "mcpgw_fake_upstream.py"), str(self.port)],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self._wait_ready()

    @property
    def base_url(self):
        return f"http://127.0.0.1:{self.port}/"

    def _wait_ready(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(0.2)
                    s.connect(("127.0.0.1", self.port))
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("fake upstream did not start listening in time")

    def calls(self):
        """Introspection: the list of tools/call requests the fixture actually received."""
        return requests.get(f"http://127.0.0.1:{self.port}/__test__/calls", timeout=5).json()["calls"]

    def close(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def register_upstream(warp, name, base_url, prefix=None, auth_mode="NONE", bearer_token=None,
                       enabled=True, oauth_authorize_url=None, oauth_token_url=None, oauth_client_id="test-client",
                       oauth_client_secret=None, oauth_scope=None, expect=201):
    body = {"name": name, "baseUrl": base_url, "authMode": auth_mode, "enabled": enabled}
    if prefix:
        body["prefix"] = prefix
    if bearer_token:
        body["bearerToken"] = bearer_token
    if oauth_authorize_url:
        body["oauthAuthorizeUrl"] = oauth_authorize_url
    if oauth_token_url:
        body["oauthTokenUrl"] = oauth_token_url
    if oauth_client_id:
        body["oauthClientId"] = oauth_client_id
    if oauth_client_secret:
        body["oauthClientSecret"] = oauth_client_secret
    if oauth_scope:
        body["oauthScope"] = oauth_scope
    r = admin(warp, "POST", "/api/mcp-upstreams", body, expect=expect)
    return r.json()


def run_upstream_test(warp, upstream_id):
    return admin(warp, "POST", f"/api/mcp-upstreams/{upstream_id}/test", {}, expect=200).json()


def wait_upstream_healthy(warp, upstream_id, timeout=15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = run_upstream_test(warp, upstream_id)
        if last.get("ok"):
            return last
        time.sleep(0.2)
    return last
