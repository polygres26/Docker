"""A tiny, dependency-free "someone else's MCP server" fixture used ONLY by
tests/python/test_mcp_gateway.py, to exercise Warp's MCP-gateway-for-other-servers feature
end-to-end without any real third-party MCP server.

Implements just enough of Streamable-HTTP MCP (2025-03-26+ spec) to be a believable upstream:
  - POST / : JSON-RPC 2.0 (initialize, tools/list, tools/call), returns a Mcp-Session-Id header.
  - A few tools: "add" (readOnlyHint True), "get_time" (readOnlyHint True), "delete_thing" (no
    annotations at all -- exercises Warp's "treat as write unless explicitly readOnlyHint:true"
    default), "slow" (sleeps, for the circuit-breaker test), "echo_headers" (reflects the
    Authorization header it received, so tests can assert the bearer token that arrived).
  - Optional static bearer token requirement (MCPGW_FAKE_BEARER_TOKEN env var): 401s any request
    missing/wrong "Authorization: Bearer <token>".
  - Optional minimal OAuth 2.0 authorization server, mounted on the SAME port:
      GET  /.well-known/oauth-authorization-server  (RFC 8414)
      GET  /oauth/authorize   (trivial always-approve: immediately 302s to redirect_uri?code=...&state=...)
      POST /oauth/token       (authorization_code and refresh_token grants; access tokens expire in
                                2s by default so refresh can be exercised quickly in tests)
    Enabled whenever MCPGW_FAKE_OAUTH=1.

Run standalone for manual poking: `python3 mcpgw_fake_upstream.py [port]`.
"""
import json
import os
import secrets
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BEARER_TOKEN = os.environ.get("MCPGW_FAKE_BEARER_TOKEN")
OAUTH_ENABLED = os.environ.get("MCPGW_FAKE_OAUTH") == "1"
ACCESS_TOKEN_TTL_SECONDS = float(os.environ.get("MCPGW_FAKE_OAUTH_TOKEN_TTL", "2"))

# In-memory OAuth state -- fine for a test-only, single-process fixture.
_issued_codes = {}   # code -> {"redirect_uri": ..., "issued_at": ...}
_access_tokens = {}  # access_token -> expires_at (monotonic)
_refresh_tokens = {} # refresh_token -> access_token history (just needs to exist)
_call_log = []       # list of (tool_name, arguments, headers) -- inspectable by tests
_lock = threading.Lock()


def tool_defs():
    return [
        {
            "name": "add",
            "description": "Adds two numbers.",
            "inputSchema": {"type": "object", "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                             "required": ["a", "b"]},
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_time",
            "description": "Returns a fixed fake timestamp.",
            "inputSchema": {"type": "object", "properties": {}},
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "delete_thing",
            "description": "Pretends to delete something (no annotations at all -- unknown safety).",
            "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]},
        },
        {
            "name": "slow",
            "description": "Sleeps for `seconds` (default 5) before responding -- for circuit-breaker tests.",
            "inputSchema": {"type": "object", "properties": {"seconds": {"type": "number"}}},
        },
        {
            "name": "echo_headers",
            "description": "Returns the Authorization header this call arrived with.",
            "inputSchema": {"type": "object", "properties": {}},
        },
    ]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # keep test output quiet

    def _send_json(self, status, obj, extra_headers=None):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _rpc_error(self, req_id, code, message):
        self._send_json(200, {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})

    def _rpc_result(self, req_id, result):
        self._send_json(200, {"jsonrpc": "2.0", "id": req_id, "result": result},
                         extra_headers={"Mcp-Session-Id": "fake-session-1"})

    def _check_bearer(self):
        if not BEARER_TOKEN:
            return True
        auth = self.headers.get("Authorization", "")
        return auth == f"Bearer {BEARER_TOKEN}"

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if OAUTH_ENABLED and parsed.path == "/.well-known/oauth-authorization-server":
            base = f"http://{self.headers.get('Host')}"
            self._send_json(200, {
                "issuer": base,
                "authorization_endpoint": base + "/oauth/authorize",
                "token_endpoint": base + "/oauth/token",
                "response_types_supported": ["code"],
                "grant_types_supported": ["authorization_code", "refresh_token"],
                "code_challenge_methods_supported": ["S256"],
            })
            return
        if OAUTH_ENABLED and parsed.path == "/oauth/authorize":
            qs = urllib.parse.parse_qs(parsed.query)
            redirect_uri = qs["redirect_uri"][0]
            state = qs.get("state", [""])[0]
            code = secrets.token_urlsafe(16)
            with _lock:
                _issued_codes[code] = {"redirect_uri": redirect_uri, "issued_at": time.monotonic()}
            location = redirect_uri + ("&" if "?" in redirect_uri else "?") + urllib.parse.urlencode(
                {"code": code, "state": state})
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if parsed.path == "/__test__/calls":
            # Test-only introspection endpoint: what tools/call requests actually arrived.
            with _lock:
                self._send_json(200, {"calls": list(_call_log)})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if OAUTH_ENABLED and parsed.path == "/oauth/token":
            length = int(self.headers.get("Content-Length", "0"))
            form = urllib.parse.parse_qs(self.rfile.read(length).decode("utf-8"))
            grant_type = form.get("grant_type", [""])[0]
            if grant_type == "authorization_code":
                code = form.get("code", [""])[0]
                with _lock:
                    if code not in _issued_codes:
                        self._send_json(400, {"error": "invalid_grant"})
                        return
                    del _issued_codes[code]
            elif grant_type == "refresh_token":
                token = form.get("refresh_token", [""])[0]
                with _lock:
                    if token not in _refresh_tokens:
                        self._send_json(400, {"error": "invalid_grant"})
                        return
            else:
                self._send_json(400, {"error": "unsupported_grant_type"})
                return
            access_token = secrets.token_urlsafe(16)
            refresh_token = secrets.token_urlsafe(16)
            with _lock:
                _access_tokens[access_token] = time.monotonic() + ACCESS_TOKEN_TTL_SECONDS
                _refresh_tokens[refresh_token] = access_token
            self._send_json(200, {"access_token": access_token, "token_type": "bearer",
                                   "expires_in": int(ACCESS_TOKEN_TTL_SECONDS), "refresh_token": refresh_token})
            return

        if parsed.path != "/":
            self._send_json(404, {"error": "not found"})
            return

        if OAUTH_ENABLED:
            auth = self.headers.get("Authorization", "")
            token = auth[len("Bearer "):] if auth.startswith("Bearer ") else None
            with _lock:
                valid = token is not None and token in _access_tokens and time.monotonic() < _access_tokens[token]
            if not valid:
                self._send_json(401, {"jsonrpc": "2.0", "id": None,
                                       "error": {"code": -32001, "message": "invalid or expired access token"}})
                return
        elif not self._check_bearer():
            self._send_json(401, {"jsonrpc": "2.0", "id": None, "error": {"code": -32001, "message": "unauthorized"}})
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            req = json.loads(body)
        except json.JSONDecodeError:
            self._rpc_error(None, -32700, "parse error")
            return
        method = req.get("method")
        req_id = req.get("id")
        params = req.get("params") or {}

        if method == "initialize":
            self._rpc_result(req_id, {"protocolVersion": "2025-06-18",
                                       "serverInfo": {"name": "mcpgw-fake-upstream", "version": "1.0"},
                                       "capabilities": {"tools": {}}})
        elif method == "notifications/initialized":
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif method == "tools/list":
            self._rpc_result(req_id, {"tools": tool_defs()})
        elif method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            with _lock:
                _call_log.append({"tool": name, "arguments": arguments,
                                   "authorization": self.headers.get("Authorization")})
            if name == "add":
                total = arguments.get("a", 0) + arguments.get("b", 0)
                self._rpc_result(req_id, {"isError": False, "content": [{"type": "text", "text": str(total)}]})
            elif name == "get_time":
                self._rpc_result(req_id, {"isError": False, "content": [{"type": "text", "text": "2026-01-01T00:00:00Z"}]})
            elif name == "delete_thing":
                self._rpc_result(req_id, {"isError": False,
                                           "content": [{"type": "text", "text": "deleted " + str(arguments.get("id"))}]})
            elif name == "slow":
                time.sleep(float(arguments.get("seconds", 5)))
                self._rpc_result(req_id, {"isError": False, "content": [{"type": "text", "text": "done"}]})
            elif name == "echo_headers":
                self._rpc_result(req_id, {"isError": False,
                                           "content": [{"type": "text",
                                                        "text": str(self.headers.get("Authorization"))}]})
            elif name == "boom":
                self._rpc_error(req_id, -32050, "boom: this tool always fails upstream-side")
            else:
                self._rpc_error(req_id, -32602, f"unknown tool {name}")
        else:
            self._rpc_error(req_id, -32601, f"method not found: {method}")


def serve(port=0):
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    srv = ThreadingHTTPServer(("127.0.0.1", p), Handler)
    print(f"mcpgw fake upstream listening on http://127.0.0.1:{p}/ (oauth={OAUTH_ENABLED}, bearer={bool(BEARER_TOKEN)})")
    srv.serve_forever()
