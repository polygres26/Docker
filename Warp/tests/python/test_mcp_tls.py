"""HTTPS for the MCP / A2A / admin listeners against real Warp subprocesses with generated certificates:
PEM (fullchain + PKCS8 key), PKCS12 keystore, self-signed dev mode, bad-password / bad-path startup errors that leave
plaintext MCP running, real initialize + tools/list + tools/call over https with a trusted test CA (python `requests`
and, when installable, the official `mcp` SDK), plaintext still working, the opt-in header-less URL-token mode
(off by default, enabled per endpoint, rejected after revoke/expiry, never logged) and mtime-poll certificate reload.
Needs the `openssl` CLI; the SDK test needs python >= 3.10 with `pip install mcp`.
"""
import asyncio
import json
import os
import shutil
import socket
import ssl
import subprocess
import time

import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres, free_port, isolated_ports
from mcp_support import ADMIN_TOKEN

os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl CLI not available")


def _run(*args, **kw):
    subprocess.run(args, check=True, capture_output=True, **kw)


@pytest.fixture(scope="module")
def certs(tmp_path_factory):
    d = tmp_path_factory.mktemp("mcptls-certs")
    p = lambda n: str(d / n)  # noqa: E731
    _run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", p("ca.key"), "-out", p("ca.pem"),
         "-days", "30", "-subj", "/CN=Warp Test CA", "-addext", "basicConstraints=critical,CA:TRUE",
         "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    (d / "leaf.ext").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n"
                                "keyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")

    def leaf(name, kind):
        if kind == "rsa":
            _run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", p(name + ".pkcs8.key"),
                 "-out", p(name + ".csr"), "-subj", "/CN=localhost")
        else:
            _run("openssl", "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", p(name + ".sec1.key"))
            _run("openssl", "pkcs8", "-topk8", "-nocrypt", "-in", p(name + ".sec1.key"), "-out", p(name + ".pkcs8.key"))
            _run("openssl", "req", "-new", "-key", p(name + ".sec1.key"), "-out", p(name + ".csr"), "-subj", "/CN=localhost")
        _run("openssl", "x509", "-req", "-in", p(name + ".csr"), "-CA", p("ca.pem"), "-CAkey", p("ca.key"),
             "-CAcreateserial", "-out", p(name + ".pem"), "-days", "30", "-extfile", p("leaf.ext"))

    leaf("rsa", "rsa")
    leaf("ec", "ec")
    (d / "rsa.fullchain.pem").write_text((d / "rsa.pem").read_text() + (d / "ca.pem").read_text())
    _run("openssl", "pkcs12", "-export", "-in", p("rsa.pem"), "-inkey", p("rsa.pkcs8.key"), "-certfile", p("ca.pem"),
         "-out", p("rsa.p12"), "-passout", "pass:changeit", "-name", "warp")
    return {"dir": str(d), "ca": p("ca.pem"), "rsa_chain": p("rsa.fullchain.pem"), "rsa_key": p("rsa.pkcs8.key"),
            "ec_cert": p("ec.pem"), "ec_key": p("ec.pkcs8.key"), "ec_sec1": p("ec.sec1.key"), "p12": p("rsa.p12")}


@pytest.fixture(scope="module")
def pg():
    db = RealPostgres()
    yield db
    db.close()


def first_bindable_cluster_port():
    for port in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("no free port in 47500..47599")


def start_warp(pg, env, name="mcp-tls"):
    ports = isolated_ports(exclude="WARP_MCP_PORT")
    proc = WarpProcess(pg, "WARP_MCP_PORT", frontend_name=name, extra_env={
        **ports,
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg.port}/postgres|postgres|postgres",
        "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
        "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_bindable_cluster_port()}",
        **env})
    proc.https_port = int(env.get("WARP_MCP_HTTPS_PORT", 0)) or None
    proc.admin_https_port = int(env.get("WARP_ADMIN_HTTPS_PORT", 0)) or None
    proc.a2a_https_port = int(env.get("WARP_A2A_HTTPS_PORT", 0)) or None
    proc.a2a_port = int(ports["WARP_A2A_PORT"])
    return proc


def log_text(proc):
    return "".join(proc._output_lines)


@pytest.fixture(scope="module")
def warp(pg, certs, tmp_path_factory):
    # private copies so the reload test can swap them without touching the shared fixtures
    d = tmp_path_factory.mktemp("live-pem")
    shutil.copy(certs["rsa_chain"], d / "cert.pem")
    shutil.copy(certs["rsa_key"], d / "key.pem")
    proc = start_warp(pg, {
        "WARP_MCP_TLS_CERT": str(d / "cert.pem"), "WARP_MCP_TLS_KEY": str(d / "key.pem"),
        "WARP_TLS_RELOAD_SECONDS": "1",
        "WARP_MCP_HTTPS_PORT": str(free_port()), "WARP_ADMIN_HTTPS_PORT": str(free_port()),
        "WARP_A2A_HTTPS_PORT": str(free_port()),
        # the global settings apply to admin and A2A (no per-listener override for those)
        "WARP_TLS_CERT": str(d / "cert.pem"), "WARP_TLS_KEY": str(d / "key.pem"),
    })
    proc.live_dir = d
    yield proc
    proc.close()


def admin_https(warp, method, path, body=None, certs=None, expect=None):
    r = requests.request(method, f"https://localhost:{warp.admin_https_port}{path}", json=body, timeout=30,
                         headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}, verify=certs["ca"])
    if expect is not None:
        assert r.status_code == expect, (r.status_code, r.text)
    return r


def rpc(url, method, params=None, token=None, verify=True, expect=200):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = requests.post(url, headers=headers, verify=verify, timeout=30,
                      json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})
    assert r.status_code == expect, (r.status_code, r.text)
    return r.json() if expect == 200 else r


def wait_live(url, token=None, want=200, verify=True, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        r = requests.post(url, headers=headers, verify=verify, timeout=10,
                          json={"jsonrpc": "2.0", "id": 0, "method": "tools/list"})
        if r.status_code == want:
            return True
        time.sleep(0.1)
    return False


@pytest.fixture(scope="module")
def endpoint(warp, certs):
    ep = admin_https(warp, "POST", "/api/mcp-endpoints", {"name": "tls-ep", "scope": "all"}, certs, expect=201).json()
    assert wait_live(f"https://localhost:{warp.https_port}{ep['path']}", ep["token"], verify=certs["ca"])
    return ep


def https_url(warp, ep):
    return f"https://localhost:{warp.https_port}{ep['path']}"


def http_url(warp, ep):
    return f"http://localhost:{warp.frontend_port}{ep['path']}"


def test_https_initialize_list_call_with_trusted_ca(warp, certs, endpoint):
    url = https_url(warp, endpoint)
    init = rpc(url, "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                   "clientInfo": {"name": "t", "version": "1"}}, endpoint["token"], certs["ca"])
    assert "result" in init and init["result"].get("serverInfo")
    tools = rpc(url, "tools/list", None, endpoint["token"], certs["ca"])["result"]["tools"]
    assert "execute_sql" in {t["name"] for t in tools}
    res = rpc(url, "tools/call", {"name": "execute_sql", "arguments": {"sql": "SELECT 41 + 1 AS answer"}},
              endpoint["token"], certs["ca"])["result"]
    assert res["isError"] is False
    assert "42" in json.dumps(res["content"])


def test_untrusted_client_is_rejected_by_tls_not_served(warp, endpoint):
    with pytest.raises(requests.exceptions.SSLError):
        requests.post(https_url(warp, endpoint), timeout=10, json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})


def test_plaintext_still_works(warp, endpoint):
    res = rpc(http_url(warp, endpoint), "tools/call", {"name": "execute_sql", "arguments": {"sql": "select 7 as seven"}},
              endpoint["token"])["result"]
    assert res["isError"] is False and "7" in json.dumps(res["content"])


def test_https_rejects_missing_token(warp, certs, endpoint):
    r = rpc(https_url(warp, endpoint), "tools/list", None, None, certs["ca"], expect=401)
    assert r.status_code == 401


def test_get_and_delete_get_405_like_a_stateless_streamable_http_server(warp, certs, endpoint):
    for method in ("GET", "DELETE"):
        r = requests.request(method, https_url(warp, endpoint), timeout=10, verify=certs["ca"],
                             headers={"Authorization": f"Bearer {endpoint['token']}"})
        assert r.status_code == 405 and r.headers.get("Allow") == "POST"
    # unauthenticated GET is still a 401, not a 405 (auth first)
    assert requests.get(https_url(warp, endpoint), timeout=10, verify=certs["ca"]).status_code == 401


def test_mcp_config_and_create_response_carry_https_urls(warp, certs, endpoint):
    cfg = admin_https(warp, "GET", "/api/mcp-config", certs=certs, expect=200).json()
    assert cfg["tlsEnabled"] is True and cfg["httpsPort"] == warp.https_port
    assert cfg["httpPort"] == warp.frontend_port
    assert cfg["baseUrl"] == f"https://localhost:{warp.https_port}" and cfg["claudeConnectorReady"] is True
    assert endpoint["url"] == f"https://localhost:{warp.https_port}{endpoint['path']}"
    ifaces = admin_https(warp, "GET", "/api/interfaces", certs=certs, expect=200).json()["interfaces"]
    mcp = next(i for i in ifaces if i["id"] == "mcp")
    assert mcp["tlsEnabled"] is True and mcp["httpsPort"] == warp.https_port


def test_admin_plain_and_https_and_a2a_https(warp, certs):
    plain = requests.get(f"http://localhost:{warp.metrics_port}/api/mcp-config", timeout=10,
                         headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
    assert plain.status_code == 200
    card = requests.get(f"https://localhost:{warp.a2a_https_port}/.well-known/agent-card.json", timeout=10,
                        verify=certs["ca"])
    assert card.status_code == 200 and card.json()["url"].startswith("https://localhost:")
    assert requests.get(f"http://localhost:{warp.a2a_port}/.well-known/agent-card.json", timeout=10).status_code == 200


def test_public_url_and_forwarded_headers_shape_the_connection_url(warp, certs):
    r = requests.get(f"https://localhost:{warp.admin_https_port}/api/mcp-config", timeout=10, verify=certs["ca"],
                     headers={"Authorization": f"Bearer {ADMIN_TOKEN}", "X-Forwarded-Proto": "https",
                              "X-Forwarded-Host": "warp.example.com"})
    # a native HTTPS listener wins over guessing from forwarded headers only when there is no forwarded host;
    # with a forwarded host (i.e. behind a proxy) that host is what the user reaches
    assert r.json()["baseUrl"] == "https://warp.example.com"


def test_url_token_is_off_by_default_and_opt_in_per_endpoint(warp, certs):
    ep = admin_https(warp, "POST", "/api/mcp-endpoints", {"name": "urltok", "scope": "all"}, certs, expect=201).json()
    assert ep["urlToken"] is False and "urlWithToken" not in ep
    base = f"https://localhost:{warp.https_port}"
    assert wait_live(base + ep["path"], ep["token"], verify=certs["ca"])
    url_tok = f"{base}{ep['path']}/t/{ep['token']}"
    # off: rejected even though the token is right
    assert rpc(url_tok, "tools/list", None, None, certs["ca"], expect=401).status_code == 401
    # header mode is untouched by the URL feature
    assert rpc(base + ep["path"], "tools/list", None, ep["token"], certs["ca"])["result"]["tools"]
    # enable it (hot reload)
    admin_https(warp, "PATCH", f"/api/mcp-endpoints/{ep['id']}", {"urlToken": True}, certs, expect=200)
    assert wait_live(url_tok, None, 200, certs["ca"])
    assert rpc(url_tok, "tools/call", {"name": "execute_sql", "arguments": {"sql": "select 5 as five"}}, None,
               certs["ca"])["result"]["isError"] is False
    # wrong token in the URL, and a header token that is right but path token wrong: rejected
    assert rpc(f"{base}{ep['path']}/t/wmcp_wrong", "tools/list", None, None, certs["ca"], expect=401).status_code == 401
    assert rpc(f"{base}{ep['path']}/t/wmcp_wrong", "tools/list", None, ep["token"], certs["ca"],
               expect=401).status_code == 401
    # revoke: the URL stops working
    admin_https(warp, "DELETE", f"/api/mcp-endpoints/{ep['id']}", None, certs, expect=200)
    assert wait_live(url_tok, None, 401, certs["ca"])
    warp.leak_tokens = getattr(warp, "leak_tokens", []) + [ep["token"]]


def test_url_token_expiry_is_enforced(warp, certs):
    ep = admin_https(warp, "POST", "/api/mcp-endpoints",
                     {"name": "urltok-exp", "scope": "all", "urlToken": True, "ttlSeconds": 4}, certs, expect=201).json()
    assert ep["urlToken"] is True
    base = f"https://localhost:{warp.https_port}"
    assert ep["urlWithToken"] == f"{base}{ep['path']}/t/{ep['token']}"
    assert wait_live(ep["urlWithToken"], None, 200, certs["ca"], timeout=3)
    time.sleep(4.5)
    assert rpc(ep["urlWithToken"], "tools/list", None, None, certs["ca"], expect=401).status_code == 401
    warp.leak_tokens = getattr(warp, "leak_tokens", []) + [ep["token"]]


def test_mcp_python_sdk_over_https(warp, certs, endpoint, monkeypatch):
    pytest.importorskip("mcp", reason="the official mcp SDK needs python >= 3.10 (pip install mcp)")
    try:
        import httpx2 as httpx
    except ImportError:  # older SDKs use httpx
        import httpx
    from mcp import ClientSession
    try:
        from mcp.client.streamable_http import streamable_http_client as connect
        new_api = True
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client as connect
        new_api = False

    async def go():
        headers = {"Authorization": f"Bearer {endpoint['token']}"}
        url = https_url(warp, endpoint)
        if new_api:
            client = httpx.AsyncClient(verify=ssl.create_default_context(cafile=certs["ca"]), headers=headers)
            cm = connect(url, http_client=client)
        else:
            monkeypatch.setenv("SSL_CERT_FILE", certs["ca"])
            cm = connect(url, headers=headers)
        async with cm as streams:
            read, write = streams[0], streams[1]
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                assert "execute_sql" in {t.name for t in tools.tools}
                res = await session.call_tool("execute_sql", {"sql": "SELECT 6 * 7 AS answer"})
                assert not getattr(res, "is_error", getattr(res, "isError", False)) and "42" in str(res.content)

    asyncio.run(asyncio.wait_for(go(), 60))


def test_certificate_hot_reload_by_mtime_poll(warp, certs):
    def served():
        return ssl.get_server_certificate(("localhost", warp.https_port))

    before = served()
    shutil.copy(certs["ec_cert"], warp.live_dir / "cert.pem")
    shutil.copy(certs["ec_sec1"], warp.live_dir / "key.pem")  # SEC1 "EC PRIVATE KEY", through a real Warp
    want = open(certs["ec_cert"]).read().strip()
    deadline = time.time() + 20
    while time.time() < deadline and served().strip() != want:
        time.sleep(0.5)
    assert served().strip() == want != before.strip(), "the renewed certificate was not picked up"
    # and it still verifies against the CA and serves MCP
    r = requests.get(f"https://localhost:{warp.https_port}/", verify=certs["ca"], timeout=10)
    assert r.status_code in (200, 401, 404, 405)
    assert "TLS material reloaded" in log_text(warp)


def test_token_never_appears_in_the_warp_log(warp, endpoint):
    text = log_text(warp)
    leaked = [endpoint["token"]] + getattr(warp, "leak_tokens", [])
    assert leaked
    for t in leaked:
        assert t not in text, "an endpoint token leaked into the Warp log"
    assert "refused" in text  # the rejected URL-token requests were logged, redacted


def test_pkcs12_keystore_and_no_plaintext_when_disabled(pg, certs):
    env = {"WARP_MCP_TLS_KEYSTORE": certs["p12"], "WARP_MCP_TLS_KEYSTORE_PASSWORD": "changeit",
           "WARP_MCP_HTTPS_PORT": str(free_port()), "WARP_MCP_HTTP_DISABLED": "true"}
    ports = isolated_ports(exclude="WARP_MCP_PORT")
    # WarpProcess waits for the plaintext MCP port, which is disabled here: probe the https port ourselves.
    proc = None
    mcp_plain = free_port()
    try:
        proc = subprocess_warp(pg, {**env, "WARP_MCP_PORT": str(mcp_plain), **ports})
        deadline = time.time() + 40
        ok = False
        while time.time() < deadline:
            try:
                r = requests.post(f"https://localhost:{env['WARP_MCP_HTTPS_PORT']}/", verify=certs["ca"], timeout=3,
                                  json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
                ok = r.status_code in (200, 401)
                if ok:
                    break
            except requests.RequestException:
                time.sleep(0.5)
        assert ok, "PKCS12-backed HTTPS did not come up:\n" + "".join(proc._output_lines[-30:])
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.post(f"http://localhost:{mcp_plain}/", timeout=3, json={})
    finally:
        if proc:
            proc.close()


def test_bad_keystore_password_and_bad_path_log_a_clear_error_and_keep_plaintext(pg, certs):
    for env, needle in (
            ({"WARP_MCP_TLS_KEYSTORE": certs["p12"], "WARP_MCP_TLS_KEYSTORE_PASSWORD": "wrong"}, "wrong keystore password"),
            ({"WARP_MCP_TLS_CERT": "/nonexistent/fullchain.pem", "WARP_MCP_TLS_KEY": "/nonexistent/privkey.pem"},
             "/nonexistent/fullchain.pem")):
        own_pg = RealPostgres()  # own config database: the Developer license caps live instances per database
        proc = start_warp(own_pg, {**env, "WARP_MCP_HTTPS_PORT": str(free_port())}, "mcp-badtls")
        try:
            # Warp started (WarpProcess waited for the plaintext MCP port) and says why HTTPS is off
            text = log_text(proc)
            assert "HTTPS is NOT enabled" in text and needle in text, text[-2000:]
            r = requests.post(f"http://localhost:{proc.frontend_port}/", timeout=10,
                              json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
            assert r.status_code in (200, 401)
            with pytest.raises(requests.exceptions.ConnectionError):
                requests.post(f"https://localhost:{proc.https_port}/", timeout=3, verify=False, json={})
        finally:
            proc.close()
            own_pg.close()


def test_self_signed_dev_mode_is_https_but_not_trusted(pg, certs):
    own_pg = RealPostgres()
    proc = start_warp(own_pg, {"WARP_MCP_TLS_SELF_SIGNED": "true", "WARP_MCP_HTTPS_PORT": str(free_port()),
                           "WARP_ADMIN_HTTPS_PORT": str(free_port()), "WARP_ADMIN_TLS_SELF_SIGNED": "true"}, "mcp-ss")
    try:
        url = f"https://localhost:{proc.https_port}/"
        with pytest.raises(requests.exceptions.SSLError):
            requests.post(url, timeout=10, json={})
        r = requests.post(url, timeout=10, verify=False, json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        assert r.status_code in (200, 401)
        cfg = requests.get(f"https://localhost:{proc.admin_https_port}/api/mcp-config", verify=False, timeout=10,
                           headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}).json()
        assert cfg["selfSigned"] is True and cfg["claudeConnectorReady"] is False
    finally:
        proc.close()
        own_pg.close()


def subprocess_warp(pg, env):
    """A Warp process without WarpProcess' plaintext-port readiness probe (used when plaintext is disabled)."""
    from warp_test_support import JAR_PATH, ADD_OPENS, REPO_ROOT
    import threading
    e = dict(os.environ)
    metrics, grpc = free_port(), free_port()
    e.update({"WARP_HOST": "localhost", "WARP_PORT": str(pg.port), "WARP_DATABASE": "postgres", "WARP_USER": "postgres",
              "WARP_PASSWORD": "postgres", "WARP_AUTH_USER": "postgres", "WARP_AUTH_PASSWORD": "postgres",
              "WARP_METRICS_PORT": str(metrics), "WARP_GRPC_PORT": str(grpc), "WARP_QOS_RATE_PER_SEC": "1000",
              "WARP_QOS_BURST": "1000", "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
              "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg.port}/postgres|postgres|postgres",
              "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
              "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_bindable_cluster_port()}"})
    e.update({k: str(v) for k, v in env.items()})
    p = subprocess.Popen(["java", *ADD_OPENS, "-jar", JAR_PATH], env=e, cwd=REPO_ROOT, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True)
    p._output_lines = []
    threading.Thread(target=lambda: [p._output_lines.append(l) for l in p.stdout], daemon=True).start()

    def close():
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
    p.close = close
    return p
