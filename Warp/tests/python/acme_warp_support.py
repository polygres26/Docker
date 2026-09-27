"""Helpers for test_acme.py: launch a real Warp jar with WARP_ACME_* pointed at the local fake ACME server, TLS clients that
trust only the fake CA and connect to 127.0.0.1 while presenting the ACME domain name (SNI + hostname verification), a DNS hook
script, and fake DNS-over-HTTPS / Cloudflare / Route 53 endpoints."""
import hashlib
import hmac
import json
import os
import re
import socket
import ssl
import stat
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from requests.adapters import HTTPAdapter

from warp_test_support import WarpProcess, free_port, isolated_ports
from mcp_support import ADMIN_TOKEN

DOMAIN = "warp.acme-test.example"
DOMAIN2 = "mcp.acme-test.example"


def first_bindable_cluster_port():
    for port in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("no free port in 47500..47599")


def start_acme_warp(pg, env, name="acme"):
    ports = isolated_ports(exclude="WARP_MCP_PORT")
    proc = WarpProcess(pg, "WARP_MCP_PORT", frontend_name=name, extra_env={
        **ports,
        "WARP_ADMIN_TOKEN": ADMIN_TOKEN,
        "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
        "WARP_BACKENDS": f"default=jdbc:postgresql://localhost:{pg.port}/postgres|postgres|postgres",
        "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
        "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_bindable_cluster_port()}",
        **env})
    proc.mcp_https = int(env["WARP_MCP_HTTPS_PORT"]) if "WARP_MCP_HTTPS_PORT" in env else None
    proc.admin_https = int(env["WARP_ADMIN_HTTPS_PORT"]) if "WARP_ADMIN_HTTPS_PORT" in env else None
    proc.s3_https = int(env["WARP_S3WIRE_HTTPS_PORT"]) if "WARP_S3WIRE_HTTPS_PORT" in env else None
    proc.s3_port = int(ports["WARP_S3WIRE_PORT"])
    return proc


def log_text(proc):
    return "".join(proc._output_lines)


class HostAdapter(HTTPAdapter):
    """Connects to 127.0.0.1 but verifies the certificate for `domain` (no DNS for the fake domain exists)."""

    def __init__(self, domain, *a, **kw):
        self._domain = domain
        super().__init__(*a, **kw)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["server_hostname"] = self._domain
        kwargs["assert_hostname"] = self._domain
        super().init_poolmanager(*args, **kwargs)


def session(ca, domain=DOMAIN):
    s = requests.Session()
    s.mount("https://", HostAdapter(domain))
    s.verify = ca
    return s


def peer_cert(port, ca, domain=DOMAIN):
    """TLS handshake trusting only `ca`; returns (parsed cert dict, DER bytes). Raises ssl.SSLError when untrusted."""
    ctx = ssl.create_default_context(cafile=ca)
    with socket.create_connection(("127.0.0.1", port), timeout=10) as raw:
        with ctx.wrap_socket(raw, server_hostname=domain) as tls:
            return tls.getpeercert(), tls.getpeercert(binary_form=True)


def der_sha256(der):
    return hashlib.sha256(der).hexdigest()


def admin_get(warp, path, ca=None, https=False, method="GET", body=None):
    if https:
        return session(ca).request(method, f"https://127.0.0.1:{warp.admin_https}{path}", json=body, timeout=30,
                                   headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
    return requests.request(method, f"http://localhost:{warp.metrics_port}{path}", json=body, timeout=30,
                            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})


def certs(warp):
    return admin_get(warp, "/api/tls/certificates").json()


def wait_for(cond, timeout=60, interval=0.25, what="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = cond()
            if last:
                return last
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what} (last: {last!r})")


def wait_acme_cert(warp, timeout=90, sha_not=None):
    """Waits for a non-placeholder ACME certificate (different from `sha_not`)."""
    def cond():
        j = certs(warp)
        a = j["acme"]
        c = a.get("certificate")
        if c and not c["placeholder"] and c["sha256"] != sha_not:
            return j
        return False
    return wait_for(cond, timeout, what="issued ACME certificate")


def write_hook(dir_, txt_file, log_file):
    path = os.path.join(dir_, "acme-dns-hook.sh")
    with open(path, "w") as f:
        f.write(f"""#!/bin/sh
echo "$ACME_ACTION $ACME_DOMAIN $ACME_TXT_NAME" >> "{log_file}"
case "$ACME_ACTION" in
  present) echo "$ACME_TXT_NAME $ACME_TXT_VALUE" >> "{txt_file}" ;;
  cleanup) grep -v "^$ACME_TXT_NAME $ACME_TXT_VALUE\\$" "{txt_file}" > "{txt_file}.tmp"; mv "{txt_file}.tmp" "{txt_file}" ;;
esac
exit 0
""")
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
    return path


def serve(handler_cls):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


class FakeDoh:
    """DNS-over-HTTPS JSON endpoint answering TXT queries from the hook's TXT file."""

    def __init__(self, txt_file):
        outer = self
        self.queries = []

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                name = q.get("name", [""])[0]
                outer.queries.append(name)
                answers = []
                try:
                    for line in open(txt_file):
                        p = line.split()
                        if len(p) == 2 and p[0] == name:
                            answers.append({"name": name, "type": 16, "TTL": 60, "data": f'"{p[1]}"'})
                except OSError:
                    pass
                body = json.dumps({"Status": 0, "Answer": answers}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/dns-json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = serve(H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/dns-query"

    def close(self):
        self.httpd.shutdown()


class FakeCloudflare:
    """Cloudflare v4 subset: GET /zones?name=, POST/GET/DELETE /zones/{id}/dns_records. Records feed a TXT file."""

    def __init__(self, zone="acme-test.example", token="cf-test-token"):
        outer = self
        self.zone, self.token = zone, token
        self.records = {}   # id -> {name, content}
        self.calls = []
        self.auth_failures = 0

        def txt_lines():
            return [f"{r['name']} {r['content']}" for r in outer.records.values()]
        self.txt_lines = txt_lines

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, status, body):
                b = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _handle(self, method):
                u = urllib.parse.urlparse(self.path)
                q = urllib.parse.parse_qs(u.query)
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length)) if length else None
                outer.calls.append((method, u.path, dict((k, v[0]) for k, v in q.items()), body))
                if self.headers.get("Authorization") != f"Bearer {outer.token}":
                    outer.auth_failures += 1
                    return self._send(403, {"success": False, "errors": [{"message": "bad token"}], "result": None})
                if method == "GET" and u.path == "/client/v4/zones":
                    name = q.get("name", [""])[0]
                    res = [{"id": "zone123", "name": name}] if name == outer.zone else []
                    return self._send(200, {"success": True, "result": res})
                m = re.match(r"/client/v4/zones/zone123/dns_records(?:/(\w+))?$", u.path)
                if m:
                    if method == "POST":
                        rid = f"rec{len(outer.records) + 1}{int(time.time() * 1000) % 100000}"
                        outer.records[rid] = {"name": body["name"], "content": body["content"], "type": body["type"], "ttl": body.get("ttl")}
                        return self._send(200, {"success": True, "result": {"id": rid}})
                    if method == "GET":
                        res = [dict(id=i, **r) for i, r in outer.records.items()
                               if r["name"] == q.get("name", [r["name"]])[0] and r["content"] == q.get("content", [r["content"]])[0]]
                        return self._send(200, {"success": True, "result": res})
                    if method == "DELETE" and m.group(1) in outer.records:
                        del outer.records[m.group(1)]
                        return self._send(200, {"success": True, "result": {"id": m.group(1)}})
                self._send(404, {"success": False, "errors": [{"message": "not found " + u.path}]})

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

            def do_DELETE(self):
                self._handle("DELETE")

        self.httpd = serve(H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/client/v4"

    def close(self):
        self.httpd.shutdown()


class FakeRoute53:
    """Route 53 subset with an INDEPENDENT (python) SigV4 verification of every request."""

    def __init__(self, zone="acme-test.example", access="AKIATESTACMEKEY", secret="secretsecretsecret", token=None):
        outer = self
        self.zone, self.access, self.secret, self.token = zone, access, secret, token
        self.records = []           # [(name, value)]
        self.calls = []
        self.sig_failures = 0

        def verify(handler, body):
            auth = handler.headers.get("Authorization", "")
            m = re.match(r"AWS4-HMAC-SHA256 Credential=([^/]+)/(\d{8})/([^/]+)/([^/]+)/aws4_request, SignedHeaders=([^,]+), Signature=(\w+)", auth)
            if not m or m.group(1) != outer.access:
                return False
            key_id, date, region, service, signed, sig = m.groups()
            u = urllib.parse.urlparse(handler.path)
            q = urllib.parse.parse_qsl(u.query, keep_blank_values=True)
            cq = "&".join(f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}" for k, v in sorted(q))
            hdrs = "".join(f"{h}:{handler.headers.get(h).strip()}\n" for h in signed.split(";"))
            payload_hash = hashlib.sha256(body).hexdigest()
            if handler.headers.get("x-amz-content-sha256") != payload_hash:
                return False
            canon = "\n".join([handler.command, u.path, cq, hdrs, signed, payload_hash])
            scope = f"{date}/{region}/{service}/aws4_request"
            to_sign = "\n".join(["AWS4-HMAC-SHA256", handler.headers["x-amz-date"], scope, hashlib.sha256(canon.encode()).hexdigest()])
            k = ("AWS4" + outer.secret).encode()
            for part in (date, region, service, "aws4_request"):
                k = hmac.new(k, part.encode(), hashlib.sha256).digest()
            return hmac.compare_digest(hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest(), sig) and service == "route53"

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, status, xml):
                b = xml.encode()
                self.send_response(status)
                self.send_header("Content-Type", "text/xml")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _handle(self, method):
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length) if length else b""
                u = urllib.parse.urlparse(self.path)
                outer.calls.append((method, u.path, u.query))
                if not verify(self, body):
                    outer.sig_failures += 1
                    return self._send(403, "<ErrorResponse><Error><Code>SignatureDoesNotMatch</Code><Message>bad signature</Message></Error></ErrorResponse>")
                if method == "GET" and u.path == "/2013-04-01/hostedzonesbyname":
                    dn = urllib.parse.parse_qs(u.query)["dnsname"][0]
                    name = outer.zone + "." if dn == outer.zone else "zzz-other."
                    return self._send(200, f"<ListHostedZonesByNameResponse><HostedZones><HostedZone><Id>/hostedzone/ZTEST1</Id><Name>{name}</Name></HostedZone></HostedZones></ListHostedZonesByNameResponse>")
                if method == "POST" and u.path == "/2013-04-01/hostedzone/ZTEST1/rrset":
                    x = body.decode()
                    action = re.search(r"<Action>(\w+)</Action>", x).group(1)
                    name = re.search(r"<Name>([^<]+)</Name>", x).group(1)
                    value = re.search(r"<Value>&quot;([^&]+)&quot;</Value>", x).group(1)
                    if action == "UPSERT":
                        outer.records = [r for r in outer.records if r[0] != name] + [(name, value)]
                    elif action == "DELETE":
                        outer.records = [r for r in outer.records if r != (name, value)]
                    return self._send(200, "<ChangeResourceRecordSetsResponse><ChangeInfo><Id>/change/CTEST1</Id><Status>PENDING</Status></ChangeInfo></ChangeResourceRecordSetsResponse>")
                if method == "GET" and u.path == "/2013-04-01/change/CTEST1":
                    return self._send(200, "<GetChangeResponse><ChangeInfo><Id>/change/CTEST1</Id><Status>INSYNC</Status></ChangeInfo></GetChangeResponse>")
                self._send(404, "<ErrorResponse><Error><Code>NoSuchHostedZone</Code><Message>nope</Message></Error></ErrorResponse>")

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

        self.httpd = serve(H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def txt_lines(self):
        return [f"{n.rstrip('.')} {v}" for n, v in self.records]

    def close(self):
        self.httpd.shutdown()
