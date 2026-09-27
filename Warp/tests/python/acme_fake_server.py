"""A local RFC 8555 ACME server for tests (NOT for production): directory, new-nonce, new-acct (JWS validated),
new-order, authorizations, http-01 validation (fetches the challenge from the client), dns-01 validation (reads TXT records
that a hook script wrote to a file), finalize with CSR parsing, signing with a generated test CA, certificate chain download,
problem+json errors (badNonce, rateLimited, arbitrary injected problems). Python 3.9+, needs `cryptography`.

Run standalone:  python3 acme_fake_server.py --port 14000 --dir /tmp/fakeacme
"""
import base64
import datetime
import hashlib
import json
import os
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def b64u(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def unb64u(s):
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


class Problem(Exception):
    def __init__(self, status, type_, detail, retry_after=None):
        self.status, self.type, self.detail, self.retry_after = status, type_, detail, retry_after


class FakeAcme:
    def __init__(self, port=0, work_dir=None, challenge_host="127.0.0.1", http_port=None, txt_file=None,
                 cert_days=60):
        self.work_dir = work_dir or os.getcwd()
        os.makedirs(self.work_dir, exist_ok=True)
        self.http_port = http_port          # where Warp's http-01 listener lives
        self.challenge_host = challenge_host
        self.txt_file = txt_file            # dns-01: lines "<txt name> <txt value>" written by the hook
        self.cert_days = cert_days
        self.txt_lines_fn = None            # optional callable returning "<name> <value>" lines (Cloudflare / Route 53 fakes)
        self.lock = threading.RLock()
        self.nonces = set()
        self.accounts = {}                  # kid url -> public key (cryptography object)
        self.account_jwks = {}              # kid url -> jwk dict
        self.orders = {}
        self.authzs = {}
        self.challenges = {}
        self.certs = {}
        self.events = []                    # (path, method) log
        # injected behaviour
        self.bad_nonce_next = 0
        self.rate_limit_new_order = None    # Retry-After seconds, or None
        self.fail_new_order = None          # (status, type, detail)
        self.processing_polls = 1           # finalize answers "processing" this many polls before "valid"
        self.validation_delay = 0.3
        self.counters = {"new_account": 0, "new_order": 0, "finalize": 0, "validated": 0, "bad_nonce_sent": 0}
        self.last_csr = None
        self.ca_key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Fake ACME Root CA")])
        now = datetime.datetime.now(datetime.timezone.utc)
        self.ca_cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(self.ca_key.public_key())
                        .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(days=1))
                        .not_valid_after(now + datetime.timedelta(days=3650))
                        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                        .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True, content_commitment=False,
                                                     key_encipherment=False, data_encipherment=False, key_agreement=False,
                                                     encipher_only=False, decipher_only=False), critical=True)
                        .sign(self.ca_key, hashes.SHA256()))
        self.ca_pem_path = os.path.join(self.work_dir, "fake-acme-ca.pem")
        with open(self.ca_pem_path, "wb") as f:
            f.write(self.ca_cert.public_bytes(serialization.Encoding.PEM))
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_HEAD(self):
                outer._dispatch(self, "HEAD")

            def do_GET(self):
                outer._dispatch(self, "GET")

            def do_POST(self):
                outer._dispatch(self, "POST")

        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.port = self.httpd.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.directory_url = self.base + "/directory"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    # ------------------------------------------------------------------ http plumbing

    def _new_nonce(self):
        n = b64u(os.urandom(16))
        with self.lock:
            self.nonces.add(n)
        return n

    def _send(self, h, status, body=None, headers=None, raw=None, ctype="application/json"):
        payload = raw if raw is not None else (b"" if body is None else json.dumps(body).encode())
        h.send_response(status)
        h.send_header("Replay-Nonce", self._new_nonce())
        h.send_header("Cache-Control", "no-store")
        h.send_header("Content-Type", ctype)
        for k, v in (headers or {}).items():
            h.send_header(k, v)
        h.send_header("Content-Length", str(len(payload)))
        h.end_headers()
        if h.command != "HEAD":
            h.wfile.write(payload)

    def _problem(self, h, p):
        headers = {}
        if p.retry_after is not None:
            headers["Retry-After"] = str(p.retry_after)
        self._send(h, p.status, {"type": p.type, "detail": p.detail, "status": p.status}, headers,
                   ctype="application/problem+json")

    def _dispatch(self, h, method):
        path = h.path
        with self.lock:
            self.events.append((method, path))
        try:
            if path == "/directory":
                return self._send(h, 200, {"newNonce": self.base + "/new-nonce", "newAccount": self.base + "/new-acct",
                                           "newOrder": self.base + "/new-order", "revokeCert": self.base + "/revoke",
                                           "meta": {"termsOfService": self.base + "/terms"}})
            if path == "/new-nonce":
                return self._send(h, 204 if method == "HEAD" else 200)
            if method != "POST":
                return self._problem(h, Problem(405, "urn:ietf:params:acme:error:malformed", "POST required"))
            length = int(h.headers.get("Content-Length", "0"))
            raw = h.rfile.read(length)
            payload, kid = self._verify_jws(h, raw)
            self._route(h, path, payload, kid)
        except Problem as p:
            self._problem(h, p)

    # ------------------------------------------------------------------ JWS

    def _verify_jws(self, h, raw):
        try:
            j = json.loads(raw)
            prot = json.loads(unb64u(j["protected"]))
        except Exception:
            raise Problem(400, "urn:ietf:params:acme:error:malformed", "bad JWS")
        with self.lock:
            if self.bad_nonce_next > 0:
                self.bad_nonce_next -= 1
                self.counters["bad_nonce_sent"] += 1
                raise Problem(400, "urn:ietf:params:acme:error:badNonce", "JWS has an invalid anti-replay nonce")
            if prot.get("nonce") not in self.nonces:
                raise Problem(400, "urn:ietf:params:acme:error:badNonce", "JWS has an invalid anti-replay nonce")
            self.nonces.discard(prot["nonce"])
        expected_url = self.base + h.path
        if prot.get("url") != expected_url:
            raise Problem(400, "urn:ietf:params:acme:error:malformed", f"JWS url {prot.get('url')} != {expected_url}")
        if prot.get("alg") != "ES256":
            raise Problem(400, "urn:ietf:params:acme:error:badSignatureAlgorithm", "only ES256 is accepted here")
        if ("jwk" in prot) == ("kid" in prot):
            raise Problem(400, "urn:ietf:params:acme:error:malformed", "exactly one of jwk / kid is required")
        kid = None
        if "jwk" in prot:
            jwk = prot["jwk"]
            if jwk.get("kty") != "EC" or jwk.get("crv") != "P-256":
                raise Problem(400, "urn:ietf:params:acme:error:badPublicKey", "need EC P-256")
            pub = ec.EllipticCurvePublicNumbers(int.from_bytes(unb64u(jwk["x"]), "big"), int.from_bytes(unb64u(jwk["y"]), "big"),
                                                ec.SECP256R1()).public_key()
        else:
            kid = prot["kid"]
            with self.lock:
                pub = self.accounts.get(kid)
            if pub is None:
                raise Problem(400, "urn:ietf:params:acme:error:accountDoesNotExist", "unknown account " + kid)
        sig = unb64u(j["signature"])
        if len(sig) != 64:
            raise Problem(400, "urn:ietf:params:acme:error:malformed", "ES256 signature must be 64 bytes (R||S)")
        der = encode_dss_signature(int.from_bytes(sig[:32], "big"), int.from_bytes(sig[32:], "big"))
        try:
            pub.verify(der, (j["protected"] + "." + j["payload"]).encode(), ec.ECDSA(hashes.SHA256()))
        except InvalidSignature:
            raise Problem(403, "urn:ietf:params:acme:error:unauthorized", "JWS verification error")
        payload = None if j["payload"] == "" else json.loads(unb64u(j["payload"]))
        h._jwk = prot.get("jwk")
        h._pub = pub
        return payload, kid

    def _thumbprint(self, kid):
        jwk = self.account_jwks[kid]
        canon = json.dumps({"crv": jwk["crv"], "kty": jwk["kty"], "x": jwk["x"], "y": jwk["y"]}, separators=(",", ":"), sort_keys=True)
        return b64u(hashlib.sha256(canon.encode()).digest())

    # ------------------------------------------------------------------ routes

    def _route(self, h, path, payload, kid):
        if path == "/new-acct":
            return self._new_account(h, payload)
        if kid is None:
            raise Problem(400, "urn:ietf:params:acme:error:malformed", "kid required")
        if path == "/new-order":
            return self._new_order(h, payload, kid)
        kind, _, ident = path.strip("/").partition("/")
        if kind == "order":
            return self._get_order(h, ident, advance=True)
        if kind == "authz":
            return self._send(h, 200, self._authz_json(ident))
        if kind == "chall":
            return self._respond_challenge(h, ident, kid)
        if kind == "finalize":
            return self._finalize(h, ident, payload)
        if kind == "cert":
            pem = self.certs.get(ident)
            if pem is None:
                raise Problem(404, "urn:ietf:params:acme:error:malformed", "no such certificate")
            return self._send(h, 200, raw=pem.encode(), ctype="application/pem-certificate-chain")
        raise Problem(404, "urn:ietf:params:acme:error:malformed", "unknown resource " + path)

    def _new_account(self, h, payload):
        if not (payload or {}).get("termsOfServiceAgreed"):
            raise Problem(400, "urn:ietf:params:acme:error:userActionRequired", "must agree to the terms of service")
        jwk = h._jwk
        with self.lock:
            for kid, existing in self.account_jwks.items():
                if existing == jwk:
                    return self._send(h, 200, {"status": "valid"}, {"Location": kid})
            if payload.get("onlyReturnExisting"):
                raise Problem(400, "urn:ietf:params:acme:error:accountDoesNotExist", "no account")
            kid = f"{self.base}/acct/{uuid.uuid4().hex[:12]}"
            self.accounts[kid] = h._pub
            self.account_jwks[kid] = jwk
            self.counters["new_account"] += 1
            self.contact = payload.get("contact")
        self._send(h, 201, {"status": "valid", "contact": payload.get("contact", [])}, {"Location": kid})

    def _new_order(self, h, payload, kid):
        with self.lock:
            if self.rate_limit_new_order is not None:
                raise Problem(429, "urn:ietf:params:acme:error:rateLimited", "too many new orders (test)", self.rate_limit_new_order)
            if self.fail_new_order:
                st, ty, de = self.fail_new_order
                raise Problem(st, ty, de)
            ids = payload.get("identifiers", [])
            if not ids or any(i.get("type") != "dns" for i in ids):
                raise Problem(400, "urn:ietf:params:acme:error:rejectedIdentifier", "dns identifiers only")
            oid = uuid.uuid4().hex[:12]
            authz_urls = []
            for i in ids:
                value = i["value"]
                wildcard = value.startswith("*.")
                base = value[2:] if wildcard else value
                aid = uuid.uuid4().hex[:12]
                chs = []
                for ctype in (["dns-01"] if wildcard else ["http-01", "dns-01"]):
                    cid = uuid.uuid4().hex[:12]
                    self.challenges[cid] = {"id": cid, "type": ctype, "token": b64u(os.urandom(24)), "status": "pending",
                                            "authz": aid, "kid": kid, "error": None}
                    chs.append(cid)
                self.authzs[aid] = {"id": aid, "domain": base, "wildcard": wildcard, "challenges": chs, "status": "pending", "order": oid}
                authz_urls.append(aid)
            self.orders[oid] = {"id": oid, "identifiers": ids, "authzs": authz_urls, "status": "pending", "kid": kid,
                                "cert": None, "polls": 0}
            self.counters["new_order"] += 1
        self._send(h, 201, self._order_json(oid), {"Location": f"{self.base}/order/{oid}"})

    def _order_json(self, oid):
        o = self.orders[oid]
        j = {"status": o["status"], "expires": (datetime.datetime.utcnow() + datetime.timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "identifiers": o["identifiers"], "authorizations": [f"{self.base}/authz/{a}" for a in o["authzs"]],
             "finalize": f"{self.base}/finalize/{oid}"}
        if o["cert"]:
            j["certificate"] = f"{self.base}/cert/{o['cert']}"
        if o.get("error"):
            j["error"] = o["error"]
        return j

    def _authz_json(self, aid):
        a = self.authzs.get(aid)
        if a is None:
            raise Problem(404, "urn:ietf:params:acme:error:malformed", "no such authz")
        chs = []
        for cid in a["challenges"]:
            c = self.challenges[cid]
            cj = {"type": c["type"], "url": f"{self.base}/chall/{cid}", "status": c["status"], "token": c["token"]}
            if c["error"]:
                cj["error"] = c["error"]
            chs.append(cj)
        j = {"status": a["status"], "identifier": {"type": "dns", "value": a["domain"]}, "challenges": chs}
        if a["wildcard"]:
            j["wildcard"] = True
        return j

    def _get_order(self, h, oid, advance=False):
        with self.lock:
            o = self.orders.get(oid)
            if o is None:
                raise Problem(404, "urn:ietf:params:acme:error:malformed", "no such order")
            if o["status"] == "pending" and all(self.authzs[a]["status"] == "valid" for a in o["authzs"]):
                o["status"] = "ready"
            if o["status"] == "pending" and any(self.authzs[a]["status"] == "invalid" for a in o["authzs"]):
                o["status"] = "invalid"
            if o["status"] == "processing":
                o["polls"] += 1
                if o["polls"] > self.processing_polls:
                    self._issue(o)
            j = self._order_json(oid)
        self._send(h, 200, j, {"Location": f"{self.base}/order/{oid}"})

    # ------------------------------------------------------------------ challenge validation

    def _respond_challenge(self, h, cid, kid):
        with self.lock:
            c = self.challenges.get(cid)
            if c is None:
                raise Problem(404, "urn:ietf:params:acme:error:malformed", "no such challenge")
            if c["status"] == "pending":
                c["status"] = "processing"
                self.authzs[c["authz"]]["status"] = "pending"
                threading.Thread(target=self._validate, args=(cid,), daemon=True).start()
            cj = {"type": c["type"], "url": f"{self.base}/chall/{cid}", "status": c["status"], "token": c["token"]}
        self._send(h, 200, cj)

    def _validate(self, cid):
        time.sleep(self.validation_delay)
        with self.lock:
            c = self.challenges[cid]
            a = self.authzs[c["authz"]]
            expected_ka = c["token"] + "." + self._thumbprint(c["kid"])
        ok, err = False, "unknown challenge type"
        if c["type"] == "http-01":
            ok, err = self._check_http(a["domain"], c["token"], expected_ka)
        elif c["type"] == "dns-01":
            ok, err = self._check_dns(a["domain"], expected_ka)
        with self.lock:
            if ok:
                c["status"] = "valid"
                a["status"] = "valid"
                self.counters["validated"] += 1
            else:
                c["status"] = "invalid"
                c["error"] = {"type": "urn:ietf:params:acme:error:unauthorized", "detail": err, "status": 403}
                a["status"] = "invalid"

    def _check_http(self, domain, token, expected):
        if not self.http_port:
            return False, "fake ACME server has no http_port configured"
        try:
            req = urllib.request.Request(f"http://{self.challenge_host}:{self.http_port}/.well-known/acme-challenge/{token}",
                                         headers={"Host": domain})
            with urllib.request.urlopen(req, timeout=5) as r:
                body = r.read().decode().strip()
        except Exception as e:  # noqa: BLE001
            return False, f"could not fetch the http-01 challenge for {domain}: {e}"
        return (body == expected), (None if body == expected else f"key authorization mismatch: got {body!r}")

    def _check_dns(self, domain, expected):
        want = b64u(hashlib.sha256(expected.encode()).digest())
        name = "_acme-challenge." + domain
        try:
            if self.txt_lines_fn is not None:
                lines = list(self.txt_lines_fn())
            else:
                with open(self.txt_file) as f:
                    lines = f.read().splitlines()
            for line in lines:
                parts = line.split()
                if len(parts) == 2 and parts[0] == name and parts[1] == want:
                    return True, None
        except OSError as e:
            return False, f"no TXT file: {e}"
        return False, f"no TXT record {name} with the expected value"

    # ------------------------------------------------------------------ finalize / issue

    def _finalize(self, h, oid, payload):
        with self.lock:
            o = self.orders.get(oid)
            if o is None:
                raise Problem(404, "urn:ietf:params:acme:error:malformed", "no such order")
            if o["status"] != "ready":
                raise Problem(403, "urn:ietf:params:acme:error:orderNotReady", f"order is {o['status']}")
            try:
                csr = x509.load_der_x509_csr(unb64u(payload["csr"]))
            except Exception as e:  # noqa: BLE001
                raise Problem(400, "urn:ietf:params:acme:error:badCSR", f"cannot parse the CSR: {e}")
            if not csr.is_signature_valid:
                raise Problem(400, "urn:ietf:params:acme:error:badCSR", "CSR signature invalid")
            san = csr.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName)
            want = sorted(i["value"] for i in o["identifiers"])
            if sorted(san) != want:
                raise Problem(400, "urn:ietf:params:acme:error:badCSR", f"CSR names {sorted(san)} != order names {want}")
            cn = csr.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            self.last_csr = {"cn": cn[0].value if cn else None, "san": san, "key": type(csr.public_key()).__name__,
                             "sig_hash": csr.signature_hash_algorithm.name}
            o["csr"] = csr
            o["status"] = "processing" if self.processing_polls > 0 else "ready"
            o["polls"] = 0
            self.counters["finalize"] += 1
            if self.processing_polls <= 0:
                self._issue(o)
            j = self._order_json(oid)
        self._send(h, 200, j, {"Location": f"{self.base}/order/{oid}"})

    def _issue(self, o):
        csr = o["csr"]
        now = datetime.datetime.now(datetime.timezone.utc)
        san = csr.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        cert = (x509.CertificateBuilder().subject_name(csr.subject).issuer_name(self.ca_cert.subject).public_key(csr.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=5))
                .not_valid_after(now + datetime.timedelta(days=self.cert_days))
                .add_extension(san, critical=False)
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
                .sign(self.ca_key, hashes.SHA256()))
        pem = cert.public_bytes(serialization.Encoding.PEM).decode() + self.ca_cert.public_bytes(serialization.Encoding.PEM).decode()
        cid = uuid.uuid4().hex[:12]
        self.certs[cid] = pem
        o["cert"] = cid
        o["status"] = "valid"


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=14000)
    ap.add_argument("--dir", default=".")
    ap.add_argument("--http-port", type=int)
    ap.add_argument("--txt-file")
    a = ap.parse_args()
    s = FakeAcme(a.port, a.dir, http_port=a.http_port, txt_file=a.txt_file)
    print("directory:", s.directory_url, "CA:", s.ca_pem_path, flush=True)
    threading.Event().wait()
