"""Built-in ACME (RFC 8555) certificate issuance / renewal against a LOCAL fake ACME server (acme_fake_server.py) with a real
Warp jar: http-01 and dns-01 (hook + DoH self-check, wildcard, Cloudflare and Route 53 API fakes), HTTPS on admin / MCP / S3 with
the issued chain and a client trusting only the fake CA, live renewal without restart, failed / rate-limited / badNonce renewals
keep the old certificate, restart reuses account and certificate, no private keys in logs, bad configuration leaves plaintext
running, and two nodes on one Postgres issuing once. NOTHING here talks to a real ACME CA."""
import json
import os
import re
import ssl
import stat
import time

import pytest
import requests

from acme_fake_server import FakeAcme
from acme_warp_support import (DOMAIN, DOMAIN2, FakeCloudflare, FakeDoh, FakeRoute53, admin_get, certs, der_sha256, log_text,
                               peer_cert, session, start_acme_warp, wait_acme_cert, wait_for, write_hook)
from warp_test_support import RealPostgres, free_port


def base_env(tmp, fake, **extra):
    return {
        "WARP_ACME_DOMAINS": f"{DOMAIN},{DOMAIN2}", "WARP_ACME_EMAIL": "ops@acme-test.example",
        "WARP_ACME_TERMS_ACCEPTED": "true", "WARP_ACME_DIRECTORY": fake.directory_url,
        "WARP_ACME_DIR": str(tmp / "acme"), "WARP_ACME_POLL_MILLIS": "100", "WARP_TLS_RELOAD_SECONDS": "1",
        "WARP_MCP_HTTPS_PORT": str(free_port()), "WARP_ADMIN_HTTPS_PORT": str(free_port()),
        "WARP_S3WIRE_HTTPS_PORT": str(free_port()), "WARP_ACME_RETRY_SECONDS": "3",
        "WARP_ACME_CHECK_SECONDS": "5", "WARP_ACME_SYNC_SECONDS": "1", "WARP_S3WIRE_ENABLED": "true", "WARP_S3WIRE_CREDENTIALS": "testkey=testsecrettestsecret",
        **extra}


def mcp_rpc(url, sess, method, params=None, token=None):
    r = sess.post(url, headers={"Authorization": f"Bearer {token}"} if token else {}, timeout=30,
                  json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})
    assert r.status_code == 200, (r.status_code, r.text)
    return r.json()


def served_sha(port, ca):
    return der_sha256(peer_cert(port, ca)[1])


def acme(warp):
    return certs(warp)["acme"]


# ================================================================== (a) http-01 + (b) HTTPS + (e) key hygiene

@pytest.fixture(scope="module")
def http01(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("acme-http01")
    http_port = free_port()
    fake = FakeAcme(work_dir=str(tmp), http_port=http_port, cert_days=60)
    pg = RealPostgres()
    warp = start_acme_warp(pg, base_env(tmp, fake, WARP_ACME_HTTP_PORT=str(http_port)))
    warp.fake, warp.tmp, warp.http_port = fake, tmp, http_port
    yield warp
    warp.close()
    pg.close()
    fake.close()


def test_http01_issues_certificate_and_api_reports_it(http01):
    j = wait_acme_cert(http01)
    c = j["acme"]["certificate"]
    assert sorted(c["domainNames"]) == sorted([DOMAIN, DOMAIN2])
    assert "Fake ACME Root CA" in c["issuer"]
    assert j["acme"]["challenge"] == "http-01" and j["acme"]["lastError"] is None
    assert http01.fake.counters["new_order"] == 1 and http01.fake.counters["new_account"] == 1
    csr = http01.fake.last_csr
    assert csr["cn"] == DOMAIN and sorted(csr["san"]) == sorted([DOMAIN, DOMAIN2]) and "EC" in csr["key"]
    assert csr["sig_hash"] == "sha256"
    entry = j["certificates"][0]
    assert entry["source"] == "acme" and entry["daysLeft"] >= 58 and entry["directory"] == http01.fake.directory_url
    assert entry["challenge"] == "http-01" and entry["lastRenewal"] and entry["nextCheck"]
    assert entry["staging"] is False and set(entry["listeners"].split(",")) >= {"ADMIN", "MCP"}
    


def test_http01_listener_only_serves_challenges(http01):
    base = f"http://127.0.0.1:{http01.http_port}"
    assert requests.get(base + "/", timeout=5).status_code == 404
    assert requests.get(base + "/api/tls/certificates", timeout=5).status_code == 404
    assert requests.get(base + "/.well-known/acme-challenge/nope", timeout=5).status_code == 404
    assert requests.post(base + "/.well-known/acme-challenge/x", timeout=5).status_code == 404


def test_https_on_admin_mcp_s3_serves_issued_chain_to_client_trusting_only_test_ca(http01):
    wait_acme_cert(http01)
    ca = http01.fake.ca_pem_path
    issued = acme(http01)["certificate"]["sha256"]
    for port in (http01.admin_https, http01.mcp_https, http01.s3_https):
        cert, der = peer_cert(port, ca)
        assert der_sha256(der) == issued, port
        assert dict(x[0] for x in cert["issuer"])["commonName"] == "Fake ACME Root CA"
        assert {v for k, v in cert["subjectAltName"]} == {DOMAIN, DOMAIN2}
    with pytest.raises(ssl.SSLError):  # a client that does not trust the test CA refuses
        peer_cert(http01.mcp_https, ssl.get_default_verify_paths().cafile or "/etc/ssl/cert.pem")
    # S3 speaks HTTPS with the same identity
    r = session(ca).get(f"https://127.0.0.1:{http01.s3_https}/", timeout=10)
    assert r.status_code in (200, 403, 404)


def test_mcp_over_https_with_trusted_test_ca_and_connector_url(http01):
    wait_acme_cert(http01)
    s = session(http01.fake.ca_pem_path)
    ep = admin_get(http01, "/api/mcp-endpoints", method="POST", body={"name": "acme-ep", "scope": "all", "urlToken": True}).json()
    assert ep["url"] == f"https://{DOMAIN}:{http01.mcp_https}{ep['path']}", ep["url"]
    assert ep["urlWithToken"].startswith(f"https://{DOMAIN}:{http01.mcp_https}/e/")
    base = f"https://127.0.0.1:{http01.mcp_https}"
    wait_for(lambda: s.post(base + ep["path"], headers={"Authorization": f"Bearer {ep['token']}"}, timeout=10,
                            json={"jsonrpc": "2.0", "id": 0, "method": "tools/list"}).status_code == 200, 20, what="endpoint live")
    init = mcp_rpc(base + ep["path"], s, "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                                          "clientInfo": {"name": "acme-t", "version": "1"}}, ep["token"])
    assert init["result"]["serverInfo"]
    res = mcp_rpc(base + ep["path"], s, "tools/call", {"name": "execute_sql", "arguments": {"sql": "SELECT 6*7 AS a"}}, ep["token"])
    assert "42" in json.dumps(res["result"]["content"])
    assert "tools" in mcp_rpc(base + f"{ep['path']}/t/{ep['token']}", s, "tools/list")["result"]   # header-less URL token
    cfg = admin_get(http01, "/api/mcp-config").json()
    assert cfg["baseUrl"] == f"https://{DOMAIN}:{http01.mcp_https}" and cfg["claudeConnectorReady"] is True
    assert cfg["acmeEnabled"] is True and cfg["acmePending"] is False


def test_private_key_files_are_mode_600_and_never_logged(http01):
    wait_acme_cert(http01)
    d = http01.tmp / "acme"
    for name in ("privkey.pem", "account.key"):
        assert stat.S_IMODE(os.stat(d / name).st_mode) == 0o600, name
    logs = log_text(http01)
    for name in ("privkey.pem", "account.key"):
        for line in (d / name).read_text().splitlines():
            if not line.startswith("-----"):
                assert line not in logs, name
    assert "PRIVATE KEY" not in logs and "ACME: issued certificate" in logs


def test_manual_renew_is_admin_only_and_guarded_against_hammering(http01):
    wait_acme_cert(http01)
    assert requests.post(f"http://localhost:{http01.metrics_port}/api/tls/renew", timeout=10).status_code == 401
    r = admin_get(http01, "/api/tls/renew", method="POST")   # issuance was seconds ago
    assert r.status_code == 429 and int(r.headers["Retry-After"]) > 0 and "retryAt" in r.json(), r.text
    assert http01.fake.counters["new_order"] == 1


# ================================================================== (c) live renewal + (d) failed renewal + badNonce / rateLimited

@pytest.fixture(scope="module")
def renewing(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("acme-renew")
    http_port = free_port()
    fake = FakeAcme(work_dir=str(tmp), http_port=http_port, cert_days=60)
    # renew-days far beyond the lifetime = "always due"; the 6 s floor keeps it from re-ordering in a loop
    pg = RealPostgres()
    warp = start_acme_warp(pg, base_env(tmp, fake, WARP_ACME_HTTP_PORT=str(http_port), WARP_ACME_RENEW_DAYS="3650",
                                        WARP_ACME_MIN_INTERVAL_SECONDS="6", WARP_ACME_MANUAL_MIN_SECONDS="1"))
    warp.fake, warp.tmp = fake, tmp
    yield warp
    warp.close()
    pg.close()
    fake.close()


def test_renewal_swaps_the_certificate_live_without_restart(renewing):
    ca = renewing.fake.ca_pem_path
    first = wait_acme_cert(renewing)["acme"]["certificate"]["sha256"]
    pid = renewing.process.pid
    second = wait_acme_cert(renewing, sha_not=first, timeout=60)["acme"]["certificate"]["sha256"]
    assert second != first and renewing.process.pid == pid and renewing.process.poll() is None
    # every HTTPS listener now presents the renewed certificate (hot reload), verified by a real handshake
    wait_for(lambda: served_sha(renewing.mcp_https, ca) == second or wait_acme_cert(renewing)["acme"]["certificate"]["sha256"] != second,
             30, what="listener presents renewed cert")
    latest = acme(renewing)["certificate"]["sha256"]
    for port in (renewing.admin_https, renewing.mcp_https, renewing.s3_https):
        wait_for(lambda p=port: served_sha(p, ca) == acme(renewing)["certificate"]["sha256"], 30, what=f"port {port} in sync")
    assert renewing.fake.counters["new_order"] >= 2 and "TLS material reloaded" in log_text(renewing)


def test_manual_renew_forces_a_renewal_now(renewing):
    n = renewing.fake.counters["new_order"]
    time.sleep(1.5)
    r = admin_get(renewing, "/api/tls/renew", method="POST")
    assert r.status_code == 202, r.text
    wait_for(lambda: renewing.fake.counters["new_order"] > n, 30, what="forced order")


def test_badnonce_is_retried_transparently(renewing):
    renewing.fake.bad_nonce_next = 2
    before = renewing.fake.counters["bad_nonce_sent"]
    wait_for(lambda: renewing.fake.counters["bad_nonce_sent"] >= before + 2, 40, what="badNonce injected")
    time.sleep(1)
    wait_for(lambda: acme(renewing)["lastError"] is None and not acme(renewing)["issuing"], 40, what="recovered from badNonce")


def test_failed_renewal_keeps_old_certificate_reports_error_then_recovers(renewing):
    ca = renewing.fake.ca_pem_path
    wait_for(lambda: not acme(renewing)["issuing"], 30, what="idle")
    fake = renewing.fake
    fake.fail_new_order = (500, "urn:ietf:params:acme:error:serverInternal", "test outage")
    try:
        # wait until an attempt has failed
        wait_for(lambda: acme(renewing)["lastError"], 60, what="failure reported")
        frozen = acme(renewing)["certificate"]["sha256"]
        a = acme(renewing)
        assert "serverInternal" in a["lastError"] and "test outage" in a["lastError"] and a["failures"] >= 1
        assert a["backoffUntil"] and a["lastErrorAt"]
        entry = certs(renewing)["certificates"][0]
        assert entry["lastError"] and entry["daysLeft"] >= 58
        # the old certificate is still what every listener serves
        for port in (renewing.admin_https, renewing.mcp_https):
            assert served_sha(port, ca) == frozen
        time.sleep(3)
        assert acme(renewing)["certificate"]["sha256"] == frozen or acme(renewing)["lastError"]
        assert "ACME issuance for" in log_text(renewing) and "Keeping the current certificate" in log_text(renewing)
    finally:
        fake.fail_new_order = None
    last = acme(renewing)["certificate"]["sha256"]
    wait_acme_cert(renewing, sha_not=last, timeout=90)   # recovers on its own once the CA is back
    assert acme(renewing)["lastError"] is None


def test_rate_limited_backs_off_and_respects_retry_after(renewing):
    fake = renewing.fake
    fake.rate_limit_new_order = 120
    try:
        wait_for(lambda: acme(renewing)["lastError"] and "rateLimited" in acme(renewing)["lastError"], 60, what="rateLimited reported")
        a = acme(renewing)
        import datetime
        until = datetime.datetime.strptime(a["backoffUntil"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
        assert (until - datetime.datetime.now(datetime.timezone.utc)).total_seconds() > 90, a   # honours Retry-After (120s), not the 3s retry base
        orders = fake.counters["new_order"]
        # a manual renewal is refused while the CA is rate-limiting us
        r = admin_get(renewing, "/api/tls/renew", method="POST")
        assert r.status_code == 429 and "rate-limited" in r.json()["error"], r.text
        time.sleep(6)
        assert fake.counters["new_order"] == orders   # no hammering
    finally:
        fake.rate_limit_new_order = None


# ================================================================== (a) dns-01 through the hook (+ DoH self check, wildcard)

@pytest.fixture(scope="module")
def dns01(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("acme-dns01")
    txt = tmp / "txt.records"
    txt.write_text("")
    hook_log = tmp / "hook.log"
    fake = FakeAcme(work_dir=str(tmp), txt_file=str(txt), cert_days=60)
    doh = FakeDoh(str(txt))
    hook = write_hook(str(tmp), str(txt), str(hook_log))
    env = base_env(tmp, fake, WARP_ACME_DOMAINS="acme-test.example,*.acme-test.example", WARP_ACME_CHALLENGE="dns-01",
                   WARP_ACME_DNS_PROVIDER="hook", WARP_ACME_DNS_HOOK=hook, WARP_ACME_DNS_WAIT_SECONDS="10",
                   WARP_ACME_DNS_CHECK="doh", WARP_ACME_DOH_URL=doh.url)
    pg = RealPostgres()
    warp = start_acme_warp(pg, env)
    warp.fake, warp.tmp, warp.hook_log, warp.doh, warp.txt = fake, tmp, hook_log, doh, txt
    yield warp
    warp.close()
    pg.close()
    fake.close()
    doh.close()


def test_dns01_hook_issues_wildcard_certificate(dns01):
    j = wait_acme_cert(dns01)
    c = j["acme"]["certificate"]
    assert sorted(c["domainNames"]) == ["*.acme-test.example", "acme-test.example"] and j["acme"]["challenge"] == "dns-01"
    log = dns01.hook_log.read_text().splitlines()
    name = "_acme-challenge.acme-test.example"
    # two authorizations (apex + wildcard) share one TXT name: presented and cleaned up one after the other
    assert log.count(f"present acme-test.example {name}") == 2 and log.count(f"cleanup acme-test.example {name}") == 2, log
    assert dns01.txt.read_text().strip() == ""   # the hook removed the records again
    assert dns01.fake.last_csr["cn"] == "acme-test.example"
    assert dns01.doh.queries and set(dns01.doh.queries) == {name}   # the DoH self-check saw the TXT record
    ca = dns01.fake.ca_pem_path
    cert, der = peer_cert(dns01.mcp_https, ca, domain="anything.acme-test.example")   # wildcard covers it
    assert der_sha256(der) == c["sha256"]


# ================================================================== (a) dns-01 through the Cloudflare and Route 53 APIs

def _api_fixture(tmp_path_factory, kind):
    tmp = tmp_path_factory.mktemp("acme-" + kind)
    fake = FakeAcme(work_dir=str(tmp), cert_days=60)
    if kind == "cloudflare":
        api = FakeCloudflare()
        fake.txt_lines_fn = api.txt_lines
        prov = {"WARP_ACME_DNS_PROVIDER": "cloudflare", "CLOUDFLARE_API_TOKEN": api.token, "WARP_ACME_CLOUDFLARE_API": api.url}
    else:
        api = FakeRoute53()
        fake.txt_lines_fn = api.txt_lines
        prov = {"WARP_ACME_DNS_PROVIDER": "route53", "AWS_ACCESS_KEY_ID": api.access, "AWS_SECRET_ACCESS_KEY": api.secret,
                "WARP_ACME_ROUTE53_ENDPOINT": api.url}
    env = base_env(tmp, fake, WARP_ACME_CHALLENGE="dns-01", WARP_ACME_DNS_WAIT_SECONDS="1", **prov)
    pg = RealPostgres()
    warp = start_acme_warp(pg, env)
    warp.fake, warp.api, warp.pg = fake, api, pg
    return warp


@pytest.fixture(scope="module")
def cf(tmp_path_factory):
    w = _api_fixture(tmp_path_factory, "cloudflare")
    yield w
    w.close()
    w.pg.close()
    w.fake.close()
    w.api.close()


@pytest.fixture(scope="module")
def r53(tmp_path_factory):
    w = _api_fixture(tmp_path_factory, "route53")
    yield w
    w.close()
    w.pg.close()
    w.fake.close()
    w.api.close()


def test_cloudflare_dns01_end_to_end_against_fake_api(cf):
    wait_acme_cert(cf)
    api = cf.api
    assert api.auth_failures == 0 and api.records == {}, api.records   # TXT records were removed again
    posts = [c for c in api.calls if c[0] == "POST"]
    assert len(posts) == 2 and all(p[3]["type"] == "TXT" and p[3]["name"] == f"_acme-challenge.{d}" and p[3]["ttl"] == 60
                                   for p, d in zip(posts, (DOMAIN, DOMAIN2)))
    zone_lookups = [c[2]["name"] for c in api.calls if c[0] == "GET" and c[1].endswith("/zones")]
    assert zone_lookups[:3] == [DOMAIN, "acme-test.example"][:0] + [DOMAIN, "acme-test.example"] + zone_lookups[2:3]
    assert sum(1 for c in api.calls if c[0] == "DELETE") == 2


def test_route53_dns01_end_to_end_against_fake_api_with_independent_sigv4_check(r53):
    wait_acme_cert(r53)
    api = r53.api
    assert api.sig_failures == 0 and api.records == [], (api.sig_failures, api.records)
    assert sum(1 for c in api.calls if c[0] == "POST") == 4   # UPSERT + DELETE per authorization
    assert any(c[1].startswith("/2013-04-01/change/") for c in api.calls)


# ================================================================== (e) restart reuses the account and the certificate

def test_restart_reuses_account_and_certificate_no_new_order(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("acme-restart")
    http_port = free_port()
    fake = FakeAcme(work_dir=str(tmp), http_port=http_port, cert_days=60)
    pg = RealPostgres()
    env = base_env(tmp, fake, WARP_ACME_HTTP_PORT=str(http_port))
    w1 = start_acme_warp(pg, env)
    try:
        sha = wait_acme_cert(w1)["acme"]["certificate"]["sha256"]
    finally:
        w1.close()
    assert fake.counters["new_order"] == 1 and fake.counters["new_account"] == 1
    env2 = dict(env, WARP_MCP_HTTPS_PORT=str(free_port()), WARP_ADMIN_HTTPS_PORT=str(free_port()), WARP_S3WIRE_HTTPS_PORT=str(free_port()))
    w2 = start_acme_warp(pg, env2)
    try:
        j = wait_acme_cert(w2, timeout=30)
        assert j["acme"]["certificate"]["sha256"] == sha
        assert served_sha(w2.mcp_https, fake.ca_pem_path) == sha   # served from the very first handshake (no placeholder phase)
        time.sleep(8)   # give the startup check time to (wrongly) order
        assert fake.counters["new_order"] == 1 and fake.counters["new_account"] == 1
        assert "existing certificate" in log_text(w2) and "placeholder" not in log_text(w2)
    finally:
        w2.close()
        pg.close()
        fake.close()


# ================================================================== (g) bad config never stops Warp

def test_bad_config_leaves_warp_running_plaintext(tmp_path):
    fake = FakeAcme(work_dir=str(tmp_path))
    cases = {
        "terms not accepted": ({"WARP_ACME_TERMS_ACCEPTED": "false"}, "WARP_ACME_TERMS_ACCEPTED"),
        "wildcard needs dns-01": ({"WARP_ACME_DOMAINS": "*.acme-test.example"}, "wildcard"),
        "bad challenge": ({"WARP_ACME_CHALLENGE": "tls-alpn-01"}, "WARP_ACME_CHALLENGE"),
        "ip address": ({"WARP_ACME_DOMAINS": "127.0.0.1"}, "IP addresses"),
    }
    for label, (extra, needle) in cases.items():
        pg = RealPostgres()   # a dedicated Postgres per case: the Developer-license instance cap otherwise counts
        env = base_env(tmp_path, fake, **extra)          # the previous case's still-fresh heartbeat row against this one
        w = start_acme_warp(pg, env)
        try:
            assert requests.get(f"http://localhost:{w.metrics_port}/metrics", timeout=10).status_code in (200, 401)
            a = admin_get(w, "/api/tls/certificates").json()["acme"]
            assert a["enabled"] is False and needle in a["reason"], (label, a)
            assert f"ACME is NOT enabled: " in log_text(w) and needle in log_text(w)
            with pytest.raises(requests.exceptions.ConnectionError):   # no HTTPS listener without TLS material
                requests.get(f"https://127.0.0.1:{w.mcp_https}/", timeout=3, verify=False)
            assert requests.post(f"http://localhost:{w.frontend_port}/", timeout=10, json={"jsonrpc": "2.0", "id": 1,
                                                                                              "method": "tools/list"}).status_code in (200, 401)
            assert admin_get(w, "/api/tls/renew", method="POST").status_code == 409
        finally:
            w.close()
            pg.close()
    fake.close()


def test_unreachable_ca_keeps_warp_up_on_placeholder_and_reports_error(tmp_path):
    pg = RealPostgres()
    dead = f"http://127.0.0.1:{free_port()}/directory"
    fake = FakeAcme(work_dir=str(tmp_path))
    env = base_env(tmp_path, fake, WARP_ACME_DIRECTORY=dead, WARP_ACME_HTTP_PORT=str(free_port()))
    w = start_acme_warp(pg, env)
    try:
        a = wait_for(lambda: acme(w) if acme(w).get("lastError") else False, 40, what="error reported")
        assert "cannot reach the ACME server" in a["lastError"] and a["certificate"]["placeholder"] is True
        # HTTPS is up with the temporary self-signed placeholder, and the UI is told the connector is NOT ready
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        import socket
        with socket.create_connection(("127.0.0.1", w.mcp_https), timeout=10) as raw, ctx.wrap_socket(raw) as tls:
            assert der_sha256(tls.getpeercert(binary_form=True)) == a["certificate"]["sha256"]
        cfg = admin_get(w, "/api/mcp-config").json()
        assert cfg["acmePending"] is True and cfg["claudeConnectorReady"] is False and cfg["acmeError"]
        entry = certs(w)["certificates"][0]
        assert entry["source"] == "self-signed"
        assert a["backoffUntil"] and a["failures"] >= 1
    finally:
        w.close()
        pg.close()
        fake.close()


def test_explicit_tls_cert_wins_over_acme(tmp_path):
    pg = RealPostgres()
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    import datetime
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "explicit.acme-test.example")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(1)
            .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=30))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("explicit.acme-test.example")]), False)
            .sign(key, hashes.SHA256()))
    (tmp_path / "c.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (tmp_path / "k.pem").write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                       serialization.NoEncryption()))
    fake = FakeAcme(work_dir=str(tmp_path), http_port=0)
    env = base_env(tmp_path, fake, WARP_TLS_CERT=str(tmp_path / "c.pem"), WARP_TLS_KEY=str(tmp_path / "k.pem"),
                   WARP_ACME_HTTP_PORT=str(free_port()))
    w = start_acme_warp(pg, env)
    try:
        j = certs(w)
        assert j["certificates"][0]["source"] == "file" and "explicit.acme-test.example" in j["certificates"][0]["domainNames"]
        assert j["acme"]["enabled"] is True
    finally:
        w.close()
        pg.close()
        fake.close()


# ================================================================== (h) two nodes, one Postgres: one order, one certificate

def test_two_nodes_share_one_postgres_issue_once_and_serve_the_same_certificate(tmp_path_factory):
    pg = RealPostgres()
    tmp = tmp_path_factory.mktemp("acme-multi")
    txt = tmp / "txt.records"
    txt.write_text("")
    hook = write_hook(str(tmp), str(txt), str(tmp / "hook.log"))
    fake = FakeAcme(work_dir=str(tmp), txt_file=str(txt), cert_days=60)
    fake.validation_delay = 1.5   # widen the window in which the second node would otherwise also order
    key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="   # base64 of 32 bytes: test-only SAYONORA_ENCRYPTION_KEY
    common = {"WARP_ACME_CHALLENGE": "dns-01", "WARP_ACME_DNS_PROVIDER": "hook", "WARP_ACME_DNS_HOOK": hook,
              "WARP_ACME_DNS_WAIT_SECONDS": "1", "SAYONORA_ENCRYPTION_KEY": key,
              "WARP_CLUSTER_SEED_NODES": "127.0.0.1:47500..47600"}
    envs = []
    for i in (1, 2):
        d = tmp / f"n{i}"
        d.mkdir()
        envs.append(base_env(d, fake, **common))
    n1 = n2 = None
    try:
        n1 = start_acme_warp(pg, envs[0], name="acme-n1")
        n2 = start_acme_warp(pg, envs[1], name="acme-n2")
        j1 = wait_acme_cert(n1, timeout=90)
        j2 = wait_acme_cert(n2, timeout=90)
        sha = j1["acme"]["certificate"]["sha256"]
        assert j2["acme"]["certificate"]["sha256"] == sha
        assert j1["acme"]["shared"] is True and j2["acme"]["shared"] is True
        time.sleep(4)
        assert fake.counters["new_order"] == 1 and fake.counters["new_account"] == 1 and fake.counters["finalize"] == 1
        for w in (n1, n2):
            assert served_sha(w.mcp_https, fake.ca_pem_path) == sha
        issued = [("ACME: issued certificate" in log_text(w)) for w in (n1, n2)]
        adopted = [("adopted the shared certificate" in log_text(w)) for w in (n1, n2)]
        assert sorted(issued) == [False, True] and adopted[issued.index(False)], (issued, adopted)
        # the key material at rest in the control plane is encrypted
        import subprocess
        out = subprocess.run(["psql", "-h", "localhost", "-p", str(pg.port), "-U", "postgres", "-d", "postgres",
                              "-t", "-A", "-F", "\t", "-c", "select name, value from warp_acme_state"],
                             env={**os.environ, "PGPASSWORD": "postgres"}, capture_output=True, text=True, check=True)
        rows = {}
        for line in out.stdout.splitlines():
            if not line.strip():
                continue
            n, v = line.split("\t", 1)
            rows[n] = v
        assert any(k.startswith("certificate:") for k in rows) and any(k.startswith("account:") for k in rows), rows
        assert all(v.startswith("encv1:") and "PRIVATE KEY" not in v for v in rows.values())
    finally:
        for w in (n2, n1):
            if w:
                w.close()
        pg.close()
        fake.close()
