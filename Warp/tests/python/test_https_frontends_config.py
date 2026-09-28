"""Failure and opt-out behaviour of the HTTPS listeners of the API frontends: a bad certificate configuration leaves every frontend
on plaintext and /api/interfaces says so, and WARP_<NAME>_HTTP_DISABLED closes the plaintext port. Separate module so each test
runs its own Warp after test_https_frontends' shared Warp is gone (a Developer license allows three live instances)."""
import socket

import pytest

from test_https_frontends import HttpsWarp, boto, pg, pki  # noqa: F401 -- fixtures re-used

def test_bad_tls_config_keeps_plaintext_and_reports_not_enabled(pg, pki, tmp_path):
    bad = tmp_path / "missing.pem"
    w = HttpsWarp(pg, pki, extra={"WARP_TLS_CERT": str(bad), "WARP_TLS_KEY": str(bad)})
    try:
        log = w.log()
        assert "HTTPS is NOT enabled" in log and "Plaintext" in log
        # plaintext still serves
        assert "TableNames" in boto("dynamodb", w, "dynamowire", tls=False).list_tables()
        with pytest.raises(OSError):
            socket.create_connection(("localhost", w.https["dynamowire"]), 2).close()
        ifs = {i["id"]: i for i in w.api("GET", "/api/interfaces")["interfaces"]}
        assert ifs["dynamowire"]["tlsEnabled"] is False and ifs["dynamowire"].get("tlsError")
    finally:
        w.close()


def test_http_disabled_closes_plaintext_port(pg, pki):
    w = HttpsWarp(pg, pki, extra={"WARP_SQSWIRE_HTTP_DISABLED": "true", "WARP_GREMLINWIRE_HTTP_DISABLED": "true"})
    try:
        for n in ("sqswire", "gremlinwire"):
            with pytest.raises(OSError):
                socket.create_connection(("localhost", w.http[n]), 2).close()
        q = boto("sqs", w, "sqswire")
        assert q.create_queue(QueueName="only-https")["QueueUrl"].startswith("https://")
        # everything else keeps its plaintext port
        boto("dynamodb", w, "dynamowire", tls=False).list_tables()
    finally:
        w.close()
