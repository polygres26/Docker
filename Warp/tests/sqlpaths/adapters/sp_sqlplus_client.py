"""sp_sqlplus_client: drives `sqlplus` inside the same gvenzl/oracle-free container (SQL*Plus is
not installed on the host) via `docker exec`, with `SET MARKUP CSV ON` so output is parseable.
No bind-variable support (matches sqlcl's adapter) -- scenarios with `binds` set `client_filter`.

Setup/main/teardown run as three separate `sqlplus` invocations rather than one script with
PROMPT-based markers -- PROMPT turned out to swallow the following line(s) in sqlcl's script mode
(see sp_sqlcl_client.py's own note), and keeping both adapters' scripting the same, simpler shape
avoids relying on a marker mechanism in either.
"""
from __future__ import annotations

import csv
import io
import os
import re
import subprocess
import sys

sys.path.insert(0, __file__.rsplit("/", 2)[0])
from sp_core import CanonicalResult, normalize_rows, normalize_error  # noqa: E402

CONTAINER = os.environ.get("SP_SQLPLUS_CONTAINER", "sp-oracle")
_ERR_RE = re.compile(r"ORA-(\d{5})")


def available():
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
                        capture_output=True, text=True)
    if r.returncode != 0 or r.stdout.strip() != "true":
        return False, f"container {CONTAINER} (holds the sqlplus binary) is not running"
    return True, ""


def _connect_string(target):
    # docker exec's sqlplus runs INSIDE the same container the real Oracle listener is in, so a
    # "native" Target (host/port aimed at the *host-mapped* port for other clients) must be
    # translated to the container-internal address here: 1521 is gvenzl/oracle-free's own
    # internal listener port, reachable as localhost from inside that same container.
    if target.extra.get("via_docker_host"):
        host, port = "host.docker.internal", target.port   # Warp RELAY/ADAPT frontend, reached from the container
    elif target.extra.get("is_native"):
        host, port = "localhost", target.extra.get("native_internal_port", 1521)  # real Oracle, container-internal port
    else:
        host, port = target.host, target.port
    service = target.extra.get("service", "FREEPDB1")
    if target.extra.get("tls"):
        return f"{target.user}/{target.password}@tcps://{host}:{port}/{service}"
    return f"{target.user}/{target.password}@//{host}:{port}/{service}"


def _exec(conn_str, script, timeout=60):
    p = subprocess.run(["docker", "exec", "-i", CONTAINER, "sqlplus", "-S", conn_str],
                        input=script, capture_output=True, text=True, timeout=timeout)
    return p.stdout


def run(scenario, target) -> CanonicalResult:
    conn_str = _connect_string(target)
    try:
        if scenario.setup:
            _exec(conn_str, "SET FEEDBACK OFF\nWHENEVER SQLERROR CONTINUE\n" +
                  "\n".join(s.rstrip(";") + ";" for s in scenario.setup) + "\nexit;\n")
        body = _exec(conn_str, "SET MARKUP CSV ON\nSET FEEDBACK OFF\nWHENEVER SQLERROR CONTINUE\n"
                     + scenario.sql.rstrip(";") + ";\nexit;\n")
    except subprocess.TimeoutExpired:
        return CanonicalResult(ok=False, error_code="TIMEOUT", error_text="sqlplus timed out")
    finally:
        if scenario.teardown:
            try:
                _exec(conn_str, "SET FEEDBACK OFF\nWHENEVER SQLERROR CONTINUE\n" +
                      "\n".join(s.rstrip(";") + ";" for s in scenario.teardown) + "\nexit;\n")
            except subprocess.TimeoutExpired:
                pass
    if "ORA-" in body:
        err = _ERR_RE.search(body)
        code, text = normalize_error(f"ORA-{err.group(1)}" if err else None, body.strip())
        return CanonicalResult(ok=False, error_code=code, error_text=text, raw=body)
    if scenario.expect != "rows":
        return CanonicalResult(ok=True, raw=body)
    text = body.strip()
    if not text:
        return CanonicalResult(ok=True, columns=[], rows=[], row_count=0, raw=body)
    reader = csv.reader(io.StringIO(text))
    all_rows = [r for r in reader if r]
    if not all_rows:
        return CanonicalResult(ok=True, columns=[], rows=[], row_count=0, raw=body)
    cols, data_rows = all_rows[0], all_rows[1:]
    ncols, nrows = normalize_rows(cols, data_rows)
    return CanonicalResult(ok=True, columns=ncols, rows=nrows, row_count=len(nrows))
