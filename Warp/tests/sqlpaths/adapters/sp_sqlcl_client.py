"""sp_sqlcl_client: drives the host `sqlcl` binary (-S -L, SET SQLFORMAT json) so its output is
machine-parseable. No bind-variable support in this adapter (sqlcl script mode doesn't expose
DB-API style binds) -- scenarios with `binds` set `client_filter` to exclude sqlcl."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys

sys.path.insert(0, __file__.rsplit("/", 2)[0])
from sp_core import CanonicalResult, normalize_rows, normalize_error  # noqa: E402

SQLCL = os.environ.get("SP_SQLCL_BIN", "/opt/homebrew/bin/sqlcl")
_ERR_RE = re.compile(r"ORA-(\d{5})")


def available():
    if not os.path.exists(SQLCL) and shutil.which("sqlcl") is None:
        return False, f"sqlcl not found at {SQLCL}"
    return True, ""


def _conn_str(target):
    if target.extra.get("tls"):
        return (f"{target.user}/{target.password}@tcps://{target.host}:{target.port}/"
                f"{target.extra.get('service','FREEPDB1')}")
    return f"{target.user}/{target.password}@//{target.host}:{target.port}/{target.extra.get('service','FREEPDB1')}"


def _exec(conn_str, script, binary, timeout=60):
    """Real gap found live: PROMPT-based markers don't work in sqlcl's -S -L script mode --
    PROMPT swallows the following line(s) up to the next statement terminator instead of just
    echoing its own line, corrupting whatever SQL followed it. Fixed by never mixing PROMPT with
    the statement under test: setup/main/teardown each run as their own separate `sqlcl`
    invocation (a fresh connection per phase costs a little time, not correctness)."""
    p = subprocess.run([binary, "-S", "-L", conn_str], input=script,
                        capture_output=True, text=True, timeout=timeout)
    return p.stdout


def run(scenario, target) -> CanonicalResult:
    conn_str = _conn_str(target)
    binary = SQLCL if os.path.exists(SQLCL) else "sqlcl"
    try:
        if scenario.setup:
            setup_script = "SET FEEDBACK OFF\n" + "\n".join(s.rstrip(";") + ";" for s in scenario.setup) + "\nexit;\n"
            _exec(conn_str, setup_script, binary)
        body = _exec(conn_str, "SET SQLFORMAT json\nSET FEEDBACK OFF\nWHENEVER SQLERROR CONTINUE\n"
                                + scenario.sql.rstrip(";") + ";\nexit;\n", binary)
    except subprocess.TimeoutExpired:
        return CanonicalResult(ok=False, error_code="TIMEOUT", error_text="sqlcl timed out")
    finally:
        if scenario.teardown:
            try:
                teardown_script = "SET FEEDBACK OFF\nWHENEVER SQLERROR CONTINUE\n" + \
                                   "\n".join(s.rstrip(";") + ";" for s in scenario.teardown) + "\nexit;\n"
                _exec(conn_str, teardown_script, binary)
            except subprocess.TimeoutExpired:
                pass
    if "ORA-" in body:
        err = _ERR_RE.search(body)
        code, text = normalize_error(f"ORA-{err.group(1)}" if err else None, body.strip())
        return CanonicalResult(ok=False, error_code=code, error_text=text, raw=body)
    if scenario.expect != "rows":
        return CanonicalResult(ok=True, raw=body)
    j = body.strip()
    start = j.find("{")
    if start == -1:
        return CanonicalResult(ok=True, columns=[], rows=[], row_count=0, raw=body)
    try:
        data = json.loads(j[start:])
    except Exception as e:  # noqa: BLE001
        return CanonicalResult(ok=False, error_code="ADAPTER_ERROR", error_text=f"bad sqlcl json: {e}", raw=body)
    results = data.get("results", [{}])
    r0 = results[0] if results else {}
    cols_meta = r0.get("columns", [])
    cols = [c["name"] for c in cols_meta]
    items = r0.get("items", [])
    rows = []
    for it in items:
        rows.append([it.get(c["name"].lower()) for c in cols_meta])
    ncols, nrows = normalize_rows(cols, rows)
    return CanonicalResult(ok=True, columns=ncols, rows=nrows, row_count=len(nrows))
