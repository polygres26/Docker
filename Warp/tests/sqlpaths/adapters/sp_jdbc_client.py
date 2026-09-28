"""sp_jdbc_client: JDBC (ojdbc11 thin) adapter -- shells out to the compiled OracleJdbcRunner.java
single-file program (javac'd once by the engine plug-in at startup)."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

sys.path.insert(0, __file__.rsplit("/", 2)[0])
from sp_core import CanonicalResult, normalize_rows, normalize_error  # noqa: E402

ADAPTERS_DIR = os.path.dirname(os.path.abspath(__file__))
OJDBC_JAR = os.environ.get(
    "SP_OJDBC_JAR",
    os.path.expanduser("~/.m2/repository/com/oracle/database/jdbc/ojdbc11/23.8.0.25.04/"
                        "ojdbc11-23.8.0.25.04.jar"))


def available():
    if not os.path.exists(OJDBC_JAR):
        return False, f"ojdbc11 jar not found at {OJDBC_JAR}"
    if shutil.which("java") is None and not os.path.exists("/usr/bin/java"):
        return False, "java not found"
    if not os.path.exists(os.path.join(ADAPTERS_DIR, "OracleJdbcRunner.class")):
        r = subprocess.run(["javac", "-cp", OJDBC_JAR, "OracleJdbcRunner.java"],
                            cwd=ADAPTERS_DIR, capture_output=True, text=True)
        if r.returncode != 0:
            return False, f"javac failed: {r.stderr[-500:]}"
    return True, ""


def run(scenario, target) -> CanonicalResult:
    proto = "tcps" if target.extra.get("tls") else "thin"
    if target.extra.get("tls"):
        url = f"jdbc:oracle:thin:@tcps://{target.host}:{target.port}/{target.extra.get('service','FREEPDB1')}?oracle.net.ssl_server_dn_match=false"
    else:
        url = f"jdbc:oracle:thin:@//{target.host}:{target.port}/{target.extra.get('service','FREEPDB1')}"
    req = {
        "url": url, "user": target.user, "password": target.password,
        "sql": scenario.sql, "setup": scenario.setup or [], "teardown": scenario.teardown or [],
        "expectRows": scenario.expect == "rows",
    }
    if scenario.binds:
        req["binds"] = [str(b) for b in scenario.binds]
    java_bin = "/usr/bin/java" if os.path.exists("/usr/bin/java") else "java"
    try:
        p = subprocess.run(
            [java_bin, "-cp", f"{ADAPTERS_DIR}:{OJDBC_JAR}", "OracleJdbcRunner"],
            input=json.dumps(req), capture_output=True, text=True, timeout=60, cwd=ADAPTERS_DIR)
    except subprocess.TimeoutExpired:
        return CanonicalResult(ok=False, error_code="TIMEOUT", error_text="jdbc runner timed out")
    if p.returncode != 0 or not p.stdout.strip():
        return CanonicalResult(ok=False, error_code="ADAPTER_ERROR",
                                error_text=(p.stderr or p.stdout)[-1000:], raw=p.stderr)
    try:
        out = json.loads(p.stdout.strip().splitlines()[-1])
    except Exception as e:  # noqa: BLE001
        return CanonicalResult(ok=False, error_code="ADAPTER_ERROR", error_text=f"bad json: {e}: {p.stdout[:500]}")
    if out.get("ok"):
        cols, rows = normalize_rows(out.get("columns") or [], out.get("rows") or [])
        return CanonicalResult(ok=True, columns=cols, rows=rows, row_count=len(rows) if out.get("rows") is not None else None)
    code, text = normalize_error(out.get("errorCode"), out.get("errorText"))
    return CanonicalResult(ok=False, error_code=code, error_text=text, raw=out.get("errorText"))
