"""sp_oracle_engine: the Oracle plug-in for the sqlpaths framework.

Owns: starting/stopping a real Oracle container (gvenzl/oracle-free:23-slim, already pulled --
no new docker pulls), the scenario corpus, and env-var wiring for RELAY (native-backend proxy)
and ADAPT (dialect-translation to Postgres) Warp instances. See docs/WARP_GUIDE.md §8.1.1.
"""
from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # tests/python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # sqlpaths/

from sp_engine_base import ClientAdapter, EnginePlugin, Target  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
SQLPATHS_DIR = os.path.dirname(os.path.dirname(HERE))
ADAPTERS_DIR = os.path.join(SQLPATHS_DIR, "adapters")
sys.path.insert(0, ADAPTERS_DIR)

ORACLE_IMAGE = "gvenzl/oracle-free:23-slim"
ORACLE_PASSWORD = "OraPass1"
ORACLE_SERVICE = "FREEPDB1"
ORACLE_USER = "system"


@dataclasses.dataclass
class OracleHandle:
    name: str
    host: str
    port: int


def _free_port():
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def start_native_db() -> OracleHandle:
    name = f"sp-oracle-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    subprocess.run(["docker", "run", "-d", "--name", name, "--memory", "2500m",
                     "-e", f"ORACLE_PASSWORD={ORACLE_PASSWORD}",
                     "-p", f"{port}:1521", ORACLE_IMAGE], check=True, capture_output=True)
    _wait_ready(name, port)
    return OracleHandle(name=name, host="localhost", port=port)


def use_existing(name: str, port: int) -> OracleHandle:
    """Attach to an already-running, already-healthy Oracle container instead of starting a new
    one -- saves the 1-3 minute cold-start when iterating on the matrix during development."""
    return OracleHandle(name=name, host="localhost", port=port)


def _wait_ready(name, port, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "exec", name, "bash", "-lc",
             f"echo 'select 1 from dual;' | sqlplus -S system/{ORACLE_PASSWORD}@//localhost:1521/{ORACLE_SERVICE}"],
            capture_output=True, text=True)
        if r.returncode == 0 and "1" in r.stdout:
            return
        time.sleep(5)
    raise TimeoutError(f"Oracle container {name} did not become ready in {timeout}s")


def stop_native_db(handle: OracleHandle):
    subprocess.run(["docker", "rm", "-f", "-v", handle.name], capture_output=True)


def relay_env(handle: OracleHandle) -> dict:
    return {
        "WARP_ORACLE_BACKEND_MODE": "native",
        "WARP_ORACLE_HOST": handle.host,
        "WARP_ORACLE_PORT": str(handle.port),
        "WARP_ORACLE_SERVICE": ORACLE_SERVICE,
    }


def bridge_env(handle: OracleHandle) -> dict:
    """Same env-var shape as relay_env -- Warp's orawire "Bridge" backend mode is (by design of
    this slice) wired generically alongside "native": same startup, same real-Oracle-backend
    pointer, only WARP_ORACLE_BACKEND_MODE differs. Bridge mode's Java implementation does not
    exist yet at the time this was written (a separate, parallel effort is building it) -- this
    just lets `--paths bridge` start a Warp instance and get graceful per-scenario CLIENT/WARP_BUG
    results (most likely connection/login failures) rather than the CLI crashing outright. Once
    Bridge mode lands in orawire, this function needs no changes to become meaningful."""
    return {
        "WARP_ORACLE_BACKEND_MODE": "bridge",
        "WARP_ORACLE_HOST": handle.host,
        "WARP_ORACLE_PORT": str(handle.port),
        "WARP_ORACLE_SERVICE": ORACLE_SERVICE,
    }


def adapt_env(postgres_handle) -> dict:
    # Default mode ("jdbc") -- no WARP_ORACLE_BACKEND_MODE needed. PgOracleSupport (see
    # Warp/src/main/java/com/sayonora/warp/core/PgOracleSupport.java) probes for the pg_oracle
    # extension and degrades gracefully when it's absent, so ADAPT still runs standard dialect
    # translation without it; see NOTES.md for what that means for this run's ADAPT coverage.
    return {}


# ----------------------------------------------------------------------------------------------
# Client adapters
# ----------------------------------------------------------------------------------------------

import sp_python_client  # noqa: E402
import sp_jdbc_client  # noqa: E402
import sp_sqlcl_client  # noqa: E402
import sp_sqlplus_client  # noqa: E402

CLIENTS = {
    "python": ClientAdapter(name="python", run=sp_python_client.run, available=sp_python_client.available),
    "jdbc": ClientAdapter(name="jdbc", run=sp_jdbc_client.run, available=sp_jdbc_client.available,
                           supports_tls=True),
    "sqlcl": ClientAdapter(name="sqlcl", run=sp_sqlcl_client.run, available=sp_sqlcl_client.available),
    "sqlplus": ClientAdapter(name="sqlplus", run=sp_sqlplus_client.run, available=sp_sqlplus_client.available),
}
CLIENTS["python"].supports_tls = True

ENGINE = EnginePlugin(
    name="oracle",
    scenarios_path=os.path.join(HERE, "scenarios.json"),
    start_native_db=start_native_db,
    stop_native_db=stop_native_db,
    relay_env=relay_env,
    adapt_env=adapt_env,
    bridge_env=bridge_env,
    clients=CLIENTS,
    frontend_env_var="WARP_ORAWIRE_PORT",
    known_diffs_path=os.path.join(SQLPATHS_DIR, "known", "oracle_adapt.yaml"),
    golden_native_path=os.path.join(HERE, "golden", "golden_native.json.gz"),
)


def make_target(native_handle, warp_frontend_port=None, path="native", tls=False,
                 via_docker_host=False) -> Target:
    """Build the Target a client adapter connects to for one (path) cell.
    path="native" -> straight to the real Oracle container, real Oracle creds.
    path="relay"  -> Warp's native-backend proxy; O5LOGON login is Warp's own login step, but it
                     just forwards to the real Oracle, so the same real Oracle creds work.
    path="adapt"  -> Warp's default dialect-translation mode; the backend is Postgres, so the
                     login is the Postgres backend's own user/password (see test_orawire.py's
                     `connect()` helper -- postgres/postgres -- this is Warp's documented ADAPT
                     login convention, not a real Oracle account)."""
    if path == "native":
        return Target(host=native_handle.host, port=native_handle.port, user=ORACLE_USER,
                       password=ORACLE_PASSWORD,
                       extra={"service": ORACLE_SERVICE, "tls": tls, "via_docker_host": False,
                              "is_native": True, "native_internal_port": 1521})
    if path in ("relay", "bridge"):
        # Both RELAY (native-backend proxy) and BRIDGE point at the same real Oracle backend with
        # the same real Oracle creds -- structurally identical Target shape, just a different
        # WARP_ORACLE_BACKEND_MODE at Warp startup (see relay_env/bridge_env above).
        return Target(host="localhost", port=warp_frontend_port, user=ORACLE_USER,
                       password=ORACLE_PASSWORD,
                       extra={"service": ORACLE_SERVICE, "tls": tls, "via_docker_host": via_docker_host,
                              "is_native": False})
    # adapt
    return Target(host="localhost", port=warp_frontend_port, user="postgres", password="postgres",
                  extra={"service": "anything", "tls": tls, "via_docker_host": via_docker_host,
                         "is_native": False})
