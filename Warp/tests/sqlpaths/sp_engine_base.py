"""sp_engine_base: the plug-in contract every engine (oracle, mysql, sqlserver, ...) implements.

An "engine plug-in" is a Python package under sqlpaths/engines/<name>/ exposing a module-level
ENGINE object (see below) that the CLI driver (run_matrix.py) uses generically. Nothing in
run_matrix.py or sp_core.py mentions Oracle/MySQL/SQL Server by name -- all of that lives in the
engine plug-in and its client adapters.
"""
from __future__ import annotations

import dataclasses
from typing import Callable, Optional


@dataclasses.dataclass
class Target:
    """Where a client should connect for one (client, path) cell."""
    host: str
    port: int
    # Free-form engine-specific extras a client adapter needs (service name, database, TLS ca...).
    extra: dict = dataclasses.field(default_factory=dict)
    user: str = ""
    password: str = ""


@dataclasses.dataclass
class ClientAdapter:
    """One client (sqlplus / sqlcl / jdbc / python / ...). `run` executes a single scenario
    against a Target and returns a sp_core.CanonicalResult. `available()` lets the driver skip a
    client cleanly (with a stated reason) when its binary/module isn't present on this machine.
    """
    name: str
    run: Callable  # (scenario, target) -> CanonicalResult
    available: Callable[[], "tuple"]  # () -> (bool, str reason-if-not)
    supports_tls: bool = False


@dataclasses.dataclass
class EnginePlugin:
    name: str
    scenarios_path: str
    # Lifecycle: engine owns starting/stopping its own real database container(s).
    start_native_db: Callable          # () -> object (a handle with .host/.port/.stop())
    stop_native_db: Callable           # (handle) -> None
    # Warp instance builders: given the native db handle (+ a RealPostgres for ADAPT), return the
    # env vars run_matrix should launch a Warp process with, per path.
    relay_env: Callable                # (native_handle) -> dict[str,str]
    adapt_env: Callable                # (postgres_handle) -> dict[str,str]
    bridge_env: Optional[Callable] = None   # (native_handle) -> dict[str,str]; same shape as
                                             # relay_env, structurally, just a different backend
                                             # mode. Optional: an engine that hasn't wired a Bridge
                                             # mode yet (or whose Bridge mode isn't implemented in
                                             # the product code yet) may omit it -- run_matrix.py
                                             # then reports "bridge" as unavailable per-scenario
                                             # rather than crashing.
    adapt_extension_sql: Optional[str] = None   # path to a SQL file to load into ADAPT's Postgres
    clients: dict = dataclasses.field(default_factory=dict)   # name -> ClientAdapter
    frontend_env_var: str = ""          # e.g. WARP_ORAWIRE_PORT
    known_diffs_path: str = ""          # path to known/<engine>_adapt.yaml
    golden_native_path: str = ""        # path to golden/golden_native.json.gz -- see sp_golden.py
