#!/usr/bin/env python3
"""run_matrix.py -- CLI driver for the engine-agnostic SQL-path test matrix.

    python3 Warp/tests/sqlpaths/run_matrix.py --engine oracle \\
        --clients sqlplus,sqlcl,jdbc,python --paths native,relay,adapt

Engine-specific behavior (container start/stop, scenario corpus, client adapters) lives entirely
in Warp/tests/sqlpaths/engines/<engine>/sp_<engine>_engine.py -- this file only knows the generic
Scenario/CanonicalResult/classify vocabulary from sp_core.py and the EnginePlugin contract from
sp_engine_base.py, so the same driver runs MySQL/SQL Server plug-ins unmodified.
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "python"))  # tests/python for warp_test_support

import sp_core  # noqa: E402


def load_engine(name):
    mod = importlib.import_module(f"engines.{name}.sp_{name}_engine")
    return mod


def load_known_diffs(path):
    """Reads known-diffs entries without a YAML dependency (pyyaml isn't installed and no network
    installs are permitted): the file uses a restricted, line-based subset (`- id: ...` blocks
    with `reason:`/`path:` fields) that this tiny parser understands directly."""
    diffs = {}
    if not os.path.exists(path):
        return diffs
    cur = None
    with open(path) as f:
        for raw in f:
            line = raw.rstrip("\n")
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s.startswith("- id:"):
                if cur:
                    diffs[cur["id"]] = cur
                cur = {"id": s.split(":", 1)[1].strip().strip('"\'')}
            elif cur is not None and ":" in s:
                k, v = s.split(":", 1)
                cur[k.strip()] = v.strip().strip('"\'')
    if cur:
        diffs[cur["id"]] = cur
    return diffs


def run_one(client_adapter, scenario, target, retries=1):
    last = None
    for _ in range(retries):
        try:
            last = client_adapter.run(scenario, target)
            return last
        except Exception as e:  # noqa: BLE001 -- adapter crash becomes an ERROR result, not a hard stop
            last = sp_core.CanonicalResult(ok=False, error_code="ADAPTER_EXCEPTION",
                                            error_text=f"{e}\n{traceback.format_exc()[-800:]}")
    return last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True)
    ap.add_argument("--clients", default="python,jdbc,sqlcl,sqlplus")
    ap.add_argument("--paths", default="native,relay,adapt")
    ap.add_argument("--tls-clients", default="jdbc,python",
                     help="clients to additionally run over TLS for relay/adapt")
    ap.add_argument("--with-tls", action="store_true", help="also run the TLS sub-matrix")
    ap.add_argument("--out-dir", default=os.path.join(HERE, "reports"))
    ap.add_argument("--reuse-oracle", default=None,
                     help="name:port of an already-running, healthy Oracle container to reuse")
    ap.add_argument("--limit", type=int, default=None, help="cap number of scenarios (smoke runs)")
    ap.add_argument("--tags", default=None, help="comma list: only run scenarios with any of these tags/categories")
    args = ap.parse_args()

    engine = load_engine(args.engine)
    clients = [c.strip() for c in args.clients.split(",") if c.strip()]
    paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    known_diffs = load_known_diffs(engine.ENGINE.known_diffs_path)
    scenarios = sp_core.load_scenarios(engine.ENGINE.scenarios_path)
    if args.tags:
        wanted = set(args.tags.split(","))
        scenarios = [s for s in scenarios if s.category in wanted or wanted & set(s.tags)]
    if args.limit:
        scenarios = scenarios[: args.limit]

    os.makedirs(args.out_dir, exist_ok=True)

    print(f"[run_matrix] engine={args.engine} clients={clients} paths={paths} "
          f"scenarios={len(scenarios)}")

    # ---- start real native DB ----
    if args.reuse_oracle:
        name, port = args.reuse_oracle.split(":")
        native_handle = engine.use_existing(name, int(port))
        owns_native = False
    else:
        print("[run_matrix] starting native Oracle container (this can take 1-3 minutes)...")
        native_handle = engine.start_native_db()
        owns_native = True
    print(f"[run_matrix] native db ready: {native_handle}")

    warp_procs = {}
    postgres = None
    results = []

    try:
        from warp_test_support import WarpProcess, RealPostgres, isolated_ports  # type: ignore

        if "relay" in paths:
            print("[run_matrix] starting Warp (RELAY: native-backend proxy mode)...")
            pg_for_relay = RealPostgres()  # relay still needs *a* Postgres primary for Warp itself
            env = {**isolated_ports(engine.ENGINE.frontend_env_var), **engine.ENGINE.relay_env(native_handle)}
            warp_procs["relay"] = (WarpProcess(pg_for_relay, engine.ENGINE.frontend_env_var,
                                                frontend_name=f"{args.engine}-relay", extra_env=env),
                                    pg_for_relay)
        if "adapt" in paths:
            print("[run_matrix] starting Warp (ADAPT: dialect translation to Postgres)...")
            postgres = RealPostgres()
            env = {**isolated_ports(engine.ENGINE.frontend_env_var), **engine.ENGINE.adapt_env(postgres)}
            warp_procs["adapt"] = (WarpProcess(postgres, engine.ENGINE.frontend_env_var,
                                                frontend_name=f"{args.engine}-adapt", extra_env=env),
                                    postgres)

        # ---- resolve available clients up front ----
        client_status = {}
        for cname in clients:
            adapter = engine.ENGINE.clients.get(cname)
            if adapter is None:
                client_status[cname] = (False, "no such client adapter registered")
                continue
            client_status[cname] = adapter.available()
        for cname, (ok, reason) in client_status.items():
            print(f"[run_matrix] client={cname} available={ok} {reason}")

        # ---- run the matrix ----
        for scenario in scenarios:
            per_client_native = {}
            for cname in clients:
                ok, reason = client_status[cname]
                if not ok:
                    results.append({"scenario": scenario.id, "client": cname, "path": "native",
                                     "verdict": "SKIP", "reason": reason})
                    continue
                if scenario.client_filter and cname not in scenario.client_filter:
                    continue
                adapter = engine.ENGINE.clients[cname]
                for path in paths:
                    if path == "native":
                        target = engine.make_target(native_handle, path="native")
                    else:
                        wp, _ = warp_procs.get(path, (None, None))
                        if wp is None:
                            continue
                        via_docker = cname == "sqlplus"
                        target = engine.make_target(native_handle, warp_frontend_port=wp.frontend_port,
                                                     path=path, via_docker_host=via_docker)
                    res = run_one(adapter, scenario, target)
                    if path == "native":
                        per_client_native[cname] = res
                        results.append({"scenario": scenario.id, "client": cname, "path": path,
                                         "verdict": "PASS" if res.ok or scenario.expect == "error" else "ERROR",
                                         "result": res.to_dict()})
                        continue
                    native_res = per_client_native.get(cname)
                    if native_res is None:
                        verdict, reason = "SKIP", "no native baseline for this client"
                    else:
                        verdict, reason = sp_core.classify(path, native_res, res, known_diffs, scenario)
                    entry = {"scenario": scenario.id, "client": cname, "path": path,
                              "verdict": verdict, "reason": reason, "result": res.to_dict()}
                    if verdict == "WARP_BUG":
                        entry["diff"] = sp_core.result_diff_text(native_res, res) if native_res else None
                        entry["repro"] = {"sql": scenario.sql, "client": cname, "path": path}
                    results.append(entry)
    finally:
        for name, (proc, pg) in warp_procs.items():
            try:
                proc.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                pg.close()
            except Exception:  # noqa: BLE001
                pass
        if owns_native:
            engine.stop_native_db(native_handle)

    out_json = os.path.join(args.out_dir, f"{args.engine}_matrix.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"[run_matrix] wrote {out_json} ({len(results)} rows)")

    from sp_report import write_reports  # local import: keeps report-writing swappable
    write_reports(args.engine, results, args.out_dir)


if __name__ == "__main__":
    main()
