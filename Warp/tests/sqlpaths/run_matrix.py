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
import concurrent.futures
import importlib
import json
import os
import sys
import time
import traceback

# Per-scenario hard timeout (seconds). Protects the whole run from a single hung ADAPT scenario
# (LLM-fallback-translator calls with no network access can otherwise hang indefinitely). Generous
# enough for legitimate slow scenarios (100k-row fetch, 10MB LOB) but well under a minute.
SCENARIO_TIMEOUT_SECS = float(os.environ.get("SP_SCENARIO_TIMEOUT", "45"))

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "python"))  # tests/python for warp_test_support

import sp_core  # noqa: E402
import sp_golden  # noqa: E402

# Paths that are actually live-executed by an ordinary run. "native" is NOT one of them any more:
# it is recorded once, offline, into a golden file (see sp_golden.py / --record-native below) and
# every ordinary run diffs RELAY/ADAPT/BRIDGE against that frozen file instead of live-querying a
# real, Warp-bypassing Oracle connection. This matches every other emulated-protocol conformance
# harness in this repo (Warp/tests/python/*_conformance/, golden.json.gz).
LIVE_PATHS = {"relay", "adapt", "bridge"}


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


TIMEOUT = "TIMEOUT"  # distinct verdict from WARP_BUG -- see SCENARIO_TIMEOUT_SECS above


class ScenarioTimeout(Exception):
    pass


def run_one(client_adapter, scenario, target, retries=1):
    last = None
    for _ in range(retries):
        try:
            # Run in a fresh single-use worker thread with a hard wall-clock timeout so one
            # scenario that hangs (e.g. ADAPT's LLM-fallback translator with no network access)
            # cannot stall the whole 4-client x 3-path run. The worker thread is a daemon-ish
            # throwaway: if it times out we abandon it (it may finish later in the background;
            # its result is simply discarded) rather than blocking on it.
            ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            fut = ex.submit(client_adapter.run, scenario, target)
            try:
                last = fut.result(timeout=SCENARIO_TIMEOUT_SECS)
                ex.shutdown(wait=False)
                return last
            except concurrent.futures.TimeoutError:
                ex.shutdown(wait=False)
                last = sp_core.CanonicalResult(
                    ok=False, error_code="SCENARIO_TIMEOUT",
                    error_text=f"scenario exceeded {SCENARIO_TIMEOUT_SECS}s hard timeout "
                               f"(possible ADAPT LLM-fallback hang or genuinely slow client)")
                last._sp_timeout = True  # marker consumed by main() to force TIMEOUT verdict
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
    ap.add_argument("--relay-oracle", default=None,
                     help="name:port of a SEPARATE already-running Oracle container for RELAY's "
                          "backend, distinct from the NATIVE-path/baseline container given via "
                          "--reuse-oracle. Keeps native and relay from competing for Oracle "
                          "Free's small session/process limit when both are exercised in the "
                          "same run. If omitted, relay reuses the native container (old behavior).")
    ap.add_argument("--limit", type=int, default=None, help="cap number of scenarios (smoke runs)")
    ap.add_argument("--tags", default=None, help="comma list: only run scenarios with any of these tags/categories")
    ap.add_argument("--record-native", action="store_true",
                     help="ONE-TIME/rare recording mode: live-executes the full corpus directly "
                          "against a real, Warp-bypassing Oracle connection (today's old "
                          "--paths native behavior) and writes the results into the frozen golden "
                          "native-baseline file (see sp_golden.py). Never used in ordinary CI/dev "
                          "runs -- run this deliberately (e.g. after an Oracle version upgrade) "
                          "when you believe Oracle's true behavior needs re-capturing. Requires "
                          "--reuse-oracle (or starts its own container if omitted).")
    ap.add_argument("--golden-out", default=None,
                     help="with --record-native: where to write the golden file (default: the "
                          "engine's own golden/golden_native.json.gz)")
    ap.add_argument("--record-times", type=int, default=2,
                     help="with --record-native: re-runs each (scenario, client) this many times "
                          "and drops unstable (non-reproducing) results, same convention as the "
                          "other *_conformance golden recorders")
    args = ap.parse_args()

    engine = load_engine(args.engine)
    clients = [c.strip() for c in args.clients.split(",") if c.strip()]
    requested_paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    known_diffs = load_known_diffs(engine.ENGINE.known_diffs_path)
    scenarios = sp_core.load_scenarios(engine.ENGINE.scenarios_path)
    if args.tags:
        wanted = set(args.tags.split(","))
        scenarios = [s for s in scenarios if s.category in wanted or wanted & set(s.tags)]
    if args.limit:
        scenarios = scenarios[: args.limit]

    os.makedirs(args.out_dir, exist_ok=True)

    # ---- start/attach the real native DB (still needed: as RELAY/BRIDGE's real backend, and as
    # ADAPT's translation target where applicable, and -- only in --record-native mode -- as the
    # direct, Warp-bypassing recording target. What's eliminated below is a SEPARATE, ordinary-run,
    # Warp-bypassing client connection used purely for the native/baseline diff.) ----
    if args.reuse_oracle:
        name, port = args.reuse_oracle.split(":")
        native_handle = engine.use_existing(name, int(port))
        owns_native = False
    else:
        print("[run_matrix] starting native Oracle container (this can take 1-3 minutes)...")
        native_handle = engine.start_native_db()
        owns_native = True
    print(f"[run_matrix] native db ready: {native_handle}")

    try:
        if args.record_native:
            client_status = {}
            for cname in clients:
                adapter = engine.ENGINE.clients.get(cname)
                if adapter is None:
                    print(f"[run_matrix] client={cname} available=False no such client adapter registered")
                    continue
                ok, reason = adapter.available()
                print(f"[run_matrix] client={cname} available={ok} {reason}")
                if ok:
                    client_status[cname] = adapter
            golden = sp_golden.record_native(
                engine, client_status,
                lambda: engine.make_target(native_handle, path="native"),
                scenarios, times=args.record_times)
            out_path = args.golden_out or engine.ENGINE.golden_native_path
            sp_golden.write_golden(golden, out_path)
            print(f"[run_matrix] wrote golden native baseline: {out_path} "
                  f"({len(golden['cases'])} scenarios x {len(golden['clients'])} clients)")
            return
    finally:
        if args.record_native and owns_native:
            engine.stop_native_db(native_handle)

    # ---- ordinary run: load the frozen golden baseline, never live-query NATIVE again ----
    golden = sp_golden.load_golden(engine.ENGINE.golden_native_path)
    paths = []
    for p in requested_paths:
        if p == "native":
            print("[run_matrix] NOTE: \"native\" is no longer a live-executed path in ordinary "
                  "runs -- it is loaded from the frozen golden file and used only as the diff "
                  "baseline for relay/adapt/bridge. Dropping it from --paths; use "
                  "--record-native to (re)record the golden file.")
            continue
        if p not in LIVE_PATHS:
            print(f"[run_matrix] NOTE: unknown path {p!r}, dropping")
            continue
        if p == "bridge" and engine.ENGINE.bridge_env is None:
            print(f"[run_matrix] NOTE: engine {args.engine!r} has no bridge_env wired yet -- "
                  "dropping \"bridge\" from --paths for this run")
            continue
        paths.append(p)

    print(f"[run_matrix] engine={args.engine} clients={clients} paths={paths} "
          f"scenarios={len(scenarios)} (golden baseline: {engine.ENGINE.golden_native_path})")

    warp_procs = {}
    postgres = None
    results = []

    try:
        from warp_test_support import WarpProcess, RealPostgres, isolated_ports  # type: ignore

        if "relay" in paths:
            print("[run_matrix] starting Warp (RELAY: native-backend proxy mode)...")
            pg_for_relay = RealPostgres()  # relay still needs *a* Postgres primary for Warp itself
            if args.relay_oracle:
                rname, rport = args.relay_oracle.split(":")
                relay_backend_handle = engine.use_existing(rname, int(rport))
                print(f"[run_matrix] RELAY backend uses SEPARATE Oracle container: {relay_backend_handle}")
            else:
                relay_backend_handle = native_handle
            env = {**isolated_ports(engine.ENGINE.frontend_env_var), **engine.ENGINE.relay_env(relay_backend_handle)}
            warp_procs["relay"] = (WarpProcess(pg_for_relay, engine.ENGINE.frontend_env_var,
                                                frontend_name=f"{args.engine}-relay", extra_env=env),
                                    pg_for_relay)
        if "bridge" in paths:
            print("[run_matrix] starting Warp (BRIDGE: real-Oracle-wire bridge mode)...")
            pg_for_bridge = RealPostgres()
            if args.relay_oracle:  # same "keep off native's Oracle" knob relay uses
                rname, rport = args.relay_oracle.split(":")
                bridge_backend_handle = engine.use_existing(rname, int(rport))
            else:
                bridge_backend_handle = native_handle
            env = {**isolated_ports(engine.ENGINE.frontend_env_var), **engine.ENGINE.bridge_env(bridge_backend_handle)}
            warp_procs["bridge"] = (WarpProcess(pg_for_bridge, engine.ENGINE.frontend_env_var,
                                                 frontend_name=f"{args.engine}-bridge", extra_env=env),
                                     pg_for_bridge)
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

        # ---- run the matrix: every cell is diffed against the golden native baseline, never a
        # live Warp-bypassing Oracle connection ----
        for scenario in scenarios:
            for cname in clients:
                ok, reason = client_status[cname]
                if not ok:
                    results.append({"scenario": scenario.id, "client": cname, "path": "golden",
                                     "verdict": "SKIP", "reason": reason})
                    continue
                if scenario.client_filter and cname not in scenario.client_filter:
                    continue
                adapter = engine.ENGINE.clients[cname]
                gold = sp_golden.golden_result(golden, scenario.id, cname)
                for path in paths:
                    wp, _ = warp_procs.get(path, (None, None))
                    if wp is None:
                        continue
                    via_docker = cname == "sqlplus"
                    target = engine.make_target(native_handle, warp_frontend_port=wp.frontend_port,
                                                 path=path, via_docker_host=via_docker)
                    res = run_one(adapter, scenario, target)
                    timed_out = getattr(res, "_sp_timeout", False)
                    if timed_out:
                        verdict, reason = TIMEOUT, f"scenario exceeded {SCENARIO_TIMEOUT_SECS}s hard timeout"
                    elif gold is None:
                        verdict, reason = "SKIP", "no golden native baseline for this client " \
                                                   "(absent, or unstable at record time)"
                    else:
                        verdict, reason = sp_core.classify(path, gold, res, known_diffs, scenario)
                    entry = {"scenario": scenario.id, "client": cname, "path": path,
                              "verdict": verdict, "reason": reason, "result": res.to_dict()}
                    if verdict == "WARP_BUG":
                        entry["diff"] = sp_core.result_diff_text(gold, res) if gold else None
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
