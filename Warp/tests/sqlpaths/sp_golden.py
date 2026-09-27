"""sp_golden: frozen NATIVE-baseline golden file, engine-agnostic.

Follows this repo's established golden-file convention (see Warp/tests/python/*_conformance/
golden.json.gz, e.g. bt_conformance/bt_harness.py's record()/replay_golden()): the "true" answer
for a scenario is recorded ONCE, offline, from the real backend and frozen into a small gzip JSON
file checked into the repo. Every ordinary run afterwards replays/diffs against that frozen file
and never re-queries the real backend again for the baseline.

File shape (per engine, e.g. engines/oracle/golden/golden_native.json.gz):

    {
      "engine": "oracle",
      "recorded_at": "<ISO8601 timestamp, informational only>",
      "clients": ["python", "jdbc", ...],       # clients this recording covers
      "cases": {
        "<scenario_id>": {
          "<client_name>": <CanonicalResult.to_dict() | null>   # null = unstable at record time
        }
      }
    }

Recording is a rare, deliberate, one-time (or occasional, e.g. after an Oracle version upgrade)
operation an engineer runs explicitly (`run_matrix.py --record-native`) -- never part of an
ordinary CI/dev run.
"""
from __future__ import annotations

import datetime
import gzip
import json
import os

import sp_core


def record_native(engine_module, clients: dict, native_target_fn, scenarios, times: int = 2) -> dict:
    """Runs every scenario against the real, Warp-bypassing NATIVE target directly, `times` times
    per (scenario, client), and keeps only stable (identical-across-runs) results -- exactly the
    bt_conformance/kf_conformance golden-recording convention. `clients` is a name -> ClientAdapter
    dict (already filtered to available() clients). `native_target_fn` builds the Target for one
    client's native connection (engine.make_target(handle, path="native")).
    Returns the golden dict (not yet written to disk)."""
    cases: dict = {}
    for scenario in scenarios:
        per_client = {}
        for cname, adapter in clients.items():
            if scenario.client_filter and cname not in scenario.client_filter:
                continue
            target = native_target_fn()
            runs = []
            for _ in range(times):
                try:
                    runs.append(adapter.run(scenario, target).to_dict())
                except Exception as e:  # noqa: BLE001 -- recorded as an error result like any other
                    runs.append({"ok": False, "error_code": "RECORD_EXCEPTION",
                                 "error_text": f"{type(e).__name__}: {e}"[:500],
                                 "columns": None, "rows": None, "row_count": None,
                                 "raw": None, "client_note": None})
            stable = all(r == runs[0] for r in runs[1:])
            per_client[cname] = runs[0] if stable else None
            print(f"[sp_golden] recorded {scenario.id}/{cname}: "
                  f"{'stable' if stable else 'UNSTABLE (dropped)'}", flush=True)
        cases[scenario.id] = per_client
    return {
        "engine": engine_module.ENGINE.name,
        "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "clients": sorted(clients.keys()),
        "cases": cases,
    }


def write_golden(golden: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt") as f:
        json.dump(golden, f, indent=0, sort_keys=True)


def load_golden(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"golden native-baseline file not found: {path}\n"
            "Ordinary runs never live-query NATIVE Oracle for the baseline any more -- record it "
            "once with `run_matrix.py --engine <engine> --record-native --reuse-oracle host:port` "
            "(see docs/sqlpaths/oracle_notes.md).")
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        return json.load(f)


def golden_result(golden: dict, scenario_id: str, client: str):
    """Returns the CanonicalResult (or None if absent/unstable) for one (scenario, client)."""
    d = (golden.get("cases", {}).get(scenario_id) or {}).get(client)
    return sp_core.CanonicalResult.from_dict(d)
