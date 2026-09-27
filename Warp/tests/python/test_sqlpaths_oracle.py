"""pytest wrapper for the Oracle sqlpaths matrix -- a FAST SMOKE SUBSET only (a handful of
scenarios, python client, relay path only), meant to run in normal CI/dev time budgets. The full
150+/70-scenario, all-client, all-path matrix runs from the CLI instead:

    python3 Warp/tests/sqlpaths/run_matrix.py --engine oracle \\
        --clients sqlplus,sqlcl,jdbc,python --paths relay,adapt,bridge

NATIVE is never live-executed here (or anywhere in an ordinary run): this test diffs RELAY against
the frozen golden native-baseline file (engines/oracle/golden/golden_native.json.gz), exactly like
`run_matrix.py`'s ordinary (non `--record-native`) mode. The only real-Oracle connection this test
makes is the one Warp itself opens to front RELAY's backend -- no separate, Warp-bypassing client
connection is used for the baseline any more. See docs/sqlpaths/oracle_notes.md for the recording
workflow (a rare, deliberate, one-time-per-Oracle-version operation, not something CI ever runs).

Requires: a real Oracle container (this test starts and tears down its own, ~1-3 min cold start,
used only as RELAY's real backend) and a built Warp jar (`mvn -DskipTests package` in Warp/
first). Skips cleanly if Docker or the Warp jar aren't available, matching this repo's other
*_support.py-based tests' style.
"""
import os
import shutil
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SQLPATHS_DIR = os.path.join(os.path.dirname(HERE), "sqlpaths")
sys.path.insert(0, SQLPATHS_DIR)
sys.path.insert(0, HERE)

import sp_core  # noqa: E402
import sp_golden  # noqa: E402
from engines.oracle import sp_oracle_engine as oracle_engine  # noqa: E402
from warp_test_support import WarpProcess, RealPostgres, isolated_ports  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")

SMOKE_IDS = {"ora001", "ora004", "ora017", "ora024", "ora028", "ora042", "ora047", "ora050"}


@pytest.fixture(scope="module")
def native_db():
    """A real Oracle container -- used ONLY as RELAY's real backend below, never queried directly
    by this test any more; the NATIVE baseline comes from the frozen golden file instead."""
    handle = oracle_engine.start_native_db()
    yield handle
    oracle_engine.stop_native_db(handle)


@pytest.fixture(scope="module")
def relay_warp(native_db):
    pg = RealPostgres()
    env = {**isolated_ports("WARP_ORAWIRE_PORT"), **oracle_engine.relay_env(native_db)}
    proc = WarpProcess(pg, "WARP_ORAWIRE_PORT", frontend_name="orawire-relay-smoke", extra_env=env)
    yield proc
    proc.close()
    pg.close()


def _scenarios():
    all_scenarios = sp_core.load_scenarios(oracle_engine.ENGINE.scenarios_path)
    return [s for s in all_scenarios if s.id in SMOKE_IDS]


@pytest.fixture(scope="module")
def golden():
    return sp_golden.load_golden(oracle_engine.ENGINE.golden_native_path)


@pytest.mark.parametrize("scenario", _scenarios(), ids=lambda s: s.id)
def test_relay_matches_golden_native_python_client(scenario, native_db, relay_warp, golden):
    known = {}  # smoke subset intentionally excludes any scenario needing a documented KNOWN_DIFF
    from adapters import sp_python_client as client

    relay_target = oracle_engine.make_target(native_db, warp_frontend_port=relay_warp.frontend_port,
                                              path="relay")
    gold = sp_golden.golden_result(golden, scenario.id, "python")
    assert gold is not None, (
        f"{scenario.id}: no golden native-baseline entry for the python client -- re-record with "
        f"`run_matrix.py --engine oracle --clients python --record-native --reuse-oracle host:port` "
        f"(see docs/sqlpaths/oracle_notes.md)")
    relay_res = client.run(scenario, relay_target)
    verdict, reason = sp_core.classify("relay", gold, relay_res, known, scenario)
    assert verdict in ("PASS", "KNOWN_DIFF"), (
        f"{scenario.id}: RELAY diverged from the golden NATIVE baseline -- "
        f"{sp_core.result_diff_text(gold, relay_res)}")
