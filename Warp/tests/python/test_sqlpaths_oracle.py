"""pytest wrapper for the Oracle sqlpaths matrix -- a FAST SMOKE SUBSET only (a handful of
scenarios, python client, native+relay), meant to run in normal CI/dev time budgets. The full
150+/70-scenario, all-client, all-path matrix runs from the CLI instead:

    python3 Warp/tests/sqlpaths/run_matrix.py --engine oracle \\
        --clients sqlplus,sqlcl,jdbc,python --paths native,relay,adapt

Requires: a real Oracle container (this test starts and tears down its own, ~1-3 min cold start)
and a built Warp jar (`mvn -DskipTests package` in Warp/ first). Skips cleanly if Docker or the
Warp jar aren't available, matching this repo's other *_support.py-based tests' style.
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
from engines.oracle import sp_oracle_engine as oracle_engine  # noqa: E402
from warp_test_support import WarpProcess, RealPostgres, isolated_ports  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")

SMOKE_IDS = {"ora001", "ora004", "ora017", "ora024", "ora028", "ora042", "ora047", "ora050"}


@pytest.fixture(scope="module")
def native_db():
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


@pytest.mark.parametrize("scenario", _scenarios(), ids=lambda s: s.id)
def test_relay_matches_native_python_client(scenario, native_db, relay_warp):
    known = {}  # smoke subset intentionally excludes any scenario needing a documented KNOWN_DIFF
    from adapters import sp_python_client as client

    native_target = oracle_engine.make_target(native_db, path="native")
    relay_target = oracle_engine.make_target(native_db, warp_frontend_port=relay_warp.frontend_port,
                                              path="relay")
    native_res = client.run(scenario, native_target)
    relay_res = client.run(scenario, relay_target)
    verdict, reason = sp_core.classify("relay", native_res, relay_res, known, scenario)
    assert verdict in ("PASS", "KNOWN_DIFF"), (
        f"{scenario.id}: RELAY diverged from NATIVE -- {sp_core.result_diff_text(native_res, relay_res)}")
