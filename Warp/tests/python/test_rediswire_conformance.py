"""rediswire differential conformance: replays the recorded answers of a REAL Redis (7.4.11) against Warp, offline.

The golden file (redis_conformance/golden/redis_golden.json.gz) is produced by
`python3 redis_conformance/run_redis_oracle.py record` (starts a redis:7 container). This test needs no Docker for the
oracle, only a Warp with native Postgres (WARP_TEST_PG_LOCAL=1) and a jar (WARP_TEST_JAR).
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "redis_conformance"))
import redis_diff as D  # noqa: E402

from redis_warp_support import start_warp  # noqa: E402
from warp_test_support import RealPostgres  # noqa: E402


@pytest.fixture(scope="module")
def warp():
    pg = RealPostgres()
    p = start_warp(pg)
    yield p
    p.close()
    pg.close()


def test_replay_golden_against_warp(warp):
    golden = D.load_golden()
    runner = D.Runner("127.0.0.1", warp.frontend_port)
    problems, replies = [], 0
    for name, resp3, steps in D.load_corpus():
        actual = runner.run_case(resp3, steps)
        replies += len(golden[name])
        problems += D.compare_case(name, steps, golden[name], actual)
    runner.close()
    assert replies > 2000
    assert not problems, "\n".join(problems[:40]) + f"\n{len(problems)} mismatches"
