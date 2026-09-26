"""Starts Warp (WARP_TEST_JAR) on local Postgres process(es) with the datastorewire frontend; 1 or N sharded backends.

  python3 ds_launch_warp.py [SHARDS]      # prints the Datastore host:port and sleeps (SIGTERM stops Warp and Postgres)
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "fs_conformance"))
import fs_launch_warp  # noqa: E402


class DsWarp(fs_launch_warp.FsWarp):
    def __init__(self, shards=1, extra_env=None, pgs=None):
        super().__init__(shards, extra_env=extra_env, pgs=pgs, store_env="WARP_DATASTOREWIRE_PORT", stores=["datastore"])


if __name__ == "__main__":
    import signal
    import time
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
    w = DsWarp(int(sys.argv[1]) if len(sys.argv) > 1 else 1)
    try:
        print(w.host, flush=True)
        time.sleep(3600)
    finally:
        w.close()
