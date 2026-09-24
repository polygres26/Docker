"""Helper for standing up TWO real Warp processes that join one real Ignite cluster
(WARP_CLUSTER_ENABLED=true), sharing one real Postgres backend -- used by
test_distributed_cache_rtt.py to prove a cross-node cache hit (nodeB serving a key that only
nodeA's read ever populated) is real, not single-node.

Discovery: WarpCluster.buildIpFinderFromEnv's "static" mode (the default,
WARP_CLUSTER_DISCOVERY unset) uses a plain TcpDiscoveryVmIpFinder seeded from
WARP_CLUSTER_SEED_NODES (comma-separated host:port, host:port1..port2 ranges supported by
Ignite's own VM finder). TcpDiscoverySpi's default local port is 47500 with a 100-port local
port range, so as long as both nodes are seeded with a range that covers whichever port each one
actually binds to, they will find each other on localhost regardless of exactly which port either
one picks (confirmed from WarpCluster.java: no code changes needed for a same-machine 2-node
test, just real env vars).
"""
import os

from warp_test_support import WarpProcess, RealPostgres, free_port  # noqa: F401

CLUSTER_SEED_RANGE = "127.0.0.1:47500..47600"


def cluster_env(cache_tables, cache_ttl_ms="60000"):
    return {
        "WARP_CLUSTER_ENABLED": "true",
        "WARP_CLUSTER_DISCOVERY": "static",
        "WARP_CLUSTER_SEED_NODES": CLUSTER_SEED_RANGE,
        "WARP_CACHE_TABLES": cache_tables,
        "WARP_CACHE_TTL_MS": cache_ttl_ms,
    }


def start_cluster_node(postgres: RealPostgres, cache_tables, frontend_name):
    return WarpProcess(
        postgres, "WARP_PGWIRE_PORT", frontend_name=frontend_name,
        extra_env=cluster_env(cache_tables),
    )
