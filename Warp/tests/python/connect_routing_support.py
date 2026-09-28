"""Shared topology for the connect-time backend routing tests (test_connect_routing*.py).

Three real Postgres servers (the Developer licence caps a Warp at 3 backends) registered as:

    default  -- the config Postgres           set "default"   (implicit)
    node_b     -- a second Postgres             set "alpha"
    node_c     -- a third Postgres              set "alpha"

Every server holds a DIFFERENT row set in the SAME table name `items`, plus schemas `cs`/`ds` (with
their own `items`) that the router schema rules `cs -> node_c`, `ds -> default` point at -- so any
statement that reaches the wrong backend returns visibly wrong rows instead of merely an error.
"""
import os

import psycopg2

from warp_test_support import RealPostgres, WarpProcess, isolated_ports

ADMIN_TOKEN = "warp-test-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN_TOKEN

ROWS = {
    "default": [(1, "default-row")],
    "node_b": [(1, "b-row"), (2, "b-row-2")],
    "node_c": [(1, "c-row")],
}
SCHEMA_ROWS = {"cs": ("node_c", "c-schema-row"), "ds": ("default", "d-schema-row")}

# tables that exist on ONE backend only (auto-federated across a set's members when joined)
ONLY = {"node_b": ("only_b", "only-b-row"), "node_c": ("only_c", "only-c-row"), "default": ("only_d", "only-d-row")}


def node_connect(pg, dbname="postgres"):
    return psycopg2.connect(host="localhost", port=pg.port, user="postgres", password="postgres", dbname=dbname)


def seed(node_by_backend):
    for backend, rows in ROWS.items():
        with node_connect(node_by_backend[backend]) as c:
            cur = c.cursor()
            cur.execute("DROP TABLE IF EXISTS items")
            cur.execute("CREATE TABLE items (id int primary key, name text)")
            for r in rows:
                cur.execute("INSERT INTO items VALUES (%s, %s)", r)
    for backend, (table, name) in ONLY.items():
        with node_connect(node_by_backend[backend]) as c:
            cur = c.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} (id int primary key, name text)")
            cur.execute(f"INSERT INTO {table} VALUES (1, %s)", (name,))
    for schema, (backend, name) in SCHEMA_ROWS.items():
        with node_connect(node_by_backend[backend]) as c:
            cur = c.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(f"DROP TABLE IF EXISTS {schema}.items")
            cur.execute(f"CREATE TABLE {schema}.items (id int primary key, name text)")
            cur.execute(f"INSERT INTO {schema}.items VALUES (1, %s)", (name,))


def rows_in(pg, sql="SELECT name FROM items ORDER BY id"):
    with node_connect(pg) as c:
        cur = c.cursor()
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]


class Topology:
    """Three Postgres servers + one Warp with every listener on its own free port."""

    def __init__(self, extra_env=None, credentials=None):
        self.pgs = [RealPostgres() for _ in range(3)]
        self.by_backend = {"default": self.pgs[0], "node_b": self.pgs[1], "node_c": self.pgs[2]}
        seed(self.by_backend)
        url = lambda pg: f"jdbc:postgresql://localhost:{pg.port}/postgres"  # noqa: E731
        spec = ";".join(f"{name}={url(pg)}|postgres|postgres" for name, pg in self.by_backend.items())
        self.ports = isolated_ports("WARP_PGWIRE_PORT")
        env = {
            "WARP_BACKENDS": spec,
            "WARP_BACKEND_GROUPS": "alpha:plain=node_b,node_c",
            "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
            "WARP_ROUTER_SCHEMA_RULES": "cs:node_c,ds:default",
            "WARP_MYWIRE_PORT": self.ports["WARP_MYWIRE_PORT"],
            **self.ports,
        }
        if credentials:
            env["WARP_AUTH_CREDENTIALS"] = credentials
        env.update(extra_env or {})
        self.warp = WarpProcess(self.pgs[0], "WARP_PGWIRE_PORT", frontend_name="connect-routing", extra_env=env)

    def port(self, var):
        return int(self.ports[var])

    def close(self):
        self.warp.close()
        for pg in self.pgs:
            pg.close()
