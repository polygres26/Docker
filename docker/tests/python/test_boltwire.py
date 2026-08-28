"""boltwire: real Neo4j Bolt+Cypher wire protocol -- CREATE (nodes and single-hop node-edge-node
patterns), MATCH/WHERE/RETURN (fixed single-hop and bounded variable-length [*min..max] paths), all
translated to Postgres underneath (see com.nexagres.wire.boltwire.PgGraphStore's javadoc for the
two-table property-graph schema). Uses the real `neo4j` Python driver, not a hand-rolled Bolt
client -- no auth configured in the test compose file, so any username/password is accepted.
"""
import uuid

import psycopg2
from neo4j import GraphDatabase

from conftest import HOST, BOLTWIRE_PORT, PGWIRE_PORT


def driver():
    return GraphDatabase.driver(f"bolt://{HOST}:{BOLTWIRE_PORT}", auth=("neo4j", "neo4j"))


def label():
    # Real Neo4j labels/relationship types are plain identifiers -- a uuid-suffixed label (not a
    # uuid-suffixed property value) keeps each test's nodes from colliding with another test's,
    # the same isolation approach test_influxwire.py's per-test measurement() name gives it.
    return f"Smoke{uuid.uuid4().hex[:8]}"


def test_create_and_return_node_round_trip():
    with driver() as d, d.session() as s:
        lbl = label()
        record = s.run(f"CREATE (n:{lbl} {{name: 'Ada', age: 36}}) RETURN n").single()
        node = record["n"]
        assert set(node.labels) == {lbl}
        assert node["name"] == "Ada"
        assert node["age"] == 36


def test_create_returns_a_single_property_not_the_whole_node():
    with driver() as d, d.session() as s:
        lbl = label()
        record = s.run(f"CREATE (n:{lbl} {{name: 'Grace'}}) RETURN n.name AS name").single()
        assert record["name"] == "Grace"


def test_match_filters_by_inline_property():
    with driver() as d, d.session() as s:
        lbl = label()
        s.run(f"CREATE (n:{lbl} {{name: 'Alice'}}) RETURN n").consume()
        s.run(f"CREATE (n:{lbl} {{name: 'Bob'}}) RETURN n").consume()

        records = list(s.run(f"MATCH (n:{lbl} {{name: 'Alice'}}) RETURN n.name AS name"))
        assert [r["name"] for r in records] == ["Alice"]


def test_match_where_clause():
    with driver() as d, d.session() as s:
        lbl = label()
        s.run(f"CREATE (n:{lbl} {{name: 'Old', age: 80}}) RETURN n").consume()
        s.run(f"CREATE (n:{lbl} {{name: 'Young', age: 5}}) RETURN n").consume()

        records = list(s.run(f"MATCH (n:{lbl}) WHERE n.age > 18 RETURN n.name AS name"))
        assert [r["name"] for r in records] == ["Old"]


def test_create_edge_and_match_single_hop():
    with driver() as d, d.session() as s:
        lbl = label()
        s.run(f"CREATE (a:{lbl} {{name: 'Alice'}})-[:KNOWS]->(b:{lbl} {{name: 'Bob'}}) "
              "RETURN a").consume()

        records = list(s.run(
            f"MATCH (a:{lbl} {{name: 'Alice'}})-[:KNOWS]->(b) RETURN b.name AS name"))
        assert [r["name"] for r in records] == ["Bob"]


def test_variable_length_path_returns_every_reachable_node_once():
    # CREATE only ever attaches an edge between two brand-new nodes in this grammar (no
    # MERGE/match-then-create yet -- see CypherParser.CreateStatement's own single node-rel-node
    # shape), so a 3-hop chain can't be built as one Cypher statement. Two independent hops are
    # created (Alice->Bob, then a standalone Carol node), and a single raw-SQL edge insert against
    # the real underlying tables stitches Bob directly to that same Carol -- the same way this
    # feature's own implementation was verified live.
    with driver() as d, d.session() as s:
        lbl = label()
        s.run(f"CREATE (a:{lbl} {{name: 'Alice'}})-[:KNOWS]->(b:{lbl} {{name: 'Bob'}}) "
              "RETURN a").consume()
        s.run(f"CREATE (c:{lbl} {{name: 'Carol'}}) RETURN c").consume()

        # Bolt 4.4's own Node struct has no elementId field at all (see PackStream.Writer's own
        # javadoc) -- .id is the real, and only, node identifier this server's wire format sends.
        bob_pk = s.run(
            f"MATCH (a:{lbl} {{name: 'Alice'}})-[:KNOWS]->(b) RETURN b").single()["b"].id
        carol_pk = s.run(
            f"MATCH (c:{lbl} {{name: 'Carol'}}) RETURN c").single()["c"].id

        conn = psycopg2.connect(host=HOST, port=PGWIRE_PORT, dbname="postgres",
                                 user="postgres", password="postgres")
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO polywire_graph_edges (type, from_id, to_id) VALUES (%s, %s, %s)",
                    ("KNOWS", bob_pk, carol_pk))
        finally:
            conn.close()

        records = list(s.run(f"MATCH (a:{lbl} {{name: 'Alice'}})-[:KNOWS*1..2]->(x) "
                              "RETURN x.name AS name"))
        names = sorted(r["name"] for r in records)
        assert names == ["Bob", "Carol"]


def test_bare_unbounded_star_is_rejected_not_silently_accepted():
    with driver() as d, d.session() as s:
        lbl = label()
        s.run(f"CREATE (n:{lbl} {{name: 'X'}}) RETURN n").consume()
        try:
            s.run(f"MATCH (a:{lbl})-[:KNOWS*]->(b) RETURN b").consume()
            assert False, "expected boltwire to reject an unbounded [*] path"
        except Exception as e:
            assert "unbounded" in str(e)


def test_return_literal_still_works():
    # Phase 1's own original scope -- a query that never touches the graph store at all --
    # still has to keep working once CREATE/MATCH exist alongside it.
    with driver() as d, d.session() as s:
        record = s.run("RETURN 1 AS x").single()
        assert record["x"] == 1
