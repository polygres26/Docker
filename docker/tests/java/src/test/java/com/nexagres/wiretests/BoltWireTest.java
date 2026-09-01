package com.nexagres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;

/** boltwire: real Neo4j Bolt+Cypher wire protocol -- CREATE (nodes and single-hop node-edge-node
 * patterns), MATCH/WHERE/RETURN (fixed single-hop and bounded variable-length {@code [*min..max]}
 * paths), all translated to Postgres underneath (see
 * {@code com.nexagres.wire.boltwire.PgGraphStore}'s own javadoc for the two-table property-graph
 * schema). Uses the real official {@code neo4j-java-driver}, not a hand-rolled Bolt client -- no
 * auth configured in the test compose file, so any username/password is accepted. */
class BoltWireTest {

    private static Driver driver() {
        return GraphDatabase.driver("bolt://" + TestConfig.HOST + ":" + TestConfig.BOLTWIRE_PORT,
                AuthTokens.basic("neo4j", "neo4j"));
    }

    private static String label() {
        // A uuid-suffixed label keeps each test's nodes from colliding with another test's --
        // same isolation approach InfluxWireTest's own uuid-suffixed measurement() gives it.
        return "Smoke" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    @Test
    void createAndReturnNodeRoundTrip() {
        try (Driver d = driver(); Session s = d.session()) {
            String lbl = label();
            Record r = s.run("CREATE (n:" + lbl + " {name: 'Ada', age: 36}) RETURN n").single();
            Node node = r.get("n").asNode();
            assertEquals(lbl, node.labels().iterator().next());
            assertEquals("Ada", node.get("name").asString());
            assertEquals(36, node.get("age").asInt());
        }
    }

    @Test
    void createReturnsASinglePropertyNotTheWholeNode() {
        try (Driver d = driver(); Session s = d.session()) {
            String lbl = label();
            Record r = s.run("CREATE (n:" + lbl + " {name: 'Grace'}) RETURN n.name AS name").single();
            assertEquals("Grace", r.get("name").asString());
        }
    }

    @Test
    void matchFiltersByInlineProperty() {
        try (Driver d = driver(); Session s = d.session()) {
            String lbl = label();
            s.run("CREATE (n:" + lbl + " {name: 'Alice'}) RETURN n").consume();
            s.run("CREATE (n:" + lbl + " {name: 'Bob'}) RETURN n").consume();

            List<Record> rows = s.run("MATCH (n:" + lbl + " {name: 'Alice'}) RETURN n.name AS name").list();
            assertEquals(1, rows.size());
            assertEquals("Alice", rows.get(0).get("name").asString());
        }
    }

    @Test
    void matchWhereClauseComparesNumerically() {
        // Real bug, found live writing this same test in Python (see test_boltwire.py): a numeric
        // WHERE comparison was being done as a lexical text comparison ("5" > "18" is true as
        // text), so this exact query used to also match age=5. Kept here too since Java's own
        // int-typed asInt() would have masked a different failure mode (a string vs. numeric type
        // mismatch) than what the Python driver's own type coercion happened to catch.
        try (Driver d = driver(); Session s = d.session()) {
            String lbl = label();
            s.run("CREATE (n:" + lbl + " {name: 'Old', age: 80}) RETURN n").consume();
            s.run("CREATE (n:" + lbl + " {name: 'Young', age: 5}) RETURN n").consume();

            List<Record> rows = s.run("MATCH (n:" + lbl + ") WHERE n.age > 18 RETURN n.name AS name").list();
            assertEquals(1, rows.size());
            assertEquals("Old", rows.get(0).get("name").asString());
        }
    }

    @Test
    void createEdgeAndMatchSingleHop() {
        try (Driver d = driver(); Session s = d.session()) {
            String lbl = label();
            s.run("CREATE (a:" + lbl + " {name: 'Alice'})-[:KNOWS]->(b:" + lbl + " {name: 'Bob'}) "
                    + "RETURN a").consume();

            List<Record> rows = s.run(
                    "MATCH (a:" + lbl + " {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name AS name").list();
            assertEquals(1, rows.size());
            assertEquals("Bob", rows.get(0).get("name").asString());
        }
    }

    @Test
    void variableLengthPathReturnsEveryReachableNodeOnce() throws Exception {
        // CREATE only ever attaches an edge between two brand-new nodes in this grammar (no
        // MERGE/match-then-create yet), so a 3-hop chain can't be built as one Cypher statement.
        // Two independent hops are created (Alice->Bob, then a standalone Carol node), and a
        // single raw-SQL edge insert against the real underlying tables (via pgwire's own JDBC
        // driver, already a dependency here) stitches Bob directly to that same Carol -- the same
        // way this feature's own implementation was verified live.
        try (Driver d = driver(); Session s = d.session()) {
            String lbl = label();
            s.run("CREATE (a:" + lbl + " {name: 'Alice'})-[:KNOWS]->(b:" + lbl + " {name: 'Bob'}) "
                    + "RETURN a").consume();
            s.run("CREATE (c:" + lbl + " {name: 'Carol'}) RETURN c").consume();

            // Bolt 4.4's own Node struct has no elementId field at all (see PackStream.Writer's
            // own javadoc) -- id() is the real, and only, node identifier this server's wire
            // format sends.
            long bobPk = s.run("MATCH (a:" + lbl + " {name: 'Alice'})-[:KNOWS]->(b) RETURN b")
                    .single().get("b").asNode().id();
            long carolPk = s.run("MATCH (c:" + lbl + " {name: 'Carol'}) RETURN c")
                    .single().get("c").asNode().id();

            // binaryTransfer=false: pgwire's own frontend doesn't support binary-format bind
            // parameters (see StatementPipeline's own bind handling) -- pgjdbc defaults to binary
            // for a bigint parameter, which real Postgres itself accepts but pgwire doesn't yet,
            // so this has to be forced off for a direct write like this one.
            String url = "jdbc:postgresql://" + TestConfig.HOST + ":" + TestConfig.PGWIRE_PORT
                    + "/postgres?binaryTransfer=false";
            try (Connection c = DriverManager.getConnection(url, "postgres", "postgres");
                    PreparedStatement ps = c.prepareStatement(
                            "INSERT INTO warp_graph_edges (type, from_id, to_id) VALUES (?, ?, ?)")) {
                ps.setString(1, "KNOWS");
                ps.setLong(2, bobPk);
                ps.setLong(3, carolPk);
                ps.executeUpdate();
            }

            List<Record> rows = s.run(
                    "MATCH (a:" + lbl + " {name: 'Alice'})-[:KNOWS*1..2]->(x) RETURN x.name AS name").list();
            List<String> names = rows.stream().map(r -> r.get("name").asString()).sorted().toList();
            assertEquals(List.of("Bob", "Carol"), names);
        }
    }

    @Test
    void bareUnboundedStarIsRejectedNotSilentlyAccepted() {
        try (Driver d = driver(); Session s = d.session()) {
            String lbl = label();
            s.run("CREATE (n:" + lbl + " {name: 'X'}) RETURN n").consume();
            try {
                s.run("MATCH (a:" + lbl + ")-[:KNOWS*]->(b) RETURN b").consume();
                fail("expected boltwire to reject an unbounded [*] path");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("unbounded"));
            }
        }
    }

    @Test
    void returnLiteralStillWorks() {
        // Phase 1's own original scope -- a query that never touches the graph store at all --
        // still has to keep working once CREATE/MATCH exist alongside it.
        try (Driver d = driver(); Session s = d.session()) {
            Result result = s.run("RETURN 1 AS x");
            assertEquals(1, result.single().get("x").asInt());
        }
    }
}
