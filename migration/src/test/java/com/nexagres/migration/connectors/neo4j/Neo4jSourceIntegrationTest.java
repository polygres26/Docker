package com.nexagres.migration.connectors.neo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.WarpGrpcSink;
import com.nexagres.migration.testsupport.WarpProcess;
import com.nexagres.migration.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

/**
 * End-to-end proof, real infrastructure throughout, using this session's own established
 * approach: since boltwire is Bolt-protocol-compatible, a real running Warp instance fronting
 * it stands in as a genuine Bolt source, using the real official Neo4j driver -- no external
 * Neo4j needed. Two separate Warp instances, exactly like migrating between two real,
 * independent systems: a source (boltwire only) and a target (grpc, for the real write path).
 *
 * <p>Proves: (1) nodes with different labels migrate correctly, including their real properties;
 * (2) a relationship between two migrated nodes resolves to the CORRECT target row ids via this
 * connector's own id-mapping table, not just "some" row; (3) {@code streamChanges} returns
 * immediately rather than blocking (this source has no live tail to follow at all).
 */
class Neo4jSourceIntegrationTest {

    @Test
    void nodesAndRelationshipsMigrateWithCorrectlyResolvedEndpointIds() throws Exception {
        try (RealPostgres sourcePostgres = RealPostgres.start();
                WarpProcess sourceWarp = WarpProcess.builder()
                        .pgBackend(sourcePostgres.host(), sourcePostgres.port(), sourcePostgres.database(), sourcePostgres.username(), sourcePostgres.password())
                        .frontend("boltwire", "WARP_BOLTWIRE_PORT")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .frontend("orawire", "WARP_ORAWIRE_PORT")
                        .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                        .frontend("sqswire", "WARP_SQSWIRE_PORT")
                        .frontend("oswire", "WARP_OSWIRE_PORT")
                        .frontend("influxwire", "WARP_INFLUXWIRE_PORT")
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start();
                RealPostgres targetPostgres = RealPostgres.start();
                WarpProcess targetWarp = WarpProcess.builder()
                        .pgBackend(targetPostgres.host(), targetPostgres.port(), targetPostgres.database(), targetPostgres.username(), targetPostgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start();
                Driver sourceDriver = GraphDatabase.driver(
                        "bolt://localhost:" + sourceWarp.port("boltwire"),
                        AuthTokens.basic(sourcePostgres.username(), sourcePostgres.password()))) {

            try (Session session = sourceDriver.session()) {
                // boltwire's own supported Cypher subset only allows ONE MATCH clause per
                // statement (confirmed live: two MATCH clauses fail with a real "expected RETURN,
                // got MATCH" parse error) -- so the relationship between two existing-ish nodes is
                // created directly via boltwire's supported CREATE-with-relationship form
                // (creating both endpoints and the edge in one statement) rather than matching two
                // pre-created nodes first.
                session.run("CREATE (n:Person {name: 'Alice', age: 30})-[r:WORKS_AT {since: 2020}]->(m:Company {name: 'Acme'}) RETURN n").consume();
                session.run("CREATE (n:Person {name: 'Bob', age: 25}) RETURN n").consume();
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(targetPostgres.jdbcUrl(), targetPostgres.username(), targetPostgres.password());
            checkpoints.ensureSchema();

            Neo4jSource source = new Neo4jSource(sourceDriver, List.of("Person", "Company"),
                    List.of(new Neo4jSource.RelationshipSpec("Person", "WORKS_AT", "Company")));
            WarpGrpcSink sink = new WarpGrpcSink("localhost", targetWarp.port("grpc"), targetPostgres.username(), targetPostgres.password());
            Coordinator coordinator = new Coordinator(source, sink, checkpoints, 1);
            coordinator.run(); // returns promptly -- streamChanges is a no-op for this connector

            try (Connection conn = DriverManager.getConnection(targetPostgres.jdbcUrl(), targetPostgres.username(), targetPostgres.password())) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM warp_graph_nodes");
                        ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(3, rs.getLong(1), "all 3 nodes (2 Person, 1 Company) should have migrated");
                }
                try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM warp_graph_edges");
                        ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(1, rs.getLong(1));
                }

                // The relationship's endpoints must resolve to the ACTUAL Alice/Acme rows, not
                // just any two node ids -- the real point of this connector's id-mapping table.
                // (Relationship-level PROPERTIES aren't migrated in v1 -- see Neo4jSource's own
                // javadoc: boltwire's RETURN clause can't project a relationship variable at all,
                // a real, confirmed limitation of this project's own test target, not the
                // connector -- so only topology + type are verified here.)
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT n1.properties->>'name' AS from_name, n2.properties->>'name' AS to_name, e.type "
                                + "FROM warp_graph_edges e "
                                + "JOIN warp_graph_nodes n1 ON e.from_id = n1.id "
                                + "JOIN warp_graph_nodes n2 ON e.to_id = n2.id");
                        ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("Alice", rs.getString("from_name"));
                    assertEquals("Acme", rs.getString("to_name"));
                    assertEquals("WORKS_AT", rs.getString("type"));
                }

                // Labels landed correctly, as a real Postgres text[].
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT labels FROM warp_graph_nodes WHERE properties->>'name' = 'Bob'");
                        ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    java.sql.Array labelsArray = rs.getArray("labels");
                    assertEquals("Person", ((String[]) labelsArray.getArray())[0]);
                }
            }
        }
    }
}
