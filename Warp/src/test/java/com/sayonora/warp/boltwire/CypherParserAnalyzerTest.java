package com.sayonora.warp.boltwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Parser / semantic-analysis unit tests (no database): precedence, clause shapes and Neo4j's compile-time errors. */
class CypherParserAnalyzerTest {

    private static String code(String q) {
        return assertThrows(CypherException.class, () -> Analyzer.analyze(CypherParser.parse(q))).code();
    }

    @Test
    void parsesMatchWithPatternsAndProjection() {
        Cy.Query q = CypherParser.parse("MATCH (a:P {n: 1})-[r:K|L*1..3]->(b) WHERE a.x > 1 RETURN DISTINCT a.n AS n ORDER BY n SKIP 1 LIMIT 2");
        Analyzer.Info info = Analyzer.analyze(q);
        assertEquals(List.of("n"), info.columns);
        assertTrue(!info.updates);
    }

    @Test
    void nullPredicatesBindTighterThanComparison() {
        Cy.Query q = CypherParser.parse("RETURN null IS NULL = true AS v");
        Cy.Return r = (Cy.Return) q.parts().get(0).get(0);
        assertTrue(r.proj().items().get(0).expr() instanceof Cy.Cmp);
    }

    @Test
    void undefinedVariableAndTypeConflictsAreSyntaxErrors() {
        assertEquals(CypherException.SYNTAX, code("MATCH (a) RETURN b"));
        assertEquals(CypherException.SYNTAX, code("WITH 1 AS n MATCH (n) RETURN n"));
        assertEquals(CypherException.SYNTAX, code("MATCH (a) CREATE (a:L)"));
        assertEquals(CypherException.SYNTAX, code("CREATE (a)-[:R|S]->(b)"));
        assertEquals(CypherException.SYNTAX, code("MATCH (n) RETURN n.x, count(*) + n.y"));
        assertEquals(CypherException.SYNTAX, code("RETURN 1 AS a, 2 AS a"));
        assertEquals(CypherException.SYNTAX, code("CREATE (a) MATCH (b) RETURN b"));
        assertEquals(CypherException.SYNTAX, code("RETURN foo(1)"));
        assertEquals(CypherException.SYNTAX, code("MATCH (n) RETURN n LIMIT n.x"));
    }

    @Test
    void lexerErrorsAndLiteralRanges() {
        assertEquals(CypherException.SYNTAX, code("RETURN 9223372036854775808"));
        assertEquals(CypherException.SYNTAX, code("RETURN -0x8000000000000001"));
        assertEquals(CypherException.SYNTAX, code("RETURN 'unterminated"));
        Analyzer.analyze(CypherParser.parse("RETURN -9223372036854775808 AS m"));
    }

    @Test
    void updatingClausesAndSubqueries() {
        assertTrue(Analyzer.analyze(CypherParser.parse("MERGE (n:A {id: 1}) ON CREATE SET n.c = 1 ON MATCH SET n.m = 1")).updates);
        assertTrue(Analyzer.analyze(CypherParser.parse("FOREACH (x IN [1] | CREATE (:N))")).updates);
        assertEquals(CypherException.SYNTAX, code("MATCH (n) WHERE EXISTS { MATCH (n)-->(m) SET m.p = 1 } RETURN n"));
        Analyzer.analyze(CypherParser.parse("MATCH (n) WHERE EXISTS { (n)-->() } AND COUNT { (n)--() } > 1 RETURN n"));
    }

    @Test
    void schemaCommands() {
        assertTrue(Analyzer.analyze(CypherParser.parse("CREATE CONSTRAINT c FOR (n:L) REQUIRE n.p IS UNIQUE")).schema);
        assertTrue(Analyzer.analyze(CypherParser.parse("SHOW INDEXES YIELD name WHERE name = 'x' RETURN name")).schema);
    }

    @Test
    void valueSemantics() {
        assertEquals(Boolean.TRUE, Values.equal(1L, 1.0));
        assertEquals(null, Values.equal(null, 1L));
        assertEquals(Boolean.FALSE, Values.equal(Double.NaN, Double.NaN));
        assertEquals(null, Values.compare("a", 1L));
        assertTrue(Values.order(null, 1L) > 0);
        assertTrue(Values.order(1L, Double.NaN) < 0);
    }

    @Test
    void temporalRoundTrip() {
        assertEquals("P1Y2M3DT4H5M6.789S", Temporal.durationString(Temporal.parseDuration("P1Y2M3DT4H5M6.789S")));
        assertEquals("2015-07-21", Temporal.toStringValue(Temporal.parseDate("2015-W30-2")));
    }
}
