package com.sayonora.warp.cqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.cqlwire.Ast.*;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

class ParserTest {

    @Test
    void createTableWithKeysStaticAndClusteringOrder() {
        CreateTable c = (CreateTable) Parser.parse("CREATE TABLE IF NOT EXISTS ks.t (a int, b text, c bigint, s int STATIC, v map<text, frozen<list<int>>>, "
                + "PRIMARY KEY ((a, b), c)) WITH CLUSTERING ORDER BY (c DESC) AND comment = 'x' AND compaction = {'class': 'LeveledCompactionStrategy'}");
        assertEquals("ks", c.ks());
        assertEquals("t", c.name());
        assertTrue(c.ine());
        assertEquals(List.of("a", "b"), c.partition());
        assertEquals(List.of("c"), c.clustering());
        assertEquals(List.of(true), c.clusteringDesc());
        assertTrue(c.cols().get(3).isStatic());
        assertEquals("map<text, frozen<list<int>>>", c.cols().get(4).type());
        assertEquals("x", c.options().get("comment"));
    }

    @Test
    void inlinePrimaryKeyAndQuotedIdentifiers() {
        CreateTable c = (CreateTable) Parser.parse("CREATE TABLE \"Mixed\" (\"Id\" int PRIMARY KEY, Val text)");
        assertEquals("Mixed", c.name());
        assertEquals(List.of("Id"), c.partition());
        assertEquals("val", c.cols().get(1).name());
    }

    @Test
    void dmlWithUsingClauseConditionsAndBinds() {
        Insert i = (Insert) Parser.parse("INSERT INTO t (a, b) VALUES (?, :name) IF NOT EXISTS USING TTL 5 AND TIMESTAMP 10");
        assertTrue(i.ine());
        assertEquals(List.of("a", "b"), i.cols());
        assertEquals(new Bind(0, null), i.values().get(0));
        assertEquals(new Bind(1, "name"), i.values().get(1));
        assertEquals(2, Parser.parseFull("INSERT INTO t (a, b) VALUES (?, :name)").binds());
        Update u = (Update) Parser.parse("UPDATE t USING TTL 5 SET a = a + 1, l = [1] + l, m['k'] = 3 WHERE k = 1 AND c IN (1, 2) IF a = 0 AND b IN (1)");
        assertEquals('+', u.assigns().get(0).arith());
        assertTrue(u.assigns().get(1).prepend());
        assertEquals(2, u.where().size());
        assertEquals(2, u.conds().size());
        Delete d = (Delete) Parser.parse("DELETE a, m['k'] FROM t USING TIMESTAMP 5 WHERE k = ? IF EXISTS");
        assertEquals(2, d.targets().size());
        assertTrue(d.ifExists());
        Batch b = (Batch) Parser.parse("BEGIN UNLOGGED BATCH USING TIMESTAMP 5 INSERT INTO t (k) VALUES (1); DELETE FROM t WHERE k = 2; APPLY BATCH");
        assertEquals("UNLOGGED", b.kind());
        assertEquals(2, b.stmts().size());
    }

    @Test
    void selectFormsAndRelations() {
        Ast.Select s = (Ast.Select) Parser.parse("SELECT DISTINCT k, count(*), writetime(v) AS w, token(k), CAST(x AS text) FROM ks.t WHERE token(k) > ? AND (c1, c2) >= (1, 2) "
                + "AND v CONTAINS KEY 'a' AND k IN ? GROUP BY k ORDER BY c DESC PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING");
        assertTrue(s.distinct() && s.allowFiltering());
        assertEquals("w", s.selectors().get(2).alias());
        assertTrue(s.where().get(0).token());
        assertTrue(s.where().get(1).multi());
        assertEquals("contains key", s.where().get(2).op());
        assertEquals("in", s.where().get(3).op());
        assertEquals(List.of("k"), s.groupBy());
        assertTrue(s.order().get(0).desc());
        Ast.Select j = (Ast.Select) Parser.parse("SELECT JSON * FROM t");
        assertTrue(j.json());
    }

    @Test
    void literalsAndTerms() {
        Insert i = (Insert) Parser.parse("INSERT INTO t (a,b,c,d,e,f,g,h,i,j) VALUES (-5, 1.5e3, 'it''s', $$dollar$$, 0xCAFE, 11111111-1111-1111-1111-111111111111, "
                + "true, NaN, 1h30m, {1: 'a'})");
        assertEquals(BigInteger.valueOf(-5), ((Lit) i.values().get(0)).value());
        assertEquals("it's", ((Lit) i.values().get(2)).value());
        assertEquals("dollar", ((Lit) i.values().get(3)).value());
        assertInstanceOf(byte[].class, ((Lit) i.values().get(4)).value());
        assertInstanceOf(long[][].class, ((Lit) i.values().get(8)).value());
        assertInstanceOf(BraceLit.class, i.values().get(9));
        Insert u = (Insert) Parser.parse("INSERT INTO t (a, b) VALUES ({street: 'x', zip: 1}, (1, 'a'))");
        assertEquals(2, ((BraceLit) u.values().get(0)).fields().size());
        assertInstanceOf(TupleLit.class, u.values().get(1));
    }

    @Test
    void syntaxErrorsAreSyntaxErrors() {
        for (String bad : List.of("", ";", "SELEC * FROM t", "SELECT", "SELECT * FROM", "INSERT INTO t (a) VALUES (1", "SELECT * FROM t LIMIT 'x'",
                "SELECT * FROM t WHERE a = 1 ALLOW FILTERING LIMIT 1", "CREATE TABLE t (a int PRIMARY KEY, primary int)", "SELECT 'unterminated FROM t",
                "SELECT -x FROM t", "USE", "FOO BAR")) {
            CqlError e = assertThrows(CqlError.class, () -> Parser.parse(bad), bad);
            assertEquals(CqlError.SYNTAX, e.code, bad);
        }
    }

    @Test
    void commentsAndTrailingSemicolons() {
        assertInstanceOf(Ast.Select.class, Parser.parse("-- c\nSELECT /* x */ * FROM t // y\n;;"));
    }

    @Test
    void describeAndUnsupported() {
        Describe d = (Describe) Parser.parse("DESCRIBE ONLY KEYSPACE ks");
        assertEquals("keyspace", d.what());
        assertTrue(d.only());
        assertEquals("ks", d.name());
        assertEquals("table", ((Describe) Parser.parse("desc table ks.t")).what());
        assertInstanceOf(Other.class, Parser.parse("GRANT SELECT ON t TO r"));
        assertInstanceOf(Other.class, Parser.parse("CREATE ROLE r WITH PASSWORD = 'x'"));
    }
}
