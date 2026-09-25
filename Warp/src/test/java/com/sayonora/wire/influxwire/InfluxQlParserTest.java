package com.sayonora.wire.influxwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** InfluxQL grammar and the "error parsing query: found X, expected Y at line L, char C" messages. */
class InfluxQlParserTest {

    private static String err(String q) {
        return assertThrows(InfluxQl.ParseError.class, () -> InfluxQl.parse(q, null)).getMessage();
    }

    @Test
    void selectClauses() {
        InfluxQl.Select s = (InfluxQl.Select) InfluxQl.parse(
                "SELECT mean(usage) AS m, host FROM \"db\".\"rp\".cpu, /c.*/ WHERE time >= now() - 1h AND (host = 'a' OR host =~ /b$/) "
                        + "GROUP BY time(20s, 5s), host fill(previous) ORDER BY time DESC LIMIT 5 OFFSET 2 SLIMIT 3 SOFFSET 1", null).get(0);
        assertEquals(2, s.fields.size());
        assertEquals("m", s.fields.get(0).alias);
        assertEquals("db", s.sources.get(0).db);
        assertEquals("rp", s.sources.get(0).rp);
        assertEquals("cpu", s.sources.get(0).name);
        assertTrue(s.sources.get(1).regex != null);
        assertEquals(2, s.dims.size());
        assertEquals(20_000_000_000L, s.dims.get(0).interval);
        assertEquals(5_000_000_000L, s.dims.get(0).offset);
        assertEquals(InfluxQl.FillKind.PREVIOUS, s.fill);
        assertTrue(s.desc);
        assertEquals(5, s.limit);
        assertEquals(2, s.offset);
        assertEquals(3, s.slimit);
        assertEquals(1, s.soffset);
    }

    @Test
    void statementsAndMultipleStatements() {
        List<InfluxQl.Stmt> l = InfluxQl.parse("SHOW MEASUREMENTS; CREATE DATABASE d WITH DURATION 1d NAME r; DROP SERIES FROM m WHERE a = 'b'; DELETE FROM m WHERE time < 5;;", null);
        assertEquals(4, l.size());
        assertEquals("MEASUREMENTS", ((InfluxQl.Show) l.get(0)).kind);
        assertEquals("CREATE_DATABASE", ((InfluxQl.Ddl) l.get(1)).kind);
        assertEquals("r", ((InfluxQl.Ddl) l.get(1)).rpName);
        assertEquals("DROP_SERIES", ((InfluxQl.Ddl) l.get(2)).kind);
        assertInstanceOf(InfluxQl.Ddl.class, l.get(3));
    }

    @Test
    void bindParametersAndComments() {
        InfluxQl.Select s = (InfluxQl.Select) InfluxQl.parse("SELECT v FROM m WHERE h = $h AND v > $n -- trailing", Map.of("h", "a", "n", 3L)).get(0);
        assertInstanceOf(InfluxQl.Bin.class, s.cond);
        assertEquals("error parsing query: missing parameter: h", err("SELECT v FROM m WHERE h = $h"));
    }

    @Test
    void errorMessagesAndPositions() {
        assertEquals("error parsing query: found EOF, expected FROM at line 1, char 9", err("select *"));
        assertEquals("error parsing query: found EOF, expected identifier at line 1, char 15", err("select * from"));
        assertEquals("error parsing query: found FORM, expected FROM at line 1, char 10", err("SELECT * FORM m"));
        assertEquals("error parsing query: found selec, expected SELECT, DELETE, SHOW, CREATE, DROP, EXPLAIN, GRANT, REVOKE, ALTER, SET, KILL at line 1, char 1",
                err("selec * from m"));
        assertEquals("error parsing query: found garbage, expected ; at line 1, char 17", err("select * from m garbage"));
        assertEquals("error parsing query: found DESC, expected ; at line 1, char 35", err("select * from m order by time asc desc"));
        assertEquals("error parsing query: found a, expected regex at line 1, char 42", err("SELECT usage, host FROM cpu WHERE host =~ 'a'"));
        assertEquals("error parsing query: invalid duration", err("SELECT v FROM m WHERE time > now() - 1y"));
        assertEquals("error parsing query: fill requires an argument, e.g.: 0, null, none, previous, linear",
                err("select mean(v) from m group by time(1s) fill()"));
        assertEquals("error parsing query: only ORDER BY time supported at this time", err("select v from m order by v"));
        assertEquals("error parsing query: found LIMIT, expected ; at line 1, char 31", err("SELECT usage FROM cpu LIMIT 1 LIMIT 2"));
    }

    @Test
    void semanticErrorsAreDeferredToTheStatement() {
        List<InfluxQl.Stmt> l = InfluxQl.parse("SELECT count(v) FROM m GROUP BY time(1s, 2s, 3s)", null);
        assertEquals("time dimension expected 1 or 2 arguments", assertInstanceOf(InfluxQl.ErrStmt.class, l.get(0)).msg);
    }
}
