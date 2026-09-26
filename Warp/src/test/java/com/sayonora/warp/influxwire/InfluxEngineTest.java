package com.sayonora.warp.influxwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** InfluxQL semantics over an in-memory backend: results are checked against what InfluxDB 1.8 returns. */
class InfluxEngineTest {

    private static final long T0 = 1_577_836_800L; // 2020-01-01T00:00:00Z
    private MemoryInfluxBackend backend;
    private InfluxEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        backend = new MemoryInfluxBackend();
        engine = new InfluxEngine(backend, true);
        StringBuilder lp = new StringBuilder();
        for (String[] h : new String[][] {{"a", "0"}, {"b", "1"}, {"c", "2"}}) {
            for (int i = 0; i < 6; i++) {
                lp.append(String.format("cpu,host=%s usage=%s,n=%di %d000000000%n", h[0], 10 + i * 1.5 + Integer.parseInt(h[1]) * 10, i,
                        T0 + i * 10 + Integer.parseInt(h[1])));
            }
        }
        backend.write("d", null, LineProtocolParser.parse(lp.toString(), null), true);
    }

    private String json(String q) throws Exception {
        return InfluxFmt.json(engine.execute(q, "d", null, true, null), null, false);
    }

    private List<Object> col(String q, int idx) throws Exception {
        return engine.execute(q, "d", null, true, null).get(0).series.get(0).values.stream().map(r -> r[idx]).collect(Collectors.toList());
    }

    @Test
    void aggregatesAndTheirTimes() throws Exception {
        assertEquals("{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"cpu\",\"columns\":[\"time\",\"count\",\"mean\"],"
                + "\"values\":[[\"1970-01-01T00:00:00Z\",18,23.75]]}]}]}", json("SELECT count(usage), mean(usage) FROM cpu"));
        // a single selector reports the selected point's time; a lower bound becomes the time of aggregates
        assertEquals("{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"cpu\",\"columns\":[\"time\",\"max\"],"
                + "\"values\":[[\"2020-01-01T00:00:52Z\",37.5]]}]}]}", json("SELECT max(usage) FROM cpu"));
        assertEquals("2020-01-01T00:00:10Z", InfluxFmt.rfc3339Nano(((InfluxFmt.TimeV) engine.execute(
                "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:10Z'", "d", null, true, null).get(0).series.get(0).values.get(0)[0]).nanos()));
    }

    @Test
    void groupByTimeFillAndAlignment() throws Exception {
        String base = "SELECT count(usage) FROM cpu WHERE host = 'a' AND time >= '2020-01-01T00:00:00Z' AND time < '2020-01-01T00:02:00Z' GROUP BY time(30s)";
        assertEquals(List.of(3L, 3L, 0L, 0L), col(base, 1));
        assertEquals(List.of(3L, 3L), col(base + " fill(none)", 1));
        assertEquals(List.of(3L, 3L, 3L, 3L), col(base + " fill(previous)", 1));
        assertEquals(List.of(3L, 3L, 9L, 9L), col(base + " fill(9)", 1));
        // buckets align to the epoch, not to the first point
        assertEquals("2020-01-01T00:00:00Z", InfluxFmt.rfc3339Nano(((InfluxFmt.TimeV) engine.execute(
                "SELECT count(usage) FROM cpu WHERE time >= '2020-01-01T00:00:07Z' AND time < '2020-01-01T00:01:00Z' GROUP BY time(20s)",
                "d", null, true, null).get(0).series.get(0).values.get(0)[0]).nanos()));
    }

    @Test
    void transformationsAndMath() throws Exception {
        assertEquals(List.of(0.15, 0.15, 0.15, 0.15, 0.15), col("SELECT derivative(usage) FROM cpu WHERE host = 'a'", 1));
        assertEquals(List.of(1.5, 1.5), col("SELECT difference(usage) FROM cpu WHERE host = 'a' LIMIT 2", 1));
        assertEquals(List.of(10.0, 21.5), col("SELECT cumulative_sum(usage) FROM cpu WHERE host = 'a' LIMIT 2", 1));
        assertEquals(List.of(3L, 4L), col("SELECT n + 1 FROM cpu WHERE host = 'a' AND n > 1 LIMIT 2", 1));
        assertEquals(List.of(1.5, 2.0), col("SELECT n / 2 FROM cpu WHERE host = 'a' AND n > 2 LIMIT 2", 1));
    }

    @Test
    void selectStarColumnsAndTagFilters() throws Exception {
        InfluxFmt.Series s = engine.execute("SELECT * FROM cpu WHERE host =~ /^[ab]$/ AND host != 'b' LIMIT 1", "d", null, true, null).get(0).series.get(0);
        assertEquals(List.of("time", "host", "n", "usage"), s.columns);
        assertEquals(1, s.values.size());
        // OFFSET without LIMIT returns exactly one row (an InfluxDB quirk we reproduce)
        assertEquals(1, engine.execute("SELECT usage FROM cpu WHERE host = 'a' OFFSET 2", "d", null, true, null).get(0).series.get(0).values.size());
    }

    @Test
    void subqueriesShowStatementsAndDdl() throws Exception {
        assertEquals(List.of(33.75), col("SELECT max(m) FROM (SELECT mean(usage) AS m FROM cpu GROUP BY host)", 1));
        assertEquals(List.of("cpu"), col("SHOW MEASUREMENTS", 0));
        assertEquals(List.of("host"), col("SHOW TAG KEYS", 0));
        engine.execute("DELETE FROM cpu WHERE host = 'a'", "d", null, true, null);
        assertEquals(List.of("b", "c"), col("SHOW TAG VALUES WITH KEY = host", 1));
        engine.execute("DROP MEASUREMENT cpu", "d", null, true, null);
        assertTrue(engine.execute("SHOW MEASUREMENTS", "d", null, true, null).get(0).series.isEmpty());
        engine.execute("CREATE DATABASE other", null, null, true, null);
        assertTrue(backend.databaseExists("other"));
    }

    @Test
    void statementErrorsAreReportedPerStatement() throws Exception {
        List<InfluxFmt.StmtResult> r = engine.execute("SELECT mean() FROM cpu; SELECT usage, mean(usage) FROM cpu; SELECT usage FROM cpu WHERE host = 'a' LIMIT 1", "d", null, true, null);
        assertEquals("invalid number of arguments for mean, expected 1, got 0", r.get(0).error);
        assertEquals("mixing aggregate and non-aggregate queries is not supported", r.get(1).error);
        assertNull(r.get(2).error);
        assertEquals("database not found: nope", engine.execute("SELECT * FROM cpu", "nope", null, true, null).get(0).error);
        assertEquals("database name required", engine.execute("SELECT * FROM cpu", null, null, true, null).get(0).error);
    }

    @Test
    void jsonNumbersFollowGo() {
        assertEquals("1e+21", InfluxFmt.jsonFloat(1e21));
        assertEquals("1.5e-7", InfluxFmt.jsonFloat(1.5e-7));
        assertEquals("100", InfluxFmt.jsonFloat(100.0));
        assertEquals("0.000001", InfluxFmt.jsonFloat(1e-6));
        assertEquals("1.7976931348623157e+308", InfluxFmt.jsonFloat(Double.MAX_VALUE));
        assertEquals("2020-01-01T00:00:00.5Z", InfluxFmt.rfc3339Nano(T0 * 1_000_000_000L + 500_000_000L));
        assertEquals("1h0m0s", InfluxFmt.goDuration(3_600_000_000_000L));
    }
}
