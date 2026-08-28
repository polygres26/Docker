package com.nexagres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Test;

/** influxwire: real InfluxDB v1 line-protocol write path plus a bounded InfluxQL read path
 * (WHERE/GROUP BY time()/aggregates), the real official {@code influxdb-java} v1 client (not the
 * v2 client -- see com.nexagres.wire.influxwire.InfluxWireServer's own javadoc for why this speaks
 * v1, not Flux), translated to Postgres underneath. No auth configured in the test compose file. */
class InfluxWireTest {

    private static InfluxDB client() {
        InfluxDB db = InfluxDBFactory.connect("http://" + TestConfig.HOST + ":" + TestConfig.INFLUXWIRE_PORT);
        db.setDatabase("testdb");
        return db;
    }

    private static String measurement() {
        // Must match influxwire's own identifier rule ([A-Za-z_][A-Za-z0-9_]*) -- see
        // PgTimeSeriesStore.pgTableName.
        return "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    @Test
    void writeAndSelectStarRoundTrip() {
        InfluxDB c = client();
        String m = measurement();
        c.write(Point.measurement(m).tag("host", "server01").addField("usage", 72.5)
                .time(1_700_000_000_000L, TimeUnit.MILLISECONDS).build());
        c.write(Point.measurement(m).tag("host", "server02").addField("usage", 15.0)
                .time(1_700_000_060_000L, TimeUnit.MILLISECONDS).build());

        QueryResult result = c.query(new Query("SELECT * FROM " + m));
        List<List<Object>> rows = firstSeriesValues(result);
        assertEquals(2, rows.size());
    }

    @Test
    void whereFiltersByTag() {
        InfluxDB c = client();
        String m = measurement();
        c.write(Point.measurement(m).tag("region", "us").addField("value", 1.0).build());
        c.write(Point.measurement(m).tag("region", "eu").addField("value", 2.0).build());

        QueryResult result = c.query(new Query("SELECT value FROM " + m + " WHERE region = 'us'"));
        List<List<Object>> rows = firstSeriesValues(result);
        assertEquals(1, rows.size());
        assertEquals(1.0, (Double) rows.get(0).get(1));
    }

    @Test
    void meanAggregateWithoutGroupBy() {
        InfluxDB c = client();
        String m = measurement();
        c.write(Point.measurement(m).addField("usage", 10.0).build());
        c.write(Point.measurement(m).addField("usage", 20.0).build());
        c.write(Point.measurement(m).addField("usage", 30.0).build());

        QueryResult result = c.query(new Query("SELECT mean(usage) FROM " + m));
        List<List<Object>> rows = firstSeriesValues(result);
        assertEquals(1, rows.size());
        // No GROUP BY here, so this row is just [mean_usage] -- no leading time column (see
        // PgTimeSeriesStore#select's javadoc: a time bucket column only appears when there's an
        // actual GROUP BY time() to bucket by).
        assertEquals(20.0, (Double) rows.get(0).get(0));
    }

    @Test
    void groupByTimeBucketsAndAggregatesPerBucket() {
        InfluxDB c = client();
        String m = measurement();
        // Real, clean UTC minute boundaries (date_bin's own 1-minute buckets align exactly to the
        // wall-clock minute, anchored at the Unix epoch) -- :10 and :50 land in the same
        // [00:00:00, 00:01:00) bucket, :01:10 lands in the next one.
        c.write(Point.measurement(m).addField("usage", 10.0)
                .time(java.time.Instant.parse("2024-01-01T00:00:10Z").toEpochMilli(), TimeUnit.MILLISECONDS).build());
        c.write(Point.measurement(m).addField("usage", 30.0)
                .time(java.time.Instant.parse("2024-01-01T00:00:50Z").toEpochMilli(), TimeUnit.MILLISECONDS).build());
        c.write(Point.measurement(m).addField("usage", 100.0)
                .time(java.time.Instant.parse("2024-01-01T00:01:10Z").toEpochMilli(), TimeUnit.MILLISECONDS).build());

        QueryResult result = c.query(new Query("SELECT mean(usage) FROM " + m + " GROUP BY time(1m)"));
        List<List<Object>> rows = firstSeriesValues(result);
        assertEquals(2, rows.size());
        assertEquals(20.0, (Double) rows.get(0).get(1));
        assertEquals(100.0, (Double) rows.get(1).get(1));
    }

    @Test
    void showMeasurementsListsWrittenTables() {
        InfluxDB c = client();
        String m = measurement();
        c.write(Point.measurement(m).addField("value", 1.0).build());

        QueryResult result = c.query(new Query("SHOW MEASUREMENTS"));
        List<List<Object>> rows = firstSeriesValues(result);
        boolean found = rows.stream().anyMatch(r -> r.get(0).equals(m));
        assertTrue(found, "expected \"" + m + "\" in SHOW MEASUREMENTS");
    }

    @Test
    void unsupportedOrReturnsAClearErrorNotAWrongAnswer() {
        InfluxDB c = client();
        String m = measurement();
        c.write(Point.measurement(m).tag("host", "a").addField("value", 1.0).build());

        // influxdb-java throws InfluxDBException for a non-2xx /query response (confirmed live --
        // an earlier version of this test assumed result.hasError() instead, which is only for a
        // 2xx response whose JSON body itself carries a per-statement error). influxwire's own
        // error message names the actual unsupported clause, matching InfluxQlParser's
        // "unrecognized clause fails loudly" policy.
        try {
            c.query(new Query("SELECT value FROM " + m + " WHERE host = 'a' OR host = 'b'"));
            fail("expected influxwire to reject OR, not silently accept it");
        } catch (org.influxdb.InfluxDBException e) {
            assertTrue(e.getMessage().contains("OR"));
        }
    }

    @Test
    void pingReportsAVersion() {
        InfluxDB c = client();
        assertTrue(c.version() != null && !c.version().isBlank());
    }

    private static List<List<Object>> firstSeriesValues(QueryResult result) {
        if (result.hasError()) {
            fail("influxwire query error: " + result.getError());
        }
        return result.getResults().get(0).getSeries().get(0).getValues();
    }
}
