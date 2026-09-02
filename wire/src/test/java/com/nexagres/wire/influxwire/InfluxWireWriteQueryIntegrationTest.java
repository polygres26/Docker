package com.nexagres.wire.influxwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.nexagres.wire.testsupport.RealPostgres;
import com.nexagres.wire.testsupport.WarpProcess;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Test;

/**
 * There is no existing test for influxwire at all -- the source (InfluxWireServer, InfluxQlParser,
 * PgTimeSeriesStore) has zero coverage in this project's test suite. This is the first: a real
 * {@code influxdb-java} client (the real InfluxDB v1 SDK, real line-protocol writes and real
 * InfluxQL over HTTP) writing points and reading them back via a SELECT with an aggregate,
 * against a real Postgres backend -- no mocks.
 */
class InfluxWireWriteQueryIntegrationTest {

    @Test
    void writtenPointsAreQueryableWithAnAggregate() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("influxwire", "WARP_INFLUXWIRE_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:" + warp.port("influxwire"));
            try {
                // No createDatabase() call -- InfluxWireServer's own javadoc documents only
                // SHOW MEASUREMENTS and a bounded SELECT subset, not CREATE DATABASE; the "db"
                // query param on /write is presumably just a namespace label, not something that
                // needs a separate real-InfluxDB-style provisioning call first.
                String db = "metrics_db";
                influxDB.setDatabase(db);

                influxDB.write(Point.measurement("cpu_load")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("host", "server-a")
                        .addField("value", 42.0)
                        .build());
                influxDB.write(Point.measurement("cpu_load")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("host", "server-a")
                        .addField("value", 58.0)
                        .build());

                QueryResult result = influxDB.query(new Query("SELECT mean(value) FROM cpu_load", db));
                assertFalse(result.hasError(), "expected no InfluxQL error -- got: " + result.getError());
                QueryResult.Series series = result.getResults().get(0).getSeries().get(0);
                java.util.List<Object> row = series.getValues().get(0);
                // Column layout for a non-GROUP-BY-time aggregate: just [value], no leading time
                // column (that only appears with GROUP BY time()) -- confirmed live rather than
                // assumed, since the first attempt here assumed [time, value] and got an
                // IndexOutOfBoundsException on a real, length-1 row.
                double mean = (double) row.get(row.size() - 1);
                assertEquals(50.0, mean, 0.001, "mean(42.0, 58.0) must be 50.0 -- a real aggregate over the real written points");
            } finally {
                influxDB.close();
            }
        }
    }
}
