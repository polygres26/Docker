package com.sayonora.wire.influxwire;

import com.google.gson.JsonObject;
import com.sayonora.wire.core.BackendRegistry;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Public, in-process entry point to influxwire's storage and query translation, for the MCP
 * gateway's InfluxDB-kind tools. Uses the same {@link PgTimeSeriesStore} tables
 * ({@code warp_influx_<measurement>}: {@code time, tags jsonb, fields jsonb}) as the HTTP frontend,
 * so points written through {@code /write} are visible here and vice versa.
 */
public final class InfluxEmbedded {

    private static final Pattern SHOW_MEASUREMENTS =
            Pattern.compile("(?is)^\\s*SHOW\\s+MEASUREMENTS\\s*;?\\s*$");

    private final PgTimeSeriesStore store;

    public InfluxEmbedded(BackendRegistry registry) {
        this.store = new PgTimeSeriesStore(registry);
    }

    /** @return number of points written. */
    public int write(String lineProtocol, String precision) throws SQLException {
        List<InfluxPoint> points = LineProtocolParser.parse(lineProtocol, precision);
        store.write(points);
        return points.size();
    }

    public List<String> measurements() throws SQLException {
        return store.listMeasurements();
    }

    /** InfluxQL (the same bounded subset the /query endpoint accepts), rendered in InfluxDB v1's
     * {@code results[].series[]} response shape. */
    public JsonObject queryInfluxQl(String q) throws SQLException {
        if (SHOW_MEASUREMENTS.matcher(q).matches()) {
            return InfluxWireServer.renderShowMeasurements(store.listMeasurements());
        }
        if (q.strip().regionMatches(true, 0, "SELECT", 0, 6)) {
            InfluxQlParser.SelectStatement stmt = InfluxQlParser.parse(q);
            return InfluxWireServer.renderQueryResult(stmt.measurement(), store.select(stmt));
        }
        throw new InfluxException("influxwire only recognizes SHOW MEASUREMENTS and a bounded SELECT subset "
                + "(WHERE/GROUP BY time()/mean|sum|count|min|max) -- got: " + q);
    }

    public static String physicalTable(String measurement) {
        return PgTimeSeriesStore.pgTableName(measurement);
    }
}
