package com.sayonora.wire.influxwire;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.core.BackendRegistry;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Public, in-process entry point to influxwire's storage and InfluxQL engine, for the MCP gateway's
 * InfluxDB-kind tools. Uses the same {@link PgTimeSeriesStore} tables and {@link InfluxEngine} as the HTTP
 * frontend, so points written through {@code /write} are visible here and vice versa. Unknown databases are
 * created on first write (the lenient default of the HTTP frontend).
 */
public final class InfluxEmbedded {

    public static final String DEFAULT_DB = "default";

    private final PgTimeSeriesStore store;
    private final InfluxEngine engine;

    public InfluxEmbedded(BackendRegistry registry) {
        this.store = new PgTimeSeriesStore(registry);
        this.engine = new InfluxEngine(store, true);
    }

    private static String dbOrDefault(String db) {
        return db == null || db.isBlank() ? DEFAULT_DB : db;
    }

    /** @return number of points written. */
    public int write(String db, String lineProtocol, String precision) throws SQLException {
        String p = precision == null || precision.isBlank() ? null : precision.equals("us") ? "u" : precision;
        if (!LineProtocolParser.validPrecision(p)) {
            throw new InfluxException("invalid precision \"" + precision + "\" (use n, u, ms, s, m or h)");
        }
        LineProtocolParser.Parsed parsed = LineProtocolParser.parseLenient(lineProtocol, p);
        if (!parsed.errors().isEmpty() && parsed.points().isEmpty()) {
            throw new InfluxException(String.join("\n", parsed.errors()));
        }
        InfluxBackend.WriteOutcome o = store.write(dbOrDefault(db), null, parsed.points(), true);
        if (!parsed.errors().isEmpty() || o.dropped() > 0) {
            throw new InfluxException("partial write: " + String.join("\n", parsed.errors())
                    + (o.conflictMessage() == null ? "" : o.conflictMessage()) + " dropped=" + o.dropped());
        }
        return parsed.points().size();
    }

    /** Every measurement name on any database. */
    public List<String> measurements() throws SQLException {
        return store.listMeasurements();
    }

    public List<String> measurements(String db) throws SQLException {
        String d = dbOrDefault(db);
        return store.databaseExists(d) ? store.measurements(d, store.defaultRp(d)) : List.of();
    }

    public List<String> databases() throws SQLException {
        return store.databases();
    }

    /** Field name to InfluxDB type (float/integer/string/boolean) of a measurement. */
    public Map<String, String> fieldTypes(String db, String measurement) throws SQLException {
        String d = dbOrDefault(db);
        return store.schema(d, store.defaultRp(d), measurement).fields;
    }

    public java.util.Set<String> tagKeys(String db, String measurement) throws SQLException {
        String d = dbOrDefault(db);
        return store.schema(d, store.defaultRp(d), measurement).tagKeys;
    }

    /** InfluxQL (the full engine the /query endpoint runs), rendered in InfluxDB v1's {@code results[].series[]} shape. */
    public JsonObject queryInfluxQl(String db, String q) throws SQLException {
        try {
            List<InfluxFmt.StmtResult> r = engine.execute(q, dbOrDefault(db), null, false, null);
            return JsonParser.parseString(InfluxFmt.json(r, null, false)).getAsJsonObject();
        } catch (InfluxQl.ParseError e) {
            throw new InfluxException(e.getMessage());
        }
    }

    public static String physicalTable(String measurement) {
        return PgTimeSeriesStore.pgTableName(measurement);
    }
}
