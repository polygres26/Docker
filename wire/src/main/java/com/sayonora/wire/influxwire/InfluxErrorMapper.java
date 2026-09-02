package com.sayonora.wire.influxwire;

import java.util.Map;

/** Translates a real Postgres backend error (SQLSTATE) into an HTTP status real InfluxDB clients
 * expect from {@code /write}/{@code /query} -- the influxwire counterpart to
 * {@code OpenSearchErrorMapper}/{@code DynamoDbErrorMapper}/{@code SqsErrorMapper}. Real InfluxDB's
 * own error responses are a flat {@code {"error": "message"}} JSON body (no OpenSearch-style
 * {@code root_cause} nesting, no DynamoDB-style {@code __type} field) -- see
 * {@code InfluxWireServer#writeError} for where this feeds that shape. */
public final class InfluxErrorMapper {

    public static final int DEFAULT_STATUS = 500;

    private static final Map<String, Integer> TABLE = Map.ofEntries(
            // undefined_table -- the measurement's own backing table is gone (dropped directly
            // against Postgres, a real outage mid-migration, etc.).
            Map.entry("42P01", 404),
            // insufficient_privilege.
            Map.entry("42501", 403),
            // Invalid input of various shapes -- a field value that doesn't fit the column type a
            // PREVIOUS write already fixed for this measurement (see PgTimeSeriesStore's javadoc
            // on why field type is fixed per measurement, not per point).
            Map.entry("22P02", 400),
            Map.entry("22003", 400),
            Map.entry("23502", 400),
            // admin_shutdown / connection_failure / too_many_connections / unable to establish a
            // new connection -- the backend is genuinely unreachable.
            Map.entry("57P01", 503),
            Map.entry("08006", 503),
            Map.entry("53300", 503),
            Map.entry("08001", 503));

    public static int status(String sqlState) {
        Integer s = sqlState == null ? null : TABLE.get(sqlState);
        return s == null ? DEFAULT_STATUS : s;
    }

    private InfluxErrorMapper() {
    }
}
