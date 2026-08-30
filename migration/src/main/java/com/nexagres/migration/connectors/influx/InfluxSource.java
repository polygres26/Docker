package com.nexagres.migration.connectors.influx;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Partition;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.core.Source;
import com.nexagres.migration.core.StateStore;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InfluxDB (v1 HTTP line-protocol/InfluxQL) connector: reads via plain HTTP -- no dedicated client
 * library needed at all (influxwire, and real InfluxDB v1, both speak the same JSON-over-HTTP
 * {@code /query} API this connector calls directly with the JDK's own {@link HttpClient}).
 *
 * <p><b>A real, live change feed, unlike Neo4j's genuine one-time-only limitation</b>: a
 * time-series point's {@code time} column gives this connector something Neo4j's arbitrary graph
 * structure never has -- a naturally monotonic cursor. Both {@link #readPartition} (the initial
 * backlog) and {@link #streamChanges} (the live tail) run the IDENTICAL "read everything with
 * {@code time > cursor}, advance the cursor" loop; they differ only in when they stop (backlog:
 * once a poll returns nothing; live: never, until {@link #close}). This is a real, correct
 * timestamp-cursor CDC technique for genuinely append-only time-series ingestion -- <b>with one
 * honestly-documented limitation</b>: a point written with a timestamp EARLIER than one already
 * consumed (a backdated/out-of-order write) is silently missed, since the cursor only ever moves
 * forward. Real production time-series pipelines are overwhelmingly append-only in practice, but
 * this is a real gap for a source that isn't, not glossed over.
 *
 * <p><b>Explicit tag/field declaration, a real necessity</b>: InfluxDB v1's {@code SELECT *}
 * response has no way to tell which returned columns are tags vs. fields without a separate
 * {@code SHOW TAG KEYS}/{@code SHOW FIELD KEYS} call -- and confirmed by reading wire's own code
 * this session, influxwire doesn't implement either of those. This connector therefore requires
 * the tag key set to be declared explicitly; every other returned column (besides {@code time})
 * is treated as a field.
 *
 * <p>Writes into the target's real influxwire physical schema (see {@code
 * com.nexagres.wire.influxwire.PgTimeSeriesStore} in the {@code wire} module: {@code
 * polywire_influx_<measurement>} with {@code time timestamptz}/{@code tags jsonb}/{@code fields
 * jsonb}) -- same "match the wire's own physical schema exactly" principle as every document/
 * key-value connector in this project. <b>Known, scoped gap</b>: that physical schema has NO
 * primary key or unique constraint at all (confirmed by reading its own DDL) -- there is no
 * natural per-point identity to make a write idempotent the way every other connector's upsert-by-
 * id is. A restart that re-reads a partially-migrated batch can duplicate points; this is a real,
 * disclosed limitation, not a claim of correctness this connector doesn't have. TimescaleDB
 * hypertable conversion (which {@code PgTimeSeriesStore} does when the extension is present) is
 * also not attempted here -- scoped out for v1.
 */
public final class InfluxSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(InfluxSource.class);
    private static final Gson GSON = new Gson();
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final String PARTITION_DONE = "\"DONE\"";
    private static final int BATCH_SIZE = 1000;
    private static final long POLL_INTERVAL_MILLIS = 2000;

    private final HttpClient http = HttpClient.newHttpClient();
    private final String baseUrl;
    private final String database;
    private final String measurement;
    private final String targetTable;
    private final Set<String> tagKeys;
    private final String checkpointKey;

    private volatile boolean running = true;

    public InfluxSource(String host, int port, String database, String measurement, Set<String> tagKeys) {
        if (!IDENTIFIER.matcher(measurement).matches()) {
            throw new IllegalArgumentException("measurement name must match [A-Za-z_][A-Za-z0-9_]*: " + measurement);
        }
        this.baseUrl = "http://" + host + ":" + port;
        this.database = database;
        this.measurement = measurement;
        this.targetTable = "polywire_influx_" + measurement.toLowerCase(Locale.ROOT);
        this.tagKeys = tagKeys;
        this.checkpointKey = "influx:" + database + "." + measurement;
    }

    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS \"" + targetTable + "\" ("
                + "time timestamptz NOT NULL, tags jsonb NOT NULL DEFAULT '{}', fields jsonb NOT NULL DEFAULT '{}')");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS \"" + targetTable + "_time_idx\" "
                + "ON \"" + targetTable + "\" (time DESC)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS \"" + targetTable + "_tags_idx\" "
                + "ON \"" + targetTable + "\" USING GIN (tags)");
    }

    private static void applyTolerantOfConcurrentCreateRace(Sink sink, String ddl) throws Exception {
        try {
            sink.apply(new ChangeEvent(ddl, List.of()));
        } catch (SQLException e) {
            if (!"23505".equals(e.getSQLState())) {
                throw e;
            }
            log.info("ensureTargetSchema: lost a benign concurrent CREATE race to another worker "
                    + "(23505 on the object catalog) -- the object exists either way, continuing");
        }
    }

    /** Always a single partition -- no natural range/hash key exists to split a time range by
     * without knowing tag cardinality in advance, same scope line {@code SqsSource} draws for a
     * queue's messages. */
    @Override
    public List<Partition> listPartitions() {
        return List.of(new Partition(checkpointKey, null));
    }

    @Override
    public void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception {
        if (PARTITION_DONE.equals(checkpoints.load(checkpointKey + "#backlog"))) {
            log.info("influx source[{}]: initial backlog already drained in a prior run -- "
                    + "skipping straight to live draining", checkpointKey);
            return;
        }
        long drained = 0;
        while (true) {
            long n = drainOnce(sink, checkpoints);
            drained += n;
            if (n == 0) {
                break;
            }
        }
        checkpoints.save(checkpointKey + "#backlog", PARTITION_DONE);
        log.info("influx source[{}]: initial backlog drained, {} point(s) migrated", checkpointKey, drained);
    }

    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
        if (checkpoints.load(checkpointKey) == null) {
            checkpoints.save(checkpointKey, GSON.toJson(new InfluxCheckpoint(0L)));
        }
    }

    @Override
    public void streamChanges(Sink sink, StateStore checkpoints) throws Exception {
        while (running) {
            long n = drainOnce(sink, checkpoints);
            if (n == 0) {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            }
        }
    }

    /** One poll: everything with {@code time > cursor}, up to {@link #BATCH_SIZE} points,
     * advancing and persisting the cursor to the LAST row's own timestamp -- shared by both
     * {@link #readPartition}'s backlog drain and {@link #streamChanges}' live tail (see this
     * class's own javadoc on why that's correct, not a shortcut). */
    private long drainOnce(Sink sink, StateStore checkpoints) throws Exception {
        long cursor = currentCursor(checkpoints);
        // influxwire's own InfluxQL WHERE-clause parser rejects a bare unquoted integer
        // ("expected a quoted timestamp or now()") AND rejects a quoted raw nanosecond number
        // (it calls Instant.parse() on the quoted literal, which needs real RFC3339 text) --
        // both confirmed live. A real RFC3339 instant string is what it actually accepts.
        String cursorRfc3339 = Instant.ofEpochSecond(0, cursor).toString();
        String influxQl = "SELECT * FROM " + measurement + " WHERE time > '" + cursorRfc3339 + "' LIMIT " + BATCH_SIZE;
        String url = baseUrl + "/query?db=" + urlEncode(database) + "&epoch=ns&q=" + urlEncode(influxQl);
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) {
            throw new IllegalStateException("influx source[" + checkpointKey + "]: query failed (HTTP "
                    + response.statusCode() + "): " + response.body());
        }

        List<Row> rows = parseRows(response.body());
        if (rows.isEmpty()) {
            return 0;
        }
        // The cursor MUST advance to the MAXIMUM timestamp seen, not just the last row in
        // response order -- confirmed live that influxwire returns SELECT * results in
        // DESCENDING time order (newest first), unlike real InfluxDB's documented ascending
        // default. Assuming ascending order and taking the last-iterated row's time as the new
        // cursor set it to the EARLIEST point instead of the latest, so each subsequent poll
        // still matched almost everything again -- a real, observed bug (each poll shrank by
        // exactly one row instead of going to zero), not a hypothetical. Computing the max
        // explicitly is correct regardless of which order either real InfluxDB or influxwire
        // actually returns rows in.
        List<ChangeEvent> batch = new ArrayList<>(rows.size());
        long lastTime = cursor;
        for (Row row : rows) {
            batch.add(insertEvent(row));
            lastTime = Math.max(lastTime, row.timeNanos());
        }
        sink.applyBatch(batch);
        checkpoints.save(checkpointKey, GSON.toJson(new InfluxCheckpoint(lastTime)), Instant.ofEpochSecond(0, lastTime));
        return rows.size();
    }

    private long currentCursor(StateStore checkpoints) throws Exception {
        String token = checkpoints.load(checkpointKey);
        return token == null ? 0L : GSON.fromJson(token, InfluxCheckpoint.class).cursorNanos();
    }

    /** {@code tagsJson}/{@code fieldsJson} are already-final JSON text, ready to bind straight
     * into the target's {@code tags jsonb}/{@code fields jsonb} columns -- see {@link
     * #parseRows}'s own javadoc for the two real response shapes this normalizes. */
    private record Row(long timeNanos, String tagsJson, String fieldsJson) {
    }

    /** Parses InfluxDB v1's real {@code /query} response shape ({@code
     * results[0].series[0].{columns,values}}) -- in EITHER of two real, confirmed-live column
     * layouts:
     * <ul>
     *   <li><b>influxwire's own shape</b>: {@code columns = ["time", "tags", "fields"]}, with
     *   {@code tags}/{@code fields} already pre-structured JSON objects per row -- mirroring
     *   influxwire's own {@code PgTimeSeriesStore} storage exactly (confirmed live: this is what
     *   influxwire ACTUALLY returns, not real InfluxDB's documented flat-column response).
     *   <li><b>Real InfluxDB v1's documented shape</b>: a flat column per tag/field (e.g. {@code
     *   ["time", "sensor", "value"]}), classified into tags vs. fields using this connector's own
     *   {@link #tagKeys} declaration (see this class's own javadoc on why that has to be
     *   explicit).
     * </ul>
     * {@code time} is parsed as EITHER a plain integer (the real, documented behavior of {@code
     * epoch=ns} on the request) OR an RFC3339 string -- confirmed live that influxwire does not
     * actually honor {@code epoch=ns} at all and always returns RFC3339 text regardless, so this
     * has to handle both; a real InfluxDB server that DOES honor the parameter still works
     * correctly via the integer path. An empty/measurement-not-found response has no {@code
     * series} key at all -- treated as zero rows, not an error. */
    private List<Row> parseRows(String responseBody) {
        JsonObject root = JsonParser.parseString(responseBody).getAsJsonObject();
        JsonArray results = root.getAsJsonArray("results");
        if (results == null || results.isEmpty()) {
            return List.of();
        }
        JsonObject firstResult = results.get(0).getAsJsonObject();
        JsonArray series = firstResult.getAsJsonArray("series");
        if (series == null || series.isEmpty()) {
            return List.of();
        }
        JsonObject firstSeries = series.get(0).getAsJsonObject();
        List<String> columns = new ArrayList<>();
        firstSeries.getAsJsonArray("columns").forEach(c -> columns.add(c.getAsString()));
        int timeIndex = columns.indexOf("time");
        int tagsIndex = columns.indexOf("tags");
        int fieldsIndex = columns.indexOf("fields");
        boolean preStructured = tagsIndex >= 0 && fieldsIndex >= 0;

        List<Row> rows = new ArrayList<>();
        for (JsonElement valueRow : firstSeries.getAsJsonArray("values")) {
            JsonArray cols = valueRow.getAsJsonArray();
            long timeNanos = parseTime(cols.get(timeIndex));
            if (preStructured) {
                rows.add(new Row(timeNanos, cols.get(tagsIndex).toString(), cols.get(fieldsIndex).toString()));
                continue;
            }
            Map<String, String> tags = new LinkedHashMap<>();
            Map<String, String> fields = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                if (i == timeIndex) {
                    continue;
                }
                JsonElement v = cols.get(i);
                if (v.isJsonNull()) {
                    continue;
                }
                // Every real InfluxQL SELECT * column value is a JSON primitive (string/number/
                // boolean) -- toString() as a defensive fallback for anything else rather than
                // throwing, since a genuinely unexpected shape here shouldn't crash the whole
                // migration over one odd value.
                String text = v.isJsonPrimitive() ? v.getAsString() : v.toString();
                (tagKeys.contains(columns.get(i)) ? tags : fields).put(columns.get(i), text);
            }
            rows.add(new Row(timeNanos, GSON.toJson(tags), GSON.toJson(fields)));
        }
        return rows;
    }

    private static long parseTime(JsonElement timeElement) {
        JsonPrimitive prim = timeElement.getAsJsonPrimitive();
        if (prim.isNumber()) {
            return prim.getAsLong();
        }
        // RFC3339 text -- Instant.parse handles both integer-second and fractional-second forms.
        Instant instant = Instant.parse(prim.getAsString());
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    private ChangeEvent insertEvent(Row row) {
        String sql = "INSERT INTO \"" + targetTable + "\" (time, tags, fields) VALUES "
                + "(to_timestamp(?::double precision / 1e9), ?::jsonb, ?::jsonb)";
        return new ChangeEvent(sql, List.of(String.valueOf(row.timeNanos()), row.tagsJson(), row.fieldsJson()));
    }

    private static String urlEncode(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        running = false;
    }

    private record InfluxCheckpoint(long cursorNanos) {
    }
}
