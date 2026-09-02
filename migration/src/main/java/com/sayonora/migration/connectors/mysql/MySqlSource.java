package com.sayonora.migration.connectors.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.gson.Gson;
import com.sayonora.migration.core.ChangeEvent;
import com.sayonora.migration.core.Partition;
import com.sayonora.migration.core.Sink;
import com.sayonora.migration.core.Source;
import com.sayonora.migration.core.StateStore;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL connector: real parallel range-partitioned reads for the initial bulk read, a real MySQL
 * binlog (ROW format) client for live sync, writing into a target Postgres table whose schema is
 * TRANSLATED from the source's real {@code information_schema} (see {@link MySqlTypeMapping}) --
 * unlike Mongo/DynamoDB, MySQL has no fixed "physical wire-protocol schema" to match (mywire proxies
 * straight through to a real Postgres backend rather than storing a JSON blob, confirmed by reading
 * wire's own code this session), so this connector's job includes real schema translation, not just
 * data movement into an already-known shape.
 *
 * <p><b>Known, scoped assumptions</b> (documented, not silently guessed):
 * <ul>
 *   <li>Single-column primary key -- same scope line every connector in this project draws (Mongo's
 *   shard key, DynamoDB's HASH/RANGE keys are each at most one attribute too).
 *   <li>{@code binlog_row_image = FULL} (MySQL's own default) -- a row event carries every column's
 *   value, not just the changed ones.
 *   <li>The source table's column set doesn't change after migration starts -- column order from
 *   {@link #loadSchema} is assumed stable for the lifetime of a run.
 *   <li>String/text column bytes from the binlog are decoded as UTF-8 always, not per-column
 *   charset/collation -- correct for the overwhelmingly common case, not guaranteed for a table
 *   deliberately mixing charsets column-to-column.
 * </ul>
 *
 * <p><b>NULL handling, a real correctness fix, not a workaround</b>: {@code
 * QueryService.Execute}'s {@code repeated string params} has no null-value marker (confirmed by
 * reading {@code wire/src/main/proto/warp.proto} directly -- every bind param is plain text).
 * Rather than accept "an UPDATE that sets a column to NULL never replicates" as a scoped gap, this
 * connector builds each row's SQL text with the literal keyword {@code NULL} inlined directly for
 * any null column (never as a bind param) and a real {@code ?::type} placeholder for every other
 * column -- correct for INSERT and UPDATE alike, at the cost of the dialect-translation cache
 * seeing more distinct SQL texts (a cache-hit-rate cost, not a correctness one) when null patterns
 * vary row to row.
 */
public final class MySqlSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(MySqlSource.class);
    private static final Gson GSON = new Gson();
    private static final String PARTITION_DONE = "\"DONE\"";
    private static final int SNAPSHOT_BATCH_SIZE = 500;

    private record ColumnInfo(String name, String postgresType) {
    }

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String sourceDatabase;
    private final String sourceTable;
    private final int partitionCount;
    private final long binlogServerId;
    private final String checkpointKey;

    private volatile List<ColumnInfo> columns;
    private volatile String primaryKeyColumn;
    private volatile BinaryLogClient activeClient;
    private volatile boolean running = true;

    public MySqlSource(String host, int port, String user, String password, String sourceDatabase, String sourceTable) {
        this(host, port, user, password, sourceDatabase, sourceTable, 1, 6000_000_000L + (sourceDatabase + sourceTable).hashCode() % 1_000_000);
    }

    /** @param binlogServerId the server id this connector presents to the source MySQL server when
     *     requesting the binlog stream -- MUST be unique among every replica/consumer connected to
     *     that server (a collision causes the source to kill one of the connections). The default
     *     single-arg constructor derives one deterministically from the table name, good enough for
     *     one migration at a time; running several concurrent migrations against the SAME source
     *     server needs distinct values passed explicitly here. */
    public MySqlSource(String host, int port, String user, String password, String sourceDatabase, String sourceTable,
            int partitionCount, long binlogServerId) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.sourceDatabase = sourceDatabase;
        this.sourceTable = sourceTable;
        this.partitionCount = Math.max(1, partitionCount);
        this.binlogServerId = binlogServerId;
        this.checkpointKey = "mysql:" + sourceDatabase + "." + sourceTable;
    }

    private String sourceJdbcUrl() {
        return "jdbc:mysql://" + host + ":" + port + "/" + sourceDatabase
                + "?useSSL=false&allowPublicKeyRetrieval=true&connectionTimeZone=SERVER";
    }

    private Connection openSource() throws SQLException {
        return DriverManager.getConnection(sourceJdbcUrl(), user, password);
    }

    /** Reads the source table's REAL columns (name + translated type, in ordinal order -- the
     * binlog's own row-value arrays are positional, matching this exact order) and its real
     * single-column primary key from {@code information_schema}, once, cached on this instance. */
    private synchronized void loadSchema() throws SQLException {
        if (columns != null) {
            return;
        }
        List<ColumnInfo> loaded = new ArrayList<>();
        try (Connection conn = openSource();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT column_name, data_type FROM information_schema.columns "
                                + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, sourceDatabase);
            ps.setString(2, sourceTable);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    loaded.add(new ColumnInfo(rs.getString(1), MySqlTypeMapping.toPostgresType(rs.getString(2))));
                }
            }
        }
        if (loaded.isEmpty()) {
            throw new IllegalStateException("MySQL table " + sourceDatabase + "." + sourceTable + " has no columns "
                    + "(or doesn't exist) -- nothing to migrate");
        }
        String pk = null;
        try (Connection conn = openSource();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT k.column_name FROM information_schema.table_constraints t "
                                + "JOIN information_schema.key_column_usage k "
                                + "ON t.constraint_name = k.constraint_name AND t.table_schema = k.table_schema "
                                + "AND t.table_name = k.table_name "
                                + "WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema = ? AND t.table_name = ? "
                                + "ORDER BY k.ordinal_position LIMIT 1")) {
            ps.setString(1, sourceDatabase);
            ps.setString(2, sourceTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    pk = rs.getString(1);
                }
            }
        }
        if (pk == null) {
            throw new IllegalStateException("MySQL table " + sourceDatabase + "." + sourceTable
                    + " has no primary key -- required for partitioning, upserts, and delete replication");
        }
        this.primaryKeyColumn = pk;
        this.columns = loaded;
    }

    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        loadSchema();
        applyTolerantOfConcurrentCreateRace(sink, "CREATE SCHEMA IF NOT EXISTS \"" + sourceDatabase + "\"");
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS " + qualifiedTable() + " (");
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo c = columns.get(i);
            if (i > 0) {
                ddl.append(", ");
            }
            ddl.append('"').append(c.name()).append("\" ").append(c.postgresType());
        }
        ddl.append(", PRIMARY KEY (\"").append(primaryKeyColumn).append("\"))");
        applyTolerantOfConcurrentCreateRace(sink, ddl.toString());
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

    @Override
    public List<Partition> listPartitions() {
        List<Partition> partitions = new ArrayList<>(partitionCount);
        for (int bucket = 0; bucket < partitionCount; bucket++) {
            partitions.add(new Partition(checkpointKey + "#p" + bucket, bucket));
        }
        return partitions;
    }

    @Override
    public void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception {
        String partitionCheckpointKey = partition.id();
        if (PARTITION_DONE.equals(checkpoints.load(partitionCheckpointKey))) {
            log.info("mysql source[{}]: partition already fully copied -- skipping", partitionCheckpointKey);
            return;
        }
        loadSchema();
        int bucket = (Integer) partition.descriptor();

        String columnList = columns.stream().map(c -> "`" + c.name() + "`").reduce((a, b) -> a + ", " + b).orElseThrow();
        String sql = "SELECT " + columnList + " FROM `" + sourceDatabase + "`.`" + sourceTable + "` "
                + (partitionCount > 1 ? "WHERE MOD(CRC32(CAST(`" + primaryKeyColumn + "` AS CHAR)), " + partitionCount + ") = " + bucket : "");

        long copied = 0;
        List<ChangeEvent> batch = new ArrayList<>(SNAPSHOT_BATCH_SIZE);
        try (Connection conn = openSource();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (ColumnInfo c : columns) {
                    row.put(c.name(), MySqlTypeMapping.isBinary(c.postgresType()) ? rs.getBytes(c.name()) : rs.getObject(c.name()));
                }
                batch.add(upsertEvent(row));
                if (batch.size() >= SNAPSHOT_BATCH_SIZE) {
                    sink.applyBatch(batch);
                    copied += batch.size();
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            sink.applyBatch(batch);
            copied += batch.size();
        }
        checkpoints.save(partitionCheckpointKey, PARTITION_DONE);
        log.info("mysql source[{}]: partition snapshot copied {} row(s)", partitionCheckpointKey, copied);
    }

    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
        if (checkpoints.load(checkpointKey) != null) {
            log.info("mysql source[{}]: change-feed checkpoint already exists -- streamChanges "
                    + "will resume from it directly", checkpointKey);
            return;
        }
        String[] binlogStatus = currentBinlogFileAndPosition();
        String file = binlogStatus[0];
        long position = Long.parseLong(binlogStatus[1]);
        checkpoints.save(checkpointKey, GSON.toJson(new BinlogCheckpoint(file, position)));
        log.info("mysql source[{}]: change-feed checkpoint captured ({}:{}) before any partition's "
                + "snapshot starts", checkpointKey, file, position);
    }

    /** {@code SHOW MASTER STATUS} was renamed {@code SHOW BINARY LOG STATUS} in MySQL 8.4 -- not
     * merely deprecated, actually REMOVED (confirmed live: the old syntax fails with a real {@code
     * SQLSyntaxErrorException} against a real {@code mysql:8.4} server, not a hypothetical
     * compatibility concern). Tries the new name first, falls back to the old one for MySQL
     * versions before 8.4/MariaDB, which don't recognize the new name at all. */
    private String[] currentBinlogFileAndPosition() throws SQLException {
        try (Connection conn = openSource(); Statement st = conn.createStatement()) {
            ResultSet rs;
            try {
                rs = st.executeQuery("SHOW BINARY LOG STATUS");
            } catch (SQLException newSyntaxUnsupported) {
                rs = st.executeQuery("SHOW MASTER STATUS");
            }
            try {
                if (!rs.next()) {
                    throw new IllegalStateException("SHOW BINARY LOG STATUS/SHOW MASTER STATUS returned no rows "
                            + "-- is binary logging (log_bin) enabled on the source MySQL server?");
                }
                return new String[] { rs.getString("File"), String.valueOf(rs.getLong("Position")) };
            } finally {
                rs.close();
            }
        }
    }

    @Override
    public void streamChanges(Sink sink, StateStore checkpoints) throws Exception {
        String token = checkpoints.load(checkpointKey);
        if (token == null) {
            throw new IllegalStateException("streamChanges called without a change-feed checkpoint for " + checkpointKey);
        }
        loadSchema();
        BinlogCheckpoint start = GSON.fromJson(token, BinlogCheckpoint.class);

        BinaryLogClient client = new BinaryLogClient(host, port, user, password);
        client.setServerId(binlogServerId);
        client.setBinlogFilename(start.file());
        client.setBinlogPosition(start.position());
        activeClient = client;

        Map<Long, TableMapEventData> tableMap = new HashMap<>();
        AtomicReference<Exception> firstError = new AtomicReference<>();

        client.registerEventListener(event -> {
            try {
                handleBinlogEvent(event, sink, checkpoints, tableMap, client);
            } catch (Exception e) {
                firstError.compareAndSet(null, e);
                log.error("mysql source[{}]: error applying a binlog event -- disconnecting", checkpointKey, e);
                try {
                    client.disconnect();
                } catch (Exception closeError) {
                    log.warn("mysql source[{}]: error disconnecting the binlog client after a prior failure", checkpointKey, closeError);
                }
            }
        });

        try {
            client.connect(); // blocks until disconnect() (running == false via close(), or an internal error)
        } finally {
            activeClient = null;
        }
        Exception error = firstError.get();
        if (error != null && running) {
            throw error;
        }
    }

    private void handleBinlogEvent(Event event, Sink sink, StateStore checkpoints, Map<Long, TableMapEventData> tableMap,
            BinaryLogClient client) throws Exception {
        Object data = event.getData();
        if (data instanceof TableMapEventData tableMapData) {
            tableMap.put(tableMapData.getTableId(), tableMapData);
            return;
        }
        Long tableId = null;
        if (data instanceof WriteRowsEventData d) {
            tableId = d.getTableId();
        } else if (data instanceof UpdateRowsEventData d) {
            tableId = d.getTableId();
        } else if (data instanceof DeleteRowsEventData d) {
            tableId = d.getTableId();
        } else {
            return; // not a row-mutation event (heartbeat, rotate, xid, etc.) -- nothing to apply
        }
        TableMapEventData table = tableMap.get(tableId);
        if (table == null || !sourceDatabase.equals(table.getDatabase()) || !sourceTable.equals(table.getTable())) {
            return; // a different table entirely -- the binlog carries the WHOLE server's traffic
        }

        if (data instanceof WriteRowsEventData d) {
            for (Serializable[] row : d.getRows()) {
                sink.apply(upsertEvent(rowToMap(row)));
            }
        } else if (data instanceof UpdateRowsEventData d) {
            for (Map.Entry<Serializable[], Serializable[]> row : d.getRows()) {
                sink.apply(upsertEvent(rowToMap(row.getValue()))); // the AFTER image, same as Mongo's fullDocument
            }
        } else if (data instanceof DeleteRowsEventData d) {
            for (Serializable[] row : d.getRows()) {
                sink.apply(deleteEvent(rowToMap(row)));
            }
        }

        EventHeaderV4 header = event.getHeader();
        String checkpointJson = GSON.toJson(new BinlogCheckpoint(client.getBinlogFilename(), header.getNextPosition()));
        checkpoints.save(checkpointKey, checkpointJson, Instant.ofEpochMilli(header.getTimestamp()));
    }

    private Map<String, Object> rowToMap(Serializable[] rowValues) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size() && i < rowValues.length; i++) {
            row.put(columns.get(i).name(), rowValues[i]);
        }
        return row;
    }

    /** Builds the VALUES clause and the ON CONFLICT SET clause as two SEPARATE passes over the
     * columns, each appending its OWN bind params in left-to-right order -- correct only because
     * every {@code ?} placeholder needs its own distinct positional bind param even when it binds
     * the SAME logical value as another {@code ?} elsewhere in the same statement (JDBC/Postgres
     * don't let two {@code ?} occurrences share one bind slot the way a named parameter could).
     * Found live, not a hypothetical: an earlier version computed each column's placeholder
     * fragment ONCE and reused that same text string in both the VALUES list and the SET clause,
     * which put two real {@code ?} tokens in the final SQL but only ever bound one param for them,
     * failing every insert with a real "No value specified for parameter" JDBC error. */
    private ChangeEvent upsertEvent(Map<String, Object> row) {
        List<String> params = new ArrayList<>();
        StringBuilder colNames = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo c = columns.get(i);
            if (i > 0) {
                colNames.append(", ");
                placeholders.append(", ");
            }
            colNames.append('"').append(c.name()).append('"');
            appendValuePlaceholder(placeholders, params, row.get(c.name()), c.postgresType());
        }
        StringBuilder setClause = new StringBuilder();
        for (ColumnInfo c : columns) {
            if (c.name().equals(primaryKeyColumn)) {
                continue;
            }
            if (setClause.length() > 0) {
                setClause.append(", ");
            }
            setClause.append('"').append(c.name()).append("\" = ");
            appendValuePlaceholder(setClause, params, row.get(c.name()), c.postgresType());
        }
        String sql = "INSERT INTO " + qualifiedTable() + " (" + colNames + ") VALUES (" + placeholders + ") "
                + "ON CONFLICT (\"" + primaryKeyColumn + "\") DO UPDATE SET " + setClause;
        return new ChangeEvent(sql, params);
    }

    private ChangeEvent deleteEvent(Map<String, Object> row) {
        ColumnInfo pkColumn = columns.stream().filter(c -> c.name().equals(primaryKeyColumn)).findFirst().orElseThrow();
        List<String> params = new ArrayList<>();
        StringBuilder placeholder = new StringBuilder();
        appendValuePlaceholder(placeholder, params, row.get(primaryKeyColumn), pkColumn.postgresType());
        return new ChangeEvent("DELETE FROM " + qualifiedTable() + " WHERE \"" + primaryKeyColumn + "\" = " + placeholder, params);
    }

    /** Appends this column's VALUES-list/SET-clause fragment to {@code sqlFragment} and, unless
     * the value is null, adds its bind param to {@code params} -- see this class's own javadoc for
     * why a null value becomes the literal keyword {@code NULL} directly in the SQL text instead of
     * a bind param (the gRPC contract has no null-parameter marker). Returns the fragment appended,
     * so callers building a SET clause can reuse the exact same text. */
    private static String appendValuePlaceholder(StringBuilder sqlFragment, List<String> params, Object value, String postgresType) {
        if (value == null) {
            sqlFragment.append("NULL");
            return "NULL";
        }
        String fragment;
        String text;
        if (value instanceof byte[] bytes && MySqlTypeMapping.isBinary(postgresType)) {
            text = hex(bytes);
            fragment = "decode(?, 'hex')";
        } else if (value instanceof byte[] bytes) {
            // A non-binary column decoded as raw bytes by the binlog library (a common quirk for
            // VARCHAR/TEXT columns depending on the source's charset) -- decode as UTF-8 text, per
            // this class's own documented assumption.
            text = new String(bytes, StandardCharsets.UTF_8);
            fragment = "?::" + postgresType;
        } else {
            text = String.valueOf(value);
            fragment = "?::" + postgresType;
        }
        params.add(text);
        sqlFragment.append(fragment);
        return fragment;
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private String qualifiedTable() {
        return "\"" + sourceDatabase + "\".\"" + sourceTable + "\"";
    }

    @Override
    public void close() {
        running = false;
        BinaryLogClient client = activeClient;
        if (client != null) {
            try {
                client.disconnect();
            } catch (Exception e) {
                log.warn("mysql source[{}]: error disconnecting the binlog client", checkpointKey, e);
            }
        }
    }

    private record BinlogCheckpoint(String file, long position) {
    }
}
