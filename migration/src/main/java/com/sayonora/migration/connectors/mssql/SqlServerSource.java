package com.sayonora.migration.connectors.mssql;

import com.google.gson.Gson;
import com.sayonora.migration.core.ChangeEvent;
import com.sayonora.migration.core.Partition;
import com.sayonora.migration.core.Sink;
import com.sayonora.migration.core.Source;
import com.sayonora.migration.core.StateStore;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL Server connector: real parallel {@code CHECKSUM}-partitioned reads for the initial bulk read,
 * SQL Server's own native Change Data Capture feature (real {@code cdc.*} change tables and
 * {@code sys.fn_cdc_*} functions, polled via plain JDBC -- no separate client library needed, unlike
 * MySQL's binlog or MongoDB's change streams) for live sync, writing into a target Postgres table
 * whose schema is TRANSLATED from the source's real {@code information_schema} -- same reasoning as
 * {@code MySqlSource}: mssqlwire proxies straight through to a real Postgres backend rather than
 * storing a JSON blob (confirmed by reading wire's own code this session), so there's no fixed
 * physical shape to match here either.
 *
 * <p><b>Change feed mechanism, genuinely different from every other connector in this project</b>:
 * SQL Server CDC is POLL-based, not push-based -- there is no persistent streaming connection the
 * way a Mongo change stream, DynamoDB Streams shard iterator, or MySQL binlog connection is. A
 * background SQL Server Agent job asynchronously reads the transaction log and populates real
 * {@code cdc.<capture_instance>_CT} change tables; this connector periodically queries {@code
 * cdc.fn_cdc_get_all_changes_<capture_instance>(@from_lsn, @to_lsn, 'all')} for whatever landed
 * since the last poll, ordered by LSN (Log Sequence Number, a monotonically increasing 10-byte
 * value SQL Server itself provides comparison functions for). This means CDC MUST be enabled first
 * -- both at the database level ({@code sys.sp_cdc_enable_db}) and the table level ({@code
 * sys.sp_cdc_enable_table}) -- and SQL Server Agent must actually be running on the source server,
 * or the capture job never populates the change tables at all; {@link #prepareChangeFeed} enables
 * both automatically if not already active, matching every other connector's "make it work without
 * extra manual source-side setup" design, but cannot start SQL Server Agent itself if it's disabled
 * server-wide -- that's a real, external server configuration this connector cannot reach into.
 *
 * <p><b>Known, scoped assumptions</b> (documented, not silently guessed):
 * <ul>
 *   <li>Single-column primary key, single target schema per source database (SQL Server's own
 *   {@code dbo} default, same scope line {@code MySqlSource} draws for a single MySQL database).
 *   <li>LSN-level checkpointing, not (LSN, seqval)-level -- a resumed poll starts at {@code
 *   sys.fn_cdc_increment_lsn(lastProcessedLsn)}, which could in principle skip a second change row
 *   sharing the exact same {@code __$start_lsn} as the last-processed row (e.g. an UPDATE producing
 *   both a before- and after-image row) if a crash happened between processing that first row and
 *   the last one at that LSN. A real, separately scoped follow-up would checkpoint at
 *   {@code (lsn, seqval)} granularity instead.
 * </ul>
 */
public final class SqlServerSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(SqlServerSource.class);
    private static final Gson GSON = new Gson();
    private static final String PARTITION_DONE = "\"DONE\"";
    private static final int SNAPSHOT_BATCH_SIZE = 500;
    private static final long POLL_INTERVAL_MILLIS = 2000;

    private record ColumnInfo(String name, String postgresType) {
    }

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String sourceDatabase;
    private final String sourceSchema;
    private final String sourceTable;
    private final int partitionCount;
    private final String checkpointKey;

    private volatile List<ColumnInfo> columns;
    private volatile String primaryKeyColumn;
    private volatile String captureInstance;
    private volatile boolean running = true;

    public SqlServerSource(String host, int port, String user, String password, String sourceDatabase,
            String sourceSchema, String sourceTable) {
        this(host, port, user, password, sourceDatabase, sourceSchema, sourceTable, 1);
    }

    public SqlServerSource(String host, int port, String user, String password, String sourceDatabase,
            String sourceSchema, String sourceTable, int partitionCount) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.sourceDatabase = sourceDatabase;
        this.sourceSchema = sourceSchema;
        this.sourceTable = sourceTable;
        this.partitionCount = Math.max(1, partitionCount);
        this.checkpointKey = "mssql:" + sourceDatabase + "." + sourceSchema + "." + sourceTable;
    }

    private String sourceJdbcUrl() {
        return "jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + sourceDatabase
                + ";encrypt=false;trustServerCertificate=true";
    }

    private Connection openSource() throws SQLException {
        return DriverManager.getConnection(sourceJdbcUrl(), user, password);
    }

    private synchronized void loadSchema() throws SQLException {
        if (columns != null) {
            return;
        }
        List<ColumnInfo> loaded = new ArrayList<>();
        try (Connection conn = openSource();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT column_name, data_type FROM information_schema.columns "
                                + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, sourceSchema);
            ps.setString(2, sourceTable);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    loaded.add(new ColumnInfo(rs.getString(1), MsSqlTypeMapping.toPostgresType(rs.getString(2))));
                }
            }
        }
        if (loaded.isEmpty()) {
            throw new IllegalStateException("SQL Server table " + sourceSchema + "." + sourceTable + " has no "
                    + "columns (or doesn't exist) -- nothing to migrate");
        }
        String pk = null;
        try (Connection conn = openSource();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT k.column_name FROM information_schema.table_constraints t "
                                + "JOIN information_schema.key_column_usage k "
                                + "ON t.constraint_name = k.constraint_name AND t.table_schema = k.table_schema "
                                + "WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema = ? AND t.table_name = ? "
                                + "ORDER BY k.ordinal_position")) {
            ps.setString(1, sourceSchema);
            ps.setString(2, sourceTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    pk = rs.getString(1);
                }
            }
        }
        if (pk == null) {
            throw new IllegalStateException("SQL Server table " + sourceSchema + "." + sourceTable
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
            log.info("mssql source[{}]: partition already fully copied -- skipping", partitionCheckpointKey);
            return;
        }
        loadSchema();
        int bucket = (Integer) partition.descriptor();

        String columnList = columns.stream().map(c -> "[" + c.name() + "]").reduce((a, b) -> a + ", " + b).orElseThrow();
        String sql = "SELECT " + columnList + " FROM [" + sourceSchema + "].[" + sourceTable + "]"
                + (partitionCount > 1 ? " WHERE ABS(CHECKSUM([" + primaryKeyColumn + "])) % " + partitionCount + " = " + bucket : "");

        long copied = 0;
        List<ChangeEvent> batch = new ArrayList<>(SNAPSHOT_BATCH_SIZE);
        try (Connection conn = openSource();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (ColumnInfo c : columns) {
                    row.put(c.name(), MsSqlTypeMapping.isBinary(c.postgresType()) ? rs.getBytes(c.name()) : rs.getObject(c.name()));
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
        log.info("mssql source[{}]: partition snapshot copied {} row(s)", partitionCheckpointKey, copied);
    }

    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
        if (checkpoints.load(checkpointKey) != null) {
            log.info("mssql source[{}]: change-feed checkpoint already exists -- streamChanges "
                    + "will resume from it directly", checkpointKey);
            return;
        }
        ensureCdcEnabled();
        byte[] maxLsn = waitForCdcCaptureToBeActive();
        checkpoints.save(checkpointKey, GSON.toJson(new CdcCheckpoint(hex(maxLsn))));
        log.info("mssql source[{}]: change-feed checkpoint captured (lsn={}, capture instance={}) "
                + "before any partition's snapshot starts", checkpointKey, hex(maxLsn), captureInstance);
    }

    /** Enables CDC at both the database and table level if not already active -- real DynamoDB
     * Streams/MySQL binlog need no such explicit enablement step by an external agent process the
     * way SQL Server CDC does (SQL Server Agent must ALSO actually be running on the source server
     * for the capture job to populate change tables at all; this method enables the FEATURE, it
     * cannot start Agent itself if Agent is disabled server-wide -- a real external prerequisite,
     * not something this connector can reach into). */
    private void ensureCdcEnabled() throws SQLException {
        try (Connection conn = openSource()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT is_cdc_enabled FROM sys.databases WHERE name = ?")) {
                ps.setString(1, sourceDatabase);
                try (ResultSet rs = ps.executeQuery()) {
                    boolean dbCdcEnabled = rs.next() && rs.getBoolean(1);
                    if (!dbCdcEnabled) {
                        try (Statement st = conn.createStatement()) {
                            st.execute("EXEC sys.sp_cdc_enable_db");
                        }
                        log.info("mssql source[{}]: enabled CDC at the database level ({})", checkpointKey, sourceDatabase);
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT ct.capture_instance FROM cdc.change_tables ct "
                            + "WHERE ct.source_object_id = OBJECT_ID(?)")) {
                ps.setString(1, sourceSchema + "." + sourceTable);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        captureInstance = rs.getString(1);
                    }
                }
            }
            if (captureInstance == null) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "EXEC sys.sp_cdc_enable_table @source_schema = ?, @source_name = ?, "
                                + "@role_name = NULL, @supports_net_changes = 0")) {
                    ps.setString(1, sourceSchema);
                    ps.setString(2, sourceTable);
                    ps.execute();
                }
                captureInstance = sourceSchema + "_" + sourceTable;
                log.info("mssql source[{}]: enabled CDC at the table level (capture instance {})", checkpointKey, captureInstance);
            }
        }
    }

    /** {@code sys.fn_cdc_get_max_lsn()} returns {@code NULL} until SQL Server Agent's background
     * capture job has ACTUALLY run at least once after {@code sys.sp_cdc_enable_db} -- which
     * requires Agent to genuinely be running on the source server. Confirmed live against Azure
     * SQL Edge (used for this connector's own snapshot-path tests, since real SQL Server images
     * don't run at all under this project's CI host's emulation): Edge has no Agent component at
     * all, so {@code max_lsn} stays {@code NULL} forever even after real inserts -- calling {@code
     * hex(null)} on that would NPE with no useful explanation. Polls briefly for a real value
     * before giving up with a clear, actionable error instead. */
    private byte[] waitForCdcCaptureToBeActive() throws SQLException, InterruptedException {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            byte[] maxLsn = queryLsn("SELECT sys.fn_cdc_get_max_lsn()");
            if (maxLsn != null) {
                return maxLsn;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("sys.fn_cdc_get_max_lsn() is still NULL after enabling CDC -- this "
                + "means SQL Server Agent's capture job has never run. Is SQL Server Agent actually running "
                + "on the source server? (A lightweight engine like Azure SQL Edge has no Agent at all and "
                + "can enable CDC's metadata but will never actually capture changes.)");
    }

    @Override
    public void streamChanges(Sink sink, StateStore checkpoints) throws Exception {
        String token = checkpoints.load(checkpointKey);
        if (token == null) {
            throw new IllegalStateException("streamChanges called without a change-feed checkpoint for " + checkpointKey);
        }
        loadSchema();
        if (captureInstance == null) {
            ensureCdcEnabled(); // a restarted process (prepareChangeFeed skipped since checkpoint already existed)
        }
        byte[] lastLsn = unhex(GSON.fromJson(token, CdcCheckpoint.class).lsnHex());

        while (running) {
            byte[] maxLsn = queryLsn("SELECT sys.fn_cdc_get_max_lsn()");
            if (compareLsn(lastLsn, maxLsn) >= 0) {
                sleepPollInterval();
                continue;
            }
            byte[] fromLsn = queryLsn("SELECT sys.fn_cdc_increment_lsn(?)", lastLsn);
            List<Object[]> rows = pollChanges(fromLsn, maxLsn);
            for (Object[] row : rows) {
                byte[] rowLsn = (byte[]) row[0];
                int operation = ((Number) row[1]).intValue();
                Map<String, Object> values = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    values.put(columns.get(i).name(), row[2 + i]);
                }
                switch (operation) {
                    case 1 -> sink.apply(deleteEvent(values)); // delete
                    case 2, 4 -> sink.apply(upsertEvent(values)); // insert, update-after
                    default -> { } // 3 = update-before -- superseded by its paired update-after row
                }
                lastLsn = rowLsn;
                Instant eventTime = mapLsnToTime(rowLsn);
                String checkpointJson = GSON.toJson(new CdcCheckpoint(hex(lastLsn)));
                if (eventTime != null) {
                    checkpoints.save(checkpointKey, checkpointJson, eventTime);
                } else {
                    checkpoints.save(checkpointKey, checkpointJson);
                }
            }
            if (rows.isEmpty()) {
                lastLsn = maxLsn; // nothing changed for OUR table, but LSN horizon still advanced
            }
        }
    }

    private List<Object[]> pollChanges(byte[] fromLsn, byte[] toLsn) throws SQLException {
        String columnList = columns.stream().map(c -> "[" + c.name() + "]").reduce((a, b) -> a + ", " + b).orElseThrow();
        String sql = "SELECT __$start_lsn, __$operation, " + columnList
                + " FROM cdc.fn_cdc_get_all_changes_" + captureInstance + "(?, ?, N'all') ORDER BY __$start_lsn, __$seqval";
        List<Object[]> result = new ArrayList<>();
        try (Connection conn = openSource(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setBytes(1, fromLsn);
            ps.setBytes(2, toLsn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Object[] row = new Object[2 + columns.size()];
                    row[0] = rs.getBytes(1);
                    row[1] = rs.getInt(2);
                    for (int i = 0; i < columns.size(); i++) {
                        ColumnInfo c = columns.get(i);
                        row[2 + i] = MsSqlTypeMapping.isBinary(c.postgresType()) ? rs.getBytes(3 + i) : rs.getObject(3 + i);
                    }
                    result.add(row);
                }
            }
        }
        return result;
    }

    private Instant mapLsnToTime(byte[] lsn) throws SQLException {
        try (Connection conn = openSource();
                PreparedStatement ps = conn.prepareStatement("SELECT sys.fn_cdc_map_lsn_to_time(?)")) {
            ps.setBytes(1, lsn);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp(1);
                    return ts == null ? null : ts.toInstant();
                }
                return null;
            }
        }
    }

    private byte[] queryLsn(String sql, byte[]... params) throws SQLException {
        try (Connection conn = openSource(); PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setBytes(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBytes(1);
            }
        }
    }

    /** Unsigned lexicographic byte comparison -- SQL Server LSNs are monotonically increasing
     * 10-byte values comparable this way (per SQL Server's own documented LSN ordering). */
    static int compareLsn(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
    }

    private void sleepPollInterval() throws InterruptedException {
        Thread.sleep(POLL_INTERVAL_MILLIS);
    }

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

    /** Same NULL-handling design as {@code MySqlSource} -- a null value becomes the literal
     * keyword {@code NULL} directly in the SQL text (never a bind param), since {@code
     * QueryService.Execute}'s {@code repeated string params} has no null-value marker. See {@code
     * MySqlSource}'s own javadoc for the full reasoning; identical here, not reinvented. */
    static String appendValuePlaceholder(StringBuilder sqlFragment, List<String> params, Object value, String postgresType) {
        if (value == null) {
            sqlFragment.append("NULL");
            return "NULL";
        }
        String fragment;
        String text;
        if (value instanceof byte[] bytes && MsSqlTypeMapping.isBinary(postgresType)) {
            text = hex(bytes);
            fragment = "decode(?, 'hex')";
        } else if (value instanceof byte[] bytes) {
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

    static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    static byte[] unhex(String hexString) {
        byte[] result = new byte[hexString.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hexString.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private String qualifiedTable() {
        return "\"" + sourceDatabase + "\".\"" + sourceTable + "\"";
    }

    @Override
    public void close() {
        running = false;
    }

    private record CdcCheckpoint(String lsnHex) {
    }
}
