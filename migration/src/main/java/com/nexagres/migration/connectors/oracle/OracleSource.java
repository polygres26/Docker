package com.nexagres.migration.connectors.oracle;

import com.google.gson.Gson;
import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Partition;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.core.Source;
import com.nexagres.migration.core.StateStore;
import java.sql.CallableStatement;
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
 * Oracle connector: real parallel {@code ORA_HASH}-partitioned reads for the initial bulk read,
 * real Oracle LogMiner for live sync, writing into a target Postgres table whose schema is
 * TRANSLATED from the source's real {@code all_tab_columns}/{@code all_constraints} -- same
 * reasoning as {@code MySqlSource}/{@code SqlServerSource}: orawire proxies straight through to a
 * real Postgres backend rather than storing a JSON blob (confirmed by reading wire's own code this
 * session), so there's no fixed physical shape to match.
 *
 * <p><b>Change feed mechanism</b>: LogMiner has no structured column/value interface -- every
 * change surfaces as a synthesized {@code SQL_REDO} statement in {@code V$LOGMNR_CONTENTS}, parsed
 * by {@link OracleRedoParser} (see its own javadoc for the full parsing design and why table-level
 * {@code ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS} is required). {@link #prepareChangeFeed} enables
 * that supplemental logging automatically if not already active, plus database-level MINIMAL
 * supplemental logging (a real prerequisite for LogMiner at all) -- the latter needs a privileged
 * connection (confirmed live: {@code ALTER DATABASE ADD SUPPLEMENTAL LOG DATA} requires DBA-level
 * privilege, a real external prerequisite this connector surfaces a clear error for rather than
 * failing obscurely, same spirit as SQL Server CDC needing Agent to actually be running).
 *
 * <p>Like SQL Server CDC, this is POLL-based (start/stop a LogMiner session over an SCN range
 * every poll), not a persistent streaming connection -- there is no Oracle-native equivalent of a
 * push-based change feed.
 *
 * <p><b>Required grants for the connection this connector runs as</b> (beyond {@code CONNECT}/
 * {@code RESOURCE} on the source schema itself): {@code EXECUTE_CATALOG_ROLE}, {@code SELECT ANY
 * TRANSACTION}, {@code LOGMINING}, and {@code SELECT_CATALOG_ROLE} -- the last one found live, not
 * assumed: {@code EXECUTE_CATALOG_ROLE} alone does NOT let a non-privileged user query {@code
 * V$DATABASE} (needed for {@code CURRENT_SCN}/{@code SUPPLEMENTAL_LOG_DATA_MIN}), confirmed by a
 * real {@code ORA-00942} the first time this connector ran end to end as an ordinary schema owner
 * rather than the {@code system} account this session's own initial LogMiner probing happened to
 * use.
 *
 * <p><b>Known, scoped assumptions</b> (documented, not silently guessed):
 * <ul>
 *   <li>Single-column primary key, same scope line every connector in this project draws.
 *   <li>{@code COMMITTED_DATA_ONLY} LogMiner option -- only committed transactions are replicated,
 *   in commit order; an in-flight (uncommitted) transaction's changes are invisible until commit,
 *   which is the correct behavior for a migration (never replicate data that could still roll
 *   back), not a limitation to work around.
 *   <li>LOB (CLOB/BLOB) values beyond LogMiner's own inline-redo size threshold are represented
 *   across multiple chained {@code V$LOGMNR_CONTENTS} rows this connector does not reassemble --
 *   see {@link OracleRedoParser}'s own javadoc.
 * </ul>
 */
public final class OracleSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(OracleSource.class);
    private static final Gson GSON = new Gson();
    private static final String PARTITION_DONE = "\"DONE\"";
    private static final int SNAPSHOT_BATCH_SIZE = 500;
    private static final long POLL_INTERVAL_MILLIS = 2000;
    private static final String MINING_DATE_FORMAT = "YYYY-MM-DD HH24:MI:SS";
    private static final String MINING_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF";

    private record ColumnInfo(String name, String postgresType) {
    }

    private final String host;
    private final int port;
    private final String serviceName;
    private final String user;
    private final String password;
    private final String sourceSchema;
    private final String sourceTable;
    private final int partitionCount;
    private final String checkpointKey;

    private volatile List<ColumnInfo> columns;
    private volatile String primaryKeyColumn;
    private volatile boolean running = true;

    public OracleSource(String host, int port, String serviceName, String user, String password,
            String sourceSchema, String sourceTable) {
        this(host, port, serviceName, user, password, sourceSchema, sourceTable, 1);
    }

    public OracleSource(String host, int port, String serviceName, String user, String password,
            String sourceSchema, String sourceTable, int partitionCount) {
        this.host = host;
        this.port = port;
        this.serviceName = serviceName;
        this.user = user;
        this.password = password;
        this.sourceSchema = sourceSchema.toUpperCase();
        this.sourceTable = sourceTable.toUpperCase();
        this.partitionCount = Math.max(1, partitionCount);
        this.checkpointKey = "oracle:" + this.sourceSchema + "." + this.sourceTable;
    }

    private String sourceJdbcUrl() {
        return "jdbc:oracle:thin:@" + host + ":" + port + "/" + serviceName;
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
                        "SELECT column_name, data_type FROM all_tab_columns "
                                + "WHERE owner = ? AND table_name = ? ORDER BY column_id")) {
            ps.setString(1, sourceSchema);
            ps.setString(2, sourceTable);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    loaded.add(new ColumnInfo(rs.getString(1), OracleTypeMapping.toPostgresType(rs.getString(2))));
                }
            }
        }
        if (loaded.isEmpty()) {
            throw new IllegalStateException("Oracle table " + sourceSchema + "." + sourceTable + " has no columns "
                    + "(or doesn't exist) -- nothing to migrate");
        }
        String pk = null;
        try (Connection conn = openSource();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT cc.column_name FROM all_constraints c "
                                + "JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner "
                                + "WHERE c.constraint_type = 'P' AND c.owner = ? AND c.table_name = ? "
                                + "ORDER BY cc.position")) {
            ps.setString(1, sourceSchema);
            ps.setString(2, sourceTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    pk = rs.getString(1);
                }
            }
        }
        if (pk == null) {
            throw new IllegalStateException("Oracle table " + sourceSchema + "." + sourceTable
                    + " has no primary key -- required for partitioning, upserts, and delete replication");
        }
        this.primaryKeyColumn = pk;
        this.columns = loaded;
    }

    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        loadSchema();
        applyTolerantOfConcurrentCreateRace(sink, "CREATE SCHEMA IF NOT EXISTS \"" + sourceSchema + "\"");
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
            log.info("oracle source[{}]: partition already fully copied -- skipping", partitionCheckpointKey);
            return;
        }
        loadSchema();
        int bucket = (Integer) partition.descriptor();

        String columnList = columns.stream().map(c -> "\"" + c.name() + "\"").reduce((a, b) -> a + ", " + b).orElseThrow();
        String sql = "SELECT " + columnList + " FROM \"" + sourceSchema + "\".\"" + sourceTable + "\""
                + (partitionCount > 1 ? " WHERE ORA_HASH(\"" + primaryKeyColumn + "\", " + (partitionCount - 1) + ") = " + bucket : "");

        long copied = 0;
        List<ChangeEvent> batch = new ArrayList<>(SNAPSHOT_BATCH_SIZE);
        try (Connection conn = openSource();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (ColumnInfo c : columns) {
                    row.put(c.name(), OracleTypeMapping.isBinary(c.postgresType()) ? rs.getBytes(c.name()) : rs.getObject(c.name()));
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
        log.info("oracle source[{}]: partition snapshot copied {} row(s)", partitionCheckpointKey, copied);
    }

    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
        if (checkpoints.load(checkpointKey) != null) {
            log.info("oracle source[{}]: change-feed checkpoint already exists -- streamChanges "
                    + "will resume from it directly", checkpointKey);
            return;
        }
        ensureSupplementalLogging();
        long startScn = queryScn("SELECT CURRENT_SCN FROM V$DATABASE");
        checkpoints.save(checkpointKey, GSON.toJson(new LogMinerCheckpoint(startScn)));
        log.info("oracle source[{}]: change-feed checkpoint captured (scn={}) before any "
                + "partition's snapshot starts", checkpointKey, startScn);
    }

    /** Enables supplemental logging at both the database level (MINIMAL -- a real LogMiner
     * prerequisite, needs DBA-level privilege, a real external requirement this surfaces a clear
     * error for rather than failing obscurely) and the table level (ALL COLUMNS -- what actually
     * makes {@link OracleRedoParser} able to recover a full row image / the primary key from every
     * UPDATE and DELETE's generated redo, confirmed live: without this, an UPDATE's WHERE clause
     * only contains the changed column's OLD value plus ROWID, not the primary key at all). */
    private void ensureSupplementalLogging() throws SQLException {
        try (Connection conn = openSource()) {
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE")) {
                rs.next();
                if ("NO".equals(rs.getString(1))) {
                    try (Statement alter = conn.createStatement()) {
                        alter.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
                    } catch (SQLException e) {
                        throw new SQLException("Could not enable database-level supplemental logging "
                                + "(required for LogMiner at all) -- this needs a DBA-privileged connection. "
                                + "Ask a DBA to run 'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA' once, or "
                                + "connect this migration as a user with that privilege. On a multitenant "
                                + "(CDB/PDB) instance this MUST run against the CDB root service, not the PDB "
                                + "-- confirmed live: it fails with ORA-01031 against a PDB service even as a "
                                + "fully DBA-privileged user.", e.getSQLState(), e);
                    }
                    log.info("oracle source[{}]: enabled database-level (MINIMAL) supplemental logging", checkpointKey);
                }
            }
            boolean tableLevelEnabled;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM all_log_groups WHERE owner = ? AND table_name = ? "
                            + "AND log_group_type = 'ALL COLUMN LOGGING'")) {
                ps.setString(1, sourceSchema);
                ps.setString(2, sourceTable);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    tableLevelEnabled = rs.getInt(1) > 0;
                }
            }
            if (!tableLevelEnabled) {
                try (Statement st = conn.createStatement()) {
                    st.execute("ALTER TABLE \"" + sourceSchema + "\".\"" + sourceTable
                            + "\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
                }
                log.info("oracle source[{}]: enabled table-level (ALL COLUMNS) supplemental logging", checkpointKey);
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
        long lastScn = GSON.fromJson(token, LogMinerCheckpoint.class).scn();

        while (running) {
            long currentScn = queryScn("SELECT CURRENT_SCN FROM V$DATABASE");
            if (currentScn <= lastScn) {
                Thread.sleep(POLL_INTERVAL_MILLIS);
                continue;
            }
            List<Object[]> changes = mineChanges(lastScn + 1, currentScn);
            for (Object[] change : changes) {
                String operation = (String) change[0];
                @SuppressWarnings("unchecked")
                Map<String, String> values = (Map<String, String>) change[1];
                Instant eventTime = (Instant) change[2];
                Map<String, Object> row = new HashMap<>(values);
                switch (operation) {
                    case "INSERT", "UPDATE" -> sink.apply(upsertEvent(row));
                    case "DELETE" -> sink.apply(deleteEvent(row));
                    default -> { }
                }
                String checkpointJson = GSON.toJson(new LogMinerCheckpoint(currentScn));
                if (eventTime != null) {
                    checkpoints.save(checkpointKey, checkpointJson, eventTime);
                } else {
                    checkpoints.save(checkpointKey, checkpointJson);
                }
            }
            lastScn = currentScn;
            if (changes.isEmpty()) {
                checkpoints.save(checkpointKey, GSON.toJson(new LogMinerCheckpoint(currentScn)));
            }
        }
    }

    /** Returns {@code [operation, Map<String,String> parsedValues, Instant eventTime]} triples,
     * one per {@code V$LOGMNR_CONTENTS} row for this table -- a fresh LogMiner session per poll
     * (start, query, end), the standard usage pattern (a session is scoped to one SCN range). */
    private List<Object[]> mineChanges(long startScn, long endScn) throws SQLException {
        List<Object[]> result = new ArrayList<>();
        try (Connection conn = openSource(); Statement session = conn.createStatement()) {
            session.execute("ALTER SESSION SET NLS_DATE_FORMAT = '" + MINING_DATE_FORMAT + "'");
            session.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '" + MINING_TIMESTAMP_FORMAT + "'");
            try (CallableStatement start = conn.prepareCall(
                    "BEGIN DBMS_LOGMNR.START_LOGMNR(startScn => ?, endScn => ?, "
                            + "options => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;")) {
                start.setLong(1, startScn);
                start.setLong(2, endScn);
                start.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT OPERATION, SQL_REDO, TIMESTAMP FROM V$LOGMNR_CONTENTS "
                            + "WHERE SEG_OWNER = ? AND TABLE_NAME = ? AND OPERATION IN ('INSERT','UPDATE','DELETE') "
                            + "ORDER BY SCN")) {
                ps.setString(1, sourceSchema);
                ps.setString(2, sourceTable);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String operation = rs.getString("OPERATION");
                        String sqlRedo = rs.getString("SQL_REDO");
                        Timestamp ts = rs.getTimestamp("TIMESTAMP");
                        OracleRedoParser.ParsedChange parsed = "INSERT".equals(operation)
                                ? OracleRedoParser.parseInsert(sqlRedo)
                                : OracleRedoParser.parseUpdateOrDelete(sqlRedo, "UPDATE".equals(operation));
                        result.add(new Object[] { operation, parsed.values(), ts == null ? null : ts.toInstant() });
                    }
                }
            } finally {
                try (Statement end = conn.createStatement()) {
                    end.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
                } catch (SQLException ignored) {
                    // best-effort -- a fresh connection next poll doesn't need a clean end anyway
                }
            }
        }
        return result;
    }

    private ChangeEvent upsertEvent(Map<String, Object> row) {
        List<String> params = new ArrayList<>();
        StringBuilder colNames = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        int included = 0;
        for (ColumnInfo c : columns) {
            if (!row.containsKey(c.name())) {
                continue; // an UPDATE's redo only carries CHANGED columns -- see this class's own javadoc
            }
            if (included++ > 0) {
                colNames.append(", ");
                placeholders.append(", ");
            }
            colNames.append('"').append(c.name()).append('"');
            appendValuePlaceholder(placeholders, params, row.get(c.name()), c.postgresType());
        }
        StringBuilder setClause = new StringBuilder();
        int setCount = 0;
        for (ColumnInfo c : columns) {
            if (c.name().equals(primaryKeyColumn) || !row.containsKey(c.name())) {
                continue;
            }
            if (setCount++ > 0) {
                setClause.append(", ");
            }
            setClause.append('"').append(c.name()).append("\" = ");
            appendValuePlaceholder(setClause, params, row.get(c.name()), c.postgresType());
        }
        String sql = "INSERT INTO " + qualifiedTable() + " (" + colNames + ") VALUES (" + placeholders + ") "
                + "ON CONFLICT (\"" + primaryKeyColumn + "\") DO UPDATE SET "
                + (setCount == 0 ? "\"" + primaryKeyColumn + "\" = EXCLUDED.\"" + primaryKeyColumn + "\"" : setClause.toString());
        return new ChangeEvent(sql, params);
    }

    private ChangeEvent deleteEvent(Map<String, Object> row) {
        ColumnInfo pkColumn = columns.stream().filter(c -> c.name().equals(primaryKeyColumn)).findFirst().orElseThrow();
        List<String> params = new ArrayList<>();
        StringBuilder placeholder = new StringBuilder();
        appendValuePlaceholder(placeholder, params, row.get(primaryKeyColumn), pkColumn.postgresType());
        return new ChangeEvent("DELETE FROM " + qualifiedTable() + " WHERE \"" + primaryKeyColumn + "\" = " + placeholder, params);
    }

    /** Same NULL-handling design as {@code MySqlSource}/{@code SqlServerSource} -- a null value
     * becomes the literal keyword {@code NULL} directly in the SQL text (never a bind param), since
     * {@code QueryService.Execute}'s {@code repeated string params} has no null-value marker.
     * Accepts a plain {@code Object} (not just a pre-stringified value) because this connector
     * has TWO different value sources feeding the same code path: the snapshot's real typed JDBC
     * objects ({@code rs.getObject}/{@code rs.getBytes}) and the CDC path's already-parsed literal
     * text from {@link OracleRedoParser} (always {@code String} or {@code null}) -- both need the
     * exact same NULL-vs-placeholder logic, so this stays generic rather than duplicating it. */
    static String appendValuePlaceholder(StringBuilder sqlFragment, List<String> params, Object value, String postgresType) {
        if (value == null) {
            sqlFragment.append("NULL");
            return "NULL";
        }
        String text;
        String fragment;
        if (value instanceof byte[] bytes && OracleTypeMapping.isBinary(postgresType)) {
            text = hex(bytes);
            fragment = "decode(?, 'hex')";
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

    private long queryScn(String sql) throws SQLException {
        try (Connection conn = openSource(); Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private String qualifiedTable() {
        return "\"" + sourceSchema + "\".\"" + sourceTable + "\"";
    }

    @Override
    public void close() {
        running = false;
    }

    private record LogMinerCheckpoint(long scn) {
    }
}
