package com.sayonora.warp.core;

import com.sayonora.warp.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.TreeMap;

/**
 * {@link EngineHa} for MySQL asynchronous / semi-synchronous source-replica replication, with GTIDs or
 * binary-log coordinates. Works on MySQL 5.7 through 9.x: it reads {@code SHOW REPLICA STATUS} and
 * falls back to {@code SHOW SLAVE STATUS} (and the {@code Slave_*}/{@code Master_*} column names) on
 * servers that predate the rename.
 *
 * <p>Privileges needed by the Warp user: {@code REPLICATION CLIENT} (to read replica status) for the
 * probes; for {@link #promote}, {@code REPLICATION_SLAVE_ADMIN} and {@code SYSTEM_VARIABLES_ADMIN}
 * (or {@code SUPER} on older servers).
 *
 * <p><b>Lag caveat, by design of MySQL:</b> {@code Seconds_Behind_Source} measures how far the SQL
 * thread is behind events the replica has already <i>retrieved</i>. It cannot see transactions the
 * primary has committed but the I/O thread has not yet fetched (a slow link, for instance), and it
 * reads 0 for an idle but disconnected-looking replica only if both threads are running -- which this
 * class requires. A replica whose threads are not both running is reported unmeasurable, never "0".
 */
final class MySqlHa implements EngineHa {

    static final MySqlHa INSTANCE = new MySqlHa();

    private static final long APPLY_WAIT_SECONDS = longEnv("WARP_FAILOVER_MYSQL_APPLY_WAIT_SECONDS", 60);

    private MySqlHa() {
    }

    private static long longEnv(String name, long fallback) {
        try {
            String v = System.getenv(name);
            return v == null || v.isBlank() ? fallback : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static Connection connect(BackendTarget n, int connectMillis, int socketMillis) throws SQLException {
        Properties props = new Properties();
        if (n.user() != null) {
            props.setProperty("user", n.user());
            String pw = SecretResolver.resolve(n.password());
            props.setProperty("password", pw == null ? "" : pw);
        }
        props.setProperty("connectTimeout", String.valueOf(connectMillis));
        props.setProperty("socketTimeout", String.valueOf(socketMillis));
        return DriverManager.getConnection(n.jdbcUrl(), props);
    }

    // ---- SHOW REPLICA STATUS ------------------------------------------------------------------

    /** The single row of {@code SHOW REPLICA STATUS} (column name -> value, case-insensitive keys),
     * or empty when this server is not configured as a replica of anything. */
    static Optional<Map<String, String>> replicaStatus(Connection c) throws SQLException {
        try {
            return readStatus(c, "SHOW REPLICA STATUS");
        } catch (SQLException e) {
            if (e.getErrorCode() == 1064) { // syntax error: a server that predates the rename
                return readStatus(c, "SHOW SLAVE STATUS");
            }
            throw e;
        }
    }

    private static Optional<Map<String, String>> readStatus(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                return Optional.empty();
            }
            ResultSetMetaData md = rs.getMetaData();
            Map<String, String> row = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 1; i <= md.getColumnCount(); i++) {
                row.put(md.getColumnLabel(i), rs.getString(i));
            }
            return Optional.of(row);
        }
    }

    /** First non-null value among the new and legacy column names. */
    static String col(Map<String, String> row, String... names) {
        for (String n : names) {
            String v = row.get(n);
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    // ---- EngineHa -----------------------------------------------------------------------------

    @Override
    public ReplicaRouter.LagSample lag(BackendTarget replica, long now) {
        try (Connection c = connect(replica, 3000, 5000)) {
            Optional<Map<String, String>> st = replicaStatus(c);
            if (st.isEmpty()) {
                return new ReplicaRouter.LagSample(true, false, 0, "not configured as a replica", now);
            }
            Map<String, String> row = st.get();
            String io = col(row, "Replica_IO_Running", "Slave_IO_Running");
            String sql = col(row, "Replica_SQL_Running", "Slave_SQL_Running");
            if (!"Yes".equalsIgnoreCase(io) || !"Yes".equalsIgnoreCase(sql)) {
                return new ReplicaRouter.LagSample(false, true, 0,
                        "replication threads not both running (IO=" + io + ", SQL=" + sql + ")", now);
            }
            String behind = col(row, "Seconds_Behind_Source", "Seconds_Behind_Master");
            if (behind == null) {
                return new ReplicaRouter.LagSample(false, true, 0, "Seconds_Behind_Source is NULL", now);
            }
            return new ReplicaRouter.LagSample(true, true, Double.parseDouble(behind), null, now);
        } catch (SQLException | RuntimeException e) {
            return new ReplicaRouter.LagSample(false, false, 0, String.valueOf(e.getMessage()), now);
        }
    }

    @Override
    public FailoverMonitor.NodeRole role(BackendTarget node) {
        try (Connection c = connect(node, 3000, 5000)) {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT @@global.read_only")) {
                if (rs.next() && rs.getInt(1) != 0) {
                    return FailoverMonitor.NodeRole.READ_ONLY;
                }
            }
            try {
                // A server that is still replicating from something is a replica even if someone left
                // read_only off: never call it writable.
                if (replicaStatus(c).isPresent()) {
                    return FailoverMonitor.NodeRole.READ_ONLY;
                }
            } catch (SQLException e) {
                if (e.getSQLState() != null && e.getSQLState().startsWith("08")) {
                    return FailoverMonitor.NodeRole.UNREACHABLE;
                }
                // No REPLICATION CLIENT privilege (or similar): read_only is all we can see. That can
                // only make a stray replica look writable, which raises the split-brain alarm and
                // changes nothing -- never the other way round.
            }
            return FailoverMonitor.NodeRole.WRITABLE;
        } catch (SQLException | RuntimeException e) {
            return FailoverMonitor.NodeRole.UNREACHABLE;
        }
    }

    @Override
    public OptionalLong walPosition(BackendTarget replica) throws SQLException {
        try (Connection c = connect(replica, 5000, 30000)) {
            Optional<Map<String, String>> st = replicaStatus(c);
            if (st.isEmpty()) {
                return OptionalLong.empty();
            }
            Map<String, String> row = st.get();
            String executed = col(row, "Executed_Gtid_Set");
            String retrieved = col(row, "Retrieved_Gtid_Set");
            if ((executed != null && !executed.isBlank()) || (retrieved != null && !retrieved.isBlank())) {
                // transactions executed + transactions received but not yet executed
                long total = GtidSets.count(executed);
                if (retrieved != null && !retrieved.isBlank()) {
                    try (java.sql.PreparedStatement ps = c.prepareStatement("SELECT GTID_SUBTRACT(?, ?)")) {
                        ps.setString(1, retrieved.replaceAll("\\s+", ""));
                        ps.setString(2, executed == null ? "" : executed.replaceAll("\\s+", ""));
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                            total += GtidSets.count(rs.getString(1));
                        }
                    }
                }
                return OptionalLong.of(total);
            }
            String file = col(row, "Source_Log_File", "Master_Log_File");
            String pos = col(row, "Read_Source_Log_Pos", "Read_Master_Log_Pos");
            if (file != null && pos != null && file.lastIndexOf('.') >= 0) {
                long number = Long.parseLong(file.substring(file.lastIndexOf('.') + 1));
                return OptionalLong.of((number << 32) | Long.parseLong(pos));
            }
            return OptionalLong.empty();
        }
    }

    @Override
    public void promote(BackendTarget replica) throws SQLException {
        try (Connection c = connect(replica, 5000, 120000); Statement st = c.createStatement()) {
            Optional<Map<String, String>> status = replicaStatus(c);
            if (status.isPresent()) {
                // Stop fetching, then let the SQL thread apply EVERYTHING already received: promoting
                // earlier would silently drop transactions this replica already holds.
                execLegacy(st, "STOP REPLICA IO_THREAD", "STOP SLAVE IO_THREAD");
                long deadline = System.currentTimeMillis() + APPLY_WAIT_SECONDS * 1000;
                while (true) {
                    Map<String, String> row = replicaStatus(c).orElseThrow(
                            () -> new SQLException("replica status vanished while waiting for the relay log"));
                    if (!"Yes".equalsIgnoreCase(col(row, "Replica_SQL_Running", "Slave_SQL_Running"))) {
                        throw new SQLException("replica SQL thread is not running ("
                                + col(row, "Last_SQL_Error", "Last_Error") + ") -- not promoting, received "
                                + "transactions may be unapplied");
                    }
                    if (fullyApplied(row)) {
                        break;
                    }
                    if (System.currentTimeMillis() > deadline) {
                        throw new SQLException("relay log not fully applied within " + APPLY_WAIT_SECONDS
                                + "s -- not promoting (it would lose transactions this replica already received)");
                    }
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("interrupted while waiting for the relay log", e);
                    }
                }
                execLegacy(st, "STOP REPLICA", "STOP SLAVE");
                execLegacy(st, "RESET REPLICA ALL", "RESET SLAVE ALL");
            }
            try {
                st.execute("SET GLOBAL super_read_only = OFF");
            } catch (SQLException e) {
                if (e.getErrorCode() != 1193) { // 1193: unknown system variable (MariaDB / very old servers)
                    throw e;
                }
            }
            st.execute("SET GLOBAL read_only = OFF");
        }
    }

    @Override
    public boolean supportsRepoint() {
        return true;
    }

    /**
     * Points the replica at the new primary with {@code CHANGE REPLICATION SOURCE TO SOURCE_HOST,
     * SOURCE_PORT} (the replication user, password and TLS settings already stored on the replica are
     * kept) and restarts replication, then waits for both threads to run against the new host.
     *
     * <p>GTID auto-positioning only: a replica using binary-log file/offset coordinates has no way to
     * find the matching position on the new primary, so it is refused rather than guessed. Needs
     * {@code REPLICATION_SLAVE_ADMIN}. If replication does not come up, the old source is restored.
     */
    @Override
    public void repoint(BackendTarget replica, BackendTarget newPrimary) throws SQLException {
        JdbcHostPort hp = JdbcHostPort.parse(newPrimary.jdbcUrl(), 3306);
        try (Connection c = connect(replica, 5000, 60000); Statement st = c.createStatement()) {
            Map<String, String> row = replicaStatus(c).orElseThrow(
                    () -> new SQLException("not configured as a replica of anything"));
            String autoPos = col(row, "Auto_Position");
            if (!"1".equals(autoPos)) {
                throw new SQLException("replica uses binary-log coordinates, not GTID auto-positioning; "
                        + "cannot work out the matching position on the new primary");
            }
            String oldHost = col(row, "Source_Host", "Master_Host");
            String oldPort = col(row, "Source_Port", "Master_Port");
            execLegacy(st, "STOP REPLICA", "STOP SLAVE");
            changeSource(st, hp.host(), hp.port());
            execLegacy(st, "START REPLICA", "START SLAVE");
            if (!waitRunning(c, hp.host())) {
                String why;
                try {
                    Map<String, String> now = replicaStatus(c).orElse(Map.of());
                    why = col(now, "Last_IO_Error", "Last_SQL_Error", "Last_Error");
                } catch (SQLException e) {
                    why = e.getMessage();
                }
                execLegacy(st, "STOP REPLICA", "STOP SLAVE");
                if (oldHost != null && oldPort != null) {
                    changeSource(st, oldHost, Integer.parseInt(oldPort));
                    execLegacy(st, "START REPLICA", "START SLAVE");
                }
                throw new SQLException("replication did not start from " + hp.host() + ":" + hp.port()
                        + (why == null || why.isBlank() ? "" : " (" + why + ")") + "; old source restored");
            }
        }
    }

    private static void changeSource(Statement st, String host, int port) throws SQLException {
        String h = host.replace("\\", "\\\\").replace("'", "''");
        try {
            st.execute("CHANGE REPLICATION SOURCE TO SOURCE_HOST='" + h + "', SOURCE_PORT=" + port
                    + ", SOURCE_AUTO_POSITION=1");
        } catch (SQLException e) {
            if (e.getErrorCode() != 1064) {
                throw e;
            }
            st.execute("CHANGE MASTER TO MASTER_HOST='" + h + "', MASTER_PORT=" + port + ", MASTER_AUTO_POSITION=1");
        }
    }

    private static boolean waitRunning(Connection c, String host) throws SQLException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Map<String, String> row = replicaStatus(c).orElse(null);
            if (row != null && "Yes".equalsIgnoreCase(col(row, "Replica_IO_Running", "Slave_IO_Running"))
                    && "Yes".equalsIgnoreCase(col(row, "Replica_SQL_Running", "Slave_SQL_Running"))
                    && host.equalsIgnoreCase(col(row, "Source_Host", "Master_Host"))) {
                return true;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean supportsSwitchover() {
        return true;
    }

    /** {@code super_read_only = ON}: blocks writes from every account including SUPER ones (it waits for
     * in-flight statements to finish). Requires GTIDs, since catching up is verified by GTID set. */
    @Override
    public void freezeWrites(BackendTarget primary) throws SQLException {
        try (Connection c = connect(primary, 5000, 120000); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT @@global.gtid_mode")) {
                if (!rs.next() || !"ON".equalsIgnoreCase(rs.getString(1))) {
                    throw new SQLException("planned switchover needs gtid_mode=ON on the primary");
                }
            }
            st.execute("SET GLOBAL super_read_only = ON");
        }
    }

    @Override
    public void unfreezeWrites(BackendTarget primary) throws SQLException {
        try (Connection c = connect(primary, 5000, 30000); Statement st = c.createStatement()) {
            st.execute("SET GLOBAL read_only = OFF");
            try {
                st.execute("SET GLOBAL super_read_only = OFF");
            } catch (SQLException e) {
                if (e.getErrorCode() != 1193) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void awaitCaughtUp(BackendTarget primary, BackendTarget replica, long timeoutSeconds) throws SQLException {
        String gtids;
        try (Connection c = connect(primary, 5000, 30000); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT @@global.gtid_executed")) {
            rs.next();
            gtids = rs.getString(1).replaceAll("\\s+", "");
        }
        if (gtids.isEmpty()) {
            return;
        }
        try (Connection c = connect(replica, 5000, (int) (timeoutSeconds * 1000 + 10000));
                java.sql.PreparedStatement ps = c.prepareStatement("SELECT WAIT_FOR_EXECUTED_GTID_SET(?, ?)")) {
            ps.setString(1, gtids);
            ps.setLong(2, timeoutSeconds);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getInt(1) != 0) {
                    throw new SQLException("replica did not execute the primary's final GTID set within "
                            + timeoutSeconds + "s");
                }
            }
        }
    }

    /**
     * Makes the old primary a GTID replica of the new one. It has no replication credentials of its own, so
     * this uses the backend's own user and password as the replication account (it needs REPLICATION SLAVE)
     * with {@code GET_SOURCE_PUBLIC_KEY=1} so a caching_sha2 login works without TLS.
     */
    @Override
    public boolean demoteToReplica(BackendTarget oldPrimary, BackendTarget newPrimary) throws SQLException {
        JdbcHostPort hp = JdbcHostPort.parse(newPrimary.jdbcUrl(), 3306);
        String pw = SecretResolver.resolve(oldPrimary.password());
        try (Connection c = connect(oldPrimary, 5000, 60000)) {
            String sql = "CHANGE REPLICATION SOURCE TO SOURCE_HOST=?, SOURCE_PORT=?, SOURCE_USER=?, "
                    + "SOURCE_PASSWORD=?, SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1";
            try (java.sql.PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, hp.host());
                ps.setInt(2, hp.port());
                ps.setString(3, oldPrimary.user() == null ? "" : oldPrimary.user());
                ps.setString(4, pw == null ? "" : pw);
                ps.execute();
            } catch (SQLException e) {
                if (e.getErrorCode() != 1064) {
                    throw e;
                }
                try (java.sql.PreparedStatement ps = c.prepareStatement("CHANGE MASTER TO MASTER_HOST=?, "
                        + "MASTER_PORT=?, MASTER_USER=?, MASTER_PASSWORD=?, MASTER_AUTO_POSITION=1, GET_MASTER_PUBLIC_KEY=1")) {
                    ps.setString(1, hp.host());
                    ps.setInt(2, hp.port());
                    ps.setString(3, oldPrimary.user() == null ? "" : oldPrimary.user());
                    ps.setString(4, pw == null ? "" : pw);
                    ps.execute();
                }
            }
            try (Statement st = c.createStatement()) {
                execLegacy(st, "START REPLICA", "START SLAVE");
            }
            if (!waitRunning(c, hp.host())) {
                throw new SQLException("the old primary was pointed at the new one but replication did not start");
            }
        }
        return true;
    }

    /** True when the SQL thread has applied everything the I/O thread fetched. */
    static boolean fullyApplied(Map<String, String> row) {
        String state = col(row, "Replica_SQL_Running_State", "Slave_SQL_Running_State");
        if (state != null) {
            return state.toLowerCase(java.util.Locale.ROOT).contains("read all relay log");
        }
        String relayFile = col(row, "Relay_Source_Log_File", "Relay_Master_Log_File");
        String srcFile = col(row, "Source_Log_File", "Master_Log_File");
        String execPos = col(row, "Exec_Source_Log_Pos", "Exec_Master_Log_Pos");
        String readPos = col(row, "Read_Source_Log_Pos", "Read_Master_Log_Pos");
        return relayFile != null && relayFile.equals(srcFile) && execPos != null && execPos.equals(readPos);
    }

    private static void execLegacy(Statement st, String modern, String legacy) throws SQLException {
        try {
            st.execute(modern);
        } catch (SQLException e) {
            if (e.getErrorCode() == 1064) {
                st.execute(legacy);
            } else {
                throw e;
            }
        }
    }
}
