package com.sayonora.warp.core;

import com.sayonora.warp.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Properties;

/**
 * {@link EngineHa} for SQL Server <b>Always On Availability Groups</b> with a readable secondary.
 * Verified live (see {@code SqlServerAgLiveTest}) against SQL Server 2022 Developer in a two-node
 * read-scale AG ({@code CLUSTER_TYPE = NONE}, asynchronous commit, readable secondary): role, lag,
 * suspended-secondary handling and follow-after-failover. <b>Not exercised:</b> clustered AGs (WSFC /
 * Pacemaker), synchronous commit, AG listeners, multiple secondaries. The pure decision logic
 * ({@link #roleFrom}, {@link #lagFrom}) is also unit-tested.
 *
 * <p>Setup it assumes: the Warp backend URL names the availability-group database
 * ({@code databaseName=...}); replica URLs point at readable secondaries (add
 * {@code applicationIntent=ReadOnly} so the connection is accepted); the Warp user has
 * {@code VIEW SERVER STATE} (for {@code sys.dm_hadr_database_replica_states}) and can connect to the
 * database. {@code ;} inside a URL is written {@code %3B} in the {@code WARP_BACKENDS} spec.
 *
 * <p>Failover. In {@code follow} mode Warp only <b>follows</b> a failover run elsewhere (the cluster's
 * automatic failover, or {@code ALTER AVAILABILITY GROUP ... FAILOVER} by a DBA). In {@code promote}
 * mode Warp performs one itself, but <b>only for an availability group with
 * {@code CLUSTER_TYPE = NONE}</b> (a read-scale AG with no cluster manager), by running
 * {@code FORCE_FAILOVER_ALLOW_DATA_LOSS} on the chosen secondary -- the only failover such an AG has when
 * the primary is gone. An AG that a cluster manager (WSFC / Pacemaker) owns is refused: forcing it behind
 * the cluster's back is how you get two primaries. Data loss is inherent to a forced failover; Warp's
 * promote safeguards (majority, lease, WAL ranking via {@code last_hardened_lsn}, lag gate) apply first.
 * After a forced failover the other secondaries stay suspended, so {@link #repoint} runs
 * {@code SET (ROLE = SECONDARY)} then {@code SET HADR RESUME} on each (repeated, because a resume issued
 * right after the failover can be accepted yet do nothing) and waits for it to synchronize from the new primary.
 * Verified live on a three-node AG. The old primary, when it returns, is a <i>second primary</i> that accepts
 * writes. {@link #rejoin} only <b>reports</b> it: demoting it is not automated, because
 * {@code SET (ROLE = SECONDARY)} on it failed live with error 41104 ("availability group resource did not come
 * online") in every attempt, and resuming it would in any case discard the transactions it committed that the new
 * primary lacks (Warp cannot prove that set is empty: {@code last_commit_lsn} moves during crash recovery even
 * when no user data was written). A DBA has to demote or rebuild it.
 * A non-readable
 * secondary rejects ordinary connections (error 978) so it is invisible to the probes until it
 * becomes the primary and starts accepting them.
 */
final class SqlServerHa implements EngineHa {

    static final SqlServerHa INSTANCE = new SqlServerHa();

    private SqlServerHa() {
    }

    // ---- pure logic ---------------------------------------------------------------------------

    /** WRITABLE iff the database is READ_WRITE and, when it belongs to an availability group, this
     * replica is its primary ({@code isPrimaryReplica} null means "not in an AG"). */
    static FailoverMonitor.NodeRole roleFrom(String updateability, Integer isPrimaryReplica) {
        if (updateability != null && updateability.trim().equalsIgnoreCase("READ_WRITE")
                && (isPrimaryReplica == null || isPrimaryReplica == 1)) {
            return FailoverMonitor.NodeRole.WRITABLE;
        }
        return FailoverMonitor.NodeRole.READ_ONLY;
    }

    /**
     * Lag of a secondary from its local row of {@code sys.dm_hadr_database_replica_states}. A
     * suspended or unhealthy database, or an undeterminable lag, is unmeasurable (ok=false) -- never 0.
     * Order of trust: {@code SYNCHRONIZED} means caught up (0); else {@code secondary_lag_seconds} when
     * the server reports it; else the redo backlog divided by the redo rate (an empty backlog is 0, a
     * backlog with no measurable rate is unknown). Note the secondary cannot see log the primary has
     * not yet shipped to it, so this can under-report on a slow link.
     */
    static ReplicaRouter.LagSample lagFrom(String updateability, Integer isPrimaryReplica, String syncState,
            String syncHealth, Boolean suspended, Double secondaryLagSeconds, Long redoQueueKb, Long redoRateKbPerSec,
            long now) {
        if (isPrimaryReplica != null && isPrimaryReplica == 1) {
            return new ReplicaRouter.LagSample(true, false, 0, "primary replica of its availability group", now);
        }
        if (isPrimaryReplica == null) {
            return new ReplicaRouter.LagSample(true, false, 0, "database is not in an availability group", now);
        }
        if (updateability == null || !updateability.trim().equalsIgnoreCase("READ_ONLY")) {
            return new ReplicaRouter.LagSample(false, true, 0, "secondary is not readable (updateability "
                    + updateability + ")", now);
        }
        if (Boolean.TRUE.equals(suspended)) {
            return new ReplicaRouter.LagSample(false, true, 0, "data movement is suspended", now);
        }
        if (syncHealth != null && !syncHealth.trim().equalsIgnoreCase("HEALTHY")) {
            return new ReplicaRouter.LagSample(false, true, 0, "synchronization health is " + syncHealth, now);
        }
        String state = syncState == null ? "" : syncState.trim().toUpperCase(java.util.Locale.ROOT);
        if (state.equals("SYNCHRONIZED")) {
            return new ReplicaRouter.LagSample(true, true, 0, null, now);
        }
        if (!state.equals("SYNCHRONIZING")) {
            return new ReplicaRouter.LagSample(false, true, 0, "synchronization state is " + syncState, now);
        }
        if (secondaryLagSeconds != null) {
            return new ReplicaRouter.LagSample(true, true, Math.max(0, secondaryLagSeconds), null, now);
        }
        if (redoQueueKb != null && redoQueueKb == 0) {
            return new ReplicaRouter.LagSample(true, true, 0, null, now);
        }
        if (redoQueueKb != null && redoRateKbPerSec != null && redoRateKbPerSec > 0) {
            return new ReplicaRouter.LagSample(true, true, (double) redoQueueKb / redoRateKbPerSec, null, now);
        }
        return new ReplicaRouter.LagSample(false, true, 0, "lag cannot be determined (no lag figure, redo backlog "
                + "with no redo rate)", now);
    }

    // ---- JDBC ---------------------------------------------------------------------------------

    private static Connection connect(BackendTarget n) throws SQLException {
        Properties props = new Properties();
        if (n.user() != null) {
            props.setProperty("user", n.user());
            String pw = SecretResolver.resolve(n.password());
            props.setProperty("password", pw == null ? "" : pw);
        }
        props.setProperty("loginTimeout", "3");
        props.setProperty("socketTimeout", "8000");
        return DriverManager.getConnection(n.jdbcUrl(), props);
    }

    @Override
    public FailoverMonitor.NodeRole role(BackendTarget node) {
        try (Connection c = connect(node); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS "
                        + "varchar(20)), sys.fn_hadr_is_primary_replica(DB_NAME())")) {
            if (!rs.next()) {
                return FailoverMonitor.NodeRole.UNREACHABLE;
            }
            int primary = rs.getInt(2);
            return roleFrom(rs.getString(1), rs.wasNull() ? null : primary);
        } catch (SQLException | RuntimeException e) {
            return FailoverMonitor.NodeRole.UNREACHABLE;
        }
    }

    @Override
    public ReplicaRouter.LagSample lag(BackendTarget replica, long now) {
        String sql = "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS varchar(20)), "
                + "sys.fn_hadr_is_primary_replica(DB_NAME()), "
                + "(SELECT TOP 1 CAST(synchronization_state_desc AS varchar(30)) FROM sys.dm_hadr_database_replica_states "
                + "WHERE is_local = 1 AND database_id = DB_ID()), "
                + "(SELECT TOP 1 CAST(synchronization_health_desc AS varchar(30)) FROM sys.dm_hadr_database_replica_states "
                + "WHERE is_local = 1 AND database_id = DB_ID()), "
                + "(SELECT TOP 1 CAST(is_suspended AS int) FROM sys.dm_hadr_database_replica_states "
                + "WHERE is_local = 1 AND database_id = DB_ID()), "
                + "(SELECT TOP 1 CAST(secondary_lag_seconds AS float) FROM sys.dm_hadr_database_replica_states "
                + "WHERE is_local = 1 AND database_id = DB_ID()), "
                + "(SELECT TOP 1 redo_queue_size FROM sys.dm_hadr_database_replica_states "
                + "WHERE is_local = 1 AND database_id = DB_ID()), "
                + "(SELECT TOP 1 redo_rate FROM sys.dm_hadr_database_replica_states "
                + "WHERE is_local = 1 AND database_id = DB_ID())";
        try (Connection c = connect(replica); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                return new ReplicaRouter.LagSample(false, false, 0, "no row from the lag query", now);
            }
            String updateability = rs.getString(1);
            int primary = rs.getInt(2);
            Integer isPrimary = rs.wasNull() ? null : primary;
            String state = rs.getString(3);
            String health = rs.getString(4);
            int susp = rs.getInt(5);
            Boolean suspended = rs.wasNull() ? null : susp != 0;
            double lagSec = rs.getDouble(6);
            Double lag = rs.wasNull() ? null : lagSec;
            long rq = rs.getLong(7);
            Long redoQueue = rs.wasNull() ? null : rq;
            long rr = rs.getLong(8);
            Long redoRate = rs.wasNull() ? null : rr;
            return lagFrom(updateability, isPrimary, state, health, suspended, lag, redoQueue, redoRate, now);
        } catch (SQLException | RuntimeException e) {
            return new ReplicaRouter.LagSample(false, false, 0, String.valueOf(e.getMessage()), now);
        }
    }

    /** {@code last_hardened_lsn} of the local secondary database. The LSN is a 25-digit decimal (VLF sequence,
     * block offset, slot), which can exceed a long; the 5-digit slot is dropped and, if it still would not fit,
     * further low digits, which keeps the ordering that ranking needs. */
    @Override
    public OptionalLong walPosition(BackendTarget replica) throws SQLException {
        try (Connection c = connect(replica); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT TOP 1 CAST(last_hardened_lsn AS decimal(25,0)) "
                        + "FROM sys.dm_hadr_database_replica_states WHERE is_local = 1 AND database_id = DB_ID()")) {
            if (!rs.next() || rs.getBigDecimal(1) == null) {
                return OptionalLong.empty();
            }
            java.math.BigInteger v = rs.getBigDecimal(1).toBigInteger().divide(java.math.BigInteger.valueOf(100_000));
            while (v.bitLength() > 62) {
                v = v.divide(java.math.BigInteger.TEN);
            }
            return OptionalLong.of(v.longValue());
        }
    }

    /**
     * Whether this secondary is connected to its availability group's primary: its own row in {@code dm_hadr_availability_replica_states}
     * reads {@code CONNECTED}. An idle primary does not advance {@code last_received_time}, so there is no age to report: a connected
     * secondary answers 0 (the primary is there), a disconnected one answers empty. Measured live: it stays CONNECTED while an idle primary
     * is alive and flips to DISCONNECTED about 13 s after the primary is killed (the AG session timeout is 10 s). Needs VIEW SERVER STATE.
     */
    @Override
    public OptionalDouble heardFromPrimarySecondsAgo(BackendTarget replica, BackendTarget primary) throws SQLException {
        try (Connection c = connect(replica); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT CAST(ars.connected_state_desc AS varchar(20)) "
                        + "FROM sys.dm_hadr_availability_replica_states ars "
                        + "JOIN sys.availability_databases_cluster adc ON adc.group_id = ars.group_id "
                        + "WHERE ars.is_local = 1 AND ars.role_desc = 'SECONDARY' AND adc.database_name = DB_NAME()")) {
            while (rs.next()) {
                if ("CONNECTED".equalsIgnoreCase(rs.getString(1))) {
                    return OptionalDouble.of(0);
                }
            }
            return OptionalDouble.empty();
        }
    }

    /**
     * A stale primary (the old primary returning after a forced failover, still PRIMARY and writable) is taken out of service with
     * {@code ALTER AVAILABILITY GROUP ... OFFLINE}: its role becomes RESOLVING and every access to the database fails with error 983
     * until the instance is restarted (verified live; the new primary is unaffected and the node comes back as a secondary). Only for a
     * read-scale AG ({@code CLUSTER_TYPE = NONE}); a clustered AG is owned by the cluster manager. {@code ALTER DATABASE ... SET
     * READ_ONLY / SINGLE_USER} is not allowed on an availability database (error 1468), which is why this is the only SQL-only way.
     */
    @Override
    public void fenceStaleWriter(BackendTarget node) throws SQLException {
        java.util.List<String> groups = new java.util.ArrayList<>();
        try (Connection c = connect(node); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT ag.name, CAST(ag.cluster_type_desc AS varchar(20)) FROM sys.availability_groups ag "
                    + "JOIN sys.availability_databases_cluster adc ON adc.group_id = ag.group_id "
                    + "JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1 "
                    + "WHERE adc.database_name = DB_NAME() AND ars.role_desc = 'PRIMARY'")) {
                while (rs.next()) {
                    if (!"NONE".equalsIgnoreCase(rs.getString(2))) {
                        throw new UnsupportedOperationException("availability group '" + rs.getString(1) + "' is " + rs.getString(2)
                                + "-managed; the cluster manager owns it, not Warp");
                    }
                    groups.add(rs.getString(1));
                }
            }
            if (groups.isEmpty()) {
                throw new UnsupportedOperationException("the database is not in a PRIMARY-role read-scale availability group on "
                        + "this server, so there is no safe SQL to stop it taking writes");
            }
            st.execute("USE master");
            for (String g : groups) {
                st.execute("ALTER AVAILABILITY GROUP [" + g.replace("]", "]]") + "] OFFLINE");
            }
        }
    }

    @Override
    public void promote(BackendTarget replica) throws SQLException {
        String group;
        try (Connection c = connect(replica); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT ag.name, CAST(ag.cluster_type_desc AS varchar(20)), "
                        + "sys.fn_hadr_is_primary_replica(DB_NAME()) FROM sys.availability_groups ag "
                        + "JOIN sys.dm_hadr_database_replica_states drs ON drs.group_id = ag.group_id "
                        + "WHERE drs.is_local = 1 AND drs.database_id = DB_ID()")) {
            if (!rs.next()) {
                throw new SQLException("the database is not in an availability group on this server");
            }
            group = rs.getString(1);
            String clusterType = rs.getString(2);
            int primary = rs.getInt(3);
            if (rs.next()) {
                throw new SQLException("the database is in more than one availability group; refusing to guess which to fail over");
            }
            if (!"NONE".equalsIgnoreCase(clusterType)) {
                throw new SQLException("availability group '" + group + "' is managed by a cluster manager ("
                        + clusterType + "); fail it over with the cluster's own tooling, not behind its back");
            }
            if (primary == 1) {
                throw new SQLException("this replica is already the primary");
            }
            // ALTER AVAILABILITY GROUP must run from master
            c.setCatalog("master");
            st.execute("ALTER AVAILABILITY GROUP [" + group.replace("]", "]]") + "] FORCE_FAILOVER_ALLOW_DATA_LOSS");
        }
        long deadline = System.currentTimeMillis() + 60_000;
        while (role(replica) != FailoverMonitor.NodeRole.WRITABLE) {
            if (System.currentTimeMillis() > deadline) {
                throw new SQLException("forced failover of '" + group + "' ran but the database did not become "
                        + "writable within 60s");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("interrupted", e);
            }
        }
    }

    // ---- after a forced failover: resume survivors, demote the old primary -----------------------

    /** The availability group of the connection's database as seen from the node itself. */
    private record AgInfo(String group, String clusterType, boolean primary, String database, boolean suspended,
            String syncState) {
    }

    private static AgInfo agInfo(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT ag.name, CAST(ag.cluster_type_desc AS varchar(20)), "
                        + "CAST(sys.fn_hadr_is_primary_replica(DB_NAME()) AS int), DB_NAME(), "
                        + "CAST(drs.is_suspended AS int), CAST(drs.synchronization_state_desc AS varchar(30)) "
                        + "FROM sys.availability_groups ag JOIN sys.dm_hadr_database_replica_states drs "
                        + "ON drs.group_id = ag.group_id WHERE drs.is_local = 1 AND drs.database_id = DB_ID()")) {
            if (!rs.next()) {
                throw new SQLException("the database is not in an availability group on this server");
            }
            AgInfo info = new AgInfo(rs.getString(1), rs.getString(2), rs.getInt(3) == 1, rs.getString(4),
                    rs.getInt(5) == 1, rs.getString(6));
            if (rs.next()) {
                throw new SQLException("the database is in more than one availability group; refusing to guess");
            }
            if (!"NONE".equalsIgnoreCase(info.clusterType())) {
                throw new SQLException("availability group '" + info.group() + "' is managed by a cluster manager ("
                        + info.clusterType() + "); not touching it behind the cluster's back");
            }
            return info;
        }
    }

    private static String bracket(String name) {
        return "[" + name.replace("]", "]]") + "]";
    }

    private static void demoteAndResume(Connection c, AgInfo info, boolean resume) throws SQLException {
        c.setCatalog("master"); // availability-group DDL runs from master
        try (Statement st = c.createStatement()) {
            st.execute("ALTER AVAILABILITY GROUP " + bracket(info.group()) + " SET (ROLE = SECONDARY)");
            if (resume) {
                st.execute("ALTER DATABASE " + bracket(info.database()) + " SET HADR RESUME");
            }
        }
    }

    private static boolean awaitSynchronizing(Connection c, long timeoutMillis) throws SQLException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT CAST(is_suspended AS int), "
                            + "CAST(synchronization_state_desc AS varchar(30)) FROM sys.dm_hadr_database_replica_states "
                            + "WHERE is_local = 1 AND database_id = DB_ID()")) {
                if (rs.next() && rs.getInt(1) == 0 && ("SYNCHRONIZING".equalsIgnoreCase(rs.getString(2))
                        || "SYNCHRONIZED".equalsIgnoreCase(rs.getString(2)))) {
                    return true;
                }
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
    public boolean supportsRepoint() {
        return true;
    }

    /**
     * After a forced failover a surviving secondary is suspended and still tied to the old primary: tells it
     * it is a secondary, resumes data movement, and waits until it synchronizes from the new primary. (A
     * secondary that hardened more log than the new primary would be rolled back to the common point; Warp promotes
     * the secondary with the most log, so that is only possible on a tie.)
     */
    @Override
    public void repoint(BackendTarget replica, BackendTarget newPrimary) throws SQLException {
        try (Connection c = connect(replica)) {
            String catalog = c.getCatalog();
            AgInfo info = agInfo(c);
            if (info.primary()) {
                throw new SQLException("this replica is a primary, not a secondary");
            }
            // Right after the forced failover a RESUME can be accepted yet change nothing until the new primary has
            // finished taking over (seen live: it works a few seconds later), so repeat the idempotent pair.
            long deadline = System.currentTimeMillis() + 90_000;
            while (true) {
                demoteAndResume(c, info, true);
                c.setCatalog(catalog);
                if (awaitSynchronizing(c, 4_000)) {
                    return;
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new SQLException("the secondary did not start synchronizing from the new primary within 90s");
                }
            }
        }
    }

    /**
     * Classification only. A node that is a primary of the availability group while Warp's primary is another node
     * (the old primary, back after a forced failover) is a second writer; Warp reports it and leaves it exactly as
     * found. See the class comment for why it is not demoted automatically.
     */
    @Override
    public RejoinResult rejoin(BackendTarget node, BackendTarget currentPrimary) throws SQLException {
        try (Connection c = connect(node)) {
            AgInfo info = agInfo(c);
            if (info.primary()) {
                return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "it is a second primary of availability group '"
                        + info.group() + "' and still takes writes; Warp does not demote it automatically (any "
                        + "transaction it committed that the new primary lacks would be lost) -- stop writes to it, then "
                        + "demote it with ALTER AVAILABILITY GROUP " + bracket(info.group()) + " SET (ROLE = SECONDARY) "
                        + "or rebuild it as a secondary");
            }
            if (info.suspended()) {
                return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "it is a secondary with data movement suspended; "
                        + "review it, then run ALTER DATABASE " + bracket(info.database()) + " SET HADR RESUME (this "
                        + "discards any transaction it holds that the new primary lacks)");
            }
            return RejoinResult.of(RejoinOutcome.NOT_NEEDED, "already a synchronizing secondary");
        }
    }
}
