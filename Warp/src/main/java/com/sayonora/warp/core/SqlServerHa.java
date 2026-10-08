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
 * Verified live on a three-node AG.
 *
 * <p>Planned switchover (read-scale AG): {@link #prepareSwitchover} puts the primary's and the target's replicas in synchronous commit,
 * requires one synchronized secondary to commit and waits for the target to be SYNCHRONIZED, so a commit is acknowledged only once the target
 * has hardened it; {@link #promote} then runs {@code FORCE_FAILOVER_ALLOW_DATA_LOSS} on the target (which loses nothing, being synchronized)
 * and puts the required-secondaries setting back at once, since the new primary has no synchronized secondary yet and would otherwise refuse
 * access (error 988); {@link #demoteToReplica} resumes the old primary, which the group has by then made a secondary, and restores the modes.
 * There is nothing to stop beforehand: {@code SET (ROLE = SECONDARY)} on a live primary fails with error 41104 and changes nothing, and
 * what protects the data is the synchronous commit. Verified live, there and back under write load, with an aborted attempt (target
 * suspended) leaving nothing changed.
 *
 * <p>The old primary after a <i>crash</i> failover is another matter: when its instance starts, a replica of a group with no cluster manager
 * brings itself online as primary, so it is always a second primary, and no T-SQL demotes it. What works is to take its group offline (its
 * writes stop, and it stays RESOLVING), restart the instance (it then joins as a secondary) and resume data movement, which discards what it
 * committed that the new primary lacks. Warp cannot restart an instance, so {@link #rejoin} does that only with
 * {@code WARP_FAILOVER_REJOIN_COMMAND}; without it the node is reported and the stale-writer guard takes it offline. Verified live.
 * A non-readable
 * secondary rejects ordinary connections (error 978) so it is invisible to the probes until it
 * becomes the primary and starts accepting them.
 */
final class SqlServerHa implements EngineHa {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SqlServerHa.class);

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
        return connect(n, 8000);
    }

    private static Connection connect(BackendTarget n, int socketMillis) throws SQLException {
        Properties props = new Properties();
        if (n.user() != null) {
            props.setProperty("user", n.user());
            String pw = SecretResolver.resolve(n.password());
            props.setProperty("password", pw == null ? "" : pw);
        }
        props.setProperty("loginTimeout", "3");
        props.setProperty("socketTimeout", String.valueOf(socketMillis));
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

    @Override
    public java.util.Optional<java.math.BigInteger> writePosition(BackendTarget primary) throws SQLException {
        return lsn(primary, "last_commit_lsn");
    }

    @Override
    public java.util.Optional<java.math.BigInteger> appliedPosition(BackendTarget replica) throws SQLException {
        return lsn(replica, "last_commit_lsn");
    }

    private java.util.Optional<java.math.BigInteger> lsn(BackendTarget n, String column) throws SQLException {
        try (Connection c = connect(n); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT TOP 1 CAST(" + column + " AS decimal(25,0)) FROM sys.dm_hadr_database_replica_states "
                        + "WHERE is_local = 1 AND database_id = DB_ID()")) {
            return rs.next() && rs.getBigDecimal(1) != null ? java.util.Optional.of(rs.getBigDecimal(1).toBigInteger()) : java.util.Optional.empty();
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
        String command = System.getenv("WARP_FAILOVER_REJOIN_COMMAND");
        if (command != null && !command.isBlank()) {
            // The operator gave Warp a way to restart the instance, so the stale primary is not just stopped but brought back: the same
            // single flow (offline, restart, resume) as a rejoin, so the two never run side by side.
            rejoin(node, node);
            return;
        }
        takeGroupOffline(node);
    }

    /** {@code ALTER AVAILABILITY GROUP ... OFFLINE} for the read-scale group that holds the node's database (see {@link #fenceStaleWriter}). */
    private static void takeGroupOffline(BackendTarget node) throws SQLException {
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
        String db = dbNameOf(replica);
        if (db == null) {
            throw new SQLException("the backend URL has no databaseName, so the availability database is unknown");
        }
        String group;
        // From master: the availability database itself may not be queryable for a moment (right after a change of availability mode, or
        // while it is being redone), and ALTER AVAILABILITY GROUP has to run from master anyway.
        try (Connection c = connect(masterOf(replica), 90_000); Statement st = c.createStatement()) {
            Replica local = localReplica(c, db); // refuses a group a cluster manager owns, and a database in more than one group
            group = local.group();
            if ("PRIMARY".equalsIgnoreCase(local.role())) {
                throw new SQLException("this replica is already the primary");
            }
            st.execute("ALTER AVAILABILITY GROUP " + bracket(group) + " FORCE_FAILOVER_ALLOW_DATA_LOSS");
            // A planned switchover required one synchronized secondary so that nothing could be lost. The new primary has none yet
            // (the old one is still suspended), and with that requirement it refuses every access (error 988) until it has one: put the
            // original setting back at once, before waiting for it to become writable.
            Pending prepared = pendingForTarget(replica.jdbcUrl());
            if (prepared != null) {
                st.execute("ALTER AVAILABILITY GROUP " + bracket(group) + " SET (REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = "
                        + prepared.required() + ")");
            }
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

    private final java.util.Set<String> rejoinsRunning = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * What a returned old primary needs. A replica of an availability group with no cluster manager brings itself back online as
     * <b>primary</b> when its instance starts (the error log says "preparing to transition to the primary role"), so after a forced failover
     * the old primary is always a second primary, and no T-SQL demotes it: {@code SET (ROLE = SECONDARY)} on a live primary fails with
     * error 41104 and changes nothing. What does work (verified live) is to take its group offline, which stops its writes and leaves it
     * RESOLVING, and then restart the instance, after which it joins as a secondary of the real primary; data movement is then resumed,
     * and SQL Server rolls the node back to the common point, <b>discarding whatever it committed that the new primary lacks</b>.
     *
     * <p>Warp cannot restart an instance, so this is the SQL Server counterpart of the Postgres rebuild: with
     * {@code WARP_FAILOVER_REJOIN_COMMAND} set (a command that restarts the instance; it is given the node and the current primary in its
     * environment) Warp does offline, restart, wait, resume; without it the node is only reported, and the stale-writer guard takes it
     * offline.
     */
    @Override
    public RejoinResult rejoin(BackendTarget node, BackendTarget currentPrimary) throws SQLException {
        String db = dbNameOf(node);
        if (db == null) {
            return RejoinResult.of(RejoinOutcome.UNSUPPORTED, "the backend URL has no databaseName, so the availability database is unknown");
        }
        Replica local;
        try (Connection c = connect(masterOf(node))) {
            local = localReplica(c, db);
        }
        if ("SECONDARY".equalsIgnoreCase(local.role())) {
            try (Connection c = connect(masterOf(node))) {
                if (awaitSynchronizingMaster(c, db, 0)) {
                    return RejoinResult.of(RejoinOutcome.NOT_NEEDED, "already a synchronizing secondary");
                }
            }
            return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "it is a secondary with data movement suspended; review it, then run ALTER "
                    + "DATABASE " + bracket(db) + " SET HADR RESUME (this discards any transaction it holds that the new primary lacks)");
        }
        String command = System.getenv("WARP_FAILOVER_REJOIN_COMMAND");
        if (command == null || command.isBlank()) {
            return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "it is a " + local.role().toLowerCase(java.util.Locale.ROOT) + " of '"
                    + local.group() + "' that is not following the current primary. Bring it back as a secondary by taking the group offline "
                    + "(ALTER AVAILABILITY GROUP " + bracket(local.group()) + " OFFLINE), restarting the SQL Server instance and then running "
                    + "ALTER DATABASE " + bracket(db) + " SET HADR RESUME; this discards whatever it committed that the new primary lacks. "
                    + "Set WARP_FAILOVER_REJOIN_COMMAND to a command that restarts the instance to have Warp do all of it");
        }
        if (!rejoinsRunning.add(node.jdbcUrl())) {
            return RejoinResult.of(RejoinOutcome.NOT_NEEDED, "a rejoin of this node is already running");
        }
        try {
            if ("PRIMARY".equalsIgnoreCase(local.role())) {
                takeGroupOffline(node); // its writes stop here, and the restart brings it back RESOLVING instead of PRIMARY
            }
            String failure = RejoinCommand.run(command, node, currentPrimary, RejoinCommand.timeoutSeconds(), 1433, java.util.Map.of());
            if (failure != null) {
                return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, failure);
            }
            long deadline = System.currentTimeMillis() + 180_000;
            while (true) {
                try (Connection c = connect(masterOf(node), 90_000)) {
                    if ("SECONDARY".equalsIgnoreCase(localRole(c, db))) {
                        // a resume right after the restart can be accepted and do nothing yet: repeat it until the node is synchronizing
                        while (true) {
                            try {
                                resumeDatabase(c, db);
                                if (awaitSynchronizingMaster(c, db, 4_000)) {
                                    return RejoinResult.of(RejoinOutcome.REJOINED, "taken offline, restarted by the rejoin command and "
                                            + "resumed as a secondary of '" + local.group() + "'; whatever it committed that the new "
                                            + "primary lacked was discarded");
                                }
                            } catch (SQLException e) {
                                log.debug("sqlserver: resume not accepted yet: {}", e.getMessage());
                            }
                            if (System.currentTimeMillis() > deadline) {
                                return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "restarted, but it did not start synchronizing in time");
                            }
                            sleep(1000);
                        }
                    }
                } catch (SQLException e) {
                    // still restarting
                }
                if (System.currentTimeMillis() > deadline) {
                    return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "the rejoin command ran but the node did not come back as a "
                            + "secondary within 180s");
                }
                sleep(1000);
            }
        } finally {
            rejoinsRunning.remove(node.jdbcUrl());
        }
    }

    // ---- planned switchover (read-scale availability group, CLUSTER_TYPE = NONE) ----------------------------------------------

    /** What {@link #prepareSwitchover} changed, so that it can be put back. */
    private record Pending(String group, String primaryReplica, String targetReplica, String primaryMode, String targetMode,
            int required, String targetUrl, String database) {
    }

    private final java.util.concurrent.ConcurrentHashMap<String, Pending> pending = new java.util.concurrent.ConcurrentHashMap<>();

    private Pending pendingForTarget(String targetUrl) {
        for (Pending p : pending.values()) {
            if (p.targetUrl().equals(targetUrl)) {
                return p;
            }
        }
        return null;
    }

    /** The database named by the URL ({@code databaseName=} or {@code database=}), or null. */
    static String dbNameOf(BackendTarget n) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(?i)(?:databaseName|database)\\s*=\\s*([^;]+)")
                .matcher(n.jdbcUrl() == null ? "" : n.jdbcUrl());
        return m.find() ? m.group(1).trim() : null;
    }

    /** The same server with {@code master} as its database: a demoted availability database cannot be logged into (error 983), but the
     * group can still be managed, and its replica states read, from master. */
    static BackendTarget masterOf(BackendTarget n) {
        String url = n.jdbcUrl();
        String master = url.matches("(?i).*(databaseName|database)\\s*=.*")
                ? url.replaceAll("(?i)(databaseName|database)\\s*=\\s*[^;]+", "databaseName=master") : url + ";databaseName=master";
        return new BackendTarget(n.name(), master, n.user(), n.password(), n.failoverOptions(), n.fallbackName(), n.connectorOperand());
    }

    private record Replica(String group, String clusterType, String replicaName, String mode, int required, String role) {
    }

    /** This server's replica of the availability group that holds {@code db}, as seen from master. */
    private static Replica localReplica(Connection master, String db) throws SQLException {
        try (java.sql.PreparedStatement ps = master.prepareStatement(
                "SELECT ag.name, CAST(ag.cluster_type_desc AS varchar(20)), r.replica_server_name, "
                        + "CAST(r.availability_mode_desc AS varchar(30)), ag.required_synchronized_secondaries_to_commit, "
                        + "CAST(ars.role_desc AS varchar(20)) FROM sys.availability_groups ag "
                        + "JOIN sys.availability_databases_cluster adc ON adc.group_id = ag.group_id AND adc.database_name = ? "
                        + "JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1 "
                        + "JOIN sys.availability_replicas r ON r.replica_id = ars.replica_id")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("database '" + db + "' is not in an availability group on this server");
                }
                Replica r = new Replica(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getInt(5),
                        rs.getString(6));
                if (rs.next()) {
                    throw new SQLException("the database is in more than one availability group; refusing to guess");
                }
                if (!"NONE".equalsIgnoreCase(r.clusterType())) {
                    throw new SQLException("availability group '" + r.group() + "' is managed by a cluster manager ("
                            + r.clusterType() + "); fail it over with the cluster's own tooling");
                }
                return r;
            }
        }
    }

    private static String literal(String s) {
        return "N'" + s.replace("'", "''") + "'";
    }

    @Override
    public boolean supportsSwitchover() {
        return true;
    }

    /**
     * Puts the primary's replica and the target's in synchronous commit, requires one synchronized secondary to commit, and waits until the
     * target is SYNCHRONIZED: from then on a commit is acknowledged only once the target has hardened it, so the demotion that follows
     * cannot lose one. Undoes all of it and throws when the target does not synchronize in time.
     */
    @Override
    public void prepareSwitchover(BackendTarget primary, BackendTarget target) throws SQLException {
        String db = dbNameOf(primary);
        if (db == null) {
            throw new SQLException("the backend URL has no databaseName, so the availability database is unknown");
        }
        Replica p;
        Replica t;
        try (Connection pc = connect(masterOf(primary), 90_000); Connection tc = connect(masterOf(target))) {
            p = localReplica(pc, db);
            t = localReplica(tc, db);
            if (!"PRIMARY".equalsIgnoreCase(p.role())) {
                throw new SQLException("the current primary is not the primary replica of '" + p.group() + "'");
            }
            if (!"SECONDARY".equalsIgnoreCase(t.role()) || !t.group().equals(p.group())) {
                throw new SQLException("the target is not a secondary replica of '" + p.group() + "'");
            }
            Pending prepared = new Pending(p.group(), p.replicaName(), t.replicaName(), p.mode(), t.mode(), p.required(),
                    target.jdbcUrl(), db);
            if (pending.putIfAbsent(primary.jdbcUrl(), prepared) != null) {
                throw new SQLException("a switchover of this backend is already prepared or running");
            }
            try (Statement st = pc.createStatement()) {
                st.execute("ALTER AVAILABILITY GROUP " + bracket(p.group()) + " MODIFY REPLICA ON " + literal(p.replicaName())
                        + " WITH (AVAILABILITY_MODE = SYNCHRONOUS_COMMIT)");
                st.execute("ALTER AVAILABILITY GROUP " + bracket(p.group()) + " MODIFY REPLICA ON " + literal(t.replicaName())
                        + " WITH (AVAILABILITY_MODE = SYNCHRONOUS_COMMIT)");
                st.execute("ALTER AVAILABILITY GROUP " + bracket(p.group()) + " SET (REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1)");
            }
            long deadline = System.currentTimeMillis() + Long.parseLong(
                    System.getenv().getOrDefault("WARP_SWITCHOVER_SYNC_SECONDS", "30")) * 1000;
            while (!synchronizationState(tc, db).equalsIgnoreCase("SYNCHRONIZED")) {
                if (System.currentTimeMillis() > deadline) {
                    restoreModes(pc, prepared);
                    pending.remove(primary.jdbcUrl());
                    throw new SQLException("the target did not reach SYNCHRONIZED within the time allowed (state: "
                            + synchronizationState(tc, db) + "); modes restored");
                }
                sleep(500);
            }
        }
    }

    private static String synchronizationState(Connection master, String db) throws SQLException {
        try (java.sql.PreparedStatement ps = master.prepareStatement(
                "SELECT CAST(synchronization_state_desc AS varchar(30)) FROM sys.dm_hadr_database_replica_states "
                        + "WHERE is_local = 1 AND database_id = DB_ID(?)")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? String.valueOf(rs.getString(1)) : "UNKNOWN";
            }
        }
    }

    private static java.math.BigDecimal hardenedLsn(Connection master, String db) throws SQLException {
        try (java.sql.PreparedStatement ps = master.prepareStatement(
                "SELECT last_hardened_lsn FROM sys.dm_hadr_database_replica_states WHERE is_local = 1 AND database_id = DB_ID(?)")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBigDecimal(1) != null ? rs.getBigDecimal(1) : null;
            }
        }
    }

    /** Puts the availability and required-secondaries settings a switchover changed back as they were. */
    private static void restoreModes(Connection master, Pending p) throws SQLException {
        try (Statement st = master.createStatement()) {
            st.execute("ALTER AVAILABILITY GROUP " + bracket(p.group()) + " SET (REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = "
                    + p.required() + ")");
            st.execute("ALTER AVAILABILITY GROUP " + bracket(p.group()) + " MODIFY REPLICA ON " + literal(p.primaryReplica())
                    + " WITH (AVAILABILITY_MODE = " + p.primaryMode() + ")");
            st.execute("ALTER AVAILABILITY GROUP " + bracket(p.group()) + " MODIFY REPLICA ON " + literal(p.targetReplica())
                    + " WITH (AVAILABILITY_MODE = " + p.targetMode() + ")");
        }
    }

    /** The role of this server's replica as seen from master ({@code PRIMARY}, {@code SECONDARY}, {@code RESOLVING}). */
    private static String localRole(Connection master, String db) throws SQLException {
        try {
            return localReplica(master, db).role();
        } catch (SQLException e) {
            return "UNKNOWN";
        }
    }

    /**
     * There is nothing to stop on a SQL Server primary, and nothing to demote: {@code SET (ROLE = SECONDARY)} on a live primary fails with
     * error 41104 and changes nothing (verified live: the role stays PRIMARY and writes keep succeeding). What makes the switchover safe is
     * what {@link #prepareSwitchover} set up: with one synchronized secondary required to commit, a commit is acknowledged only after the
     * target has hardened it, so every acknowledged write is on the target; and once the target is promoted the old primary can commit
     * nothing (error 988, "lacks a quorum") until the group demotes it. A transaction that was still waiting for its acknowledgement then
     * is rolled back, which its client sees as an error, never as a success. This only checks that the preparation is still in place.
     */
    @Override
    public void freezeWrites(BackendTarget primary) throws SQLException {
        if (!pending.containsKey(primary.jdbcUrl())) {
            throw new SQLException("no switchover was prepared for this backend");
        }
    }

    /** The target is SYNCHRONIZED with the primary: it has hardened every commit the primary acknowledged. */
    @Override
    public void awaitCaughtUp(BackendTarget primary, BackendTarget replica, long timeoutSeconds) throws SQLException {
        String db = dbNameOf(primary);
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
        try (Connection rc = connect(masterOf(replica))) {
            while (!synchronizationState(rc, db).equalsIgnoreCase("SYNCHRONIZED")) {
                if (System.currentTimeMillis() > deadline) {
                    throw new SQLException("the target is " + synchronizationState(rc, db) + ", not SYNCHRONIZED");
                }
                sleep(300);
            }
        }
    }

    /** Aborting before the target was promoted: puts the availability modes and the required-secondaries setting back, on whichever of
     * the two is the primary now. Nothing was demoted or stopped, so nothing else needs undoing. */
    @Override
    public void unfreezeWrites(BackendTarget primary) throws SQLException {
        Pending p = pending.remove(primary.jdbcUrl());
        if (p == null) {
            return;
        }
        BackendTarget target = new BackendTarget(primary.name() + "-target", p.targetUrl(), primary.user(), primary.password(),
                primary.failoverOptions(), primary.fallbackName(), primary.connectorOperand());
        for (BackendTarget node : new BackendTarget[] {primary, target}) {
            try (Connection c = connect(masterOf(node), 90_000)) {
                if ("PRIMARY".equalsIgnoreCase(localRole(c, p.database()))) {
                    restoreModes(c, p);
                    return;
                }
            } catch (SQLException e) {
                // that node is unreachable: try the other
            }
        }
        throw new SQLException("neither node is the primary of '" + p.group() + "': the availability modes were left as the switchover "
                + "set them (synchronous commit, one required synchronized secondary)");
    }

    private static void resumeDatabase(Connection master, String db) throws SQLException {
        try (Statement st = master.createStatement()) {
            st.execute("ALTER DATABASE " + bracket(db) + " SET HADR RESUME");
        }
    }

    private static boolean awaitSynchronizingMaster(Connection master, String db, long timeoutMillis) throws SQLException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        do { // always looks at least once
            try (java.sql.PreparedStatement ps = master.prepareStatement(
                    "SELECT CAST(is_suspended AS int), CAST(synchronization_state_desc AS varchar(30)) "
                            + "FROM sys.dm_hadr_database_replica_states WHERE is_local = 1 AND database_id = DB_ID(?)")) {
                ps.setString(1, db);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt(1) == 0 && ("SYNCHRONIZING".equalsIgnoreCase(rs.getString(2))
                            || "SYNCHRONIZED".equalsIgnoreCase(rs.getString(2)))) {
                        return true;
                    }
                }
            }
            sleep(500);
        } while (System.currentTimeMillis() < deadline);
        return false;
    }

    private static void sleep(long millis) throws SQLException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted", e);
        }
    }

    /**
     * After the target has been promoted: resume the old primary (now a secondary) so it synchronizes from the new primary, then put the
     * availability modes back on the new primary. Returns true: the old primary replicates again.
     */
    @Override
    public boolean demoteToReplica(BackendTarget oldPrimary, BackendTarget newPrimary) throws SQLException {
        Pending p = pending.get(oldPrimary.jdbcUrl());
        if (p == null) {
            return false;
        }
        try (Connection oc = connect(masterOf(oldPrimary), 90_000)) {
            long deadline = System.currentTimeMillis() + 90_000;
            // The old primary is demoted by the group once the new primary has taken over, which takes a moment; until then a resume is
            // refused, or accepted and ignored: repeat it until the node is synchronizing.
            while (true) {
                try {
                    resumeDatabase(oc, p.database());
                    if (awaitSynchronizingMaster(oc, p.database(), 4_000)) {
                        break;
                    }
                } catch (SQLException e) {
                    log.debug("sqlserver: resume of the old primary not accepted yet: {}", e.getMessage());
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new SQLException("the old primary did not start synchronizing from the new primary within 90s");
                }
                sleep(1000);
            }
        }
        try (Connection nc = connect(masterOf(newPrimary), 90_000)) {
            restoreModes(nc, p);
        } finally {
            pending.remove(oldPrimary.jdbcUrl());
        }
        return true;
    }
}
