package com.sayonora.warp.core;

import com.sayonora.warp.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalLong;
import java.util.Properties;

/**
 * {@link EngineHa} for SQL Server <b>Always On Availability Groups</b> with a readable secondary.
 * <b>Written to Microsoft's documentation and NOT exercised against a live Availability Group</b>
 * (none can be run in this project's test setup), so only the pure decision logic ({@link #roleFrom},
 * {@link #lagFrom}) is unit-tested.
 *
 * <p>Setup it assumes: the Warp backend URL names the availability-group database
 * ({@code databaseName=...}); replica URLs point at readable secondaries (add
 * {@code applicationIntent=ReadOnly} so the connection is accepted); the Warp user has
 * {@code VIEW SERVER STATE} (for {@code sys.dm_hadr_database_replica_states}) and can connect to the
 * database. {@code ;} inside a URL is written {@code %3B} in the {@code WARP_BACKENDS} spec.
 *
 * <p>Like Oracle, Warp only <b>follows</b> a failover run elsewhere (the cluster's automatic failover,
 * or {@code ALTER AVAILABILITY GROUP ... FAILOVER} by a DBA); it never performs one. A non-readable
 * secondary rejects ordinary connections (error 978) so it is invisible to the probes until it
 * becomes the primary and starts accepting them.
 */
final class SqlServerHa implements EngineHa {

    static final SqlServerHa INSTANCE = new SqlServerHa();

    private SqlServerHa() {
    }

    @Override
    public boolean supportsPromote() {
        return false;
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

    @Override
    public OptionalLong walPosition(BackendTarget replica) {
        return OptionalLong.empty();
    }

    @Override
    public void promote(BackendTarget replica) {
        throw new UnsupportedOperationException("Warp does not fail over SQL Server Availability Groups: use the "
                + "cluster's automatic failover or `ALTER AVAILABILITY GROUP ... FAILOVER`; Warp follows once the "
                + "new primary accepts connections");
    }
}
