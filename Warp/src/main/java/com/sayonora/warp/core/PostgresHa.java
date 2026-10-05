package com.sayonora.warp.core;

import com.sayonora.warp.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalLong;
import java.util.Properties;

/** {@link EngineHa} for Postgres streaming replication. */
final class PostgresHa implements EngineHa {

    static final PostgresHa INSTANCE = new PostgresHa();

    private PostgresHa() {
    }

    @Override
    public ReplicaRouter.LagSample lag(BackendTarget replica, long nowMillis) {
        ReplicationLag.Result r = ReplicationLag.check(replica);
        return new ReplicaRouter.LagSample(r.ok(), r.isReplica(), r.lagSeconds(), r.message(), nowMillis);
    }

    @Override
    public FailoverMonitor.NodeRole role(BackendTarget node) {
        Properties props = new Properties();
        try {
            if (node.user() != null) {
                props.setProperty("user", node.user());
                String pw = SecretResolver.resolve(node.password());
                props.setProperty("password", pw == null ? "" : pw);
            }
        } catch (RuntimeException e) {
            return FailoverMonitor.NodeRole.UNREACHABLE;
        }
        props.setProperty("loginTimeout", "3");
        props.setProperty("socketTimeout", "5");
        props.setProperty("connectTimeout", "3");
        try (Connection c = DriverManager.getConnection(node.jdbcUrl(), props);
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select pg_is_in_recovery()")) {
            if (!rs.next()) {
                return FailoverMonitor.NodeRole.UNREACHABLE;
            }
            return rs.getBoolean(1) ? FailoverMonitor.NodeRole.READ_ONLY : FailoverMonitor.NodeRole.WRITABLE;
        } catch (SQLException e) {
            return FailoverMonitor.NodeRole.UNREACHABLE;
        }
    }

    private static Connection connect(BackendTarget n) throws SQLException {
        Properties props = new Properties();
        if (n.user() != null) {
            props.setProperty("user", n.user());
            String pw = SecretResolver.resolve(n.password());
            props.setProperty("password", pw == null ? "" : pw);
        }
        props.setProperty("loginTimeout", "5");
        props.setProperty("socketTimeout", "90");
        return DriverManager.getConnection(n.jdbcUrl(), props);
    }

    @Override
    public OptionalLong walPosition(BackendTarget n) throws SQLException {
        try (Connection c = connect(n); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select pg_wal_lsn_diff(greatest(pg_last_wal_receive_lsn(), "
                        + "pg_last_wal_replay_lsn()), '0/0')")) {
            if (rs.next() && rs.getBigDecimal(1) != null) {
                return OptionalLong.of(rs.getBigDecimal(1).longValue());
            }
            return OptionalLong.empty();
        }
    }

    @Override
    public void promote(BackendTarget n) throws SQLException {
        try (Connection c = connect(n); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select pg_promote(true, 60)")) {
            if (!rs.next() || !rs.getBoolean(1)) {
                throw new SQLException("pg_promote() did not complete within 60s");
            }
        }
    }
}
