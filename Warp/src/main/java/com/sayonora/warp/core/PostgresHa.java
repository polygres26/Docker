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

    @Override
    public boolean supportsRepoint() {
        return true;
    }

    private static final long STREAM_WAIT_SECONDS = Long.parseLong(
            System.getenv().getOrDefault("WARP_FAILOVER_REPOINT_WAIT_SECONDS", "30"));

    /**
     * Rewrites the replica's {@code primary_conninfo} to name the new primary's host and port (every
     * other setting -- replication user, password, sslmode -- is kept), reloads, and waits for the WAL
     * receiver to stream from it. If it does not, the old setting is restored and this throws.
     *
     * <p>Needs a superuser (or ALTER SYSTEM + pg_reload_conf privileges plus pg_read_all_settings) on the
     * replica. If {@code primary_slot_name} is set, the slot is created on the new primary when missing.
     * The host/port come from the new primary's JDBC URL, so the replica must be able to reach that same
     * address. A replica that is ahead of the new primary (it received WAL the promoted node did not)
     * cannot follow it; that surfaces as "not streaming" and the replica is left as it was.
     */
    @Override
    public void repoint(BackendTarget replica, BackendTarget newPrimary) throws Exception {
        JdbcHostPort hp = JdbcHostPort.parse(newPrimary.jdbcUrl(), 5432);
        try (Connection c = connect(replica)) {
            c.setAutoCommit(true);
            String oldInfo = setting(c, "primary_conninfo");
            if (oldInfo == null || oldInfo.isBlank()) {
                throw new SQLException("replica has no primary_conninfo (not a streaming standby, or the Warp "
                        + "user cannot read the setting)");
            }
            String slot = setting(c, "primary_slot_name");
            if (slot != null && !slot.isBlank()) {
                ensureSlot(newPrimary, slot);
            }
            String newInfo = PgConninfo.withHostPort(oldInfo, hp.host(), hp.port());
            alterConninfo(c, newInfo);
            if (!waitStreaming(c, hp.host())) {
                alterConninfo(c, oldInfo);
                throw new SQLException("replica did not start streaming from " + hp.host() + ":" + hp.port()
                        + " within " + STREAM_WAIT_SECONDS + "s (it may be ahead of the new primary); "
                        + "primary_conninfo restored");
            }
        }
    }

    private static String setting(Connection c, String name) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("show " + name)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static void alterConninfo(Connection c, String conninfo) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("alter system set primary_conninfo = '" + conninfo.replace("'", "''") + "'");
            st.execute("select pg_reload_conf()");
        }
    }

    private static void ensureSlot(BackendTarget newPrimary, String slot) throws SQLException {
        try (Connection c = connect(newPrimary);
                java.sql.PreparedStatement ps = c.prepareStatement(
                        "select pg_create_physical_replication_slot(?) where not exists "
                                + "(select 1 from pg_replication_slots where slot_name = ?)")) {
            ps.setString(1, slot);
            ps.setString(2, slot);
            ps.execute();
        }
    }

    private static boolean waitStreaming(Connection c, String host) throws SQLException {
        long deadline = System.currentTimeMillis() + STREAM_WAIT_SECONDS * 1000;
        while (System.currentTimeMillis() < deadline) {
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("select status, conninfo from pg_stat_wal_receiver")) {
                if (rs.next() && "streaming".equals(rs.getString(1))) {
                    String ci = rs.getString(2);
                    if (ci != null && PgConninfo.parse(ci).stream()
                            .anyMatch(kv -> kv[0].equals("host") && kv[1].equals(host))) {
                        return true;
                    }
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

    /** {@code pg_stat_wal_receiver}: the standby's WAL receiver is streaming and its sender is {@code primary}. On a crash the row
     * disappears at once; an idle healthy primary still sends something about every 30 s (wal_sender_timeout / 2), which is why callers
     * compare the age against a threshold well above that. */
    @Override
    public OptionalDouble heardFromPrimarySecondsAgo(BackendTarget replica, BackendTarget primary) throws SQLException {
        JdbcHostPort want = JdbcHostPort.parse(primary.jdbcUrl(), 5432);
        try (Connection c = connect(replica); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select status, sender_host, sender_port, "
                        + "extract(epoch from now() - last_msg_receipt_time) from pg_stat_wal_receiver")) {
            while (rs.next()) {
                if (!"streaming".equalsIgnoreCase(rs.getString(1)) || rs.getObject(4) == null) {
                    continue;
                }
                if (rs.getInt(3) == want.port() && HaHosts.sameHost(rs.getString(2), want.host())) {
                    return OptionalDouble.of(rs.getDouble(4));
                }
            }
            return OptionalDouble.empty();
        }
    }

    /** A fresh session on a node that is not in recovery reports {@code transaction_read_only = on} when {@code default_transaction_read_only}
     * is set cluster-wide, which is how both a switchover and the stale-writer guard freeze a node. */
    @Override
    public boolean writesFrozen(BackendTarget node) throws SQLException {
        try (Connection c = connect(node); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select current_setting('transaction_read_only'), pg_is_in_recovery()")) {
            return rs.next() && "on".equalsIgnoreCase(rs.getString(1)) && !rs.getBoolean(2);
        }
    }

    @Override
    public boolean supportsSwitchover() {
        return true;
    }

    /**
     * Sets {@code default_transaction_read_only = on} cluster-wide (ALTER SYSTEM + reload) and terminates
     * every other client session so nothing keeps a pre-freeze read-write transaction open. A client that
     * explicitly runs {@code SET transaction_read_only = off} after reconnecting could still write; that is
     * why the switchover then verifies the replica caught up to the primary's final WAL position.
     * Needs a superuser.
     */
    @Override
    public void freezeWrites(BackendTarget primary) throws SQLException {
        try (Connection c = connect(primary); Statement st = c.createStatement()) {
            c.setAutoCommit(true);
            st.execute("alter system set default_transaction_read_only = on");
            st.execute("select pg_reload_conf()");
            st.execute("select pg_terminate_backend(pid) from pg_stat_activity where pid <> pg_backend_pid() "
                    + "and backend_type = 'client backend'");
        }
    }

    @Override
    public void unfreezeWrites(BackendTarget primary) throws SQLException {
        try (Connection c = connect(primary); Statement st = c.createStatement()) {
            c.setAutoCommit(true);
            st.execute("alter system reset default_transaction_read_only");
            st.execute("select pg_reload_conf()");
        }
    }

    @Override
    public void awaitCaughtUp(BackendTarget primary, BackendTarget replica, long timeoutSeconds) throws SQLException {
        String target;
        try (Connection c = connect(primary); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select pg_current_wal_lsn()::text")) {
            rs.next();
            target = rs.getString(1);
        }
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
        try (Connection c = connect(replica)) {
            while (true) {
                try (java.sql.PreparedStatement ps = c.prepareStatement(
                        "select pg_last_wal_replay_lsn() >= ?::pg_lsn")) {
                    ps.setString(1, target);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next() && rs.getBoolean(1)) {
                            return;
                        }
                    }
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new SQLException("replica did not replay up to " + target + " within " + timeoutSeconds + "s");
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("interrupted", e);
                }
            }
        }
    }
    // demoteToReplica stays false: turning a running primary into a standby needs a restart with
    // standby.signal (and usually pg_rewind) on the host, which Warp cannot do over SQL.

    /** Postgres cannot demote a running primary over SQL. Still in recovery means nothing to do. A standalone
     * primary needs a host-side rebuild (stop it, pg_rewind or pg_basebackup, start it as a standby): Warp has no
     * host access, so it runs the operator's {@code WARP_FAILOVER_REJOIN_COMMAND} for that when one is set (see
     * {@code scripts/pg-rebuild-standby.sh}), and otherwise only reports that a rebuild is needed. */
    @Override
    public RejoinResult rejoin(BackendTarget node, BackendTarget currentPrimary) throws SQLException {
        try (Connection c = connect(node); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select pg_is_in_recovery()")) {
            rs.next();
            if (rs.getBoolean(1)) {
                return RejoinResult.of(RejoinOutcome.NOT_NEEDED, "already a standby");
            }
        }
        String command = System.getenv("WARP_FAILOVER_REJOIN_COMMAND");
        if (command == null || command.isBlank()) {
            return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "it is running as a standalone primary; Warp cannot "
                    + "turn a running Postgres primary into a standby -- stop it and rebuild it from the current "
                    + "primary with pg_rewind or pg_basebackup (or set WARP_FAILOVER_REJOIN_COMMAND to have Warp run "
                    + "that for you)");
        }
        long timeout = 120;
        String t = System.getenv("WARP_FAILOVER_REJOIN_TIMEOUT_SECONDS");
        if (t != null && !t.isBlank()) {
            timeout = Long.parseLong(t.trim());
        }
        return runRejoinCommand(command, node, currentPrimary, timeout);
    }

    /** Runs the operator's rebuild command with the node and the current primary described in its environment
     * (WARP_REJOIN_NODE_URL, WARP_REJOIN_PRIMARY_URL / _HOST / _PORT / _USER, and PGPASSWORD for the primary), then
     * checks the node really came back as a standby. */
    static RejoinResult runRejoinCommand(String command, BackendTarget node, BackendTarget primary, long timeoutSeconds) {
        try {
            ProcessBuilder pb = new ProcessBuilder("sh", "-c", command).redirectErrorStream(true);
            java.util.Map<String, String> env = pb.environment();
            env.put("WARP_REJOIN_NODE_URL", node.jdbcUrl());
            env.put("WARP_REJOIN_PRIMARY_URL", primary.jdbcUrl());
            java.net.URI u = java.net.URI.create(primary.jdbcUrl().substring("jdbc:".length()));
            env.put("WARP_REJOIN_PRIMARY_HOST", u.getHost());
            env.put("WARP_REJOIN_PRIMARY_PORT", String.valueOf(u.getPort() < 0 ? 5432 : u.getPort()));
            env.put("WARP_REJOIN_PRIMARY_USER", primary.user() == null ? "" : primary.user());
            String pw = SecretResolver.resolve(primary.password());
            if (pw != null) {
                env.put("PGPASSWORD", pw);
            }
            Process p = pb.start();
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            Thread drain = new Thread(() -> {
                try {
                    p.getInputStream().transferTo(out);
                } catch (java.io.IOException ignored) {
                    // process ended
                }
            }, "warp-rejoin-output");
            drain.setDaemon(true);
            drain.start();
            if (!p.waitFor(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "the rejoin command did not finish within "
                        + timeoutSeconds + "s and was stopped");
            }
            drain.join(1000);
            String tail = tail(out.toString());
            if (p.exitValue() != 0) {
                return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "the rejoin command failed (exit "
                        + p.exitValue() + "): " + tail);
            }
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                try (Connection c = connect(node); Statement st = c.createStatement();
                        ResultSet rs = st.executeQuery("select pg_is_in_recovery()")) {
                    rs.next();
                    if (rs.getBoolean(1)) {
                        return RejoinResult.of(RejoinOutcome.REJOINED, "rebuilt as a standby of the current primary by "
                                + "the rejoin command");
                    }
                } catch (SQLException e) {
                    // still restarting
                }
                Thread.sleep(500);
            }
            return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "the rejoin command succeeded but the node is not a "
                    + "standby: " + tail);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "interrupted while running the rejoin command");
        } catch (Exception e) {
            return RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "could not run the rejoin command: " + e.getMessage());
        }
    }

    private static String tail(String s) {
        String t = s.strip().replaceAll("\\s+", " ");
        return t.length() > 300 ? t.substring(t.length() - 300) : t;
    }
}
