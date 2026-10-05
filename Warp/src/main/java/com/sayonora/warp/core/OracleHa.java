package com.sayonora.warp.core;

import com.sayonora.warp.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EngineHa} for an Oracle Data Guard <b>physical standby opened read-only with redo apply</b>
 * (Active Data Guard). <b>This implementation is written to Oracle's documentation and has NOT been
 * exercised against a live Data Guard configuration</b> -- Data Guard is not available in the free
 * editions Warp's test setup can run -- so only its pure decision logic ({@link #roleFrom},
 * {@link #lagFrom}, {@link #parseIntervalSeconds}) is unit-tested.
 *
 * <p>What it does: reads the node's role and open mode from {@code V$DATABASE} and the standby's
 * apply/transport lag from {@code V$DATAGUARD_STATS}. The Warp user needs {@code SELECT} on
 * {@code V_$DATABASE} and {@code V_$DATAGUARD_STATS}.
 *
 * <p>What it deliberately does <b>not</b> do: promote. A Data Guard failover is run by the Data Guard
 * Broker (Fast-Start Failover, or {@code dgmgrl FAILOVER}) or a DBA; getting it wrong loses data and
 * cannot be rolled back, and Warp cannot verify its own implementation here. Warp therefore only
 * <i>follows</i>: once the broker has opened the standby as the new primary (role PRIMARY, open mode
 * READ WRITE), failover-follow repoints the backend at it. A standby that is only MOUNTED (no Active
 * Data Guard) cannot be probed with an ordinary login, so it is neither used for reads nor seen as a
 * replica -- but it is picked up the moment it opens read-write as the new primary.
 */
final class OracleHa implements EngineHa {

    private static final Logger log = LoggerFactory.getLogger(OracleHa.class);

    static final OracleHa INSTANCE = new OracleHa();

    /** Data Guard statistics older than this are not trusted. */
    private static final double MAX_STATS_AGE_SECONDS = 120;

    private static final Pattern INTERVAL = Pattern.compile("^([+-])?(\\d+) (\\d+):(\\d+):(\\d+)(?:\\.(\\d+))?$");

    private OracleHa() {
    }

    @Override
    public boolean supportsPromote() {
        return false;
    }

    // ---- pure logic ---------------------------------------------------------------------------

    /** {@code +DD HH:MI:SS[.fff]} (Oracle's INTERVAL DAY TO SECOND text) to seconds; null if not that shape. */
    static Double parseIntervalSeconds(String text) {
        if (text == null) {
            return null;
        }
        Matcher m = INTERVAL.matcher(text.trim());
        if (!m.matches()) {
            return null;
        }
        double s = Long.parseLong(m.group(2)) * 86400.0 + Long.parseLong(m.group(3)) * 3600.0
                + Long.parseLong(m.group(4)) * 60.0 + Long.parseLong(m.group(5));
        if (m.group(6) != null) {
            s += Double.parseDouble("0." + m.group(6));
        }
        return "-".equals(m.group(1)) ? -s : s;
    }

    /** WRITABLE only for a PRIMARY that is open READ WRITE; anything else that answered is READ_ONLY. */
    static FailoverMonitor.NodeRole roleFrom(String databaseRole, String openMode) {
        if (databaseRole != null && databaseRole.trim().equalsIgnoreCase("PRIMARY")
                && openMode != null && openMode.trim().equalsIgnoreCase("READ WRITE")) {
            return FailoverMonitor.NodeRole.WRITABLE;
        }
        return FailoverMonitor.NodeRole.READ_ONLY;
    }

    /** Lag decision from what {@code V$DATABASE} / {@code V$DATAGUARD_STATS} reported. {@code
     * statsAgeSeconds} is how old the apply-lag figure is (database clock). Anything that cannot be
     * trusted yields {@code ok=false}: never a made-up 0. */
    static ReplicaRouter.LagSample lagFrom(String databaseRole, String openMode, String applyLag,
            String transportLag, Double statsAgeSeconds, long now) {
        if (databaseRole == null) {
            return new ReplicaRouter.LagSample(false, false, 0, "database role unknown", now);
        }
        String role = databaseRole.trim().toUpperCase(java.util.Locale.ROOT);
        if (role.equals("PRIMARY")) {
            return new ReplicaRouter.LagSample(true, false, 0, "not a standby", now);
        }
        if (!role.contains("STANDBY")) {
            return new ReplicaRouter.LagSample(false, false, 0, "unexpected database role " + databaseRole, now);
        }
        if (openMode == null || !openMode.trim().toUpperCase(java.util.Locale.ROOT).startsWith("READ ONLY")) {
            return new ReplicaRouter.LagSample(false, true, 0,
                    "standby open mode is " + openMode + " -- serving reads needs Active Data Guard", now);
        }
        Double apply = parseIntervalSeconds(applyLag);
        if (apply == null) {
            return new ReplicaRouter.LagSample(false, true, 0,
                    "apply lag unavailable (is managed recovery running?)", now);
        }
        if (statsAgeSeconds == null || statsAgeSeconds > MAX_STATS_AGE_SECONDS) {
            return new ReplicaRouter.LagSample(false, true, 0, "Data Guard lag statistics are stale", now);
        }
        Double transport = parseIntervalSeconds(transportLag);
        double lag = Math.max(apply, transport == null ? 0 : transport);
        return new ReplicaRouter.LagSample(true, true, Math.max(0, lag), null, now);
    }

    // ---- JDBC ---------------------------------------------------------------------------------

    private static Connection connect(BackendTarget n) throws SQLException {
        Properties props = new Properties();
        if (n.user() != null) {
            props.setProperty("user", n.user());
            String pw = SecretResolver.resolve(n.password());
            props.setProperty("password", pw == null ? "" : pw);
        }
        props.setProperty("oracle.net.CONNECT_TIMEOUT", "3000");
        props.setProperty("oracle.jdbc.ReadTimeout", "8000");
        return DriverManager.getConnection(n.jdbcUrl(), props);
    }

    @Override
    public FailoverMonitor.NodeRole role(BackendTarget node) {
        try (Connection c = connect(node)) {
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT database_role, open_mode FROM v$database")) {
                if (!rs.next()) {
                    return FailoverMonitor.NodeRole.UNREACHABLE;
                }
                return roleFrom(rs.getString(1), rs.getString(2));
            } catch (SQLException e) {
                // ORA-00942 / ORA-01031: connected, but not allowed to look. The probe cannot vouch
                // for this node either way, so it never reports writable.
                log.warn("oracle role probe of {} could not read V$DATABASE ({}); grant SELECT on V_$DATABASE "
                        + "to the Warp user", BackendSetModel.maskUrl(node.jdbcUrl()), e.getMessage());
                return FailoverMonitor.NodeRole.UNREACHABLE;
            }
        } catch (SQLException | RuntimeException e) {
            return FailoverMonitor.NodeRole.UNREACHABLE;
        }
    }

    @Override
    public ReplicaRouter.LagSample lag(BackendTarget replica, long now) {
        String sql = "SELECT d.database_role, d.open_mode, "
                + "(SELECT value FROM v$dataguard_stats WHERE name = 'apply lag'), "
                + "(SELECT value FROM v$dataguard_stats WHERE name = 'transport lag'), "
                + "(SELECT (SYSDATE - TO_DATE(datum_time, 'MM/DD/YYYY HH24:MI:SS')) * 86400 "
                + "FROM v$dataguard_stats WHERE name = 'apply lag') FROM v$database d";
        try (Connection c = connect(replica); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                return new ReplicaRouter.LagSample(false, false, 0, "no row from V$DATABASE", now);
            }
            double age = rs.getDouble(5);
            Double ageOrNull = rs.wasNull() ? null : age;
            return lagFrom(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), ageOrNull, now);
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
        throw new UnsupportedOperationException("Warp does not promote Oracle Data Guard standbys: run the "
                + "failover with the Data Guard Broker (Fast-Start Failover or `dgmgrl FAILOVER TO ...`); Warp "
                + "follows once the new primary is open READ WRITE");
    }
}
