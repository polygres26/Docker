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
 * <p><b>Opt-in control (also unverified).</b> With {@code WARP_ORACLE_DATAGUARD_CONTROL=true} Warp can also perform a <i>planned switchover</i>
 * and an automatic <i>failover</i> of a physical standby with SQL (see {@link #prepareSwitchover}, {@link #promote}); this has been written to
 * Oracle's documentation and checked only against a scripted fake database, never against a real Data Guard configuration, so it is off by default
 * and must be rehearsed in a staging environment first. It needs a SYSDBA account (the backend user, or {@code SYS AS SYSDBA}), a configuration
 * without the Data Guard Broker ({@code DG_BROKER_START=FALSE}: behind the broker's back the broker loses track), and redo transport already
 * configured in both directions.
 *
 * <p>What it does <b>not</b> do unless that is switched on: promote. A Data Guard failover is run by the Data Guard
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

    /** Whether Warp may drive Data Guard itself: {@code WARP_ORACLE_DATAGUARD_CONTROL=true}. Off by default; see the class comment. */
    static boolean controlEnabled() {
        Boolean o = controlOverride;
        return o != null ? o : "true".equalsIgnoreCase(System.getenv("WARP_ORACLE_DATAGUARD_CONTROL"));
    }

    private static volatile Boolean controlOverride;

    /** How long an aborted switchover waits for the old primary to be able to switch back; tests shorten it. */
    static volatile long unfreezeWaitMillis = 60_000;

    /** Tests only. */
    static void controlForTesting(Boolean enabled) {
        controlOverride = enabled;
    }

    @Override
    public boolean supportsPromote() {
        return controlEnabled();
    }

    @Override
    public boolean supportsSwitchover() {
        return controlEnabled();
    }

    /** Warp never promotes Oracle, so the "does a standby still hear from the primary" check has nothing to guard (the default, empty,
     * means no evidence). Stopping a stale Oracle primary taking writes is not attempted: a Data Guard primary cannot be demoted over
     * SQL and there is no live Oracle here to verify a restricted-session approach on. */
    @Override
    public void fenceStaleWriter(BackendTarget node) {
        throw new UnsupportedOperationException("Warp has no verified SQL to stop an Oracle primary taking writes; stop it or convert "
                + "it to a standby with Data Guard (DGMGRL / ALTER DATABASE CONVERT TO PHYSICAL STANDBY)");
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

    /** The highest SCN (change number) of the redo this standby has received: monotonic, so the standby with the most is the one that loses least. */
    @Override
    public OptionalLong walPosition(BackendTarget replica) throws SQLException {
        String v = sql.one(replica, false, "SELECT MAX(next_change#) FROM v$archived_log WHERE registrar IN ('RFS', 'SRMN')");
        return v == null || v.isBlank() ? OptionalLong.empty() : OptionalLong.of(new java.math.BigDecimal(v.trim()).longValue());
    }

    @Override
    public java.util.Optional<java.math.BigInteger> writePosition(BackendTarget primary) throws SQLException {
        return scn(primary);
    }

    @Override
    public java.util.Optional<java.math.BigInteger> appliedPosition(BackendTarget replica) throws SQLException {
        return scn(replica);
    }

    /** The database's current SCN: on a physical standby with apply running it is the SCN it has applied up to. */
    private java.util.Optional<java.math.BigInteger> scn(BackendTarget n) throws SQLException {
        String v = sql.one(n, false, "SELECT current_scn FROM v$database");
        return v == null || v.isBlank() ? java.util.Optional.empty() : java.util.Optional.of(new java.math.BigDecimal(v.trim()).toBigInteger());
    }

    // ---- driving Data Guard with SQL (opt-in, UNVERIFIED against a real Data Guard configuration) ---------------------------

    /** The statements a switchover and a failover are made of, run as SYSDBA. A seam so the sequence can be tested without a database. */
    interface Sql {
        /** The first column of the first row, or null. */
        String one(BackendTarget node, boolean sysdba, String query) throws SQLException;

        void exec(BackendTarget node, boolean sysdba, String statement) throws SQLException;
    }

    private static final Sql JDBC = new Sql() {
        private Connection open(BackendTarget n, boolean sysdba) throws SQLException {
            Properties props = new Properties();
            if (n.user() != null) {
                props.setProperty("user", n.user());
                String pw = SecretResolver.resolve(n.password());
                props.setProperty("password", pw == null ? "" : pw);
            }
            if (sysdba) {
                props.setProperty("internal_logon", "sysdba");
            }
            props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
            props.setProperty("oracle.jdbc.ReadTimeout", "120000"); // a switchover command waits for the end of redo to be applied
            return DriverManager.getConnection(n.jdbcUrl(), props);
        }

        @Override
        public String one(BackendTarget node, boolean sysdba, String query) throws SQLException {
            try (Connection c = open(node, sysdba); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(query)) {
                return rs.next() ? rs.getString(1) : null;
            }
        }

        @Override
        public void exec(BackendTarget node, boolean sysdba, String statement) throws SQLException {
            try (Connection c = open(node, sysdba); Statement st = c.createStatement()) {
                st.execute(statement);
            }
        }
    };

    static volatile Sql sql = JDBC;

    /** Tests only. */
    static void useSqlForTesting(Sql fake) {
        sql = fake == null ? JDBC : fake;
    }

    /** A switchover in progress, keyed by the primary's URL: how far it got, so an abort knows what to undo. */
    private record Switchover(String targetUrl, boolean demoted) {
    }

    private final java.util.concurrent.ConcurrentHashMap<String, Switchover> switchovers = new java.util.concurrent.ConcurrentHashMap<>();

    private static String databaseRole(BackendTarget n) throws SQLException {
        return String.valueOf(sql.one(n, true, "SELECT database_role FROM v$database")).trim().toUpperCase(java.util.Locale.ROOT);
    }

    private static String switchoverStatus(BackendTarget n) throws SQLException {
        return String.valueOf(sql.one(n, true, "SELECT switchover_status FROM v$database")).trim().toUpperCase(java.util.Locale.ROOT);
    }

    private static String openMode(BackendTarget n) throws SQLException {
        return String.valueOf(sql.one(n, true, "SELECT open_mode FROM v$database")).trim().toUpperCase(java.util.Locale.ROOT);
    }

    private static void requireBrokerOff(BackendTarget n) throws SQLException {
        String broker = sql.one(n, true, "SELECT value FROM v$parameter WHERE name = 'dg_broker_start'");
        if (broker != null && broker.trim().equalsIgnoreCase("TRUE")) {
            throw new SQLException(BackendSetModel.maskUrl(n.jdbcUrl()) + " runs the Data Guard Broker (DG_BROKER_START=TRUE): switch over "
                    + "with `dgmgrl SWITCHOVER TO ...`; a SQL switchover behind the broker's back leaves it with the wrong picture, and Warp "
                    + "follows the new primary once it is open READ WRITE");
        }
    }

    /**
     * Checks everything that can be checked without changing anything, so a switchover that cannot work is refused before the primary is
     * touched: the primary is a PRIMARY that can switch over (SWITCHOVER_STATUS {@code TO STANDBY} or {@code SESSIONS ACTIVE}), the target is a
     * physical standby, and neither runs the broker.
     */
    @Override
    public void prepareSwitchover(BackendTarget primary, BackendTarget target) throws SQLException {
        requireControl();
        if (!"PRIMARY".equals(databaseRole(primary))) {
            throw new SQLException("the current primary is not a PRIMARY database");
        }
        String status = switchoverStatus(primary);
        if (!status.equals("TO STANDBY") && !status.equals("SESSIONS ACTIVE")) {
            throw new SQLException("the primary cannot switch over now (SWITCHOVER_STATUS is " + status + "; it must be TO STANDBY or "
                    + "SESSIONS ACTIVE). Check that redo transport to the standby is running and has no errors");
        }
        if (!"PHYSICAL STANDBY".equals(databaseRole(target))) {
            throw new SQLException("the target is not a physical standby (DATABASE_ROLE is " + databaseRole(target) + ")");
        }
        requireBrokerOff(primary);
        requireBrokerOff(target);
        if (switchovers.putIfAbsent(primary.jdbcUrl(), new Switchover(target.jdbcUrl(), false)) != null) {
            throw new SQLException("a switchover of this backend is already prepared or running");
        }
    }

    private static void requireControl() {
        if (!controlEnabled()) {
            throw new UnsupportedOperationException("Warp does not drive Oracle Data Guard unless WARP_ORACLE_DATAGUARD_CONTROL=true");
        }
    }

    /**
     * The demotion: {@code COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH SESSION SHUTDOWN} ends every session, flushes the redo, sends
     * the end-of-redo marker to the standby and turns this database into a (mounted) physical standby. Nothing can write to it afterwards.
     */
    @Override
    public void freezeWrites(BackendTarget primary) throws SQLException {
        Switchover s = switchovers.get(primary.jdbcUrl());
        if (s == null) {
            throw new SQLException("no switchover was prepared for this backend");
        }
        log.warn("oracle: switching {} over to a physical standby -- Oracle control is not verified against a real Data Guard "
                + "configuration", BackendSetModel.maskUrl(primary.jdbcUrl()));
        sql.exec(primary, true, "ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH SESSION SHUTDOWN");
        switchovers.put(primary.jdbcUrl(), new Switchover(s.targetUrl(), true));
    }

    /** The standby has applied everything up to the end-of-redo marker: SWITCHOVER_STATUS {@code TO PRIMARY}. */
    @Override
    public void awaitCaughtUp(BackendTarget primary, BackendTarget replica, long timeoutSeconds) throws SQLException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
        String status = switchoverStatus(replica);
        while (!status.equals("TO PRIMARY")) {
            if (status.equals("RECOVERY NEEDED")) {
                try { // redo apply is not running: start it so the end-of-redo marker can be applied
                    sql.exec(replica, true, "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT FROM SESSION");
                } catch (SQLException e) {
                    if (!String.valueOf(e.getMessage()).contains("ORA-01153")) { // already running
                        throw e;
                    }
                }
            }
            if (System.currentTimeMillis() > deadline) {
                throw new SQLException("the standby's SWITCHOVER_STATUS is " + status + ", not TO PRIMARY");
            }
            sleep(1000);
            status = switchoverStatus(replica);
        }
    }

    /**
     * Makes the standby the primary: {@code COMMIT TO SWITCHOVER TO PRIMARY} when it has the end-of-redo marker applied (a planned
     * switchover), else a failover ({@code ALTER DATABASE FAILOVER TO <db_unique_name>}, 12.1 and later), which applies whatever redo it has
     * received, discards the rest and cannot be undone; a failed primary that comes back has to be reinstated or rebuilt. Then the database
     * is opened if it is only mounted, and the call returns once it is open READ WRITE.
     */
    @Override
    public void promote(BackendTarget replica) throws SQLException {
        requireControl();
        if (!"PHYSICAL STANDBY".equals(databaseRole(replica))) {
            throw new SQLException("this database is not a physical standby (DATABASE_ROLE is " + databaseRole(replica) + ")");
        }
        requireBrokerOff(replica);
        String status = switchoverStatus(replica);
        if (status.equals("TO PRIMARY") || status.equals("SWITCHOVER PENDING")) {
            sql.exec(replica, true, "ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN");
        } else {
            String unique = sql.one(replica, true, "SELECT db_unique_name FROM v$database");
            if (unique == null || !unique.matches("[A-Za-z0-9_$#]+")) {
                throw new SQLException("cannot read a usable DB_UNIQUE_NAME to fail over to");
            }
            log.warn("oracle: failing {} over (ALTER DATABASE FAILOVER TO {}) -- irreversible, and not verified against a real Data Guard "
                    + "configuration", BackendSetModel.maskUrl(replica.jdbcUrl()), unique);
            sql.exec(replica, true, "ALTER DATABASE FAILOVER TO " + unique);
        }
        if (!openMode(replica).startsWith("READ WRITE")) {
            sql.exec(replica, true, "ALTER DATABASE OPEN");
        }
        long deadline = System.currentTimeMillis() + 60_000;
        while (!(databaseRole(replica).equals("PRIMARY") && openMode(replica).equals("READ WRITE"))) {
            if (System.currentTimeMillis() > deadline) {
                throw new SQLException("the new primary did not open READ WRITE within 60s");
            }
            sleep(500);
        }
    }

    /** After the standby took over: the old primary (now a mounted standby) is opened read only and redo apply is started. */
    @Override
    public boolean demoteToReplica(BackendTarget oldPrimary, BackendTarget newPrimary) throws SQLException {
        Switchover s = switchovers.get(oldPrimary.jdbcUrl());
        if (s == null || !s.demoted()) {
            return false;
        }
        try {
            if (openMode(oldPrimary).equals("MOUNTED")) {
                sql.exec(oldPrimary, true, "ALTER DATABASE OPEN");
            }
            try {
                sql.exec(oldPrimary, true, "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT FROM SESSION");
            } catch (SQLException e) {
                if (!String.valueOf(e.getMessage()).contains("ORA-01153")) { // redo apply already running
                    throw e;
                }
            }
            return true;
        } finally {
            switchovers.remove(oldPrimary.jdbcUrl());
        }
    }

    /**
     * Aborting before the standby was promoted. Nothing was changed when the old primary had not been switched yet. When it had, it is a
     * standby now: redo apply is started if it is not running, and once it reports it can become the primary again it is switched back and
     * opened. If that does not work within the time allowed this <b>throws</b>, saying the old primary is a standby: the caller must not
     * report writes as re-enabled, and an operator has to finish it (for example with {@code dgmgrl} or the same statements by hand).
     */
    @Override
    public void unfreezeWrites(BackendTarget primary) throws SQLException {
        Switchover s = switchovers.remove(primary.jdbcUrl());
        if (s == null || !s.demoted()) {
            return;
        }
        long deadline = System.currentTimeMillis() + unfreezeWaitMillis;
        String status = switchoverStatus(primary);
        while (!status.equals("TO PRIMARY") && !status.equals("SWITCHOVER PENDING")) {
            if (status.equals("RECOVERY NEEDED") || status.equals("NOT ALLOWED")) {
                try {
                    sql.exec(primary, true, "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT FROM SESSION");
                } catch (SQLException e) {
                    if (!String.valueOf(e.getMessage()).contains("ORA-01153")) {
                        throw e;
                    }
                }
            }
            if (System.currentTimeMillis() > deadline) {
                throw new SQLException("the old primary was switched to a standby for a switchover that was aborted, and could not be switched "
                        + "back (its SWITCHOVER_STATUS is " + status + "): it is a physical standby now and there is no primary; finish it by "
                        + "hand with ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY on it (or fail the other standby over)");
            }
            sleep(1000);
            status = switchoverStatus(primary);
        }
        sql.exec(primary, true, "ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN");
        if (!openMode(primary).startsWith("READ WRITE")) {
            sql.exec(primary, true, "ALTER DATABASE OPEN");
        }
    }

    private static void sleep(long millis) throws SQLException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted", e);
        }
    }
}
