package com.nexagres.wire.core;

import com.nexagres.wire.secrets.SecretResolver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How far behind a fallback backend is, measured on the fallback itself via {@code
 * pg_is_in_recovery()}/{@code pg_last_xact_replay_timestamp()} -- no connection to the primary
 * needed, and no dependency on matching a {@code pg_stat_replication} row by client address.
 * Backs two switchover/failover behaviors:
 *
 * <ul>
 *   <li>{@code BackendHealthChecker} (unplanned failover): checked once a primary is found
 *       unreachable, purely to log whether the automatic redirect to its fallback stayed within
 *       {@code POLYWIRE_FAILOVER_MAX_LAG_SECONDS} -- the "acceptable data loss" window for an
 *       outage nobody chose the timing of. This is advisory, not a gate: the primary is already
 *       down, so refusing to redirect would only turn a bounded-data-loss failover into a total
 *       outage, which is never the better trade.
 *   <li>The admin drain route (planned switchover): {@code MetricsServer} polls this and BLOCKS
 *       (bounded by the same grace period the pool drain already uses) until lag reaches zero
 *       before reporting drain complete -- unlike an unplanned outage, a planned maintenance
 *       window can afford to wait for a real zero-data-loss cutover, and should.
 * </ul>
 *
 * <p>Only meaningful for a genuine Postgres streaming replica ({@code pg_is_in_recovery()} true).
 * A fallback that ISN'T a replica of its primary -- e.g. a cross-region backend that's simply
 * another independent target, exactly as {@code BackendTarget.fallbackName}'s javadoc describes as
 * an equally valid case -- has no meaningful "lag" to measure; {@link #check} returns {@link
 * Result#NOT_A_REPLICA} rather than guessing, and both call sites above treat that the same as "no
 * lag information available, proceed" (there's nothing more to wait for or warn about).
 */
public final class ReplicationLag {

    private static final Logger log = LoggerFactory.getLogger(ReplicationLag.class);
    private static final int LOGIN_TIMEOUT_SECONDS = 5;

    /** {@code lagSeconds} is meaningless (0) when {@code isReplica} is false. A replica that's
     * caught up with zero transactions replayed yet reports {@code lagSeconds = 0} -- there's
     * nothing to be behind on. */
    public record Result(boolean ok, boolean isReplica, double lagSeconds, String message) {
        public static final Result NOT_A_REPLICA = new Result(true, false, 0, "not a streaming replica");

        public static Result unreachable(String message) {
            return new Result(false, false, 0, message);
        }
    }

    private ReplicationLag() {
    }

    public static Result check(BackendTarget target) {
        String resolvedPassword;
        try {
            resolvedPassword = SecretResolver.resolve(target.password());
        } catch (RuntimeException e) {
            return Result.unreachable("secret resolution failed: " + e.getMessage());
        }
        Properties props = new Properties();
        if (target.user() != null) {
            props.setProperty("user", target.user());
            props.setProperty("password", resolvedPassword == null ? "" : resolvedPassword);
        }
        props.setProperty("loginTimeout", String.valueOf(LOGIN_TIMEOUT_SECONDS));
        try (Connection conn = DriverManager.getConnection(target.jdbcUrl(), props);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT pg_is_in_recovery(), "
                                + "EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))")) {
            if (!rs.next()) {
                return Result.unreachable("no row returned from lag probe");
            }
            boolean inRecovery = rs.getBoolean(1);
            if (!inRecovery) {
                return Result.NOT_A_REPLICA;
            }
            double lag = rs.getDouble(2);
            // NULL (no transaction replayed yet on a fresh replica) reads as 0.0 via getDouble --
            // correct here: there's nothing yet to be behind on.
            return new Result(true, true, rs.wasNull() ? 0.0 : lag, null);
        } catch (SQLException e) {
            return Result.unreachable(e.getMessage());
        }
    }

    /** Polls {@link #check} against {@code fallback} until its lag reaches (or was never above)
     * {@code maxAcceptableLagSeconds}, or {@code timeoutMillis} elapses -- used by the drain route
     * to wait for a real zero-data-loss cutover during planned maintenance. Returns the last
     * {@link Result} observed either way; the caller decides what a timed-out wait means for
     * whether to actually proceed (see {@code MetricsServer}'s drain handler). {@code
     * NOT_A_REPLICA}/an unreachable probe both return immediately -- there's nothing to poll for.
     */
    public static Result awaitLagBelow(BackendTarget fallback, double maxAcceptableLagSeconds, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Result last = check(fallback);
        while (last.ok() && last.isReplica() && last.lagSeconds() > maxAcceptableLagSeconds
                && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            last = check(fallback);
        }
        return last;
    }
}
