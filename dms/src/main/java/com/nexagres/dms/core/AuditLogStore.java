package com.nexagres.dms.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Append-only audit trail for the admin console -- who did what, when. Same embedded-HSQLDB
 * shape as {@link ConnectionStore} (same file, same {@code NEXAGRES_DATA_DIR}, same pool key),
 * a second table rather than a second storage mechanism.
 *
 * <p>{@link #record} is a genuine no-op unless {@link DmsLicensing#auditLoggingEnabled()} is
 * {@code true} -- the free/Developer tier's mutating actions still happen exactly the same, they
 * just aren't recorded to a queryable audit trail beyond this process's own SLF4J logs (every
 * caller of this class already logs the same event at INFO/WARN independently -- see {@code
 * AdminAuth}, {@code ConnectionsRoute}, {@code MigrationJobsRoute} -- so nothing is silently lost,
 * only the structured, queryable record of it is Enterprise-only). Deliberately fails OPEN, not
 * closed: a database hiccup while writing an audit entry must never block the actual action being
 * audited (see {@link #record}'s own try/catch) -- an admin console that stops working because its
 * OWN audit log is unavailable would be a worse outcome than an audit trail with a gap in it.
 */
public class AuditLogStore {

    private static final Logger log = LoggerFactory.getLogger(AuditLogStore.class);
    private static final String DEFAULT_POOL_KEY = "nexagres-control";

    private final String poolKey;
    private final String jdbcUrl;

    public AuditLogStore() {
        this(DEFAULT_POOL_KEY, "jdbc:hsqldb:file:"
                + System.getenv().getOrDefault("NEXAGRES_DATA_DIR", System.getProperty("user.home") + "/.nexagres")
                + "/nexagres-store;shutdown=true");
    }

    /** Mainly a test seam -- a distinct pool key/URL so a test's isolated in-memory HSQLDB
     * instance never collides with {@link #DEFAULT_POOL_KEY}'s process-wide, JVM-lifetime-cached
     * pool (see {@link BackendConnectionPools}'s own javadoc: pools are cached by key alone, first
     * caller wins the URL). Public because it's also a legitimate way to point this at a
     * different real store, not just a test hook. */
    public AuditLogStore(String poolKey, String jdbcUrl) {
        this.poolKey = poolKey;
        this.jdbcUrl = jdbcUrl;
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS audit_log ("
                + "id VARCHAR(64) PRIMARY KEY, "
                + "occurred_at VARCHAR(64), "
                + "actor VARCHAR(255), "
                + "action VARCHAR(128), "
                + "detail VARCHAR(1024))");
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize audit log store at " + jdbcUrl, e);
        }
    }

    /** Records one audit entry -- a real no-op (not even a connection borrowed) on the
     * free/Developer tier. Never throws: a failure writing the audit entry is logged and
     * swallowed rather than propagated, see this class's own javadoc on failing open. */
    public void record(String actor, String action, String detail) {
        if (!DmsLicensing.auditLoggingEnabled()) {
            return;
        }
        try (Connection connection = borrow();
                PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO audit_log (id, occurred_at, actor, action, detail) VALUES (?, ?, ?, ?, ?)")) {
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, Instant.now().toString());
            ps.setString(3, actor == null ? "(unknown)" : actor);
            ps.setString(4, action);
            ps.setString(5, detail == null ? "" : detail);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("failed to write audit log entry for actor={} action={} -- the action itself "
                    + "still happened, only its audit record was lost", actor, action, e);
        }
    }

    /** Every recorded entry, newest first -- {@code emptyList()} on the free/Developer tier (there
     * is never anything to return, since {@link #record} never wrote anything). */
    public List<Entry> list() {
        List<Entry> entries = new ArrayList<>();
        try (Connection connection = borrow();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM audit_log ORDER BY occurred_at DESC")) {
            while (rs.next()) {
                entries.add(new Entry(rs.getString("occurred_at"), rs.getString("actor"),
                        rs.getString("action"), rs.getString("detail")));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not list audit log entries", e);
        }
        return entries;
    }

    public record Entry(String occurredAt, String actor, String action, String detail) {
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(poolKey, jdbcUrl, "SA", "");
    }
}
