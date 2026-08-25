package com.polygres.wire.xa;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable record of in-flight two-phase commits, persisted in the control-plane Postgres (same
 * home as {@code polywire_config}/{@code polywire_failed_statements} -- this project's existing
 * pattern for "small operational table, opened fresh per call so a rotated backend never needs a
 * restart to pick up"). Closes the gap flagged by a competitive comparison against ShardingSphere:
 * {@link XaTransaction#commit()} used to log-and-rethrow when a branch failed to commit after a
 * successful prepare vote, leaving that branch prepared (holding locks) at its backend forever,
 * with nothing in PolyWire recording that it needed resolving. See {@link XaRecovery} for the
 * startup pass that reads this log and finishes what a crashed coordinator left in doubt.
 *
 * <p>Only the commit-decision window is logged -- once every branch has voted to prepare, the
 * transaction's fate (commit, since all votes succeeded) is fixed and durable; a crash between
 * that decision and applying it to every branch is exactly the "in-doubt" scenario 2PC recovery
 * exists for. A crash during the prepare phase itself has no logged decision, so this log has
 * nothing to say about it: those branches are left prepared but never voted a shared decision,
 * and are safe to roll back manually via their {@code XAResource.recover()} results -- that path
 * isn't automated here since, unlike the always-COMMIT case above, it isn't universally safe to
 * assume a rollback is correct for every deployment's isolation needs.
 */
public final class XaRecoveryLog {

    private static final Logger log = LoggerFactory.getLogger(XaRecoveryLog.class);

    public record Branch(String gtridHex, int branchIndex, String backendName) {
    }

    private final com.polygres.wire.server.ServerOptions options;

    public XaRecoveryLog(com.polygres.wire.server.ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS polywire_xa_log ("
                    + "gtrid_hex text NOT NULL, "
                    + "branch_index integer NOT NULL, "
                    + "backend_name text NOT NULL, "
                    + "created_at timestamptz NOT NULL DEFAULT now(), "
                    + "resolved_at timestamptz, "
                    + "PRIMARY KEY (gtrid_hex, branch_index))");
        } catch (SQLException e) {
            log.warn("xa recovery log: could not ensure polywire_xa_log schema exists -- in-doubt "
                    + "transactions from a coordinator crash will NOT be recoverable until this is fixed", e);
        }
    }

    /** Logged once the commit decision is made (every branch voted to prepare) and before any
     * branch's actual commit() call -- this is the durable point-of-no-return recovery resumes from. */
    public void logDecided(String gtridHex, List<Branch> branches) {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO polywire_xa_log (gtrid_hex, branch_index, backend_name) VALUES (?, ?, ?) "
                                + "ON CONFLICT (gtrid_hex, branch_index) DO NOTHING")) {
            for (Branch b : branches) {
                ps.setString(1, gtridHex);
                ps.setInt(2, b.branchIndex());
                ps.setString(3, b.backendName());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            // Broad catch deliberately, matching FailedStatementLog's convention -- best-effort
            // logging must never itself take down an otherwise-successful commit, including on a
            // connection-pool-level failure (e.g. HikariCP's fail-fast init throws a
            // RuntimeException, not a SQLException) reaching this far.
            log.warn("xa recovery log: could not record commit decision for gtrid {} -- if the "
                    + "coordinator crashes before every branch commits, this transaction will NOT be "
                    + "auto-recovered on restart: {}", gtridHex, e.getMessage());
        }
    }

    /** Marks one branch resolved as soon as its real commit() call actually succeeds -- called both
     * from the normal synchronous commit path and from startup recovery, so a transaction that's
     * fully committed (whether live or recovered) stops appearing in {@link #findUnresolved()}. */
    public void markBranchResolved(String gtridHex, int branchIndex) {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options);
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE polywire_xa_log SET resolved_at = now() "
                                + "WHERE gtrid_hex = ? AND branch_index = ? AND resolved_at IS NULL")) {
            ps.setString(1, gtridHex);
            ps.setInt(2, branchIndex);
            ps.executeUpdate();
        } catch (Exception e) {
            log.warn("xa recovery log: could not mark gtrid {} branch {} resolved -- it will be "
                    + "re-checked (harmlessly) on the next startup recovery pass: {}",
                    gtridHex, branchIndex, e.getMessage());
        }
    }

    /** Every branch still awaiting resolution, grouped by transaction -- what startup recovery
     * (all committed, by construction: see {@link #logDecided}) needs to finish applying. */
    public Map<String, List<Branch>> findUnresolved() throws SQLException {
        Map<String, List<Branch>> byGtrid = new LinkedHashMap<>();
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT gtrid_hex, branch_index, backend_name FROM polywire_xa_log "
                                + "WHERE resolved_at IS NULL ORDER BY gtrid_hex, branch_index")) {
            while (rs.next()) {
                String gtridHex = rs.getString(1);
                byGtrid.computeIfAbsent(gtridHex, k -> new ArrayList<>())
                        .add(new Branch(gtridHex, rs.getInt(2), rs.getString(3)));
            }
        }
        return byGtrid;
    }

    public static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static byte[] unhex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }
}
