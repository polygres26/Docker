package com.polygres.wire.core;

import com.polygres.wire.xa.XaBackendFactory;
import com.polygres.wire.xa.XaTransaction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Terminal executor wrapping a frontend's own default connection.
 * <ul>
 *   <li>If {@link RouterStage} assigned a single {@link Statement#targetBackend()},
 *   routes to that {@link BackendRegistry} entry instead — see
 *   {@link #beginTransaction()}'s javadoc for whether that's a fresh connection per
 *   statement or one held for an open transaction.</li>
 *   <li>If it assigned {@link #SCATTER_ALL}, fans the same statement out to
 *   every backend in {@link BackendRegistry#shardGroup()} and merges the
 *   results — ARCHITECTURE.md §5.5 sharding's scatter-gather read path.
 *   Query-only: {@link RouterStage} only ever assigns this marker to
 *   {@code SELECT}s (see its {@code ShardRule} handling), and this executor
 *   still enforces it as a hard invariant since a DML fan-out would need
 *   real distributed-transaction semantics this narrow slice doesn't have —
 *   XA/2PC across shards is future work, not implemented here. Result merge
 *   is a plain row concatenation using the first shard's column list — no
 *   cross-shard ORDER BY/aggregation re-merge, another documented
 *   narrow-slice limit. Scatter-gather always uses a fresh connection per
 *   shard per call — it's SELECT-only and read-committed is fine.</li>
 * </ul>
 * Statements with no {@code targetBackend} (the common case when no rule
 * matches, or {@link BackendRegistry} is empty) fall through to
 * {@code defaultExecutor} — usually a {@link JdbcBackendExecutor} bound to
 * the connection the frontend already has open — with zero overhead.
 */
public final class RoutingBackendExecutor implements BackendExecutor {

    private static final Logger log = LoggerFactory.getLogger(RoutingBackendExecutor.class);

    /** {@link RouterStage} sentinel meaning "fan this SELECT out across the shard group," not a real backend name. */
    public static final String SCATTER_ALL = "*scatter-all*";

    private final BackendRegistry registry;
    private final BackendExecutor defaultExecutor;

    /**
     * {@code null} outside an explicit transaction (the common case — see {@link #beginTransaction()}).
     * Keyed by target backend name, populated lazily: the first routed statement to a given target
     * during an open transaction opens the connection and adds it here; every subsequent routed
     * statement to that <em>same</em> target during the <em>same</em> transaction reuses it.
     */
    private Map<String, Connection> transactionConnections;

    /**
     * {@code null} outside an explicit transaction, same lifetime as {@link #transactionConnections}.
     * A real {@code FETCH}/{@code CLOSE} statement never repeats the table's {@code @link} suffix —
     * it only ever names the <em>cursor</em> (e.g. {@code FETCH 100 FROM c1}), which by itself gives
     * {@link DbLinkStage} nothing to route on, so those statements would otherwise silently fall
     * through to {@code defaultExecutor} — a different physical connection than the one the cursor
     * actually lives on, found live as a real {@code postgres_fdw} failure
     * ({@code cursor "c1" does not exist}) even after {@link #beginTransaction()}/
     * {@link #executeOnTransactionConnection} alone fixed the {@code DECLARE CURSOR} step. This map
     * remembers, purely from watching SQL text go by, which target a {@code DECLARE <name> CURSOR}
     * actually ran against, so a later same-transaction {@code FETCH}/{@code CLOSE} referencing that
     * same cursor name gets routed to the same connection without needing its own {@code @link}.
     */
    private Map<String, String> cursorTargets;

    /**
     * Non-null exactly when {@link #transactionConnections} is (same lifetime) — coordinates a
     * real Postgres 2PC commit across however many distinct shard backends the client's open
     * transaction ends up touching. Every {@link #transactionConnections} entry is opened via
     * {@link XaBackendFactory#open} (a real {@code PGXADataSource} branch) instead of a plain
     * autocommit-false connection, and enlisted into this transaction the moment it's opened
     * ({@link XaTransaction#addBranch}) — so a client transaction that only ever touches one
     * shard still pays the (small) XA prepare/commit round trip, but a transaction spanning two
     * or more shards gets real atomicity: {@link #endTransaction} drives one
     * {@link XaTransaction#commit()}/{@link XaTransaction#rollback()} across every branch instead
     * of committing/rolling back each connection independently — see {@link RouterStage}'s and
     * this class's prior javadoc for why that used to be "future work" (this is that work, scoped
     * to DML inside an explicit client {@code BEGIN}/{@code COMMIT}; {@link #executeScatterGather}
     * remains the separate SELECT-only fan-out path and is unaffected).
     */
    private XaTransaction xaTransaction;

    /**
     * {@code true} once any statement routed to a transaction branch (via
     * {@link #executeOnTransactionConnection}) has thrown a {@link SQLException} during the
     * current explicit transaction — e.g. a real constraint violation. Reset by
     * {@link #beginTransaction()}, consulted (and forced back to {@code false}) by
     * {@link #endTransaction}.
     *
     * <p>Exists because real Postgres XA (pgjdbc's {@code PGXAConnection}) does not reliably
     * surface a poisoned branch as a {@link XaTransaction#commit()} <em>prepare</em> failure the
     * way the 2PC protocol's own rules would suggest — found live: a genuine duplicate-key
     * violation on one shard's statement can still let that branch's {@code prepare()} report
     * success, so {@link XaTransaction#commit()} proceeds into its commit-all loop, commits the
     * healthy branch durably, and only then hits a client-side {@code currentXid}/{@code
     * preparedXid} state mismatch on the poisoned branch's own {@code commit()} call — a genuine
     * partial commit, not just an in-doubt window. Trusting the JDBC driver's prepare-phase
     * bookkeeping to catch this case turned out not to be safe, so this class tracks "did any
     * statement in this transaction actually fail" itself, at the layer that already knows —
     * {@link #execute}'s own catch is the only place a poisoned branch is unambiguously observed —
     * and {@link #endTransaction} refuses to even attempt {@link XaTransaction#commit()} when this
     * is {@code true}, forcing a full rollback across every branch instead (same as real Postgres:
     * a client that sends {@code COMMIT} after an error in the transaction gets an implicit
     * rollback, not a partial one).
     */
    private boolean transactionFailed;

    private static final Pattern DECLARE_CURSOR = Pattern.compile("(?i)^\\s*DECLARE\\s+(\\w+)\\s+CURSOR\\b");
    private static final Pattern FETCH_OR_CLOSE_CURSOR =
            Pattern.compile("(?i)^\\s*(?:FETCH\\b.*\\b(?:FROM|IN)\\s+(\\w+)|CLOSE\\s+(\\w+))\\s*;?\\s*$");

    public RoutingBackendExecutor(BackendRegistry registry, BackendExecutor defaultExecutor) {
        this.registry = registry;
        this.defaultExecutor = defaultExecutor;
    }

    /**
     * Called by a session-scoped frontend (today: {@code pgwire} — see
     * {@code PgWireSessionHandler#handleTransactionControl}) when the client's own SQL text is a
     * real {@code BEGIN}. Found live to matter: a real {@code postgres_fdw} connection issues
     * {@code BEGIN} then, in the <em>same</em> transaction, {@code DECLARE CURSOR}/{@code FETCH}/
     * {@code CLOSE} against a {@code table@link}-routed target — but this class's original design
     * (a fresh connection per routed statement, "each routed statement pays a new-connection cost")
     * meant the routed {@code DECLARE CURSOR} landed on a different physical connection than
     * {@code BEGIN} ran on, so the target backend correctly rejected it
     * ("DECLARE CURSOR can only be used in transaction blocks" — {@code BEGIN} never reached its
     * session). Outside an explicit transaction, behavior is unchanged: still a fresh
     * autocommitted connection per routed statement, since that's simpler and correct for the
     * common case (no explicit transaction spanning more than one routed statement).
     */
    /**
     * {@code true} between {@link #beginTransaction} and the matching {@link #endTransaction} —
     * lets a session-scoped frontend (e.g. {@code PgWireSessionHandler}) know whether a statement
     * that failed came from inside an explicit client transaction, so it can call
     * {@link #markTransactionFailed} for failures this class never even sees itself (a statement
     * rejected by an earlier pipeline stage — QoS admission control, the result cache, etc. — never
     * reaches {@link #execute} at all, so {@link #transactionFailed}'s own catch in
     * {@link #executeOnTransactionConnection} can't record it).
     */
    public boolean inTransaction() {
        return transactionConnections != null;
    }

    /**
     * Marks the current explicit transaction as failed, the same as a routed statement throwing
     * from inside {@link #executeOnTransactionConnection} — see {@link #transactionFailed}'s
     * javadoc. For failures {@link #execute} never sees itself (thrown by an earlier pipeline
     * stage before routing even happens). No-op outside an explicit transaction.
     */
    public void markTransactionFailed() {
        if (transactionConnections != null) {
            transactionFailed = true;
        }
    }

    public void beginTransaction() {
        transactionConnections = new LinkedHashMap<>();
        cursorTargets = new LinkedHashMap<>();
        xaTransaction = new XaTransaction();
        transactionFailed = false;
    }

    /**
     * Drives one real 2PC commit/rollback across every shard branch opened for the transaction
     * just ended (via {@link XaTransaction#commit}/{@link XaTransaction#rollback}), then closes
     * every branch connection, then returns to the default (fresh-per-statement) behavior. Safe to
     * call even if no routed statement ever actually opened a branch (transactionConnections empty
     * — {@link XaTransaction#hasBranches()} is false, commit()/rollback() on it are no-ops over an
     * empty resource list).
     *
     * <p>{@code commit} is downgraded to a rollback when {@link #transactionFailed} — see that
     * field's javadoc for why this class does not trust {@link XaTransaction#commit()} alone to
     * catch a poisoned branch.
     */
    public void endTransaction(boolean commit) throws SQLException {
        if (transactionConnections == null) {
            return;
        }
        boolean actuallyCommit = commit && !transactionFailed;
        if (commit && transactionFailed) {
            log.warn("xa: client sent COMMIT but a statement failed earlier in this transaction -- rolling back all branches instead (same as real Postgres implicitly rolling back a COMMIT after an error)");
        }
        SQLException firstFailure = null;
        try {
            if (actuallyCommit) {
                xaTransaction.commit();
            } else {
                xaTransaction.rollback();
            }
        } catch (SQLException e) {
            firstFailure = e;
        }
        for (Connection connection : transactionConnections.values()) {
            try {
                connection.close();
            } catch (SQLException ignoredOnCleanup) {
                // already reporting firstFailure if there was one; a close failure on the way out isn't more useful
            }
        }
        transactionConnections = null;
        cursorTargets = null;
        xaTransaction = null;
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    @Override
    public ExecutionResult execute(Statement statement) throws SQLException {
        String targetName = statement.targetBackend();
        if (targetName == null && transactionConnections != null) {
            targetName = cursorTargets.get(cursorNameReferenced(statement.sqlText()));
        }
        if (targetName == null || registry.isEmpty()) {
            return defaultExecutor.execute(statement);
        }
        if (SCATTER_ALL.equals(targetName)) {
            return executeScatterGather(statement);
        }
        BackendTarget target = registry.get(targetName);
        if (target == null) {
            throw new SQLException("router assigned unknown backend \"" + targetName + "\"");
        }
        if (transactionConnections == null) {
            return executeOnFreshConnection(target, statement);
        }
        rememberCursorTarget(statement.sqlText(), targetName);
        return executeOnTransactionConnection(target, statement);
    }

    /** {@code null} unless {@code sql} is a {@code FETCH}/{@code CLOSE} naming a cursor — the only shapes {@link #cursorTargets} needs to answer for. */
    private static String cursorNameReferenced(String sql) {
        Matcher matcher = FETCH_OR_CLOSE_CURSOR.matcher(sql);
        if (!matcher.matches()) {
            return null;
        }
        return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    }

    /** If {@code sql} is the {@code DECLARE <name> CURSOR} that just ran against {@code targetName}, remember the association for a later same-transaction {@code FETCH}/{@code CLOSE}. */
    private void rememberCursorTarget(String sql, String targetName) {
        Matcher matcher = DECLARE_CURSOR.matcher(sql);
        if (matcher.find()) {
            cursorTargets.put(matcher.group(1), targetName);
        }
    }

    private ExecutionResult executeScatterGather(Statement statement) throws SQLException {
        if (!statement.sqlText().strip().regionMatches(true, 0, "SELECT", 0, 6)) {
            throw new SQLException("scatter-gather only supports SELECT, got: " + statement.sqlText());
        }
        List<String> shardNames = registry.shardGroup();
        if (shardNames.isEmpty()) {
            throw new SQLException("router assigned scatter-gather but POLYWIRE_SHARD_BACKENDS is not configured");
        }
        List<ColumnInfo> columns = null;
        List<List<Object>> mergedRows = new ArrayList<>();
        for (String shardName : shardNames) {
            BackendTarget target = registry.get(shardName);
            if (target == null) {
                throw new SQLException("shard group references unknown backend \"" + shardName + "\"");
            }
            ExecutionResult result = executeOnFreshConnection(target, statement);
            if (columns == null) {
                columns = result.columns();
            }
            mergedRows.addAll(result.rows());
        }
        return ExecutionResult.ofQuery(columns, mergedRows);
    }

    private ExecutionResult executeOnFreshConnection(BackendTarget target, Statement statement) throws SQLException {
        try (Connection connection = target.open()) {
            return new JdbcBackendExecutor(connection).execute(statement);
        }
    }

    private ExecutionResult executeOnTransactionConnection(BackendTarget target, Statement statement) throws SQLException {
        Connection connection = transactionConnections.get(target.name());
        if (connection == null) {
            // Real XA branch (PGXADataSource), not a plain autocommit-false connection -- see
            // xaTransaction's javadoc for why every routed target during an explicit transaction
            // gets one, not just multi-shard ones.
            XaBackendFactory.XaBranch branch = XaBackendFactory.open(target);
            xaTransaction.addBranch(branch.resource());
            connection = branch.connection();
            transactionConnections.put(target.name(), connection);
        }
        try {
            return new JdbcBackendExecutor(connection).execute(statement);
        } catch (SQLException e) {
            // See #transactionFailed's javadoc -- recorded here, not inferred later from
            // XaTransaction's own prepare/commit outcome, since that outcome isn't a reliable
            // signal for this.
            transactionFailed = true;
            throw e;
        }
    }
}
