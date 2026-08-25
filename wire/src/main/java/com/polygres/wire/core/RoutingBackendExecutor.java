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

public final class RoutingBackendExecutor implements BackendExecutor {

    private static final Logger log = LoggerFactory.getLogger(RoutingBackendExecutor.class);

    public static final String SCATTER_ALL = "*scatter-all*";

    private final BackendRegistry registry;
    private final BackendExecutor defaultExecutor;

    private Map<String, Connection> transactionConnections;

    private Map<String, String> cursorTargets;

    private XaTransaction xaTransaction;

    private boolean transactionFailed;

    private static final Pattern DECLARE_CURSOR = Pattern.compile("(?i)^\\s*DECLARE\\s+(\\w+)\\s+CURSOR\\b");
    private static final Pattern FETCH_OR_CLOSE_CURSOR =
            Pattern.compile("(?i)^\\s*(?:FETCH\\b.*\\b(?:FROM|IN)\\s+(\\w+)|CLOSE\\s+(\\w+))\\s*;?\\s*$");

    public RoutingBackendExecutor(BackendRegistry registry, BackendExecutor defaultExecutor) {
        this.registry = registry;
        this.defaultExecutor = defaultExecutor;
    }

    public boolean inTransaction() {
        return transactionConnections != null;
    }

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
        
        if (targetName == null || registry.isEmpty() || BackendRegistry.DEFAULT_BACKEND_NAME.equals(targetName)) {
            // The common case -- no POLYWIRE_ROUTER_* rule matched -- normally always uses
            // defaultExecutor, a connection borrowed once for the whole client session. That's
            // correct and cheapest for the vast majority of statements, but it structurally can't
            // read-route: the same connection serves every statement in the session regardless of
            // read/write. When read routing is enabled, route an eligible read through the fresh-
            // connection-per-statement path instead (same eligibility rule as the explicit-target
            // path below: autocommit, READ-classified, and only when a default target actually
            // exists to route against).
            if (READ_ROUTING_ENABLED && transactionConnections == null && !registry.isEmpty()
                    && SqlMetricsCollector.classify(statement.sqlText()) == SqlMetricsCollector.StatementKind.READ) {
                BackendTarget defaultTarget = registry.get(BackendRegistry.DEFAULT_BACKEND_NAME);
                if (defaultTarget != null) {
                    return executeOnFreshConnection(defaultTarget, statement);
                }
            }
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

    private static String cursorNameReferenced(String sql) {
        Matcher matcher = FETCH_OR_CLOSE_CURSOR.matcher(sql);
        if (!matcher.matches()) {
            return null;
        }
        return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    }

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

    // Opt-in (default off): reading from a standby means reading data that may be behind the
    // primary by however long replication lag currently is -- a real correctness tradeoff, not
    // a free win, so this must never be silently on. Off by default matches every other
    // behavior-changing toggle in this codebase (e.g. POLYWIRE_DYNAMOWIRE_CACHE_ENABLED's
    // sibling pattern), except inverted: here the safer default (always read the primary) is
    // the one that ships without an explicit opt-in.
    private static final boolean READ_ROUTING_ENABLED =
            "true".equalsIgnoreCase(System.getenv("POLYWIRE_READ_ROUTING_ENABLED"));

    private ExecutionResult executeOnFreshConnection(BackendTarget target, Statement statement) throws SQLException {
        // Only single, autocommit, read-classified statements are eligible -- this method is
        // only ever called when transactionConnections == null (see execute()), so "not inside a
        // transaction" is already guaranteed by the caller; the remaining condition is purely
        // "would sending this to a standby be safe", which for a WRITE or an unclassifiable
        // statement it is not.
        boolean preferStandby = READ_ROUTING_ENABLED
                && SqlMetricsCollector.classify(statement.sqlText()) == SqlMetricsCollector.StatementKind.READ;
        try (Connection connection = preferStandby ? target.openPreferringStandby() : target.open()) {
            return new JdbcBackendExecutor(connection).execute(statement);
        }
    }

    private ExecutionResult executeOnTransactionConnection(BackendTarget target, Statement statement) throws SQLException {
        Connection connection = transactionConnections.get(target.name());
        if (connection == null) {
            
            XaBackendFactory.XaBranch branch = XaBackendFactory.open(target);
            xaTransaction.addBranch(branch.resource());
            connection = branch.connection();
            transactionConnections.put(target.name(), connection);
        }
        try {
            return new JdbcBackendExecutor(connection).execute(statement);
        } catch (SQLException e) {
            
            transactionFailed = true;
            throw e;
        }
    }
}
