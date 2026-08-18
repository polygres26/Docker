package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * One named JDBC backend a {@link RouterStage} rule can route a statement
 * to. Config only. Connections come from {@link BackendConnectionPools},
 * keyed by the real {@code (jdbcUrl, user)} identity via {@link
 * BackendConnectionPools#poolKeyFor} — not by {@code name}, which is only a
 * routing label a config author picked and may not be unique per physical
 * backend — shared across every caller/session that targets the same real
 * backend, not opened fresh each time (see that class's javadoc for what
 * "shared" buys).
 */
public record BackendTarget(String name, String jdbcUrl, String user, String password) {

    /**
     * The SQL dialect this backend's JDBC URL implies — always {@link SourceDialect#POSTGRES} in
     * PolyWire, since every backend target is Postgres-only (unlike Polywire, which routed to a
     * mix of Oracle/MySQL/Snowflake/Redshift/BigQuery/Databricks/SQL Server/generic-REST backends
     * too — see this class's git history for that full dispatch table). Kept as a method (not a
     * constant) so {@link DialectTranslationStage} callers don't need to change, and so a future
     * non-Postgres backend only needs this one place touched. {@code null} for any URL this project
     * doesn't recognize as Postgres at all — {@link DialectTranslationStage} treats {@code null} the
     * same as "no rules for this pairing," passing the statement through unchanged rather than
     * guessing.
     */
    public SourceDialect dialect() {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (url.startsWith("jdbc:postgresql:")) {
            return SourceDialect.POSTGRES;
        }
        return null;
    }

    /** Plain (non-XA) autocommit connection — used by {@link RoutingBackendExecutor} for one-shot routed/scattered statements. */
    public Connection open() throws SQLException {
        Connection connection = borrow();
        connection.setAutoCommit(true);
        return connection;
    }

    /** Plain (non-XA) manual-commit connection — used by best-effort (non-XA) replication, where the client's own COMMIT/ROLLBACK must control the transaction boundary alongside the primary connection. */
    public Connection openManualCommit() throws SQLException {
        Connection connection = borrow();
        connection.setAutoCommit(false);
        return connection;
    }

    private Connection borrow() throws SQLException {
        // Keyed on the physical (jdbcUrl, user) identity, not on this target's own configured
        // name -- see BackendConnectionPools#poolKeyFor's javadoc for why: two different names
        // routing to the same real backend must share one pool, not silently double it.
        return BackendConnectionPools.borrow(BackendConnectionPools.poolKeyFor(jdbcUrl, user), jdbcUrl, user, password);
    }
}
