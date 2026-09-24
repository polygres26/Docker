package com.sayonora.wire.core.access;

import com.sayonora.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The mssqlwire equivalent of {@link OraclePgEmulationSessionInitializer}/{@code
 * MySqlPgEmulationSessionInitializer}: every mssqlwire session's backend Postgres connection
 * needs {@code SET db_emulation = 'sqlserver'} issued, or none of pg_sqlserver's unqualified
 * SQL-Server-shaped names (sys.tables, OBJECT_ID(...), SCOPE_IDENTITY(), ...) resolve at all.
 *
 * <p>Unlike mywire's own initializer (which had no prior {@link NativeRlsSessionInitializer} to
 * build on), mssqlwire already used {@link PostgresRlsSessionInitializer} for its warp.* GUC
 * propagation -- this class delegates to it rather than duplicating it, the same "adds exactly
 * one thing on top" shape as {@link OraclePgEmulationSessionInitializer}.
 *
 * <p>Best-effort by design, same as the Oracle/MySQL sides: if db/pg_sqlserver isn't installed on
 * the target database at all, {@code SET db_emulation = 'sqlserver'} would fail loudly on every
 * single statement (an unrecognized GUC name, or an invalid enum value on an older pg_oracle).
 * {@link com.sayonora.wire.core.PgSqlServerSupport} detects that up front (cached per backend)
 * and this initializer skips the {@code SET} entirely when pg_sqlserver isn't installed.
 */
public final class MssqlPgEmulationSessionInitializer implements NativeRlsSessionInitializer {

    private final PostgresRlsSessionInitializer delegate = new PostgresRlsSessionInitializer();

    // Same redundant-per-statement-round-trip shape PostgresRlsSessionInitializer's own javadoc
    // now documents -- this instance is constructed exactly once per mssqlwire session
    // (MssqlWireSessionHandler's ctor), and `SET db_emulation` only ever needs re-asserting when
    // THIS executor's connection actually changes (a fresh pooled physical connection, or a
    // failover rebind) -- never on every statement against the same connection it was already set
    // on. Cached by connection IDENTITY so a rebind (new pooled connection, possibly last used by
    // a completely different dialect's session) is always a cache miss and re-asserts for real,
    // matching OraclePgEmulationSessionInitializer's own db_emulation_assign_hook-reconciliation
    // caveat (see its javadoc) -- this cache never assumes correctness across a connection swap,
    // only across repeated statements on the SAME already-initialized connection.
    private Connection lastEmulationConnection;

    @Override
    public boolean runEvenWhenAnonymous() {
        // See NativeRlsSessionInitializer's own comment, and OraclePgEmulationSessionInitializer's
        // identical override -- db_emulation is a protocol-level requirement of every mssqlwire
        // session, not a per-user RBAC/VPD concern, so it must not be skipped just because the
        // connection has no real authenticated identity.
        return true;
    }

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        delegate.initialize(connection, accessContext);

        if (connection == lastEmulationConnection) {
            return;
        }

        if (!com.sayonora.wire.core.PgSqlServerSupport.isAvailable(connection)) {
            return;
        }
        // Enterprise-only -- see DbCompatLicensing's own javadoc. Free tier: the session just
        // runs against plain Postgres semantics, same as pg_sqlserver not being installed at all.
        if (!com.sayonora.wire.license.DbCompatLicensing.dbEmulationAllowed()) {
            return;
        }

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET db_emulation = 'sqlserver'");
        }
        lastEmulationConnection = connection;
    }
}
