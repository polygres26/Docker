package com.nexagres.wire.core.access;

import com.nexagres.wire.core.AccessContext;
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
 * build on), mssqlwire already used {@link PostgresRlsSessionInitializer} for its polywire.* GUC
 * propagation -- this class delegates to it rather than duplicating it, the same "adds exactly
 * one thing on top" shape as {@link OraclePgEmulationSessionInitializer}.
 *
 * <p>Best-effort by design, same as the Oracle/MySQL sides: if db/pg_sqlserver isn't installed on
 * the target database at all, {@code SET db_emulation = 'sqlserver'} would fail loudly on every
 * single statement (an unrecognized GUC name, or an invalid enum value on an older pg_oracle).
 * {@link com.nexagres.wire.core.PgSqlServerSupport} detects that up front (cached per backend)
 * and this initializer skips the {@code SET} entirely when pg_sqlserver isn't installed.
 */
public final class MssqlPgEmulationSessionInitializer implements NativeRlsSessionInitializer {

    private final PostgresRlsSessionInitializer delegate = new PostgresRlsSessionInitializer();

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

        if (!com.nexagres.wire.core.PgSqlServerSupport.isAvailable(connection)) {
            return;
        }
        // Enterprise-only -- see DbCompatLicensing's own javadoc. Free tier: the session just
        // runs against plain Postgres semantics, same as pg_sqlserver not being installed at all.
        if (!com.nexagres.wire.license.DbCompatLicensing.dbEmulationAllowed()) {
            return;
        }

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET db_emulation = 'sqlserver'");
        }
    }
}
