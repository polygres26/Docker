package com.sayonora.warp.core.access;

import com.sayonora.warp.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;

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
 * <p>Best-effort by design, same as the Oracle/MySQL sides: if Shim/pg_sqlserver isn't installed on
 * the target database at all, {@code SET db_emulation = 'sqlserver'} would fail loudly on every
 * single statement (an unrecognized GUC name, or an invalid enum value on an older pg_oracle).
 * {@link com.sayonora.warp.core.PgSqlServerSupport} detects that up front (cached per backend)
 * and this initializer skips the {@code SET} entirely when pg_sqlserver isn't installed.
 */
public final class MssqlPgEmulationSessionInitializer implements NativeRlsSessionInitializer {

    // Same rls delegate as before, but built for a session that keeps its own emulation ("plainSession" false)
    // and always asserts warp.user_id, as this frontend always did. What was applied to a physical connection
    // is recorded on the connection (PhysicalSessionState), not on this per-session instance: with connection
    // multiplexing a session's next statement may run on a different connection than its last one.
    private final PostgresRlsSessionInitializer delegate = new PostgresRlsSessionInitializer(false, true);

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

        PhysicalSessionState.State st = PhysicalSessionState.of(connection);
        SessionStateReconciler.clearTenantPath(connection, st);
        if (!st.emulationUnknown && "sqlserver".equals(st.emulation)) {
            return;
        }

        if (!com.sayonora.warp.core.PgSqlServerSupport.isAvailable(connection)) {
            return;
        }
        // Enterprise-only -- see DbCompatLicensing's own javadoc. Free tier: the session just
        // runs against plain Postgres semantics, same as pg_sqlserver not being installed at all.
        if (!com.sayonora.warp.license.DbCompatLicensing.dbEmulationAllowed()) {
            return;
        }
        SessionStateReconciler.ensureEmulation(connection, st, "sqlserver");
    }
}
