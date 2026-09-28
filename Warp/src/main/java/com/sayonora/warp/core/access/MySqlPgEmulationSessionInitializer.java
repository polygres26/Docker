package com.sayonora.warp.core.access;

import com.sayonora.warp.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The mywire equivalent of {@link OraclePgEmulationSessionInitializer}: every mywire session's
 * backend Postgres connection needs {@code SET db_emulation = 'mysql'} issued (see Shim/pg_mysql
 * and Shim/pg_oracle/src/pg_oracle.c's own header comment for the full mechanism), or none of
 * pg_mysql's unqualified MySQL-compatible function names (LAST_INSERT_ID(), GROUP_CONCAT(...),
 * DATE_FORMAT(...), ...) resolve at all.
 *
 * <p>Unlike {@link OraclePgEmulationSessionInitializer}, mywire has no existing
 * {@code PostgresRlsSessionInitializer}-based warp.* GUC propagation to delegate to -- this
 * class is the first {@link NativeRlsSessionInitializer} mywire has ever needed, so it's a plain
 * standalone implementation rather than a wrapper adding one thing on top of something else.
 *
 * <p>Best-effort by design, same as the Oracle side: if Shim/pg_mysql isn't installed on the target
 * database at all -- Warp can be deployed against a plain, unmodified Postgres backend, or an
 * older pg_oracle predating the 'mysql' enum value -- {@code SET db_emulation = 'mysql'} would
 * fail loudly on every single statement (an unrecognized GUC name, or an invalid enum value)
 * exactly the way orawire's own {@code SET db_emulation = 'oracle'} once did before {@code
 * PgOracleSupport} was added. {@link com.sayonora.warp.core.PgMysqlSupport} detects that up front
 * (cached per backend) and this initializer skips the {@code SET} entirely when pg_mysql isn't
 * installed, rather than failing every statement against a backend that doesn't have it.
 */
public final class MySqlPgEmulationSessionInitializer implements NativeRlsSessionInitializer {

    // Emulation state is recorded per PHYSICAL connection (PhysicalSessionState), not per session: with
    // connection multiplexing (SessionConnectionLease) a session's next statement may land on another
    // connection, and several sessions take turns on each one. Unchanged connection = zero round trips.

    @Override
    public boolean runEvenWhenAnonymous() {
        // See NativeRlsSessionInitializer's own comment, and
        // OraclePgEmulationSessionInitializer's identical override -- db_emulation is a
        // protocol-level requirement of every mywire session, not a per-user RBAC/VPD concern, so
        // it must not be skipped just because the connection has no real authenticated identity
        // (a plain username/password mywire login with no WARP_AUTH_MODE configured produces
        // AccessContext.ANONYMOUS, exactly the case orawire's own history already found this
        // matters for).
        return true;
    }

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        PhysicalSessionState.State st = PhysicalSessionState.of(connection);
        // mywire has no identity of its own: never inherit another frontend's warp.* identity or tenant
        // search_path from a shared physical connection.
        SessionStateReconciler.applyRls(connection, st, AccessContext.ANONYMOUS, false);
        SessionStateReconciler.clearTenantPath(connection, st);
        if (!st.emulationUnknown && "mysql".equals(st.emulation)) {
            return;
        }
        if (!com.sayonora.warp.core.PgMysqlSupport.isAvailable(connection)) {
            return;
        }
        // Enterprise-only -- see DbCompatLicensing's own javadoc. Free tier: the session just
        // runs against plain Postgres semantics, same as pg_mysql not being installed at all.
        if (!com.sayonora.warp.license.DbCompatLicensing.dbEmulationAllowed()) {
            return;
        }
        SessionStateReconciler.ensureEmulation(connection, st, "mysql");
    }
}
