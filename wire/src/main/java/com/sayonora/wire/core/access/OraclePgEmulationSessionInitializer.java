package com.sayonora.wire.core.access;

import com.sayonora.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The missing link between orawire and db/pg_oracle: every orawire session's backend Postgres
 * connection needs {@code SET db_emulation = 'oracle'} issued exactly once, or none of
 * pg_oracle's unqualified V$, DBA_*, DBMS_*, or UTL_* names resolve at all -- confirmed live
 * while building pg_oracle (see db/pg_oracle/README.md) that without this, orawire's translated SQL
 * hits plain "relation does not exist"/"function does not exist" against a stock Postgres
 * session with no idea it should be looking in oracle_catalog/dbms_output/etc.
 *
 * Delegates everything else to {@link PostgresRlsSessionInitializer} rather than duplicating
 * its warp.* GUC propagation -- this class adds exactly one thing on top, it doesn't
 * replace the existing access-context wiring every other protocol also relies on.
 *
 * Also forwards the access context into pg_oracle's own SYS_CONTEXT store
 * (db/pg_oracle's dbms_session.set_context('warp_ctx', ...) plus set_identifier() for the
 * userId) -- not because orawire needs it for anything today, but because it's the exact,
 * intended integration point db/pg_oracle's VPD section was designed for: an Oracle-migrated
 * app whose RLS/VPD policies already call SYS_CONTEXT('warp_ctx', 'tenant_id') keeps
 * working unmodified once fronted by orawire, with zero policy-side changes. If pg_oracle isn't
 * installed in the target database at all (a Postgres backend orawire is pointed at with no
 * pg_oracle extension), {@link com.sayonora.wire.core.PgOracleSupport} detects that up front and
 * this initializer skips both the {@code SET db_emulation} and the SYS_CONTEXT forwarding below
 * entirely, rather than failing every statement against a plain Postgres backend the way it used
 * to -- see that class's own javadoc, and {@link com.sayonora.wire.core.DialectTranslations} for
 * the matching degradation on the translation side (TO_CHAR/TO_DATE left unqualified instead of
 * pointed at a schema that doesn't exist).
 *
 * A real, subtle interaction with {@link com.sayonora.wire.core.LazyPooledConnection} found
 * live and fixed on pg_oracle's own side (db/pg_oracle's db_emulation_assign_hook, see that
 * file's comment): LazyPooledConnection issues its own unconditional `SET search_path TO
 * "&lt;tenant&gt;", public` the first time its Java wrapper opens a (possibly pool-reused)
 * physical connection, with no idea this class's search_path append exists. Because a Postgres
 * backend process can outlive what this code thinks is "a fresh logical connection", `SET
 * db_emulation = 'oracle'` here can look like a no-op-by-value on the C side (already 'oracle'
 * from a prior logical session sharing the same physical backend) even though
 * LazyPooledConnection just wiped the search_path out from under it. Nothing to change here --
 * it's fixed by making pg_oracle's own hook reconcile against the actual current search_path on
 * every call instead of trusting the enum value didn't change -- but worth knowing about if this
 * class's `SET` ever looks like a no-op that should have done something.
 */
public final class OraclePgEmulationSessionInitializer implements NativeRlsSessionInitializer {

    private final PostgresRlsSessionInitializer delegate = new PostgresRlsSessionInitializer();

    @Override
    public boolean runEvenWhenAnonymous() {
        // See NativeRlsSessionInitializer's own comment -- db_emulation is a protocol-level
        // requirement of every orawire session, not a per-user RBAC/VPD concern, so it must not
        // be skipped just because the connection has no real authenticated identity. Found live:
        // a plain username/password orawire login (no WARP_AUTH_MODE configured) produces
        // AccessContext.ANONYMOUS, and without this override SET db_emulation = 'oracle' was
        // silently never issued at all -- every unqualified V$/DBA_*/DBMS_*/UTL_* reference and
        // this extension's own new one-argument TO_CHAR/TO_DATE overloads failed with
        // ORA-00942/ORA-00904 ("table or view does not exist"/"invalid identifier") even though
        // pg_oracle itself was correctly installed and working (schema-qualified calls like
        // DBMS_RANDOM.STRING(...) still worked throughout, since those don't depend on
        // search_path at all -- that's what actually pointed at this bug).
        return true;
    }

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        delegate.initialize(connection, accessContext);

        // Warp can be deployed against a plain, unmodified Postgres backend with no pg_oracle
        // extension installed at all -- `SET db_emulation` on such a backend used to fail loudly
        // on every single statement, since `db_emulation` is simply an unrecognized GUC name
        // there (see this class's own now-outdated javadoc, and com.sayonora.wire.core.
        // PgOracleSupport for the detection this replaces that with). Detected and cached per
        // backend, not re-probed every call -- see PgOracleSupport's own javadoc for why that
        // caching is safe.
        if (!com.sayonora.wire.core.PgOracleSupport.isAvailable(connection)) {
            return;
        }
        // Enterprise-only -- see DbCompatLicensing's own javadoc. Free tier: the session just
        // runs against plain Postgres semantics, same as pg_oracle not being installed at all
        // (which also means no SYS_CONTEXT propagation below -- that's part of the same gate,
        // not a separate check, since it's meaningless without db_emulation active).
        if (!com.sayonora.wire.license.DbCompatLicensing.dbEmulationAllowed()) {
            return;
        }

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET db_emulation = 'oracle'");
        }

        // Best-effort SYS_CONTEXT propagation -- pg_oracle's own create_context() is owner-only
        // (see db/pg_oracle/README.md's DBMS_NETWORK_ACL_ADMIN-pattern privilege model), so the
        // 'warp_ctx' namespace must already have been registered once by a DBA/admin
        // connection before this call, exactly the same way an Oracle DBA runs CREATE CONTEXT
        // once before any session can SET_CONTEXT against it. A session where nobody's done
        // that yet gets ORA-01403 here -- caught and swallowed deliberately, not surfaced as an
        // orawire connection failure, since not every deployment will have VPD/SYS_CONTEXT
        // wired up at all and forcing that setup just to accept a connection would be wrong.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SELECT dbms_session.set_identifier(" + quoteLiteral(accessContext.userId()) + ")");
            for (var entry : accessContext.attributes().entrySet()) {
                stmt.execute("SELECT dbms_session.set_context('warp_ctx', "
                        + quoteLiteral(entry.getKey()) + ", " + quoteLiteral(entry.getValue()) + ")");
            }
        } catch (SQLException ignoredContextNotRegistered) {
            // See comment above -- expected and harmless when 'warp_ctx' hasn't been
            // created via pg_oracle's oracle_catalog.create_context() in this database.
        }
    }

    private static String quoteLiteral(String value) {
        if (value == null) {
            return "NULL";
        }
        return "'" + value.replace("'", "''") + "'";
    }
}
