package com.polygres.wire.core.access;

import com.polygres.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * §3.6 of {@code docs/design/end-user-data-access-security.md} — populates a real backend
 * session's security context (Postgres GUCs via {@code set_config}, Oracle's
 * {@code DBMS_SESSION.SET_CONTEXT}) from an {@link AccessContext} so the database's own native
 * row-level security (Postgres {@code CREATE POLICY}) or Virtual Private Database
 * ({@code DBMS_RLS}) engine enforces row visibility — no SQL-text rewriting involved, immune to
 * the class of clause-placement gap {@link WhereClauseInjector}'s javadoc documents. PolyWire's
 * job here is only to populate the session context faithfully; the actual policy/predicate lives
 * on the target database, owned by its DBA.
 *
 * <p>Implementations must be idempotent-safe to call once per statement (today's granularity —
 * see {@code JdbcBackendExecutor}) and must not throw for an {@link AccessContext} with no
 * attributes (a no-op initialization is correct, not an error).
 */
public interface NativeRlsSessionInitializer {

    void initialize(Connection connection, AccessContext accessContext) throws SQLException;
}
