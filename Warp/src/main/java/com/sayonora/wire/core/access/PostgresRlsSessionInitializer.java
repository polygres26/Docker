package com.sayonora.wire.core.access;

import com.sayonora.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Real Postgres RLS session-context propagation: {@code SELECT set_config('warp.<attr>', ?, false)}
 * per {@link AccessContext} attribute, letting the backend's own {@code CREATE POLICY ... USING
 * (col = current_setting('warp.<attr>', true))} enforce row security natively -- no SQL rewriting,
 * no trust that Warp's own {@code AccessControlStage} caught every access path.
 *
 * <p>Deliberately {@code is_local=false} (session-scoped, not {@code is_local=true}/transaction-
 * scoped) -- this is a real, caller-dependent tradeoff, not an oversight, and a future caller of
 * this class MUST re-evaluate it rather than assume {@code false} is always safe:
 * <ul>
 *   <li>{@code is_local=true} was the first thing tried and found live to silently evaporate under
 *       plain autocommit -- the {@code set_config} call and the query that follows are each their
 *       own implicit transaction, so a transaction-scoped setting is gone before the query it was
 *       meant to govern ever runs.
 *   <li>{@code is_local=false} is safe ONLY when the connection this runs against is never returned
 *       to a pool shared across different callers before being closed/reset -- true today for
 *       {@link SchemaFederationStage}'s federated mounts (Calcite's {@code JdbcSchema.dataSource}
 *       opens/closes a fresh physical connection per query execution, confirmed, no pooling) and for
 *       {@code JdbcBackendExecutor}'s DEFAULT-BACKEND path only insofar as that path is NOT yet
 *       wired to actually construct this initializer for real pooled traffic. If this is ever wired
 *       into a caller that DOES source connections from {@code BackendConnectionPools} (real
 *       HikariCP pooling), {@code is_local=false} becomes a real cross-request leak: the next
 *       borrower of that same pooled connection would silently inherit the previous caller's
 *       {@code warp.*} settings until explicitly reset. Do not wire this into a pooled-connection
 *       caller without first adding either a Hikari connection-reset hook or an explicit
 *       transaction wrap (turn off autocommit for the one statement, {@code is_local=true}, commit/
 *       rollback after) -- see this class's own tests for the exact SQL shape either fix needs.
 * </ul>
 */
public final class PostgresRlsSessionInitializer implements NativeRlsSessionInitializer {

    // Connection multiplexing (WARP_MULTIPLEX_SESSIONS, see SessionConnectionLease): a session no longer owns
    // one pooled connection for life, so the old per-instance "same Connection + same AccessContext" cache is
    // gone -- a pool hands out a new proxy per borrow and many sessions share each physical connection. What
    // was applied is recorded per PHYSICAL connection (PhysicalSessionState) and reconciled on every statement:
    // unchanged = zero round trips, changed/foreign = set for this identity, or CLEAR for an identity-less
    // borrower -- so identity can never leak between clients that share a physical connection. That also
    // resolves the is_local=false hazard described above for pooled callers.
    private final boolean plainSession;
    private final boolean applyAnonymous;

    /** pgwire's shape: anonymous = no identity, and a leftover emulation/tenant search_path from another
     * frontend's session is stripped. */
    public PostgresRlsSessionInitializer() {
        this(true, false);
    }

    /** @param plainSession true if this session wants no db_emulation and no tenant search_path
     * @param applyAnonymous true to assert {@code warp.user_id = 'anonymous'} for an anonymous context (the
     *        emulating frontends always did), false to treat anonymous as "no identity" */
    public PostgresRlsSessionInitializer(boolean plainSession, boolean applyAnonymous) {
        this.plainSession = plainSession;
        this.applyAnonymous = applyAnonymous;
    }

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        PhysicalSessionState.State st = PhysicalSessionState.of(connection);
        if (plainSession) {
            SessionStateReconciler.clearForPlainSession(connection, st);
        }
        SessionStateReconciler.applyRls(connection, st, accessContext, applyAnonymous);
    }

    @Override
    public boolean needsRunEvenWhenAnonymous(Connection connection) {
        // An anonymous pgwire session normally skips the initializer entirely; it must still run when the
        // physical connection carries a previous borrower's identity/emulation/tenant path to clear.
        PhysicalSessionState.State st = PhysicalSessionState.peek(connection);
        return st != null && st.dirtyForPlainSession();
    }
}
