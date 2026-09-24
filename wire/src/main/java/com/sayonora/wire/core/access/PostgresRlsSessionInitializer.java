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

    // Real, found-live bottleneck (same shape as DialectTranslationStage's own recordAccess fix):
    // JdbcBackendExecutor.execute() calls initialize() on EVERY statement, unconditionally -- but
    // this class's own instance is constructed exactly once per session (see PgWireSessionHandler/
    // OraclePgEmulationSessionInitializer/MssqlPgEmulationSessionInitializer, none of which ever
    // share one instance across sessions or threads), and a session's AccessContext essentially
    // never changes statement-to-statement. Re-running a real `SELECT set_config(...)` round trip
    // per warp.* attribute on every single statement -- when the exact same values were already
    // pushed onto the exact same connection just before -- was pure waste, measured live (mssqlwire
    // write RTT) at 400-1000us per statement, comparable to the statement's own execute() cost.
    // Cached by connection IDENTITY (not equals()) plus AccessContext#equals() (a record, so this
    // is real value equality, not reference equality) -- a rebind() to a different physical
    // connection (dual-exec/failover, or a fresh pooled connection for a new session) is a cache
    // miss by construction, so this never skips re-asserting warp.* on a connection it hasn't
    // actually asserted them on yet. Deliberately NOT static/shared -- each caller already
    // constructs its own instance per session, so this is exactly as session-scoped as the
    // sessionConnection() these SET_CONFIGs actually apply to.
    private Connection lastConnection;
    private AccessContext lastAccessContext;

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        if (connection == lastConnection && accessContext.equals(lastAccessContext)) {
            return;
        }
        setConfig(connection, "warp.user_id", accessContext.userId());
        for (var entry : accessContext.attributes().entrySet()) {
            setConfig(connection, "warp." + entry.getKey(), entry.getValue());
        }
        lastConnection = connection;
        lastAccessContext = accessContext;
    }

    private void setConfig(Connection connection, String settingName, String value) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT set_config(?, ?, false)")) {
            stmt.setString(1, settingName);
            stmt.setString(2, value);
            stmt.execute();
        }
    }
}
