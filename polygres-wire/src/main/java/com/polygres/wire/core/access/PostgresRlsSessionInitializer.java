package com.polygres.wire.core.access;

import com.polygres.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Postgres native RLS pass-through — same mechanism PostgREST uses
 * ({@code SET LOCAL request.jwt.claims = '<json>'} + policies reading it via
 * {@code current_setting(...)}), adapted to per-attribute session GUCs via
 * {@code set_config(name, value, true)} (the JDBC-bindable, parameterizable equivalent of
 * {@code SET LOCAL}) rather than one JSON blob — simpler for a Postgres {@code CREATE POLICY} to
 * read with plain {@code current_setting('polywire.tenant', true)} instead of a JSON path
 * expression. Every attribute is namespaced under {@code polywire.} so a policy author can't
 * collide with an unrelated extension's own session GUCs.
 *
 * <p><b>{@code is_local=false} (session-scoped), not transaction-local</b> — found live: with
 * {@code is_local=true} (the {@code SET LOCAL} equivalent) the setting silently evaporates before
 * the actual query runs whenever the connection is autocommit (the common case for every current
 * call site — see {@code JdbcBackendExecutor}'s javadoc), because the {@code set_config} call and
 * the query are each their own implicit transaction, so the "local" value never survives past the
 * statement that set it. Session-scoped means the value lives for the connection's lifetime
 * instead, which is safe for every current caller (each one owns a connection scoped to a single
 * client session/request — see {@code AdHocQueryRunner}/{@code PgConnections.open} — never a
 * connection pool shared across different end users' statements); a future caller that <em>does</em>
 * hand this a pooled, cross-request connection would need to either reset these GUCs after each
 * statement or switch this class back to transaction-scoped and wrap set_config+query in one
 * explicit transaction — flagged here rather than silently assumed away.
 */
public final class PostgresRlsSessionInitializer implements NativeRlsSessionInitializer {

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        setConfig(connection, "polywire.user_id", accessContext.userId());
        for (var entry : accessContext.attributes().entrySet()) {
            setConfig(connection, "polywire." + entry.getKey(), entry.getValue());
        }
    }

    private void setConfig(Connection connection, String settingName, String value) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT set_config(?, ?, false)")) {
            stmt.setString(1, settingName);
            stmt.setString(2, value);
            stmt.execute();
        }
    }
}
