package com.sayonora.warp.tls.acme;

import com.sayonora.warp.pgwire.PgConnections;
import com.sayonora.warp.secrets.FieldCipher;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Control-plane sharing for multi-node deployments: rows {@code account:<hash>} and {@code certificate:<primary domain>} in
 * {@code warp_acme_state}, values encrypted with {@link FieldCipher} (SAYONORA_ENCRYPTION_KEY), and a session-level Postgres
 * advisory lock that elects the single node allowed to talk to the CA.
 */
public final class AcmeDb {

    private final ServerOptions options;
    private final long lockKey;

    public AcmeDb(ServerOptions options, String primaryDomain) {
        this.options = options;
        this.lockKey = 0x5741_5250_0000_0000L ^ primaryDomain.hashCode();
    }

    public void ensureSchema() throws SQLException {
        try (Connection c = PgConnections.open(options); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS warp_acme_state (name text PRIMARY KEY, value text NOT NULL, "
                    + "updated_at timestamptz NOT NULL DEFAULT now())");
        }
    }

    /** @return the decrypted value, or null. */
    public String get(String name) throws SQLException {
        try (Connection c = PgConnections.open(options); PreparedStatement ps = c.prepareStatement("SELECT value FROM warp_acme_state WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? FieldCipher.decrypt(rs.getString(1)) : null;
            }
        }
    }

    public void put(String name, String plaintext) throws SQLException {
        try (Connection c = PgConnections.open(options); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO warp_acme_state(name, value, updated_at) VALUES (?, ?, now()) "
                        + "ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = now()")) {
            ps.setString(1, name);
            ps.setString(2, FieldCipher.encrypt(plaintext));
            ps.executeUpdate();
        }
    }

    /** A held advisory lock; {@link #close()} releases it (closing the session releases it too, e.g. after a crash). */
    public static final class Lock implements AutoCloseable {
        private final Connection conn;

        Lock(Connection c) {
            this.conn = c;
        }

        @Override
        public void close() {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // the server drops session locks with the session
            }
        }
    }

    /** @return the lock, or null when another node holds it. */
    public Lock tryLock() throws SQLException {
        Connection c = PgConnections.openRaw(options);
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            ps.setLong(1, lockKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getBoolean(1)) {
                    return new Lock(c);
                }
            }
        } catch (SQLException | RuntimeException e) {
            c.close();
            throw e;
        }
        c.close();
        return null;
    }
}
