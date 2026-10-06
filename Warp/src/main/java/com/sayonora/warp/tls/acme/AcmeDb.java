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
            com.sayonora.warp.core.DdlTemplates.run(st, "postgres", "warp_acme_state", java.util.Map.of());
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

    /** How the stored ACME state is protected: label ({@code plaintext}, {@code encv1}, a key id) to row count; empty if there is none. */
    public static java.util.Map<String, Integer> protection(ServerOptions options) throws SQLException {
        java.util.Map<String, Integer> counts = new java.util.LinkedHashMap<>();
        try (Connection c = PgConnections.open(options); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT value FROM warp_acme_state")) {
            while (rs.next()) {
                counts.merge(FieldCipher.protection(rs.getString(1)), 1, Integer::sum);
            }
        } catch (SQLException e) {
            if ("42P01".equals(e.getSQLState())) { // ACME was never used: no table
                return counts;
            }
            throw e;
        }
        return counts;
    }

    /** Re-encrypts every stored value under the active key; returns how many rows changed (0 when ACME was never used). */
    public static int reencryptAll(ServerOptions options) throws SQLException {
        int changed = 0;
        try (Connection c = PgConnections.open(options)) {
            java.util.List<String[]> rows = new java.util.ArrayList<>();
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT name, value FROM warp_acme_state")) {
                while (rs.next()) {
                    rows.add(new String[] {rs.getString(1), rs.getString(2)});
                }
            } catch (SQLException e) {
                if ("42P01".equals(e.getSQLState())) {
                    return 0;
                }
                throw e;
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_acme_state SET value = ?, updated_at = now() WHERE name = ?")) {
                for (String[] r : rows) {
                    String fresh = FieldCipher.reencrypt(r[1]);
                    if (!fresh.equals(r[1])) {
                        ps.setString(1, fresh);
                        ps.setString(2, r[0]);
                        ps.executeUpdate();
                        changed++;
                    }
                }
            }
        }
        return changed;
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
