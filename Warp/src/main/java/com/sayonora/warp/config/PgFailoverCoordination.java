package com.sayonora.warp.config;

import com.sayonora.warp.core.FailoverCoordination;
import com.sayonora.warp.pgwire.PgConnections;
import com.sayonora.warp.server.ServerOptions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.UUID;

/**
 * {@link FailoverCoordination} on the config Postgres. The lease is one row per backend, taken with a
 * single atomic upsert that only succeeds if the row is free, expired (database clock) or already
 * ours; a takeover bumps the term so a stale holder can be told apart. Votes are one row per
 * (backend, instance), refreshed by every monitor pass.
 */
public final class PgFailoverCoordination implements FailoverCoordination {

    private final ServerOptions options;
    private final String holder = UUID.randomUUID().toString();

    public PgFailoverCoordination(ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() throws SQLException {
        try (Connection c = PgConnections.open(options); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS warp_failover_lease ("
                    + "backend text PRIMARY KEY, holder text NOT NULL, term bigint NOT NULL, "
                    + "expires_at timestamptz NOT NULL)");
            st.execute("CREATE TABLE IF NOT EXISTS warp_failover_observation ("
                    + "backend text NOT NULL, instance text NOT NULL, primary_url text NOT NULL, "
                    + "down boolean NOT NULL, observed_at timestamptz NOT NULL DEFAULT now(), "
                    + "PRIMARY KEY (backend, instance))");
        }
    }

    @Override
    public void publishObservation(String backend, String primaryUrl, boolean down) throws SQLException {
        try (Connection c = PgConnections.open(options);
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO warp_failover_observation (backend, instance, primary_url, down, observed_at) "
                                + "VALUES (?, ?, ?, ?, now()) ON CONFLICT (backend, instance) DO UPDATE SET "
                                + "primary_url = EXCLUDED.primary_url, down = EXCLUDED.down, observed_at = now()")) {
            ps.setString(1, backend);
            ps.setString(2, holder);
            ps.setString(3, primaryUrl);
            ps.setBoolean(4, down);
            ps.executeUpdate();
        }
    }

    @Override
    public Optional<Long> tryAcquireLease(String backend, long ttlSeconds) throws SQLException {
        try (Connection c = PgConnections.open(options);
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO warp_failover_lease (backend, holder, term, expires_at) "
                                + "VALUES (?, ?, 1, now() + (? || ' seconds')::interval) "
                                + "ON CONFLICT (backend) DO UPDATE SET holder = EXCLUDED.holder, "
                                + "term = CASE WHEN warp_failover_lease.holder = EXCLUDED.holder "
                                + "THEN warp_failover_lease.term ELSE warp_failover_lease.term + 1 END, "
                                + "expires_at = EXCLUDED.expires_at "
                                + "WHERE warp_failover_lease.holder = EXCLUDED.holder "
                                + "OR warp_failover_lease.expires_at < now() RETURNING term")) {
            ps.setString(1, backend);
            ps.setString(2, holder);
            ps.setString(3, String.valueOf(ttlSeconds));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
            }
        }
    }

    @Override
    public void releaseLease(String backend, long term) throws SQLException {
        try (Connection c = PgConnections.open(options);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE warp_failover_lease SET expires_at = now() - interval '1 second' "
                                + "WHERE backend = ? AND holder = ? AND term = ?")) {
            ps.setString(1, backend);
            ps.setString(2, holder);
            ps.setLong(3, term);
            ps.executeUpdate();
        }
    }

    @Override
    public int votesPrimaryDown(String backend, String primaryUrl, long freshSeconds) throws SQLException {
        try (Connection c = PgConnections.open(options);
                PreparedStatement ps = c.prepareStatement(
                        "SELECT count(*) FROM warp_failover_observation WHERE backend = ? AND primary_url = ? "
                                + "AND down AND observed_at > now() - (? || ' seconds')::interval")) {
            ps.setString(1, backend);
            ps.setString(2, primaryUrl);
            ps.setString(3, String.valueOf(freshSeconds));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    @Override
    public int liveInstances() throws SQLException {
        return Math.max(1, NodeRegistry.countLive(options));
    }
}
