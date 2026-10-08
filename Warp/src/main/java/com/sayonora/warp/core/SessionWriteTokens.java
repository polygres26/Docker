package com.sayonora.warp.core;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-your-writes for replica routing (opt-in, {@code WARP_READ_YOUR_WRITES=true}). A fixed "stay on the primary for N ms after a write"
 * window is wrong in both directions: a replica that lags by more than the window serves a read that cannot see the session's own write,
 * and one that has long caught up is still avoided. Instead, right after a session's write the primary's log position is recorded; a read
 * may go to a replica only once that replica has APPLIED the log up to that position (a replica that has already done so is remembered, since
 * the position only moves forward). The token is per session and per backend, and is dropped if the backend's primary changes.
 *
 * <p>When the engine cannot report positions, or capturing one failed, the answer is {@link Verdict#UNKNOWN} and the caller keeps using the
 * time window, so this never makes routing less safe than before. Not thread-safe: one instance per session, like its owner.
 */
final class SessionWriteTokens {

    private static final Logger log = LoggerFactory.getLogger(SessionWriteTokens.class);

    /** Position probes; a seam so the logic can be tested without a database. */
    interface Positions {
        Optional<BigInteger> write(BackendTarget primary) throws Exception;

        Optional<BigInteger> applied(BackendTarget replica) throws Exception;
    }

    enum Verdict {
        /** This session has no write token for the backend. */
        NO_TOKEN,
        /** The replica holds the session's last write. */
        SAFE,
        /** The replica has not applied it yet. */
        BEHIND,
        /** Positions are unavailable: fall back to the time window. */
        UNKNOWN
    }

    private record Token(String primaryUrl, BigInteger position, Set<String> satisfied) {
    }

    static final Positions ENGINE = new Positions() {
        @Override
        public Optional<BigInteger> write(BackendTarget primary) throws Exception {
            EngineHa ha = EngineHa.forDialect(primary.dialect());
            return ha == null ? Optional.empty() : ha.writePosition(primary);
        }

        @Override
        public Optional<BigInteger> applied(BackendTarget replica) throws Exception {
            EngineHa ha = EngineHa.forDialect(replica.dialect());
            return ha == null ? Optional.empty() : ha.appliedPosition(replica);
        }
    };

    static boolean enabledByEnv() {
        return "true".equalsIgnoreCase(System.getenv("WARP_READ_YOUR_WRITES"));
    }

    private final Positions positions;
    // a null position means "a write happened but its position could not be captured"
    private final Map<String, Token> tokens = new HashMap<>();

    SessionWriteTokens(Positions positions) {
        this.positions = positions;
    }

    /** Records the primary's position after a write by this session. */
    void recordWrite(String backend, BackendTarget primary) {
        BigInteger position = null;
        try {
            position = positions.write(primary).orElse(null);
        } catch (Exception e) {
            log.debug("read-your-writes: could not read the log position of {}: {}", backend, e.toString());
        }
        tokens.put(backend, new Token(primary.jdbcUrl(), position, new HashSet<>()));
    }

    Verdict check(String backend, BackendTarget primary, BackendTarget replica) {
        Token t = tokens.get(backend);
        if (t == null || !t.primaryUrl().equals(primary.jdbcUrl())) {
            if (t != null) {
                tokens.remove(backend); // the primary changed (failover): its positions mean nothing for the new one
            }
            return Verdict.NO_TOKEN;
        }
        if (t.position() == null) {
            return Verdict.UNKNOWN;
        }
        if (t.satisfied().contains(replica.jdbcUrl())) {
            return Verdict.SAFE;
        }
        try {
            Optional<BigInteger> applied = positions.applied(replica);
            if (applied.isEmpty()) {
                return Verdict.UNKNOWN;
            }
            if (applied.get().compareTo(t.position()) >= 0) {
                t.satisfied().add(replica.jdbcUrl());
                return Verdict.SAFE;
            }
            return Verdict.BEHIND;
        } catch (Exception e) {
            log.debug("read-your-writes: could not read the applied position of {}: {}", replica.jdbcUrl(), e.toString());
            return Verdict.UNKNOWN;
        }
    }
}
