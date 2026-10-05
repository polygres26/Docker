package com.sayonora.warp.core;

import java.util.Optional;

/**
 * What promote mode needs from the shared control plane so that several Warp instances never
 * promote at once and never promote on a single instance's private view: a per-backend lease, and
 * a tally of which instances currently see the primary as down. Implemented on the config
 * Postgres ({@code PgFailoverCoordination}); when that database is unreachable every method throws
 * and Warp does NOT promote -- a promotion that cannot be coordinated is refused, not guessed.
 */
public interface FailoverCoordination {

    /** Records that this instance currently sees {@code primaryUrl} as down ({@code down=true}) or
     * writable ({@code down=false}). */
    void publishObservation(String backend, String primaryUrl, boolean down) throws Exception;

    /** Tries to take (or renew, if already ours) the promotion lease for {@code backend}; returns the
     * lease term when held, empty when another instance holds an unexpired lease. Expiry uses the
     * database clock, not any Warp host's. */
    Optional<Long> tryAcquireLease(String backend, long ttlSeconds) throws Exception;

    void releaseLease(String backend, long term) throws Exception;

    /** Instances (including this one) that reported {@code primaryUrl} down within the last
     * {@code freshSeconds}. */
    int votesPrimaryDown(String backend, String primaryUrl, long freshSeconds) throws Exception;

    /** Warp instances with a recent heartbeat -- the electorate for the majority rule. */
    int liveInstances() throws Exception;
}
