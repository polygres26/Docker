package com.nexagres.migration.core;

import java.time.Instant;

/**
 * Durable resume-point storage, one row per source key. {@code
 * com.nexagres.migration.checkpoint.CdcCheckpointStore} is the only implementation today, backed
 * by a plain Postgres table; kept as an interface so a future connector or deployment isn't
 * hard-wired to that one choice.
 */
public interface StateStore {

    /** {@code null} means this source has never been checkpointed -- the caller's own signal to
     * run a fresh initial snapshot instead of resuming a stream from a point that doesn't exist. */
    String load(String sourceKey) throws Exception;

    void save(String sourceKey, String token) throws Exception;

    /** Same as {@link #save(String, String)}, plus the SOURCE's own timestamp for the event that
     * produced this checkpoint (a Mongo change event's {@code clusterTime}, a binlog event's
     * commit time, etc.) -- what makes a real change-feed LAG metric possible ({@code now() -
     * eventTimestamp}), not just "was a checkpoint saved recently" (which only proves the worker
     * process is alive, not that it's caught up). Defaults to the two-arg {@link #save}, silently
     * dropping the timestamp, so a connector or StateStore that doesn't (yet) care about lag isn't
     * forced to implement this. */
    default void save(String sourceKey, String token, Instant eventTimestamp) throws Exception {
        save(sourceKey, token);
    }
}
