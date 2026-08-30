package com.nexagres.migration.core;

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
}
