package com.nexagres.migration.core;

import java.util.List;

/**
 * What every migration connector implements -- the real generalization a massively-parallel sync
 * tool needs (Dsync's own architecture, at a high level: partition the source for parallel initial
 * sync, then hand off to a live change feed once caught up -- see this session's own
 * migration-plan discussion for the full "cutover first, CDC bridge for stragglers" reasoning).
 * Deliberately does NOT expose any source-specific type anywhere in this interface -- a {@link
 * com.nexagres.migration.coordinator.Coordinator} driving N Mongo partitions and N MySQL
 * partitions in the same run looks identical from here, and always writes through the same
 * {@link Sink} regardless of which connector produced the {@link ChangeEvent}.
 */
public interface Source extends AutoCloseable {

    /** Creates whatever target table(s) this source writes into if they don't already exist --
     * called once by the {@link com.nexagres.migration.coordinator.Coordinator} before any
     * partition is read. A no-op default since not every connector needs this (a target already
     * managed by a running Polywire instance, e.g. mongowire's own tables, may already exist from
     * prior live traffic -- {@code IF NOT EXISTS} makes this safe to call unconditionally either
     * way). */
    default void ensureTargetSchema(Sink sink) throws Exception {
    }

    /** Called once by the {@link com.nexagres.migration.coordinator.Coordinator}, after {@link
     * #ensureTargetSchema} but BEFORE any partition is read, for a source that needs to establish
     * its own live-change-feed resume point up front (Mongo change streams, DynamoDB Streams, a
     * binlog/WAL position) -- capturing that point before the (possibly long, possibly
     * multi-partition-parallel) initial snapshot starts is what guarantees a write landing on the
     * source mid-snapshot is never silently missed by {@link #streamChanges} afterward. A no-op
     * default for a source with no such concept (a plain point-in-time export). Idempotent: an
     * implementation should skip straight through if a resume point was already checkpointed by a
     * prior run. */
    default void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
    }

    /** However many partitions this source can usefully split into for parallel reads -- a
     * single-element list is always valid (no partitioning), just not maximally parallel. */
    List<Partition> listPartitions() throws Exception;

    /** Streams every {@link ChangeEvent} needed to fully copy one partition's current data to
     * {@code sink} -- the "initial sync" half of a migration. Blocks until the partition is fully
     * read.
     *
     * <p>{@code checkpoints} is passed here too, not just to {@link #streamChanges} -- a
     * change-feed-based source (Mongo change streams, DynamoDB Streams, a binlog position) needs
     * to establish and persist its OWN resume point before this method's snapshot read starts,
     * not after, or a write that happens during a long snapshot can be silently missed. Whether
     * an implementation actually needs this is its own business; a source with no such concept
     * (a plain point-in-time export) is free to ignore the parameter entirely. */
    void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception;

    /** Blocks, applying every subsequent change to {@code sink} as it happens, until {@link
     * #close()} is called from another thread -- the "CDC" half. {@code checkpoints} is where
     * resumability lives: an implementation is responsible for both saving its own resume point
     * after every applied event and resuming from a saved one on restart (see {@code MongoSource}
     * for the reference implementation of that contract). */
    void streamChanges(Sink sink, StateStore checkpoints) throws Exception;
}
