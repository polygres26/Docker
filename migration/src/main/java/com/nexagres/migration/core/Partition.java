package com.nexagres.migration.core;

/**
 * A source-defined, opaque unit of parallel work -- a ROWID range, a hash bucket, a shard-key
 * range, or (for a source too small/simple to partition, like a single MongoDB collection in
 * v1) the whole source as one partition. {@link com.nexagres.migration.coordinator.Coordinator}
 * never looks inside {@code descriptor}; it only exists for a {@link Source} implementation's own
 * {@link Source#readPartition} to interpret.
 */
public record Partition(String id, Object descriptor) {
}
