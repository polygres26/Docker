package com.polygres.wire.core;

import java.io.Serializable;

/**
 * Backend-agnostic column metadata, captured once by {@link JdbcBackendExecutor} and reused by
 * every frontend's describe-info response. {@code Serializable} so {@link ExecutionResult}
 * (which embeds a list of these) can be stored in the distributed cache (ARCHITECTURE.md §12.3,
 * {@code com.polygres.wire.cluster.CacheStage}) — Ignite serializes cache values across the network.
 */
public record ColumnInfo(String name, int jdbcType, int precision, int scale, int displaySize, boolean nullable)
        implements Serializable {
}
