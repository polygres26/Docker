package com.polygres.wire.core;

import java.sql.SQLException;

/**
 * One stage of the ordered Statement Pipeline (ARCHITECTURE.md §3/§5):
 * firewall, NL2SQL, dialect translation, stats, QoS, routing, sharding,
 * replication, cache. A stage may transform {@code statement} before
 * calling {@code next.proceed(...)}, short-circuit by returning its own
 * {@link ExecutionResult} without calling {@code next} (e.g. a firewall
 * rejection, a cache hit), or wrap the call for cross-cutting behavior
 * (e.g. stats timing). Only {@link StatsCollectorStage} has real behavior
 * so far — firewall/QoS/router/sharding/cache stages land in later phases
 * per ARCHITECTURE.md §6; until then the pipeline with zero stages is a
 * valid, purely pass-through configuration.
 */
@FunctionalInterface
public interface PipelineStage {
    ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException;
}
