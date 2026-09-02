package com.sayonora.migration.core;

import java.util.List;

/**
 * Applies one {@link ChangeEvent}. {@code com.sayonora.migration.sink.WarpGrpcSink} is the
 * only real implementation in this project -- every migration write goes through Warp's own
 * native gRPC driver, never a direct backdoor to the target Postgres -- but this stays an
 * interface (mirroring the Source/Sink split real parallel-sync tools like Dsync are built
 * around) so a {@link Source} connector never depends on gRPC directly, only on this contract.
 */
public interface Sink {
    void apply(ChangeEvent event) throws Exception;

    /** Applies a batch of events, in order. The default just calls {@link #apply} once per event
     * -- correct but strictly sequential (one gRPC round trip per row). A real implementation
     * (see {@code WarpGrpcSink}) is expected to override this to pipeline the batch instead --
     * the single biggest throughput lever for the initial bulk-sync phase, since a naive one-row-
     * per-RPC sink means the network round trip, not the database, is the bottleneck. */
    default void applyBatch(List<ChangeEvent> events) throws Exception {
        for (ChangeEvent event : events) {
            apply(event);
        }
    }
}
