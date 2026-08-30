package com.nexagres.migration.core;

/**
 * Applies one {@link ChangeEvent}. {@code com.nexagres.migration.sink.PolywireGrpcSink} is the
 * only real implementation in this project -- every migration write goes through Polywire's own
 * native gRPC driver, never a direct backdoor to the target Postgres -- but this stays an
 * interface (mirroring the Source/Sink split real parallel-sync tools like Dsync are built
 * around) so a {@link Source} connector never depends on gRPC directly, only on this contract.
 */
public interface Sink {
    void apply(ChangeEvent event) throws Exception;
}
