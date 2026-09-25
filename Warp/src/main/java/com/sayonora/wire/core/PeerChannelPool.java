package com.sayonora.wire.core;

import com.sayonora.wire.grpc.WarpPeerGrpcServer;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Phase 2 of the "Warp-native parallel execution engine" design: reuses one mTLS {@link
 * ManagedChannel} per peer across every partition dispatch and every query, instead of {@link
 * ParallelJoinExecutor}'s original Phase 1b behavior of opening (and immediately tearing down) a
 * fresh channel per partition per call. A real TLS handshake is genuine, non-trivial cost -- paying
 * it once per peer and amortizing it across every subsequent dispatch is the actual efficiency win;
 * gRPC's own {@link ManagedChannel} already handles transient reconnection internally (Netty
 * transport-level retry), which is exactly why a long-lived, reused channel is the right shape here
 * rather than something this class needs to reimplement.
 *
 * <p>Keyed by {@code host:port} -- process-wide, not per-query -- since the same peer is reused
 * across every federated join that happens to route work to it. Never proactively closed on a
 * successful path (there's no query-scoped or process-shutdown hook wired to it yet, matching how
 * {@code WarpGrpcServer}/{@code WarpPeerGrpcServer} themselves are never explicitly stopped in
 * {@code Main} either -- the JVM's own exit reclaims it); {@link #evict} is called by {@link
 * ParallelJoinExecutor} only when a dispatch actually FAILS, so a channel that's gone genuinely bad
 * (not just a transient blip gRPC's own retry already absorbs) doesn't stay cached forever.
 */
final class PeerChannelPool {

    private static final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    private PeerChannelPool() {
    }

    static ManagedChannel getOrCreate(String host, int port, String keystorePath, String keystorePassword)
            throws GeneralSecurityException, IOException {
        String key = key(host, port);
        ManagedChannel existing = channels.get(key);
        if (existing != null && !existing.isShutdown() && !existing.isTerminated()) {
            return existing;
        }
        ManagedChannel created = WarpPeerGrpcServer.openPeerChannel(host, port, keystorePath, keystorePassword);
        ManagedChannel prior = channels.putIfAbsent(key, created);
        if (prior != null) {
            // Lost a race with another thread creating the same peer's channel concurrently --
            // keep the one that's already published, discard this one rather than leaking it.
            created.shutdownNow();
            return prior;
        }
        return created;
    }

    /** Removes (and shuts down) a peer's cached channel after a real dispatch failure, so the NEXT
     * attempt to that peer builds a fresh one instead of retrying against a connection that's
     * already proven bad -- e.g. the peer's own process restarted with a new identity, or the
     * network path genuinely changed, not just a momentary blip gRPC's own transport-level retry
     * would have absorbed without ever surfacing as a dispatch failure here. */
    static void evict(String host, int port) {
        ManagedChannel removed = channels.remove(key(host, port));
        if (removed != null) {
            removed.shutdownNow();
        }
    }

    private static String key(String host, int port) {
        return host + ":" + port;
    }
}
