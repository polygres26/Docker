package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Pure, no-network unit coverage for {@link PeerChannelPool}'s reuse/eviction semantics. Doesn't
 * need a real listening peer -- {@link ManagedChannel} construction (via {@code
 * WarpPeerGrpcServer#openPeerChannel}) is lazy; it never actually dials anything until an RPC is
 * made, so this can prove identity/caching behavior without any real gRPC traffic. Reuses the same
 * on-the-fly {@code keytool} self-signed keystore approach {@code WarpPeerServiceIntegrationTest}
 * already established.
 */
class PeerChannelPoolTest {

    private static java.nio.file.Path generateSelfSignedKeystore(java.nio.file.Path dir, String password) throws Exception {
        java.nio.file.Path keystorePath = dir.resolve("peer-pool-test.p12");
        Process keytool = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-alias", "warp-peer-pool-test",
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "30",
                "-keystore", keystorePath.toString(), "-storetype", "PKCS12",
                "-storepass", password, "-keypass", password,
                "-dname", "CN=localhost, OU=warp test, O=warp, L=Test, ST=Test, C=US")
                .redirectErrorStream(true)
                .start();
        boolean finished = keytool.waitFor(30, TimeUnit.SECONDS);
        if (!finished || keytool.exitValue() != 0) {
            throw new IllegalStateException("keytool failed: " + new String(keytool.getInputStream().readAllBytes()));
        }
        return keystorePath;
    }

    @Test
    void getOrCreateReturnsTheSameChannelInstanceForTheSamePeerAcrossCalls() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("peer-pool-test");
        java.nio.file.Path keystore = generateSelfSignedKeystore(dir, "test-password");

        ManagedChannel first = PeerChannelPool.getOrCreate("localhost", 54321, keystore.toString(), "test-password");
        try {
            ManagedChannel second = PeerChannelPool.getOrCreate("localhost", 54321, keystore.toString(), "test-password");
            assertSame(first, second, "the same (host, port) must reuse the SAME channel instance, not open a new one");

            // A DIFFERENT port is a different peer -- must never share a channel with the first.
            ManagedChannel differentPeer = PeerChannelPool.getOrCreate("localhost", 54322, keystore.toString(), "test-password");
            try {
                assertNotSame(first, differentPeer);
            } finally {
                differentPeer.shutdownNow();
            }
        } finally {
            first.shutdownNow();
        }
    }

    @Test
    void evictForcesTheNextGetOrCreateToBuildAFreshChannel() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("peer-pool-test");
        java.nio.file.Path keystore = generateSelfSignedKeystore(dir, "test-password");

        ManagedChannel first = PeerChannelPool.getOrCreate("localhost", 54323, keystore.toString(), "test-password");
        PeerChannelPool.evict("localhost", 54323);
        ManagedChannel second = PeerChannelPool.getOrCreate("localhost", 54323, keystore.toString(), "test-password");
        try {
            assertNotSame(first, second, "after eviction, the next call must build a genuinely new channel");
            assertEquals(true, first.isShutdown(), "the evicted channel must actually be shut down, not just forgotten");
        } finally {
            second.shutdownNow();
        }
    }
}
