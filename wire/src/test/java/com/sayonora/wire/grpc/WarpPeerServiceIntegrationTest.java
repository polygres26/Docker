package com.sayonora.wire.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.grpc.proto.JoinPartitionRequest;
import com.sayonora.wire.grpc.proto.JoinPartitionResponse;
import com.sayonora.wire.grpc.proto.Row;
import com.sayonora.wire.grpc.proto.WarpPeerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real, end-to-end proof of Phase 1a's remote-partition-join RPC primitive: a genuine self-signed
 * PKCS12 keystore (generated on the fly via the real {@code keytool} binary, the same tool {@code
 * scripts/generate-dev-cert.sh} already uses for this project's dev-TLS story), a real {@link
 * WarpPeerGrpcServer} bound to a real port with mutual TLS required, and a real mTLS {@link
 * ManagedChannel} + generated {@code WarpPeerServiceGrpc} stub acting as "the coordinator" --
 * proving the full handshake + serialization + {@code RemotePartitionJoin} matching path together,
 * not any one piece in isolation. No {@code RealPostgres}/{@code WarpProcess} needed: Phase 1a's
 * whole design point is that the peer side never touches a backend at all.
 */
class WarpPeerServiceIntegrationTest {

    private WarpPeerGrpcServer server;
    private ManagedChannel channel;

    @AfterEach
    void stopInfra() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.stop();
        }
    }

    private static Path generateSelfSignedKeystore(Path dir, String fileName, String storePassword) throws IOException, InterruptedException {
        Path keystorePath = dir.resolve(fileName);
        Process keytool = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-alias", "warp-peer-test",
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "30",
                "-keystore", keystorePath.toString(), "-storetype", "PKCS12",
                "-storepass", storePassword, "-keypass", storePassword,
                "-dname", "CN=localhost, OU=warp test, O=warp, L=Test, ST=Test, C=US")
                .redirectErrorStream(true)
                .start();
        boolean finished = keytool.waitFor(30, TimeUnit.SECONDS);
        if (!finished || keytool.exitValue() != 0) {
            String output = new String(keytool.getInputStream().readAllBytes());
            throw new IllegalStateException("keytool failed to generate a test keystore -- output: " + output);
        }
        return keystorePath;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static JoinPartitionRequest smallJoinRequest() {
        return JoinPartitionRequest.newBuilder()
                .addBuildColumnNames("id").addBuildColumnNames("name")
                .addBuildRows(row("1", "alice"))
                .addBuildRows(row("2", "bob"))
                .setBuildKeyOrdinal(0)
                .addProbeColumnNames("order_id").addProbeColumnNames("customer_id").addProbeColumnNames("amount")
                .addProbeRows(row("100", "1", "50.0"))
                .addProbeRows(row("101", "2", "75.0"))
                .addProbeRows(row("102", "999", "0.0")) // no matching build row -- must be dropped, not error
                .setProbeKeyOrdinal(1)
                .setLeftIsBuild(true)
                .build();
    }

    private static Row row(String... values) {
        Row.Builder builder = Row.newBuilder();
        for (String value : values) {
            builder.addIsNull(false);
            builder.addValues(value);
        }
        return builder.build();
    }

    @Test
    void aRealMutualTlsClientGetsCorrectlyJoinedRowsBack() throws Exception {
        String password = "test-password";
        java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory("warp-peer-test");
        Path keystorePath = generateSelfSignedKeystore(tempDir, "peer.p12", password);
        int port = findFreePort();

        server = WarpPeerGrpcServer.create(port, keystorePath.toString(), password);
        server.start();

        channel = WarpPeerGrpcServer.openPeerChannel("localhost", port, keystorePath.toString(), password);
        WarpPeerServiceGrpc.WarpPeerServiceBlockingStub stub = WarpPeerServiceGrpc.newBlockingStub(channel);

        JoinPartitionResponse response = stub.joinPartition(smallJoinRequest());

        assertTrue(response.getSuccess(), "a valid mTLS request must succeed -- error: " + response.getErrorMessage());
        assertEquals(List.of("id", "name", "order_id", "customer_id", "amount"), response.getColumnNamesList());
        assertEquals(2, response.getRowsCount(), "exactly the two matching orders, the unmatched one dropped");
    }

    @Test
    void aClientWithADifferentUntrustedCertIsRejectedByTheTlsHandshakeItself() throws Exception {
        String password = "test-password";
        java.nio.file.Path serverDir = java.nio.file.Files.createTempDirectory("warp-peer-test-server");
        java.nio.file.Path clientDir = java.nio.file.Files.createTempDirectory("warp-peer-test-client");
        Path serverKeystore = generateSelfSignedKeystore(serverDir, "server.p12", password);
        // A DIFFERENT self-signed keystore -- not signed by the same CA the server trusts, so real
        // mTLS must reject it, proving peer trust is a real cryptographic boundary, not just a flag.
        Path untrustedClientKeystore = generateSelfSignedKeystore(clientDir, "untrusted-client.p12", password);
        int port = findFreePort();

        server = WarpPeerGrpcServer.create(port, serverKeystore.toString(), password);
        server.start();

        channel = WarpPeerGrpcServer.openPeerChannel(
                "localhost", port, untrustedClientKeystore.toString(), password);
        WarpPeerServiceGrpc.WarpPeerServiceBlockingStub stub = WarpPeerServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS);

        assertThrows(StatusRuntimeException.class, () -> stub.joinPartition(smallJoinRequest()),
                "a client presenting a certificate the server doesn't trust must be rejected at the TLS "
                        + "handshake, never reach WarpPeerServiceImpl");
    }
}
