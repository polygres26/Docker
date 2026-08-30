package com.nexagres.migration.sink;

import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Sink;
import com.nexagres.wire.grpc.proto.ExecuteRequest;
import com.nexagres.wire.grpc.proto.ExecuteResponse;
import com.nexagres.wire.grpc.proto.QueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * The ONE write path every migration connector in this project uses: Polywire's own native gRPC
 * driver (<code>QueryService.Execute</code>, {@code wire/src/main/proto/polywire.proto}) -- the
 * exact same protocol a real Type-3 JDBC client speaks, not a JDBC backdoor straight to the
 * target Postgres. Every migration write goes through Polywire's real pipeline this way:
 * firewall, QoS admission control, dialect translation, and -- the concrete correctness reason
 * this matters, not just an architectural purity argument -- cache invalidation. A CDC-applied
 * write that bypassed Polywire's own {@code RowCache}/result cache would leave a stale entry a
 * live mongowire/pgwire client could read right afterward, since nothing would ever tell that
 * cache the row changed. See {@code QueryServiceImpl#execute} on the server side: the request is
 * run through the exact same {@code StatementPipeline} every other protocol uses, not a shortcut.
 *
 * <p>Plaintext gRPC for now (no TLS) -- Polywire's gRPC listener does support TLS from one PKCS12
 * keystore (see the security page), wiring this sink to use it is a real, scoped follow-up, not
 * done yet.
 */
public final class PolywireGrpcSink implements Sink, AutoCloseable {

    private final ManagedChannel channel;
    private final QueryServiceGrpc.QueryServiceBlockingStub stub;
    private final String username;
    private final String password;

    public PolywireGrpcSink(String host, int port, String username, String password) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = QueryServiceGrpc.newBlockingStub(channel);
        this.username = username;
        this.password = password;
    }

    @Override
    public void apply(ChangeEvent event) throws SQLException {
        ExecuteRequest.Builder request = ExecuteRequest.newBuilder()
                .setUsername(username)
                .setPassword(password)
                .setSql(event.sql());
        event.params().forEach(request::addParams);
        ExecuteResponse response = stub.execute(request.build());
        if (!response.getSuccess()) {
            throw new SQLException(response.getErrorMessage(),
                    response.getSqlState() == null || response.getSqlState().isBlank() ? "58000" : response.getSqlState());
        }
    }

    @Override
    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
