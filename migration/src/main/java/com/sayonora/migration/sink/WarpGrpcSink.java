package com.sayonora.migration.sink;

import com.sayonora.migration.core.ChangeEvent;
import com.sayonora.migration.core.Sink;
import com.sayonora.wire.grpc.proto.ExecuteRequest;
import com.sayonora.wire.grpc.proto.ExecuteResponse;
import com.sayonora.wire.grpc.proto.QueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The ONE write path every migration connector in this project uses: Warp's own native gRPC
 * driver (<code>QueryService.Execute</code>, {@code wire/src/main/proto/warp.proto}) -- the
 * exact same protocol a real Type-3 JDBC client speaks, not a JDBC backdoor straight to the
 * target Postgres. Every migration write goes through Warp's real pipeline this way:
 * firewall, QoS admission control, dialect translation, and -- the concrete correctness reason
 * this matters, not just an architectural purity argument -- cache invalidation. A CDC-applied
 * write that bypassed Warp's own {@code RowCache}/result cache would leave a stale entry a
 * live mongowire/pgwire client could read right afterward, since nothing would ever tell that
 * cache the row changed. See {@code QueryServiceImpl#execute} on the server side: the request is
 * run through the exact same {@code StatementPipeline} every other protocol uses, not a shortcut.
 *
 * <p>Plaintext gRPC for now (no TLS) -- Warp's gRPC listener does support TLS from one PKCS12
 * keystore (see the security page), wiring this sink to use it is a real, scoped follow-up, not
 * done yet.
 */
public final class WarpGrpcSink implements Sink, AutoCloseable {

    private final ManagedChannel channel;
    private final QueryServiceGrpc.QueryServiceBlockingStub stub;
    private final QueryServiceGrpc.QueryServiceStub asyncStub;
    private final String username;
    private final String password;

    public WarpGrpcSink(String host, int port, String username, String password) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = QueryServiceGrpc.newBlockingStub(channel);
        this.asyncStub = QueryServiceGrpc.newStub(channel);
        this.username = username;
        this.password = password;
    }

    @Override
    public void apply(ChangeEvent event) throws SQLException {
        ExecuteResponse response = stub.execute(toRequest(event));
        failIfUnsuccessful(response);
    }

    /** Pipelines the whole batch over gRPC's async stub instead of one blocking round trip per
     * row -- QueryService.Execute has no multi-statement batch RPC of its own (each call still
     * runs exactly one statement through Warp's real StatementPipeline, firewall/QoS/cache
     * invalidation included, same as {@link #apply}), so "batching" here means overlapping the
     * NETWORK round trips rather than waiting for each one before sending the next. This is the
     * throughput lever the initial bulk-sync phase actually needs: a naive one-row-per-blocking-
     * RPC sink makes round-trip latency, not the database, the bottleneck once a source is
     * partitioned for real parallel reads. */
    @Override
    public void applyBatch(List<ChangeEvent> events) throws Exception {
        if (events.isEmpty()) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(events.size());
        AtomicReference<SQLException> firstError = new AtomicReference<>();
        for (ChangeEvent event : events) {
            asyncStub.execute(toRequest(event), new StreamObserver<ExecuteResponse>() {
                @Override
                public void onNext(ExecuteResponse response) {
                    if (!response.getSuccess()) {
                        firstError.compareAndSet(null, toSqlException(response));
                    }
                }

                @Override
                public void onError(Throwable t) {
                    firstError.compareAndSet(null, new SQLException(t));
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            });
        }
        if (!latch.await(60, TimeUnit.SECONDS)) {
            throw new SQLException("timed out waiting for a batch of " + events.size() + " migration write(s)");
        }
        SQLException error = firstError.get();
        if (error != null) {
            throw error;
        }
    }

    private ExecuteRequest toRequest(ChangeEvent event) {
        ExecuteRequest.Builder request = ExecuteRequest.newBuilder()
                .setUsername(username)
                .setPassword(password)
                .setSql(event.sql());
        event.params().forEach(request::addParams);
        return request.build();
    }

    private void failIfUnsuccessful(ExecuteResponse response) throws SQLException {
        if (!response.getSuccess()) {
            throw toSqlException(response);
        }
    }

    private SQLException toSqlException(ExecuteResponse response) {
        return new SQLException(response.getErrorMessage(),
                response.getSqlState() == null || response.getSqlState().isBlank() ? "58000" : response.getSqlState());
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
