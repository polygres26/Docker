package com.sayonora.warp.datastorewire;

import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.BeginTransactionResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.DatastoreGrpc;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.ReserveIdsRequest;
import com.google.datastore.v1.ReserveIdsResponse;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RollbackResponse;
import com.google.datastore.v1.RunAggregationQueryRequest;
import com.google.datastore.v1.RunAggregationQueryResponse;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The {@code google.datastore.v1.Datastore} gRPC service on top of {@link DsService}. */
final class DsGrpc extends DatastoreGrpc.DatastoreImplBase {

    private static final Logger log = LoggerFactory.getLogger(DsGrpc.class);

    private final DsService svc;
    private final DsWireServer.Hooks hooks;

    DsGrpc(DsService svc, DsWireServer.Hooks hooks) {
        this.svc = svc;
        this.hooks = hooks;
    }

    private <T> void unary(String op, boolean write, StreamObserver<T> obs, Supplier<T> fn) {
        long t0 = System.nanoTime();
        try {
            obs.onNext(fn.get());
            obs.onCompleted();
        } catch (DsException e) {
            obs.onError(e.toStatus().asRuntimeException());
        } catch (RuntimeException e) {
            log.error("datastorewire {} failed", op, e);
            obs.onError(Status.INTERNAL.withDescription("Internal error: " + e).asRuntimeException());
        } finally {
            hooks.record(op, write, System.nanoTime() - t0);
        }
    }

    @Override
    public void lookup(LookupRequest r, StreamObserver<LookupResponse> o) {
        unary("Lookup", false, o, () -> svc.lookup(r));
    }

    @Override
    public void runQuery(RunQueryRequest r, StreamObserver<RunQueryResponse> o) {
        unary("RunQuery", false, o, () -> svc.runQuery(r));
    }

    @Override
    public void runAggregationQuery(RunAggregationQueryRequest r, StreamObserver<RunAggregationQueryResponse> o) {
        unary("RunAggregationQuery", false, o, () -> svc.runAggregation(r));
    }

    @Override
    public void beginTransaction(BeginTransactionRequest r, StreamObserver<BeginTransactionResponse> o) {
        unary("BeginTransaction", false, o, () -> svc.beginTransaction(r));
    }

    @Override
    public void commit(CommitRequest r, StreamObserver<CommitResponse> o) {
        unary("Commit", true, o, () -> svc.commit(r));
    }

    @Override
    public void rollback(RollbackRequest r, StreamObserver<RollbackResponse> o) {
        unary("Rollback", false, o, () -> svc.rollback(r));
    }

    @Override
    public void allocateIds(AllocateIdsRequest r, StreamObserver<AllocateIdsResponse> o) {
        unary("AllocateIds", true, o, () -> svc.allocateIds(r));
    }

    @Override
    public void reserveIds(ReserveIdsRequest r, StreamObserver<ReserveIdsResponse> o) {
        unary("ReserveIds", true, o, () -> svc.reserveIds(r));
    }
}
