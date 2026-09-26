package com.sayonora.wire.firestorewire;

import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.BeginTransactionRequest;
import com.google.firestore.v1.BeginTransactionResponse;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CommitResponse;
import com.google.firestore.v1.CreateDocumentRequest;
import com.google.firestore.v1.DeleteDocumentRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.FirestoreGrpc;
import com.google.firestore.v1.GetDocumentRequest;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListCollectionIdsResponse;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.ListenRequest;
import com.google.firestore.v1.ListenResponse;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.RollbackRequest;
import com.google.firestore.v1.RunAggregationQueryRequest;
import com.google.firestore.v1.RunAggregationQueryResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.UpdateDocumentRequest;
import com.google.firestore.v1.WriteRequest;
import com.google.firestore.v1.WriteResponse;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The {@code google.firestore.v1.Firestore} gRPC service on top of {@link FsService}. */
final class FsGrpc extends FirestoreGrpc.FirestoreImplBase {

    private static final Logger log = LoggerFactory.getLogger(FsGrpc.class);

    private final FsService svc;
    private final FsStreams streams;
    private final FsWireServer.Hooks hooks;

    FsGrpc(FsService svc, FsStreams streams, FsWireServer.Hooks hooks) {
        this.svc = svc;
        this.streams = streams;
        this.hooks = hooks;
    }

    private <T> void unary(String op, boolean write, StreamObserver<T> obs, Supplier<T> fn) {
        long t0 = System.nanoTime();
        try {
            T r = fn.get();
            obs.onNext(r);
            obs.onCompleted();
        } catch (FsException e) {
            obs.onError(e.toStatus().asRuntimeException());
        } catch (RuntimeException e) {
            log.error("firestorewire {} failed", op, e);
            obs.onError(Status.INTERNAL.withDescription("Internal error: " + e).asRuntimeException());
        } finally {
            hooks.record(op, write, System.nanoTime() - t0);
        }
    }

    @Override
    public void getDocument(GetDocumentRequest r, StreamObserver<Document> o) {
        unary("GetDocument", false, o, () -> svc.getDocument(r));
    }

    @Override
    public void listDocuments(ListDocumentsRequest r, StreamObserver<ListDocumentsResponse> o) {
        unary("ListDocuments", false, o, () -> svc.listDocuments(r));
    }

    @Override
    public void createDocument(CreateDocumentRequest r, StreamObserver<Document> o) {
        unary("CreateDocument", true, o, () -> svc.createDocument(r));
    }

    @Override
    public void updateDocument(UpdateDocumentRequest r, StreamObserver<Document> o) {
        unary("UpdateDocument", true, o, () -> svc.updateDocument(r));
    }

    @Override
    public void deleteDocument(DeleteDocumentRequest r, StreamObserver<Empty> o) {
        unary("DeleteDocument", true, o, () -> {
            svc.deleteDocument(r);
            return Empty.getDefaultInstance();
        });
    }

    @Override
    public void batchGetDocuments(BatchGetDocumentsRequest r, StreamObserver<BatchGetDocumentsResponse> o) {
        long t0 = System.nanoTime();
        try {
            svc.batchGet(r, o::onNext);
            o.onCompleted();
        } catch (FsException e) {
            o.onError(e.toStatus().asRuntimeException());
        } catch (RuntimeException e) {
            log.error("firestorewire BatchGetDocuments failed", e);
            o.onError(Status.INTERNAL.withDescription("Internal error: " + e).asRuntimeException());
        } finally {
            hooks.record("BatchGetDocuments", false, System.nanoTime() - t0);
        }
    }

    @Override
    public void beginTransaction(BeginTransactionRequest r, StreamObserver<BeginTransactionResponse> o) {
        unary("BeginTransaction", false, o, () -> {
            FsNames.Db db = FsNames.parseDb(r.getDatabase());
            var t = svc.begin(db, r.getOptions());
            return BeginTransactionResponse.newBuilder().setTransaction(t.id).build();
        });
    }

    @Override
    public void commit(CommitRequest r, StreamObserver<CommitResponse> o) {
        unary("Commit", true, o, () -> svc.commit(r));
    }

    @Override
    public void rollback(RollbackRequest r, StreamObserver<Empty> o) {
        unary("Rollback", false, o, () -> {
            svc.rollback(FsNames.parseDb(r.getDatabase()), r.getTransaction());
            return Empty.getDefaultInstance();
        });
    }

    @Override
    public void runQuery(RunQueryRequest r, StreamObserver<RunQueryResponse> o) {
        long t0 = System.nanoTime();
        try {
            ServerCallStreamObserver<RunQueryResponse> so = (ServerCallStreamObserver<RunQueryResponse>) o;
            svc.runQuery(r, x -> {
                if (so.isCancelled()) {
                    throw new FsException(Status.Code.CANCELLED, "cancelled");
                }
                o.onNext(x);
            });
            o.onCompleted();
        } catch (FsException e) {
            if (e.code != Status.Code.CANCELLED) {
                o.onError(e.toStatus().asRuntimeException());
            }
        } catch (RuntimeException e) {
            log.error("firestorewire RunQuery failed", e);
            o.onError(Status.INTERNAL.withDescription("Internal error: " + e).asRuntimeException());
        } finally {
            hooks.record("RunQuery", false, System.nanoTime() - t0);
        }
    }

    @Override
    public void runAggregationQuery(RunAggregationQueryRequest r, StreamObserver<RunAggregationQueryResponse> o) {
        unary("RunAggregationQuery", false, o, () -> svc.runAggregation(r));
    }

    @Override
    public void partitionQuery(PartitionQueryRequest r, StreamObserver<PartitionQueryResponse> o) {
        unary("PartitionQuery", false, o, () -> svc.partitionQuery(r));
    }

    @Override
    public void listCollectionIds(ListCollectionIdsRequest r, StreamObserver<ListCollectionIdsResponse> o) {
        unary("ListCollectionIds", false, o, () -> svc.listCollectionIds(r));
    }

    @Override
    public void batchWrite(BatchWriteRequest r, StreamObserver<BatchWriteResponse> o) {
        unary("BatchWrite", true, o, () -> svc.batchWrite(r));
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> o) {
        return streams.write(o);
    }

    @Override
    public StreamObserver<ListenRequest> listen(StreamObserver<ListenResponse> o) {
        return streams.listen(o);
    }
}
