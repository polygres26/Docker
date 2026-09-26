package com.sayonora.wire.bigtablewire;

import com.google.longrunning.Operation;
import com.google.protobuf.Empty;
import com.sayonora.wire.bigtablewire.admin.v2.BigtableTableAdminGrpc;
import com.sayonora.wire.bigtablewire.admin.v2.CheckConsistencyRequest;
import com.sayonora.wire.bigtablewire.admin.v2.CheckConsistencyResponse;
import com.sayonora.wire.bigtablewire.admin.v2.CreateTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.DeleteTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.DropRowRangeRequest;
import com.sayonora.wire.bigtablewire.admin.v2.GenerateConsistencyTokenRequest;
import com.sayonora.wire.bigtablewire.admin.v2.GenerateConsistencyTokenResponse;
import com.sayonora.wire.bigtablewire.admin.v2.GetTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.ListTablesRequest;
import com.sayonora.wire.bigtablewire.admin.v2.ListTablesResponse;
import com.sayonora.wire.bigtablewire.admin.v2.ModifyColumnFamiliesRequest;
import com.sayonora.wire.bigtablewire.admin.v2.Table;
import com.sayonora.wire.bigtablewire.admin.v2.UpdateTableRequest;
import com.sayonora.wire.bigtablewire.v2.BigtableGrpc;
import com.sayonora.wire.bigtablewire.v2.CheckAndMutateRowRequest;
import com.sayonora.wire.bigtablewire.v2.CheckAndMutateRowResponse;
import com.sayonora.wire.bigtablewire.v2.MutateRowRequest;
import com.sayonora.wire.bigtablewire.v2.MutateRowResponse;
import com.sayonora.wire.bigtablewire.v2.MutateRowsRequest;
import com.sayonora.wire.bigtablewire.v2.MutateRowsResponse;
import com.sayonora.wire.bigtablewire.v2.PingAndWarmRequest;
import com.sayonora.wire.bigtablewire.v2.PingAndWarmResponse;
import com.sayonora.wire.bigtablewire.v2.ReadModifyWriteRowRequest;
import com.sayonora.wire.bigtablewire.v2.ReadModifyWriteRowResponse;
import com.sayonora.wire.bigtablewire.v2.ReadRowsRequest;
import com.sayonora.wire.bigtablewire.v2.ReadRowsResponse;
import com.sayonora.wire.bigtablewire.v2.SampleRowKeysRequest;
import com.sayonora.wire.bigtablewire.v2.SampleRowKeysResponse;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The gRPC surface: google.bigtable.v2.Bigtable and google.bigtable.admin.v2.BigtableTableAdmin (unimplemented methods answer UNIMPLEMENTED). */
final class BtGrpc {

    private static final Logger log = LoggerFactory.getLogger(BtGrpc.class);

    @FunctionalInterface
    interface Recorder {
        void record(String op, boolean write, long nanos);
    }

    private final BtData data;
    private final BtAdmin admin;
    private final Recorder recorder;

    BtGrpc(BtData data, BtAdmin admin, Recorder recorder) {
        this.data = data;
        this.admin = admin;
        this.recorder = recorder;
    }

    List<ServerServiceDefinition> services() {
        return List.of(new DataService().bindService(), new AdminService().bindService());
    }

    private <Q, R> void unary(String op, boolean write, Q req, StreamObserver<R> obs, Function<Q, R> f) {
        long t0 = System.nanoTime();
        try {
            R r = f.apply(req);
            obs.onNext(r);
            obs.onCompleted();
        } catch (BtException e) {
            obs.onError(e.toGrpc());
        } catch (RuntimeException e) {
            log.error("bigtablewire {} failed", op, e);
            obs.onError(Status.INTERNAL.withDescription("Internal error").asRuntimeException());
        } finally {
            recorder.record(op, write, System.nanoTime() - t0);
        }
    }

    private <Q, R> void stream(String op, Q req, StreamObserver<R> obs, java.util.function.BiConsumer<Q, ServerCallStreamObserver<R>> f) {
        long t0 = System.nanoTime();
        try {
            f.accept(req, (ServerCallStreamObserver<R>) obs);
        } catch (BtException e) {
            obs.onError(e.toGrpc());
        } catch (RuntimeException e) {
            log.error("bigtablewire {} failed", op, e);
            obs.onError(Status.INTERNAL.withDescription("Internal error").asRuntimeException());
        } finally {
            recorder.record(op, false, System.nanoTime() - t0);
        }
    }

    private final class DataService extends BigtableGrpc.BigtableImplBase {
        @Override
        public void readRows(ReadRowsRequest req, StreamObserver<ReadRowsResponse> obs) {
            stream("ReadRows", req, obs, data::readRows);
        }

        @Override
        public void sampleRowKeys(SampleRowKeysRequest req, StreamObserver<SampleRowKeysResponse> obs) {
            stream("SampleRowKeys", req, obs, data::sampleRowKeys);
        }

        @Override
        public void mutateRow(MutateRowRequest req, StreamObserver<MutateRowResponse> obs) {
            unary("MutateRow", true, req, obs, data::mutateRow);
        }

        @Override
        public void mutateRows(MutateRowsRequest req, StreamObserver<MutateRowsResponse> obs) {
            unary("MutateRows", true, req, obs, data::mutateRows); // server-streaming: one message with every entry
        }

        @Override
        public void checkAndMutateRow(CheckAndMutateRowRequest req, StreamObserver<CheckAndMutateRowResponse> obs) {
            unary("CheckAndMutateRow", true, req, obs, data::checkAndMutateRow);
        }

        @Override
        public void readModifyWriteRow(ReadModifyWriteRowRequest req, StreamObserver<ReadModifyWriteRowResponse> obs) {
            unary("ReadModifyWriteRow", true, req, obs, data::readModifyWriteRow);
        }

        @Override
        public void pingAndWarm(PingAndWarmRequest req, StreamObserver<PingAndWarmResponse> obs) {
            unary("PingAndWarm", false, req, obs, r -> PingAndWarmResponse.getDefaultInstance());
        }
    }

    private final class AdminService extends BigtableTableAdminGrpc.BigtableTableAdminImplBase {
        @Override
        public void createTable(CreateTableRequest req, StreamObserver<Table> obs) {
            unary("CreateTable", true, req, obs, admin::createTable);
        }

        @Override
        public void getTable(GetTableRequest req, StreamObserver<Table> obs) {
            unary("GetTable", false, req, obs, admin::getTable);
        }

        @Override
        public void listTables(ListTablesRequest req, StreamObserver<ListTablesResponse> obs) {
            unary("ListTables", false, req, obs, admin::listTables);
        }

        @Override
        public void deleteTable(DeleteTableRequest req, StreamObserver<Empty> obs) {
            unary("DeleteTable", true, req, obs, admin::deleteTable);
        }

        @Override
        public void modifyColumnFamilies(ModifyColumnFamiliesRequest req, StreamObserver<Table> obs) {
            unary("ModifyColumnFamilies", true, req, obs, admin::modifyColumnFamilies);
        }

        @Override
        public void dropRowRange(DropRowRangeRequest req, StreamObserver<Empty> obs) {
            unary("DropRowRange", true, req, obs, admin::dropRowRange);
        }

        @Override
        public void generateConsistencyToken(GenerateConsistencyTokenRequest req, StreamObserver<GenerateConsistencyTokenResponse> obs) {
            unary("GenerateConsistencyToken", false, req, obs, admin::generateConsistencyToken);
        }

        @Override
        public void checkConsistency(CheckConsistencyRequest req, StreamObserver<CheckConsistencyResponse> obs) {
            unary("CheckConsistency", false, req, obs, admin::checkConsistency);
        }

        @Override
        public void updateTable(UpdateTableRequest req, StreamObserver<Operation> obs) {
            unary("UpdateTable", true, req, obs, admin::updateTable);
        }
    }

    /** Requires {@code authorization: Bearer <token>} when tokens are configured. */
    static ServerInterceptor auth(Set<String> tokens) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                if (!tokens.isEmpty()) {
                    String h = headers.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER));
                    if (h == null || !h.regionMatches(true, 0, "Bearer ", 0, 7) || !tokens.contains(h.substring(7).trim())) {
                        call.close(Status.UNAUTHENTICATED.withDescription(
                                "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other "
                                        + "valid authentication credential."), new Metadata());
                        return new ServerCall.Listener<>() {
                        };
                    }
                }
                return next.startCall(call, headers);
            }
        };
    }
}
