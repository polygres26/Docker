package com.sayonora.wire.grpc;

import com.sayonora.wire.core.RemotePartitionJoin;
import com.sayonora.wire.grpc.proto.JoinPartitionRequest;
import com.sayonora.wire.grpc.proto.JoinPartitionResponse;
import com.sayonora.wire.grpc.proto.Row;
import com.sayonora.wire.grpc.proto.WarpPeerServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 1a server-side handler for {@code WarpPeerService.JoinPartition} -- see {@code
 * src/main/proto/warp.proto}'s own service javadoc for why this is a SEPARATE service (and
 * SEPARATE trust domain, mutual TLS via {@code WarpPeerGrpcServer} rather than the end-user
 * {@code CredentialStore} check {@link QueryServiceImpl} does) from Warp's client-facing gRPC.
 *
 * <p>Deliberately never touches a JDBC connection or an original backend -- the whole point of
 * shipping a partition's already-collected rows here is that the peer only ever does in-memory
 * matching ({@link RemotePartitionJoin}, the SAME algorithm {@code core.ParallelJoinExecutor}'s
 * local worker threads use, so a remotely-run partition can never silently disagree with a
 * locally-run one) and returns the joined rows -- no backend credentials, no SQL, ever cross this
 * service.
 */
public final class WarpPeerServiceImpl extends WarpPeerServiceGrpc.WarpPeerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(WarpPeerServiceImpl.class);

    @Override
    public void joinPartition(JoinPartitionRequest request, StreamObserver<JoinPartitionResponse> responseObserver) {
        try {
            List<List<Object>> buildRows = toRows(request.getBuildRowsList());
            List<List<Object>> probeRows = toRows(request.getProbeRowsList());
            Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(buildRows, request.getBuildKeyOrdinal());
            List<List<Object>> joined = RemotePartitionJoin.probeAll(table, probeRows,
                    request.getProbeKeyOrdinal(), request.getLeftIsBuild());

            List<String> columnNames = new ArrayList<>(request.getBuildColumnNamesCount() + request.getProbeColumnNamesCount());
            if (request.getLeftIsBuild()) {
                columnNames.addAll(request.getBuildColumnNamesList());
                columnNames.addAll(request.getProbeColumnNamesList());
            } else {
                columnNames.addAll(request.getProbeColumnNamesList());
                columnNames.addAll(request.getBuildColumnNamesList());
            }

            JoinPartitionResponse.Builder builder = JoinPartitionResponse.newBuilder()
                    .setSuccess(true)
                    .addAllColumnNames(columnNames);
            for (List<Object> row : joined) {
                builder.addRows(toRow(row));
            }
            responseObserver.onNext(builder.build());
        } catch (RuntimeException e) {
            log.warn("peer join partition: failed -- {}", e.toString());
            responseObserver.onNext(JoinPartitionResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage() == null ? "peer join failed" : e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    /** As {@code QueryServiceImpl#toResponse}'s own row encoding -- text-encoded values, parallel
     * {@code is_null}, matching the wire convention {@link Row} already establishes. */
    private static List<List<Object>> toRows(List<Row> rows) {
        List<List<Object>> result = new ArrayList<>(rows.size());
        for (Row row : rows) {
            List<Object> converted = new ArrayList<>(row.getValuesCount());
            for (int i = 0; i < row.getValuesCount(); i++) {
                converted.add(row.getIsNull(i) ? null : row.getValues(i));
            }
            result.add(converted);
        }
        return result;
    }

    private static Row toRow(List<Object> row) {
        Row.Builder builder = Row.newBuilder();
        for (Object value : row) {
            builder.addIsNull(value == null);
            builder.addValues(value == null ? "" : String.valueOf(value));
        }
        return builder.build();
    }
}
