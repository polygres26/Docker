package com.polygres.wire.grpc;

import com.polygres.wire.core.ExecutionResult;
import com.polygres.wire.core.JdbcBackendExecutor;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.SourceDialect;
import com.polygres.wire.core.Statement;
import com.polygres.wire.core.StatementPipeline;
import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.grpc.proto.ExecuteRequest;
import com.polygres.wire.grpc.proto.ExecuteResponse;
import com.polygres.wire.grpc.proto.QueryServiceGrpc;
import com.polygres.wire.grpc.proto.Row;
import com.polygres.wire.server.ServerOptions;
import io.grpc.stub.StreamObserver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(QueryServiceImpl.class);

    private final ServerOptions options;
    private final List<PipelineStage> sharedStages;
    private final com.polygres.wire.core.BackendRegistry backendRegistry;
    private final CredentialStore credentials = new CredentialStore();
    private final com.polygres.wire.core.SqlMetricsCollector sqlMetrics;

    public QueryServiceImpl(ServerOptions options, List<PipelineStage> sharedStages,
            com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.sqlMetrics = com.polygres.wire.core.StatsCollectorStage.findIn(sharedStages);
    }

    @Override
    public void execute(ExecuteRequest request, StreamObserver<ExecuteResponse> responseObserver) {
        byte[] expected = credentials.lookupPassword(request.getUsername());
        if (expected == null || !request.getPassword().equals(new String(expected, StandardCharsets.UTF_8))) {
            responseObserver.onNext(ExecuteResponse.newBuilder()
                    .setSuccess(false)
                    .setSqlState("28P01")
                    .setErrorMessage("password authentication failed for user \"" + request.getUsername() + "\"")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        // RTT: from here (request already deserialized by gRPC) through onNext() below --
        // the one request-in, response-out boundary this unary RPC has.
        long rttStart = System.nanoTime();
        try (Connection backend = openBackend()) {
            StatementPipeline pipeline = new StatementPipeline(sharedStages,
                    new com.polygres.wire.core.RoutingBackendExecutor(backendRegistry, new JdbcBackendExecutor(backend),
                            new com.polygres.wire.xa.XaRecoveryLog(options),
                            com.polygres.wire.core.RouterStage.shardRulesIn(sharedStages), com.polygres.wire.core.RouterStage.tableShardRulesIn(sharedStages))
                            .withFederationSupport(com.polygres.wire.core.RouterStage.statisticsStoreIn(sharedStages),
                                    com.polygres.wire.core.RouterStage.planStoreIn(sharedStages)));
            List<Object> binds = new ArrayList<>(request.getParamsList());
            Statement statement = Statement.of(SourceDialect.POLYWIRE_NATIVE, request.getSql(), binds);
            ExecutionResult result = pipeline.execute(statement);
            responseObserver.onNext(toResponse(result));
        } catch (SQLException e) {
            log.debug("native driver statement failed: {}", e.getMessage());
            responseObserver.onNext(ExecuteResponse.newBuilder()
                    .setSuccess(false)
                    .setSqlState(e.getSQLState() == null ? "58000" : e.getSQLState())
                    .setErrorMessage(e.getMessage() == null ? "backend error" : e.getMessage())
                    .build());
        }
        if (sqlMetrics != null) {
            sqlMetrics.recordRtt(SourceDialect.POLYWIRE_NATIVE, request.getSql(), System.nanoTime() - rttStart);
        }
        responseObserver.onCompleted();
    }

    private Connection openBackend() throws SQLException {
        Connection connection = com.polygres.wire.pgwire.PgConnections.open(options);
        connection.setAutoCommit(true);
        return connection;
    }

    private static ExecuteResponse toResponse(ExecutionResult result) {
        ExecuteResponse.Builder builder = ExecuteResponse.newBuilder()
                .setSuccess(true)
                .setIsQuery(result.isQuery())
                .setUpdateCount(result.updateCount());
        if (result.isQuery()) {
            builder.addAllColumnNames(result.columnNames());
            for (List<Object> row : result.rows()) {
                Row.Builder rowBuilder = Row.newBuilder();
                for (Object value : row) {
                    rowBuilder.addIsNull(value == null);
                    rowBuilder.addValues(value == null ? "" : String.valueOf(value));
                }
                builder.addRows(rowBuilder);
            }
        }
        return builder.build();
    }
}
