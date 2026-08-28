package com.nexagres.wire.grpc;

import com.nexagres.wire.core.ExecutionResult;
import com.nexagres.wire.core.JdbcBackendExecutor;
import com.nexagres.wire.core.PipelineStage;
import com.nexagres.wire.core.SourceDialect;
import com.nexagres.wire.core.Statement;
import com.nexagres.wire.core.StatementPipeline;
import com.nexagres.wire.auth.CredentialStore;
import com.nexagres.wire.grpc.proto.ExecuteRequest;
import com.nexagres.wire.grpc.proto.ExecuteResponse;
import com.nexagres.wire.grpc.proto.QueryServiceGrpc;
import com.nexagres.wire.grpc.proto.Row;
import com.nexagres.wire.server.ServerOptions;
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
    private final com.nexagres.wire.core.BackendRegistry backendRegistry;
    private final CredentialStore credentials = new CredentialStore();
    private final com.nexagres.wire.core.SqlMetricsCollector sqlMetrics;

    public QueryServiceImpl(ServerOptions options, List<PipelineStage> sharedStages,
            com.nexagres.wire.core.BackendRegistry backendRegistry) {
        this.options = options;
        this.sharedStages = sharedStages;
        this.backendRegistry = backendRegistry;
        this.sqlMetrics = com.nexagres.wire.core.StatsCollectorStage.findIn(sharedStages);
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
                    new com.nexagres.wire.core.RoutingBackendExecutor(backendRegistry, new JdbcBackendExecutor(backend),
                            new com.nexagres.wire.xa.XaRecoveryLog(options),
                            com.nexagres.wire.core.RouterStage.shardRulesIn(sharedStages), com.nexagres.wire.core.RouterStage.tableShardRulesIn(sharedStages))
                            .withFederationSupport(com.nexagres.wire.core.RouterStage.statisticsStoreIn(sharedStages),
                                    com.nexagres.wire.core.RouterStage.planStoreIn(sharedStages)));
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
        Connection connection = com.nexagres.wire.pgwire.PgConnections.open(options);
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
