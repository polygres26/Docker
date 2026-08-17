package com.polygres.wire.grpc;

import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.server.ServerOptions;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.List;

public final class PolyWireGrpcServer {

    private final Server server;

    public PolyWireGrpcServer(ServerOptions options, List<PipelineStage> sharedStages,
            com.polygres.wire.core.BackendRegistry backendRegistry) {
        this.server = ServerBuilder.forPort(options.grpcPort())
                .addService(new QueryServiceImpl(options, sharedStages, backendRegistry))
                .intercept(new ConnectionLimitInterceptor())
                .build();
    }

    public void start() throws IOException {
        server.start();
    }
}
