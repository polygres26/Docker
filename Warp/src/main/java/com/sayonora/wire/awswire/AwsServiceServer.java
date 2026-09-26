package com.sayonora.wire.awswire;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One AWS service on its own listener ({@code WARP_SNSWIRE_PORT}, {@code WARP_KINESISWIRE_PORT}, ...). */
public final class AwsServiceServer {

    private static final Logger log = LoggerFactory.getLogger(AwsServiceServer.class);

    private final Server server;
    private final AwsService service;

    public AwsServiceServer(int port, AwsRuntime rt, AwsService service) {
        this.service = service;
        AwsHttp http = new AwsHttp(rt);
        this.server = new Server(new QueuedThreadPool(threads()));
        // HTTP/1.1 and cleartext HTTP/2 (prior knowledge) on one port: the AWS SDK for JavaScript talks h2c to Kinesis
        ServerConnector c = H2cConnectionFactory.connector(server, port, ex -> {
            try {
                H2Dispatch.serve(rt, http, service, ex, service.metricsProtocol());
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }, server.getThreadPool());
        server.addConnector(c);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response) throws IOException {
                base.setHandled(true);
                http.serve(service, request, response, service.metricsProtocol());
            }
        });
    }

    static int threads() {
        String v = System.getenv("WARP_AWSWIRE_MAX_THREADS");
        try {
            return v == null || v.isBlank() ? 400 : Math.max(16, Integer.parseInt(v.trim()));
        } catch (NumberFormatException e) {
            return 400;
        }
    }

    public void start() throws Exception {
        server.start();
        log.info("warp {} ({}) listening on port {}", service.id(), service.metricsProtocol(),
                ((ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
    }
}
