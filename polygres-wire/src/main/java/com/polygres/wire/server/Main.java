package com.polygres.wire.server;

import com.polygres.wire.core.BackendRegistry;
import com.polygres.wire.core.DialectTranslationStage;
import com.polygres.wire.core.PipelineStage;
import com.polygres.wire.core.RouterStage;
import com.polygres.wire.mywire.MySqlWireSessionHandler;
import com.polygres.wire.orawire.session.SessionHandler;
import com.polygres.wire.pgwire.PgBackendPool;
import com.polygres.wire.pgwire.PgWireSessionHandler;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal bootstrap: starts the three frontend listeners (pgwire, mywire, orawire), each wired
 * through a shared {@link RouterStage} + {@link DialectTranslationStage} pipeline onto a single
 * Postgres backend (configured via the existing ORAPG_PG_* env vars, defaults localhost:5432/
 * postgres). Deliberately narrow first pass — see class javadoc in the individual stages for what
 * is NOT wired here: QosControlStage, CacheStage, RollupStage, StatsCollectorStage, and the gRPC/
 * HTTP admin frontends from Omnigate's ProxyServer are all skipped. Only what's needed to get a
 * real client round-tripping through each of the three wire protocols to a real Postgres backend.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ServerOptions options = ServerOptions.parse(args);

        BackendRegistry backendRegistry = BackendRegistry.fromConfig(null, null);
        List<PipelineStage> pipelineStages = List.of(
                new RouterStage(),
                new DialectTranslationStage(backendRegistry));

        PgBackendPool backendPool = new PgBackendPool(options);

        ExecutorService sessionExecutor = Executors.newCachedThreadPool();
        ExecutorService listenerExecutor = Executors.newCachedThreadPool();

        listenerExecutor.submit(() -> acceptPgWireLoop(options, pipelineStages, backendRegistry, sessionExecutor));
        listenerExecutor.submit(() -> acceptMySqlWireLoop(options, pipelineStages, backendRegistry, sessionExecutor));
        acceptOraWireLoop(options, backendPool, pipelineStages, backendRegistry, sessionExecutor);
    }

    private static void acceptPgWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor) {
        try (ServerSocket serverSocket = new ServerSocket(options.pgWireListenPort())) {
            log.info("polywire listening for TCP (Postgres wire) on port {}, proxying to postgres {}:{}/{}",
                    options.pgWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new PgWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Postgres wire listener on port {} failed", options.pgWireListenPort(), e);
        }
    }

    private static void acceptMySqlWireLoop(ServerOptions options, List<PipelineStage> pipelineStages,
            BackendRegistry backendRegistry, ExecutorService sessionExecutor) {
        try (ServerSocket serverSocket = new ServerSocket(options.myWireListenPort())) {
            log.info("polywire listening for TCP (MySQL wire) on port {}, proxying to postgres {}:{}/{}",
                    options.myWireListenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new MySqlWireSessionHandler(clientSocket, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("MySQL wire listener on port {} failed", options.myWireListenPort(), e);
        }
    }

    private static void acceptOraWireLoop(ServerOptions options, PgBackendPool backendPool,
            List<PipelineStage> pipelineStages, BackendRegistry backendRegistry, ExecutorService sessionExecutor) {
        try (ServerSocket serverSocket = new ServerSocket(options.listenPort())) {
            log.info("polywire listening for TCP (Oracle wire) on port {}, proxying to postgres {}:{}/{}",
                    options.listenPort(), options.pgHost(), options.pgPort(), options.pgDatabase());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                sessionExecutor.submit(new SessionHandler(clientSocket, backendPool, options, pipelineStages, backendRegistry));
            }
        } catch (IOException e) {
            log.error("Oracle wire listener on port {} failed", options.listenPort(), e);
        }
    }

    private Main() {
    }
}
