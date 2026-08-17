package com.polygres.wire.core;

import java.sql.SQLException;
import java.util.List;

/**
 * Chains an ordered list of {@link PipelineStage}s in front of a terminal
 * {@link BackendExecutor}. Every {@code WireFrontend} runs its statements
 * through one shared instance instead of talking to JDBC directly, so a
 * stage (stats today; firewall/QoS/router/cache later) is written once and
 * applies to every frontend automatically.
 *
 * <p><b>RTT optimization</b> (found live via the ARCHITECTURE.md §11 latency
 * measurement): every pgwire/mywire/grpc/orawire frontend constructs a
 * fresh {@code StatementPipeline} per statement (a new terminal executor is
 * needed each time anyway, since it wraps that statement's borrowed
 * connection — see {@code RoutingBackendExecutor}). {@link #execute}
 * previously rebuilt the whole stage chain via {@link #buildChain} — one
 * recursive call plus one lambda allocation per stage — on <em>every single
 * statement</em>, even though {@code stages} never changes for the life of
 * this instance. The chain is now built once, in the constructor, and
 * reused for every {@link #execute} call.
 */
public final class StatementPipeline {

    private final PipelineChain chain;

    public StatementPipeline(List<PipelineStage> stages, BackendExecutor terminal) {
        this.chain = buildChain(List.copyOf(stages), 0, terminal);
    }

    public ExecutionResult execute(Statement statement) throws SQLException {
        return chain.proceed(statement);
    }

    private static PipelineChain buildChain(List<PipelineStage> stages, int index, BackendExecutor terminal) {
        if (index == stages.size()) {
            return terminal::execute;
        }
        PipelineStage stage = stages.get(index);
        PipelineChain next = buildChain(stages, index + 1, terminal);
        return statement -> stage.handle(statement, next);
    }
}
