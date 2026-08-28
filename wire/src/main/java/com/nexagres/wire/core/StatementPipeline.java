package com.nexagres.wire.core;

import java.sql.SQLException;
import java.util.List;

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
