package com.polygres.wire.core;

import java.sql.SQLException;

/** Invokes the next stage (or the terminal backend executor) in a {@link StatementPipeline}. */
@FunctionalInterface
public interface PipelineChain {
    ExecutionResult proceed(Statement statement) throws SQLException;
}
