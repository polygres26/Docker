package com.polygres.wire.core;

import java.sql.SQLException;

@FunctionalInterface
public interface PipelineStage {
    ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException;
}
