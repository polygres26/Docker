package com.polygres.wire.core;

import java.sql.SQLException;

@FunctionalInterface
public interface PipelineChain {
    ExecutionResult proceed(Statement statement) throws SQLException;
}
