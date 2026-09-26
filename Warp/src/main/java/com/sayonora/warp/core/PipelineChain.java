package com.sayonora.warp.core;

import java.sql.SQLException;

@FunctionalInterface
public interface PipelineChain {
    ExecutionResult proceed(Statement statement) throws SQLException;
}
