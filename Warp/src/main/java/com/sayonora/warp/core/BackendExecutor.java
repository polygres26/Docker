package com.sayonora.warp.core;

import java.sql.SQLException;

@FunctionalInterface
public interface BackendExecutor {
    ExecutionResult execute(Statement statement) throws SQLException;
}
