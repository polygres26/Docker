package com.nexagres.wire.core;

import java.sql.SQLException;

@FunctionalInterface
public interface BackendExecutor {
    ExecutionResult execute(Statement statement) throws SQLException;
}
