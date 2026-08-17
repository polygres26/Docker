package com.polygres.wire.core;

import java.sql.SQLException;

/** Terminal step of a {@link StatementPipeline}: actually runs a {@link Statement} against a backend. */
@FunctionalInterface
public interface BackendExecutor {
    ExecutionResult execute(Statement statement) throws SQLException;
}
