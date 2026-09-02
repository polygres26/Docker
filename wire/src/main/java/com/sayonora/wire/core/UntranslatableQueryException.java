package com.sayonora.wire.core;

import java.sql.SQLException;

public class UntranslatableQueryException extends SQLException {

    public UntranslatableQueryException(String sqlText, SourceDialect from, SourceDialect to, String reason) {
        super("statement cannot be translated from " + from + " to " + to + " (needs manual migration): "
                + reason + " -- original SQL: " + sqlText, "0A000");
    }

    public UntranslatableQueryException(String sqlText, SourceDialect from, SourceDialect to, String reason, Throwable cause) {
        super("statement cannot be translated from " + from + " to " + to + " (needs manual migration): "
                + reason + " -- original SQL: " + sqlText, "0A000", cause);
    }
}
