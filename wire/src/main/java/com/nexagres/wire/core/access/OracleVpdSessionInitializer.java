package com.nexagres.wire.core.access;

import com.nexagres.wire.core.AccessContext;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

public final class OracleVpdSessionInitializer implements NativeRlsSessionInitializer {

    private final String setAttributeProcedure;

    public OracleVpdSessionInitializer() {
        this("polywire_ctx_pkg.set_attribute");
    }

    public OracleVpdSessionInitializer(String setAttributeProcedure) {
        this.setAttributeProcedure = setAttributeProcedure;
    }

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        setAttribute(connection, "user_id", accessContext.userId());
        for (var entry : accessContext.attributes().entrySet()) {
            setAttribute(connection, entry.getKey(), entry.getValue());
        }
    }

    private void setAttribute(Connection connection, String name, String value) throws SQLException {
        try (CallableStatement stmt = connection.prepareCall("{call " + setAttributeProcedure + "(?, ?)}")) {
            stmt.setString(1, name);
            stmt.setString(2, value);
            stmt.execute();
        }
    }
}
