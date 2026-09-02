package com.sayonora.wire.core.access;

import com.sayonora.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class PostgresRlsSessionInitializer implements NativeRlsSessionInitializer {

    @Override
    public void initialize(Connection connection, AccessContext accessContext) throws SQLException {
        setConfig(connection, "warp.user_id", accessContext.userId());
        for (var entry : accessContext.attributes().entrySet()) {
            setConfig(connection, "warp." + entry.getKey(), entry.getValue());
        }
    }

    private void setConfig(Connection connection, String settingName, String value) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT set_config(?, ?, false)")) {
            stmt.setString(1, settingName);
            stmt.setString(2, value);
            stmt.execute();
        }
    }
}
