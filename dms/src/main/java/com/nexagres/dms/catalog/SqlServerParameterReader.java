package com.nexagres.dms.catalog;

import com.nexagres.dms.core.BackendTarget;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL Server {@link ParameterReader} -- {@code sys.configurations}, the closest equivalent to
 * Oracle's {@code V$PARAMETER}. {@code value} reports {@code value_in_use} (the currently active
 * setting, which may differ from {@code value} until {@code RECONFIGURE} runs) since that's what
 * actually governs server behavior right now.
 */
public class SqlServerParameterReader implements ParameterReader {

    @Override
    public List<ParameterInfo> listParameters(BackendTarget target) throws SQLException {
        List<ParameterInfo> params = new ArrayList<>();
        try (Connection connection = target.open();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT name, value_in_use, description FROM sys.configurations ORDER BY name")) {
            while (rs.next()) {
                params.add(new ParameterInfo(
                    rs.getString("name"),
                    rs.getString("value_in_use"),
                    null,
                    false,
                    rs.getString("description")
                ));
            }
        }
        return params;
    }
}
