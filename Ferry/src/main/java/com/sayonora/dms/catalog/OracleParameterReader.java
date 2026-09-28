package com.sayonora.dms.catalog;

import com.sayonora.dms.core.BackendTarget;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Database parameter/init-param viewer -- {@code V$PARAMETER}, per the explicit ask. Needs
 * {@code SELECT} on {@code V_$PARAMETER} (commonly granted via {@code SELECT_CATALOG_ROLE}, same
 * privilege tier as {@link com.sayonora.dms.workload.OracleWorkloadCapture}) -- a locked-down
 * read-only schema account may not have it, so callers should treat a permissions failure here as
 * "parameters unavailable," not a hard error.
 */
public class OracleParameterReader implements ParameterReader {

    @Override
    public List<ParameterInfo> listParameters(BackendTarget target) throws SQLException {
        List<ParameterInfo> params = new ArrayList<>();
        try (Connection connection = target.open();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT NAME, VALUE, ISDEFAULT, DESCRIPTION FROM V$PARAMETER ORDER BY NAME")) {
            while (rs.next()) {
                boolean isDefault = "TRUE".equalsIgnoreCase(rs.getString("ISDEFAULT"));
                params.add(new ParameterInfo(
                    rs.getString("NAME"),
                    rs.getString("VALUE"),
                    isDefault ? rs.getString("VALUE") : null,
                    isDefault,
                    rs.getString("DESCRIPTION")
                ));
            }
        }
        return params;
    }
}
