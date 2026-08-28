package com.nexagres.advisor.catalog;

import com.nexagres.advisor.core.BackendTarget;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQL/MariaDB {@link ParameterReader} -- {@code SHOW VARIABLES}, the closest equivalent to
 * Oracle's {@code V$PARAMETER}. Unlike Oracle, MySQL doesn't expose "is this still the compiled-in
 * default" directly through {@code SHOW VARIABLES} -- {@code isDefault}/{@code defaultValue} are
 * left unset (not guessed at) rather than reported as if the server told us.
 */
public class MySqlParameterReader implements ParameterReader {

    @Override
    public List<ParameterInfo> listParameters(BackendTarget target) throws SQLException {
        List<ParameterInfo> params = new ArrayList<>();
        try (Connection connection = target.open();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SHOW VARIABLES")) {
            while (rs.next()) {
                params.add(new ParameterInfo(
                    rs.getString("Variable_name"),
                    rs.getString("Value"),
                    null,
                    false,
                    null
                ));
            }
        }
        return params;
    }
}
