package com.polygres.advisor.catalog;

import com.polygres.advisor.core.BackendTarget;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Backs the "explore database objects for each connection" UI requirement -- a lighter-weight,
 * on-demand sibling to {@link OracleCatalogProfiler} (which computes an aggregate feature-count
 * snapshot for scoring). This returns actual object names and column detail for browsing, not
 * counts for scoring.
 */
public class OracleObjectExplorer {

    /** Object name -> object type, grouped for a tree view. Scoped to USER_OBJECTS (the connecting schema only). */
    public Map<String, List<String>> listObjects(BackendTarget target) throws SQLException {
        Map<String, List<String>> byType = new LinkedHashMap<>();
        try (Connection connection = target.open();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT OBJECT_TYPE, OBJECT_NAME FROM USER_OBJECTS "
               + "WHERE OBJECT_TYPE IN ('TABLE','VIEW','PACKAGE','PROCEDURE','FUNCTION','TRIGGER','SEQUENCE','SYNONYM','MATERIALIZED VIEW','INDEX') "
               + "ORDER BY OBJECT_TYPE, OBJECT_NAME")) {
            while (rs.next()) {
                byType.computeIfAbsent(rs.getString("OBJECT_TYPE"), k -> new ArrayList<>())
                      .add(rs.getString("OBJECT_NAME"));
            }
        }
        return byType;
    }

    public record ColumnDetail(String name, String dataType, boolean nullable, String defaultValue) {}

    public List<ColumnDetail> describeTable(BackendTarget target, String tableName) throws SQLException {
        List<ColumnDetail> columns = new ArrayList<>();
        try (Connection connection = target.open();
             PreparedStatement ps = connection.prepareStatement(
                 "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT "
               + "FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? ORDER BY COLUMN_ID")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String type = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("DATA_PRECISION");
                    int scale = rs.getInt("DATA_SCALE");
                    int length = rs.getInt("DATA_LENGTH");
                    String fullType = switch (type) {
                        case "NUMBER" -> precision > 0 ? type + "(" + precision + "," + scale + ")" : type;
                        case "VARCHAR2", "CHAR" -> type + "(" + length + ")";
                        default -> type;
                    };
                    columns.add(new ColumnDetail(
                        rs.getString("COLUMN_NAME"),
                        fullType,
                        "Y".equals(rs.getString("NULLABLE")),
                        rs.getString("DATA_DEFAULT")
                    ));
                }
            }
        }
        return columns;
    }

    /** Full source for PACKAGE, PACKAGE BODY, PROCEDURE, FUNCTION, or TRIGGER -- delegates to the same query {@link OracleCatalogProfiler#fetchSource} uses. */
    public String fetchSource(BackendTarget target, String objectName, String objectType) throws SQLException {
        return new OracleCatalogProfiler().fetchSource(target, objectName, objectType);
    }
}
