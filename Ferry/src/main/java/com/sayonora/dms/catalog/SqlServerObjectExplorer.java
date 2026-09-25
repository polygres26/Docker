package com.sayonora.dms.catalog;

import com.sayonora.dms.core.BackendTarget;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** SQL Server {@link ObjectExplorer} -- {@code sys.*} catalog views, same shape as the Oracle/MySQL explorers. */
public class SqlServerObjectExplorer implements ObjectExplorer {

    @Override
    public Map<String, List<String>> listObjects(BackendTarget target) throws SQLException {
        Map<String, List<String>> byType = new LinkedHashMap<>();
        try (Connection connection = target.open(); Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(
                    "SELECT 'TABLE' AS obj_type, name AS obj_name FROM sys.tables "
                  + "UNION ALL SELECT 'VIEW', name FROM sys.views "
                  + "UNION ALL SELECT 'PROCEDURE', name FROM sys.procedures "
                  + "UNION ALL SELECT 'FUNCTION', name FROM sys.objects WHERE type IN ('FN','TF','IF') "
                  + "UNION ALL SELECT 'TRIGGER', name FROM sys.triggers "
                  + "ORDER BY 1, 2")) {
                while (rs.next()) {
                    byType.computeIfAbsent(rs.getString("obj_type"), k -> new ArrayList<>())
                          .add(rs.getString("obj_name"));
                }
            }
        }
        return byType;
    }

    @Override
    public List<ColumnDetail> describeTable(BackendTarget target, String tableName) throws SQLException {
        List<ColumnDetail> columns = new ArrayList<>();
        try (Connection connection = target.open();
             PreparedStatement ps = connection.prepareStatement(
                 "SELECT c.name AS column_name, t.name AS type_name, c.max_length, c.precision, c.scale, "
               + "c.is_nullable, dc.definition AS default_value "
               + "FROM sys.columns c "
               + "JOIN sys.types t ON c.user_type_id = t.user_type_id "
               + "LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id "
               + "WHERE c.object_id = OBJECT_ID(?) "
               + "ORDER BY c.column_id")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String type = rs.getString("type_name");
                    String fullType = switch (type) {
                        case "varchar", "nvarchar", "char", "nchar" -> type + "(" + rs.getInt("max_length") + ")";
                        case "decimal", "numeric" -> type + "(" + rs.getInt("precision") + "," + rs.getInt("scale") + ")";
                        default -> type;
                    };
                    columns.add(new ColumnDetail(
                        rs.getString("column_name"),
                        fullType,
                        rs.getBoolean("is_nullable"),
                        rs.getString("default_value")
                    ));
                }
            }
        }
        return columns;
    }

    /** {@code OBJECT_DEFINITION} covers procedures, functions, and triggers uniformly -- SQL Server doesn't need the per-type branching Oracle/MySQL's explorers do. */
    @Override
    public String fetchSource(BackendTarget target, String objectName, String objectType) throws SQLException {
        try (Connection connection = target.open();
             PreparedStatement ps = connection.prepareStatement("SELECT OBJECT_DEFINITION(OBJECT_ID(?))")) {
            ps.setString(1, objectName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? (rs.getString(1) == null ? "" : rs.getString(1)) : "";
            }
        }
    }
}
