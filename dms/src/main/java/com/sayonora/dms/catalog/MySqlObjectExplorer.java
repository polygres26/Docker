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

/** MySQL/MariaDB {@link ObjectExplorer} -- {@code information_schema}-backed, same shape as {@link OracleObjectExplorer}. */
public class MySqlObjectExplorer implements ObjectExplorer {

    @Override
    public Map<String, List<String>> listObjects(BackendTarget target) throws SQLException {
        Map<String, List<String>> byType = new LinkedHashMap<>();
        try (Connection connection = target.open(); Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(
                    "SELECT 'TABLE' AS obj_type, table_name AS obj_name FROM information_schema.tables "
                  + "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' "
                  + "UNION ALL "
                  + "SELECT 'VIEW', table_name FROM information_schema.views WHERE table_schema = DATABASE() "
                  + "UNION ALL "
                  + "SELECT 'PROCEDURE', routine_name FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'PROCEDURE' "
                  + "UNION ALL "
                  + "SELECT 'FUNCTION', routine_name FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'FUNCTION' "
                  + "UNION ALL "
                  + "SELECT 'TRIGGER', trigger_name FROM information_schema.triggers WHERE trigger_schema = DATABASE() "
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
                 "SELECT column_name, column_type, is_nullable, column_default "
               + "FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? "
               + "ORDER BY ordinal_position")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(new ColumnDetail(
                        rs.getString("column_name"),
                        rs.getString("column_type"),
                        "YES".equalsIgnoreCase(rs.getString("is_nullable")),
                        rs.getString("column_default")
                    ));
                }
            }
        }
        return columns;
    }

    /** Procedure/function body via information_schema.routines; trigger body via information_schema.triggers -- MySQL has no combined "source view" the way Oracle's USER_SOURCE covers everything. */
    @Override
    public String fetchSource(BackendTarget target, String objectName, String objectType) throws SQLException {
        String column = "TRIGGER".equalsIgnoreCase(objectType) ? "action_statement" : "routine_definition";
        String table = "TRIGGER".equalsIgnoreCase(objectType) ? "information_schema.triggers" : "information_schema.routines";
        String nameColumn = "TRIGGER".equalsIgnoreCase(objectType) ? "trigger_name" : "routine_name";
        String schemaColumn = "TRIGGER".equalsIgnoreCase(objectType) ? "trigger_schema" : "routine_schema";

        try (Connection connection = target.open();
             PreparedStatement ps = connection.prepareStatement(
                 "SELECT " + column + " AS body FROM " + table + " WHERE " + schemaColumn + " = DATABASE() AND " + nameColumn + " = ?")) {
            ps.setString(1, objectName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? (rs.getString("body") == null ? "" : rs.getString("body")) : "";
            }
        }
    }
}
