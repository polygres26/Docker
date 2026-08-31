package com.nexagres.dms.catalog;

import com.nexagres.dms.core.BackendTarget;
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
 * snapshot for scoring, still scoped to {@code USER_*} views -- see that class's javadoc). This
 * returns actual object names and column detail for browsing, not counts for scoring.
 *
 * <p>Queries {@code ALL_OBJECTS}/{@code ALL_TAB_COLUMNS}/{@code ALL_SOURCE} rather than the
 * {@code USER_*} equivalents -- a real migration assessment is normally run with a privileged
 * account (the same assumption {@link com.nexagres.dms.workload.OracleWorkloadCapture} makes
 * about {@code GV$} access) that can see every application schema, not just objects it happens to
 * own itself. {@link #ORACLE_SUPPLIED_SCHEMAS} excludes Oracle-maintained schemas (SYS, SYSTEM,
 * the XML DB/Spatial/Context/etc. component schemas, APEX, ...) so the tree only ever shows a
 * customer's own application objects. Object names are returned as {@code OWNER.NAME} to stay
 * unambiguous across schemas; {@link #describeTable} and {@link #fetchSource} both accept that
 * qualified form (and fall back to the connecting user's own schema for a bare, unqualified name).
 */
public class OracleObjectExplorer implements ObjectExplorer {

    /**
     * Every schema Oracle itself creates and maintains as part of the database/options install,
     * not anything a customer's application put there. Not exhaustive of every optional component
     * schema Oracle has ever shipped, but covers what a default 19c install (Database Vault, Text,
     * Spatial, XML DB, Multimedia, Data Mining, etc. all installed) creates -- revisit if a real
     * customer instance turns up one this list misses.
     */
    static final List<String> ORACLE_SUPPLIED_SCHEMAS = List.of(
        "SYS", "SYSTEM", "XDB", "OUTLN", "DBSNMP", "APPQOSSYS", "AUDSYS", "CTXSYS", "DVSYS", "DVF",
        "GSMADMIN_INTERNAL", "GSMCATUSER", "GSMUSER", "GSMROOTUSER", "LBACSYS", "MDSYS", "OLAPSYS",
        "ORDDATA", "ORDPLUGINS", "ORDSYS", "SI_INFORMTN_SCHEMA", "WMSYS", "XS$NULL", "ANONYMOUS",
        "FLOWS_FILES", "ORACLE_OCM", "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR",
        "REMOTE_SCHEDULER_AGENT", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC", "GGSYS", "DBSFWUSER",
        "PDBADMIN", "DIP", "MDDATA", "OJVMSYS",
        // PUBLIC isn't a real schema -- it's the pseudo-owner Oracle files every PUBLIC SYNONYM
        // under, and a default install creates thousands of them (one per ALL_*/DBA_*/V$ view and
        // system package) pointing at SYS/SYSTEM objects. Without this, PUBLIC synonyms alone would
        // drown out a customer's real application objects.
        "PUBLIC"
    );

    /** {@code OWNER NOT IN (...) AND OWNER NOT LIKE 'APEX\_%' ESCAPE '\'} -- APEX installs one schema per version (APEX_200100, ...), a fixed list can't keep up. */
    private static final String OWNER_FILTER = "OWNER NOT IN (" + ORACLE_SUPPLIED_SCHEMAS.stream()
        .map(s -> "'" + s + "'").reduce((a, b) -> a + "," + b).orElse("''")
        + ") AND OWNER NOT LIKE 'APEX\\_%' ESCAPE '\\'";

    @Override
    /** Object owner+name -> object type, grouped for a tree view. Cross-schema; see class javadoc. */
    public Map<String, List<String>> listObjects(BackendTarget target) throws SQLException {
        Map<String, List<String>> byType = new LinkedHashMap<>();
        try (Connection connection = target.open();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT OBJECT_TYPE, OWNER, OBJECT_NAME FROM ALL_OBJECTS "
               + "WHERE OBJECT_TYPE IN ('TABLE','VIEW','PACKAGE','PROCEDURE','FUNCTION','TRIGGER','SEQUENCE','SYNONYM','MATERIALIZED VIEW','INDEX') "
               + "AND " + OWNER_FILTER + " "
               + "ORDER BY OBJECT_TYPE, OWNER, OBJECT_NAME")) {
            while (rs.next()) {
                byType.computeIfAbsent(rs.getString("OBJECT_TYPE"), k -> new ArrayList<>())
                      .add(rs.getString("OWNER") + "." + rs.getString("OBJECT_NAME"));
            }
        }
        return byType;
    }

    @Override
    public List<ColumnDetail> describeTable(BackendTarget target, String tableName) throws SQLException {
        List<ColumnDetail> columns = new ArrayList<>();
        try (Connection connection = target.open()) {
            String[] qualified = splitOwner(connection, tableName);
            try (PreparedStatement ps = connection.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT "
                   + "FROM ALL_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID")) {
                ps.setString(1, qualified[0]);
                ps.setString(2, qualified[1]);
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
        }
        return columns;
    }

    /** Full source for PACKAGE, PACKAGE BODY, PROCEDURE, FUNCTION, or TRIGGER -- ALL_SOURCE, owner-qualified (see class javadoc), not {@link OracleCatalogProfiler#fetchSource}'s USER_SOURCE-scoped version. */
    @Override
    public String fetchSource(BackendTarget target, String objectName, String objectType) throws SQLException {
        StringBuilder source = new StringBuilder();
        try (Connection connection = target.open()) {
            String[] qualified = splitOwner(connection, objectName);
            try (PreparedStatement ps = connection.prepareStatement(
                     "SELECT TEXT FROM ALL_SOURCE WHERE OWNER = ? AND NAME = ? AND TYPE = ? ORDER BY LINE")) {
                ps.setString(1, qualified[0]);
                ps.setString(2, qualified[1]);
                ps.setString(3, objectType);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        source.append(rs.getString("TEXT"));
                    }
                }
            }
        }
        return source.toString();
    }

    /**
     * "OWNER.NAME" -> {owner, name}. A bare name (no dot) is resolved against the connecting
     * user's own schema (a one-off {@code SELECT USER FROM DUAL}, since a bind parameter can't
     * carry the {@code USER} SQL keyword itself) -- kept for callers that predate owner-qualified
     * names rather than as the expected path from {@link #listObjects}, which always qualifies.
     */
    private static String[] splitOwner(Connection connection, String qualifiedOrBareName) throws SQLException {
        int dot = qualifiedOrBareName.indexOf('.');
        if (dot >= 0) {
            return new String[] { qualifiedOrBareName.substring(0, dot), qualifiedOrBareName.substring(dot + 1) };
        }
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT USER FROM DUAL")) {
            rs.next();
            return new String[] { rs.getString(1), qualifiedOrBareName };
        }
    }
}
