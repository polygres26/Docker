package com.polygres.wire.core;

import java.util.Map;

public final class SqlStateErrorMapper {

    public static final int ORACLE_DEFAULT = 942;
    
    public static final int MYSQL_DEFAULT = 1105;
    
    public static final int SQL_SERVER_DEFAULT = 50000;

    private record NativeErrors(int oracle, int mysql, int sqlServer) {
    }

    private static final Map<String, NativeErrors> TABLE = Map.ofEntries(
            
            Map.entry("42P01", new NativeErrors(942, 1146, 208)),
            
            Map.entry("42703", new NativeErrors(904, 1054, 207)),
            
            Map.entry("42601", new NativeErrors(900, 1064, 102)),
            
            Map.entry("23505", new NativeErrors(1, 1062, 2627)),
            
            Map.entry("23502", new NativeErrors(1400, 1048, 515)),
            
            Map.entry("23503", new NativeErrors(2291, 1452, 547)),
            
            Map.entry("42P07", new NativeErrors(955, 1050, 2714)));

    private SqlStateErrorMapper() {
    }

    public static int toOracleError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? ORACLE_DEFAULT : n.oracle();
    }

    public static int toMySqlError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? MYSQL_DEFAULT : n.mysql();
    }

    public static int toSqlServerError(String sqlState) {
        NativeErrors n = sqlState == null ? null : TABLE.get(sqlState);
        return n == null ? SQL_SERVER_DEFAULT : n.sqlServer();
    }
}
