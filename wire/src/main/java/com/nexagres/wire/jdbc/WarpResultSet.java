package com.nexagres.wire.jdbc;

import com.nexagres.wire.grpc.proto.ExecuteResponse;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

final class WarpResultSet {

    static ResultSet create(ExecuteResponse response) {
        int[] rowIndex = {-1};
        boolean[] lastWasNull = {false};

        UnsupportedInvocationHandler handler = new UnsupportedInvocationHandler("ResultSet");
        handler.on("next", args -> {
            rowIndex[0]++;
            return rowIndex[0] < response.getRowsCount();
        });
        handler.on("close", args -> null);
        handler.on("isClosed", args -> false);
        handler.on("wasNull", args -> lastWasNull[0]);
        handler.on("getString", args -> getValue(response, rowIndex[0], columnIndex(response, args[0]), lastWasNull));
        handler.on("getObject", args -> getValue(response, rowIndex[0], columnIndex(response, args[0]), lastWasNull));
        handler.on("getMetaData", args -> createMetaData(response));

        return (ResultSet) Proxy.newProxyInstance(
                WarpResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class}, handler);
    }

    private static int columnIndex(ExecuteResponse response, Object arg) {
        if (arg instanceof Integer i) {
            return i - 1;
        }
        return response.getColumnNamesList().indexOf((String) arg);
    }

    private static String getValue(ExecuteResponse response, int row, int col, boolean[] lastWasNull) {
        var values = response.getRows(row);
        lastWasNull[0] = values.getIsNull(col);
        return lastWasNull[0] ? null : values.getValues(col);
    }

    private static ResultSetMetaData createMetaData(ExecuteResponse response) {
        UnsupportedInvocationHandler handler = new UnsupportedInvocationHandler("ResultSetMetaData");
        handler.on("getColumnCount", args -> response.getColumnNamesCount());
        handler.on("getColumnLabel", args -> response.getColumnNames((Integer) args[0] - 1));
        handler.on("getColumnName", args -> response.getColumnNames((Integer) args[0] - 1));
        handler.on("getColumnType", args -> Types.VARCHAR);
        return (ResultSetMetaData) Proxy.newProxyInstance(
                WarpResultSet.class.getClassLoader(), new Class<?>[] {ResultSetMetaData.class}, handler);
    }

    private WarpResultSet() {
    }
}
