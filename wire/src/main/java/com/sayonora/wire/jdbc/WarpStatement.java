package com.sayonora.wire.jdbc;

import com.sayonora.wire.grpc.proto.ExecuteRequest;
import com.sayonora.wire.grpc.proto.ExecuteResponse;
import com.sayonora.wire.grpc.proto.QueryServiceGrpc;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class WarpStatement {

    static PreparedStatement create(QueryServiceGrpc.QueryServiceBlockingStub stub, String username, String password,
            String sql) {
        List<String> binds = new ArrayList<>();
        ExecuteResponse[] lastResponse = {null};

        UnsupportedInvocationHandler handler = new UnsupportedInvocationHandler("PreparedStatement");
        handler.on("setObject", args -> {
            setBind(binds, (Integer) args[0], args[1] == null ? null : String.valueOf(args[1]));
            return null;
        });
        handler.on("setString", args -> {
            setBind(binds, (Integer) args[0], (String) args[1]);
            return null;
        });
        handler.on("setInt", args -> {
            setBind(binds, (Integer) args[0], String.valueOf(args[1]));
            return null;
        });
        handler.on("setLong", args -> {
            setBind(binds, (Integer) args[0], String.valueOf(args[1]));
            return null;
        });
        handler.on("setBoolean", args -> {
            setBind(binds, (Integer) args[0], String.valueOf(args[1]));
            return null;
        });
        handler.on("setNull", args -> {
            setBind(binds, (Integer) args[0], null);
            return null;
        });
        handler.on("execute", args -> {
            String text = args.length > 0 ? (String) args[0] : sql;
            lastResponse[0] = runExecuteUnchecked(stub, username, password, text, binds);
            return lastResponse[0].getIsQuery();
        });
        handler.on("executeQuery", args -> {
            String text = args.length > 0 ? (String) args[0] : sql;
            lastResponse[0] = runExecuteUnchecked(stub, username, password, text, binds);
            return WarpResultSet.create(lastResponse[0]);
        });
        handler.on("executeUpdate", args -> {
            String text = args.length > 0 ? (String) args[0] : sql;
            lastResponse[0] = runExecuteUnchecked(stub, username, password, text, binds);
            return (int) lastResponse[0].getUpdateCount();
        });
        handler.on("getResultSet", args -> WarpResultSet.create(lastResponse[0]));
        handler.on("getUpdateCount", args -> (int) lastResponse[0].getUpdateCount());
        handler.on("close", args -> null);
        handler.on("isClosed", args -> false);

        return (PreparedStatement) Proxy.newProxyInstance(
                WarpStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class}, handler);
    }

    private static void setBind(List<String> binds, int oneBasedIndex, String value) {
        while (binds.size() < oneBasedIndex) {
            binds.add(null);
        }
        binds.set(oneBasedIndex - 1, value);
    }

    private static ExecuteResponse runExecuteUnchecked(QueryServiceGrpc.QueryServiceBlockingStub stub,
            String username, String password, String sql, List<String> binds) {
        ExecuteRequest.Builder req = ExecuteRequest.newBuilder()
                .setUsername(username)
                .setPassword(password)
                .setSql(sql);
        for (String bind : binds) {
            req.addParams(bind == null ? "" : bind);
        }
        ExecuteResponse response = stub.execute(req.build());
        if (!response.getSuccess()) {
            throw new UnsupportedInvocationHandler.WrappedSql(
                    new SQLException(response.getErrorMessage(), response.getSqlState()));
        }
        return response;
    }

    private WarpStatement() {
    }
}
