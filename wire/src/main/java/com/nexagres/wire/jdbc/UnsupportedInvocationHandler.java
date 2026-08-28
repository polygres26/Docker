package com.nexagres.wire.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

final class UnsupportedInvocationHandler implements InvocationHandler {

    private final Map<String, Function<Object[], Object>> impls = new HashMap<>();
    private final String typeName;

    UnsupportedInvocationHandler(String typeName) {
        this.typeName = typeName;
    }

    void on(String methodName, Function<Object[], Object> impl) {
        impls.put(methodName, impl);
    }

    private static final Object[] EMPTY = new Object[0];

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        Object[] safeArgs = args == null ? EMPTY : args;
        if (impls.containsKey(name)) {
            try {
                return impls.get(name).apply(safeArgs);
            } catch (WrappedSql e) {
                throw e.cause;
            }
        }
        return switch (name) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> typeName + "@" + Integer.toHexString(System.identityHashCode(proxy));
            case "isWrapperFor" -> false;
            case "unwrap" -> throw new SQLFeatureNotSupportedException("unwrap");
            case "close", "isClosed" -> impls.containsKey("close") ? null : false;
            default -> throw new SQLFeatureNotSupportedException(typeName + "." + name + " is not implemented");
        };
    }

    static final class WrappedSql extends RuntimeException {
        final java.sql.SQLException cause;

        WrappedSql(java.sql.SQLException cause) {
            this.cause = cause;
        }
    }
}
