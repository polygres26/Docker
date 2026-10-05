package com.sayonora.warp.core;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

/**
 * Classifies a failure of the Postgres store behind a store-backed protocol. A failover, a lost connection or an exhausted pool clears on
 * its own, and a protocol that reports it with its "retry me" error lets the client's own retry logic ride it out; reporting it as an
 * unknown or permanent error makes the client give up.
 */
public final class StoreFailures {

    private StoreFailures() {
    }

    /** The SQLException behind {@code t}, looking through proxy wrappers ({@link java.lang.reflect.UndeclaredThrowableException}) and
     * causes, or null when there is none. */
    public static SQLException sqlCause(Throwable t) {
        for (int depth = 0; t != null && depth < 8; depth++) {
            if (t instanceof SQLException se) {
                return se;
            }
            if (t instanceof java.lang.reflect.UndeclaredThrowableException ute && ute.getUndeclaredThrowable() != null) {
                t = ute.getUndeclaredThrowable();
            } else if (t instanceof java.lang.reflect.InvocationTargetException ite && ite.getTargetException() != null) {
                t = ite.getTargetException();
            } else {
                t = t.getCause() == t ? null : t.getCause();
            }
        }
        return null;
    }

    /** True for a condition that clears on its own: a connection that died or cannot be made (08xxx), a server shutting down, crashing or
     * not yet accepting connections (57P01-03), a read-only transaction (a node that was just demoted), a serialization failure or deadlock
     * (40001, 40P01), too many connections (53300), an exhausted pool, or a JDBC "transient"/"recoverable" exception. */
    public static boolean isTransient(Throwable t) {
        SQLException se = sqlCause(t);
        if (se == null) {
            return false;
        }
        if (se instanceof BackendPoolExhaustedException || se instanceof SQLTransientException || se instanceof SQLRecoverableException) {
            return true;
        }
        String state = se.getSQLState();
        return state != null && (state.startsWith("08") || state.startsWith("57P") || "25006".equals(state) || "40001".equals(state)
                || "40P01".equals(state) || "53300".equals(state));
    }
}
