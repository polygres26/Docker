package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Test;

class StoreFailuresTest {

    @Test
    void failoverShapedErrorsAreTransient() {
        for (String state : new String[] {"08006", "08001", "57P01", "57P02", "57P03", "25006", "40001", "40P01", "53300"}) {
            assertTrue(StoreFailures.isTransient(new SQLException("x", state)), state);
        }
        assertTrue(StoreFailures.isTransient(new SQLTransientConnectionException("pool: Connection is not available")));
    }

    @Test
    void ordinaryErrorsAreNot() {
        assertFalse(StoreFailures.isTransient(new SQLException("syntax", "42601")));
        assertFalse(StoreFailures.isTransient(new SQLException("unique", "23505")));
        assertFalse(StoreFailures.isTransient(new RuntimeException("no sql here")));
        assertFalse(StoreFailures.isTransient(null));
    }

    @Test
    void aProxyWrapperIsLookedThrough() {
        SQLException real = new SQLException("terminating connection", "57P01");
        assertSame(real, StoreFailures.sqlCause(new UndeclaredThrowableException(real)));
        assertSame(real, StoreFailures.sqlCause(new IllegalStateException("wrapped", new UndeclaredThrowableException(real))));
        assertTrue(StoreFailures.isTransient(new UndeclaredThrowableException(real)));
        assertNull(StoreFailures.sqlCause(new UndeclaredThrowableException(new RuntimeException("not sql"))));
        assertEquals(real, StoreFailures.sqlCause(new java.lang.reflect.InvocationTargetException(real)));
    }
}
