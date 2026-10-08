package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JdbcHostPortTest {

    @Test
    void parsesCommonShapes() {
        assertEquals(new JdbcHostPort("db1", 5433), JdbcHostPort.parse("jdbc:postgresql://db1:5433/app?ssl=true", 5432));
        assertEquals(new JdbcHostPort("db1", 5432), JdbcHostPort.parse("jdbc:postgresql://db1/app", 5432));
        assertEquals(new JdbcHostPort("m", 3307), JdbcHostPort.parse("jdbc:mysql://m:3307", 3306));
        assertEquals(new JdbcHostPort("::1", 5432), JdbcHostPort.parse("jdbc:postgresql://[::1]:5432/x", 5432));
        assertEquals(new JdbcHostPort("a", 3306), JdbcHostPort.parse("jdbc:mysql://a,b:3306/x", 3306), "first of several hosts");
        assertEquals(new JdbcHostPort("ora", 1522), JdbcHostPort.parse("jdbc:oracle:thin:@//ora:1522/FREEPDB1", 1521), "Oracle thin service URL");
        assertEquals(new JdbcHostPort("ms", 14331), JdbcHostPort.parse("jdbc:sqlserver://ms:14331;databaseName=w;encrypt=true", 1433), "SQL Server properties");
        assertEquals(new JdbcHostPort("ms", 1433), JdbcHostPort.parse("jdbc:sqlserver://ms;databaseName=w", 1433));
    }

    @Test
    void rejectsUrlsWithoutAHost() {
        assertThrows(IllegalArgumentException.class, () -> JdbcHostPort.parse("jdbc:oracle:thin:@x", 1521));
        assertThrows(IllegalArgumentException.class, () -> JdbcHostPort.parse("jdbc:postgresql:///db", 5432));
    }
}
