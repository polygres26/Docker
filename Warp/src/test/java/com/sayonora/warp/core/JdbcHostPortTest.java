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
    }

    @Test
    void rejectsUrlsWithoutAHost() {
        assertThrows(IllegalArgumentException.class, () -> JdbcHostPort.parse("jdbc:oracle:thin:@x", 1521));
        assertThrows(IllegalArgumentException.class, () -> JdbcHostPort.parse("jdbc:postgresql:///db", 5432));
    }
}
