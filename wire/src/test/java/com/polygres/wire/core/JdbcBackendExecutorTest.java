package com.polygres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.polygres.wire.testsupport.RealPostgres;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression coverage for the real, found-live fix in {@link JdbcBackendExecutor}: it used to
 * {@code prepareStatement}/close a brand-new {@link PreparedStatement} on every single call, even
 * for the exact same SQL text executed hundreds of times in a row -- which meant pgjdbc's own
 * server-side prepare (only activates once the SAME {@code PreparedStatement} object has actually
 * been executed {@code prepareThreshold} times) could never trigger. See
 * {@code docs/PERFORMANCE.md} and this fix's own commit for the live-measured before/after
 * (server-side avg roughly halved on both reads and writes).
 *
 * <p>This asserts the actual mechanism, not just the end-to-end result -- a future change that
 * "simplifies" {@link JdbcBackendExecutor} back to preparing fresh every call would still return
 * correct query results (the bug was never a correctness bug), so a purely result-based test
 * wouldn't catch the regression. {@link #countingConnection} wraps a real Postgres connection in a
 * dynamic proxy that counts real {@code Connection#prepareStatement} calls, so these tests fail
 * the moment that invariant breaks, against a real backend, not a mock.
 */
class JdbcBackendExecutorTest {

    private RealPostgres pg;
    private Connection realConnection;

    @BeforeEach
    void startInfra() throws Exception {
        pg = RealPostgres.start();
        realConnection = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
        realConnection.setAutoCommit(true);
        try (var st = realConnection.createStatement()) {
            st.execute("CREATE TABLE jdbc_exec_test (id BIGSERIAL PRIMARY KEY, val TEXT)");
            st.execute("INSERT INTO jdbc_exec_test (val) VALUES ('seed')");
        }
    }

    @AfterEach
    void stopInfra() throws Exception {
        if (realConnection != null) {
            realConnection.close();
        }
        if (pg != null) {
            pg.close();
        }
    }

    /** A dynamic proxy around a real {@link Connection} that counts every real {@code
     * prepareStatement(String)} call -- the one thing that must NOT happen again for a SQL text
     * already cached. Every other method delegates straight through to the real connection. */
    private static Connection countingConnection(Connection real, AtomicInteger prepareCallCount) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("prepareStatement".equals(method.getName()) && args.length >= 1 && args[0] instanceof String) {
                prepareCallCount.incrementAndGet();
            }
            try {
                return method.invoke(real, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (Connection) Proxy.newProxyInstance(
                JdbcBackendExecutorTest.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    @Test
    void repeatedIdenticalSqlTextReusesOnePreparedStatement() throws SQLException {
        AtomicInteger prepareCalls = new AtomicInteger();
        Connection counting = countingConnection(realConnection, prepareCalls);
        JdbcBackendExecutor executor = new JdbcBackendExecutor(counting);

        String sql = "SELECT val FROM jdbc_exec_test WHERE id = ?";
        for (int i = 0; i < 10; i++) {
            ExecutionResult result = executor.execute(Statement.of(SourceDialect.POSTGRES, sql, List.of(1L)));
            assertEquals(1, result.rows().size());
            assertEquals("seed", result.rows().get(0).get(0));
        }

        assertEquals(1, prepareCalls.get(),
                "the exact same SQL text executed 10 times must prepare exactly once, not once per call -- "
                        + "this is the invariant that lets pgjdbc's own server-side prepare ever activate");
    }

    @Test
    void differentSqlTextGetsItsOwnPreparedStatement() throws SQLException {
        AtomicInteger prepareCalls = new AtomicInteger();
        Connection counting = countingConnection(realConnection, prepareCalls);
        JdbcBackendExecutor executor = new JdbcBackendExecutor(counting);

        executor.execute(Statement.of(SourceDialect.POSTGRES, "SELECT val FROM jdbc_exec_test WHERE id = ?", List.of(1L)));
        executor.execute(Statement.of(SourceDialect.POSTGRES, "SELECT id FROM jdbc_exec_test WHERE val = ?", List.of("seed")));
        executor.execute(Statement.of(SourceDialect.POSTGRES, "SELECT val FROM jdbc_exec_test WHERE id = ?", List.of(1L)));

        assertEquals(2, prepareCalls.get(),
                "two distinct SQL texts must each prepare once -- caching must key on the exact text, "
                        + "not collapse unrelated statements together");
    }

    @Test
    void repeatedStatementWithDifferentBindValuesStillReturnsCorrectResultsPerCall() throws SQLException {
        JdbcBackendExecutor executor = new JdbcBackendExecutor(realConnection);
        String insert = "INSERT INTO jdbc_exec_test (val) VALUES (?)";
        for (int i = 0; i < 5; i++) {
            executor.execute(Statement.of(SourceDialect.POSTGRES, insert, List.of("row-" + i)));
        }

        ExecutionResult result = executor.execute(
                Statement.of(SourceDialect.POSTGRES, "SELECT val FROM jdbc_exec_test WHERE val LIKE 'row-%' ORDER BY val",
                        List.of()));
        assertEquals(5, result.rows().size());
        for (int i = 0; i < 5; i++) {
            assertEquals("row-" + i, result.rows().get(i).get(0),
                    "reusing one PreparedStatement object across calls must not leak a stale bound "
                            + "value from a previous execution into a later one");
        }
    }

    @Test
    void rebindClosesCachedStatementsAndDoesNotReuseThemAgainstTheNewConnection() throws Exception {
        AtomicInteger prepareCalls = new AtomicInteger();
        Connection counting = countingConnection(realConnection, prepareCalls);
        JdbcBackendExecutor executor = new JdbcBackendExecutor(counting);

        String sql = "SELECT val FROM jdbc_exec_test WHERE id = ?";
        executor.execute(Statement.of(SourceDialect.POSTGRES, sql, List.of(1L)));
        assertEquals(1, prepareCalls.get());

        try (Connection second = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password())) {
            AtomicInteger secondPrepareCalls = new AtomicInteger();
            Connection secondCounting = countingConnection(second, secondPrepareCalls);
            executor.rebind(secondCounting);

            executor.execute(Statement.of(SourceDialect.POSTGRES, sql, List.of(1L)));

            assertEquals(1, secondPrepareCalls.get(),
                    "the same SQL text must prepare again against the NEW connection after rebind -- "
                            + "a PreparedStatement from the old connection is never valid to reuse here");
        }
    }

    @Test
    void aFailingStatementIsEvictedAndDoesNotPoisonLaterCallsWithTheSameText() {
        JdbcBackendExecutor executor = new JdbcBackendExecutor(realConnection);
        String badSql = "SELECT val FROM jdbc_exec_test WHERE nonexistent_column = ?";

        assertTrue(assertThrowsSql(() -> executor.execute(Statement.of(SourceDialect.POSTGRES, badSql, List.of(1L)))));
        assertTrue(assertThrowsSql(() -> executor.execute(Statement.of(SourceDialect.POSTGRES, badSql, List.of(1L)))),
                "a statement that failed to prepare must be evicted from the cache, not handed back out "
                        + "again on the next identical call");
    }

    private static boolean assertThrowsSql(org.junit.jupiter.api.function.Executable executable) {
        try {
            executable.execute();
            return false;
        } catch (SQLException expected) {
            return true;
        } catch (Throwable other) {
            throw new AssertionError("expected SQLException, got " + other, other);
        }
    }
}
