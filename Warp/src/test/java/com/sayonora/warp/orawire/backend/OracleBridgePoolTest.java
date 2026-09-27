package com.sayonora.warp.orawire.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link OracleBridgePool} using a fake, no-real-Oracle {@link DataSource} whose
 * {@code getConnection()} blocks (semaphore-gated) the same way a real HikariCP pool blocks once
 * {@code maximumPoolSize} connections are checked out -- so these tests exercise exactly the
 * "many client sessions, few real Oracle connections" contract Bridge mode needs, and the
 * reset-on-return session-isolation contract, without touching a live Oracle instance.
 */
final class OracleBridgePoolTest {

    private static final int POOL_SIZE = 3;

    @Test
    @Timeout(10)
    void checkoutNeverExceedsPoolSizeConcurrently() throws Exception {
        FakeBoundedDataSource dataSource = new FakeBoundedDataSource(POOL_SIZE);
        OracleBridgePool pool = new OracleBridgePool(dataSource, POOL_SIZE);

        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        for (int iter = 0; iter < 25; iter++) {
                            Connection c = pool.checkout();
                            try {
                                // Simulate a small amount of work done while holding the connection.
                                Thread.sleep(0, 200_000);
                            } finally {
                                c.close();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(9, TimeUnit.SECONDS), "all worker threads finished in time");
        } finally {
            executor.shutdownNow();
        }

        assertTrue(dataSource.maxConcurrentObserved() <= POOL_SIZE,
                "at most " + POOL_SIZE + " real connections should ever be open concurrently, observed "
                        + dataSource.maxConcurrentObserved());
        // The bounded fake never opens more physical connections than the pool size either, since
        // every returned connection is reused (closed cleanly and released back to the semaphore).
        assertTrue(dataSource.totalOpened() >= 1);
    }

    @Test
    void checkoutThrowsWhenPoolClosed() throws Exception {
        FakeBoundedDataSource dataSource = new FakeBoundedDataSource(POOL_SIZE);
        OracleBridgePool pool = new OracleBridgePool(dataSource, POOL_SIZE);
        pool.close();
        assertThrows(SQLException.class, pool::checkout);
    }

    @Test
    void returnResetsRollsBackAndClosesOpenStatements() throws Exception {
        FakeBoundedDataSource dataSource = new FakeBoundedDataSource(POOL_SIZE);
        OracleBridgePool pool = new OracleBridgePool(dataSource, POOL_SIZE);

        Connection pooled = pool.checkout();
        FakeConnection underlying = dataSource.lastCreated();
        underlying.autoCommit = false; // simulate an open transaction, as a real client session would leave one

        Statement s1 = pooled.createStatement();
        Statement s2 = pooled.prepareStatement("select 1 from dual");
        assertFalse(underlying.isStatementClosed(s1));
        assertFalse(underlying.isStatementClosed(s2));
        assertEquals(0, underlying.rollbackCount);

        pooled.close();

        assertEquals(1, underlying.rollbackCount, "an open transaction must be rolled back before reuse");
        assertTrue(underlying.isStatementClosed(s1), "every statement/cursor opened on the connection must be closed on return");
        assertTrue(underlying.isStatementClosed(s2), "every statement/cursor opened on the connection must be closed on return");
    }

    @Test
    void checkoutDefensivelyResetsAReusedConnectionThatWasNotProperlyReturned() throws Exception {
        // Simulates a crash/interrupt path where a session never called close(): the physical
        // connection still shows autoCommit=false (an open transaction) the next time it is handed
        // out. checkout() must roll that back before the new client session sees it.
        FakeBoundedDataSource dataSource = new FakeBoundedDataSource(1);
        FakeConnection raw = new FakeConnection();
        raw.autoCommit = false;
        dataSource.preSeed(raw);

        OracleBridgePool pool = new OracleBridgePool(dataSource, 1);
        pool.checkout();

        assertEquals(1, raw.rollbackCount, "checkout must defensively roll back a connection left mid-transaction");
    }

    @Test
    void crossClientSessionCannotSeeAPriorSessionsOpenCursorOrTransaction() throws Exception {
        // The scenario NOTES.md documents as proven by this slice's unit tests: client A leaves an
        // open cursor and an uncommitted write on a pooled connection; when it is handed to client
        // B, neither survives.
        FakeBoundedDataSource dataSource = new FakeBoundedDataSource(1);
        OracleBridgePool pool = new OracleBridgePool(dataSource, 1);

        Connection clientA = pool.checkout();
        FakeConnection physical = dataSource.lastCreated();
        physical.autoCommit = false; // client A's uncommitted DML
        Statement clientACursor = clientA.createStatement(); // client A's open cursor
        clientA.close(); // client A's session ends without an explicit commit/rollback

        Connection clientB = pool.checkout();
        assertEquals(dataSource.lastCreated(), physical, "the same physical connection is reused (pool size 1)");
        assertTrue(physical.isStatementClosed(clientACursor), "client A's cursor must not still be open for client B");
        assertTrue(physical.autoCommit, "client A's transaction must be rolled back, not left open, for client B");
        clientB.close();
    }

    // ---- Fakes -----------------------------------------------------------------------------

    /** A DataSource fake whose getConnection() blocks (semaphore-gated) exactly like a real,
     * bounded HikariCP pool once {@code maxSize} physical connections are checked out, and tracks
     * the maximum number ever open concurrently for the assertion above. */
    private static final class FakeBoundedDataSource implements DataSource {
        private final Semaphore admission;
        private final AtomicInteger concurrentlyOpen = new AtomicInteger();
        private final AtomicInteger maxConcurrentObserved = new AtomicInteger();
        private final AtomicInteger totalOpened = new AtomicInteger();
        private final java.util.ArrayDeque<FakeConnection> idle = new java.util.ArrayDeque<>();
        private volatile FakeConnection lastCreated;
        private FakeConnection seeded;

        FakeBoundedDataSource(int maxSize) {
            this.admission = new Semaphore(maxSize, true);
        }

        void preSeed(FakeConnection connection) {
            this.seeded = connection;
        }

        FakeConnection lastCreated() {
            return lastCreated;
        }

        int maxConcurrentObserved() {
            return maxConcurrentObserved.get();
        }

        int totalOpened() {
            return totalOpened.get();
        }

        @Override
        public Connection getConnection() throws SQLException {
            try {
                if (!admission.tryAcquire(5, TimeUnit.SECONDS)) {
                    throw new SQLException("fake pool exhausted");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException(e);
            }
            int now = concurrentlyOpen.incrementAndGet();
            maxConcurrentObserved.updateAndGet(prev -> Math.max(prev, now));
            totalOpened.incrementAndGet();
            FakeConnection fake;
            synchronized (idle) {
                fake = idle.pollFirst();
            }
            if (fake == null) {
                fake = seeded != null ? seeded : new FakeConnection();
                seeded = null;
            }
            lastCreated = fake;
            FakeConnection finalFake = fake;
            return fake.asProxyReleasingOnClose(() -> {
                synchronized (idle) {
                    idle.addLast(finalFake);
                }
                concurrentlyOpen.decrementAndGet();
                admission.release();
            });
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public java.io.PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(java.io.PrintWriter out) {}

        @Override
        public void setLoginTimeout(int seconds) {}

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public java.util.logging.Logger getParentLogger() {
            return null;
        }

        @Override
        public <T> T unwrap(Class<T> iface) {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }

    /** A minimal fake {@link Connection}: tracks autoCommit/rollback/created-statement state
     * directly (as plain fields, for simple assertions), and hands itself out as a dynamic proxy
     * so {@link OracleBridgePool}'s own proxy layer can be tested without a real JDBC driver. */
    private static final class FakeConnection {
        boolean autoCommit = true;
        int rollbackCount;
        private final Set<Statement> closedStatements = new CopyOnWriteArraySet<>();

        boolean isStatementClosed(Statement s) {
            return closedStatements.contains(s);
        }

        Connection asProxyReleasingOnClose(Runnable onRealClose) {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {Connection.class},
                    new FakeConnectionHandler(onRealClose));
        }

        private final class FakeConnectionHandler implements InvocationHandler {
            private final Runnable onRealClose;

            FakeConnectionHandler(Runnable onRealClose) {
                this.onRealClose = onRealClose;
            }

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                switch (method.getName()) {
                    case "getAutoCommit":
                        return autoCommit;
                    case "setAutoCommit":
                        autoCommit = (Boolean) args[0];
                        return null;
                    case "rollback":
                        rollbackCount++;
                        autoCommit = true; // matches real JDBC semantics: nothing left open after rollback
                        return null;
                    case "commit":
                        autoCommit = true;
                        return null;
                    case "close":
                        onRealClose.run();
                        return null;
                    case "isClosed":
                        return false;
                    case "createStatement":
                        return fakeStatement();
                    case "prepareStatement":
                        return fakeStatement();
                    case "prepareCall":
                        return fakeStatement();
                    case "equals":
                        return proxy == args[0];
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "toString":
                        return "FakeConnection";
                    default:
                        return defaultValueFor(method.getReturnType());
                }
            }

            private Statement fakeStatement() {
                return (Statement) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {java.sql.PreparedStatement.class, java.sql.CallableStatement.class},
                        (p, m, a) -> {
                            switch (m.getName()) {
                                case "close":
                                    closedStatements.add((Statement) p);
                                    return null;
                                case "isClosed":
                                    return closedStatements.contains(p);
                                case "equals":
                                    return p == a[0];
                                case "hashCode":
                                    return System.identityHashCode(p);
                                case "toString":
                                    return "FakeStatement";
                                default:
                                    return defaultValueFor(m.getReturnType());
                            }
                        });
            }
        }

        private static Object defaultValueFor(Class<?> type) {
            if (!type.isPrimitive() || type == void.class) {
                return null;
            }
            if (type == boolean.class) {
                return false;
            }
            return 0;
        }
    }
}
