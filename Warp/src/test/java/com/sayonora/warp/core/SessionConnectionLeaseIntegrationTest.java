package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.access.PhysicalSessionState;
import com.sayonora.warp.core.access.PostgresRlsSessionInitializer;
import com.sayonora.warp.testsupport.RealPostgres;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link SessionConnectionLease} against a real Postgres and a real HikariCP pool of ONE connection, so every
 * "two clients share a physical connection" claim is literally true here: the second client can only run because
 * the first one gave its connection back, and any state one leaves behind is visible to the next borrower unless
 * Warp cleans it.
 */
class SessionConnectionLeaseIntegrationTest {

    private static RealPostgres pg;
    private HikariDataSource pool;

    @BeforeAll
    static void startPostgres() throws Exception {
        pg = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE ROLE rls_reader LOGIN PASSWORD 'rls'");
            st.execute("CREATE TABLE docs (owner text, body text)");
            st.execute("INSERT INTO docs VALUES ('alice', 'alice-secret'), ('bob', 'bob-secret')");
            st.execute("GRANT SELECT ON docs TO rls_reader");
            st.execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY");
            st.execute("CREATE POLICY own_rows ON docs USING (owner = current_setting('warp.user_id', true))");
        }
    }

    @AfterAll
    static void stopPostgres() {
        if (pg != null) {
            pg.close();
        }
    }

    private HikariDataSource pool(String user, String password, int size) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(pg.jdbcUrl());
        cfg.setUsername(user);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(size);
        cfg.setMinimumIdle(0);
        cfg.setConnectionTimeout(400); // fail fast when the pool is exhausted
        return new HikariDataSource(cfg);
    }

    @BeforeEach
    void openPool() {
        pool = pool(pg.username(), pg.password(), 1);
    }

    @AfterEach
    void closePool() {
        pool.close();
    }

    private SessionConnectionLease lease() {
        return new SessionConnectionLease(pool::getConnection, connection -> pool.evictConnection(connection), true);
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void idleSessionsDoNotHoldTheOnlyConnection() throws Exception {
        // pool size 1, three "clients": each runs a statement and goes idle; none may starve the others
        SessionConnectionLease a = lease();
        SessionConnectionLease b = lease();
        SessionConnectionLease c = lease();
        assertEquals("1", scalar(a.acquire(), "SELECT 1"));
        a.releaseIfIdle();
        assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
        assertEquals("2", scalar(b.acquire(), "SELECT 2"));
        b.releaseIfIdle();
        assertEquals("3", scalar(c.acquire(), "SELECT 3"));
        c.releaseIfIdle();
        // and they are all still usable later
        assertEquals("4", scalar(a.acquire(), "SELECT 4"));
        a.releaseIfIdle();
        assertEquals(2, a.borrowCount());
        assertEquals(2, a.releaseCount());
    }

    @Test
    void openTransactionPinsUntilCommitAndOthersWaitThenFail() throws Exception {
        SessionConnectionLease tx = lease();
        SessionConnectionLease other = lease();
        Connection c = tx.begin();
        try (Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS pin_tx (id int)");
            st.execute("INSERT INTO pin_tx VALUES (1)");
        }
        tx.releaseIfIdle();
        assertTrue(tx.isHeld(), "a transaction must keep its connection");
        assertEquals("open transaction", tx.pinReason());
        assertSame(c, tx.acquire(), "same physical connection for every statement inside the transaction");
        assertThrows(SQLTransientConnectionException.class, other::acquire, "the only connection is pinned");

        tx.commit();
        tx.releaseIfIdle();
        assertFalse(tx.isHeld());
        // committed data is visible to the next client on the same physical connection
        assertEquals("1", scalar(other.acquire(), "SELECT count(*) FROM pin_tx"));
        other.releaseIfIdle();
    }

    @Test
    void rollbackDiscardsWorkAndUnpins() throws Exception {
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = admin.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS pin_rb (id int)");
        }
        SessionConnectionLease tx = lease();
        Connection c = tx.begin();
        try (Statement st = c.createStatement()) {
            st.execute("INSERT INTO pin_rb VALUES (1)");
        }
        tx.rollback();
        tx.releaseIfIdle();
        assertFalse(tx.isHeld());
        SessionConnectionLease other = lease();
        assertEquals("0", scalar(other.acquire(), "SELECT count(*) FROM pin_rb"));
        other.releaseIfIdle();
    }

    @Test
    void closingAPinnedTransactionSessionRollsBackAndReleases() throws Exception {
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = admin.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS pin_close (id int)");
        }
        SessionConnectionLease tx = lease();
        Connection c = tx.begin();
        try (Statement st = c.createStatement()) {
            st.execute("INSERT INTO pin_close VALUES (1)");
        }
        tx.close(); // client disconnected mid-transaction
        assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections(), "closing must release the pinned connection");
        SessionConnectionLease other = lease();
        assertEquals("0", scalar(other.acquire(), "SELECT count(*) FROM pin_close"), "uncommitted work rolled back");
        other.close();
    }

    @Test
    void sessionStatePinKeepsStateWhileHeldAndIsWipedOnClose() throws Exception {
        SessionConnectionLease a = lease();
        SessionConnectionLease b = lease();
        a.pinSessionState("SET");
        try (Statement st = a.acquire().createStatement()) {
            st.execute("SET warp.leak_probe = 'from-a'");
            st.execute("CREATE TEMP TABLE leak_tmp (id int)");
            st.execute("INSERT INTO leak_tmp VALUES (7)");
        }
        a.releaseIfIdle();
        assertTrue(a.isHeld(), "session state must keep the connection");
        assertEquals("from-a", scalar(a.acquire(), "SELECT current_setting('warp.leak_probe', true)"));
        assertEquals("7", scalar(a.acquire(), "SELECT id FROM leak_tmp"), "temp table survives across statements");
        a.close();
        Connection next = b.acquire();
        String probe = scalar(next, "SELECT coalesce(current_setting('warp.leak_probe', true), '')");
        assertEquals("", probe, "a session's SET must not leak to the next borrower");
        assertEquals("0", scalar(next, "SELECT count(*) FROM pg_tables WHERE tablename = 'leak_tmp'"),
                "a session's temp table must not leak to the next borrower");
        b.close();
    }

    @Test
    void plainSetIsReplayedOntoWhicheverConnectionTheSessionGetsAndNeverLeaksToOthers() throws Exception {
        SessionConnectionLease a = lease();
        SessionConnectionLease b = lease();
        // client A: SET application_name (executed on the held connection, then recorded -- what pgwire does)
        try (Statement st = a.acquire().createStatement()) {
            st.execute("SET application_name = 'mux_a'");
        }
        a.recordSetting("application_name", "SET application_name = 'mux_a'");
        a.releaseIfIdle();
        assertFalse(a.isHeld(), "a plain SET must not pin the connection");
        // client B, no settings, same (only) physical connection: must not see A's setting
        assertEquals("PostgreSQL JDBC Driver", scalar(b.acquire(), "SHOW application_name"),
                "A's SET leaked to another client (RESET ALL expected on hand-over)");
        b.releaseIfIdle();
        // A's next statement: its setting is replayed on the connection it gets now
        assertEquals("mux_a", scalar(a.acquire(), "SHOW application_name"));
        a.recordSetting("timezone", "SET TIME ZONE 'Asia/Tokyo'");
        try (Statement st = a.acquire().createStatement()) {
            st.execute("SET TIME ZONE 'Asia/Tokyo'");
        }
        a.releaseIfIdle();
        assertEquals("Asia/Tokyo", scalar(a.acquire(), "SHOW timezone"));
        a.forgetSetting("application_name");
        try (Statement st = a.acquire().createStatement()) {
            st.execute("RESET application_name");
        }
        a.releaseIfIdle();
        assertEquals("PostgreSQL JDBC Driver", scalar(a.acquire(), "SHOW application_name"));
        assertEquals("Asia/Tokyo", scalar(a.acquire(), "SHOW timezone"), "the other setting must survive");
        a.close();
        b.close();
    }

    @Test
    void killSwitchRestoresHoldForSession() throws Exception {
        SessionConnectionLease held = new SessionConnectionLease(pool::getConnection, null, false);
        assertEquals("1", scalar(held.acquire(), "SELECT 1"));
        held.releaseIfIdle();
        assertTrue(held.isHeld(), "multiplexing off: the connection stays with the session");
        assertEquals(1, pool.getHikariPoolMXBean().getActiveConnections());
        assertThrows(SQLTransientConnectionException.class, () -> lease().acquire());
        held.close();
        assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
    }

    @Test
    void cursorPinIsReversible() throws Exception {
        SessionConnectionLease s = lease();
        Connection c = s.begin();
        try (Statement st = c.createStatement()) {
            st.execute("DECLARE cur CURSOR FOR SELECT generate_series(1, 5)");
        }
        s.pinCursor();
        s.commit(); // a non-HOLD cursor dies with its transaction, so the pin dies too
        s.releaseIfIdle();
        assertFalse(s.isHeld());
    }

    @Test
    void rlsIdentityNeverLeaksBetweenClientsSharingAPhysicalConnection() throws Exception {
        pool.close();
        pool = pool("rls_reader", "rls", 1); // a role that is subject to the row-level-security policy
        SessionConnectionLease a = lease();
        SessionConnectionLease b = lease();
        SessionConnectionLease c = lease();
        JdbcBackendExecutor alice = new JdbcBackendExecutor(null, new PostgresRlsSessionInitializer());
        JdbcBackendExecutor anonymous = new JdbcBackendExecutor(null, new PostgresRlsSessionInitializer());
        JdbcBackendExecutor bob = new JdbcBackendExecutor(null, new PostgresRlsSessionInitializer());
        alice.bindLease(a);
        anonymous.bindLease(b);
        bob.bindLease(c);
        AccessContext aliceCtx = new AccessContext("alice", Set.of(), Map.of());
        AccessContext bobCtx = new AccessContext("bob", Set.of(), Map.of("tenant", "t1"));

        ExecutionResult r1 = alice.execute(stmt(aliceCtx, "SELECT body FROM docs ORDER BY body"));
        Object physicalA = PhysicalSessionState.physicalKey(a.acquire());
        a.releaseIfIdle();
        assertEquals(List.of(List.of("alice-secret")), r1.rows(), "alice sees only her rows");

        // the anonymous client gets the SAME physical connection: it must not see alice's context
        ExecutionResult r2 = anonymous.execute(stmt(AccessContext.ANONYMOUS,
                "SELECT coalesce(current_setting('warp.user_id', true), '<unset>') AS uid, (SELECT count(*) FROM docs) AS visible"));
        Object physicalB = PhysicalSessionState.physicalKey(b.acquire());
        b.releaseIfIdle();
        assertSame(physicalA, physicalB, "test precondition: both clients ran on one physical connection");
        String uid = String.valueOf(r2.rows().get(0).get(0));
        assertTrue(uid.isEmpty() || uid.equals("<unset>"), "anonymous client inherited identity: " + uid);
        assertEquals(0L, ((Number) r2.rows().get(0).get(1)).longValue(), "anonymous client saw rows it must not see");

        // a third client with a different identity and attribute
        ExecutionResult r3 = bob.execute(stmt(bobCtx,
                "SELECT current_setting('warp.user_id', true), current_setting('warp.tenant', true), "
                        + "(SELECT string_agg(body, ',') FROM docs)"));
        c.releaseIfIdle();
        assertEquals("bob", r3.rows().get(0).get(0));
        assertEquals("t1", r3.rows().get(0).get(1));
        assertEquals("bob-secret", r3.rows().get(0).get(2));

        // and alice again: bob's tenant attribute must be gone, her own identity back
        ExecutionResult r4 = alice.execute(stmt(aliceCtx,
                "SELECT current_setting('warp.user_id', true), coalesce(current_setting('warp.tenant', true), '')"));
        a.releaseIfIdle();
        assertEquals("alice", r4.rows().get(0).get(0));
        assertEquals("", r4.rows().get(0).get(1), "bob's attribute leaked into alice's session");
        a.close();
        b.close();
        c.close();
    }

    @Test
    void rolledBackTransactionInvalidatesAppliedIdentityAndItIsReapplied() throws Exception {
        pool.close();
        pool = pool("rls_reader", "rls", 1);
        SessionConnectionLease s = lease();
        JdbcBackendExecutor alice = new JdbcBackendExecutor(null, new PostgresRlsSessionInitializer());
        alice.bindLease(s);
        AccessContext aliceCtx = new AccessContext("alice", Set.of(), Map.of());
        s.begin();
        alice.execute(stmt(aliceCtx, "SELECT 1")); // set_config runs INSIDE the transaction
        s.rollback();                                   // ... and is rolled back with it
        ExecutionResult r = alice.execute(stmt(aliceCtx, "SELECT body FROM docs"));
        assertEquals(List.of(List.of("alice-secret")), r.rows(), "identity must be re-applied after a rollback");
        s.close();
    }

    @Test
    void identityAppliedOncePerPhysicalConnectionNotPerStatement() throws Exception {
        pool.close();
        pool = pool("rls_reader", "rls", 1);
        SessionConnectionLease s = lease();
        JdbcBackendExecutor alice = new JdbcBackendExecutor(null, new PostgresRlsSessionInitializer());
        alice.bindLease(s);
        AccessContext ctx = new AccessContext("alice", Set.of(), Map.of());
        alice.execute(stmt(ctx, "SELECT 1"));
        s.releaseIfIdle();
        PhysicalSessionState.State st = PhysicalSessionState.of(s.acquire());
        assertEquals(ctx, st.rls);
        // same physical connection handed back: recorded state already matches, nothing left to apply
        s.releaseIfIdle();
        alice.execute(stmt(ctx, "SELECT 2"));
        assertEquals(ctx, PhysicalSessionState.of(s.acquire()).rls);
        assertNotEquals(null, st.rls);
        s.close();
    }

    @Test
    void poolWaitThresholdRejectsFastWithTooManyConnectionsSqlState() throws Exception {
        // the default pool's real key, filled in the way PgConnections/BackendTarget do on every borrow
        String url = pg.jdbcUrl();
        String key = BackendConnectionPools.poolKeyFor(url, pg.username());
        List<Connection> held = new java.util.ArrayList<>();
        for (int i = 0; i < 30; i++) { // WARP_POOL_MAX_SIZE default
            held.add(BackendConnectionPools.borrow(key, url, pg.username(), pg.password()));
        }
        BackendConnectionPools.registerBackendAlias(BackendConnectionPools.DEFAULT_ALIAS, key);
        Thread waiter = new Thread(() -> {
            try (Connection ignored = BackendConnectionPools.borrow(key, url, pg.username(), pg.password())) {
                // got one after a release below
            } catch (SQLException e) {
                // pool timeout -- also fine, the waiter only has to be visible in the stats for a moment
            }
        });
        waiter.start();
        long deadline = System.currentTimeMillis() + 3000;
        while (BackendConnectionPools.statsForBackend(null).threadsAwaitingConnection() < 1
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        try {
            QosControlStage qos = new QosControlStage(new QosControlStage.ClassLimit(1000, 1000, 0), Map.of(), 1, null);
            long start = System.nanoTime();
            SQLException e = assertThrows(SQLException.class, () -> qos.handle(stmt(AccessContext.ANONYMOUS, "SELECT 1"),
                    s -> ExecutionResult.ofQuery(List.of(), List.of())));
            assertEquals("53300", e.getSQLState());
            assertTrue(e.getMessage().contains("Warp backend"), e.getMessage());
            assertTrue((System.nanoTime() - start) / 1_000_000 < 500, "must reject immediately, not wait");
            // a threshold above the number of waiters lets the statement through
            QosControlStage lenient = new QosControlStage(new QosControlStage.ClassLimit(1000, 1000, 0), Map.of(), 50, null);
            lenient.handle(stmt(AccessContext.ANONYMOUS, "SELECT 1"),
                    s -> ExecutionResult.ofQuery(List.of(), List.of()));
        } finally {
            for (Connection c : held) {
                c.close();
            }
            waiter.join(6000);
        }
    }

    @Test
    void exhaustedPoolRaisesActionableBackendPoolExhaustedException() throws Exception {
        String url = pg.jdbcUrl() + "?ApplicationName=exhaust-test";
        String key = BackendConnectionPools.poolKeyFor(url, pg.username());
        List<Connection> held = new java.util.ArrayList<>();
        for (int i = 0; i < 30; i++) {
            held.add(BackendConnectionPools.borrowForSession(key, url, pg.username(), pg.password()));
        }
        try {
            BackendPoolExhaustedException e = assertThrows(BackendPoolExhaustedException.class,
                    () -> BackendConnectionPools.borrowForSession(key, url, pg.username(), pg.password()));
            assertEquals("53300", e.getSQLState());
            assertTrue(e.getMessage().contains("WARP_POOL_MAX_SIZE"), e.getMessage());
            assertTrue(e.getMessage().contains("waited"), e.getMessage());
            assertEquals(30, e.maxPoolSize());
        } finally {
            for (Connection c : held) {
                c.close();
            }
        }
    }

    private static com.sayonora.warp.core.Statement stmt(AccessContext ctx, String sql) {
        return com.sayonora.warp.core.Statement.of(SourceDialect.POSTGRES, sql, List.of(), ctx);
    }
}
