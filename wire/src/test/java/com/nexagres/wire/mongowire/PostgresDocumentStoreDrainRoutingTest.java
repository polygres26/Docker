package com.nexagres.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves the switchover/failover mechanism (drain -> {@code BackendRegistry.resolveForRouting}
 * preferring a configured fallback) now actually redirects mongowire's own traffic, not just
 * pgwire/mssqlwire/mywire/orawire's. {@link PostgresDocumentStore} previously resolved every
 * backend via the exact, unredirected {@code BackendRegistry.get} -- this is package-scoped (same
 * package as the store, which has a package-private constructor and methods) so it can exercise
 * the store directly against two real Postgres containers without needing to also speak the full
 * Mongo wire protocol -- {@code insertOne} is the real, same method {@code MongoWireSessionHandler}
 * calls for a client's {@code insert} command either way.
 */
class PostgresDocumentStoreDrainRoutingTest {

    private RealPostgres primary;
    private RealPostgres fallback;
    private BackendRegistry registry;

    @BeforeEach
    void startInfra() throws Exception {
        primary = RealPostgres.start();
        fallback = RealPostgres.start();
        registry = BackendRegistry.fromConfig(
                "default=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password() + "|fallback"
                        + ";fallback=" + fallback.jdbcUrl() + "|" + fallback.username() + "|" + fallback.password(),
                null);
    }

    @AfterEach
    void stopInfra() {
        if (primary != null) primary.close();
        if (fallback != null) fallback.close();
    }

    @Test
    void insertOneRedirectsToTheFallbackWhileDefaultIsDraining() throws Exception {
        PostgresDocumentStore store = new PostgresDocumentStore(registry);

        store.insertOne("testdb", "orders", new Document("name", "alice"));
        assertEquals(1, countRows(primary, "testdb.orders"));

        registry.setState(BackendRegistry.DEFAULT_BACKEND_NAME, BackendRegistry.BackendState.DRAINING);

        // Deliberately the SAME collection as the pre-drain insert -- ensureTable's ensured-schema
        // cache is keyed per (collection, physical backend URL), not per collection alone, exactly
        // so this keeps working: a flat per-collection cache would mean the fallback, which never
        // saw this collection before, never gets its table created and this insert would fail with
        // "relation does not exist" instead of landing here.
        store.insertOne("testdb", "orders", new Document("name", "bob"));
        assertEquals(1, countRows(primary, "testdb.orders"),
                "the DRAINING default backend must not receive the second insert (still just the first)");
        assertEquals(1, countRows(fallback, "testdb.orders"),
                "resolveForRouting must have redirected the second insert to the configured fallback, "
                        + "including creating the table there for the first time");
    }

    private static int countRows(RealPostgres pg, String qualifiedTable) throws Exception {
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + qualifiedTable)) {
                return rs.next() ? rs.getInt(1) : 0;
            } catch (java.sql.SQLException e) {
                // Table never got created on this backend -- exactly the "0 rows here" case some
                // assertions above are checking for.
                return 0;
            }
        }
    }
}
