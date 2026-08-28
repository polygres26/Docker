package com.nexagres.wire.oswire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.JsonObject;
import com.nexagres.wire.core.BackendRegistry;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * As {@code com.nexagres.wire.mongowire.PostgresDocumentStoreDrainRoutingTest}, for oswire:
 * {@link PostgresSearchStore} previously resolved every backend (including each shard target in
 * the doc_id-hash point ops and scatter-merge search built earlier this session) via the exact,
 * unredirected {@code BackendRegistry.get} -- proves {@code resolveForRouting} now actually
 * redirects its traffic too.
 */
class PostgresSearchStoreDrainRoutingTest {

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
    void indexDocumentRedirectsToTheFallbackWhileDefaultIsDraining() throws Exception {
        PostgresSearchStore store = new PostgresSearchStore(registry);

        JsonObject doc = new JsonObject();
        doc.addProperty("name", "widget");
        store.indexDocument("products", "doc-1", doc, null);
        assertEquals(1, countRows(primary, PostgresSearchStore.pgTableName("products")));

        registry.setState(BackendRegistry.DEFAULT_BACKEND_NAME, BackendRegistry.BackendState.DRAINING);

        // Deliberately the SAME collection as the pre-drain index -- ensureCollection's
        // ensured-table cache is keyed per (collection, physical backend jdbcUrl), not per
        // collection alone, exactly so this keeps working: a flat per-collection cache would mean
        // the fallback, which never saw this collection before, never gets its table created and
        // this write would fail with "relation does not exist" instead of landing here.
        store.indexDocument("products", "doc-2", doc, null);
        assertEquals(1, countRows(primary, PostgresSearchStore.pgTableName("products")),
                "the DRAINING default backend must not receive the second index write (still just the first)");
        assertEquals(1, countRows(fallback, PostgresSearchStore.pgTableName("products")),
                "resolveForRouting must have redirected the second index write to the configured "
                        + "fallback, including creating the table there for the first time");
    }

    private static int countRows(RealPostgres pg, String qualifiedTable) throws Exception {
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + qualifiedTable)) {
                return rs.next() ? rs.getInt(1) : 0;
            } catch (java.sql.SQLException e) {
                return 0;
            }
        }
    }
}
