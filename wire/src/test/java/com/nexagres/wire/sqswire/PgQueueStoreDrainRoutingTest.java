package com.nexagres.wire.sqswire;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
 * As {@code com.nexagres.wire.mongowire.PostgresDocumentStoreDrainRoutingTest} /
 * {@code com.nexagres.wire.oswire.PostgresSearchStoreDrainRoutingTest}, for sqswire: {@link
 * PgQueueStore} previously resolved every backend (including its catalog and per-queue shard
 * target) via the exact, unredirected {@code BackendRegistry.get} -- proves {@code
 * resolveForRouting} now actually redirects its traffic too.
 */
class PgQueueStoreDrainRoutingTest {

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
    void sendMessageRedirectsToTheFallbackWhileDefaultIsDraining() throws Exception {
        PgQueueStore store = new PgQueueStore(registry);

        store.createQueue("before-drain-queue", new PgQueueStore.QueueAttributes(30, false, null, null));
        store.sendMessage("before-drain-queue", "hello", null, null);
        assertEquals(1, store.countMessages("before-drain-queue").visible());

        registry.setState(BackendRegistry.DEFAULT_BACKEND_NAME, BackendRegistry.BackendState.DRAINING);

        // A different queue than the pre-drain send -- each queue's own table gets created lazily
        // on whichever backend createQueue resolves to, so this exercises resolveForRouting on a
        // fresh resolution rather than reusing an already-open connection to primary.
        store.createQueue("after-drain-queue", new PgQueueStore.QueueAttributes(30, false, null, null));
        store.sendMessage("after-drain-queue", "world", null, null);

        assertEquals(0, countRows(primary, "sqs_queue_after_drain_queue"),
                "the DRAINING default backend must not receive this queue's table or message");
        assertEquals(1, countRows(fallback, "sqs_queue_after_drain_queue"),
                "resolveForRouting must have redirected queue creation and the message to the configured fallback");
    }

    private static int countRows(RealPostgres pg, String table) throws Exception {
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
                return rs.next() ? rs.getInt(1) : 0;
            } catch (java.sql.SQLException e) {
                return 0;
            }
        }
    }
}
