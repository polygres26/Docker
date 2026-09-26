package com.sayonora.warp.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * {@link CacheTriggerInstaller} against a real Postgres (same docker-CLI fixture every other
 * real-infra test here uses) -- the idempotency, privilege-failure and payload-cap claims in its
 * javadoc are exactly the kind that only mean something when a real {@code pg_trigger} catalog
 * and a real {@code pg_notify} are on the other end.
 */
class CacheTriggerInstallerTest {

    private static RealPostgres postgres;

    @BeforeAll
    static void startPostgres() throws Exception {
        postgres = RealPostgres.start();
        try (Connection c = admin(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id int PRIMARY KEY, total numeric)");
            st.execute("CREATE TABLE dynamo_item_orders (pk_value text NOT NULL, sk_value text NOT NULL DEFAULT '', "
                    + "sk_num numeric, item jsonb NOT NULL, PRIMARY KEY (pk_value, sk_value))");
            st.execute("CREATE TABLE nokey (a int, b int)");
            st.execute("CREATE SCHEMA \"Shop\"");
            st.execute("CREATE TABLE \"Shop\".\"Items\" (id text PRIMARY KEY, doc jsonb)");
        }
    }

    @AfterAll
    static void stopPostgres() {
        if (postgres != null) {
            postgres.close();
        }
    }

    private static Connection admin() throws SQLException {
        return DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
    }

    private static BackendTarget target() {
        return new BackendTarget("default", postgres.jdbcUrl(), postgres.username(), postgres.password());
    }

    private static Map<String, Long> triggerOids(Connection c) throws SQLException {
        Map<String, Long> out = new TreeMap<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT tgrelid::regclass::text || '/' || tgname, oid FROM pg_trigger "
                        + "WHERE tgname LIKE 'warp_cache_%' AND NOT tgisinternal")) {
            while (rs.next()) {
                out.put(rs.getString(1), rs.getLong(2));
            }
        }
        return out;
    }

    @Test
    void installTwiceIsANoOpTheSecondTimeAndUninstallDropsEverything() throws Exception {
        CacheTriggerInstaller installer = new CacheTriggerInstaller(
                List.of("orders", "public.dynamo_item_orders", "\"Shop\".\"Items\"", "nokey", "does_not_exist"),
                List.of("dynamo_item_orders", "\"Shop\".\"Items\"", "nokey"));

        int created = installer.install(target());
        // orders: 1 stmt trigger; dynamo_item_orders: ins/upd/del/trunc; Shop.Items: same 4;
        // nokey: forced back to table mode (no PK) -> 1; does_not_exist: skipped.
        assertEquals(10, created, "first install must create every trigger");
        Map<String, Long> first;
        try (Connection c = admin()) {
            first = triggerOids(c);
        }
        assertEquals(10, first.size(), "expected exactly 10 warp_cache_% triggers -- got " + first);
        assertTrue(first.containsKey("nokey/warp_cache_notify_stmt"), "a table with no PK gets the table-level trigger: " + first);
        assertTrue(first.containsKey("\"Shop\".\"Items\"/warp_cache_notify_upd"), "quoted mixed-case names resolve: " + first);

        // The idempotency claim, proven by catalog identity: an unchanged trigger keeps its
        // pg_trigger OID, a dropped-and-recreated one would get a new one.
        assertEquals(0, installer.install(target()), "second install must not create anything");
        try (Connection c = admin()) {
            assertEquals(first, triggerOids(c), "no trigger may have been dropped and recreated on a no-op install");
        }

        // Switching a table from rows mode to table mode is a real change and must be applied.
        CacheTriggerInstaller changed = new CacheTriggerInstaller(List.of("dynamo_item_orders"), List.of());
        assertEquals(1, changed.install(target()));
        try (Connection c = admin()) {
            Map<String, Long> after = triggerOids(c);
            assertFalse(after.containsKey("dynamo_item_orders/warp_cache_notify_upd"), "stale rows-mode triggers must go: " + after);
            assertTrue(after.containsKey("dynamo_item_orders/warp_cache_notify_stmt"));
            assertEquals(first.get("orders/warp_cache_notify_stmt"), after.get("orders/warp_cache_notify_stmt"),
                    "a table that wasn't in this install's list is left untouched");
        }

        try (Connection c = admin(); Statement st = c.createStatement()) {
            String script = installer.uninstallScript(c);
            assertTrue(script.contains("DROP FUNCTION IF EXISTS warp_cache_notify_rows()"), script);
            st.execute(script);
            assertTrue(triggerOids(c).isEmpty(), "uninstall must drop every warp_cache_% trigger");
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_proc WHERE proname LIKE 'warp_cache_notify_%'")) {
                rs.next();
                assertEquals(0, rs.getInt(1), "uninstall must drop both functions");
            }
        }
    }

    @Test
    void installScriptCarriesBothFunctionsAndCanonicalTriggerDdl() throws Exception {
        CacheTriggerInstaller installer = new CacheTriggerInstaller(List.of("orders", "dynamo_item_orders"), List.of("dynamo_item_orders"));
        try (Connection c = admin()) {
            String script = installer.installScript(c);
            assertTrue(script.contains("CREATE OR REPLACE FUNCTION warp_cache_notify_stmt()"), script);
            assertTrue(script.contains("CREATE OR REPLACE FUNCTION warp_cache_notify_rows()"), script);
            assertTrue(script.contains("CREATE TRIGGER warp_cache_notify_stmt AFTER INSERT OR DELETE OR UPDATE OR TRUNCATE ON public.orders "
                    + "FOR EACH STATEMENT EXECUTE FUNCTION warp_cache_notify_stmt();"), script);
            assertTrue(script.contains("CREATE TRIGGER warp_cache_notify_upd AFTER UPDATE ON public.dynamo_item_orders REFERENCING OLD TABLE "
                    + "AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION warp_cache_notify_rows('pk_value', 'sk_value');"), script);
        }
    }

    @Test
    void aRoleWithoutTriggerPrivilegeGetsAnErrorNamingThePrivilege() throws Exception {
        try (Connection c = admin(); Statement st = c.createStatement()) {
            st.execute("CREATE ROLE limited LOGIN PASSWORD 'limited'");
            st.execute("GRANT CREATE, USAGE ON SCHEMA public TO limited");
            st.execute("GRANT SELECT, UPDATE ON orders TO limited");
        }
        CacheTriggerInstaller installer = new CacheTriggerInstaller(List.of("orders"), List.of());
        BackendTarget limited = new BackendTarget("limited", postgres.jdbcUrl(), "limited", "limited");
        SQLException e = assertThrows(SQLException.class, () -> installer.install(limited));
        assertTrue(e.getMessage().contains("TRIGGER privilege"), "must name the missing privilege -- got: " + e.getMessage());
        assertTrue(e.getMessage().contains("CREATE TRIGGER warp_cache_notify_stmt"), "must name the failing SQL -- got: " + e.getMessage());
        try (Connection c = admin()) {
            assertFalse(triggerOids(c).containsKey("orders/warp_cache_notify_stmt"),
                    "the whole install is one transaction -- nothing may be left behind after a failure");
        }
    }

    @Test
    void refusesANonPostgresBackend() {
        CacheTriggerInstaller installer = new CacheTriggerInstaller(List.of("orders"), List.of());
        BackendTarget oracle = new BackendTarget("ora", "jdbc:oracle:thin:@//h:1521/x", "u", "p");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> installer.install(oracle));
        assertTrue(e.getMessage().contains("Postgres-only"), e.getMessage());
    }

    @Test
    void rowsModePayloadStaysUnderTheNotifyCapForATenThousandRowUpdate() throws Exception {
        CacheTriggerInstaller installer = new CacheTriggerInstaller(List.of("dynamo_item_orders"), List.of("dynamo_item_orders"));
        installer.install(target());
        try (Connection writer = admin(); Connection listener = admin(); Statement st = writer.createStatement()) {
            listener.createStatement().execute("LISTEN " + CacheTriggerInstaller.CHANNEL);
            st.execute("TRUNCATE dynamo_item_orders");
            // The batch INSERT itself also fires a (degraded) notify; drain it before the UPDATE.
            st.execute("INSERT INTO dynamo_item_orders (pk_value, sk_value, item) SELECT 'k' || g, 's', '{}'::jsonb FROM generate_series(1, 10000) g");
            PGNotification[] insertNotes = listener.unwrap(PGConnection.class).getNotifications(5000);
            assertNotNull(insertNotes);
            assertEquals(1, insertNotes.length, "exactly ONE notify per statement, however many rows");

            st.execute("UPDATE dynamo_item_orders SET item = '{\"x\":1}'::jsonb");
            PGNotification[] notes = listener.unwrap(PGConnection.class).getNotifications(5000);
            assertNotNull(notes);
            assertEquals(1, notes.length, "exactly ONE notify per statement");
            String payload = notes[0].getParameter();
            assertTrue(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 8000, "payload must stay under pg_notify's 8000-byte cap: " + payload.length());
            CacheInvalidationListener.Payload parsed = CacheInvalidationListener.Payload.parse(payload);
            assertNotNull(parsed);
            assertEquals("dynamo_item_orders", parsed.table());
            assertEquals("UPDATE", parsed.op());
            assertNull(parsed.keys(), "a 10k-row update must degrade to keys:null (whole-table) -- got: " + payload);

            // And a small, targeted UPDATE really does carry its keys.
            st.execute("UPDATE dynamo_item_orders SET item = '{\"x\":2}'::jsonb WHERE pk_value IN ('k1', 'k2')");
            notes = listener.unwrap(PGConnection.class).getNotifications(5000);
            assertEquals(1, notes.length);
            parsed = CacheInvalidationListener.Payload.parse(notes[0].getParameter());
            assertNotNull(parsed.keys(), notes[0].getParameter());
            assertEquals(2, parsed.keys().size(), notes[0].getParameter());
            assertEquals("s", parsed.keys().get(0)[1]);

            // TRUNCATE has no transition tables: the separate table-level trigger covers it.
            st.execute("TRUNCATE dynamo_item_orders");
            notes = listener.unwrap(PGConnection.class).getNotifications(5000);
            assertEquals(1, notes.length);
            parsed = CacheInvalidationListener.Payload.parse(notes[0].getParameter());
            assertEquals("TRUNCATE", parsed.op());
            assertNull(parsed.keys());
        }
    }
}
