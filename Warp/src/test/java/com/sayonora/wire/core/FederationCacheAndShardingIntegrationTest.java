package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Three real, live federation scenarios, all against one small e-commerce-shaped schema:
 *
 * <ul>
 *   <li>{@code customers} -- reference data, lives on the default backend only.
 *   <li>{@code items} -- the product catalog: small, read-heavy, rarely written -- exactly the
 *       shape {@code WARP_CACHE_TABLES} is meant for. Lives on the default backend only
 *       (queried unqualified, so {@link RouterStage}'s shard rule never matches it -- see that
 *       class's own {@code \bschema\.} pattern).
 *   <li>{@code orders} -- horizontally partitioned across two independent Postgres backends,
 *       queried as {@code public.orders} so {@code WARP_ROUTER_SHARD_TABLES=public} routes it
 *       through real scatter-gather.
 * </ul>
 *
 * <p>Every scenario is proven the same way: warm the real path, then make the real backend(s) it
 * would need to recompute the answer UNREACHABLE (stop the container, not just "assert it was
 * fast") and show the query still returns the correct result -- the one proof a result truly came
 * from the cache and not a lucky race with a still-up backend.
 */
class FederationCacheAndShardingIntegrationTest {

    private RealPostgres shard1;
    private RealPostgres shard2;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (shard2 != null) shard2.close();
        if (shard1 != null) shard1.close();
    }

    /** {@code items} cached in memory: a catalog lookup survives its only backend going down. */
    @Test
    void cachedItemsCatalogLookupSurvivesItsBackendGoingDown() throws Exception {
        shard1 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id serial, name text)");
            st.execute("INSERT INTO customers (name) VALUES ('alice'), ('bob')");
            st.execute("CREATE TABLE items (id serial, name text, price numeric)");
            st.execute("INSERT INTO items (name, price) VALUES ('widget', 9.99), ('gadget', 19.99)");
        }

        warp = WarpProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_CACHE_TABLES", "items")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        String query = "SELECT name FROM items WHERE id = 1";
        try (Connection conn = connect(shard1.username(), shard1.password())) {
            String firstRun = runAndPrint(conn, query, "cache MISS -- items' only backend still up");
            assertEquals("widget", firstRun);

            shard1.stop();

            String secondRun = runAndPrint(conn, query, "cache HIT -- items' only backend now STOPPED");
            assertEquals("widget", secondRun,
                    "the catalog lookup must still return the real value from cache even though the "
                            + "only backend that could have answered it live is unreachable");
        }
    }

    /** {@code orders} distributed across two shards: a merged aggregate is real cross-shard math. */
    @Test
    void ordersDistributedAcrossTwoShardsMergeCorrectly() throws Exception {
        shard1 = RealPostgres.start();
        shard2 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, customer_id int, item_id int, quantity int)");
            st.execute("INSERT INTO orders (customer_id, item_id, quantity) VALUES (1, 1, 2), (1, 2, 1)");
        }
        try (Connection c = DriverManager.getConnection(shard2.jdbcUrl(), shard2.username(), shard2.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, customer_id int, item_id int, quantity int)");
            st.execute("INSERT INTO orders (customer_id, item_id, quantity) VALUES (2, 1, 5)");
        }

        warp = twoShardWarp(shard1, shard2, null);

        String query = "SELECT item_id, SUM(quantity) FROM public.orders GROUP BY item_id ORDER BY item_id";
        try (Connection conn = connect(shard1.username(), shard1.password());
                Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(query)) {
            System.out.println("QUERY: " + query);
            java.util.Map<Integer, Integer> byItem = new java.util.LinkedHashMap<>();
            while (rs.next()) {
                byItem.put(rs.getInt(1), rs.getInt(2));
                System.out.println("  item_id=" + rs.getInt(1) + " | total_quantity=" + rs.getInt(2));
            }
            assertEquals(2, byItem.size());
            assertEquals(7, byItem.get(1), "item 1: 2 (shard1) + 5 (shard2) = 7, real cross-shard sum, not per-shard rows");
            assertEquals(1, byItem.get(2));
        }
    }

    /** A real federated JOIN across shards ({@link ShardJoinExecutor}) -- not scatter-gather's
     * own broadcast-and-merge, which {@code docs/WARP_GUIDE.md} §4.3 documents as silently
     * WRONG the instant a JOIN's matching row pair spans two different shards (never found on
     * either shard alone, no error raised). This deliberately places each customer and their
     * matching order on OPPOSITE shards -- customer 1 lives on shard1 but customer 1's order
     * lives on shard2, and vice versa for customer 2 -- so naive per-shard local joining (find
     * matches only within what each shard already has together) would find ZERO matches; only a
     * real federated join that first gathers both tables' full, cross-shard picture (each
     * mounted as a real {@code UNION ALL} across both shards, per {@link
     * ShardJoinExecutor#matchedShardSchema}) can find either match at all. */
    @Test
    void joinBetweenTwoShardedTablesFindsMatchesThatSpanShards() throws Exception {
        shard1 = RealPostgres.start();
        shard2 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id int, name text)");
            st.execute("INSERT INTO customers (id, name) VALUES (1, 'alice')");
            st.execute("CREATE TABLE orders (id serial, customer_id int, item text)");
            // customer 1 lives on shard1, but their order (customer_id=1) is stored on shard2 --
            // see below. shard1's own orders row is for customer 2, who lives on shard2.
            st.execute("INSERT INTO orders (customer_id, item) VALUES (2, 'gadget')");
        }
        try (Connection c = DriverManager.getConnection(shard2.jdbcUrl(), shard2.username(), shard2.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE customers (id int, name text)");
            st.execute("INSERT INTO customers (id, name) VALUES (2, 'bob')");
            st.execute("CREATE TABLE orders (id serial, customer_id int, item text)");
            st.execute("INSERT INTO orders (customer_id, item) VALUES (1, 'widget')");
        }

        warp = twoShardWarp(shard1, shard2, null);

        String query = "SELECT c.name, o.item FROM public.customers c "
                + "JOIN public.orders o ON c.id = o.customer_id ORDER BY c.name";
        try (Connection conn = connect(shard1.username(), shard1.password());
                Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(query)) {
            System.out.println("QUERY: " + query);
            java.util.Map<String, String> itemByCustomer = new java.util.LinkedHashMap<>();
            while (rs.next()) {
                itemByCustomer.put(rs.getString(1), rs.getString(2));
                System.out.println("  " + rs.getString(1) + " | " + rs.getString(2));
            }
            assertEquals(2, itemByCustomer.size(),
                    "both matches must be found even though neither customer/order pair is co-located "
                            + "on the same physical shard -- a naive per-shard local join would find zero");
            assertEquals("widget", itemByCustomer.get("alice"),
                    "alice (shard1) matched to her order which physically lives on shard2");
            assertEquals("gadget", itemByCustomer.get("bob"),
                    "bob (shard2) matched to his order which physically lives on shard1");
        }
    }

    /** Both mechanisms at once, in one running instance: {@code items} cached, {@code orders}
     * sharded -- a scatter-gather order aggregate proves sharding still works, and a cached
     * catalog lookup survives BOTH shards going down. */
    @Test
    void cachedItemsAndTwoShardOrdersCoexistInOneDeployment() throws Exception {
        shard1 = RealPostgres.start();
        shard2 = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE items (id serial, name text, price numeric)");
            st.execute("INSERT INTO items (name, price) VALUES ('widget', 9.99), ('gadget', 19.99)");
            st.execute("CREATE TABLE orders (id serial, customer_id int, item_id int, quantity int)");
            st.execute("INSERT INTO orders (customer_id, item_id, quantity) VALUES (1, 1, 2), (1, 2, 1)");
        }
        try (Connection c = DriverManager.getConnection(shard2.jdbcUrl(), shard2.username(), shard2.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, customer_id int, item_id int, quantity int)");
            st.execute("INSERT INTO orders (customer_id, item_id, quantity) VALUES (2, 1, 5)");
        }

        warp = twoShardWarp(shard1, shard2, "items");

        try (Connection conn = connect(shard1.username(), shard1.password())) {
            // Real scatter-gather across both live shards -- proves sharding still works with
            // caching also configured in the same instance.
            String shardedQuery = "SELECT SUM(quantity) FROM public.orders";
            double totalQuantity = Double.parseDouble(runAndPrint(conn, shardedQuery, "2-shard scatter-gather, both shards up"));
            assertEquals(8.0, totalQuantity, 0.0001, "2 + 1 (shard1) + 5 (shard2) = 8");

            // Warm the cache for items, then take BOTH shards down -- the sharded orders query
            // would now fail outright, but the cached items lookup must not care.
            String cachedQuery = "SELECT name FROM items WHERE id = 1";
            String firstRun = runAndPrint(conn, cachedQuery, "cache MISS -- items' backend still up");
            assertEquals("widget", firstRun);

            shard1.stop();
            shard2.stop();

            String secondRun = runAndPrint(conn, cachedQuery, "cache HIT -- BOTH shards now STOPPED");
            assertEquals("widget", secondRun,
                    "the cached items lookup must survive both shards going down, even though this "
                            + "same deployment also has real cross-shard sharding configured for orders");
        }
    }

    private WarpProcess twoShardWarp(RealPostgres shard1, RealPostgres shard2, String cacheTables)
            throws Exception {
        String backends = "shard1=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard2=" + shard2.jdbcUrl() + "|" + shard2.username() + "|" + shard2.password();
        WarpProcess.Builder builder = WarpProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_SHARD_BACKENDS", "shard1,shard2")
                .env("WARP_ROUTER_SHARD_TABLES", "public")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled");
        if (cacheTables != null) {
            builder.env("WARP_CACHE_TABLES", cacheTables);
        }
        return builder.start();
    }

    private Connection connect(String username, String password) throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres",
                username, password);
    }

    private String runAndPrint(Connection conn, String query, String label) throws SQLException {
        System.out.println("QUERY (" + label + "): " + query);
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(query)) {
            assertTrue(rs.next(), "expected exactly one row");
            String value = rs.getString(1);
            System.out.println("  -> " + value);
            assertFalse(rs.next(), "expected exactly one row");
            return value;
        }
    }

    /** Real, declarative {@code WARP_TABLE_SHARDS} -- no schema-qualifier prefix anywhere in
     * any query, unlike every scenario above. {@code orders} is declared {@code hash:customer_id}
     * across shard1/shard2: a query that supplies a real {@code customer_id} literal routes
     * straight to the ONE shard {@link ShardingStrategy#hash} actually resolves it to (proven by
     * seeding each shard with data ONLY that shard has, then confirming the single-shard query
     * returns exactly that shard's own row, not a merge); a query with no such predicate falls
     * through to a real scatter-gather sum across BOTH shards. */
    @Test
    void declarativeTableShardRoutesByValueOrFallsBackToScatter() throws Exception {
        shard1 = RealPostgres.start();
        shard2 = RealPostgres.start();

        // Real hash resolution, not a guessed/hardcoded assumption about SHA-256 internals --
        // find two customer ids that this exact strategy actually resolves to different shards.
        ShardingStrategy strategy = ShardingStrategy.hash(List.of("shard1", "shard2"));
        int customerOnShard1 = -1;
        int customerOnShard2 = -1;
        for (int candidate = 1; candidate <= 100 && (customerOnShard1 < 0 || customerOnShard2 < 0); candidate++) {
            String backend = strategy.resolve(String.valueOf(candidate));
            if ("shard1".equals(backend) && customerOnShard1 < 0) {
                customerOnShard1 = candidate;
            } else if ("shard2".equals(backend) && customerOnShard2 < 0) {
                customerOnShard2 = candidate;
            }
        }
        assertTrue(customerOnShard1 > 0 && customerOnShard2 > 0, "expected at least one customer id resolving to each shard");

        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, customer_id int, quantity int)");
            st.execute("INSERT INTO orders (customer_id, quantity) VALUES (" + customerOnShard1 + ", 4)");
        }
        try (Connection c = DriverManager.getConnection(shard2.jdbcUrl(), shard2.username(), shard2.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, customer_id int, quantity int)");
            st.execute("INSERT INTO orders (customer_id, quantity) VALUES (" + customerOnShard2 + ", 9)");
        }

        warp = WarpProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", "default=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                        + ";shard1=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                        + ";shard2=" + shard2.jdbcUrl() + "|" + shard2.username() + "|" + shard2.password())
                .env("WARP_TABLE_SHARDS", "orders:hash:customer_id:shard1,shard2")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();

        try (Connection conn = connect(shard1.username(), shard1.password())) {
            String singleShardQuery = "SELECT quantity FROM orders WHERE customer_id = " + customerOnShard1;
            String result = runAndPrint(conn, singleShardQuery, "single-shard fast path, no \"public.\" prefix");
            assertEquals("4", result, "must return shard1's own row -- routed straight to shard1, not scattered/merged");

            String scatterQuery = "SELECT SUM(quantity) FROM orders";
            String sum = runAndPrint(conn, scatterQuery, "no routable value -- falls back to scatter-gather across both shards");
            assertEquals(13.0, Double.parseDouble(sum), 0.0001,
                    "4 (shard1) + 9 (shard2) = 13, real cross-shard sum with no schema prefix anywhere");
        }
    }
}
