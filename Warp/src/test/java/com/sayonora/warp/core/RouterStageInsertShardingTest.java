package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** An INSERT into a declaratively sharded table goes to the shard that owns its key, on every dialect, or is refused with a reason. */
class RouterStageInsertShardingTest {

    private static final BackendRegistry REGISTRY = new BackendRegistry(Map.of(
            "default", new BackendTarget("default", "jdbc:postgresql://h:5432/db", "u", "p"),
            "s1", new BackendTarget("s1", "jdbc:postgresql://h1:5432/db", "u", "p"),
            "s2", new BackendTarget("s2", "jdbc:mysql://h2:3306/db", "u", "p")), List.of());

    private static final RouterStage ROUTER = RouterStage.fromConfig(null, null, null, null, "orders:hash:customer_id:s1,s2", REGISTRY);

    private static String routed(SourceDialect dialect, String sql, Object... binds) throws SQLException {
        String[] captured = new String[1];
        ROUTER.handle(new Statement("t", dialect, sql, java.util.Arrays.asList(binds), "default", null, AccessContext.ANONYMOUS), s -> {
            captured[0] = s.targetBackend();
            return ExecutionResult.ofQuery(List.of(), List.of());
        });
        return captured[0];
    }

    @Test
    void literalInsertsLandOnTheShardThatAKeyedReadWillLookAt() throws SQLException {
        Set<String> used = new HashSet<>();
        for (int customer = 1; customer <= 40; customer++) {
            String insertShard = routed(SourceDialect.POSTGRES, "INSERT INTO orders (id, customer_id, amount) VALUES (" + customer + ", " + customer + ", 5)");
            String readShard = routed(SourceDialect.POSTGRES, "SELECT * FROM orders WHERE customer_id = " + customer);
            assertEquals(readShard, insertShard, "customer " + customer + " is read from the shard it was written to");
            assertNotEquals("default", insertShard);
            used.add(insertShard);
        }
        assertEquals(Set.of("s1", "s2"), used, "rows spread over both shards");
    }

    @Test
    void bindParameterInsertsRouteTheSameWayOnEveryDialectsBindStyle() throws SQLException {
        for (int customer = 1; customer <= 20; customer++) {
            String expected = routed(SourceDialect.POSTGRES, "SELECT * FROM orders WHERE customer_id = " + customer);
            assertEquals(expected, routed(SourceDialect.POSTGRES, "INSERT INTO orders (id, customer_id) VALUES ($1, $2)", 1, customer));
            assertEquals(expected, routed(SourceDialect.MYSQL, "INSERT INTO `orders` (`id`, `customer_id`) VALUES (?, ?)", 1, customer));
            assertEquals(expected, routed(SourceDialect.SQL_SERVER, "INSERT INTO [dbo].[orders] ([id], [customer_id]) VALUES (?, ?)", 1, customer));
            assertEquals(expected, routed(SourceDialect.ORACLE, "INSERT INTO orders (id, customer_id) VALUES (:1, :2)", 1, customer));
        }
    }

    @Test
    void aMultiRowInsertThatStaysOnOneShardIsRoutedAndOneThatSpansShardsIsRefused() throws SQLException {
        String one = routed(SourceDialect.POSTGRES, "SELECT * FROM orders WHERE customer_id = 1");
        // find a second customer on the other shard
        int other = 2;
        while (routed(SourceDialect.POSTGRES, "SELECT * FROM orders WHERE customer_id = " + other).equals(one)) {
            other++;
        }
        assertEquals(one, routed(SourceDialect.POSTGRES, "INSERT INTO orders (id, customer_id) VALUES (1, 1), (2, 1)"));
        final int split = other;
        // spanning shards is no longer refused: it is cut into one INSERT per shard by the executor
        assertEquals(RoutingBackendExecutor.SCATTER_ALL,
                routed(SourceDialect.POSTGRES, "INSERT INTO orders (id, customer_id) VALUES (1, 1), (2, " + split + ")"));
        assertEquals(RoutingBackendExecutor.SCATTER_ALL,
                routed(SourceDialect.MYSQL, "INSERT INTO orders (id, customer_id) VALUES (?, ?), (?, ?)", 1, 1, 2, split));
        // numbered binds cannot be regrouped without renumbering, so those are still refused
        SQLException spans = assertThrows(SQLException.class,
                () -> routed(SourceDialect.POSTGRES, "INSERT INTO orders (id, customer_id) VALUES ($1, $2), ($3, $4)", 1, 1, 2, split));
        assertTrue(spans.getMessage().contains("different shards"), spans.getMessage());
    }

    @Test
    void aWriteThatAlsoReadsAnotherShardedTableOrItselfIsRefused() {
        RouterStage two = RouterStage.fromConfig(null, null, null, null,
                "orders:hash:customer_id:s1,s2|items:hash:order_id:s1,s2", REGISTRY);
        for (String sql : new String[] {
                "UPDATE orders SET amount = 1 FROM items WHERE items.order_id = orders.id",
                "DELETE FROM orders WHERE id IN (SELECT order_id FROM items)",
                "UPDATE orders o SET amount = 0 WHERE o.amount > (SELECT max(amount) FROM orders)",
                "UPDATE orders o JOIN items i ON i.order_id = o.id SET o.amount = 1"}) {
            SQLException e = assertThrows(SQLException.class, () -> two.handle(new Statement("t", SourceDialect.POSTGRES, sql,
                    List.of(), "default", null, AccessContext.ANONYMOUS), s -> ExecutionResult.ofUpdate(0)), sql);
            assertTrue(e.getMessage().contains("also reads sharded table"), e.getMessage());
        }
    }

    @Test
    void aWriteJoiningAnUnshardedReferenceTableIsStillAllowed() throws SQLException {
        assertEquals(RoutingBackendExecutor.SCATTER_ALL, routed(SourceDialect.POSTGRES,
                "UPDATE orders SET amount = r.rate FROM rates r WHERE r.id = 1"));
        assertEquals("s1".equals(routed(SourceDialect.POSTGRES, "SELECT * FROM orders WHERE customer_id = 3")) ? "s1" : "s2",
                routed(SourceDialect.POSTGRES, "UPDATE orders SET amount = r.rate FROM rates r WHERE customer_id = 3"));
    }

    @Test
    void whatCannotBeRoutedIsRefusedInsteadOfLandingOnTheDefaultBackend() {
        for (String sql : new String[] {"INSERT INTO orders VALUES (1, 5, 10)", "INSERT INTO orders (id, amount) VALUES (1, 10)",
                "INSERT INTO orders (id, customer_id) SELECT id, cid FROM staging", "INSERT INTO orders (id, customer_id) VALUES (1, NULL)"}) {
            SQLException e = assertThrows(SQLException.class, () -> routed(SourceDialect.POSTGRES, sql), sql);
            assertTrue(e.getMessage().contains("cannot route this INSERT"), e.getMessage());
            assertTrue(e.getMessage().contains("orders") && e.getMessage().contains("customer_id"), e.getMessage());
        }
    }

    @Test
    void otherStatementsAndOtherTablesAreUnchanged() throws SQLException {
        assertEquals("default", routed(SourceDialect.POSTGRES, "INSERT INTO items (id, name) VALUES (1, 'x')"));
        assertEquals("default", routed(SourceDialect.POSTGRES, "INSERT INTO orders_archive (customer_id) VALUES (1)"));
        assertEquals("default", routed(SourceDialect.POSTGRES, "UPDATE items SET name = 'y'"));
    }

    @Test
    void anUpdateOrDeleteWithTheKeyGoesToTheOwningShardAndWithoutItToEveryShard() throws SQLException {
        for (int customer = 1; customer <= 20; customer++) {
            String owner = routed(SourceDialect.POSTGRES, "SELECT * FROM orders WHERE customer_id = " + customer);
            assertEquals(owner, routed(SourceDialect.POSTGRES, "UPDATE orders SET amount = 1 WHERE customer_id = " + customer));
            assertEquals(owner, routed(SourceDialect.POSTGRES, "DELETE FROM orders WHERE id = 5 AND customer_id = " + customer));
            assertEquals(owner, routed(SourceDialect.MYSQL, "DELETE FROM `orders` WHERE `customer_id` = ?", customer));
            assertEquals(owner, routed(SourceDialect.SQL_SERVER, "UPDATE [orders] SET [amount] = ? WHERE [customer_id] = ?", 5, customer));
        }
        for (String sql : new String[] {"UPDATE orders SET amount = 0", "DELETE FROM orders", "DELETE FROM orders WHERE amount = 0",
                "DELETE FROM orders WHERE customer_id = 3 OR id = 9", "UPDATE orders SET amount = 0 WHERE customer_id > 4"}) {
            assertEquals(RoutingBackendExecutor.SCATTER_ALL, routed(SourceDialect.POSTGRES, sql), sql);
        }
    }

    @Test
    void assigningTheShardKeyIsRefused() {
        SQLException e = assertThrows(SQLException.class, () -> routed(SourceDialect.POSTGRES, "UPDATE orders SET customer_id = 9 WHERE id = 1"));
        assertTrue(e.getMessage().contains("cannot UPDATE the shard key"), e.getMessage());
    }
}
