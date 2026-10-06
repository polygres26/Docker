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
        SQLException spans = assertThrows(SQLException.class,
                () -> routed(SourceDialect.POSTGRES, "INSERT INTO orders (id, customer_id) VALUES (1, 1), (2, " + split + ")"));
        assertTrue(spans.getMessage().contains("different shards"), spans.getMessage());
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
}
