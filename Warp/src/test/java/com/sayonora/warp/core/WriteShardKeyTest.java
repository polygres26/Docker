package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.sayonora.warp.core.WriteShardKey.Broadcast;
import com.sayonora.warp.core.WriteShardKey.KeyChange;
import com.sayonora.warp.core.WriteShardKey.Keyed;
import com.sayonora.warp.core.WriteShardKey.NotApplicable;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class WriteShardKeyTest {

    private static WriteShardKey.Result r(String sql, Object... binds) {
        return WriteShardKey.analyze(sql, "orders", "customer_id", Arrays.asList(binds));
    }

    private static void keyed(String expected, String sql, Object... binds) {
        assertEquals(expected, assertInstanceOf(Keyed.class, r(sql, binds), sql).keyValue(), sql);
    }

    private static void broadcast(String sql, Object... binds) {
        assertInstanceOf(Broadcast.class, r(sql, binds), sql);
    }

    @Test
    void aKeyEqualityInAPlainConjunctionRoutesToOneShard() {
        keyed("42", "UPDATE orders SET amount = 5 WHERE customer_id = 42");
        keyed("42", "update ORDERS set amount = 5 where CUSTOMER_ID=42 and amount > 3 and id <> 7");
        keyed("42", "DELETE FROM orders WHERE customer_id = 42");
        keyed("42", "DELETE FROM public.orders o WHERE o.customer_id = 42");
        keyed("alice", "DELETE FROM orders WHERE customer_id = 'alice'");
        keyed("-3", "UPDATE orders SET amount = 1 WHERE id = 9 AND customer_id = -3");
        keyed("42", "DELETE orders WHERE customer_id = 42"); // SQL Server: no FROM
        keyed("42", "UPDATE `orders` SET `amount` = 5 WHERE `customer_id` = 42 ORDER BY id LIMIT 1"); // MySQL
        keyed("42", "UPDATE [dbo].[orders] SET [amount] = 5 WHERE [customer_id] = 42"); // SQL Server
        keyed("42", "UPDATE orders SET amount = 5 WHERE customer_id = 42 RETURNING id");
    }

    @Test
    void bindParametersAreMatchedByPositionIncludingThoseInTheSetList() {
        keyed("42", "UPDATE orders SET amount = ? WHERE customer_id = ?", 5, 42);
        keyed("42", "UPDATE orders SET amount = $1 WHERE customer_id = $2", 5, 42);
        keyed("42", "UPDATE orders SET amount = :1 WHERE customer_id = :2", 5, 42);
        keyed("42", "DELETE FROM orders WHERE id = ? AND customer_id = ?", 1, 42);
        broadcast("DELETE FROM orders WHERE customer_id = ?"); // no bind value: cannot prove one shard
        broadcast("DELETE FROM orders WHERE customer_id = ?", (Object) null);
    }

    @Test
    void whatCannotBeProvenToTouchOneShardIsBroadcast() {
        broadcast("UPDATE orders SET amount = 0");
        broadcast("DELETE FROM orders");
        broadcast("DELETE FROM orders WHERE amount = 0");
        broadcast("UPDATE orders SET amount = 0 WHERE customer_id > 10"); // a range
        broadcast("UPDATE orders SET amount = 0 WHERE customer_id IN (1, 2)");
        broadcast("DELETE FROM orders WHERE customer_id = 5 OR id = 7"); // the other branch may live elsewhere
        broadcast("DELETE FROM orders WHERE id IN (SELECT id FROM orders WHERE customer_id = 5)"); // a subquery, not the statement's own filter
        broadcast("UPDATE orders SET amount = (SELECT max(x) FROM t WHERE customer_id = 3) WHERE id = 1");
        broadcast("DELETE FROM orders WHERE note = 'customer_id = 5'"); // inside a string literal
        broadcast("DELETE FROM orders -- WHERE customer_id = 5\n WHERE id = 1");
    }

    @Test
    void theKeyOnTheLeftOfSetIsNotAFilter() {
        // the old text matcher routed this by the NEW value 9
        assertInstanceOf(KeyChange.class, r("UPDATE orders SET customer_id = 9 WHERE id = 1"));
        assertInstanceOf(KeyChange.class, r("UPDATE orders SET amount = 1, customer_id = 9 WHERE id = 1"));
        assertInstanceOf(KeyChange.class, r("UPDATE orders SET o.customer_id = 9 WHERE id = 1"));
        assertInstanceOf(KeyChange.class, r("UPDATE orders SET \"customer_id\" = ? WHERE id = ?", 9, 1));
        // naming the key on the right of an assignment, or in the WHERE only, is fine
        keyed("5", "UPDATE orders SET amount = customer_id WHERE customer_id = 5");
    }

    @Test
    void otherStatementsAndTablesAreNotThisAnalysis() {
        for (String sql : new String[] {"SELECT * FROM orders WHERE customer_id = 1", "INSERT INTO orders (customer_id) VALUES (1)",
                "UPDATE items SET x = 1", "DELETE FROM orders_archive", "UPDATE orders_2 SET x = 1"}) {
            assertInstanceOf(NotApplicable.class, r(sql), sql);
        }
    }
}
