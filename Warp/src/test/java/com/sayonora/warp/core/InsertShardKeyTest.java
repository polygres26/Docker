package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.InsertShardKey.NotApplicable;
import com.sayonora.warp.core.InsertShardKey.Routed;
import com.sayonora.warp.core.InsertShardKey.Unroutable;
import java.util.List;
import org.junit.jupiter.api.Test;

class InsertShardKeyTest {

    private static List<String> routed(String sql, Object... binds) {
        var r = InsertShardKey.analyze(sql, "orders", "customer_id", java.util.Arrays.asList(binds));
        return assertInstanceOf(Routed.class, r, String.valueOf(r)).keyValues();
    }

    private static String unroutable(String sql, Object... binds) {
        var r = InsertShardKey.analyze(sql, "orders", "customer_id", java.util.Arrays.asList(binds));
        return assertInstanceOf(Unroutable.class, r, String.valueOf(r)).reason();
    }

    @Test
    void aLiteralRowAnyCaseAnyColumnOrder() {
        assertEquals(List.of("42"), routed("INSERT INTO orders (id, customer_id, amount) VALUES (1, 42, 10)"));
        assertEquals(List.of("42"), routed("insert into ORDERS (amount, CUSTOMER_ID, id) values (10, 42, 1)"));
        assertEquals(List.of("-7"), routed("INSERT INTO orders (customer_id) VALUES (-7)"));
        assertEquals(List.of("alice"), routed("INSERT INTO orders (customer_id, id) VALUES ('alice', 1)"));
        assertEquals(List.of("it's"), routed("INSERT INTO orders (customer_id) VALUES ('it''s')"));
    }

    @Test
    void multiRowKeepsRowOrder() {
        assertEquals(List.of("1", "2", "3"), routed("INSERT INTO orders (id, customer_id) VALUES (10, 1), (11, 2), (12, 3)"));
    }

    @Test
    void quotedAndQualifiedNamesInEveryDialectsQuoting() {
        assertEquals(List.of("5"), routed("INSERT INTO public.orders (\"id\", \"customer_id\") VALUES (1, 5)"));
        assertEquals(List.of("5"), routed("INSERT INTO `shop`.`orders` (`id`, `customer_id`) VALUES (1, 5)"));
        assertEquals(List.of("5"), routed("INSERT INTO [dbo].[orders] ([id], [customer_id]) VALUES (1, 5)"));
        assertEquals(List.of("5"), routed("INSERT orders (id, customer_id) VALUES (1, 5)")); // INTO is optional on MySQL and SQL Server
        assertEquals(List.of("5"), routed("INSERT IGNORE INTO orders (id, customer_id) VALUE (1, 5)")); // MySQL
    }

    @Test
    void bindParametersByPosition() {
        assertEquals(List.of("42"), routed("INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)", 1, 42, 10));
        assertEquals(List.of("42"), routed("INSERT INTO orders (id, customer_id, amount) VALUES ($1, $2, $3)", 1, 42, 10));
        assertEquals(List.of("42"), routed("INSERT INTO orders (id, customer_id, amount) VALUES (:1, :2, :3)", 1, 42, 10));
        assertEquals(List.of("7", "8"), routed("INSERT INTO orders (id, customer_id) VALUES (?, ?), (?, ?)", 1, 7, 2, 8));
        // a literal in front of the key does not use up a placeholder, a placeholder inside a function call does
        assertEquals(List.of("9"), routed("INSERT INTO orders (id, customer_id) VALUES (coalesce(?, 0), ?)", 1, 9));
    }

    @Test
    void stringsCommentsAndNestingDoNotConfuseTheRowReader() {
        assertEquals(List.of("3"), routed("INSERT INTO orders (note, customer_id) VALUES ('a, (b), ?', 3)"));
        assertEquals(List.of("3"), routed("/* batch */ INSERT INTO orders -- target\n (note, customer_id) VALUES (concat('x', lower('Y,Z')), /* key */ 3)"));
    }

    @Test
    void trailingClausesAreIgnored() {
        assertEquals(List.of("5"), routed("INSERT INTO orders (id, customer_id) VALUES (1, 5) ON CONFLICT (id) DO NOTHING RETURNING id"));
        assertEquals(List.of("5"), routed("INSERT INTO orders (id, customer_id) VALUES (1, 5) ON DUPLICATE KEY UPDATE amount = amount + 1"));
    }

    @Test
    void notAnInsertIntoThisTable() {
        for (String sql : new String[] {"SELECT * FROM orders WHERE customer_id = 1", "UPDATE orders SET amount = 1",
                "INSERT INTO orders_archive (customer_id) VALUES (1)", "INSERT INTO items (id) VALUES (1)", "DELETE FROM orders"}) {
            assertInstanceOf(NotApplicable.class, InsertShardKey.analyze(sql, "orders", "customer_id", List.of()), sql);
        }
    }

    @Test
    void whatCannotBeRoutedIsRefusedWithAReasonNotGuessed() {
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) SELECT id, cid FROM staging").contains("SELECT"));
        assertTrue(unroutable("INSERT INTO orders SELECT * FROM staging").contains("INSERT ... VALUES"));
        assertTrue(unroutable("INSERT INTO orders VALUES (1, 5, 10)").contains("no column list"));
        assertTrue(unroutable("INSERT INTO orders (id, amount) VALUES (1, 10)").contains("does not include"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1, NULL)").contains("NULL"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1, DEFAULT)").contains("DEFAULT"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1, 2 + 3)").contains("expression"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1, lower('A'))").contains("expression"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1, :cid)").contains("named bind"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1, ?)").contains("no value to route on"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (?, ?)", 1, null).contains("NULL"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1)").contains("1 values for 2 columns"));
        assertTrue(unroutable("INSERT INTO orders (id, customer_id) VALUES (1, 'a' || 'b')").contains("expression"));
    }
}
