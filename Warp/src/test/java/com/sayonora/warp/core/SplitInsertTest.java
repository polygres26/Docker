package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** A multi-row INSERT that spans shards is cut into one INSERT per shard, each with exactly its own rows and binds. */
class SplitInsertTest {

    private static final RouterStage.TableShardRule RULE = new RouterStage.TableShardRule("orders",
            Pattern.compile("\\borders\\b", Pattern.CASE_INSENSITIVE), "customer_id", ShardingStrategy.list(Map.of("1", "s1", "2", "s2", "3", "s1")));

    private static Map<String, Statement> split(String sql, Object... binds) throws SQLException {
        List<Object> b = Arrays.asList(binds);
        var routed = assertInstanceOf(InsertShardKey.Routed.class, InsertShardKey.analyze(sql, "orders", "customer_id", b));
        return RoutingBackendExecutor.splitInsert(new Statement("t", SourceDialect.POSTGRES, sql, b, "default", null, AccessContext.ANONYMOUS),
                RULE, routed);
    }

    @Test
    void literalRowsAreRegroupedByOwnerInOrder() throws SQLException {
        var out = split("INSERT INTO orders (id, customer_id) VALUES (10, 1), (11, 2), (12, 3) ON CONFLICT DO NOTHING");
        assertEquals("INSERT INTO orders (id, customer_id) VALUES (10, 1), (12, 3) ON CONFLICT DO NOTHING", out.get("s1").sqlText().replace("  ", " "));
        assertEquals("INSERT INTO orders (id, customer_id) VALUES (11, 2) ON CONFLICT DO NOTHING", out.get("s2").sqlText().replace("  ", " "));
    }

    @Test
    void positionalBindsFollowTheirRows() throws SQLException {
        var out = split("INSERT INTO orders (id, customer_id, note) VALUES (?, ?, 'a?b'), (?, ?, ?), (?, ?, ?)", 10, 1, 11, 2, "x", 12, 3, "y");
        assertEquals(List.of(10, 1, 12, 3, "y"), out.get("s1").bindParams());
        assertEquals(List.of(11, 2, "x"), out.get("s2").bindParams());
    }

    @Test
    void numberedBindsAndReturningAreNotSplittable() {
        assertNull(((InsertShardKey.Routed) InsertShardKey.analyze("INSERT INTO orders (id, customer_id) VALUES ($1, $2), ($3, $4)", "orders",
                "customer_id", List.of(1, 1, 2, 2))).split());
        assertNull(((InsertShardKey.Routed) InsertShardKey.analyze("INSERT INTO orders (id, customer_id) VALUES (1, 1), (2, 2) RETURNING id", "orders",
                "customer_id", List.of())).split());
        assertNotNull(((InsertShardKey.Routed) InsertShardKey.analyze("INSERT INTO orders (id, customer_id) VALUES (1, 1), (2, 2)", "orders",
                "customer_id", List.of())).split());
    }
}
