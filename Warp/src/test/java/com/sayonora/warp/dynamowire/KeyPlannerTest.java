package com.sayonora.warp.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KeyPlannerTest {

    private static AttributeValue s(String v) { return AttributeValue.ofS(v); }

    private static TableSchema table() {
        TableSchema.Meta m = TableSchema.Meta.defaults("pk", "S", "sk", "S");
        Map<String, String> at = new LinkedHashMap<>(m.attributeTypes());
        at.put("kind", "S");
        at.put("createdAt", "S");
        at.put("alt", "S");
        TableSchema.IndexDef multi = new TableSchema.IndexDef("multi", false, List.of(new TableSchema.KeyAttr("kind", "S")),
                List.of(new TableSchema.KeyAttr("createdAt", "S"), new TableSchema.KeyAttr("alt", "S")), "ALL", List.of(), "ACTIVE", "dxi_m", 0, 0);
        TableSchema.IndexDef lsi = new TableSchema.IndexDef("lsi", true, List.of(new TableSchema.KeyAttr("pk", "S")),
                List.of(new TableSchema.KeyAttr("alt", "S")), "KEYS_ONLY", List.of(), "ACTIVE", "dxi_l", 0, 0);
        return new TableSchema("tbl", "pk", "S", "sk", "S", "ACTIVE", 0, m.withAttributeTypes(at).withIndexes(List.of(multi, lsi)));
    }

    private static List<Expr.KeyCond> terms(String expr, Object... kv) {
        ExpressionContext c = new ExpressionContext();
        for (int i = 0; i < kv.length; i += 2) c.values.put((String) kv[i], (AttributeValue) kv[i + 1]);
        return KeyPlanner.termsFromCondition(ExprParser.parseCondition(expr, "KeyConditionExpression", c));
    }

    private static String plan(TableSchema t, String index, List<Expr.KeyCond> terms) {
        try {
            KeyPlanner.plan(t, index == null ? null : t.index(index), terms);
            return "ok";
        } catch (DynamoException e) {
            return e.getMessage();
        }
    }

    @Test
    void tableKeyConditions() {
        TableSchema t = table();
        assertEquals("ok", plan(t, null, terms("pk = :p", ":p", s("a"))));
        assertEquals("ok", plan(t, null, terms("sk >= :s AND pk = :p", ":p", s("a"), ":s", s("b"))), "either order");
        assertEquals("ok", plan(t, null, terms("(pk = :p AND (sk BETWEEN :a AND :b))", ":p", s("a"), ":a", s("a"), ":b", s("z"))));
        assertEquals("ok", plan(t, null, terms("pk = :p AND begins_with(sk, :b)", ":p", s("a"), ":b", s("z"))));
        assertEquals("Query condition missed key schema element: pk", plan(t, null, terms("sk = :s", ":s", s("a"))));
        assertEquals("Query key condition not supported", plan(t, null, terms("pk = :p AND nope = :x", ":p", s("a"), ":x", s("b"))));
        assertEquals("Query key condition not supported", plan(t, null, terms("pk > :p", ":p", s("a"))));
        assertTrue(plan(t, null, terms("pk = :p", ":p", AttributeValue.ofN("1"))).contains("Condition parameter type does not match schema type"));
        assertTrue(assertThrows(DynamoException.class, () -> terms("pk = :p OR sk = :s", ":p", s("a"), ":s", s("b"))).getMessage().contains("OR"));
    }

    @Test
    void multiAttributeGsiRules() {
        TableSchema t = table();
        assertEquals("ok", plan(t, "multi", terms("kind = :s", ":s", s("x"))));
        assertEquals("ok", plan(t, "multi", terms("kind = :s AND createdAt > :c", ":s", s("x"), ":c", s("1"))));
        assertEquals("ok", plan(t, "multi", terms("kind = :s AND createdAt = :c AND alt > :a", ":s", s("x"), ":c", s("1"), ":a", s("2"))));
        assertTrue(plan(t, "multi", terms("kind = :s AND alt = :a", ":s", s("x"), ":a", s("2"))).contains("missed key schema element"), "sort keys are left to right without gaps");
        assertEquals("Query key condition not supported",
                plan(t, "multi", terms("kind = :s AND createdAt > :c AND alt = :a", ":s", s("x"), ":c", s("1"), ":a", s("2"))), "only the last sort key may use a range operator");
    }

    @Test
    void lsiUsesTheTablePartitionKeyAndItsOwnSortKey() {
        TableSchema t = table();
        assertEquals("ok", plan(t, "lsi", terms("pk = :p AND alt = :a", ":p", s("a"), ":a", s("b"))));
        assertEquals("Query key condition not supported", plan(t, "lsi", terms("pk = :p AND sk = :a", ":p", s("a"), ":a", s("b"))));
    }

    @Test
    void filterMayNotTouchTheQueriedKeys() {
        TableSchema t = table();
        ExpressionContext c = new ExpressionContext();
        c.values.put(":v", s("x"));
        Expr.Cond filter = ExprParser.parseCondition("createdAt > :v", "FilterExpression", c);
        assertTrue(assertThrows(DynamoException.class, () -> KeyPlanner.checkFilterHasNoKeys(filter, t, t.index("multi"))).getMessage()
                .contains("Primary key attribute: createdAt"));
        KeyPlanner.checkFilterHasNoKeys(filter, t, null);
    }

    @Test
    void startKeyShapeIsChecked() {
        TableSchema t = table();
        Map<String, AttributeValue> good = new LinkedHashMap<>(Map.of("pk", s("a"), "sk", s("b")));
        KeyPlanner.checkStartKey(t, null, good);
        assertThrows(DynamoException.class, () -> KeyPlanner.checkStartKey(t, null, Map.of("pk", s("a"))));
        assertThrows(DynamoException.class, () -> KeyPlanner.checkStartKey(t, null, Map.of("pk", AttributeValue.ofN("1"), "sk", s("b"))));
    }
}
