package com.sayonora.wire.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests of the expression engine: parser, condition evaluation, update application, projection. */
class ExprEngineTest {

    private static AttributeValue s(String v) { return AttributeValue.ofS(v); }
    private static AttributeValue n(String v) { return AttributeValue.ofN(v); }

    private static Map<String, AttributeValue> item() {
        Map<String, AttributeValue> m = new LinkedHashMap<>();
        m.put("pk", s("a"));
        m.put("s", s("hello"));
        m.put("n", n("10"));
        m.put("t", AttributeValue.ofBool(true));
        m.put("nul", AttributeValue.ofNull());
        m.put("l", AttributeValue.ofL(new java.util.ArrayList<>(List.of(s("x"), n("2"), AttributeValue.ofM(new LinkedHashMap<>(Map.of("k", s("v"))))))));
        Map<String, AttributeValue> inner = new LinkedHashMap<>();
        inner.put("c", n("3"));
        Map<String, AttributeValue> mm = new LinkedHashMap<>();
        mm.put("a", s("1"));
        mm.put("b", AttributeValue.ofM(inner));
        m.put("m", AttributeValue.ofM(mm));
        m.put("ss", AttributeValue.ofSet(AttributeValue.Type.SS, new LinkedHashSet<>(List.of("a", "b"))));
        m.put("ns", AttributeValue.ofSet(AttributeValue.Type.NS, new LinkedHashSet<>(List.of("1", "2.5"))));
        return m;
    }

    private static ExpressionContext ctx(Object... kv) {
        ExpressionContext c = new ExpressionContext();
        for (int i = 0; i < kv.length; i += 2) {
            String k = (String) kv[i];
            if (k.startsWith("#")) c.names.put(k, (String) kv[i + 1]);
            else c.values.put(k, (AttributeValue) kv[i + 1]);
        }
        return c;
    }

    private static boolean cond(String expr, Object... kv) {
        return ExprEval.test(ExprParser.parseCondition(expr, "ConditionExpression", ctx(kv)), item());
    }

    private static String error(Runnable r) {
        return assertThrows(DynamoException.class, r::run).getMessage();
    }

    @Test
    void comparisonsFollowDynamoDbSemantics() {
        assertTrue(cond("s = :v", ":v", s("hello")));
        assertTrue(cond("n = :v", ":v", n("10.0")), "numbers compare by value");
        assertTrue(cond("n < :v AND n >= :w", ":v", n("11"), ":w", n("10")));
        assertFalse(cond("s = :v", ":v", n("1")), "different types are never equal");
        assertTrue(cond("s <> :v", ":v", n("1")));
        assertFalse(cond("s < :v", ":v", n("1")), "and never ordered");
        assertTrue(cond("t = :v", ":v", AttributeValue.ofBool(true)), "BOOL equality");
        assertTrue(cond("t <> :v", ":v", AttributeValue.ofBool(false)));
        assertTrue(cond("nul = :v", ":v", AttributeValue.ofNull()));
        assertFalse(cond("nothere = :v", ":v", s("x")));
        assertTrue(cond("nothere <> :v", ":v", s("x")), "a missing attribute is <> anything");
        assertTrue(cond("NOT nothere = :v", ":v", s("x")));
        assertTrue(cond("ss = :v", ":v", AttributeValue.ofSet(AttributeValue.Type.SS, new LinkedHashSet<>(List.of("b", "a")))), "sets are unordered");
        assertTrue(cond("m.b.c = :v AND l[1] = :w AND l[2].k = :x", ":v", n("3"), ":w", n("2"), ":x", s("v")));
        assertTrue(cond("#p.#q = :v", "#p", "m", "#q", "a", ":v", s("1")));
    }

    @Test
    void functionsAndOperators() {
        assertTrue(cond("attribute_exists(m.b.c) AND attribute_not_exists(zz)"));
        assertTrue(cond("attribute_type(s, :t) AND attribute_type(l, :l)", ":t", s("S"), ":l", s("L")));
        assertFalse(cond("attribute_type(s, :t)", ":t", s("N")));
        assertTrue(cond("begins_with(s, :p)", ":p", s("he")));
        assertTrue(cond("contains(s, :p) AND contains(ss, :a) AND contains(ns, :one) AND contains(l, :two)",
                ":p", s("ell"), ":a", s("a"), ":one", n("1"), ":two", n("2")));
        assertFalse(cond("contains(n, :p)", ":p", n("1")));
        assertTrue(cond("size(s) = :five AND size(l) = :three AND size(ss) = :two AND size(m) = :two", ":five", n("5"), ":three", n("3"), ":two", n("2")));
        assertTrue(cond("n BETWEEN :a AND :b", ":a", n("5"), ":b", n("10")));
        assertFalse(cond("n BETWEEN :a AND :b", ":a", n("11"), ":b", n("20")));
        assertTrue(cond("s IN (:a, :b)", ":a", s("x"), ":b", s("hello")));
        assertTrue(cond("(n = :a OR s = :b) AND NOT t = :f", ":a", n("1"), ":b", s("hello"), ":f", AttributeValue.ofBool(false)));
        assertFalse(cond("(n = :a OR s = :b) AND NOT t = :f", ":a", n("1"), ":b", s("nope"), ":f", AttributeValue.ofBool(false)));
        assertTrue(cond("n = :a and s = :b or t = :f", ":a", n("10"), ":b", s("hello"), ":f", AttributeValue.ofBool(false)), "keywords are case-insensitive");
    }

    @Test
    void errorsMatchDynamoDbMessages() {
        assertEquals("Invalid ConditionExpression: Attribute name is a reserved keyword; reserved keyword: name",
                error(() -> cond("name = :v", ":v", s("x"))));
        assertEquals("Invalid ConditionExpression: An expression attribute value used in expression is not defined; attribute value: :v",
                error(() -> cond("s = :v")));
        assertEquals("Invalid ConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #x",
                error(() -> cond("#x = :v", ":v", s("x"))));
        assertTrue(error(() -> cond("s == :v", ":v", s("x"))).startsWith("Invalid ConditionExpression: Syntax error; token: \"=\""));
        assertTrue(error(() -> cond("s = ")).contains("<EOF>"));
        assertEquals("Invalid ConditionExpression: Invalid function name; function: foo", error(() -> cond("foo(s)")));
        assertEquals("Invalid ConditionExpression: Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2",
                error(() -> cond("attribute_exists(s, n)")));
        assertEquals("Invalid ConditionExpression: The expression has redundant parentheses;", error(() -> cond("((s = :v))", ":v", s("x"))));
        assertTrue(error(() -> cond("n = n")).contains("The first operand must be distinct"));
        assertTrue(error(() -> cond("n BETWEEN :a AND :b", ":a", n("5"), ":b", s("x"))).contains("same data type for lower and upper bounds"));
        assertTrue(error(() -> cond("n BETWEEN :a AND :b", ":a", n("9"), ":b", n("5"))).contains("upper bound to be greater than or equal to lower bound"));
        assertTrue(error(() -> cond("n < :v", ":v", AttributeValue.ofBool(true))).contains("Incorrect operand type"));
        assertEquals("Invalid ConditionExpression: The expression can not be empty;", error(() -> cond("  ")));
    }

    @Test
    void unusedNamesAndValuesAreReported() {
        ExpressionContext c = ctx(":v", s("x"), ":w", s("y"), "#a", "s");
        ExprParser.parseCondition("s = :v", "ConditionExpression", c);
        String m = error(() -> c.checkUnused("ConditionExpression"));
        assertEquals("Value provided in ExpressionAttributeNames unused in expressions: keys: {#a}", m);
        ExpressionContext d = ctx(":v", s("x"), ":w", s("y"));
        ExprParser.parseCondition("s = :v", "ConditionExpression", d);
        assertEquals("Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}", error(() -> d.checkUnused("x")));
        ExpressionContext e = ctx(":v", s("x"));
        assertEquals("ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null",
                error(() -> e.checkUnused("ConditionExpression")));
    }

    private static Map<String, AttributeValue> update(String expr, Object... kv) {
        Expr.UpdatePlan plan = ExprParser.parseUpdate(expr, ctx(kv));
        return ExprEval.apply(plan, item()).item();
    }

    @Test
    void updateSetArithmeticListsAndIfNotExists() {
        assertEquals("15", update("SET n = n + :v", ":v", n("5")).get("n").scalar);
        assertEquals("9.9", update("SET n = n - :v", ":v", n("0.1")).get("n").scalar);
        assertEquals("0.3", update("SET q = :a + :b", ":a", n("0.1"), ":b", n("0.2")).get("q").scalar, "exact decimal arithmetic");
        assertEquals("1", update("SET q = if_not_exists(q, :z) + :o", ":z", n("0"), ":o", n("1")).get("q").scalar);
        assertEquals("10", update("SET n = if_not_exists(n, :z)", ":z", n("99")).get("n").scalar);
        Map<String, AttributeValue> r = update("SET l = list_append(l, :x), m.b.c = :v", ":x", AttributeValue.ofL(List.of(s("z"))), ":v", n("4"));
        assertEquals(4, r.get("l").list.size());
        assertEquals("z", r.get("l").list.get(3).scalar);
        assertEquals("4", r.get("m").map.get("b").map.get("c").scalar);
        assertTrue(error(() -> update("SET l = list_append(l, :x), l[0] = :y", ":x", AttributeValue.ofL(List.of(s("z"))), ":y", s("Y"))).contains("overlap"));
        assertEquals("Y", update("SET l[0] = :y", ":y", s("Y")).get("l").list.get(0).scalar);
        Map<String, AttributeValue> swap = update("SET s = n, n = s");
        assertEquals("10", swap.get("s").scalar, "SET evaluates against the original item, so this swaps");
        assertEquals("hello", swap.get("n").scalar);
        assertEquals(4, update("SET l[10] = :v", ":v", s("end")).get("l").list.size(), "an index past the end appends");
    }

    @Test
    void updateRemoveAddDelete() {
        assertFalse(update("REMOVE s").containsKey("s"));
        assertFalse(update("REMOVE m.b.c").get("m").map.get("b").map.containsKey("c"));
        Map<String, AttributeValue> r = update("REMOVE l[0], l[1]");
        assertEquals(1, r.get("l").list.size(), "removing several list indexes uses the original positions");
        assertEquals(AttributeValue.Type.M, r.get("l").list.get(0).type, "only the map element (index 2) is left");
        assertEquals("15", update("ADD n :v", ":v", n("5")).get("n").scalar);
        assertEquals("5", update("ADD q :v", ":v", n("5")).get("q").scalar, "ADD creates a missing number");
        assertEquals(new LinkedHashSet<>(List.of("a", "b", "c")), update("ADD ss :v", ":v", AttributeValue.ofSet(AttributeValue.Type.SS, new LinkedHashSet<>(List.of("c")))).get("ss").stringSet);
        assertEquals(new LinkedHashSet<>(List.of("b")), update("DELETE ss :v", ":v", AttributeValue.ofSet(AttributeValue.Type.SS, new LinkedHashSet<>(List.of("a")))).get("ss").stringSet);
        assertFalse(update("DELETE ss :v", ":v", AttributeValue.ofSet(AttributeValue.Type.SS, new LinkedHashSet<>(List.of("a", "b")))).containsKey("ss"), "an emptied set disappears");
        assertTrue(error(() -> update("ADD s :v", ":v", s("x"))).contains("ALLOWED_FOR_ADD_OPERAND"));
        assertTrue(error(() -> update("ADD ss :v", ":v", AttributeValue.ofSet(AttributeValue.Type.NS, new LinkedHashSet<>(List.of("1"))))).contains("incorrect data type"));
        assertEquals("The document path provided in the update expression is invalid for update", error(() -> update("SET nope.x = :v", ":v", s("x"))));
        assertEquals("The document path provided in the update expression is invalid for update", error(() -> update("REMOVE nope.x")));
        assertEquals("The provided expression refers to an attribute that does not exist in the item", error(() -> update("SET q = zz")));
        assertTrue(error(() -> update("SET n = s + :v", ":v", n("1"))).contains("Incorrect operand type for operator or function; operator or function: +, operand type: S"));
    }

    @Test
    void updateExpressionStructureErrors() {
        assertTrue(error(() -> update("SET s = :v SET n = :v", ":v", s("x"))).contains("The \"SET\" section can only be used once"));
        assertTrue(error(() -> update("SET m = :v, m.a = :v", ":v", s("x"))).contains("Two document paths overlap"));
        assertTrue(error(() -> update("SET m.a = :v, m[0] = :v", ":v", s("x"))).contains("Two document paths conflict"));
        assertTrue(error(() -> update("SET s :v", ":v", s("x"))).contains("Syntax error"));
        assertTrue(error(() -> update("", ":v", s("x"))).contains("can not be empty"));
        assertTrue(error(() -> update("FOO s = :v", ":v", s("x"))).contains("Syntax error"));
        Expr.UpdatePlan p = ExprParser.parseUpdate("SET s = :v ADD n :d REMOVE t, nul DELETE ss :x", ctx(":v", s("a"), ":d", n("1"),
                ":x", AttributeValue.ofSet(AttributeValue.Type.SS, new LinkedHashSet<>(List.of("a")))));
        assertEquals(5, p.actions().size());
    }

    @Test
    void projectionKeepsNestedStructureAndRejectsOverlaps() {
        ExpressionContext c = ctx("#x", "s");
        List<Expr.Path> paths = ExprParser.parseProjection("#x, m.b.c, l[2].k, nothere", c);
        Map<String, AttributeValue> out = ExprEval.project(item(), paths);
        assertEquals("hello", out.get("s").scalar);
        assertEquals("3", out.get("m").map.get("b").map.get("c").scalar);
        assertEquals("v", out.get("l").list.get(0).map.get("k").scalar);
        assertFalse(out.containsKey("nothere"));
        assertTrue(error(() -> ExprParser.parseProjection("m, m.a", ctx())).contains("Two document paths overlap"));
        assertTrue(error(() -> ExprParser.parseProjection("name", ctx())).contains("reserved keyword"));
        assertNull(ExprEval.project(null, null));
    }

    @Test
    void updatedValuesReturnWholeTopLevelAttributes() {
        Expr.UpdatePlan plan = ExprParser.parseUpdate("SET m.a = :v", ctx(":v", s("new")));
        ExprEval.UpdateResult r = ExprEval.apply(plan, item());
        Map<String, AttributeValue> neu = ExprEval.valuesAt(r.item(), r.touched());
        assertEquals(List.of("m"), List.copyOf(neu.keySet()));
        assertEquals(2, neu.get("m").map.size());
        assertEquals("1", ExprEval.valuesAt(item(), r.touched()).get("m").map.get("a").scalar);
    }
}
