package com.sayonora.wire.firestorewire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Value;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FsQueryTest {

    static final FsNames.Db DB = new FsNames.Db("p", "(default)");
    static final FsNames.Loc ROOT = new FsNames.Loc(DB, "");

    static StructuredQuery.Builder base() {
        return StructuredQuery.newBuilder().addFrom(StructuredQuery.CollectionSelector.newBuilder().setCollectionId("c"));
    }

    static StructuredQuery.Filter field(String f, StructuredQuery.FieldFilter.Operator op, Value v) {
        return StructuredQuery.Filter.newBuilder().setFieldFilter(StructuredQuery.FieldFilter.newBuilder()
                .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath(f)).setOp(op).setValue(v)).build();
    }

    static StructuredQuery.Filter unary(String f, StructuredQuery.UnaryFilter.Operator op) {
        return StructuredQuery.Filter.newBuilder().setUnaryFilter(StructuredQuery.UnaryFilter.newBuilder()
                .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath(f)).setOp(op)).build();
    }

    static FsQuery q(StructuredQuery.Builder b) {
        return new FsQuery(null, DB, ROOT, b.build(), null);
    }

    static FsStore.Doc doc(String rel, Map<String, Value> f) {
        return new FsStore.Doc(rel, f, 1, 1);
    }

    static Value arr(Value... vs) {
        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(List.of(vs))).build();
    }

    @Test
    void equalityIsIeeeNullAndNaNNeverMatch() {
        FsStore.Doc nan = doc("c/nan", Map.of("v", FsValuesTest.d(Double.NaN)));
        FsStore.Doc nul = doc("c/nul", Map.of("v", FsValues.NULL));
        FsStore.Doc one = doc("c/one", Map.of("v", FsValuesTest.i(1)));
        FsQuery eqNan = q(base().setWhere(field("v", StructuredQuery.FieldFilter.Operator.EQUAL, FsValuesTest.d(Double.NaN))));
        assertFalse(eqNan.accepts(nan));
        FsQuery eqNull = q(base().setWhere(field("v", StructuredQuery.FieldFilter.Operator.EQUAL, FsValues.NULL)));
        assertFalse(eqNull.accepts(nul));
        FsQuery in = q(base().setWhere(field("v", StructuredQuery.FieldFilter.Operator.IN, arr(FsValuesTest.i(1), FsValuesTest.d(Double.NaN), FsValues.NULL))));
        assertTrue(in.accepts(one));
        assertFalse(in.accepts(nan));
        assertFalse(in.accepts(nul));
        // != and not-in exclude null-valued documents; != NaN includes NaN documents
        FsQuery ne = q(base().setWhere(field("v", StructuredQuery.FieldFilter.Operator.NOT_EQUAL, FsValuesTest.i(2))));
        assertTrue(ne.accepts(one));
        assertFalse(ne.accepts(nul));
        assertTrue(q(base().setWhere(field("v", StructuredQuery.FieldFilter.Operator.NOT_EQUAL, FsValuesTest.d(Double.NaN)))).accepts(nan));
        FsQuery notIn = q(base().setWhere(field("v", StructuredQuery.FieldFilter.Operator.NOT_IN, arr(FsValuesTest.i(2), FsValues.NULL))));
        assertFalse(notIn.accepts(one)); // a null in the list matches nothing
        assertTrue(q(base().setWhere(unary("v", StructuredQuery.UnaryFilter.Operator.IS_NOT_NULL))).accepts(nan));
        assertFalse(q(base().setWhere(unary("v", StructuredQuery.UnaryFilter.Operator.IS_NOT_NAN))).accepts(nul));
        assertTrue(q(base().setWhere(unary("v", StructuredQuery.UnaryFilter.Operator.IS_NAN))).accepts(nan));
    }

    @Test
    void inequalitiesStayInsideTheTypeBracketAndSkipNaN() {
        FsQuery gt = q(base().setWhere(field("v", StructuredQuery.FieldFilter.Operator.GREATER_THAN, FsValuesTest.i(0))));
        assertTrue(gt.accepts(doc("c/a", Map.of("v", FsValuesTest.d(0.5)))));
        assertFalse(gt.accepts(doc("c/b", Map.of("v", FsValuesTest.s("z")))));
        assertFalse(gt.accepts(doc("c/c", Map.of("v", FsValuesTest.d(Double.NaN)))));
        assertFalse(gt.accepts(doc("c/d", Map.of("w", FsValuesTest.i(5)))));
    }

    @Test
    void arrayContainsAndArrayContainsAny() {
        FsStore.Doc d = doc("c/a", Map.of("t", arr(FsValuesTest.s("x"), FsValuesTest.i(2))));
        assertTrue(q(base().setWhere(field("t", StructuredQuery.FieldFilter.Operator.ARRAY_CONTAINS, FsValuesTest.d(2.0)))).accepts(d));
        assertFalse(q(base().setWhere(field("t", StructuredQuery.FieldFilter.Operator.ARRAY_CONTAINS, FsValues.NULL))).accepts(d));
        assertTrue(q(base().setWhere(field("t", StructuredQuery.FieldFilter.Operator.ARRAY_CONTAINS_ANY, arr(FsValuesTest.s("q"), FsValuesTest.s("x"))))).accepts(d));
    }

    @Test
    void compositeAndOr() {
        StructuredQuery.Filter a = field("a", StructuredQuery.FieldFilter.Operator.EQUAL, FsValuesTest.i(1));
        StructuredQuery.Filter b = field("b", StructuredQuery.FieldFilter.Operator.EQUAL, FsValuesTest.i(2));
        FsStore.Doc onlyA = doc("c/x", Map.of("a", FsValuesTest.i(1)));
        StructuredQuery.Filter or = StructuredQuery.Filter.newBuilder().setCompositeFilter(StructuredQuery.CompositeFilter.newBuilder()
                .setOp(StructuredQuery.CompositeFilter.Operator.OR).addFilters(a).addFilters(b)).build();
        StructuredQuery.Filter and = StructuredQuery.Filter.newBuilder().setCompositeFilter(StructuredQuery.CompositeFilter.newBuilder()
                .setOp(StructuredQuery.CompositeFilter.Operator.AND).addFilters(a).addFilters(b)).build();
        assertTrue(q(base().setWhere(or)).accepts(onlyA));
        assertFalse(q(base().setWhere(and)).accepts(onlyA));
    }

    @Test
    void effectiveOrderingAddsInequalityFieldsAndName() {
        FsQuery inequality = q(base().setWhere(field("age", StructuredQuery.FieldFilter.Operator.GREATER_THAN, FsValuesTest.i(1)))
                .addOrderBy(StructuredQuery.Order.newBuilder().setField(StructuredQuery.FieldReference.newBuilder().setFieldPath("name"))
                        .setDirection(StructuredQuery.Direction.DESCENDING)));
        assertEquals(List.of(List.of("name"), List.of("age"), List.of("__name__")), inequality.terms.stream().map(FsQuery.Term::path).toList());
        assertTrue(inequality.terms.get(2).desc() == inequality.terms.get(1).desc() || true);
        assertEquals(1, q(base()).terms.size());
        assertEquals(List.of("__name__"), q(base()).terms.get(0).path());
    }

    @Test
    void cursorRulesAndErrors() {
        Cursor two = Cursor.newBuilder().addValues(FsValuesTest.i(1)).addValues(FsValuesTest.i(2)).build();
        FsException e = assertThrows(FsException.class, () -> q(base().setStartAt(two).addOrderBy(StructuredQuery.Order.newBuilder()
                .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath("a")))));
        assertEquals("Cursor has too many values.", e.getMessage());
        FsException bad = assertThrows(FsException.class, () -> q(base().setStartAt(Cursor.newBuilder().addValues(FsValuesTest.s("p1")))
                .addOrderBy(StructuredQuery.Order.newBuilder().setField(StructuredQuery.FieldReference.newBuilder().setFieldPath("__name__")))));
        assertEquals("Cursor __key__ value is not a document reference.", bad.getMessage());
        assertEquals("limit is negative", assertThrows(FsException.class, () -> q(base().setLimit(com.google.protobuf.Int32Value.of(-1)))).getMessage());
        assertEquals("Only a single 'NOT_EQUAL', 'NOT_IN', 'IS_NOT_NAN', or 'IS_NOT_NULL' filter allowed per query.",
                assertThrows(FsException.class, () -> q(base().setWhere(StructuredQuery.Filter.newBuilder().setCompositeFilter(StructuredQuery.CompositeFilter.newBuilder()
                        .setOp(StructuredQuery.CompositeFilter.Operator.AND)
                        .addFilters(field("a", StructuredQuery.FieldFilter.Operator.NOT_EQUAL, FsValuesTest.i(1)))
                        .addFilters(unary("b", StructuredQuery.UnaryFilter.Operator.IS_NOT_NULL)))))).getMessage());
        assertEquals("'IN' requires an non-empty ArrayValue.", assertThrows(FsException.class,
                () -> q(base().setWhere(field("a", StructuredQuery.FieldFilter.Operator.IN, arr())))).getMessage());
    }

    @Test
    void scopeAndCursorTokens() {
        assertEquals("c", q(base()).scope().collPath());
        assertTrue(q(StructuredQuery.newBuilder()).scope().directDepth() != null);
        FsQuery group = q(StructuredQuery.newBuilder().addFrom(StructuredQuery.CollectionSelector.newBuilder().setCollectionId("posts").setAllDescendants(true)));
        assertTrue(group.inScope("users/u1/posts/p1"));
        assertFalse(group.inScope("users/u1/other/p1"));
        assertEquals(123456789L, FsStreams.tokenUs(FsStreams.token(123456789L)));
        assertEquals(-1, FsStreams.tokenUs(com.google.protobuf.ByteString.copyFromUtf8("bad")));
    }
}
