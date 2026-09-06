package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * Pure, no-DB, no-network unit coverage for {@link RexRowEvaluator} -- builds real {@link RexNode}
 * trees via a real {@link RexBuilder} (the same class Calcite's own planner uses internally), the
 * deterministic way to test this rather than depending on a live query happening to leave a filter
 * un-pushed-down (which Calcite's own cost-based optimizer decides, not something a test can force
 * reliably from SQL text alone).
 */
class RexRowEvaluatorTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    private static final RexBuilder BUILDER = new RexBuilder(TYPE_FACTORY);

    private static RexInputRef intRef(int ordinal) {
        return BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), ordinal);
    }

    private static RexNode literal(int value) {
        return BUILDER.makeExactLiteral(java.math.BigDecimal.valueOf(value));
    }

    @Test
    void compareOrdinalAgainstALiteral() {
        // amount > 100, where "amount" is column ordinal 1
        RexNode condition = BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN, intRef(1), literal(100));
        RexRowEvaluator.RowPredicate predicate = RexRowEvaluator.compile(condition);

        assertTrue(predicate.test(List.of(1, 150)));
        assertFalse(predicate.test(List.of(1, 50)));
        assertFalse(predicate.test(List.of(1, 100)), "strictly greater-than must exclude the boundary value");
    }

    @Test
    void andOfTwoComparisons() {
        // id = 1 AND amount >= 50
        RexNode condition = BUILDER.makeCall(SqlStdOperatorTable.AND,
                BUILDER.makeCall(SqlStdOperatorTable.EQUALS, intRef(0), literal(1)),
                BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, intRef(1), literal(50)));
        RexRowEvaluator.RowPredicate predicate = RexRowEvaluator.compile(condition);

        assertTrue(predicate.test(List.of(1, 50)));
        assertFalse(predicate.test(List.of(1, 49)));
        assertFalse(predicate.test(List.of(2, 100)));
    }

    @Test
    void orOfTwoComparisons() {
        RexNode condition = BUILDER.makeCall(SqlStdOperatorTable.OR,
                BUILDER.makeCall(SqlStdOperatorTable.EQUALS, intRef(0), literal(1)),
                BUILDER.makeCall(SqlStdOperatorTable.EQUALS, intRef(0), literal(2)));
        RexRowEvaluator.RowPredicate predicate = RexRowEvaluator.compile(condition);

        assertTrue(predicate.test(List.of(1)));
        assertTrue(predicate.test(List.of(2)));
        assertFalse(predicate.test(List.of(3)));
    }

    @Test
    void notNegatesItsOperand() {
        RexNode condition = BUILDER.makeCall(SqlStdOperatorTable.NOT,
                BUILDER.makeCall(SqlStdOperatorTable.EQUALS, intRef(0), literal(1)));
        RexRowEvaluator.RowPredicate predicate = RexRowEvaluator.compile(condition);

        assertFalse(predicate.test(List.of(1)));
        assertTrue(predicate.test(List.of(2)));
    }

    @Test
    void aComparisonAgainstANullValueIsNeverAMatch() {
        // SQL semantics: any comparison against NULL is UNKNOWN, filtered out by WHERE either way.
        RexNode condition = BUILDER.makeCall(SqlStdOperatorTable.EQUALS, intRef(0), literal(1));
        RexRowEvaluator.RowPredicate predicate = RexRowEvaluator.compile(condition);

        List<Object> rowWithNull = new java.util.ArrayList<>();
        rowWithNull.add(null);
        assertFalse(predicate.test(rowWithNull));
    }

    @Test
    void isNullAndIsNotNull() {
        RexNode isNull = BUILDER.makeCall(SqlStdOperatorTable.IS_NULL, intRef(0));
        RexNode isNotNull = BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, intRef(0));
        RexRowEvaluator.RowPredicate isNullPredicate = RexRowEvaluator.compile(isNull);
        RexRowEvaluator.RowPredicate isNotNullPredicate = RexRowEvaluator.compile(isNotNull);

        List<Object> nullRow = new java.util.ArrayList<>();
        nullRow.add(null);
        assertTrue(isNullPredicate.test(nullRow));
        assertFalse(isNotNullPredicate.test(nullRow));
        assertFalse(isNullPredicate.test(List.of(5)));
        assertTrue(isNotNullPredicate.test(List.of(5)));
    }

    @Test
    void anUnsupportedFunctionCallReturnsNullRatherThanGuessing() {
        // UPPER(name) = 'ALICE' -- a function call, not a bare column ref or literal -- must be
        // refused outright, not silently misevaluated.
        RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 50);
        RexInputRef nameRef = BUILDER.makeInputRef(varchar, 0);
        RexNode upperCall = BUILDER.makeCall(SqlStdOperatorTable.UPPER, nameRef);
        RexNode condition = BUILDER.makeCall(SqlStdOperatorTable.EQUALS, upperCall, BUILDER.makeLiteral("ALICE"));

        assertNull(RexRowEvaluator.compile(condition));
    }
}
