package com.sayonora.wire.core;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

/**
 * Compiles a Calcite {@link RexNode} boolean condition into a {@link RowPredicate} evaluable
 * directly against a plain {@code List<Object>} row -- what {@link ParallelJoinPlanner} needs to
 * support a residual {@code Filter} Calcite left un-pushed-down directly above a leaf scan (a real
 * gap disclosed since Phase 0: unlike a plain column-selection {@code Project}, which is just an
 * ordinal remap, an arbitrary filter predicate needs actual evaluation).
 *
 * <p>Deliberately narrow, in the same spirit as {@link ParallelJoinPlanner}'s own eligibility
 * checks and {@link SemiJoinPushdown}: supports {@code AND}/{@code OR}/{@code NOT}, {@code IS
 * [NOT] NULL}, and the six comparison operators over a bare column reference and/or a literal --
 * anything else ({@code RexCall} to an unsupported function, a subquery, a cast, a computed
 * expression on either side) returns {@code null} from {@link #compile}, and the caller falls back
 * to today's unchanged sequential path, exactly like every other real, disclosed narrowing in this
 * engine. Never guesses; a condition this can't confidently evaluate is a condition it refuses to
 * evaluate at all.
 */
final class RexRowEvaluator {

    private RexRowEvaluator() {
    }

    interface RowPredicate {
        boolean test(List<Object> row);
    }

    /** A resolved side of a comparison: either a bare column ordinal (evaluated per row) or a
     * constant literal value (the same value every time). */
    private interface ValueExtractor {
        Object get(List<Object> row);
    }

    static RowPredicate compile(RexNode condition) {
        if (!(condition instanceof RexCall call)) {
            return null;
        }
        SqlKind kind = call.getKind();
        List<RexNode> operands = call.getOperands();
        switch (kind) {
            case AND: {
                List<RowPredicate> predicates = compileAll(operands);
                return predicates == null ? null : row -> allMatch(predicates, row);
            }
            case OR: {
                List<RowPredicate> predicates = compileAll(operands);
                return predicates == null ? null : row -> anyMatch(predicates, row);
            }
            case NOT: {
                if (operands.size() != 1) {
                    return null;
                }
                RowPredicate inner = compile(operands.get(0));
                return inner == null ? null : row -> !inner.test(row);
            }
            case IS_NULL:
            case IS_NOT_NULL: {
                if (operands.size() != 1 || !(operands.get(0) instanceof RexInputRef ref)) {
                    return null;
                }
                int ordinal = ref.getIndex();
                boolean wantNull = kind == SqlKind.IS_NULL;
                return row -> (ordinal < row.size() && row.get(ordinal) == null) == wantNull;
            }
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL: {
                if (operands.size() != 2) {
                    return null;
                }
                ValueExtractor left = extractorFor(operands.get(0));
                ValueExtractor right = extractorFor(operands.get(1));
                if (left == null || right == null) {
                    return null;
                }
                return row -> evaluateComparison(kind, left.get(row), right.get(row));
            }
            default:
                return null;
        }
    }

    private static List<RowPredicate> compileAll(List<RexNode> operands) {
        List<RowPredicate> predicates = new ArrayList<>(operands.size());
        for (RexNode operand : operands) {
            RowPredicate predicate = compile(operand);
            if (predicate == null) {
                return null;
            }
            predicates.add(predicate);
        }
        return predicates;
    }

    private static boolean allMatch(List<RowPredicate> predicates, List<Object> row) {
        for (RowPredicate predicate : predicates) {
            if (!predicate.test(row)) {
                return false;
            }
        }
        return true;
    }

    private static boolean anyMatch(List<RowPredicate> predicates, List<Object> row) {
        for (RowPredicate predicate : predicates) {
            if (predicate.test(row)) {
                return true;
            }
        }
        return false;
    }

    /** SQL comparison semantics: a comparison against a {@code NULL} operand is unknown, which a
     * {@code WHERE} clause treats as "filtered out" -- never a match, on either side, for any of
     * these six operators (matching how {@link ParallelJoinExecutor}'s own join-key handling
     * already treats a {@code null} key as never matching anything). */
    private static boolean evaluateComparison(SqlKind kind, Object left, Object right) {
        if (left == null || right == null) {
            return false;
        }
        int cmp = compareValues(left, right);
        return switch (kind) {
            case EQUALS -> cmp == 0;
            case NOT_EQUALS -> cmp != 0;
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUAL -> cmp >= 0;
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUAL -> cmp <= 0;
            default -> false;
        };
    }

    /** Numeric comparison when both sides parse as a number (covers the common case of an integer/
     * decimal column compared to a numeric literal, regardless of which concrete Java type the JDBC
     * driver or Calcite's own literal representation happens to use); otherwise falls back to a
     * plain string comparison -- correct for equality on any type, and a reasonable, disclosed
     * approximation for ordering comparisons on non-numeric values (matching how the rest of this
     * engine already treats values as text once they cross a wire boundary). */
    private static int compareValues(Object left, Object right) {
        BigDecimal leftNumber = asBigDecimalOrNull(left);
        BigDecimal rightNumber = asBigDecimalOrNull(right);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    private static BigDecimal asBigDecimalOrNull(Object value) {
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        if (value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        try {
            return new BigDecimal(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ValueExtractor extractorFor(RexNode node) {
        if (node instanceof RexInputRef ref) {
            int ordinal = ref.getIndex();
            return row -> ordinal < row.size() ? row.get(ordinal) : null;
        }
        if (node instanceof RexLiteral literal) {
            Object value = literalValue(literal);
            return row -> value;
        }
        return null;
    }

    /** Normalizes a {@link RexLiteral}'s own internal representation (Calcite wraps a string
     * literal in {@link NlsString}, not a bare {@link String}) into a plain Java value usable
     * directly against a JDBC-sourced row value. */
    private static Object literalValue(RexLiteral literal) {
        Object raw = literal.getValue();
        if (raw instanceof NlsString nlsString) {
            return nlsString.getValue();
        }
        return raw;
    }
}
