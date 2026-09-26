package com.sayonora.warp.bigtablewire;

import com.sayonora.warp.bigtablewire.v2.RowFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/**
 * Bigtable RowFilter evaluation: a filter is compiled once per request into a tree of {@link Node}s, each mapping the cells of a
 * row (standard order) to the cells that survive; a row whose result is empty is not returned. Chain feeds each output into the
 * next, Interleave merges the outputs of its children back into standard order (duplicates kept, children order for ties),
 * Condition picks the true or false branch by whether the predicate keeps any cell (a missing branch keeps nothing).
 */
final class BtFilter {

    interface Node {
        List<BtCell> apply(byte[] rowKey, List<BtCell> in);
    }

    static final Node PASS = (k, in) -> in;
    private static final java.util.regex.Pattern LABEL = java.util.regex.Pattern.compile("[a-z0-9\\-]+");

    private BtFilter() {
    }

    static Node compile(RowFilter f) {
        if (f == null) {
            return PASS;
        }
        switch (f.getFilterCase()) {
            case FILTER_NOT_SET:
                return PASS;
            case CHAIN: {
                List<RowFilter> l = f.getChain().getFiltersList();
                if (l.size() < 2) {
                    throw BtException.invalid("Chain must contain at least two RowFilters");
                }
                List<Node> nodes = l.stream().map(BtFilter::compile).toList();
                return (k, in) -> {
                    List<BtCell> cur = in;
                    for (Node n : nodes) {
                        cur = n.apply(k, cur);
                        if (cur.isEmpty()) {
                            break;
                        }
                    }
                    return cur;
                };
            }
            case INTERLEAVE: {
                List<RowFilter> l = f.getInterleave().getFiltersList();
                if (l.size() < 2) {
                    throw BtException.invalid("Interleave must contain at least two RowFilters");
                }
                List<Node> nodes = l.stream().map(BtFilter::compile).toList();
                return (k, in) -> {
                    List<BtCell> out = new ArrayList<>();
                    for (Node n : nodes) {
                        out.addAll(n.apply(k, in));
                    }
                    out.sort(BtCell.ORDER);
                    return out;
                };
            }
            case CONDITION: {
                RowFilter.Condition c = f.getCondition();
                Node pred = c.hasPredicateFilter() ? compile(c.getPredicateFilter()) : PASS;
                Node yes = c.hasTrueFilter() ? compile(c.getTrueFilter()) : null;
                Node no = c.hasFalseFilter() ? compile(c.getFalseFilter()) : null;
                return (k, in) -> {
                    Node b = pred.apply(k, in).isEmpty() ? no : yes;
                    return b == null ? List.of() : b.apply(k, in);
                };
            }
            case SINK:
                return PASS;
            case PASS_ALL_FILTER:
                if (!f.getPassAllFilter()) {
                    throw BtException.invalid("pass_all_filter must be true if set");
                }
                return PASS;
            case BLOCK_ALL_FILTER:
                if (!f.getBlockAllFilter()) {
                    throw BtException.invalid("block_all_filter must be true if set");
                }
                return (k, in) -> List.of();
            case ROW_KEY_REGEX_FILTER: {
                if (f.getRowKeyRegexFilter().isEmpty()) {
                    throw BtException.invalid("Error in field 'row_key_regex_filter' : argument must not be empty");
                }
                Predicate<byte[]> p = BtRegex.compile("row_key_regex_filter", f.getRowKeyRegexFilter().toByteArray());
                return (k, in) -> p.test(k) ? in : List.of();
            }
            case ROW_SAMPLE_FILTER: {
                double p = f.getRowSampleFilter();
                if (!(p > 0.0 && p < 1.0)) {
                    throw BtException.invalid("row_sample_filter argument must be between 0.0 and 1.0");
                }
                return (k, in) -> ThreadLocalRandom.current().nextDouble() < p ? in : List.of();
            }
            case FAMILY_NAME_REGEX_FILTER: {
                Predicate<byte[]> p = BtRegex.compile("family_name_regex_filter", f.getFamilyNameRegexFilterBytes().toByteArray());
                return (k, in) -> filter(in, c -> p.test(c.family().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            }
            case COLUMN_QUALIFIER_REGEX_FILTER: {
                Predicate<byte[]> p = BtRegex.compile("column_qualifier_regex_filter", f.getColumnQualifierRegexFilter().toByteArray());
                return (k, in) -> filter(in, c -> p.test(c.qual()));
            }
            case VALUE_REGEX_FILTER: {
                Predicate<byte[]> p = BtRegex.compile("value_regex_filter", f.getValueRegexFilter().toByteArray());
                return (k, in) -> filter(in, c -> p.test(c.value()));
            }
            case COLUMN_RANGE_FILTER: {
                var r = f.getColumnRangeFilter();
                String fam = r.getFamilyName();
                byte[] lo = r.hasStartQualifierClosed() ? r.getStartQualifierClosed().toByteArray() : r.hasStartQualifierOpen() ? r.getStartQualifierOpen().toByteArray() : null;
                boolean loClosed = r.hasStartQualifierClosed();
                byte[] hi = r.hasEndQualifierClosed() ? r.getEndQualifierClosed().toByteArray() : r.hasEndQualifierOpen() ? r.getEndQualifierOpen().toByteArray() : null;
                boolean hiClosed = r.hasEndQualifierClosed();
                return (k, in) -> filter(in, c -> c.family().equals(fam) && inRange(c.qual(), lo, loClosed, hi, hiClosed));
            }
            case VALUE_RANGE_FILTER: {
                var r = f.getValueRangeFilter();
                byte[] lo = r.hasStartValueClosed() ? r.getStartValueClosed().toByteArray() : r.hasStartValueOpen() ? r.getStartValueOpen().toByteArray() : null;
                boolean loClosed = r.hasStartValueClosed();
                byte[] hi = r.hasEndValueClosed() ? r.getEndValueClosed().toByteArray() : r.hasEndValueOpen() ? r.getEndValueOpen().toByteArray() : null;
                boolean hiClosed = r.hasEndValueClosed();
                return (k, in) -> filter(in, c -> inRange(c.value(), lo, loClosed, hi, hiClosed));
            }
            case TIMESTAMP_RANGE_FILTER: {
                var r = f.getTimestampRangeFilter();
                long lo = r.getStartTimestampMicros();
                long hi = r.getEndTimestampMicros();
                return (k, in) -> filter(in, c -> c.ts() >= lo && (hi == 0 || c.ts() < hi));
            }
            case CELLS_PER_ROW_OFFSET_FILTER: {
                int n = f.getCellsPerRowOffsetFilter();
                if (n < 0) {
                    throw BtException.invalid("Error in field 'cells_per_row_offset_filter' : argument must be >= 0");
                }
                return (k, in) -> n >= in.size() ? List.of() : new ArrayList<>(in.subList(n, in.size()));
            }
            case CELLS_PER_ROW_LIMIT_FILTER: {
                int n = f.getCellsPerRowLimitFilter();
                if (n <= 0) {
                    throw BtException.invalid("Error in field 'cells_per_row_limit_filter' : argument must be > 0");
                }
                return (k, in) -> in.size() <= n ? in : new ArrayList<>(in.subList(0, n));
            }
            case CELLS_PER_COLUMN_LIMIT_FILTER: {
                int n = f.getCellsPerColumnLimitFilter();
                if (n <= 0) {
                    throw BtException.invalid("Error in field 'cells_per_column_limit_filter' : argument must be > 0");
                }
                return (k, in) -> {
                    List<BtCell> out = new ArrayList<>();
                    BtCell first = null;
                    int seen = 0;
                    for (BtCell c : in) {
                        if (!c.sameColumn(first)) {
                            first = c;
                            seen = 0;
                        }
                        if (++seen <= n) {
                            out.add(c);
                        }
                    }
                    return out;
                };
            }
            case STRIP_VALUE_TRANSFORMER:
                return (k, in) -> {
                    List<BtCell> out = new ArrayList<>(in.size());
                    for (BtCell c : in) {
                        out.add(c.withValue(new byte[0]));
                    }
                    return out;
                };
            case APPLY_LABEL_TRANSFORMER: {
                if (!LABEL.matcher(f.getApplyLabelTransformer()).find()) {
                    throw BtException.invalid("apply_label_transformer must match RE2([a-z0-9\\-]+), but found " + f.getApplyLabelTransformer());
                }
                List<String> label = List.of(f.getApplyLabelTransformer());
                return (k, in) -> {
                    List<BtCell> out = new ArrayList<>(in.size());
                    for (BtCell c : in) {
                        out.add(c.withLabels(label));
                    }
                    return out;
                };
            }
            default:
                throw BtException.unimplemented("Unsupported filter: " + f.getFilterCase());
        }
    }

    private static List<BtCell> filter(List<BtCell> in, Predicate<BtCell> keep) {
        List<BtCell> out = new ArrayList<>();
        for (BtCell c : in) {
            if (keep.test(c)) {
                out.add(c);
            }
        }
        return out;
    }

    private static boolean inRange(byte[] v, byte[] lo, boolean loClosed, byte[] hi, boolean hiClosed) {
        if (lo != null) {
            int c = Arrays.compareUnsigned(v, lo);
            if (c < 0 || (c == 0 && !loClosed)) {
                return false;
            }
        }
        if (hi != null) {
            int c = Arrays.compareUnsigned(v, hi);
            return c < 0 || (c == 0 && hiClosed);
        }
        return true;
    }

    /** Applies {@code node} to a whole row; null when nothing survives. */
    static BtRow apply(Node node, BtRow row) {
        List<BtCell> out = node.apply(row.key(), row.cells());
        return out.isEmpty() ? null : new BtRow(row.key(), out);
    }

}
