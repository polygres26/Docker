package com.sayonora.warp.bigtablewire;

import com.sayonora.warp.bigtablewire.admin.v2.Table;
import com.sayonora.warp.bigtablewire.v2.Mutation;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Validation (all of a request's mutations before any is applied) and application of Bigtable mutations to one row. */
final class BtMutations {

    sealed interface Op permits SetCell, DelCol, DelFam, DelRow {
    }

    record SetCell(String family, byte[] qual, long ts, byte[] value) implements Op {
    }

    record DelCol(String family, byte[] qual, long start, long end) implements Op {
    }

    record DelFam(String family) implements Op {
    }

    record DelRow() implements Op {
    }

    private BtMutations() {
    }

    /** Timestamp granularity of a table: milliseconds; -1 asks for the server time. */
    static long checkTimestamp(long ts, long nowMicros) {
        if (ts == -1) {
            return nowMicros;
        }
        if (ts < 0 || ts % 1000 != 0) {
            throw BtException.unknown("invalid timestamp " + ts);
        }
        return ts;
    }

    static List<Op> compile(Table table, List<Mutation> muts, long nowMicros) {
        List<Op> ops = new ArrayList<>(muts.size());
        for (Mutation m : muts) {
            switch (m.getMutationCase()) {
                case SET_CELL: {
                    var s = m.getSetCell();
                    requireFamily(table, s.getFamilyName());
                    long ts = checkTimestamp(s.getTimestampMicros(), nowMicros);
                    ops.add(new SetCell(s.getFamilyName(), s.getColumnQualifier().toByteArray(), ts, s.getValue().toByteArray()));
                    break;
                }
                case DELETE_FROM_COLUMN: {
                    var d = m.getDeleteFromColumn();
                    requireFamily(table, d.getFamilyName());
                    long start = d.getTimeRange().getStartTimestampMicros();
                    long end = d.getTimeRange().getEndTimestampMicros();
                    if (end != 0 && start >= end) {
                        throw BtException.unknown("inverted or invalid timestamp range [" + start + ", " + end + "]");
                    }
                    if (start < 0 || end < 0) {
                        throw BtException.unknown("invalid timestamp " + Math.min(start, end));
                    }
                    if (start % 1000 != 0) {
                        throw BtException.unknown("invalid timestamp " + start);
                    }
                    if (end % 1000 != 0) {
                        throw BtException.unknown("invalid timestamp " + end);
                    }
                    ops.add(new DelCol(d.getFamilyName(), d.getColumnQualifier().toByteArray(), start, end));
                    break;
                }
                case DELETE_FROM_FAMILY:
                    ops.add(new DelFam(m.getDeleteFromFamily().getFamilyName()));
                    break;
                case DELETE_FROM_ROW:
                    ops.add(new DelRow());
                    break;
                case ADD_TO_CELL:
                    throw BtException.unknown("illegal attempt to use AddToCell on non-aggregate cell");
                case MERGE_TO_CELL:
                    throw BtException.unknown("illegal attempt to use MergeToCell on non-aggregate cell");
                default:
                    throw BtException.unknown("can't handle mutation type <nil>");
            }
        }
        return ops;
    }

    static void requireFamily(Table table, String family) {
        if (!table.containsColumnFamilies(family)) {
            throw BtException.unknown("unknown family \"" + family + "\"");
        }
    }

    static void apply(Connection c, String tbl, byte[] key, List<Op> ops) throws SQLException {
        for (Op op : ops) {
            if (op instanceof SetCell s) {
                BtStore.putCell(c, tbl, key, s.family(), s.qual(), s.ts(), s.value());
            } else if (op instanceof DelCol d) {
                BtStore.deleteColumn(c, tbl, key, d.family(), d.qual(), d.start(), d.end());
            } else if (op instanceof DelFam d) {
                BtStore.deleteFamily(c, tbl, key, d.family());
            } else {
                BtStore.deleteRow(c, tbl, key);
            }
        }
    }
}
