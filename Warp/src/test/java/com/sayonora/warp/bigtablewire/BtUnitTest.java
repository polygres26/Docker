package com.sayonora.warp.bigtablewire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.sayonora.warp.bigtablewire.v2.RowFilter;
import com.sayonora.warp.bigtablewire.v2.RowRange;
import com.sayonora.warp.bigtablewire.v2.RowSet;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BtUnitTest {

    private static byte[] b(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
    }

    private static BtCell cell(String f, String q, long ts, String v) {
        return new BtCell(f, b(q), ts, b(v), BtCell.NO_LABELS);
    }

    private static List<BtCell> row() {
        List<BtCell> l = new ArrayList<>(List.of(cell("cf", "a", 1000, "v1"), cell("cf", "a", 2000, "v2"), cell("cf", "b", 1000, "w"),
                cell("cf2", "a", 3000, "x")));
        l.sort(BtCell.ORDER);
        return l;
    }

    private static List<String> vals(List<BtCell> l) {
        return l.stream().map(c -> new String(c.value())).toList();
    }

    @Test
    void regexIsFullMatchByteWise() {
        assertTrue(BtRegex.compile("f", b("r[12]")).test(b("r1")));
        assertFalse(BtRegex.compile("f", b("r")).test(b("r1")));
        assertTrue(BtRegex.compile("f", b("caf\\C\\C")).test(b("cafÃ©")));
        assertFalse(BtRegex.compile("f", b("caf.")).test(b("cafÃ©")));
        assertTrue(BtRegex.compile("f", b("[[:digit:]]+")).test(b("123")));
        assertTrue(BtRegex.compile("f", b("(?P<n>a)b")).test(b("ab")));
        assertFalse(BtRegex.compile("f", b("a.b")).test(b("a\nb")));
    }

    @Test
    void regexRejectsWhatRe2Rejects() {
        for (String p : new String[] {"(", "[", "(?=a)", "(a)\\1", "a**"}) {
            BtException e = assertThrows(BtException.class, () -> BtRegex.compile("row_key_regex_filter", b(p)), p);
            assertEquals(Status.Code.INVALID_ARGUMENT, e.code);
        }
    }

    @Test
    void filtersChainInterleaveCondition() {
        RowFilter qa = RowFilter.newBuilder().setColumnQualifierRegexFilter(ByteString.copyFromUtf8("a")).build();
        RowFilter one = RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build();
        RowFilter chain = RowFilter.newBuilder().setChain(RowFilter.Chain.newBuilder().addFilters(qa).addFilters(one)).build();
        assertEquals(List.of("v2", "x"), vals(BtFilter.compile(chain).apply(b("r"), row())));
        RowFilter inter = RowFilter.newBuilder().setInterleave(RowFilter.Interleave.newBuilder().addFilters(qa).addFilters(qa)).build();
        assertEquals(6, BtFilter.compile(inter).apply(b("r"), row()).size());
        RowFilter cond = RowFilter.newBuilder().setCondition(RowFilter.Condition.newBuilder()
                .setPredicateFilter(RowFilter.newBuilder().setValueRegexFilter(ByteString.copyFromUtf8("nope")))
                .setFalseFilter(RowFilter.newBuilder().setCellsPerRowLimitFilter(1))).build();
        assertEquals(1, BtFilter.compile(cond).apply(b("r"), row()).size());
        assertNull(BtFilter.apply(BtFilter.compile(RowFilter.newBuilder().setBlockAllFilter(true).build()), new BtRow(b("r"), row())));
    }

    @Test
    void offsetAndLimitsAndErrors() {
        assertEquals(2, BtFilter.compile(RowFilter.newBuilder().setCellsPerRowOffsetFilter(2).build()).apply(b("r"), row()).size());
        assertEquals(3, BtFilter.compile(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build()).apply(b("r"), row()).size());
        assertThrows(BtException.class, () -> BtFilter.compile(RowFilter.newBuilder().setCellsPerRowLimitFilter(0).build()));
        assertThrows(BtException.class, () -> BtFilter.compile(RowFilter.newBuilder().setChain(RowFilter.Chain.getDefaultInstance()).build()));
        assertThrows(BtException.class, () -> BtFilter.compile(RowFilter.newBuilder().setApplyLabelTransformer("UP").build()));
        assertThrows(BtException.class, () -> BtFilter.compile(RowFilter.newBuilder().setRowSampleFilter(1.0).build()));
    }

    @Test
    void rowSetIntervalsAreMergedAndValidated() {
        RowSet rs = RowSet.newBuilder().addRowKeys(ByteString.copyFromUtf8("c")).addRowKeys(ByteString.copyFromUtf8("a"))
                .addRowRanges(RowRange.newBuilder().setStartKeyClosed(ByteString.copyFromUtf8("a")).setEndKeyOpen(ByteString.copyFromUtf8("b")))
                .addRowRanges(RowRange.newBuilder().setStartKeyClosed(ByteString.copyFromUtf8("b")).setEndKeyClosed(ByteString.copyFromUtf8("b"))).build();
        var ivs = BtData.intervals(rs);
        assertEquals(2, ivs.size()); // [a, b\0) merged, point c
        assertArrayEquals(b("a"), ivs.get(0).lo());
        assertArrayEquals(new byte[] {'b', 0}, ivs.get(0).hi());
        assertNull(BtData.intervals(RowSet.getDefaultInstance()));
        RowSet bad = RowSet.newBuilder().addRowRanges(RowRange.newBuilder().setStartKeyClosed(ByteString.copyFromUtf8("d"))
                .setEndKeyOpen(ByteString.copyFromUtf8("b"))).build();
        assertEquals("Error in element #0: start_key_closed must be less than end_key_open",
                assertThrows(BtException.class, () -> BtData.intervals(bad)).getMessage());
    }

    @Test
    void timestampsAndPrefixSuccessor() {
        assertEquals(5000, BtMutations.checkTimestamp(-1, 5000));
        assertEquals(2000, BtMutations.checkTimestamp(2000, 5000));
        assertThrows(BtException.class, () -> BtMutations.checkTimestamp(1234, 5000));
        assertThrows(BtException.class, () -> BtMutations.checkTimestamp(-2, 5000));
        assertArrayEquals(b("ab"), BtAdmin.successor(b("aa")));
        assertArrayEquals(new byte[] {2}, BtAdmin.successor(new byte[] {1, (byte) 0xff}));
        assertNull(BtAdmin.successor(new byte[] {(byte) 0xff}));
    }
}
