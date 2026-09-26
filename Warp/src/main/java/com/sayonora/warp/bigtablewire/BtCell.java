package com.sayonora.warp.bigtablewire;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** One cell of a row in the flat (family, qualifier, timestamp descending) order every Bigtable read returns. */
record BtCell(String family, byte[] qual, long ts, byte[] value, List<String> labels) {

    static final List<String> NO_LABELS = List.of();

    /** Family name (C collation == UTF-8 byte order for the ASCII family names Bigtable allows), qualifier bytes, newest first. */
    static final Comparator<BtCell> ORDER = (a, b) -> {
        int c = a.family.compareTo(b.family);
        if (c != 0) {
            return c;
        }
        c = Arrays.compareUnsigned(a.qual, b.qual);
        if (c != 0) {
            return c;
        }
        return Long.compare(b.ts, a.ts);
    };

    BtCell withValue(byte[] v) {
        return new BtCell(family, qual, ts, v, labels);
    }

    BtCell withLabels(List<String> l) {
        return new BtCell(family, qual, ts, value, l);
    }

    boolean sameColumn(BtCell o) {
        return o != null && family.equals(o.family) && Arrays.equals(qual, o.qual);
    }

    long size() {
        return family.length() + qual.length + value.length + 16;
    }
}
