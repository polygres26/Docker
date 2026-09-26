package com.sayonora.wire.bigtablewire;

import java.util.List;

/** A row key and its cells in standard order. */
record BtRow(byte[] key, List<BtCell> cells) {

    boolean isEmpty() {
        return cells.isEmpty();
    }
}
