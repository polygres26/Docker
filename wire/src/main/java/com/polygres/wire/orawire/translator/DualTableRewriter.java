package com.polygres.wire.orawire.translator;

import java.util.regex.Pattern;

public final class DualTableRewriter {

    private static final Pattern FROM_DUAL =
            Pattern.compile("(?i)(\\bfrom\\s+)dual\\b");

    public static String rewrite(String oracleSql) {
        return FROM_DUAL.matcher(oracleSql).replaceAll("$1(select 1) dual_placeholder");
    }

    private DualTableRewriter() {
    }
}
