package com.nexagres.wire.orawire.translator;

import java.util.regex.Pattern;

public final class DualTableRewriter {

    private static final Pattern FROM_DUAL =
            Pattern.compile("(?i)(\\bfrom\\s+)(?:sys\\.)?dual\\b");

    // Real SQL*Plus sends this exact statement (schema-qualified SYS.DUAL, plus a call to
    // XS_SYS_CONTEXT -- an Oracle-only namespace function this codebase makes no attempt to
    // implement) as its own internal startup probe, right after login, before showing its first
    // prompt -- confirmed live via a real capture: it's not something a user's own query ever
    // sends, it's sqlplus itself establishing what to print for "Connected to ... as USER" before
    // it will run anything else. Because FROM_DUAL's rewrite still leaves the DECODE/
    // XS_SYS_CONTEXT call in the SELECT list (Postgres has neither), that rewrite alone still
    // fails this specific query -- confirmed live via ORA-00942 coming back from a real Postgres
    // "relation does not exist" error surfacing where XS_SYS_CONTEXT would have been called, which
    // silently aborted the client (no further prompt, no visible error) rather than erroring
    // loudly. Not attempting a real XS_SYS_CONTEXT implementation for this: the query only exists
    // to power a cosmetic banner line, so substituting a value only Postgres needs to understand
    // is enough to unblock the client without pretending to implement Oracle's XS namespace.
    private static final Pattern SQLPLUS_STARTUP_USER_PROBE = Pattern.compile(
            "(?i)select\\s+decode\\s*\\(\\s*user\\s*,\\s*'XS\\$NULL'\\s*,\\s*"
                    + "XS_SYS_CONTEXT\\s*\\(\\s*'XS\\$SESSION'\\s*,\\s*'USERNAME'\\s*\\)\\s*,\\s*user\\s*\\)\\s+"
                    + "from\\s+(?:sys\\.)?dual\\b");

    public static String rewrite(String oracleSql) {
        if (SQLPLUS_STARTUP_USER_PROBE.matcher(oracleSql).find()) {
            return "select current_user";
        }
        return FROM_DUAL.matcher(oracleSql).replaceAll("$1(select 1) dual_placeholder");
    }

    private DualTableRewriter() {
    }
}
