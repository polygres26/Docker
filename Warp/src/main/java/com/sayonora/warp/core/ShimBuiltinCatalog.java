package com.sayonora.warp.core;

import java.util.Locale;
import java.util.Map;

/**
 * A small, hand-maintained allowlist of Oracle built-in package procedures/functions that
 * Shim/pg_oracle genuinely implements as real Postgres functions -- see
 * {@code Shim/pg_oracle/sql/pg_oracle--0.1.sql} and its README for the full, much larger set this
 * codebase does NOT yet know how to call. Confirmed live (2026-09-28): Postgres's own SQL parser
 * (even with pg_oracle loaded) has no grammar for Oracle's bare {@code BEGIN proc(...); END;}
 * anonymous-block syntax at all -- it only understands its own {@code DO $$...$$} block or a plain
 * function call -- so a real Oracle wire client's {@code {call schema.func(?, ?)}} shape (which
 * orawire's {@code isPlSqlBlock}/{@code RequestLoop} already recognizes) needs rewriting into an
 * ordinary Postgres function call before Adapt mode can reach these real, working Shim functions
 * at all.
 *
 * <p>Deliberately NOT a general Oracle-dictionary-backed resolver the way
 * {@code OracleProcedureCatalog} is for Bridge/dual-exec (that class queries real Oracle's own
 * {@code ALL_ARGUMENTS} view for arbitrary user procedures) -- plain Adapt mode has no Oracle
 * connection to query at all, so this table is hand-built from Shim's own known, fixed function
 * signatures instead. Scoped narrowly on purpose (see
 * {@code warp-adapt-plsql-shim-reach-plan} memory note for the full plan/scope writeup): only
 * covers exactly the entries below, not Shim's entire surface -- every other Shim builtin, and
 * every user-defined package, still falls through to {@code handlePlSqlExecute}'s existing clean
 * refusal in plain Adapt mode.
 */
public final class ShimBuiltinCatalog {

    /** @param hasReturnValue true for a real Oracle FUNCTION (scalar return, called via the JDBC
     *      {@code {? = call schema.func(...)}} shape); false for a real Oracle PROCEDURE (no
     *      return value, called via the plain {@code {call schema.func(...)}} shape). Every entry
     *      here is IN-only -- none of these Shim builtins are known to have an OUT parameter (a
     *      real one, if ever added, needs the same signature-table treatment as the return value,
     *      not guessed at). */
    public record Signature(int inParamCount, boolean hasReturnValue) {
    }

    private static final Map<String, Signature> BUILTINS = Map.ofEntries(
            // dbms_output: the most commonly-needed builtin for migrated application
            // logging/debugging (confirmed live: real output round-trips correctly through
            // Shim's own dbms_output.put_line/get_line when called via Postgres's native
            // function-call syntax -- see the memory note's own live-capture evidence).
            Map.entry("dbms_output.put_line", new Signature(1, false)),
            Map.entry("dbms_output.put", new Signature(1, false)),
            Map.entry("dbms_output.new_line", new Signature(0, false)),
            Map.entry("dbms_output.enable", new Signature(1, false)),
            Map.entry("dbms_output.disable", new Signature(0, false)));

    // Real bug found live, NOT yet fixed, tracked here rather than silently claimed to work:
    // hasReturnValue=true (the JDBC "{? = call schema.func}" function-return shape) was tried
    // against real Shim functions (dbms_random.value, dbms_random.random) and confirmed BROKEN --
    // a real ojdbc client rejects the response with "out parameter 1 was of type java.sql.Types=X
    // however type java.sql.Types=2 (NUMERIC) was registered", where X varies with the Postgres
    // function's own real return type (8/DOUBLE for dbms_random.value, 4/INTEGER for
    // dbms_random.random) -- this varying-by-real-return-type pattern means the client IS
    // detecting something real about the response, not hitting a fixed client-side quirk. This
    // exact JDBC call shape had never actually been EXECUTED anywhere in this codebase before this
    // feature (every earlier attempt at it, in Bridge/dual-exec mode, was refused outright before
    // ever reaching real execution -- see handlePlSqlExecute's own history), so this is a newly
    // exposed, genuinely separate bug in the shared response-writing path, not specific to Shim
    // routing -- needs its own live-capture-diff investigation (same discipline as every other fix
    // this session) before any scalar-function-return builtin can be added to this table. Until
    // then, ONLY hasReturnValue=false (void procedure) entries belong here.

    /** Looks up {@code schemaDotFunction} (e.g. {@code "dbms_output.put_line"}, case-insensitive)
     * against the allowlist above. Returns {@code null} if this isn't a known, supported Shim
     * builtin -- callers must fall through to the existing refusal, never guess at an unknown
     * signature. */
    public static Signature lookup(String schemaDotFunction) {
        return BUILTINS.get(schemaDotFunction.toLowerCase(Locale.ROOT));
    }

    private ShimBuiltinCatalog() {
    }
}
