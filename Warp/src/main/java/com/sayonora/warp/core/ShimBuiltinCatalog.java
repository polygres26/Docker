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
 * ordinary Postgres function call before Emulate mode can reach these real, working Shim functions
 * at all.
 *
 * <p>Deliberately NOT a general Oracle-dictionary-backed resolver the way
 * {@code OracleProcedureCatalog} is for Bridge/dual-exec (that class queries real Oracle's own
 * {@code ALL_ARGUMENTS} view for arbitrary user procedures) -- plain Emulate mode has no Oracle
 * connection to query at all, so this table is hand-built from Shim's own known, fixed function
 * signatures instead. Scoped narrowly on purpose (see
 * {@code warp-adapt-plsql-shim-reach-plan} memory note for the full plan/scope writeup): only
 * covers exactly the entries below, not Shim's entire surface -- every other Shim builtin, and
 * every user-defined package, still falls through to {@code handlePlSqlExecute}'s existing clean
 * refusal in plain Emulate mode.
 */
public final class ShimBuiltinCatalog {

    /** @param hasReturnValue true for a real Oracle FUNCTION (scalar return, called via the JDBC
     *      {@code {? = call schema.func(...)}} shape); false for a real Oracle PROCEDURE (no
     *      return value, called via the plain {@code {call schema.func(...)}} shape). Every entry
     *      here is IN-only -- none of these Shim builtins are known to have an OUT parameter (a
     *      real one, if ever added, needs the same signature-table treatment as the return value,
     *      not guessed at).
     * @param returnSqlType the real {@link java.sql.Types} constant of the underlying Postgres
     *      function's OWN return type (e.g. {@code Types.INTEGER} for a Postgres {@code integer}
     *      return, {@code Types.DOUBLE} for {@code double precision}) -- {@code 0} when {@code
     *      hasReturnValue} is false. Real bug this fixes, found live: this must be the function's
     *      REAL Postgres return type, not a fixed guess -- PGJDBC's own {@code CallableStatement}
     *      validates the type registered via {@code registerOutParameter} against the actual
     *      function's real signature and throws a real {@code SQLException} ("out parameter 1 was
     *      of type java.sql.Types=X however type java.sql.Types=Y was registered") if they don't
     *      match. Confirmed live: this exception was being thrown server-side (inside this
     *      feature's own PGJDBC call, not by the receiving Oracle client at all -- a misleading
     *      first impression, since the client's own generic error-help URL made it look
     *      client-side) whenever the registered type (previously always hardcoded to {@code
     *      Types.NUMERIC}) didn't match the real function's return type. The value returned to the
     *      ORACLE-speaking client is still always encoded as a plain Oracle NUMBER regardless of
     *      this field -- {@code writeColumnValue}'s existing NUMBER encoding already converts any
     *      real Java numeric type correctly -- this field only controls what's registered against
     *      PGJDBC on the Postgres side. */
    public record Signature(int inParamCount, boolean hasReturnValue, int returnSqlType) {
        public Signature(int inParamCount, boolean hasReturnValue) {
            this(inParamCount, hasReturnValue, 0);
        }
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
            Map.entry("dbms_output.disable", new Signature(0, false)),
            // Confirmed live via \df dbms_random.* against a real pg_oracle install: random()
            // returns Postgres "integer", value() returns "double precision" -- registerOutParameter
            // must match exactly (see Signature's own javadoc for the real bug this fixes).
            Map.entry("dbms_random.random", new Signature(0, true, java.sql.Types.INTEGER)),
            Map.entry("dbms_random.value", new Signature(0, true, java.sql.Types.DOUBLE)));

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
