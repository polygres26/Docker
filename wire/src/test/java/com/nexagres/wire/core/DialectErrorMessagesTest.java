package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Every "postgresMessage" input string below was captured from a REAL Postgres 16 instance (not
 * guessed) by actually triggering each error via psql -- see the session notes for the exact
 * DDL/DML used. This locks in both halves of {@link DialectErrorMessages}: that the
 * identifier-extraction regex still matches Postgres's real wording, and that the resulting
 * dialect-native message is exactly the real Oracle/MySQL/SQL Server text for that error.
 */
class DialectErrorMessagesTest {

    @Test
    void undefinedTableGetsOracleNativeWording() {
        assertEquals("ORA-00942: table or view does not exist",
                DialectErrorMessages.render(SourceDialect.ORACLE, "42P01",
                        "relation \"nonexistent_table\" does not exist"));
    }

    @Test
    void undefinedTableGetsMySqlNativeWordingWithTheRealTableName() {
        assertEquals("Table 'nonexistent_table' doesn't exist",
                DialectErrorMessages.render(SourceDialect.MYSQL, "42P01",
                        "relation \"nonexistent_table\" does not exist"));
    }

    @Test
    void undefinedTableGetsSqlServerNativeWordingWithTheRealTableName() {
        assertEquals("Invalid object name 'nonexistent_table'.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "42P01",
                        "relation \"nonexistent_table\" does not exist"));
    }

    @Test
    void undefinedColumnCarriesTheRealColumnNameIntoEveryDialect() {
        String pg = "column \"nonexistent_column\" does not exist";
        assertEquals("ORA-00904: \"nonexistent_column\": invalid identifier",
                DialectErrorMessages.render(SourceDialect.ORACLE, "42703", pg));
        assertEquals("Unknown column 'nonexistent_column'",
                DialectErrorMessages.render(SourceDialect.MYSQL, "42703", pg));
        assertEquals("Invalid column name 'nonexistent_column'.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "42703", pg));
    }

    @Test
    void uniqueViolationCarriesTheRealConstraintName() {
        String pg = "duplicate key value violates unique constraint \"t_pkey\"";
        assertEquals("ORA-00001: unique constraint (t_pkey) violated",
                DialectErrorMessages.render(SourceDialect.ORACLE, "23505", pg));
        assertEquals("Duplicate entry for key 't_pkey'",
                DialectErrorMessages.render(SourceDialect.MYSQL, "23505", pg));
        assertEquals("Violation of UNIQUE KEY constraint 't_pkey'. Cannot insert duplicate key.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "23505", pg));
    }

    @Test
    void notNullViolationCarriesBothTheRealColumnAndRelationName() {
        String pg = "null value in column \"name\" of relation \"t\" violates not-null constraint";
        assertEquals("ORA-01400: cannot insert NULL into (\"t\".\"name\")",
                DialectErrorMessages.render(SourceDialect.ORACLE, "23502", pg));
        assertEquals("Column 'name' cannot be null",
                DialectErrorMessages.render(SourceDialect.MYSQL, "23502", pg));
        assertEquals("Cannot insert the value NULL into column 'name', table 't'; column does not allow nulls.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "23502", pg));
    }

    @Test
    void foreignKeyViolationCarriesTheRealTableAndConstraintName() {
        String pg = "insert or update on table \"t2\" violates foreign key constraint \"t2_id_fkey\"";
        assertEquals("ORA-02291: integrity constraint (t2_id_fkey) violated - parent key not found",
                DialectErrorMessages.render(SourceDialect.ORACLE, "23503", pg));
        assertEquals("Cannot add or update a child row: a foreign key constraint fails",
                DialectErrorMessages.render(SourceDialect.MYSQL, "23503", pg));
        assertEquals(
                "The INSERT statement conflicted with the FOREIGN KEY constraint \"t2_id_fkey\". "
                        + "The conflict occurred in table \"t2\".",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "23503", pg));
    }

    @Test
    void duplicateIndexCarriesTheRealRelationName() {
        // The scenario the whole plan started from: CREATE INDEX on a name that already exists.
        String pg = "relation \"idx_t_name\" already exists";
        assertEquals("ORA-00955: name is already used by an existing object",
                DialectErrorMessages.render(SourceDialect.ORACLE, "42P07", pg));
        assertEquals("Table 'idx_t_name' already exists",
                DialectErrorMessages.render(SourceDialect.MYSQL, "42P07", pg));
        assertEquals("There is already an object named 'idx_t_name' in the database.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "42P07", pg));
    }

    @Test
    void checkViolationCarriesTheRealTableAndConstraintName() {
        String pg = "new row for relation \"t\" violates check constraint \"t_amount_check\"";
        assertEquals("ORA-02290: check constraint (t_amount_check) violated",
                DialectErrorMessages.render(SourceDialect.ORACLE, "23514", pg));
        assertEquals("Check constraint 't_amount_check' is violated.",
                DialectErrorMessages.render(SourceDialect.MYSQL, "23514", pg));
        assertEquals(
                "The INSERT statement conflicted with the CHECK constraint \"t_amount_check\". "
                        + "The conflict occurred in database table \"t\".",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "23514", pg));
    }

    @Test
    void invalidLiteralCarriesTheRealTypeAndValue() {
        String pg = "invalid input syntax for type integer: \"abc\"";
        assertEquals("ORA-01722: invalid number",
                DialectErrorMessages.render(SourceDialect.ORACLE, "22P02", pg));
        assertEquals("Incorrect integer value: 'abc'",
                DialectErrorMessages.render(SourceDialect.MYSQL, "22P02", pg));
        assertEquals("Conversion failed when converting the varchar value 'abc' to data type integer.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "22P02", pg));
    }

    @Test
    void permissionDeniedCarriesTheRealObjectName() {
        String pg = "permission denied for table t";
        assertEquals("ORA-01031: insufficient privileges",
                DialectErrorMessages.render(SourceDialect.ORACLE, "42501", pg));
        assertEquals("Access denied for table 't'",
                DialectErrorMessages.render(SourceDialect.MYSQL, "42501", pg));
        assertEquals("The SELECT permission was denied on the object 't'.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "42501", pg));
    }

    @Test
    void undefinedFunctionCarriesTheRealSignature() {
        String pg = "function nonexistent_func(integer, integer) does not exist";
        assertEquals("ORA-00904: \"nonexistent_func(integer, integer)\": invalid identifier",
                DialectErrorMessages.render(SourceDialect.ORACLE, "42883", pg));
        assertEquals("FUNCTION nonexistent_func(integer, integer) does not exist",
                DialectErrorMessages.render(SourceDialect.MYSQL, "42883", pg));
        assertEquals("'nonexistent_func(integer, integer)' is not a recognized built-in function name.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "42883", pg));
    }

    @Test
    void lockNotAvailableCarriesTheRealRelationNameThroughTheExtractorButTemplatesAreFixedText() {
        // Captured real PG text does contain the relation name ("could not obtain lock on row in
        // relation \"t\""), but real Oracle/MySQL/SQL Server NOWAIT errors don't name the row/
        // relation either -- so this SQLSTATE deliberately has no extractor and every template is
        // fixed text, matching each vendor's real (identifier-less) wording.
        String pg = "could not obtain lock on row in relation \"t\"";
        assertEquals("ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired",
                DialectErrorMessages.render(SourceDialect.ORACLE, "55P03", pg));
        assertEquals("Lock request time out period exceeded.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "55P03", pg));
    }

    @Test
    void deadlockDetectedIsFixedTextInEveryDialectSinceItNeedsNoIdentifier() {
        String pg = "deadlock detected";
        assertEquals("ORA-00060: deadlock detected while waiting for resource",
                DialectErrorMessages.render(SourceDialect.ORACLE, "40P01", pg));
        assertEquals("Deadlock found when trying to get lock; try restarting transaction",
                DialectErrorMessages.render(SourceDialect.MYSQL, "40P01", pg));
        assertEquals(
                "Transaction was deadlocked on lock resources with another process and has been "
                        + "chosen as the deadlock victim. Rerun the transaction.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "40P01", pg));
    }

    @Test
    void divisionByZeroIsFixedTextInEveryDialect() {
        String pg = "division by zero";
        assertEquals("ORA-01476: divisor is equal to zero",
                DialectErrorMessages.render(SourceDialect.ORACLE, "22012", pg));
        assertEquals("Division by 0",
                DialectErrorMessages.render(SourceDialect.MYSQL, "22012", pg));
        assertEquals("Divide by zero error encountered.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "22012", pg));
    }

    @Test
    void queryCanceledOnSqlServerIsAnHonestDescriptionNotAFabricatedRealMessage() {
        // 57014 has no real sys.messages entry on SQL Server at all -- see
        // SqlStateErrorMapper's comment on this SQLSTATE. Just confirming it renders something
        // sane rather than crashing or leaking a raw {0}.
        assertEquals("Query was canceled.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "57014",
                        "canceling statement due to statement timeout"));
    }

    @Test
    void anUnmappedSqlStatePassesThePostgresMessageThroughUnchanged() {
        String pg = "syntax error at or near \"SELCT\"";
        assertEquals(pg, DialectErrorMessages.render(SourceDialect.ORACLE, "42601", pg));
        assertEquals(pg, DialectErrorMessages.render(SourceDialect.MYSQL, "42601", pg));
        assertEquals(pg, DialectErrorMessages.render(SourceDialect.SQL_SERVER, "42601", pg));
    }

    @Test
    void aNullSqlStateOrMessagePassesThrough() {
        assertEquals("some message", DialectErrorMessages.render(SourceDialect.ORACLE, null, "some message"));
        assertEquals(null, DialectErrorMessages.render(SourceDialect.ORACLE, "42P01", null));
    }

    @Test
    void anExtractorThatDoesNotMatchAnUnexpectedPostgresWordingFallsBackToTheRawMessage() {
        // Simulates a future/older Postgres version phrasing this differently than what the
        // regex expects -- must never crash or produce a mangled partial render.
        String unexpectedWording = "table foo cannot be found (unusual future PG wording)";
        assertEquals(unexpectedWording,
                DialectErrorMessages.render(SourceDialect.ORACLE, "42P01", unexpectedWording));
    }

    @Test
    void nonEmulatingDialectsThrowRatherThanSilentlyReturningAWrongVocabulary() {
        // WARP_NATIVE/POSTGRES/MCP clients already expect real Postgres wording -- calling
        // render() for one of those would be a caller bug, not a case to silently paper over.
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> DialectErrorMessages.render(SourceDialect.POSTGRES, "42P01", "relation \"t\" does not exist"));
    }

    @Test
    void ambiguousColumnCarriesTheRealColumnName() {
        String pg = "column reference \"id\" is ambiguous";
        assertEquals("ORA-00918: column ambiguously defined",
                DialectErrorMessages.render(SourceDialect.ORACLE, "42702", pg));
        assertEquals("Column 'id' is ambiguous",
                DialectErrorMessages.render(SourceDialect.MYSQL, "42702", pg));
        assertEquals("Ambiguous column name 'id'.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "42702", pg));
    }

    @Test
    void tooManyConnectionsIsFixedTextInEveryDialect() {
        String pg = "sorry, too many clients already";
        assertEquals("ORA-00018: maximum number of sessions exceeded",
                DialectErrorMessages.render(SourceDialect.ORACLE, "53300", pg));
        assertEquals("Too many connections",
                DialectErrorMessages.render(SourceDialect.MYSQL, "53300", pg));
        assertEquals("Too many connections.",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "53300", pg));
    }

    @Test
    void foreignKeyViolationOnTheInsertSideStillUsesTheOriginalTemplate() {
        // Regression guard: adding the delete-side special case must not change the existing,
        // already-tested insert-side behavior (see foreignKeyViolationCarriesTheRealTableAndConstraintName
        // above) for a plain insert-side message.
        String pg = "insert or update on table \"t2\" violates foreign key constraint \"t2_id_fkey\"";
        assertEquals("ORA-02291: integrity constraint (t2_id_fkey) violated - parent key not found",
                DialectErrorMessages.render(SourceDialect.ORACLE, "23503", pg));
    }

    @Test
    void foreignKeyViolationOnTheDeleteSideGetsRealVendorWordingForThatDirection() {
        String pg = "update or delete on table \"parent\" violates foreign key constraint "
                + "\"child_id_fkey\" on table \"child\"";
        assertEquals("ORA-02292: integrity constraint (child_id_fkey) violated - child record found",
                DialectErrorMessages.render(SourceDialect.ORACLE, "23503", pg));
        assertEquals("Cannot delete or update a parent row: a foreign key constraint fails",
                DialectErrorMessages.render(SourceDialect.MYSQL, "23503", pg));
        assertEquals(
                "The DELETE statement conflicted with the REFERENCE constraint \"child_id_fkey\". "
                        + "The conflict occurred in table \"child\".",
                DialectErrorMessages.render(SourceDialect.SQL_SERVER, "23503", pg));
    }

    @Test
    void foreignKeyViolationDeleteSideRenderingSurvivesTheRealErrorPrefix() {
        // Same real bug as SqlStateErrorMapperTest's matching regression test -- a genuinely
        // caught SQLException's getMessage() carries Postgres's "ERROR: " prefix, which broke an
        // earlier startsWith(...) check here too (fixed to contains(...)).
        String realCaughtMessage = "ERROR: update or delete on table \"ojdbc_fk_parent\" violates "
                + "foreign key constraint \"ojdbc_fk_child_id_fkey\" on table \"ojdbc_fk_child\"";
        assertEquals("ORA-02292: integrity constraint (ojdbc_fk_child_id_fkey) violated - child record found",
                DialectErrorMessages.render(SourceDialect.ORACLE, "23503", realCaughtMessage));
    }
}
