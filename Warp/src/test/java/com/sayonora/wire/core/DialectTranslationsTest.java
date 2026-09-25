package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Regression coverage for a real bug found live testing orawire against a genuine SQLcl client
 * (not just the JDBC driver every prior orawire test used): {@link DialectTranslations#translate}
 * used to return its rewritten SQL unconditionally once a normalizer/renderer pair existed for
 * (from, to), even when neither actually recognized anything in the input and just handed it back
 * unchanged -- so "CREATE TABLE t (id NUMBER)" reached Postgres as literally "NUMBER", a type that
 * doesn't exist there, and {@link com.sayonora.wire.core.DialectTranslationStage}'s LLM fallback
 * (which only fires when this method returns {@code null}) was structurally unreachable for it.
 */
class DialectTranslationsTest {

    @Test
    void oracleSpecificTypeInDdlIsNotSilentlyPassedThroughUnchanged() {
        String result = DialectTranslations.translate(
                "CREATE TABLE t (id NUMBER PRIMARY KEY)", SourceDialect.ORACLE, SourceDialect.POSTGRES);
        assertNull(result, "NUMBER is not valid Postgres syntax and no rule rewrites it -- "
                + "this must return null so DialectTranslationStage's LLM fallback actually runs, "
                + "not silently hand back unchanged, invalid-on-Postgres SQL");
    }

    @Test
    void varchar2IsNotSilentlyPassedThroughUnchanged() {
        String result = DialectTranslations.translate(
                "SELECT CAST(x AS VARCHAR2(50)) FROM t", SourceDialect.ORACLE, SourceDialect.POSTGRES);
        assertNull(result);
    }

    @Test
    void connectByIsNotSilentlyPassedThroughUnchanged() {
        String result = DialectTranslations.translate(
                "SELECT * FROM t CONNECT BY PRIOR id = parent_id", SourceDialect.ORACLE, SourceDialect.POSTGRES);
        assertNull(result);
    }

    @Test
    void ordinaryQueryTheDeterministicRulesDoHandleStillTranslatesNormally() {
        // A plain rule-covered rewrite (NVL -> COALESCE) must still succeed and NOT trip the new
        // safety net -- it doesn't contain any of the unhandled-construct markers.
        String result = DialectTranslations.translate(
                "SELECT NVL(x, 0) FROM t", SourceDialect.ORACLE, SourceDialect.POSTGRES);
        assertEquals("SELECT COALESCE(x, 0) FROM t", result);
    }

    @Test
    void selectFromDualStillTranslatesNormally() {
        String result = DialectTranslations.translate(
                "SELECT 1 FROM dual", SourceDialect.ORACLE, SourceDialect.POSTGRES);
        assertEquals("SELECT 1", result);
    }

    @Test
    void unhandledConstructInsideAStringLiteralDoesNotFalselyTripTheSafetyNet() {
        // "VARCHAR2" appearing inside a string literal isn't a construct the backend needs to
        // parse as SQL -- the safety net must be literal-aware, same as every other rule here.
        String result = DialectTranslations.translate(
                "SELECT 'this mentions VARCHAR2 in a comment string' AS note FROM t",
                SourceDialect.ORACLE, SourceDialect.POSTGRES);
        assertTrue(result != null && result.contains("VARCHAR2"),
                "a literal containing the word VARCHAR2 must translate normally, not be treated as an unhandled construct");
    }

    @Test
    void mysqlToPostgresIsUnaffectedByTheOracleOnlySafetyNet() {
        // The safety net is scoped to ORACLE->POSTGRES specifically (see translate()'s javadoc) --
        // a MySQL-only construct like backtick identifiers must keep working exactly as before.
        String result = DialectTranslations.translate("SELECT `col` FROM t", SourceDialect.MYSQL, SourceDialect.POSTGRES);
        assertEquals("SELECT \"col\" FROM t", result);
    }
}
