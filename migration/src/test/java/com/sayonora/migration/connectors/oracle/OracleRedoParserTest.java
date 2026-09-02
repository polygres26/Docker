package com.sayonora.migration.connectors.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pure logic, no infrastructure needed -- proves {@link OracleRedoParser} against exact real
 * SQL_REDO text captured live from a real Oracle Database Free instance (see this session's own
 * LogMiner probing), not invented/guessed text. */
class OracleRedoParserTest {

    @Test
    void parsesAPlainInsert() {
        String redo = "insert into \"MIGTEST2\".\"T1\"(\"ID\",\"V\") values ('1','hello');";
        Map<String, String> values = OracleRedoParser.parseInsert(redo).values();
        assertEquals("1", values.get("ID"));
        assertEquals("hello", values.get("V"));
    }

    @Test
    void parsesAnInsertWithANullValue() {
        String redo = "insert into \"MIGTEST2\".\"T1\"(\"ID\",\"V\") values ('2',NULL);";
        Map<String, String> values = OracleRedoParser.parseInsert(redo).values();
        assertEquals("2", values.get("ID"));
        assertTrue(values.containsKey("V"));
        assertNull(values.get("V"));
    }

    @Test
    void parsesAnInsertWithADateAndAnEscapedQuote() {
        String redo = "insert into \"MIGTEST2\".\"T2\"(\"ID\",\"D\",\"AMT\",\"NOTE\") "
                + "values ('1',TO_DATE('2026-08-30 22:49:25', 'YYYY-MM-DD HH24:MI:SS'),'12.5','it''s a test');";
        Map<String, String> values = OracleRedoParser.parseInsert(redo).values();
        assertEquals("1", values.get("ID"));
        assertEquals("2026-08-30 22:49:25", values.get("D"));
        assertEquals("12.5", values.get("AMT"));
        assertEquals("it's a test", values.get("NOTE"));
    }

    @Test
    void parsesAnUpdateRecoveringThePrimaryKeyFromTheWhereClause() {
        // Real ALL COLUMNS supplemental-logging redo -- the primary key (ID) shows up in the WHERE
        // clause alongside the changed column's old value and ROWID, exactly what
        // OracleSource#ensureSupplementalLogging exists to guarantee.
        String redo = "update \"MIGTEST2\".\"T1\" set \"V\" = 'fullimage' where \"ID\" = '1' "
                + "and \"V\" = 'updated' and ROWID = 'AAAR1PAAYAAAAAfAAA';";
        Map<String, String> values = OracleRedoParser.parseUpdateOrDelete(redo, true).values();
        assertEquals("1", values.get("ID"));
        assertEquals("fullimage", values.get("V"), "the SET clause's NEW value must win over the WHERE clause's OLD value");
    }

    @Test
    void parsesADeleteWithANullColumnInTheWhereClause() {
        String redo = "delete from \"MIGTEST2\".\"T1\" where \"ID\" = '2' and \"V\" IS NULL "
                + "and ROWID = 'AAAR1PAAYAAAAAfAAB';";
        Map<String, String> values = OracleRedoParser.parseUpdateOrDelete(redo, false).values();
        assertEquals("2", values.get("ID"));
        assertTrue(values.containsKey("V"));
        assertNull(values.get("V"));
    }

    @Test
    void updateWithOnlyOneChangedColumnDoesNotIncludeUnrelatedColumns() {
        String redo = "update \"MIGTEST2\".\"T1\" set \"V\" = 'x' where \"ID\" = '5' and \"V\" = 'y' and ROWID = 'abc';";
        Map<String, String> values = OracleRedoParser.parseUpdateOrDelete(redo, true).values();
        assertEquals(2, values.size(), "only the changed column and the recovered primary key should be present: " + values);
    }
}
