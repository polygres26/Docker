package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** The parts of MySQL support that need no server: GTID counting, column-name fallbacks, apply check. */
class MySqlHaTest {

    @Test
    void gtidSetsAreCountedAcrossSourcesAndIntervals() {
        assertEquals(0, GtidSets.count(null));
        assertEquals(0, GtidSets.count("  "));
        assertEquals(5, GtidSets.count("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"));
        assertEquals(7, GtidSets.count("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7-8"));
        assertEquals(1, GtidSets.count("3e11fa47-71ca-11e1-9e33-c80aa9429562:42"));
        assertEquals(8, GtidSets.count("aaaaaaaa-0000-0000-0000-000000000001:1-5,\n"
                + "bbbbbbbb-0000-0000-0000-000000000002:1-3"));
        assertThrows(IllegalArgumentException.class, () -> GtidSets.count("uuid:one-two"));
    }

    private static Map<String, String> row(String... kv) {
        Map<String, String> m = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void columnsResolveUnderTheNewAndLegacyNamesCaseInsensitively() {
        assertEquals("Yes", MySqlHa.col(row("Replica_IO_Running", "Yes"), "Replica_IO_Running", "Slave_IO_Running"));
        assertEquals("Yes", MySqlHa.col(row("slave_io_running", "Yes"), "Replica_IO_Running", "Slave_IO_Running"));
        assertNull(MySqlHa.col(row("x", "1"), "Replica_IO_Running", "Slave_IO_Running"));
    }

    @Test
    void fullyAppliedUsesTheSqlThreadStateOnBothGenerationsOfServer() {
        assertTrue(MySqlHa.fullyApplied(row("Replica_SQL_Running_State",
                "Replica has read all relay log; waiting for more updates")));
        assertTrue(MySqlHa.fullyApplied(row("Slave_SQL_Running_State",
                "Slave has read all relay log; waiting for more updates")));
        assertFalse(MySqlHa.fullyApplied(row("Replica_SQL_Running_State", "Applying batch of row changes (write)")));
    }

    @Test
    void withoutAStateColumnItComparesExecutedAndReadPositions() {
        assertTrue(MySqlHa.fullyApplied(row("Relay_Master_Log_File", "b.000003", "Master_Log_File", "b.000003",
                "Exec_Master_Log_Pos", "900", "Read_Master_Log_Pos", "900")));
        assertFalse(MySqlHa.fullyApplied(row("Relay_Master_Log_File", "b.000003", "Master_Log_File", "b.000003",
                "Exec_Master_Log_Pos", "400", "Read_Master_Log_Pos", "900")));
        assertFalse(MySqlHa.fullyApplied(row("Relay_Master_Log_File", "b.000002", "Master_Log_File", "b.000003",
                "Exec_Master_Log_Pos", "900", "Read_Master_Log_Pos", "900")));
    }

    @Test
    void mysqlAndPostgresAreSupportedEnginesAndOthersAreNot() {
        assertTrue(EngineHa.forDialect(SourceDialect.MYSQL) != null);
        assertTrue(EngineHa.forDialect(SourceDialect.POSTGRES) != null);
        assertNull(EngineHa.forDialect(SourceDialect.ORACLE));
        assertNull(EngineHa.forDialect(SourceDialect.SQL_SERVER));
        assertNull(EngineHa.forDialect(null));
    }
}
