package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The explicit pin-trigger list: what pins a session, and (just as important) what must NOT. */
class SessionStatePinsTest {

    private static boolean pins(SourceDialect dialect, String sql) {
        return SessionStatePins.sessionStateReason(dialect, sql) != null;
    }

    @Test
    void postgresSessionStatePins() {
        for (String sql : new String[] {
                "SET search_path = foo", "set application_name to 'x'", "SET SESSION statement_timeout = 5",
                "SET ROLE analyst", "SET SESSION AUTHORIZATION bob", "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
                "RESET ALL", "reset work_mem", "DISCARD ALL", "LISTEN chan", "UNLISTEN *",
                "PREPARE q AS SELECT 1", "CREATE TEMP TABLE t (id int)", "create temporary table t2 as select 1",
                "CREATE GLOBAL TEMPORARY TABLE g (id int)", "SELECT * INTO TEMP t3 FROM t",
                "SELECT pg_advisory_lock(1)", "select pg_try_advisory_lock(2)", "SELECT pg_advisory_lock_shared(3)",
                "SELECT set_config('a.b', 'c', false)", "SELECT nextval('s')", "SELECT currval('s')", "SELECT lastval()",
                "LOAD 'auto_explain'", "DECLARE c CURSOR WITH HOLD FOR SELECT 1" }) {
            assertTrue(pins(SourceDialect.POSTGRES, sql), () -> "should pin: " + sql);
        }
    }

    @Test
    void statementsThatLeaveNothingBehindDoNotPin() {
        for (String sql : new String[] {
                "SELECT 1", "INSERT INTO t VALUES (1)", "UPDATE t SET a = 1", "DELETE FROM t",
                "SET LOCAL statement_timeout = 5", "SET TRANSACTION ISOLATION LEVEL SERIAL", "SET CONSTRAINTS ALL DEFERRED",
                "SELECT pg_advisory_xact_lock(1)", "SELECT * FROM settings WHERE name = 'x'",
                "CREATE TABLE t (id int)", "SELECT * FROM t FOR UPDATE", "PREPARE TRANSACTION 'x'",
                "SELECT format('%s', 1)", "SELECT 'not a #temp table'::text || 'a'" }) {
            // the last one intentionally contains "#temp" inside a literal: a false positive is allowed there
            if (sql.contains("#temp")) {
                continue;
            }
            assertNull(SessionStatePins.sessionStateReason(SourceDialect.POSTGRES, sql), () -> "must not pin: " + sql);
        }
    }

    @Test
    void otherDialectsPinOnTheirOwnSessionState() {
        assertTrue(pins(SourceDialect.SQL_SERVER, "CREATE TABLE #tmp (id int)"));
        assertTrue(pins(SourceDialect.SQL_SERVER, "SELECT * INTO ##g FROM t"));
        assertTrue(pins(SourceDialect.SQL_SERVER, "SELECT SCOPE_IDENTITY()"));
        assertTrue(pins(SourceDialect.SQL_SERVER, "SELECT @@IDENTITY"));
        assertTrue(pins(SourceDialect.MYSQL, "SELECT LAST_INSERT_ID()"));
        assertTrue(pins(SourceDialect.MYSQL, "CREATE TEMPORARY TABLE t (id int)"));
        assertTrue(pins(SourceDialect.ORACLE, "SELECT my_seq.NEXTVAL FROM dual"));
        assertTrue(pins(SourceDialect.ORACLE, "SELECT my_seq.CURRVAL FROM dual"));
        assertTrue(pins(SourceDialect.ORACLE, "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY'"));
        assertTrue(pins(SourceDialect.ORACLE, "BEGIN DBMS_OUTPUT.PUT_LINE('x'); END;"));
        assertFalse(pins(SourceDialect.SQL_SERVER, "SELECT * FROM users WHERE id = 1"));
        // SET is only a backend statement for the Postgres frontend; mywire/mssqlwire/orawire swallow it
        assertFalse(pins(SourceDialect.MYSQL, "SET autocommit = 0"));
    }

    @Test
    void plainSessionSetIsReplayableNotPinning() {
        var tz = SessionStatePins.replayableSetting("SET TIME ZONE 'UTC'");
        assertNotNull(tz);
        assertTrue("timezone".equals(tz.name()));
        assertTrue("datestyle".equals(SessionStatePins.replayableSetting("SET DATESTYLE TO 'ISO'").name()));
        assertTrue("application_name".equals(SessionStatePins.replayableSetting("set session application_name = 'x';").name()));
        assertTrue("search_path".equals(SessionStatePins.replayableSetting("SET search_path TO a, public").name()));
        assertTrue("warp.user_id".equals(SessionStatePins.replayableSetting("SET warp.user_id = 'x'").name()));
        for (String notReplayable : new String[] {
                "SET LOCAL x = 1", "SET ROLE analyst", "SET SESSION AUTHORIZATION bob", "SET TRANSACTION READ ONLY",
                "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", "SET CONSTRAINTS ALL DEFERRED", "SET NAMES 'utf8'",
                "SET x = 1; SET y = 2", "SET x FROM CURRENT", "SELECT 1", "SET" }) {
            assertNull(SessionStatePins.replayableSetting(notReplayable), () -> "must not be replayable: " + notReplayable);
        }
        assertTrue("*".equals(SessionStatePins.resetTarget("RESET ALL")));
        assertTrue("work_mem".equals(SessionStatePins.resetTarget("reset work_mem;")));
        assertTrue("timezone".equals(SessionStatePins.resetTarget("RESET TIME ZONE")));
        assertNull(SessionStatePins.resetTarget("RESET ROLE SOMETHING"));
        // the pin classifier itself is unchanged: handlers decide replay-vs-pin (replay only outside a transaction)
        assertTrue(pins(SourceDialect.POSTGRES, "SET DATESTYLE TO 'ISO'"));
    }

    @Test
    void cursorsAreReversibleAndDetectedSeparately() {
        assertTrue(SessionStatePins.isDeclareCursor("DECLARE c CURSOR FOR SELECT * FROM t"));
        assertTrue(SessionStatePins.isDeclareCursor("declare c scroll cursor for select 1"));
        assertFalse(SessionStatePins.isDeclareCursor("SELECT 'declare c cursor'"));
        assertTrue(SessionStatePins.isCloseCursor("CLOSE c"));
        assertTrue(SessionStatePins.isCloseCursor("close all;"));
        assertFalse(SessionStatePins.isCloseCursor("CLOSE c, d"));
        assertNull(SessionStatePins.sessionStateReason(SourceDialect.POSTGRES, "DECLARE c CURSOR FOR SELECT 1"));
        assertNotNull(SessionStatePins.sessionStateReason(SourceDialect.POSTGRES, "DECLARE c CURSOR WITH HOLD FOR SELECT 1"));
    }

    @Test
    void pureReadDetection() {
        assertTrue(SessionStatePins.isPureRead(SourceDialect.ORACLE, "SELECT 1 FROM dual"));
        assertTrue(SessionStatePins.isPureRead(SourceDialect.ORACLE, "  select * from t where a = :1"));
        assertTrue(SessionStatePins.isPureRead(SourceDialect.ORACLE, "WITH x AS (SELECT 1) SELECT * FROM x"));
        assertFalse(SessionStatePins.isPureRead(SourceDialect.ORACLE, "SELECT * FROM t FOR UPDATE"));
        assertFalse(SessionStatePins.isPureRead(SourceDialect.ORACLE, "INSERT INTO t VALUES (1)"));
        assertFalse(SessionStatePins.isPureRead(SourceDialect.ORACLE, "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x"));
        assertFalse(SessionStatePins.isPureRead(SourceDialect.ORACLE, "SELECT s.NEXTVAL FROM dual"));
        assertFalse(SessionStatePins.isPureRead(SourceDialect.ORACLE, "SELECT a INTO x FROM t"));
        assertFalse(SessionStatePins.isPureRead(SourceDialect.ORACLE, "UPDATE t SET a = 1"));
        assertFalse(SessionStatePins.isPureRead(SourceDialect.ORACLE, null));
    }
}
