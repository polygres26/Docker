package com.sayonora.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real proof that a real ojdbc client's {@code PreparedStatement.addBatch()}/{@code
 * executeBatch()} -- run TWICE on the same statement, exercising both a fresh-parse array-execute
 * and a cursor-reuse one with new bind values -- works through orawire.
 *
 * <p>Two real, distinct bugs found and fixed via byte-level capture of real ojdbc traffic (see
 * {@code ExecuteRequest#bindRows} and {@code ExecuteRequestReader#read}'s two-arg overload for the
 * full findings):
 * <ol>
 *   <li>A real array-execute sends N bind rows back-to-back, each self-delimited by its own
 *       {@code ROW_DATA} tag byte, with NO row-count field anywhere on the wire ({@code numIters}
 *       reads 0 for every Execute observed, batched or not) -- {@code
 *       ExecuteRequestReader#readBindParams} used to read exactly one row, silently discarding
 *       every row after the first.
 *   <li>Reusing an already-parsed cursor with NEW bind values (a real {@code FUNC_EXECUTE}, not a
 *       {@code FUNC_REEXECUTE}) sends those values WITHOUT re-describing their types (the
 *       {@code bindsPointer} field reads 0), using the types from the cursor's original parse --
 *       {@code ExecuteRequestReader#read} used to treat {@code bindsPointer == 0} as "no bind
 *       values at all" and left those real trailing bytes completely unread, corrupting the TTC
 *       stream for the rest of the session.
 * </ol>
 */
class OracleBatchExecuteIntegrationTest {

    @Test
    @Timeout(30)
    void addBatchExecuteBatchTwiceOnTheSamePreparedStatementInsertsAllFourRows() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("orawire", "WARP_ORAWIRE_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
            try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password())) {
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE batch_execute_it (id INTEGER PRIMARY KEY, val VARCHAR(20))");
                    conn.commit();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO batch_execute_it (id, val) VALUES (?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setString(2, "a");
                    ps.addBatch();
                    ps.setInt(1, 2);
                    ps.setString(2, "b");
                    ps.addBatch();
                    int[] batch1 = ps.executeBatch();
                    conn.commit();
                    assertEquals(2, batch1.length, "first batch must report a result per row");

                    // The SAME PreparedStatement, reused -- this second executeBatch() is a
                    // cursor-reuse Execute with NEW bind values (bug #2 above), not a fresh parse.
                    ps.setInt(1, 3);
                    ps.setString(2, "c");
                    ps.addBatch();
                    ps.setInt(1, 4);
                    ps.setString(2, "d");
                    ps.addBatch();
                    int[] batch2 = ps.executeBatch();
                    conn.commit();
                    assertEquals(2, batch2.length, "second batch must report a result per row");
                }

                try (Statement check = conn.createStatement();
                        ResultSet rs = check.executeQuery(
                                "SELECT id, val FROM batch_execute_it ORDER BY id")) {
                    for (int expectedId = 1; expectedId <= 4; expectedId++) {
                        assertTrue(rs.next(), "row " + expectedId + " must have actually landed on the backend");
                        assertEquals(expectedId, rs.getInt(1));
                        assertEquals(String.valueOf((char) ('a' + expectedId - 1)), rs.getString(2));
                    }
                    assertTrue(!rs.next(), "exactly 4 rows total, no duplicates or extras");
                }
            }
        }
    }
}
