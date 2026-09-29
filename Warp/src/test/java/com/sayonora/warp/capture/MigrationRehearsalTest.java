package com.sayonora.warp.capture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.sayonora.warp.capture.MigrationRehearsal.StatementOutcome;
import com.sayonora.warp.capture.MigrationRehearsal.Verdict;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@link MigrationRehearsal}'s two pure/near-pure pieces: the outcome-diff classifier (zero I/O,
 * exhaustively testable) and the row-order-insensitive content signature (a hand-built fake
 * ResultSet, so this needs no real database). Together these are what makes a migration-rehearsal
 * report trustworthy: diff() must classify every real combination correctly, and the signature
 * must not report a false mismatch just because two backends returned the same rows in a
 * different physical order (the common case for a query with no ORDER BY). */
class MigrationRehearsalTest {

    private static StatementOutcome ok(boolean hasResultSet, int rowCount, int signature, long updateCount) {
        return new StatementOutcome(true, null, null, hasResultSet, rowCount, signature, updateCount, 1_000_000L);
    }

    private static StatementOutcome error(String sqlState, String message) {
        return new StatementOutcome(false, sqlState, message, false, 0, 0, -1, 1_000_000L);
    }

    @Test
    void bothSucceedSameQueryShapeIsMatch() {
        assertEquals(Verdict.MATCH, MigrationRehearsal.diff(ok(true, 3, 42, -1), ok(true, 3, 42, -1)));
    }

    @Test
    void bothSucceedDifferentRowCountIsResultMismatch() {
        assertEquals(Verdict.RESULT_MISMATCH, MigrationRehearsal.diff(ok(true, 3, 42, -1), ok(true, 5, 42, -1)));
    }

    @Test
    void bothSucceedSameRowCountDifferentContentIsResultMismatch() {
        assertEquals(Verdict.RESULT_MISMATCH, MigrationRehearsal.diff(ok(true, 3, 42, -1), ok(true, 3, 99, -1)));
    }

    @Test
    void oneHasResultSetOtherDoesNotIsResultMismatch() {
        assertEquals(Verdict.RESULT_MISMATCH, MigrationRehearsal.diff(ok(true, 3, 42, -1), ok(false, 0, 0, 3)));
    }

    @Test
    void bothSucceedSameUpdateCountIsMatch() {
        assertEquals(Verdict.MATCH, MigrationRehearsal.diff(ok(false, 0, 0, 7), ok(false, 0, 0, 7)));
    }

    @Test
    void bothSucceedDifferentUpdateCountIsUpdateCountMismatch() {
        assertEquals(Verdict.UPDATE_COUNT_MISMATCH, MigrationRehearsal.diff(ok(false, 0, 0, 7), ok(false, 0, 0, 8)));
    }

    @Test
    void sourceOkWarpFailedIsTheSharpestSignal() {
        assertEquals(Verdict.ONLY_WARP_FAILED, MigrationRehearsal.diff(ok(true, 1, 1, -1), error("42601", "syntax error")));
    }

    @Test
    void sourceFailedWarpOkIsOnlySourceFailed() {
        assertEquals(Verdict.ONLY_SOURCE_FAILED, MigrationRehearsal.diff(error("42601", "syntax error"), ok(true, 1, 1, -1)));
    }

    @Test
    void bothFailedSameSqlStateIsEquivalentFailure() {
        assertEquals(Verdict.BOTH_FAILED_SAME_STATE, MigrationRehearsal.diff(error("23505", "duplicate key"), error("23505", "duplicate key value")));
    }

    @Test
    void bothFailedDifferentSqlStateIsNotEquivalent() {
        assertEquals(Verdict.BOTH_FAILED_DIFFERENT_STATE, MigrationRehearsal.diff(error("23505", "duplicate key"), error("42601", "syntax error")));
    }

    @Test
    void bothFailedWithNullSqlStatesOnBothSidesCountsAsSameState() {
        assertEquals(Verdict.BOTH_FAILED_SAME_STATE, MigrationRehearsal.diff(error(null, "boom"), error(null, "kaboom")));
    }

    // --- signatureOf: row-order-insensitive content signature ------------------------------------

    @Test
    void signatureIsIdenticalRegardlessOfRowOrder() throws Exception {
        ResultSet a = fakeResultSet(List.<Object[]>of(new Object[] {1, "alice"}, new Object[] {2, "bob"}));
        ResultSet b = fakeResultSet(List.<Object[]>of(new Object[] {2, "bob"}, new Object[] {1, "alice"}));
        StatementOutcome outcomeA = MigrationRehearsal.signatureOf(a, 0);
        StatementOutcome outcomeB = MigrationRehearsal.signatureOf(b, 0);
        assertEquals(outcomeA.rowCount(), outcomeB.rowCount());
        assertEquals(outcomeA.contentSignature(), outcomeB.contentSignature(),
                "two result sets with the same rows in a different order must produce the same signature");
    }

    @Test
    void signatureDiffersWhenContentActuallyDiffers() throws Exception {
        ResultSet a = fakeResultSet(List.<Object[]>of(new Object[] {1, "alice"}));
        ResultSet b = fakeResultSet(List.<Object[]>of(new Object[] {1, "ALICE"}));
        StatementOutcome outcomeA = MigrationRehearsal.signatureOf(a, 0);
        StatementOutcome outcomeB = MigrationRehearsal.signatureOf(b, 0);
        assertNotEquals(outcomeA.contentSignature(), outcomeB.contentSignature());
    }

    @Test
    void nullValuesAreDistinctFromTheStringNull() throws Exception {
        ResultSet a = fakeResultSet(List.<Object[]>of(new Object[] {(Object) null}));
        ResultSet b = fakeResultSet(List.<Object[]>of(new Object[] {"NULL"}));
        StatementOutcome outcomeA = MigrationRehearsal.signatureOf(a, 0);
        StatementOutcome outcomeB = MigrationRehearsal.signatureOf(b, 0);
        assertNotEquals(outcomeA.contentSignature(), outcomeB.contentSignature(),
                "a real NULL and the literal string \"NULL\" must not collide");
    }

    @Test
    void rowCountReflectsTheFullResultEvenWhenTruncatedForTheSignature() throws Exception {
        List<Object[]> manyRows = new java.util.ArrayList<>();
        for (int i = 0; i < 700; i++) {
            manyRows.add(new Object[] {i});
        }
        ResultSet rs = fakeResultSet(manyRows);
        StatementOutcome outcome = MigrationRehearsal.signatureOf(rs, 0);
        assertEquals(700, outcome.rowCount(), "the true row count must be reported even past the signature's row cap");
    }

    /** A minimal dynamic-proxy ResultSet/ResultSetMetaData backed by a plain row list -- just
     * enough of the interface for {@link MigrationRehearsal#signatureOf} to run against, with no
     * real database anywhere in this test. */
    private static ResultSet fakeResultSet(List<Object[]> rows) {
        int columns = rows.isEmpty() ? 1 : rows.get(0).length;
        ResultSetMetaData meta = (ResultSetMetaData) Proxy.newProxyInstance(
                MigrationRehearsalTest.class.getClassLoader(), new Class<?>[] {ResultSetMetaData.class},
                (proxy, method, methodArgs) -> "getColumnCount".equals(method.getName()) ? columns : null);
        int[] cursor = {-1};
        InvocationHandler handler = (proxy, method, methodArgs) -> {
            switch (method.getName()) {
                case "next":
                    cursor[0]++;
                    return cursor[0] < rows.size();
                case "getMetaData":
                    return meta;
                case "getObject":
                    int col = (int) methodArgs[0];
                    return rows.get(cursor[0])[col - 1];
                case "close":
                    return null;
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        };
        return (ResultSet) Proxy.newProxyInstance(MigrationRehearsalTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class}, handler);
    }
}
