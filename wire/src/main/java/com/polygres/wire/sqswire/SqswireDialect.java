package com.polygres.wire.sqswire;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Real per-engine SQL differences for {@link PgQueueStore} -- Postgres's own existing query shape
 * (a single {@code UPDATE ... WHERE msg_id = (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING ...}
 * statement) is untouched; this class is only ever consulted for Oracle/SQL Server/MySQL.
 *
 * <p><b>The "claim a message" design decision, made deliberately</b>: Oracle's own JDBC driver
 * really does support a single-statement {@code RETURNING ... INTO} equivalent via {@code
 * OraclePreparedStatement.registerReturnParameter} (verified against Oracle's own JDBC API docs)
 * -- but using it means a real, separate Oracle-specific Java code path (an {@code
 * oracle.jdbc.OraclePreparedStatement} cast), not just different SQL text. Rather than give Oracle
 * its own fourth code path, all three of Oracle/SQL Server/MySQL share ONE real, portable
 * two-statement pattern instead, run inside one transaction on the same connection: a {@code
 * SELECT ... FOR UPDATE} (with each engine's own real lock-skip hint) to find and lock a
 * candidate row (Java already has its data from this SELECT, no RETURNING needed at all), then a
 * plain {@code UPDATE ... WHERE msg_id = ?} to claim it. One extra real round trip per claim on
 * these three engines versus Postgres's single statement -- a real, disclosed tradeoff for a
 * uniform, maintainable Java code path, not a hidden cost.
 *
 * <p><b>Insert-id capture</b> ({@code SendMessage}) uses the standard JDBC {@code
 * Statement.RETURN_GENERATED_KEYS}/{@code getGeneratedKeys()} API instead of {@code RETURNING} --
 * genuinely portable across Oracle/SQL Server/MySQL's own real IDENTITY/AUTO_INCREMENT columns,
 * no engine-specific SQL needed for this one, simpler case.
 *
 * <p><b>{@code is_fifo} binding</b>: Oracle has no native SQL {@code BOOLEAN} type at all (only in
 * PL/SQL) -- {@code ddl/oracle/sqswire_catalog.sql} uses {@code NUMBER(1)}, bound here via {@code
 * setInt(1/0)}, not {@code setBoolean} (which Postgres/MySQL/SQL Server's own real boolean-ish
 * column types all accept directly).
 */
final class SqswireDialect {

    private SqswireDialect() {
    }

    static String catalogUpsertSql(String engine) {
        return switch (engine) {
            case "mysql" -> """
                    INSERT INTO _sqs_queues (queue_name, visibility_timeout, is_fifo, dlq_queue_name, max_receive_count)
                    VALUES (?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                      visibility_timeout = VALUES(visibility_timeout), is_fifo = VALUES(is_fifo),
                      dlq_queue_name = VALUES(dlq_queue_name), max_receive_count = VALUES(max_receive_count)
                    """;
            case "oracle" -> """
                    MERGE INTO _sqs_queues t
                    USING (SELECT ? AS queue_name, ? AS visibility_timeout, ? AS is_fifo, ? AS dlq_queue_name, ? AS max_receive_count FROM dual) s
                    ON (t.queue_name = s.queue_name)
                    WHEN MATCHED THEN UPDATE SET t.visibility_timeout = s.visibility_timeout, t.is_fifo = s.is_fifo,
                      t.dlq_queue_name = s.dlq_queue_name, t.max_receive_count = s.max_receive_count
                    WHEN NOT MATCHED THEN INSERT (queue_name, visibility_timeout, is_fifo, dlq_queue_name, max_receive_count)
                      VALUES (s.queue_name, s.visibility_timeout, s.is_fifo, s.dlq_queue_name, s.max_receive_count)
                    """;
            case "sqlserver" -> """
                    MERGE INTO _sqs_queues WITH (HOLDLOCK) t
                    USING (SELECT ? AS queue_name, ? AS visibility_timeout, ? AS is_fifo, ? AS dlq_queue_name, ? AS max_receive_count) s
                    ON (t.queue_name = s.queue_name)
                    WHEN MATCHED THEN UPDATE SET visibility_timeout = s.visibility_timeout, is_fifo = s.is_fifo,
                      dlq_queue_name = s.dlq_queue_name, max_receive_count = s.max_receive_count
                    WHEN NOT MATCHED THEN INSERT (queue_name, visibility_timeout, is_fifo, dlq_queue_name, max_receive_count)
                      VALUES (s.queue_name, s.visibility_timeout, s.is_fifo, s.dlq_queue_name, s.max_receive_count);
                    """;
            default -> throw new IllegalArgumentException("sqswire: no catalog upsert SQL for engine " + engine);
        };
    }

    /** See this class's own javadoc on {@code is_fifo} binding. */
    static void bindIsFifo(PreparedStatement ps, int paramIndex, boolean isFifo, String engine) throws SQLException {
        if ("oracle".equals(engine)) {
            ps.setInt(paramIndex, isFifo ? 1 : 0);
        } else {
            ps.setBoolean(paramIndex, isFifo);
        }
    }

    /** @return a real SQL expression, with exactly one {@code ?} placeholder for the integer
     *     seconds, meaning "now + N seconds" in this engine's own real dialect. */
    static String nowPlusSecondsExpr(String engine) {
        return switch (engine) {
            case "mysql" -> "DATE_ADD(NOW(6), INTERVAL ? SECOND)";
            case "oracle" -> "SYSTIMESTAMP + NUMTODSINTERVAL(?, 'SECOND')";
            case "sqlserver" -> "DATEADD(SECOND, ?, SYSDATETIMEOFFSET())";
            default -> throw new IllegalArgumentException("sqswire: no interval-add SQL for engine " + engine);
        };
    }

    private static String nowExpr(String engine) {
        return switch (engine) {
            case "mysql" -> "NOW(6)";
            case "oracle" -> "SYSTIMESTAMP";
            case "sqlserver" -> "SYSDATETIMEOFFSET()";
            default -> throw new IllegalArgumentException("sqswire: no now() SQL for engine " + engine);
        };
    }

    /** FIFO dedup lookup -- real equivalent of Postgres's own {@code enqueued_at > now() - (? ||
     * ' seconds')::interval}, one {@code ?} placeholder for the dedup-id string, plus one for the
     * window's own integer seconds (in that order). */
    static String dedupLookupSql(String engine, String table) {
        String now = nowExpr(engine);
        String limitClause = "sqlserver".equals(engine) ? "" : ("mysql".equals(engine) ? "LIMIT 1" : "FETCH FIRST 1 ROWS ONLY");
        String topClause = "sqlserver".equals(engine) ? "TOP 1 " : "";
        return switch (engine) {
            case "mysql" -> "SELECT " + topClause + "msg_id FROM " + table + " WHERE dedup_id = ? "
                    + "AND enqueued_at > DATE_SUB(" + now + ", INTERVAL ? SECOND) ORDER BY msg_id " + limitClause;
            case "oracle" -> "SELECT " + topClause + "msg_id FROM " + table + " WHERE dedup_id = ? "
                    + "AND enqueued_at > (" + now + " - NUMTODSINTERVAL(?, 'SECOND')) ORDER BY msg_id " + limitClause;
            case "sqlserver" -> "SELECT " + topClause + "msg_id FROM " + table + " WHERE dedup_id = ? "
                    + "AND enqueued_at > DATEADD(SECOND, -(?), " + now + ") ORDER BY msg_id";
            default -> throw new IllegalArgumentException("sqswire: no dedup lookup SQL for engine " + engine);
        };
    }

    /** @return true only for Postgres -- see this class's own javadoc for why Oracle/SQL Server/
     *     MySQL all share the real two-statement claim pattern instead. */
    static boolean claimIsSingleStatement(String engine) {
        return "postgres".equals(engine);
    }

    /** Step 1 of the real two-statement claim: finds and locks (each engine's own real
     * lock-skip hint) one candidate row without claiming it yet -- Java already has {@code
     * msg_id}/{@code body}/{@code read_ct} from this SELECT, so the later UPDATE never needs to
     * report anything back. */
    static String claimSelectSql(String engine, String table, boolean fifo) {
        String now = nowExpr(engine);
        String fifoPredicate = fifo
                ? " AND (t.message_group_id IS NULL OR NOT EXISTS (SELECT 1 FROM " + table + " o "
                        + "WHERE o.message_group_id = t.message_group_id AND o.vt > " + now + "))"
                : "";
        return switch (engine) {
            case "mysql" -> "SELECT t.msg_id, t.body, t.read_ct FROM " + table + " t WHERE t.vt <= " + now
                    + fifoPredicate + " ORDER BY t.msg_id LIMIT 1 FOR UPDATE SKIP LOCKED";
            // Real bug, found live, twice: Oracle rejects "FOR UPDATE" the instant its own target
            // row set was chosen through ANY view carrying an ORDER BY -- "ORDER BY ... FETCH
            // FIRST n ROWS ONLY FOR UPDATE" (ORA-02014) failed first; wrapping in a ROWNUM-filtered
            // inline view instead (the textbook pre-FETCH-FIRST idiom) hit the exact same
            // ORA-02014, because that inline view ALSO carries the ORDER BY needed to pick the
            // right row. The real, actually-working idiom (found via real Oracle community
            // threads hitting this same wall) sidesteps it entirely: pick the target row's own
            // ROWID via an ORDER BY + ROWNUM view with NO FOR UPDATE on it at all, then the OUTER
            // query -- a trivial ROWID equality lookup, no ORDER BY of its own -- is what actually
            // carries FOR UPDATE SKIP LOCKED, and Oracle has no objection to that shape.
            case "oracle" -> "SELECT msg_id, body, read_ct FROM " + table + " WHERE ROWID = ("
                    + "SELECT rid FROM (SELECT t.ROWID AS rid FROM " + table + " t WHERE t.vt <= " + now
                    + fifoPredicate + " ORDER BY t.msg_id) WHERE ROWNUM = 1) FOR UPDATE SKIP LOCKED";
            case "sqlserver" -> "SELECT TOP 1 t.msg_id, t.body, t.read_ct FROM " + table + " t WITH (ROWLOCK, READPAST, UPDLOCK) "
                    + "WHERE t.vt <= " + now + fifoPredicate + " ORDER BY t.msg_id";
            default -> throw new IllegalArgumentException("sqswire: no claim-select SQL for engine " + engine);
        };
    }

    /** Step 2 of the real two-statement claim -- plain {@code UPDATE ... WHERE msg_id = ?}, real
     * portable SQL, no engine-specific syntax beyond {@link #nowPlusSecondsExpr}'s own vt
     * expression. Bind order: (seconds, receiptHandle, msgId). */
    static String claimUpdateSql(String engine, String table) {
        return "UPDATE " + table + " SET vt = " + nowPlusSecondsExpr(engine)
                + ", receipt_handle = ?, read_ct = read_ct + 1 WHERE msg_id = ?";
    }

    /** {@code changeMessageVisibility}'s own UPDATE -- portable beyond the vt expression, same as
     * {@link #claimUpdateSql}. Bind order: (seconds, receiptHandle). */
    static String changeVisibilitySql(String engine, String table) {
        return "UPDATE " + table + " SET vt = " + nowPlusSecondsExpr(engine) + " WHERE receipt_handle = ?";
    }

    /** Real equivalent of Postgres's own {@code count(*) FILTER (WHERE ...)} -- none of Oracle,
     * SQL Server, or MySQL support {@code FILTER}; {@code SUM(CASE WHEN ... THEN 1 ELSE 0 END)}
     * is the real, portable equivalent every one of them supports identically. */
    static String visibleCountSql(String engine, String table) {
        String now = nowExpr(engine);
        return "SELECT SUM(CASE WHEN vt <= " + now + " THEN 1 ELSE 0 END) AS visible, "
                + "SUM(CASE WHEN vt > " + now + " THEN 1 ELSE 0 END) AS in_flight FROM " + table;
    }
}
