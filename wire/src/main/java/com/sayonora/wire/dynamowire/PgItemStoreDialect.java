package com.sayonora.wire.dynamowire;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Real per-engine differences for {@link PgItemStore}'s own item upsert (PutItem, and
 * UpdateItem's own "write the merged item back" step -- both use the exact same "insert this
 * item, or replace it if the (pk_value, sk_value) pair already exists" shape) -- the same real
 * gap {@code SqswireDialect} already closed for sqswire, found live the same way: this SQL was
 * real, carefully written, and completely untested against anything but Postgres.
 *
 * <p>Postgres's own {@code INSERT ... ON CONFLICT (...) DO UPDATE} has no MySQL/Oracle/SQL Server
 * equivalent by that name -- confirmed live (a real {@code SQLSyntaxErrorException} against a
 * real MySQL backend the first time this ran against one: MySQL doesn't parse {@code ON CONFLICT}
 * at all). MySQL has its own real upsert syntax, {@code ON DUPLICATE KEY UPDATE}, needing the
 * exact same 4 bind parameters in the exact same order as Postgres (and no {@code ::jsonb} cast --
 * MySQL's {@code JSON} column type accepts a plain string via {@code setString} directly). Oracle
 * and SQL Server have neither -- both need a real {@code MERGE} statement instead, which changes
 * more than the SQL text: the bind parameters themselves have to repeat (once for the
 * {@code USING} subquery's own {@code pk_value}/{@code sk_value}, once for
 * {@code WHEN MATCHED THEN UPDATE}, once more for {@code WHEN NOT MATCHED THEN INSERT}), 8 binds
 * instead of 4 -- {@link #bindUpsert} is the one place that ordering is real, not something a
 * caller should try to reconstruct itself from {@link #upsertSql}'s own text.
 */
final class PgItemStoreDialect {

    static String upsertSql(String engine, String table) {
        return switch (engine) {
            case "mysql" -> "INSERT INTO " + table + " (pk_value, sk_value, sk_num, item) VALUES (?,?,?,?) "
                    + "ON DUPLICATE KEY UPDATE sk_num = VALUES(sk_num), item = VALUES(item)";
            case "oracle" -> "MERGE INTO " + table + " dst "
                    + "USING (SELECT ? AS pk_value, ? AS sk_value FROM dual) src "
                    + "ON (dst.pk_value = src.pk_value AND dst.sk_value = src.sk_value) "
                    + "WHEN MATCHED THEN UPDATE SET sk_num = ?, item = ? "
                    + "WHEN NOT MATCHED THEN INSERT (pk_value, sk_value, sk_num, item) VALUES (?, ?, ?, ?)";
            case "sqlserver" -> "MERGE INTO " + table + " AS dst "
                    + "USING (SELECT ? AS pk_value, ? AS sk_value) AS src "
                    + "ON (dst.pk_value = src.pk_value AND dst.sk_value = src.sk_value) "
                    + "WHEN MATCHED THEN UPDATE SET sk_num = ?, item = ? "
                    + "WHEN NOT MATCHED THEN INSERT (pk_value, sk_value, sk_num, item) VALUES (?, ?, ?, ?);";
            default -> "INSERT INTO " + table + " (pk_value, sk_value, sk_num, item) VALUES (?,?,?,?::jsonb) "
                    + "ON CONFLICT (pk_value, sk_value) DO UPDATE SET sk_num = EXCLUDED.sk_num, item = EXCLUDED.item";
        };
    }

    /** Binds {@code ps} (already prepared from {@link #upsertSql}'s own text for this same
     * {@code engine}) with the real, per-engine-correct parameter count and order. */
    static void bindUpsert(PreparedStatement ps, String engine, String pk, String sk, BigDecimal skNum, String json)
            throws SQLException {
        switch (engine) {
            case "oracle", "sqlserver" -> {
                // USING subquery
                ps.setString(1, pk);
                ps.setString(2, sk);
                // WHEN MATCHED THEN UPDATE
                bindSkNum(ps, 3, skNum);
                ps.setString(4, json);
                // WHEN NOT MATCHED THEN INSERT
                ps.setString(5, pk);
                ps.setString(6, sk);
                bindSkNum(ps, 7, skNum);
                ps.setString(8, json);
            }
            default -> {
                ps.setString(1, pk);
                ps.setString(2, sk);
                bindSkNum(ps, 3, skNum);
                ps.setString(4, json);
            }
        }
    }

    private static void bindSkNum(PreparedStatement ps, int index, BigDecimal skNum) throws SQLException {
        if (skNum != null) {
            ps.setBigDecimal(index, skNum);
        } else {
            ps.setNull(index, Types.NUMERIC);
        }
    }

    private PgItemStoreDialect() {
    }
}
