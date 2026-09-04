package com.sayonora.wire.core;

import java.io.Serializable;

/** @param typeName the real backend driver's own column type name (e.g. pgjdbc's {@code
 *      ResultSetMetaData.getColumnTypeName()} -- {@code "uuid"}/{@code "jsonb"}/{@code "bytea"}/
 *      {@code "timestamptz"}/{@code "_text"}), or {@code null} when unavailable (a
 *      backend-independent constructed column, e.g. {@code ScatterGatherAggregateMerge}'s merged
 *      result). Needed because {@code jdbcType} alone is genuinely ambiguous for real Postgres
 *      columns -- pgjdbc reports BOTH {@code uuid} and {@code jsonb} as the same generic
 *      {@code java.sql.Types.OTHER}, so only the real type name can tell pgwire's own
 *      RowDescription which actual OID to report back to the client (see
 *      {@code PgMessages#oidFor}). */
public record ColumnInfo(String name, int jdbcType, int precision, int scale, int displaySize, boolean nullable,
        String typeName) implements Serializable {

    public ColumnInfo(String name, int jdbcType, int precision, int scale, int displaySize, boolean nullable) {
        this(name, jdbcType, precision, scale, displaySize, nullable, null);
    }
}
