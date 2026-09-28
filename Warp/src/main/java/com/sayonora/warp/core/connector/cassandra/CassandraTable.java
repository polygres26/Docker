package com.sayonora.warp.core.connector.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * One Cassandra table exposed as a queryable SQL table -- ported from the sibling ThinkingSense
 * project's real, tested {@code com.omnigate.calcite.cassandra.CassandraTable}. Real per-column
 * types (from the driver's own {@link ColumnMetadata}), unlike {@code
 * com.sayonora.warp.core.connector.mongo.MongoTable}'s blanket {@code VARCHAR}. Plain {@link
 * ScannableTable} (no predicate pushdown) -- CQL's own {@code WHERE} restrictions (partition-key-
 * or-{@code ALLOW FILTERING} only) make correct pushdown real, separate work, not attempted here.
 *
 * <p><b>REQUIRED full-scan guard, new work beyond the ThinkingSense port</b>: {@code scan()} there is
 * a genuine full-cluster table scan with zero predicate pushdown -- unsafe to expose unguarded. A
 * plain {@link ScannableTable} gives this class no visibility at all into the query's own WHERE
 * clause (only {@link org.apache.calcite.schema.FilterableTable}/{@link
 * org.apache.calcite.schema.ProjectableFilterableTable} see the filter list, and even those only
 * see Calcite-side {@code RexNode}s post-parse, not "does this CQL table's real partition key have
 * an equality match" -- upgrading to one of those interfaces to inspect filters is itself new,
 * separate scope this connector's design deliberately did not include). The mechanically real guard
 * given {@link ScannableTable}'s actual interface is therefore an EXPLICIT, construction-time
 * operator opt-in, not a runtime per-query check: either {@code partitionKeyEquals} (naming this
 * table's real partition-key columns, an attestation that this table is small/safe to fully scan)
 * or {@code allowFullScan=true} must be declared, or every {@link #scan} call refuses outright with
 * {@code ERR_CASSANDRA_FULL_SCAN_REFUSED}. {@code partitionKeyEquals}, when present, is validated
 * against the table's real partition key columns (a config typo is caught at construction, not
 * silently ignored) but does not (cannot, with this interface) change what {@link #scan} actually
 * does -- it is the same full scan either way, just one that required the operator to name real
 * partition-key columns as their attestation instead of a blunter boolean flag.
 */
final class CassandraTable extends AbstractTable implements ScannableTable {

    private final CqlSession session;
    private final String keyspace;
    private final String table;
    private final List<ColumnMetadata> columns;

    CassandraTable(CqlSession session, String keyspace, String table, TableMetadata metadata,
            Set<String> partitionKeyEquals, boolean allowFullScan) {
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
        this.columns = List.copyOf(metadata.getColumns().values());
        if (!allowFullScan) {
            if (partitionKeyEquals.isEmpty()) {
                throw com.sayonora.warp.core.ErrorCatalog.runtimeException(
                        "ERR_CASSANDRA_FULL_SCAN_REFUSED", keyspace, table);
            }
            Set<String> realPartitionKeyColumns = metadata.getPartitionKey().stream()
                    .map(c -> c.getName().asInternal())
                    .collect(java.util.stream.Collectors.toSet());
            if (!realPartitionKeyColumns.equals(partitionKeyEquals)) {
                throw new IllegalArgumentException("cassandra: table \"" + keyspace + "." + table
                        + "\"'s declared partitionKeyEquals=" + partitionKeyEquals
                        + " doesn't match its real partition key columns " + realPartitionKeyColumns
                        + " -- declare the table's ACTUAL partition key columns (an attestation this "
                        + "table is safe to fully scan), or use allowFullScan=true instead");
            }
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnMetadata column : columns) {
            builder.add(column.getName().asInternal(), sqlTypeFor(column.getType(), typeFactory)).nullable(true);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // Real bug class this guards against: SELECT * doesn't guarantee its result-set column
        // order matches TableMetadata#getColumns()'s own iteration order, so a positional
        // row[i] <-> columns[i] mapping would silently scramble values/types when the two orders
        // happen to differ. An explicit, ordered column list makes the result set's positions
        // match this.columns exactly, by construction.
        StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                columnList.append(", ");
            }
            columnList.append('"').append(columns.get(i).getName().asInternal()).append('"');
        }
        String cql = "SELECT " + columnList + " FROM \"" + keyspace + "\".\"" + table + "\"";
        List<Object[]> rows = new ArrayList<>();
        for (Row row : session.execute(cql)) {
            Object[] values = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                values[i] = readValue(row, i, columns.get(i).getType());
            }
            rows.add(values);
        }
        return Linq4j.asEnumerable(rows);
    }

    /** Converts a Cassandra CQL value to whatever Calcite's internal Enumerable-row representation
     * expects for the declared SQL type (see {@link #sqlTypeFor}). */
    private static Object readValue(Row row, int index, DataType type) {
        if (row.isNull(index)) {
            return null;
        }
        if (type.equals(DataTypes.BOOLEAN)) {
            return row.getBoolean(index);
        }
        if (type.equals(DataTypes.TINYINT)) {
            return (int) row.getByte(index);
        }
        if (type.equals(DataTypes.SMALLINT)) {
            return (int) row.getShort(index);
        }
        if (type.equals(DataTypes.INT)) {
            return row.getInt(index);
        }
        if (type.equals(DataTypes.BIGINT) || type.equals(DataTypes.COUNTER) || type.equals(DataTypes.VARINT)) {
            return row.getLong(index);
        }
        if (type.equals(DataTypes.FLOAT)) {
            return row.getFloat(index);
        }
        if (type.equals(DataTypes.DOUBLE)) {
            return row.getDouble(index);
        }
        if (type.equals(DataTypes.DECIMAL)) {
            return row.getBigDecimal(index);
        }
        if (type.equals(DataTypes.DATE)) {
            return (int) row.getLocalDate(index).toEpochDay();
        }
        if (type.equals(DataTypes.TIMESTAMP)) {
            return row.getInstant(index).toEpochMilli();
        }
        // TEXT/ASCII/UUID/TIMEUUID/INET/BLOB and every collection/UDT/tuple type: declared VARCHAR
        // (see sqlTypeFor) -- getObject's own toString() covers all of them uniformly.
        return String.valueOf(row.getObject(index));
    }

    private static RelDataType sqlTypeFor(DataType type, RelDataTypeFactory typeFactory) {
        if (type.equals(DataTypes.BOOLEAN)) {
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        }
        if (type.equals(DataTypes.TINYINT) || type.equals(DataTypes.SMALLINT) || type.equals(DataTypes.INT)) {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
        if (type.equals(DataTypes.BIGINT) || type.equals(DataTypes.COUNTER) || type.equals(DataTypes.VARINT)) {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        if (type.equals(DataTypes.FLOAT)) {
            return typeFactory.createSqlType(SqlTypeName.REAL);
        }
        if (type.equals(DataTypes.DOUBLE)) {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
        if (type.equals(DataTypes.DECIMAL)) {
            return typeFactory.createSqlType(SqlTypeName.DECIMAL);
        }
        if (type.equals(DataTypes.DATE)) {
            return typeFactory.createSqlType(SqlTypeName.DATE);
        }
        if (type.equals(DataTypes.TIMESTAMP)) {
            return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        }
        // TEXT/ASCII/UUID/TIMEUUID/INET/BLOB and every collection/UDT/tuple type: VARCHAR -- honest
        // degrade, not a real Calcite ROW/ARRAY/MAP mapping.
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
}
