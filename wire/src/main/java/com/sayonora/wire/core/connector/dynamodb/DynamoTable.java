package com.sayonora.wire.core.connector.dynamodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

/**
 * One DynamoDB table exposed as a SQL table over its declared {@code fields} -- see {@link
 * DynamoSchemaFactory}'s javadoc for why the field list is explicit, every column is {@code
 * VARCHAR}, and there's no predicate pushdown. A plain {@link ScannableTable}: Calcite plans it via
 * {@code EnumerableTableScan} using the {@code EnumerableRules} {@code SchemaFederationStage}
 * already registers, so no extra planner rule is needed.
 */
public final class DynamoTable extends AbstractTable implements ScannableTable {

    private final DynamoDbClient client;
    private final String tableName;
    private final List<String> fields;

    DynamoTable(DynamoDbClient client, String tableName, List<String> fields) {
        this.client = client;
        this.tableName = tableName;
        this.fields = List.copyOf(fields);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String field : fields) {
            builder.add(field, typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(true);
        }
        return builder.build();
    }

    /**
     * A full-table {@code Scan}, paginated by the SDK's own {@code scanPaginator}.
     *
     * <p><b>Consistency: eventually consistent.</b> No {@code consistentRead(true)} is set, so this
     * uses DynamoDB's own default read consistency for Scan -- a write acknowledged moments before
     * the query may not be visible yet, and a federated JOIN against it can therefore miss (or show
     * a stale version of) a very recently written item. This is AWS's documented default, kept
     * as-is from the ported source; it must be stated in customer-facing compatibility docs.
     *
     * <p>Sequential single-segment scan only. ThinkingSense's version splits the table into
     * {@code totalSegments} real DynamoDB parallel-scan segments read concurrently through its
     * {@code LakehouseFileScanExecutor} (see ThinkingSense {@code
     * com/omnigate/calcite/dynamodb/DynamoTable.java}, {@code scan}/{@code readSegment}); that is
     * a deliberate fast-follow here, not ported for v1.
     */
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        ScanRequest request = ScanRequest.builder().tableName(tableName).build();
        for (Map<String, AttributeValue> item : client.scanPaginator(request).items()) {
            rows.add(toRow(item, fields));
        }
        return Linq4j.asEnumerable(rows);
    }

    static Object[] toRow(Map<String, AttributeValue> item, List<String> fields) {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = stringify(item.get(fields.get(i)));
        }
        return values;
    }

    /** {@code AttributeValue} is a tagged union (exactly one of S/N/BOOL/NULL/... is set): the
     * present scalar is returned as its string form. {@code N} stays DynamoDB's own exact decimal
     * string (never parsed to a double, so no precision loss). A missing attribute or {@code
     * NULL=true} maps to SQL {@code NULL}. */
    static String stringify(AttributeValue value) {
        if (value == null || Boolean.TRUE.equals(value.nul())) {
            return null;
        }
        if (value.s() != null) {
            return value.s();
        }
        if (value.n() != null) {
            return value.n();
        }
        if (value.bool() != null) {
            return String.valueOf(value.bool());
        }
        // SS/NS/BS/L/M/B: no plain scalar form. The SDK's own toString() is readable
        // ("AttributeValue(L=[...])") -- an honest degrade, not a crash.
        return value.toString();
    }
}
