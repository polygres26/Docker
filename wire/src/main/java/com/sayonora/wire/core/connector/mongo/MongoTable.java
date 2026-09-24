package com.sayonora.wire.core.connector.mongo;

import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.Document;

/**
 * One Mongo collection exposed as a SQL table over its declared {@code fields}. Every column is
 * {@code VARCHAR} -- a schemaless document field has no single static type -- with each value
 * converted via {@code String.valueOf}.
 *
 * <p><b>No predicate pushdown, deliberately</b> (kept from the ThinkingSense source, where the risk
 * was found and rejected before shipping): because every column is declared {@code VARCHAR}
 * regardless of the field's real per-document BSON type, translating {@code field = 'literal'} into
 * {@code Filters.eq(field, "literal")} would silently match zero documents wherever that field is
 * actually stored as a number/boolean/date -- a string-typed equality never matches a differently
 * typed BSON value. That's a wrong answer, strictly worse than no pushdown, so every filter is left
 * to Calcite's own post-filter over the already-stringified values, which is always correct. Revisit
 * only alongside real per-field type declarations, never by guessing a BSON type from a literal.
 */
public final class MongoTable extends AbstractTable implements ScannableTable {

    private final MongoCollection<Document> collection;
    private final List<String> fields;

    MongoTable(MongoCollection<Document> collection, List<String> fields) {
        this.collection = collection;
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

    /** A plain whole-collection {@code find()}. Sequential only: ThinkingSense's version splits the
     * collection into disjoint {@code _id} ranges via a {@code $bucketAuto} aggregation and reads
     * them concurrently (ThinkingSense {@code com/omnigate/calcite/mongo/MongoTable.java}, {@code
     * idRangeFiltersOrEmpty}) -- a deliberate fast-follow, not ported for v1. */
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<Object[]> rows = new ArrayList<>();
        for (Document document : collection.find()) {
            rows.add(toRow(document, fields));
        }
        return Linq4j.asEnumerable(rows);
    }

    /** Missing field -> SQL NULL; anything else -> {@code String.valueOf} (an ObjectId becomes its
     * hex string, an Integer "42", a nested Document its JSON-ish {@code toString}). Top-level
     * field names only -- a dotted path is looked up as a literal key, not traversed. */
    static Object[] toRow(Document document, List<String> fields) {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Object raw = document.get(fields.get(i));
            values[i] = raw == null ? null : String.valueOf(raw);
        }
        return values;
    }
}
